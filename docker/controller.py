#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.
#
# Contrôleur 2110_io — variante DOCKER / AF_XDP (PF, pas de VF).
#
# Joue le rôle agent+contrôleur attendu par l'orchestrateur : sert :8080 (métriques, même
# format que get_metrics) et :8081 (/nmos/subscribe pour recevoir le SDP IS-05 + /status pour
# la liveness). À l'activation NMOS, écrit /tmp/nmos_recv_v_{idx}.sdp, le détecte, le PARSE
# (mcast/port/PT/format) et lance le binaire C `mtl_rx --pmd af_xdp --iface <PF>` qui reçoit le
# flux ST 2110 et l'écrit en ZÉRO-COPIE dans /dev/shm/{HOSTNAME}_{idx}. Sans SDP : mire de
# simulation (numpy), comme le receiver LXC.
#
# Config par variables d'environnement (passées par `docker run -e` du driver Docker) :
#   HOSTNAME, VIDEO_COUNT, IFACE, LCORES, RING, WIDTH, HEIGHT, FPS, CHROMA, BIT_DEPTH.
# mcast/port/PT NE viennent PAS de l'env : ils arrivent par NMOS → :8081/nmos/subscribe → SDP.
#
# Ce fichier est exécuté tel quel dans le conteneur (pas de str.format) → accolades normales.

import json, mmap, os, re, signal, struct, subprocess, threading, time
import numpy as np
from http.server import HTTPServer, BaseHTTPRequestHandler

# bobimxl : binding du SDK MXL (sur le PYTHONPATH de l'image). La simu/txgen produit des FLOWS MXL
# (mêmes que mtl_rx) au lieu des rings shm maison. Tolérant : si la lib manque (image legacy),
# _HAS_MXL=False → les boucles simu/txgen restent inertes (le live RX C reste prioritaire).
try:
    import bobimxl
    _HAS_MXL = True
except Exception as _mxl_err:
    _HAS_MXL = False
    print("controller: bobimxl indisponible:", _mxl_err, flush=True)

HOSTNAME   = os.environ.get("HOSTNAME_RX") or os.environ.get("HOSTNAME") or "mtlrx"
N_VIDEO    = int(os.environ.get("RX_COUNT") or os.environ.get("VIDEO_COUNT") or 1)   # slots RX vidéo
N_TX       = int(os.environ.get("TX_COUNT") or 0)                                     # slots TX (senders)
N_AUDIO    = int(os.environ.get("AUDIO_COUNT") or 0)                                   # slots RX audio (st30)
N_ANC      = int(os.environ.get("ANC_COUNT") or 0)                                     # slots RX ANC (st40)
A_CHANNELS = 8
A_RING     = max(2, int(os.environ.get("AUDIO_RING") or 100))   # ring shm audio (chunks 1ms)
# Ptime audio (ST 2110-30) par DÉFAUT (ms) — repli quand le SDP n'a pas d'a=ptime. Réglable par
# installation (setting mtl_audio_ptime → env AUDIO_PTIME). Le SDP a=ptime PRIME (auto par entrée).
A_PTIME_DEF = float(os.environ.get("AUDIO_PTIME") or 1.0)
ACTIVE_RX   = int(os.environ.get("ACTIVE_RX_COUNT") or min(6, max(1, N_VIDEO)))
ACTIVE_TX_C = int(os.environ.get("ACTIVE_TX_COUNT") or min(6, max(0, N_TX)))
_cpu_last_usec = None
_cpu_last_time = None
_bw_last = {}
_xdp_sessions_active = 0
# Files RÉSERVÉES au dernier mtl_init (rx_queues/tx_queues passés au lancement). Distinct de
# `_rx/_tx_queues_alloc` qui suit la DEMANDE courante (recalculée à chaque _write_config) : le daemon
# ne relit PAS rx_queues après mtl_init → la réservation est FIGÉE jusqu'au prochain (re)lancement.
# C'est le « plafond à chaud » : au-delà, créer une session échoue tant qu'on n'a pas relancé.
_rx_queues_reserved = 0
_tx_queues_reserved = 0
# subsystem_device → (label, aggregate_gbps)  — source: Intel product brief + sysfs
_E810_MODELS = {
    "0x0002": ("E810-CQDA2", 100),   # E810-C for QSFP 2-port, 1 controller (node-1 confirmé)
    "0x0003": ("E810-2CQDA2", 200),  # E810-C for QSFP 2×2-port, 2 controllers indépendants
    "0x0004": ("E810-CQDA1", 100),   # E810-C for QSFP 1-port
    "0x0005": ("E810-CQDA2", 100),   # variante OEM
}


def _nic_model(iface):
    try:
        sub = open(f"/sys/class/net/{iface}/device/subsystem_device").read().strip().lower()
        return _E810_MODELS.get(sub, ("E810 QSFP", 100))
    except Exception:
        return ("E810 QSFP", 100)


def _nic_hw_queues(iface):
    """Lit le nombre de combined queues via ethtool -l (max carte + actuel kernel).
    Retourne {"max": 48, "current": 4, "xdp_available": 44} ou None en cas d'erreur.
    Ré-interrogé à chaque appel (valeur change après réglage depuis l'UI Réglages)."""
    try:
        out = subprocess.run(["ethtool", "-l", iface],
                             capture_output=True, text=True, timeout=3).stdout
        m_max = re.search(r"Pre-set maximums.*?Combined:\s*(\d+)", out, re.S)
        m_cur = re.search(r"Current hardware settings.*?Combined:\s*(\d+)", out, re.S)
        if not m_max or not m_cur:
            return None
        hw_max = int(m_max.group(1))
        hw_cur = int(m_cur.group(1))
        return {"max": hw_max, "current": hw_cur, "xdp_available": hw_max - hw_cur}
    except Exception:
        return None


def _nic_bps(iface):
    """Débit RX/TX via ethtool -S (compteurs matériels, inclut AF_XDP zero-copy).
    sysfs statistics/tx_bytes ne compte PAS le trafic AF_XDP → toujours 0 pour MTL."""
    global _bw_last
    now = time.monotonic()
    try:
        cap = int(open(f"/sys/class/net/{iface}/speed").read().strip()) / 1000
    except Exception:
        cap = 100.0
    last = _bw_last
    # Cache : évite d'appeler ethtool à chaque requête :8080 (coût ~3 ms)
    if last and now < last.get("t", 0) + 0.5:
        return last.get("rx_gbps"), last.get("tx_gbps"), cap
    try:
        out = subprocess.run(["ethtool", "-S", iface],
                             capture_output=True, text=True, timeout=3).stdout
        def _stat(name):
            vals = re.findall(r"^\s+" + re.escape(name) + r":\s*(\d+)", out, re.M)
            return int(vals[-1]) if vals else None
        rx = _stat("rx_bytes")
        tx = _stat("tx_bytes")
    except Exception:
        _bw_last = {"rx": None, "tx": None, "t": now, "rx_gbps": None, "tx_gbps": None}
        return None, None, cap
    if rx is None or tx is None:
        _bw_last = {"rx": None, "tx": None, "t": now, "rx_gbps": None, "tx_gbps": None}
        return None, None, cap
    rx_gbps = tx_gbps = None
    if last and last.get("rx") is not None and now > last.get("t", 0) + 0.5:
        dt = now - last["t"]
        if dt > 0:
            rx_gbps = round((rx - last["rx"]) * 8 / dt / 1e9, 2)
            tx_gbps = round((tx - last["tx"]) * 8 / dt / 1e9, 2)
    _bw_last = {"rx": rx, "tx": tx, "t": now, "rx_gbps": rx_gbps, "tx_gbps": tx_gbps}
    return rx_gbps, tx_gbps, cap


def _cgroup_cpu_usec():
    try:
        with open("/sys/fs/cgroup/cpu.stat") as f:
            for line in f:
                if line.startswith("usage_usec"):
                    return int(line.split()[1])
    except Exception:
        pass
    return None


def _cgroup_mem():
    used = limit = None
    try:
        with open("/sys/fs/cgroup/memory.current") as f:
            used = int(f.read().strip())
    except Exception:
        pass
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            s = f.read().strip()
            limit = 0 if s == "max" else int(s)
    except Exception:
        pass
    return used, limit


def _get_n_cpus():
    """Cores alloués : quota cpu.max (--cpus X) prioritaire, sinon affinity (--cpuset-cpus)."""
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            parts = f.read().strip().split()
            if parts[0] != "max":
                return max(1, round(int(parts[0]) / int(parts[1])))
    except Exception:
        pass
    try:
        return max(1, len(os.sched_getaffinity(0)))
    except Exception:
        pass
    return 1


IFACE      = os.environ.get("IFACE") or "ens1f0np0"
LCORES     = os.environ.get("LCORES") or "1,2,3"
# Quota Mb/s par scheduler (lcore) libmtl : au-delà, les nouvelles sessions vont sur un autre
# lcore (≈ 2×1080p50 à 5000). Sans quota, tout s'empile sur sch_0 → epoch drops à la charge.
QUOTA_MBS  = int(os.environ.get("MTL_SCH_QUOTA_MBS") or 5000)
V_RING     = max(2, int(os.environ.get("RING") or 8))   # ring du pipeline (réglage) ; mtl_rx borne ≤8
WIDTH      = int(os.environ.get("WIDTH") or 1280)     # défaut/simu (réel = lu du SDP)
HEIGHT     = int(os.environ.get("HEIGHT") or 720)
FPS        = float(os.environ.get("FPS") or 25)
CHROMA     = str(os.environ.get("CHROMA") or "422")
BIT_DEPTH  = int(os.environ.get("BIT_DEPTH") or 10)
MTL_RX     = os.environ.get("MTL_RX_BIN") or "/usr/local/bin/mtl_rx"
HDR        = 64
SDP_DIR    = "/tmp"


def _detect_iface_ip(iface):
    """IP v4 du PF — source IP des paquets TX (sip). Sans elle, source 0.0.0.0 → paquets souvent
    rejetés / SSM impossible. La réception RX (IGMP join) n'en a pas besoin, mais l'émission oui."""
    try:
        out = subprocess.run(["ip", "-4", "-o", "addr", "show", iface],
                             capture_output=True, text=True, timeout=3).stdout
        m = re.search(r"inet (\d+\.\d+\.\d+\.\d+)", out)
        return m.group(1) if m else ""
    except Exception:
        return ""

SIP = os.environ.get("SIP") or _detect_iface_ip(IFACE)


def _detect_iface_mac(iface):
    """MAC de IFACE au format EUI-48 RFC 7273 (AA-BB-CC-DD-EE-FF) pour a=ts-refclk:localmac.
    Repli d'horloge quand PTP n'est pas dispo : un SDP sans ts-refclk est rejeté (500) par
    les récepteurs ST 2110-10 stricts (nmos-cpp)."""
    try:
        mac = open("/sys/class/net/{}/address".format(iface)).read().strip()
        return mac.upper().replace(":", "-") if mac else None
    except Exception:
        return None

IFACE_MAC = _detect_iface_mac(IFACE)

# Ligne a=ts-refclk localmac (par section média) — vide si MAC illisible. C'est un REPLI : le
# conteneur ne gère pas le PTP (ptp4l tourne sur l'hôte). L'orchestrateur REMPLACE cette ligne par
# le ts-refclk:ptp traçable du grandmaster du nœud, lu via SSH pmc (services/nmos + app/ptp).
_LOCALMAC_REFCLK = "a=ts-refclk:localmac={}\r\n".format(IFACE_MAC) if IFACE_MAC else ""

# ─── Layout shm (simu) — RÉSOLUTION DYNAMIQUE ───────────────────────
# La simu (GÉN ou fallback sans SDP) doit suivre la résolution du flux LIVE (lue du SDP) pour
# que le shm garde la MÊME taille que mtl_rx → les consommateurs ne cassent pas au basculement
# RX↔simu. WIDTH/HEIGHT (env) ne sont que le défaut quand aucun SDP n'est connu.
_DEEP    = BIT_DEPTH >= 10
_BPS     = 2 if _DEEP else 1
_DT      = "<u2" if _DEEP else "u1"
_NEUTRAL = 1 << (BIT_DEPTH - 1)
_BLACK   = 16 << (BIT_DEPTH - 8) if _DEEP else 16
_WHITE   = 235 << (BIT_DEPTH - 8) if _DEEP else 235
_CW = {"420": 2, "422": 2, "444": 1}.get(CHROMA, 2)
_CH = {"420": 2, "422": 1, "444": 1}.get(CHROMA, 1)


def _layout(w, h):
    uv_w, uv_h = w // _CW, h // _CH
    y = w * h * _BPS; uv = uv_w * uv_h * _BPS; vf = y + 2 * uv
    return {"w": w, "h": h, "uv_w": uv_w, "uv_h": uv_h,
            "y": y, "uv": uv, "vf": vf, "total": HDR + V_RING * vf}


# ─── MXL (bus partagé) — la simu/txgen écrit des flows MXL (mêmes que mtl_rx) ──────
_DTNP = np.uint16 if _DEEP else np.uint8
_MXL_DOMAIN = os.environ.get("MXL_DOMAIN") or "/dev/shm/mxl"
_mxl_inst = None
_mxl_inst_lock = threading.Lock()


def _mxl():
    """Instance MXL du process controller (domaine partagé avec mtl_rx). Paresseuse, à vie."""
    global _mxl_inst
    if _mxl_inst is None:
        with _mxl_inst_lock:
            if _mxl_inst is None:
                _mxl_inst = bobimxl.Instance(_MXL_DOMAIN)
    return _mxl_inst


def _fps_rational(f):
    """Cadence (double) → rational standard (grain_rate du flowDef). Miroir de mtl_rx.fps_to_rational."""
    f = float(f or 25.0)
    for std, n, d in ((23.98, 24000, 1001), (24, 24, 1), (25, 25, 1), (29.97, 30000, 1001),
                      (30, 30, 1), (50, 50, 1), (59.94, 60000, 1001), (60, 60, 1),
                      (100, 100, 1), (120, 120, 1)):
        if abs(f - std) < (0.1 if std in (23.98, 59.94) else 0.05):
            return n, d
    return int(round(f)), 1


def _mk_video_writer(name, w, h, fps, interlace="progressive"):
    """Writer MXL vidéo planar (index_mode tai → grille continue avec le live RX).
    `interlace`=interlaced_tff/bff → libmxl dimensionne chaque grain à 1 CHAMP (½h) et double la
    cadence ; le producteur écrit 2 grains-champs/trame aux index CHAMP (cf. _txgen_loop)."""
    n, d = _fps_rational(fps)
    return bobimxl.Writer(_mxl(), name, w, h, chroma=CHROMA, bit_depth=BIT_DEPTH,
                          fps_num=n, fps_den=d, index_mode="tai", interlace=interlace)


def _field_test(fi, f, w, fh):
    """Mire de TEST DE CHAMP (entrelacé) — révèle l'ordre/le timing de champ à l'œil.
    f = 0 (1er champ = TOP en tff) / 1 (2e champ = BOTTOM). Renvoie des plans de CHAMP (fh = h/2).
    Contenu : fond gris + BARRE verticale qui avance de 24 px/CHAMP (mouvement fort → peigne net si
    le timing de champ est faux) + BLOC marqueur haut-gauche (clair en champ 0 / sombre en champ 1)
    + teinte chroma VERTE(champ 0)/MAGENTA(champ 1) → on voit l'ordre de champ sur tout bord. Si la
    sortie est bonne : barre fluide, lignes vert/magenta régulières. Si défaut : peigne sur la barre."""
    dt = np.dtype(_DT)
    GRAY = (_BLACK + _WHITE) // 2
    y = np.full((fh, w), GRAY, dtype=dt)
    fp = fi * 2 + f                                  # compteur de CHAMP global (cadence champ)
    bx = (fp * 24) % w                               # barre : +24 px par CHAMP
    x2 = min(bx + 16, w)
    y[:, bx:x2] = _WHITE
    if bx + 16 > w:
        y[:, 0:(bx + 16 - w)] = _WHITE
    y[0:max(2, fh // 8), 0:max(2, w // 8)] = _WHITE if f == 0 else _BLACK   # marqueur de champ
    uv_w, uv_h = w // _CW, fh // _CH
    tint = (24 << (BIT_DEPTH - 8)) if _DEEP else 24
    off = -tint if f == 0 else tint                  # vert (champ 0) vs magenta (champ 1)
    cb = np.full((uv_h, uv_w), _NEUTRAL + off, dtype=dt)
    cr = np.full((uv_h, uv_w), _NEUTRAL + off, dtype=dt)
    return y, cb, cr


def _fill_grain_planes(view, lay, y, cb, cr):
    """Écrit Y|Cb|Cr (numpy _DT) dans la vue uint8 d'un grain MXL (zéro-copie, planar contigu)."""
    yb, uvb = lay["y"], lay["uv"]
    view[0:yb].view(_DTNP).reshape(lay["h"], lay["w"])[:] = y
    view[yb:yb + uvb].view(_DTNP).reshape(lay["uv_h"], lay["uv_w"])[:] = cb
    view[yb + uvb:yb + 2 * uvb].view(_DTNP).reshape(lay["uv_h"], lay["uv_w"])[:] = cr


# Résolution courante par slot (pour la simu + la taille IDENT), suit le SDP live.
_slot_res = [[WIDTH, HEIGHT] for _ in range(N_VIDEO)]

metrics = [{"idx": i, "essence": "video", "fps": 0.0, "frame_index": 0, "mode": "init"}
           for i in range(N_VIDEO)]
metrics_lock = threading.Lock()

# ─── Plan de contrôle par slot (:8082 /gen, /ident) — identique receiver_2110 ──────
# gen        : force la mire locale (simu) sur ce slot, même si un SDP est actif.
# ident      : incrustation 3 lignes (nom · source · format) en haut à droite, taille réglable.
_ctl = [{"gen": False, "ident": False, "ident_size": 0, "info": None, "pattern": "bars"}
        for _ in range(N_VIDEO)]
_ctl_lock = threading.Lock()
# Patch IDENT pré-rendu par slot : (patch2D_uint8, bw, bh) ou None. Rendu À CHAQUE CHANGEMENT
# (pas par frame) → coût CPU nul tant qu'IDENT off. Sert à l'overlay simu (numpy) ET au fichier
# binaire lu par mtl_rx (incrustation live, Partie B).
_ident_patch = [None] * N_VIDEO

try:
    from PIL import Image, ImageDraw, ImageFont
    _HAS_PIL = True
except Exception:
    _HAS_PIL = False


def _font(size):
    for p in ("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
              "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"):
        try:
            return ImageFont.truetype(p, size)
        except Exception:
            pass
    return ImageFont.load_default()


def _ident_lines(idx):
    """3 lignes : nom · source (mcast ou « Gen ») · format."""
    rt = _ctl[idx]
    info = rt.get("info")
    l1 = "{} · RX{}".format(HOSTNAME, idx)
    if rt.get("gen") or not info:
        l2 = "Gen : mire"; w, h, fps = WIDTH, HEIGHT, FPS
    else:
        l2 = "{}:{}".format(info.get("mcast", "?"), info.get("port", "?"))
        w, h, fps = info.get("width", WIDTH), info.get("height", HEIGHT), info.get("fps", FPS)
    l3 = "{}x{} {} {:.0f}p".format(w, h, CHROMA, fps)
    return [l1, l2, l3]


def _render_ident(idx):
    """Rend le patch IDENT (plan Y 8 bits) du slot. Renvoie (patch2D_uint8, bw, bh) ou None.
    La position (haut-droite) est calculée par le consommateur (overlay simu ici, mtl_rx en C)
    selon le W/H réel de la frame → patch indépendant de la résolution."""
    if not (_HAS_PIL and _ctl[idx]["ident"]):
        return None
    _h = _slot_res[idx][1]                       # taille IDENT ∝ résolution live courante
    size = int(_ctl[idx]["ident_size"]) or max(12, _h // 28)
    size = max(10, min(size, _h // 4))
    lines = _ident_lines(idx)
    font = _font(size)
    pad = max(3, size // 4); gap = max(1, size // 6)
    probe = ImageDraw.Draw(Image.new("L", (1, 1)))
    bboxes = [probe.textbbox((0, 0), t, font=font) for t in lines]
    bw = max(b[2] - b[0] for b in bboxes) + 2 * pad
    bh = sum(b[3] - b[1] for b in bboxes) + gap * (len(lines) - 1) + 2 * pad
    bw += bw % 2; bh += bh % 2                       # dims paires (alignement chroma)
    img = Image.new("L", (bw, bh), 16)               # fond Y=16
    d = ImageDraw.Draw(img); cy = pad
    for t, b in zip(lines, bboxes):
        d.text((pad - b[0], cy - b[1]), t, font=font, fill=235); cy += (b[3] - b[1]) + gap
    patch = np.frombuffer(img.tobytes(), dtype=np.uint8).reshape(bh, bw)
    return patch, bw, bh


def _ident_file(idx):
    return "/dev/shm/{}_{}_ident".format(HOSTNAME, idx)


def _update_ident(idx):
    """Re-rend le patch IDENT et le PUBLIE : cache mémoire (overlay simu) + fichier binaire
    [u32 bw][u32 bh][bh*bw octets Y8] lu par mtl_rx (live). Fichier supprimé si IDENT off."""
    p = _render_ident(idx)
    _ident_patch[idx] = p
    fpath = _ident_file(idx)
    try:
        if p is None:
            if os.path.exists(fpath):
                os.remove(fpath)
        else:
            patch, bw, bh = p
            tmp = fpath + ".tmp"
            with open(tmp, "wb") as f:
                f.write(struct.pack("II", bw, bh)); f.write(patch.tobytes())
            os.replace(tmp, fpath)   # publication atomique (mtl_rx lit le mtime)
    except Exception as e:
        print("ident file err:", e, flush=True)


def _overlay_simu(mm, off, idx, lay):
    """Incruste le patch IDENT (Y + chroma neutre) dans la frame simu (haut-droite), en place."""
    p = _ident_patch[idx]
    if not p:
        return
    patch, bw, bh = p
    w, h = lay["w"], lay["h"]
    if bw > w or bh > h:
        return
    x0 = w - bw - 8; y0 = 8
    x0 -= x0 % 2; y0 -= y0 % 2
    if x0 < 0:
        x0 = 0
    pp = (patch.astype(np.uint16) << (BIT_DEPTH - 8)) if _DEEP else patch
    y = np.frombuffer(mm, dtype=np.dtype(_DT), count=lay["y"] // _BPS, offset=off).reshape(h, w)
    y[y0:y0 + bh, x0:x0 + bw] = pp
    ux0, uy0, ubw, ubh = x0 // _CW, y0 // _CH, bw // _CW, bh // _CH
    for poff in (off + lay["y"], off + lay["y"] + lay["uv"]):
        c = np.frombuffer(mm, dtype=np.dtype(_DT), count=lay["uv"] // _BPS, offset=poff).reshape(lay["uv_h"], lay["uv_w"])
        c[uy0:uy0 + ubh, ux0:ux0 + ubw] = _NEUTRAL


def _read_tx_stats(idx):
    """Stats du sender TX idx écrites par mtl_rx (fps réel + late = trames ayant raté leur epoch)."""
    try:
        with open("/tmp/mtl_tx{}.json".format(idx)) as f:
            d = json.load(f)
        return float(d.get("fps", 0.0)), int(d.get("late", 0))
    except Exception:
        return None, None


# ─── :8080 métriques (format get_metrics) ────────────────────────────
class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        with metrics_lock:
            recs = [dict(m) for m in metrics]
        # Receivers ANC (2110-40) : pas de simu (n'existent que si abonnés) → lus à la volée depuis
        # leur stats json (fps + frame_index + timecode ATC) et exposés en essence "anc".
        for idx in range(N_ANC):
            d = _read_stats_raw("/tmp/mtl_anc{}.json".format(idx))
            if d is None:
                continue
            rec = {"idx": idx, "essence": "anc", "fps": float(d.get("fps", 0.0)),
                   "frame_index": int(d.get("frame_index", 0)),
                   "mode": "mtl" if float(d.get("fps", 0.0)) > 0 else "idle"}
            if d.get("timecode"):
                rec["timecode"] = d["timecode"]; rec["df"] = bool(d.get("df"))
            recs.append(rec)
        # fps agrégé = premier slot actif (compat get_metrics qui lit .fps top-level)
        top_fps = next((m["fps"] for m in recs if m.get("mode") == "mtl"), recs[0]["fps"] if recs else 0.0)
        # Senders TX : SDP exposé dès que la destination est configurée (même sans câblage).
        # Permet aux receivers NMOS de préparer leur abonnement IS-05 avant câblage de la source.
        with _tx_lock:
            senders = []
            for i in range(N_TX):
                t = _tx[i]
                inputs_lat = {}
                if t["enabled"] and t["shm_in"] and t.get("lat_ms") is not None:
                    inputs_lat = {t["shm_in"]: t["lat_ms"]}
                if t["mcast"] and t["udp_port"]:
                    tx_fps, tx_late = _read_tx_stats(i)
                    with _tx_gen_lock:
                        _id_on, _id_sz = _tx_gen[i]["ident"], _tx_gen[i]["ident_size"]
                    senders.append({"tx_idx": i, "idx": i, "essence": "video",
                                    "fps": tx_fps, "fps_nominal": float(t.get("fps") or 0),
                                    "late": tx_late, "sdp": _tx_sdp(i, t),
                                    "ident": _id_on, "ident_size": _id_sz,
                                    "inputs_latency_ms": inputs_lat})
                # Senders AUDIO (2110-30) : un SDP par flux audio configuré (dest mcast+port).
                for ai, acfg in enumerate(t.get("audios") or []):
                    if acfg.get("mcast") and acfg.get("port"):
                        senders.append({"tx_idx": i, "idx": i, "essence": "audio", "audio_idx": ai,
                                        "sdp": _aud_sdp(i, ai, acfg),
                                        "inputs_latency_ms": inputs_lat})
                if t.get("anc_mcast") and t.get("anc_port"):
                    senders.append({"tx_idx": i, "idx": i, "essence": "anc",
                                    "sdp": _anc_sdp(i, t),
                                    "inputs_latency_ms": inputs_lat})
        rx_gbps, tx_gbps, port_cap = _nic_bps(IFACE)
        model_label, aggregate_gbps = _nic_model(IFACE)
        hw_q = _nic_hw_queues(IFACE)
        payload = {"fps": top_fps, "receivers": recs, "senders": senders,
                   "nic": {"rx_gbps": rx_gbps, "tx_gbps": tx_gbps,
                            "port_capacity_gbps": port_cap,
                            "aggregate_gbps": aggregate_gbps,
                            "model": model_label},
                   "xdp": {"allocated":           _rx_queues_alloc + _tx_queues_alloc,
                            "reserved":            _rx_queues_reserved + _tx_queues_reserved,
                            "active":              _xdp_sessions_active,
                            "hw_max_combined":     hw_q["max"]          if hw_q else None,
                            "hw_current_combined": hw_q["current"]       if hw_q else None,
                            "hw_xdp_available":    hw_q["xdp_available"] if hw_q else None}}
        self.wfile.write(json.dumps(payload).encode())
    def log_message(self, *a): pass


# ─── :8081 contrat agent : /nmos/subscribe (SDP IS-05) + /status ─────
class AgentHandler(BaseHTTPRequestHandler):
    def _json(self, code, obj):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(obj).encode())

    def do_GET(self):
        if self.path.rstrip("/") == "/status":
            return self._json(200, {"running": True})
        if self.path.rstrip("/") == "/stats":
            global _cpu_last_usec, _cpu_last_time
            now  = time.monotonic()
            usec = _cgroup_cpu_usec()
            mem_used, mem_limit = _cgroup_mem()
            n_cpus = _get_n_cpus()
            cpu_pct = None
            if usec is not None and _cpu_last_usec is not None and _cpu_last_time is not None:
                delta_wall = (now - _cpu_last_time) * 1_000_000
                if delta_wall > 0:
                    cpu_pct = round(
                        max(0.0, min(100.0, (usec - _cpu_last_usec) / delta_wall / n_cpus * 100)), 1
                    )
            if usec is not None:
                _cpu_last_usec = usec
                _cpu_last_time = now
            return self._json(200, {"cpu_pct": cpu_pct, "mem_used": mem_used,
                                    "mem_limit": mem_limit, "cpu_count": n_cpus})
        return self._json(404, {"error": "not found"})

    def do_POST(self):
        route = self.path.rstrip("/")
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
        except Exception as e:
            return self._json(400, {"error": str(e)})

        if route == "/nmos/subscribe":           # ── RX : activation IS-05 (SDP) d'un slot receiver
            essence = body.get("essence", "video")
            idx     = int(body.get("receiver_index") or 0)
            enabled = bool(body.get("enabled"))
            sdp     = body.get("sdp")
            if isinstance(sdp, list):            # compat ancien format (deux SDP séparés) → on garde leg0
                sdp = sdp[0] if sdp else None
            if essence not in ("video", "audio", "anc"):
                return self._json(200, {"ok": True, "note": "{} ignoré".format(essence)})
            pfx  = {"audio": "nmos_recv_a_", "anc": "nmos_recv_anc_"}.get(essence, "nmos_recv_v_")
            path = os.path.join(SDP_DIR, "{}{}.sdp".format(pfx, idx))
            try:
                if enabled and sdp:
                    with open(path, "w") as f: f.write(sdp)
                elif os.path.exists(path):
                    os.remove(path)
            except Exception as e:
                return self._json(500, {"error": str(e)})
            return self._json(200, {"ok": True})

        if route == "/tx":                       # ── TX : spec complète d'un slot sender (poussée par l'orchestrateur)
            try:
                idx = int(body.get("idx", 0))
            except Exception:
                idx = -1
            if not (0 <= idx < N_TX):
                return self._json(400, {"error": "idx TX hors limites"})
            with _tx_lock:
                t = _tx[idx]
                t["enabled"] = bool(body.get("enabled"))
                if "mcast"     in body: t["mcast"]     = body.get("mcast") or None
                if "udp_port"  in body: t["udp_port"]  = int(body.get("udp_port") or 0)
                if "mcast2"    in body: t["mcast2"]    = body.get("mcast2") or None
                if "udp_port2" in body: t["udp_port2"] = int(body.get("udp_port2") or 0)
                if "pt"        in body: t["pt"]        = int(body.get("pt") or 96)
                if "shm_in"    in body: t["shm_in"]    = (body.get("shm_in") or "").strip() or None
                if "audios" in body:
                    t["audios"] = [{"mcast": a.get("mcast") or None,
                                    "port": int(a.get("port") or 0),
                                    "pt": int(a.get("pt") or 97),
                                    "mcast2": a.get("mcast2") or None,
                                    "port2": int(a.get("port2") or 0)}
                                   for a in (body.get("audios") or [])[:2]]
                if "anc_mcast"   in body: t["anc_mcast"]   = body.get("anc_mcast") or None
                if "anc_port"    in body: t["anc_port"]    = int(body.get("anc_port") or 0)
                if "anc_mcast2"  in body: t["anc_mcast2"]  = body.get("anc_mcast2") or None
                if "anc_port2"   in body: t["anc_port2"]   = int(body.get("anc_port2") or 0)
                if "anc_pt"      in body: t["anc_pt"]      = int(body.get("anc_pt") or 97)
                # scan/field_order : ne clobber QUE si fournis (le câblage :8082/input porte le
                # format réel de la source ; un push /tx de dest seule ne doit pas les écraser).
                if "scan"        in body: t["scan"]        = "i" if str(body.get("scan")).lower() == "i" else "p"
                if "field_order" in body: t["field_order"] = "bff" if str(body.get("field_order")).lower() == "bff" else "tff"
                for k_in, k_st in (("width","w"),("height","h"),("fps","fps"),("bit_depth","bd"),("ring","ring")):
                    if body.get(k_in):
                        t[k_st] = (float(body[k_in]) if k_st == "fps" else int(body[k_in]))
                # Resync des câblages audio/ANC indépendants (le conteneur redémarre sans état).
                if "audio_shm_in" in body:
                    lst = body.get("audio_shm_in") or []
                    t["audio_cable_shm"] = [((lst[ai] or "").strip() or None) if ai < len(lst) else None
                                            for ai in range(N_AUD_PER_TX)]
                if "anc_shm_in" in body:
                    t["anc_cable_shm"] = (body.get("anc_shm_in") or "").strip() or None
            # Resync de la config de tonalité par sous-flux audio (audios[ai]["tone"]).
            for _ai, _a in enumerate((body.get("audios") or [])[:2]):
                _tn = (_a or {}).get("tone")
                if isinstance(_tn, dict):
                    with _tx_gen_lock:
                        cur = _tx_tone[idx][_ai]
                        cur["enabled"]  = bool(_tn.get("enabled"))
                        cur["freq"]     = int(_tn.get("freq") or 1000)
                        cur["level_db"] = float(_tn.get("level_db") if _tn.get("level_db") is not None else -18.0)
                        _act = _tn.get("active") or []
                        _rup = _tn.get("rupted") or []
                        cur["active"]  = [bool(_act[c]) if c < len(_act) else True for c in range(A_CHANNELS)]
                        cur["rupted"]  = [bool(_rup[c]) if c < len(_rup) else False for c in range(A_CHANNELS)]
            if "gen_enabled" in body:
                with _tx_gen_lock:
                    _tx_gen[idx]["user_enabled"] = bool(body.get("gen_enabled"))
            if "gen_pattern" in body:
                with _tx_gen_lock:
                    _tx_gen[idx]["pattern"] = str(body.get("gen_pattern") or "bars")
            _ident_dirty = False
            if "ident" in body:
                with _tx_gen_lock:
                    _tx_gen[idx]["ident"] = bool(body.get("ident"))
                _ident_dirty = True
            if "ident_size" in body:
                with _tx_gen_lock:
                    try: _tx_gen[idx]["ident_size"] = max(0, int(body.get("ident_size") or 0))
                    except Exception: pass
                _ident_dirty = True
            if _ident_dirty:
                _update_tx_ident(idx)
            if "fallback_mode" in body:
                v = str(body.get("fallback_mode") or "none")
                with _tx_lock:
                    _tx[idx]["fallback_mode"] = v if v in ("none", "black", "bars") else "none"
            _tx_gen_apply(idx)
            return self._json(200, {"ok": True})

        return self._json(404, {"error": "not found"})

    def log_message(self, *a): pass


# ─── :8082 contrôle à chaud : /gen, /ident (RX) + /input, /state (câblage TX) ─────
class ControlHandler(BaseHTTPRequestHandler):
    def _json(self, code, obj):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(obj).encode())

    def do_GET(self):
        # /state : shm câblé sur chaque slot TX (lu par la page Câbles via state_field tx{i}_shm)
        if self.path.rstrip("/") == "/state":
            with _tx_lock:
                st = {"tx{}_shm".format(i): (_tx[i].get("cable_shm") or "") for i in range(N_TX)}
                for i in range(N_TX):
                    # Câblages audio (index linéaire ap) + ANC, lus par la page Câbles (state_field).
                    for ai in range(N_AUD_PER_TX):
                        st["tx_audio{}_shm".format(i * N_AUD_PER_TX + ai)] = (
                            (_tx[i]["audio_cable_shm"] or [None] * N_AUD_PER_TX)[ai] or "")
                    st["tx_anc{}_shm".format(i)] = (_tx[i].get("anc_cable_shm") or "")
            return self._json(200, st)
        return self._json(404, {"error": "not found"})

    def do_POST(self):
        path = self.path.rstrip("/")
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
        except Exception as e:
            return self._json(400, {"error": str(e)})

        if path == "/input":        # câblage à chaud d'un shm vers un slot TX (générique plugin)
            essence = body.get("essence", "video")
            if essence == "audio":
                # Câblage AUDIO indépendant : slot = index linéaire ap → (i, ai). shm vide = décâble
                # (silence). Format audio fixe (8ch L24/48k) → rien à poser.
                try: ap = int(body.get("slot", -1))
                except Exception: ap = -1
                i, ai = divmod(ap, N_AUD_PER_TX) if ap >= 0 else (-1, -1)
                if not (0 <= i < N_TX and 0 <= ai < N_AUD_PER_TX):
                    return self._json(400, {"error": "slot audio TX hors limites"})
                with _tx_lock:
                    _tx[i]["audio_cable_shm"][ai] = (body.get("shm") or "").strip() or None
                return self._json(200, {"ok": True})
            if essence == "data":
                # Câblage ANC indépendant : slot = index du slot TX. shm vide = décâble.
                try: i = int(body.get("slot", -1))
                except Exception: i = -1
                if not (0 <= i < N_TX):
                    return self._json(400, {"error": "slot ANC TX hors limites"})
                with _tx_lock:
                    _tx[i]["anc_cable_shm"] = (body.get("shm") or "").strip() or None
                return self._json(200, {"ok": True})
            try: slot = int(body.get("slot", 0))
            except Exception: slot = -1
            if not (0 <= slot < N_TX):
                return self._json(400, {"error": "slot TX hors limites"})
            shm = (body.get("shm") or "").strip() or None
            fmt = body.get("format") or {}
            with _tx_lock:
                t = _tx[slot]
                t["cable_shm"] = shm                # mémorise le câblage réel (séparé de shm_in qui peut pointer txgen)
                if fmt.get("width"):     t["w"] = int(fmt["width"])
                if fmt.get("height"):    t["h"] = int(fmt["height"])
                if fmt.get("bit_depth"): t["bd"] = int(fmt["bit_depth"])
                if fmt.get("fps"):       t["fps"] = float(fmt["fps"])
                # Mode de balayage de la SOURCE → passthrough entrelacé (émis tel quel en 2110-20).
                if "scan" in fmt:        t["scan"] = "i" if str(fmt.get("scan")).lower() == "i" else "p"
                if fmt.get("field_order"): t["field_order"] = "bff" if str(fmt["field_order"]).lower() == "bff" else "tff"
            # shm_in et enabled recalculés : câblage prime toujours sur gen
            _tx_gen_apply(slot)
            return self._json(200, {"ok": True})

        if path == "/fieldtest":    # DIAGNOSTIC : mire de TEST DE CHAMP entrelacée sur un slot TX
            # Bascule le slot en mire GÉNÉRÉE 1080i (sans le Newt) → isole entrée vs sortie : si la
            # mire combe à l'écran = défaut d'ÉMISSION TX ; si propre = défaut d'ENTRÉE (RX). On
            # mémorise le câblage réel pour le restaurer à l'extinction (enabled=false).
            try: slot = int(body.get("idx", 0))
            except Exception: slot = -1
            if not (0 <= slot < N_TX):
                return self._json(400, {"error": "slot TX hors limites"})
            on = bool(body.get("enabled", True))
            fo = "bff" if str(body.get("field_order", "tff")).lower() == "bff" else "tff"
            with _tx_lock:
                t = _tx[slot]
                if on:
                    if slot not in _fieldtest_saved:
                        _fieldtest_saved[slot] = (t["cable_shm"], t["w"], t["h"], t["fps"],
                                                  t["scan"], t["field_order"])
                    t["cable_shm"] = None
                    t["w"], t["h"], t["fps"] = 1920, 1080, 25.0
                    t["scan"], t["field_order"] = "i", fo
                else:
                    sv = _fieldtest_saved.pop(slot, None)
                    if sv:
                        (t["cable_shm"], t["w"], t["h"], t["fps"],
                         t["scan"], t["field_order"]) = sv
            with _tx_gen_lock:
                _tx_gen[slot]["user_enabled"] = on
                if on:
                    _tx_gen[slot]["pattern"] = "field_test"
            _tx_gen_apply(slot)
            return self._json(200, {"ok": True, "slot": slot, "enabled": on, "field_order": fo})

        try:
            idx = int(body.get("idx", 0))
        except Exception:
            idx = -1
        if not (0 <= idx < N_VIDEO):
            return self._json(400, {"error": "idx hors limites"})
        if path == "/gen":          # bascule générateur simu (force la mire sur ce slot)
            with _ctl_lock:
                _ctl[idx]["gen"] = bool(body.get("enabled"))
                if "pattern" in body:
                    _ctl[idx]["pattern"] = str(body["pattern"])
            return self._json(200, {"ok": True})
        if path == "/ident":        # bascule/taille de l'incrustation (à chaud, sans respawn)
            with _ctl_lock:
                if "enabled" in body:
                    _ctl[idx]["ident"] = bool(body["enabled"])
                if "size" in body:
                    try: _ctl[idx]["ident_size"] = max(0, int(body["size"] or 0))
                    except Exception: pass
            _update_ident(idx)
            return self._json(200, {"ok": True, "pil": _HAS_PIL})

        if path == "/gen_tx":       # bascule le générateur de mire sur un slot TX
            try: tx_idx = int(body.get("idx", -1))
            except Exception: tx_idx = -1
            if not (0 <= tx_idx < N_TX):
                return self._json(400, {"error": "idx TX hors limites"})
            with _tx_gen_lock:
                _tx_gen[tx_idx]["user_enabled"] = bool(body.get("enabled"))
                if "pattern" in body:
                    _tx_gen[tx_idx]["pattern"] = str(body["pattern"])
            _tx_gen_apply(tx_idx)
            return self._json(200, {"ok": True})

        if path == "/ident_tx":     # bascule/taille de l'IDENT sur une sortie TX (overlay émis)
            try: tx_idx = int(body.get("idx", -1))
            except Exception: tx_idx = -1
            if not (0 <= tx_idx < N_TX):
                return self._json(400, {"error": "idx TX hors limites"})
            with _tx_gen_lock:
                if "enabled" in body:
                    _tx_gen[tx_idx]["ident"] = bool(body["enabled"])
                if "size" in body:
                    try: _tx_gen[tx_idx]["ident_size"] = max(0, int(body["size"] or 0))
                    except Exception: pass
            _update_tx_ident(tx_idx)
            return self._json(200, {"ok": True, "pil": _HAS_PIL})

        if path == "/tone_tx":      # générateur de tonalité (1 kHz/-18 dBFS, canaux + ruptage) d'une sortie audio
            try: tx_idx = int(body.get("idx", -1)); ai = int(body.get("ai", -1))
            except Exception: tx_idx = ai = -1
            if not (0 <= tx_idx < N_TX and 0 <= ai < 2):
                return self._json(400, {"error": "idx/ai TX hors limites"})
            with _tx_gen_lock:
                tn = _tx_tone[tx_idx][ai]
                if "enabled" in body:
                    tn["enabled"] = bool(body["enabled"])
                if "freq" in body:
                    try: tn["freq"] = max(20, min(20000, int(body["freq"] or 1000)))
                    except Exception: pass
                if "level_db" in body:
                    try: tn["level_db"] = max(-60.0, min(0.0, float(body["level_db"])))
                    except Exception: pass
                if isinstance(body.get("active"), list):
                    tn["active"] = [bool(x) for x in body["active"][:A_CHANNELS]] + \
                                   [False] * max(0, A_CHANNELS - len(body["active"]))
                if isinstance(body.get("rupted"), list):
                    tn["rupted"] = [bool(x) for x in body["rupted"][:A_CHANNELS]] + \
                                   [False] * max(0, A_CHANNELS - len(body["rupted"]))
            return self._json(200, {"ok": True})

        return self._json(404, {"error": "not found"})

    def log_message(self, *a): pass


threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8080), MetricsHandler).serve_forever(),
                 daemon=True).start()
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8081), AgentHandler).serve_forever(),
                 daemon=True).start()
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8082), ControlHandler).serve_forever(),
                 daemon=True).start()


# ─── Parseur SDP minimal (ST 2110-20) ───────────────────────────────
def _parse_sdp(path):
    try:
        txt = open(path).read()
    except Exception:
        return None
    info = {"interlaced": False}
    m = re.search(r"^m=video\s+(\d+)\s+RTP/AVP\s+(\d+)", txt, re.M)
    if not m:
        return None
    info["port"] = int(m.group(1))
    info["pt"]   = int(m.group(2))
    c = re.search(r"^c=IN\s+IP4\s+([0-9.]+)", txt, re.M)
    if not c:
        return None
    info["mcast"] = c.group(1)
    fmtp = re.search(r"^a=fmtp:\d+\s+(.*)$", txt, re.M)
    if fmtp:
        params = fmtp.group(1)
        w = re.search(r"width=(\d+)", params)
        h = re.search(r"height=(\d+)", params)
        fr = re.search(r"exactframerate=(\d+)(?:/(\d+))?", params)
        if w: info["width"] = int(w.group(1))
        if h: info["height"] = int(h.group(1))
        if "interlace" in params:
            info["interlaced"] = True
        if fr:
            num = int(fr.group(1)); den = int(fr.group(2)) if fr.group(2) else 1
            fps = num / den
            # ST 2110-20 : un flux entrelacé conforme n'a jamais exactframerate > 30. Une source
            # non conforme annonçant la cadence CHAMP (field rate) est ramenée à la cadence trame.
            if info.get("interlaced") and fps > 30:
                fps /= 2.0
            info["fps"] = round(fps, 2)
    info.setdefault("width", WIDTH)
    info.setdefault("height", HEIGHT)
    info.setdefault("fps", FPS)
    return info


# ─── Audio (ST 2110-30) ─────────────────────────────────────────────
def _parse_sdp_audio(path):
    """SDP minimal 2110-30 : m=audio + c= + pt. Canaux fixés à 8 (L24/48k, convention MXL)."""
    try:
        txt = open(path).read()
    except Exception:
        return None
    m = re.search(r"^m=audio\s+(\d+)\s+RTP/AVP\s+(\d+)", txt, re.M)
    c = re.search(r"^c=IN\s+IP4\s+([0-9.]+)", txt, re.M)
    if not (m and c):
        return None
    # Ptime (durée de paquet, ms) : a=ptime:<v>. AUTO par entrée — doit matcher le flux sinon
    # mtl_rx droppe TOUS les paquets (« pkt len mismatch »). Absent → défaut install (A_PTIME_DEF).
    pt_m = re.search(r"^a=ptime:\s*([0-9.]+)", txt, re.M)
    ptime = float(pt_m.group(1)) if pt_m else A_PTIME_DEF
    return {"port": int(m.group(1)), "pt": int(m.group(2)), "mcast": c.group(1),
            "channels": A_CHANNELS, "ptime": ptime}

def _audio_session(idx, info):
    """Session RX audio st30 → /dev/shm/{hn}_audio_{idx} (L24 8ch BE, écrit tel quel par mtl_rx)."""
    return {"kind": "audio", "role": "rx",
            "mcast": info["mcast"], "udp_port": info["port"], "payload_type": info["pt"],
            "channels": info.get("channels", A_CHANNELS), "ptime": info.get("ptime", A_PTIME_DEF),
            "ring": A_RING, "hdr": HDR,
            "targets": [{"idx": idx, "shm": "/dev/shm/{}_audio_{}".format(HOSTNAME, idx),
                         "stats": "/tmp/mtl_a{}.json".format(idx)}]}

def _derive_audio_shm(video_shm, idx=0):
    """shm vidéo câblé → shm audio associé : 'host_0' + idx=1 → 'host_audio_1'."""
    m = re.match(r"^(.*)_(\d+)$", (video_shm or "").strip())
    return "{}_audio_{}".format(m.group(1), idx) if m else None

def _audio_tx_session(idx, acfg, shm_in):
    """Session TX audio st30 : émet le shm audio d'entrée (BE passthrough) vers la dest audio."""
    return {"kind": "audio", "role": "tx",
            "mcast": acfg["mcast"], "udp_port": acfg["port"], "payload_type": acfg.get("pt", 97),
            "channels": A_CHANNELS, "ptime": A_PTIME_DEF, "ring": A_RING, "hdr": HDR,
            "targets": [{"idx": idx, "shm": shm_in, "stats": "/tmp/mtl_atx{}.json".format(idx)}]}


# ─── ANC (ST 2110-40 / data) ────────────────────────────────────────
def _parse_sdp_anc(path):
    """SDP 2110-40 : un m=video déclaré en smpte291 (a=rtpmap:<pt> smpte291/90000). On extrait
    c=/port/pt. C'est l'ANC : passthrough + extraction timecode côté mtl_rx."""
    try:
        txt = open(path).read()
    except Exception:
        return None
    m = re.search(r"^m=video\s+(\d+)\s+RTP/AVP\s+(\d+)", txt, re.M)
    c = re.search(r"^c=IN\s+IP4\s+([0-9.]+)", txt, re.M)
    if not (m and c and re.search(r"smpte291", txt, re.I)):
        return None
    return {"port": int(m.group(1)), "pt": int(m.group(2)), "mcast": c.group(1)}

def _anc_session(idx, info):
    """Session RX ANC st40 → /dev/shm/{hn}_anc_{idx} (meta+udw sérialisés par mtl_rx)."""
    return {"kind": "data", "role": "rx",
            "mcast": info["mcast"], "udp_port": info["port"], "payload_type": info["pt"],
            "ring": 8, "hdr": HDR,
            "targets": [{"idx": idx, "shm": "/dev/shm/{}_anc_{}".format(HOSTNAME, idx),
                         "stats": "/tmp/mtl_anc{}.json".format(idx)}]}

def _derive_anc_shm(video_shm):
    """shm vidéo câblé → shm ANC associé : 'mtl_0' → 'mtl_anc_0' (None si pas de _N final)."""
    m = re.match(r"^(.*)_(\d+)$", (video_shm or "").strip())
    return "{}_anc_{}".format(m.group(1), m.group(2)) if m else None

def _anc_tx_session(idx, t, shm_in):
    """Session TX ANC st40 : ré-émet le shm ANC d'entrée (passthrough) vers la dest ANC du slot."""
    return {"kind": "data", "role": "tx",
            "mcast": t["anc_mcast"], "udp_port": t["anc_port"], "payload_type": t.get("anc_pt", 97),
            "fps": t.get("fps") or FPS, "ring": 8, "hdr": HDR,
            "targets": [{"idx": idx, "shm": shm_in, "stats": "/tmp/mtl_anctx{}.json".format(idx)}]}


# ─── Gestionnaire de sessions central ───────────────────────────────
# UN SEUL mtl_rx multi-session (un mtl_init = un PF) sert TOUTES les sessions actives. Le manager
# recalcule l'ensemble actif (slots avec SDP, pas GÉN) et (re)lance mtl_rx avec un config JSON
# quand cet ensemble change. Les slots inactifs sont écrits en simu par leur _simu_loop.
_CONFIG_PATH = "/tmp/mtl_config.json"
_mtl_proc = None
_mtl_lock = threading.Lock()
_cur_sig = None
_live = [False] * N_VIDEO     # slot vidéo idx actuellement servi par mtl_rx ?
_last_launch = 0.0            # horodatage du dernier (re)lancement de mtl_rx
_fail_streak = 0             # échecs rapides consécutifs (backoff)

# ─── Slots TX (émetteurs) — poussés par l'orchestrateur via :8081/tx ──────────
# Chaque slot TX = une destination (mcast/port) + un shm d'ENTRÉE câblé (+ son format). Le manager
# en fait une session role=tx dans le config ; mtl_rx lit le shm et émet en 2110-20. Un slot sans
# shm_in (non câblé) ou désactivé n'émet pas.
# Flux audio par slot TX (= audio_count // tx_count, ≥1) — base de l'index LINÉAIRE des ports audio
# câblables ap = i*N_AUD_PER_TX + ai (cf. hook topology_ports / manifeste). Doit matcher la formule
# orchestrateur (before_deploy).
N_AUD_PER_TX = max(1, (N_AUDIO // N_TX) if N_TX else 1)
_tx = [{"enabled": False, "mcast": None, "udp_port": 0, "pt": 96,
        "mcast2": None, "udp_port2": 0,      # leg1 SMPTE 2022-7 (vidéo)
        "shm_in": None, "cable_shm": None,   # cable_shm = shm câblé réel (distinct de shm_in qui peut pointer vers txgen)
        "fallback_mode": "black",             # repli automatique sans câble : "none"|"black"|"bars"
        "w": WIDTH, "h": HEIGHT, "fps": FPS, "bd": BIT_DEPTH, "ring": V_RING,
        "scan": "p", "field_order": "tff",   # passthrough entrelacé : suit le format de la source câblée
        "audios": [],       # liste de {mcast, port, pt, mcast2, port2} — jusqu'à 2 flux 2110-30
        "audio_cable_shm": [None] * N_AUD_PER_TX,  # shm audio CÂBLÉ par sous-flux ai (indépendant de la vidéo)
        "anc_cable_shm": None,                     # shm ANC CÂBLÉ (indépendant de la vidéo)
        "anc_mcast": None, "anc_port": 0, "anc_pt": 97,
        "anc_mcast2": None, "anc_port2": 0,  # leg1 ANC (SMPTE 2022-7)
        "lat_ms": None}     # âge du signal depuis la capture originale (ts_ns header SHM)
       for _ in range(N_TX)]
_tx_lock = threading.Lock()

# Générateur TX : mire synthétique + silence audio (gen explicite ou repli auto selon fallback_mode).
# user_enabled = ON/OFF explicite depuis l'UI ; enabled = état effectif (inclut le fallback auto).
# câblé → shm câblé prime toujours ; user_gen > fallback > rien.
_tx_gen = [{"user_enabled": False, "enabled": False, "pattern": "bars",
            "ident": False, "ident_size": 0} for _ in range(N_TX)]
_tx_gen_lock = threading.Lock()
# Mire de test de champ (route :8082 /fieldtest) : sauvegarde du câblage réel par slot pour
# restauration à l'extinction (le slot bascule en mire générée 1080i le temps du diagnostic).
_fieldtest_saved = {}

# Générateur de TONALITÉ par sortie audio (autonome, indépendant du GEN vidéo) — modèle des entrées :
# par (slot i, sous-flux audio ai) {enabled, freq, level_db, active[8], rupted[8]}. Quand enabled,
# l'audio émis du flux est la tonalité générée (écrase l'audio câblé). Protégé par _tx_gen_lock.
def _default_tone():
    return {"enabled": False, "freq": 1000, "level_db": -18.0,
            "active": [True] * A_CHANNELS, "rupted": [False] * A_CHANNELS}
_tx_tone = [[_default_tone() for _ in range(2)] for _ in range(N_TX)]


def _tx_lat_monitor():
    """Thread passif : latence transit de chaque slot Tx actif = (now_tai − lastWriteTime) du FLUX
    MXL d'entrée câblé (runtime info MXL, en TAI ns). Expose _tx[i]["lat_ms"] pour les métriques
    :8080. Readers MXL mis en cache par nom de flux (recréés si le flux est absent/remplacé)."""
    if not _HAS_MXL:
        return
    readers = {}   # nom de flux → bobimxl.Reader
    def _drop(nm):
        r = readers.pop(nm, None)
        if r is not None:
            try: r.close()
            except Exception: pass
    while True:
        with _tx_lock:
            slots = [(i, t["shm_in"]) for i, t in enumerate(_tx) if t["enabled"] and t["shm_in"]]
        wanted = set()
        for idx, shm_name in slots:
            nm = shm_name[9:] if shm_name.startswith("/dev/shm/") else shm_name
            wanted.add(nm)
            lat = None
            try:
                r = readers.get(nm)
                if r is None:
                    r = bobimxl.Reader(_mxl(), nm); readers[nm] = r
                lw = r.last_write_time()
                if lw:
                    v = round((bobimxl.now_tai() - lw) / 1e6, 1)
                    lat = v if 0 < v < 30_000 else None
                else:
                    _drop(nm)   # flux absent/remplacé → recréer le reader au tour suivant
            except Exception:
                _drop(nm)
            with _tx_lock:
                _tx[idx]["lat_ms"] = lat
        for nm in [k for k in readers if k not in wanted]:
            _drop(nm)
        time.sleep(0.1)   # 10 Hz — runtime info uniquement, coût négligeable


threading.Thread(target=_tx_lat_monitor, daemon=True).start()


def _xdp_off():
    """Détache tout programme XDP résiduel de l'interface (l'hôte est partagé en --network host).
    INDISPENSABLE entre deux lancements de mtl_rx : même un arrêt gracieux ne détache pas toujours
    le XDP (mtl_uninit incomplet, MtlManager qui ne peut pas remplacer un dispatcher existant) →
    la mtl_init suivante échoue en boucle (`native xdp dev init fail -5`). On repart d'une interface
    propre. Coût : refaute ptp4l ~15 s (auto-recovery), acceptable au (re)lancement."""
    try:
        subprocess.run(["ip", "link", "set", IFACE, "xdp", "off"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
    except Exception as e:
        print("xdp off échoué:", e, flush=True)


def _flush_ntuple():
    """Purge les règles ntuple (flow director) résiduelles de l'interface AVANT chaque (re)lancement
    de mtl_rx. Un arrêt NON gracieux (SIGKILL via `docker rm -f`, ou crash) laisse les règles fdir
    sur le MATÉRIEL (elles survivent au conteneur) ; la création d'un nouveau flow pour le même
    5-tuple échoue alors (« socket add flow fail » → init_hw fail -5) → session muette sans retry.
    On repart d'une table de flow propre — mtl_init réinstalle les règles voulues. Sûr : l'interface
    (PF E810) est dédiée à MTL sur ce nœud."""
    try:
        out = subprocess.run(["ethtool", "-n", IFACE], capture_output=True, text=True, timeout=5).stdout
        ids = re.findall(r"Filter:\s*(\d+)", out)
        for rid in ids:
            subprocess.run(["ethtool", "-N", IFACE, "delete", rid],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
        if ids:
            print("ntuple purge: {} règle(s) résiduelle(s) supprimée(s) avant mtl_init".format(len(ids)),
                  flush=True)
    except Exception as e:
        print("ntuple purge échouée:", e, flush=True)


# Multicast PTP (SMPTE 2059-2 / IEEE 1588) : Announce/Sync/Delay sur 224.0.1.129, P2P delay sur
# 224.0.0.107. Par défaut RSS répartit ce trafic sur TOUTES les queues — dont celles possédées par
# les sockets AF-XDP de libmtl, qui l'avalent → ptp4l noyau ne reçoit plus les Announce → bascules
# SLAVE↔MASTER permanentes. La 1ʳᵉ approche (ethtool -N → queue 0) est INCOMPATIBLE avec AF-XDP
# (ntuple ↔ flow-steering libmtl mutuellement exclusifs). On restreint donc la table RSS à la queue
# 0 (cf. _steer_ptp_to_kernel_queue) : PTP → queue 0 (noyau), média → queues XSK via fdir libmtl.
PTP_MCAST = ("224.0.1.129", "224.0.0.107")
PTP_KERNEL_QUEUE = 0

def _steer_ptp_to_kernel_queue():
    """PTP coexistence avec AF-XDP — par RESTRICTION RSS (PAS ethtool -N).

    Sur l'E810/ice, les règles ntuple `ethtool -N` sont MUTUELLEMENT EXCLUSIVES avec le flow-steering
    AF-XDP de libmtl : dès qu'une règle RX libmtl existe, l'ajout d'une règle ntuple échoue (« rmgr:
    Cannot insert RX class rule »), et symétriquement une règle ntuple PTP préexistante fait échouer
    la création des flows RX (« socket add flow fail for queue 1 »). On NE PEUT donc PAS épingler le
    PTP via ntuple sur un moteur qui reçoit.

    À la place on restreint la table d'indirection RSS à la SEULE queue noyau (queue 0) : tout le
    trafic haché par RSS — dont le multicast PTP (224.0.1.129 / 224.0.0.107) — tombe sur la queue 0
    (ptp4l noyau), tandis que libmtl dirige EXPLICITEMENT ses flux média vers les queues XSK (≥1) via
    ses propres règles fdir, qui PRIMENT sur RSS. Aucune règle ntuple → aucun conflit. (Validé E810 :
    RX média intacte à 50 fps après `ethtool -X equal 1`.) Réappliqué une fois après le mtl_init du
    daemon (par sécurité, au cas où l'init toucherait la table). Best-effort."""
    def _apply(tag):
        try:
            rc = subprocess.run(["ethtool", "-X", IFACE, "equal", str(PTP_KERNEL_QUEUE + 1)],
                                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=5)
            if rc.returncode == 0:
                print("PTP coexistence ({}): RSS restreint à la queue {} (noyau/ptp4l) ; "
                      "média via fdir libmtl (≥1)".format(tag, PTP_KERNEL_QUEUE), flush=True)
            else:
                print("RSS restrict ({}) échoué: {}".format(tag, (rc.stderr or b'').decode()[:150]),
                      flush=True)
        except Exception as e:
            print("RSS restrict ({}) échoué: {}".format(tag, e), flush=True)
    _apply("launch")
    threading.Timer(5.0, _apply, args=("post-init",)).start()   # après le mtl_init du daemon


def _kill_mtl():
    """Arrêt GRACIEUX de l'unique mtl_rx : SIGTERM + longue attente pour laisser mtl_uninit
    détacher proprement le XDP et se désinscrire de MtlManager. SIGKILL en TOUT dernier recours.
    PUIS purge inconditionnelle du XDP résiduel (_xdp_off) : c'est ce qui débloque la relance après
    un changement d'abonnement (sinon crash-loop permanent `native xdp dev init fail -5`)."""
    global _mtl_proc
    if _mtl_proc:
        try:
            _mtl_proc.terminate()
            try: _mtl_proc.wait(timeout=10)
            except Exception:
                print("mtl_rx ne s'arrête pas — SIGKILL (XDP peut fuir)", flush=True)
                _mtl_proc.kill()
                try: _mtl_proc.wait(timeout=3)
                except Exception: pass
        except Exception:
            pass
        _mtl_proc = None
    _xdp_off()   # interface propre avant toute (re)lance — y compris le 1er lancement


def _video_target(idx):
    """Une cible = un slot shm de sortie (avec son IDENT propre)."""
    return {"idx": idx,
            "shm": "/dev/shm/{}_{}".format(HOSTNAME, idx),
            "stats": "/tmp/mtl_v{}.json".format(idx),
            "ident_file": _ident_file(idx)}


def _video_session(info, idxs):
    """Une session = un flux réseau décodé UNE fois (un flow RX), fan-out vers tous les slots
    `idxs` qui demandent cette même source (mcast:port). Évite le conflit AF_XDP « même 5-tuple,
    2 files RX » : un seul flow, recopie interne par mtl_rx vers chaque cible."""
    # Ordre de champ : pas porté par le SDP 2110-20 → défaut par résolution (1080i=TFF, 576i=BFF),
    # même règle que le helper orchestrateur. mtl_rx s'en sert pour la parité du merge RX.
    fo = "bff" if 0 < int(info.get("height") or 0) <= 576 else "tff"
    return {"kind": "video",
            "mcast": info["mcast"], "udp_port": info["port"], "payload_type": info["pt"],
            "width": info["width"], "height": info["height"], "fps": info["fps"],
            "interlaced": bool(info.get("interlaced")), "field_order": fo, "bit_depth": BIT_DEPTH,
            "ring": V_RING, "hdr": HDR,
            "targets": [_video_target(i) for i in idxs]}


def _tx_session(idx, t):
    """Une session TX = lit le shm d'entrée câblé (t['shm_in'], au format du producteur) et émet en
    2110-20 vers t['mcast']:t['udp_port']. Le format (w/h/bd/ring) est celui du shm consommé.
    Le câblage fournit le NOM du shm (ex. 'mtl_0') → on préfixe /dev/shm/ (le RX écrit en chemin
    complet ; sans ça le feeder n'ouvre rien → 0 frame)."""
    shm = t["shm_in"] or ""
    if shm and not shm.startswith("/"):
        shm = "/dev/shm/" + shm
    # ST 2110-20 : en entrelacé, la cadence libmtl/SDP est la cadence TRAME. Un t['fps'] hérité en
    # cadence CHAMP (field rate, > 30 en entrelacé — via câblage ou SDP source non conforme)
    # configurerait libmtl en 50i et casserait la session. On ramène à la cadence trame.
    _fps = float(t["fps"] or 25)
    if t.get("scan") == "i" and _fps > 30:
        _fps /= 2.0
    return {"kind": "video", "role": "tx",
            "mcast": t["mcast"], "udp_port": t["udp_port"], "payload_type": t["pt"],
            "width": t["w"], "height": t["h"], "fps": _fps,
            # Passthrough du balayage : on ré-émet en entrelacé si la source câblée l'est.
            "interlaced": (t.get("scan") == "i"), "field_order": t.get("field_order") or "tff",
            "bit_depth": t["bd"], "ring": t["ring"], "hdr": HDR,
            # ident_file TOUJOURS présent (sig stable → toggle IDENT sans recréer la session) ;
            # le fichier n'existe que quand l'IDENT est actif (mtl_rx libère le patch sinon).
            "targets": [{"idx": idx, "shm": shm, "stats": "/tmp/mtl_tx{}.json".format(idx),
                         "ident_file": _tx_ident_file(idx)}]}


_FR = {25.0: "25", 50.0: "50", 24.0: "24", 30.0: "30", 60.0: "60", 100.0: "100", 120.0: "120",
       23.98: "24000/1001", 29.97: "30000/1001", 59.94: "60000/1001"}

def _fps_str(fps):
    f = round(float(fps or 25), 2)
    return _FR.get(f) or (str(int(f)) if float(f).is_integer() else "{}/1001".format(int(round(f + 1))))

def _tx_sdp(i, t):
    """SDP ST 2110-20 d'un slot TX. Si mcast2/udp_port2 présents (SMPTE 2022-7),
    génère un unique SDP avec deux sections m=video (leg0 + leg1)."""
    sip   = SIP or "0.0.0.0"
    interlaced = t.get("scan") == "i"
    scan  = "interlace; " if interlaced else ""
    pt    = int(t.get("pt") or 96)
    w, h  = int(t.get("w") or 1920), int(t.get("h") or 1080)
    # ST 2110-20 : en entrelacé, exactframerate = cadence TRAME. Ceinture-bretelles si un slot
    # legacy/non normalisé porte encore la cadence CHAMP (field rate, > 30 en entrelacé).
    _fps = float(t.get("fps") or 25)
    if interlaced and _fps > 30:
        _fps /= 2.0
    fr    = _fps_str(_fps)
    fmtp  = ("sampling=YCbCr-4:2:2; width={w}; height={h}; exactframerate={fr}; depth=10; "
             "{scan}colorimetry=BT709; PM=2110GPM; SSN=ST2110-20:2017; TP=2110TPN;").format(
             w=w, h=h, fr=fr, scan=scan)
    dual = bool(t.get("mcast2") and t.get("udp_port2"))
    leg0 = (
        "m=video {port} RTP/AVP {pt}\r\n"
        "c=IN IP4 {mcast}/64\r\n"
        "{mid}"
        "a=source-filter: incl IN IP4 {mcast} {sip}\r\n"
        "a=rtpmap:{pt} raw/90000\r\n"
        "a=fmtp:{pt} {fmtp}\r\n"
        "{refclk}"
        "a=mediaclk:direct=0\r\n"
    ).format(port=int(t.get("udp_port") or 0), pt=pt, mcast=t.get("mcast") or "0.0.0.0",
             sip=sip, fmtp=fmtp, refclk=_LOCALMAC_REFCLK,
             mid="a=mid:DUP-1\r\n" if dual else "")
    grp = "a=group:DUP DUP-1 DUP-2\r\n" if dual else ""
    sdp = "v=0\r\no=- 0 0 IN IP4 {sip}\r\ns={hn} TX{i}\r\nt=0 0\r\n{grp}".format(
          sip=sip, hn=HOSTNAME, i=i, grp=grp) + leg0
    if dual:
        leg1 = (
            "m=video {port} RTP/AVP {pt}\r\n"
            "c=IN IP4 {mcast}/64\r\n"
            "a=mid:DUP-2\r\n"
            "a=source-filter: incl IN IP4 {mcast} {sip}\r\n"
            "a=rtpmap:{pt} raw/90000\r\n"
            "a=fmtp:{pt} {fmtp}\r\n"
            "{refclk}"
            "a=mediaclk:direct=0\r\n"
        ).format(port=int(t["udp_port2"]), pt=pt, mcast=t["mcast2"], sip=sip, fmtp=fmtp,
                 refclk=_LOCALMAC_REFCLK)
        sdp += leg1
    return sdp

def _anc_sdp(i, t):
    """SDP ST 2110-40 (ANC) d'un slot TX. Dual-section si anc_mcast2/anc_port2 présents (2022-7)."""
    sip  = SIP or "0.0.0.0"
    pt   = int(t.get("anc_pt") or 97)
    dual = bool(t.get("anc_mcast2") and t.get("anc_port2"))
    leg0 = (
        "m=video {port} RTP/AVP {pt}\r\n"
        "c=IN IP4 {mcast}/64\r\n"
        "{mid}"
        "a=source-filter: incl IN IP4 {mcast} {sip}\r\n"
        "a=rtpmap:{pt} smpte291/90000\r\n"
        "a=fmtp:{pt} TP=2110TPN; SSN=ST2110-40:2018;\r\n"
        "{refclk}"
        "a=mediaclk:direct=0\r\n"
    ).format(port=int(t.get("anc_port") or 0), pt=pt,
             mcast=t.get("anc_mcast") or "0.0.0.0", sip=sip, refclk=_LOCALMAC_REFCLK,
             mid="a=mid:DUP-1\r\n" if dual else "")
    grp = "a=group:DUP DUP-1 DUP-2\r\n" if dual else ""
    sdp = "v=0\r\no=- 0 0 IN IP4 {sip}\r\ns={hn} TX{i} ANC\r\nt=0 0\r\n{grp}".format(
          sip=sip, hn=HOSTNAME, i=i, grp=grp) + leg0
    if dual:
        leg1 = (
            "m=video {port} RTP/AVP {pt}\r\n"
            "c=IN IP4 {mcast}/64\r\n"
            "a=mid:DUP-2\r\n"
            "a=source-filter: incl IN IP4 {mcast} {sip}\r\n"
            "a=rtpmap:{pt} smpte291/90000\r\n"
            "a=fmtp:{pt} TP=2110TPN; SSN=ST2110-40:2018;\r\n"
            "{refclk}"
            "a=mediaclk:direct=0\r\n"
        ).format(port=int(t["anc_port2"]), pt=pt, mcast=t["anc_mcast2"], sip=sip,
                 refclk=_LOCALMAC_REFCLK)
        sdp += leg1
    return sdp

def _aud_sdp(i, ai, acfg):
    """SDP ST 2110-30 d'un flux audio TX (L24 / 48 kHz / 8 ch). Dual-section si mcast2/port2
    présents (SMPTE 2022-7 : group:DUP + a=mid:). ts-refclk:localmac (upgrade PTP côté orchestrateur)."""
    sip = SIP or "0.0.0.0"
    pt  = int(acfg.get("pt") or 97)
    ptime = A_PTIME_DEF if A_PTIME_DEF in (0.125, 0.25, 1.0, 4.0) else 1.0
    ptime_s = ("%g" % ptime)
    dual = bool(acfg.get("mcast2") and acfg.get("port2"))
    def _leg(mcast, port, mid):
        return (
            "m=audio {port} RTP/AVP {pt}\r\n"
            "c=IN IP4 {mcast}/64\r\n"
            "{mid}"
            "a=source-filter: incl IN IP4 {mcast} {sip}\r\n"
            "a=rtpmap:{pt} L24/48000/{ch}\r\n"
            "a=fmtp:{pt} channel-order=SMPTE2110.(U{ch:02d})\r\n"
            "a=ptime:{ptime}\r\n"
            "{refclk}"
            "a=mediaclk:direct=0\r\n"
        ).format(port=int(port or 0), pt=pt, mcast=mcast or "0.0.0.0", sip=sip,
                 ch=A_CHANNELS, ptime=ptime_s, refclk=_LOCALMAC_REFCLK, mid=mid)
    grp = "a=group:DUP DUP-1 DUP-2\r\n" if dual else ""
    sdp = "v=0\r\no=- 0 0 IN IP4 {sip}\r\ns={hn} TX{i} AUDIO{ai}\r\nt=0 0\r\n{grp}".format(
          sip=sip, hn=HOSTNAME, i=i, ai=ai, grp=grp)
    sdp += _leg(acfg.get("mcast"), acfg.get("port"), "a=mid:DUP-1\r\n" if dual else "")
    if dual:
        sdp += _leg(acfg.get("mcast2"), acfg.get("port2"), "a=mid:DUP-2\r\n")
    return sdp


# ─── Patterns numpy (simu) ───────────────────────────────────────────
_SMPTE_BARS_YUV = [
    (235, 128, 128), (210,  16, 146), (170, 166,  16), (145,  54,  34),
    (106, 202, 222), ( 81,  90, 240), ( 41, 240, 110),
]
_pattern_cache = {}   # (name, w, h) → (y, cb, cr) arrays

def _scale8(v):
    return (v << (BIT_DEPTH - 8)) if _DEEP else v

def _build_pattern(name, w, h):
    dt = np.dtype(_DT)
    uv_w, uv_h = w // _CW, h // _CH
    if name == "bars":
        n = len(_SMPTE_BARS_YUV)
        bw = w // n
        y_c, cb_c, cr_c = [], [], []
        for i, (y, cb, cr) in enumerate(_SMPTE_BARS_YUV):
            sw  = w   - bw * (n - 1) if i == n - 1 else bw
            swc = uv_w - (bw // _CW) * (n - 1) if i == n - 1 else bw // _CW
            y_c.append(np.full((h,    sw),  _scale8(y),  dtype=dt))
            cb_c.append(np.full((uv_h, swc), _scale8(cb), dtype=dt))
            cr_c.append(np.full((uv_h, swc), _scale8(cr), dtype=dt))
        return np.hstack(y_c), np.hstack(cb_c), np.hstack(cr_c)
    elif name == "gradient":
        y  = np.tile(np.linspace(_scale8(16), _scale8(235), w).astype(dt), (h, 1))
        uv = np.full((uv_h, uv_w), _NEUTRAL, dtype=dt)
        return y, uv, uv.copy()
    else:  # "black" + fallback
        y  = np.full((h,    w),    _BLACK,   dtype=dt)
        uv = np.full((uv_h, uv_w), _NEUTRAL, dtype=dt)
        return y, uv, uv.copy()

def _get_pattern(name, fi, lay):
    w, h = lay["w"], lay["h"]
    dt = np.dtype(_DT)
    if name == "moving":
        y   = np.full((h, w), _BLACK, dtype=dt)
        col = (fi * 8) % w
        y[:, col:min(col + 8, w)] = _WHITE
        uv  = np.full((lay["uv_h"], lay["uv_w"]), _NEUTRAL, dtype=dt)
        return y, uv, uv
    key = (name, w, h)
    if key not in _pattern_cache:
        _pattern_cache[key] = _build_pattern(name, w, h)
    return _pattern_cache[key]


# ─── Simulation : le rendu de mire écrit désormais des GRAINS MXL (cf. _simu_loop / _txgen_loop)
#     via bobimxl.Writer (open_grain → _fill_grain_planes → commit). Plus de ring shm maison. ──


def _read_stats(stats_path):
    try:
        with open(stats_path) as f:
            d = json.load(f)
        return float(d.get("fps", 0.0)), int(d.get("frame_index", 0))
    except Exception:
        return None

def _read_stats_raw(stats_path):
    """Lit le stats json complet (fps/frame_index + timecode/df pour l'ANC), ou None si absent."""
    try:
        with open(stats_path) as f:
            return json.load(f)
    except Exception:
        return None


_rx_queues_alloc = 0   # dernières files RX/TX demandées au daemon (exposé via :8080 xdp.allocated)
_tx_queues_alloc = 0


def _write_config(sessions):
    """Écrit le config lu par le DAEMON mtl_rx : device params + sessions désirées. Le daemon détecte
    le changement de mtime et RÉCONCILIE à chaud — aucune relance.

    `rx_queues`/`tx_queues` (= `mtl_init` rx/tx_queues_cnt, 1 file AF-XDP par session libmtl) sont
    dimensionnés au NOMBRE RÉEL de sessions de ce config. AVANT : forfait `ACTIVE_RX*3` (réserve
    1 vidéo+1 audio+1 ANC par slot) → sur-réservation qui plafonnait à ~16 sessions (48 files HW / 3)
    même en vidéo-seule. Maintenant : exact → une RX vidéo-seule peut monter jusqu'aux ~48 files HW.
    `MTL_RX_QUEUE_HEADROOM`/`MTL_TX_QUEUE_HEADROOM` (env, défaut 0) pré-réservent des files pour
    ajouter audio/ANC à chaud SANS réinit mtl (compromis capacité ↔ souplesse dynamique)."""
    global _rx_queues_alloc, _tx_queues_alloc
    # RX = toute session non explicitement TX (la session vidéo RX n'a pas de clé 'role' ;
    # audio/ANC RX portent role='rx' ; TX portent role='tx').
    n_rx = sum(1 for s in sessions if s.get("role") != "tx")
    n_tx = sum(1 for s in sessions if s.get("role") == "tx")
    hr_rx = max(0, int(os.environ.get("MTL_RX_QUEUE_HEADROOM") or 0))
    hr_tx = max(0, int(os.environ.get("MTL_TX_QUEUE_HEADROOM") or 0))
    _rx_queues_alloc = max(1, n_rx + hr_rx)
    _tx_queues_alloc = max(1, n_tx + hr_tx)
    with open(_CONFIG_PATH, "w") as f:
        json.dump({"pmd": "af_xdp", "iface": IFACE, "lcores": LCORES, "sip": SIP,
                   "quota_mbs": QUOTA_MBS,
                   "rx_queues": _rx_queues_alloc, "tx_queues": _tx_queues_alloc,
                   "sessions": sessions}, f)


def _launch_mtl():
    """(Re)lance le daemon mtl_rx. Purge d'abord le XDP ET les règles ntuple résiduels : au 1er
    lancement (ou après un crash / `docker rm -f`) une instance précédente a pu laisser un programme
    XDP accroché (`native xdp dev init fail -5`) ET/OU des règles fdir sur le matériel (« socket add
    flow fail » → session muette). On repart d'une interface propre."""
    global _mtl_proc, _last_launch, _rx_queues_reserved, _tx_queues_reserved
    _xdp_off()
    _flush_ntuple()
    _mtl_proc = subprocess.Popen([MTL_RX, "--config", _CONFIG_PATH])
    _last_launch = time.time()
    # Fige la réservation effective = ce que le config porte À CET INSTANT (lu par mtl_init au boot du
    # daemon). _write_config a déjà posé _rx/_tx_queues_alloc juste avant dans la boucle du manager.
    _rx_queues_reserved = _rx_queues_alloc
    _tx_queues_reserved = _tx_queues_alloc
    print("mtl_rx daemon (re)lancé", flush=True)
    # APRÈS le flush ntuple : PTP vers la queue noyau via RESTRICTION RSS (ethtool -N est incompatible
    # avec le flow-steering AF-XDP de libmtl) — sinon les Announce PTP sont avalés par les queues XSK.
    _steer_ptp_to_kernel_queue()


def _tx_gen_apply(idx):
    """Recalcule shm_in/enabled d'un slot TX selon la priorité : câblage réel > gen > fallback > rien.
    Appelé après tout changement de cable_shm, user_enabled ou fallback_mode."""
    # Budget de queues AF_XDP partagé RX+TX : seuls les ACTIVE_TX_C premiers slots peuvent émettre.
    # Sans ce garde-fou, un repli (fallback != none) activerait TOUS les slots provisionnés (jusqu'à
    # TX_COUNT) → ils émettraient du noir en continu et satureraient les queues de la NIC (ENOMEM /
    # registre plein). Les slots au-delà du budget restent silencieux tant qu'ils ne sont pas activés.
    if idx >= ACTIVE_TX_C:
        with _tx_lock:
            _tx[idx]["shm_in"] = None
            _tx[idx]["enabled"] = False
        with _tx_gen_lock:
            _tx_gen[idx]["enabled"] = False
        return
    with _tx_lock:
        cable    = _tx[idx].get("cable_shm") or ""
        fallback = _tx[idx].get("fallback_mode") or "none"
    with _tx_gen_lock:
        user_gen = _tx_gen[idx]["user_enabled"]

    if cable:
        with _tx_lock:
            _tx[idx]["shm_in"] = cable
            _tx[idx]["enabled"] = True
        with _tx_gen_lock:
            _tx_gen[idx]["enabled"] = False
    elif user_gen:
        shm_name = "/dev/shm/{}_txgen_{}".format(HOSTNAME, idx)
        with _tx_lock:
            _tx[idx]["shm_in"] = shm_name
            _tx[idx]["enabled"] = True
        with _tx_gen_lock:
            _tx_gen[idx]["enabled"] = True
    elif fallback != "none":
        shm_name = "/dev/shm/{}_txgen_{}".format(HOSTNAME, idx)
        with _tx_lock:
            _tx[idx]["shm_in"] = shm_name
            _tx[idx]["enabled"] = True
        with _tx_gen_lock:
            _tx_gen[idx]["enabled"] = True
            _tx_gen[idx]["pattern"] = fallback   # le repli impose sa mire
    else:
        with _tx_lock:
            _tx[idx]["shm_in"] = None
            _tx[idx]["enabled"] = False
        with _tx_gen_lock:
            _tx_gen[idx]["enabled"] = False


def _overlay_patch(mm, off, patch_data, lay):
    """Overlay générique d'un patch ident (plan Y+chroma neutre) haut-droit d'une frame."""
    if not patch_data:
        return
    patch, bw, bh = patch_data
    w, h = lay["w"], lay["h"]
    if bw > w or bh > h:
        return
    x0 = w - bw - 8; y0 = 8
    x0 -= x0 % 2; y0 -= y0 % 2
    if x0 < 0: x0 = 0
    pp = (patch.astype(np.uint16) << (BIT_DEPTH - 8)) if _DEEP else patch
    yy = np.frombuffer(mm, dtype=np.dtype(_DT), count=lay["y"] // _BPS, offset=off).reshape(h, w)
    yy[y0:y0 + bh, x0:x0 + bw] = pp
    ux0, uy0, ubw, ubh = x0 // _CW, y0 // _CH, bw // _CW, bh // _CH
    for poff in (off + lay["y"], off + lay["y"] + lay["uv"]):
        c = np.frombuffer(mm, dtype=np.dtype(_DT), count=lay["uv"] // _BPS, offset=poff).reshape(lay["uv_h"], lay["uv_w"])
        c[uy0:uy0 + ubh, ux0:ux0 + ubw] = _NEUTRAL


def _render_patch_lines(lines, size):
    """Rend un patch Y8 (plan luma) à partir de 3 lignes de texte + une taille de police.
    Mutualisé par l'IDENT TX (overlay mire Python + fichier lu par mtl_rx en C)."""
    font = _font(size)
    pad = max(3, size // 4); gap = max(1, size // 6)
    probe = ImageDraw.Draw(Image.new("L", (1, 1)))
    bboxes = [probe.textbbox((0, 0), ln, font=font) for ln in lines]
    bw = max(b[2] - b[0] for b in bboxes) + 2 * pad
    bh = sum(b[3] - b[1] for b in bboxes) + gap * (len(lines) - 1) + 2 * pad
    bw += bw % 2; bh += bh % 2
    img = Image.new("L", (bw, bh), 16)
    d = ImageDraw.Draw(img); cy = pad
    for ln, b in zip(lines, bboxes):
        d.text((pad - b[0], cy - b[1]), ln, font=font, fill=235); cy += (b[3] - b[1]) + gap
    patch = np.frombuffer(img.tobytes(), dtype=np.uint8).reshape(bh, bw)
    return patch, bw, bh


def _tx_ident_lines(idx):
    """3 lignes IDENT d'une sortie TX : nom · destination · format."""
    with _tx_lock:
        mcast_s = _tx[idx].get("mcast") or "?"
        port_s  = str(_tx[idx].get("udp_port") or 0)
        w, h, fps = _tx[idx]["w"], _tx[idx]["h"], _tx[idx]["fps"]
        scan = "i" if _tx[idx].get("scan") == "i" else "p"
    return ["{} · TX{}".format(HOSTNAME, idx),
            "{}:{}".format(mcast_s, port_s),
            "{}x{} {} {:.0f}{}".format(w, h, CHROMA, float(fps or FPS), scan)]


def _txgen_ident_patch(idx):
    """Patch ident pour le générateur TX : nom + mode (GEN/REPLI) + destination."""
    if not _HAS_PIL:
        return None
    with _tx_lock:
        mcast_s  = _tx[idx].get("mcast") or "?"
        port_s   = str(_tx[idx].get("udp_port") or 0)
    with _tx_gen_lock:
        pat_name  = _tx_gen[idx]["pattern"]
        user_gen  = _tx_gen[idx]["user_enabled"]
    mode_label = "GEN" if user_gen else "REPLI"
    size = max(12, HEIGHT // 28)
    lines = ["{} TX{}".format(HOSTNAME, idx), "{} · {}".format(mode_label, pat_name), "{}:{}".format(mcast_s, port_s)]
    return _render_patch_lines(lines, size)


def _tx_ident_file(idx):
    return "/dev/shm/{}_tx{}_ident".format(HOSTNAME, idx)


def _render_tx_ident(idx):
    """Patch IDENT user d'une sortie TX (plan Y8) ou None si IDENT off / PIL absent."""
    with _tx_gen_lock:
        on   = _tx_gen[idx]["ident"]
        usz  = int(_tx_gen[idx]["ident_size"] or 0)
    if not (_HAS_PIL and on):
        return None
    with _tx_lock:
        h_ref = int(_tx[idx]["h"] or HEIGHT)
    size = usz or max(12, h_ref // 28)
    size = max(10, min(size, h_ref // 4))
    return _render_patch_lines(_tx_ident_lines(idx), size)


def _update_tx_ident(idx):
    """Re-rend le patch IDENT TX et l'écrit dans le fichier binaire [u32 bw][u32 bh][Y8] lu par
    mtl_rx (overlay C sur la frame émise). Fichier supprimé si IDENT off → mtl_rx libère le patch."""
    p = _render_tx_ident(idx)
    fpath = _tx_ident_file(idx)
    try:
        if p is None:
            if os.path.exists(fpath):
                os.remove(fpath)
        else:
            patch, bw, bh = p
            tmp = fpath + ".tmp"
            with open(tmp, "wb") as f:
                f.write(struct.pack("II", bw, bh)); f.write(patch.tobytes())
            os.replace(tmp, fpath)   # publication atomique (mtl_rx lit le mtime)
    except Exception as e:
        print("tx ident file err:", e, flush=True)


def _manager_loop():
    """Calcule l'ensemble RX voulu (SDP actifs, groupés par source pour le fan-out) et RÉÉCRIT le
    config. Le daemon mtl_rx — lancé UNE fois et MAINTENU en vie (mtl_init à vie) — réconcilie les
    sessions à chaud : plus de kill/relance, ptp4l ne faute qu'au 1er lancement. On ne relance QUE
    si le daemon meurt (crash), avec backoff + purge XDP."""
    global _mtl_proc, _cur_sig, _fail_streak, _xdp_sessions_active
    while True:
        groups = {}        # (mcast, port) → {"info": sdp, "idxs": [..]} — fan-out même-source
        for idx in range(N_VIDEO):
            sdp_path = "{}/nmos_recv_v_{}.sdp".format(SDP_DIR, idx)
            sdp = _parse_sdp(sdp_path) if os.path.exists(sdp_path) else None
            cur = [sdp["width"], sdp["height"]] if sdp else [WIDTH, HEIGHT]
            if _slot_res[idx] != cur:
                _slot_res[idx] = cur; _update_ident(idx)   # la taille IDENT suit la résolution
            _ctl[idx]["info"] = sdp
            if sdp and not _ctl[idx]["gen"]:                # GÉN forcé → reste en simu
                key = (sdp["mcast"], sdp["port"])
                groups.setdefault(key, {"info": sdp, "idxs": []})["idxs"].append(idx)
        sessions = [_video_session(g["info"], g["idxs"]) for g in groups.values()]
        active = set(t["idx"] for s in sessions if s["kind"] == "video" for t in s["targets"])
        for idx in range(N_VIDEO):
            _live[idx] = idx in active

        # Sessions RX audio (2110-30) : SDP audio actif → écrit /dev/shm/{hn}_audio_{idx} en L24 BE.
        for idx in range(N_AUDIO):
            apath = "{}/nmos_recv_a_{}.sdp".format(SDP_DIR, idx)
            ainfo = _parse_sdp_audio(apath) if os.path.exists(apath) else None
            if ainfo:
                sessions.append(_audio_session(idx, ainfo))

        # Sessions RX ANC (2110-40) : SDP smpte291 actif → écrit /dev/shm/{hn}_anc_{idx} + timecode.
        for idx in range(N_ANC):
            dpath = "{}/nmos_recv_anc_{}.sdp".format(SDP_DIR, idx)
            dinfo = _parse_sdp_anc(dpath) if os.path.exists(dpath) else None
            if dinfo:
                sessions.append(_anc_session(idx, dinfo))

        # Sessions TX : un slot émet s'il est activé, a une destination et un shm d'entrée câblé.
        # Plafonné à ACTIVE_TX_C (budget de queues partagé RX+TX) — les slots provisionnés au-delà
        # ne créent aucune session (cf. _tx_gen_apply qui les force déjà à enabled=False).
        with _tx_lock:
            for i in range(min(N_TX, ACTIVE_TX_C)):
                t = _tx[i]
                if t["enabled"] and t["mcast"] and t["udp_port"] and t["shm_in"]:
                    sessions.append(_tx_session(i, t))
                # TX audio : priorité TONALITÉ (gen autonome) > mire/repli (GEN vidéo) > câblé.
                # NON câblé et sans tonalité ⇒ pas de session (silence).
                _acable = t.get("audio_cable_shm") or []
                for ai, acfg in enumerate(t.get("audios") or []):
                    if not acfg.get("mcast") or not acfg.get("port"):
                        continue
                    _tone_on = bool(_tx_tone[i][ai]["enabled"]) if ai < len(_tx_tone[i]) else False
                    if _tone_on or (not t.get("cable_shm") and _tx_gen[i]["enabled"]):
                        ashm = "/dev/shm/{}_audio_txgen_{}_{}".format(HOSTNAME, i, ai)
                    else:
                        ashm = _acable[ai] if ai < len(_acable) else None
                    if t["enabled"] and ashm:
                        if not ashm.startswith("/"):
                            ashm = "/dev/shm/" + ashm
                        sessions.append(_audio_tx_session(i * 2 + ai, acfg, ashm))
                # TX ANC : câblage INDÉPENDANT (anc_cable_shm). NON câblé ⇒ pas de session.
                dshm = t.get("anc_cable_shm")
                if t["enabled"] and t.get("anc_mcast") and t.get("anc_port") and dshm:
                    if not dshm.startswith("/"):
                        dshm = "/dev/shm/" + dshm
                    sessions.append(_anc_tx_session(i, t, dshm))

        # 1) config : réécrit dès qu'il change → le daemon réconcilie à chaud (aucune relance/faute PTP)
        sig = json.dumps(sessions, sort_keys=True)
        if sig != _cur_sig:
            with _mtl_lock:
                _write_config(sessions)
            _cur_sig = sig
            _xdp_sessions_active = len(sessions)
            print("mtl_rx config: {} session(s)".format(len(sessions)), flush=True)

        # 2) cycle de vie : lancé 1× au 1er besoin, maintenu en vie ; relancé seulement s'il a crashé
        dead = (_mtl_proc is not None and _mtl_proc.poll() is not None)
        with _mtl_lock:
            if _mtl_proc is None and sessions:
                _launch_mtl()                               # 1er lancement (mtl_init → 1 seule faute PTP)
            elif dead and sessions:
                if (time.time() - _last_launch) < 6:
                    _fail_streak = min(_fail_streak + 1, 6)
                    wait = min(2 + 2 * _fail_streak, 15)
                    print("mtl_rx crash → backoff {}s (#{})".format(wait, _fail_streak), flush=True)
                    time.sleep(wait)
                else:
                    _fail_streak = 0
                _launch_mtl()                               # relance après crash (purge XDP incluse)
            elif dead:
                _mtl_proc = None                            # mort sans rien à servir → relance au besoin
        time.sleep(0.5)


def _simu_loop(idx):
    """Écrit la mire de simu dans le FLUX MXL du slot TANT QU'IL N'EST PAS servi par mtl_rx (_live).
    Quand le slot est live, mtl_rx possède le flux ; on ne fait que relayer ses stats sur :8080."""
    name = "{}_{}".format(HOSTNAME, idx)
    writer = None; sim_res = None; fi = 0
    def _close():
        nonlocal writer, sim_res
        if writer is not None:
            try: writer.close()
            except Exception: pass
        writer = None; sim_res = None
    while True:
        if _live[idx] or not _HAS_MXL:
            if writer is not None:
                _close()
            if not _HAS_MXL and not _live[idx]:
                time.sleep(0.5); continue
            d = _read_stats_raw("/tmp/mtl_v{}.json".format(idx))
            with metrics_lock:
                # Échec de création de session RX (budget lcores…) : mtl_rx écrit {error} dans le stats.
                # On le remonte (mode="error" + rx_error) au lieu de prétendre « mtl » à tort.
                if d and d.get("error"):
                    metrics[idx]["mode"] = "error"
                    metrics[idx]["rx_error"] = d.get("error")
                    metrics[idx]["fps"] = 0.0
                    metrics[idx]["rx_latency_ms"] = None
                else:
                    metrics[idx]["mode"] = "mtl"
                    metrics[idx].pop("rx_error", None)
                    if d:
                        metrics[idx]["fps"] = float(d.get("fps", 0.0))
                        metrics[idx]["frame_index"] = int(d.get("frame_index", 0))
                        # Latence de réception (segment A = capture média → écriture shm), en ms.
                        metrics[idx]["rx_latency_ms"] = d.get("rx_latency_ms")
            time.sleep(0.5)
            continue
        w, h = _slot_res[idx]
        lay = _layout(w, h)
        try:
            # même format que mtl_rx (résolution live) → pas de glitch au basculement RX↔simu.
            # Changement de résolution → recréer le writer (le flowDef change) ; GC l'ancien flux.
            if writer is None or sim_res != (w, h):
                _close()
                try: _mxl().garbage_collect()
                except Exception: pass
                writer = _mk_video_writer(name, w, h, FPS); sim_res = (w, h)
            with _ctl_lock:
                pat = _ctl[idx]["pattern"]
            y, cb, cr = _get_pattern(pat, fi, lay)
            _, gi, view = writer.open_grain()
            _fill_grain_planes(view, lay, y, cb, cr)
            _overlay_simu(view, 0, idx, lay)     # incrustation IDENT (coût nul si off)
            writer.commit(gi)
        except Exception as e:
            print("simu err idx={}: {}".format(idx, e), flush=True)
            _close(); time.sleep(0.5); continue
        fi += 1
        if fi % 25 == 0:
            with metrics_lock:
                metrics[idx]["mode"] = "simu"; metrics[idx]["fps"] = 25.0; metrics[idx]["frame_index"] = fi
                metrics[idx]["rx_latency_ms"] = None   # pas de latence réception en simu (généré localement)
        time.sleep(1.0 / 25)


def _txgen_loop(idx):
    """Écrit une mire SMPTE dans le FLUX MXL txgen d'un slot TX quand gen est actif → mtl_rx (TX)
    le lit comme n'importe quel flux d'entrée câblé. Overlay ident (nom + GEN + dest) via PIL."""
    name = "{}_txgen_{}".format(HOSTNAME, idx)
    writer = None; res = None; fi = 0; patch = None; patch_age = 0
    next_t = None   # échéance absolue (monotone) du prochain GRAIN → pacing exact (compense le calcul)
    def _close():
        nonlocal writer, res
        if writer is not None:
            try: writer.close()
            except Exception: pass
        writer = None; res = None
    def _pace(period):
        # dort jusqu'à l'échéance (next_t += period), compense le temps de génération ; resync si
        # on accumule >½ période de retard (évite de courir derrière indéfiniment).
        nonlocal next_t
        next_t = (time.monotonic() if next_t is None else next_t) + period
        dt = next_t - time.monotonic()
        if dt > 0:
            time.sleep(dt)
        elif dt < -period:
            next_t = time.monotonic()
    while True:
        with _tx_gen_lock:
            gen_on = _tx_gen[idx]["enabled"]
        if not gen_on or not _HAS_MXL:
            if writer is not None: _close()
            time.sleep(0.1)
            continue
        with _tx_lock:
            w, h, fps = _tx[idx]["w"], _tx[idx]["h"], _tx[idx]["fps"]
            il = (_tx[idx].get("scan") == "i")
            fo = _tx[idx].get("field_order") or "tff"
        with _tx_gen_lock:
            pat = _tx_gen[idx]["pattern"]
        fps = max(1.0, float(fps or FPS))
        if il and fps > 30:          # cadence stockée en CHAMP (50) → cadence TRAME pour le flowDef
            fps = fps / 2.0
        lay = _layout(w, h)
        try:
            if writer is None or res != (w, h, il, fo):
                _close()
                try: _mxl().garbage_collect()
                except Exception: pass
                il_mode = (("interlaced_bff" if fo == "bff" else "interlaced_tff") if il else "progressive")
                writer = _mk_video_writer(name, w, h, fps, interlace=il_mode); res = (w, h, il, fo)
                patch = None; patch_age = 0   # forcer recalcul ident après resize
            if il:
                # ENTRELACÉ : 2 grains-CHAMPS par trame, aux index CHAMP (fi×2 + champ). Mire de test
                # de champ (field-aware) ; tout autre motif est éclaté en champs (lignes paires/impaires).
                fh = h // 2
                layf = _layout(w, fh)
                full = None if pat == "field_test" else _get_pattern(pat, fi, lay)
                for fld in (0, 1):
                    if pat == "field_test":
                        yy, cbb, crr = _field_test(fi, fld, w, fh)
                    else:
                        yy, cbb, crr = full[0][fld::2], full[1][fld::2], full[2][fld::2]
                    _, gi, view = writer.open_grain(index=fi * 2 + fld)
                    _fill_grain_planes(view, layf, yy, cbb, crr)
                    writer.commit(gi)
                    _pace(0.5 / fps)   # 1 CHAMP = ½ période trame → cadence champ EXACTE 50/s (anti-late)
            else:
                y_arr, cb_arr, cr_arr = _get_pattern(pat, fi, lay)
                # IDENT user actif → mtl_rx incrustera l'IDENT sur la mire au passage du feeder TX ;
                # on n'ajoute PAS le libellé auto de la mire (évite le doublon à l'écran).
                with _tx_gen_lock:
                    user_ident = _tx_gen[idx]["ident"]
                if user_ident:
                    patch = None
                elif patch_age <= 0 or patch is None:
                    patch = _txgen_ident_patch(idx); patch_age = int(fps)  # recalcul 1× par seconde
                else:
                    patch_age -= 1
                _, gi, view = writer.open_grain()
                _fill_grain_planes(view, lay, y_arr, cb_arr, cr_arr)
                _overlay_patch(view, 0, patch, lay)
                writer.commit(gi)
        except Exception as e:
            print("txgen err idx={}: {}".format(idx, e), flush=True)
            _close(); next_t = None; time.sleep(0.2); continue
        fi += 1
        if not il:
            _pace(1.0 / fps)   # progressif : 1 grain = 1 trame (l'entrelacé a déjà pacé ses 2 champs)


def _build_tone_second(freq, level_db, chan_on):
    """Buffer s24be 8ch INTERLEAVED d'1 seconde (48000 éch.) : sinusoïde freq/level_db sur les
    canaux où chan_on[ch], silence ailleurs. 1 s = nb entier de périodes pour toute fréquence
    entière → boucle sans discontinuité. Renvoie des bytes (SR·8·3 = 1 152 000 octets)."""
    SR = 48000
    amp = 10.0 ** (float(level_db) / 20.0)        # niveau normalisé [-1,1] (float32 MXL)
    t = np.arange(SR, dtype=np.float64)
    wave = (amp * np.sin(2.0 * np.pi * float(freq) * t / SR)).astype(np.float32)
    buf = np.zeros((SR, A_CHANNELS), dtype=np.float32)
    for ch in range(A_CHANNELS):
        if ch < len(chan_on) and chan_on[ch]:
            buf[:, ch] = wave
    return buf                                     # (48000, 8) float32, 1 s entière (boucle propre)


def _txgen_audio_loop(idx, ai):
    """Audio généré d'une sortie TX → FLUX MXL audio (float32 8ch, 1 ms/bloc). Source par priorité :
    (1) TONALITÉ configurée (_tx_tone[idx][ai]) si activée — choix des canaux + ruptage ;
    (2) sinon audio de mire (1 kHz tous canaux) quand le GEN vidéo+mire est actif ;
    (3) sinon silence. Ruptage = 0,9 s ON / 0,1 s OFF (mod sur la seconde), comme aux entrées."""
    SR = 48000
    N = SR // 1000                                # 48 éch. = 1 ms
    name = "{}_audio_txgen_{}_{}".format(HOSTNAME, idx, ai)
    silence = np.zeros((N, A_CHANNELS), dtype=np.float32)
    writer = None; fi = 0
    sig = None; buf_on = None; buf_off = None        # buffers 1 s (ON-phase / OFF-phase ruptage)
    def _close():
        nonlocal writer
        if writer is not None:
            try: writer.close()
            except Exception: pass
        writer = None
    while True:
        with _tx_gen_lock:
            tone = dict(_tx_tone[idx][ai])
            gen_on  = _tx_gen[idx]["enabled"]
            pattern = _tx_gen[idx].get("pattern") or "black"
        tone_on = bool(tone.get("enabled"))
        want_mire = gen_on and pattern == "bars"
        if (not tone_on and not gen_on) or not _HAS_MXL:   # rien à émettre → veille (flux fermé)
            if writer is not None: _close()
            sig = None
            time.sleep(0.1)
            continue
        try:
            if writer is None:
                writer = bobimxl.AudioWriter(_mxl(), name, channels=A_CHANNELS,
                                             sample_rate=SR, index_mode="tai")
            if tone_on:
                active = (tone.get("active") or [])
                rupted = (tone.get("rupted") or [])
                freq = int(tone.get("freq") or 1000)
                level = float(tone.get("level_db") if tone.get("level_db") is not None else -18.0)
                on_mask  = [bool(active[c]) if c < len(active) else False for c in range(A_CHANNELS)]
                off_mask = [on_mask[c] and not (c < len(rupted) and bool(rupted[c])) for c in range(A_CHANNELS)]
                nsig = ("tone", freq, level, tuple(on_mask), tuple(off_mask))
                if nsig != sig:
                    buf_on  = _build_tone_second(freq, level, on_mask)
                    buf_off = _build_tone_second(freq, level, off_mask)
                    sig = nsig
                c = fi % 1000                            # position dans la seconde (ruptage)
                src = buf_on if c < 900 else buf_off
                chunk = src[c * N:(c + 1) * N]
            elif want_mire:                              # legacy : 1 kHz tous canaux, sans ruptage
                if sig != ("mire",):
                    buf_on = _build_tone_second(1000, -18.0, [True] * A_CHANNELS)
                    buf_off = buf_on; sig = ("mire",)
                c = fi % 1000
                chunk = buf_on[c * N:(c + 1) * N]
            else:                                        # GEN vidéo sans mire → silence
                chunk = silence; sig = None
            writer.write(chunk)                          # (48,8) float32 → index TAI interne
        except Exception as e:
            print("txgen audio err idx={} ai={}: {}".format(idx, ai, e), flush=True)
            _close(); sig = None; time.sleep(0.2); continue
        fi += 1
        time.sleep(0.001)   # 1ms


def _cleanup(*a):
    # Arrêt du conteneur : teardown gracieux du daemon (SIGTERM → free sessions + mtl_stop/uninit)
    # puis purge XDP (_kill_mtl). C'est le SEUL endroit qui arrête mtl_rx (plus de kill par changement).
    with _mtl_lock:
        _kill_mtl()
    raise SystemExit(0)


signal.signal(signal.SIGTERM, _cleanup)
signal.signal(signal.SIGINT, _cleanup)

# Un thread de simu par slot vidéo (écrit le shm quand le slot n'est pas servi par mtl_rx) +
# UN manager central qui pilote l'unique mtl_rx multi-session.
for _i in range(N_VIDEO):
    threading.Thread(target=_simu_loop, args=(_i,), daemon=True).start()
threading.Thread(target=_manager_loop, daemon=True).start()
# Générateur TX : un thread vidéo + 2 threads audio par slot TX (restent en veille si gen off).
for _i in range(N_TX):
    threading.Thread(target=_txgen_loop, args=(_i,), daemon=True).start()
    for _ai in range(2):
        threading.Thread(target=_txgen_audio_loop, args=(_i, _ai), daemon=True).start()

print("2110_io (docker/af_xdp) multi-session : {}v iface={} lcores={} ring={}".format(
    N_VIDEO, IFACE, LCORES, V_RING), flush=True)

while True:
    time.sleep(3600)

#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.
#
# Contrôleur receiver_2110_mtl — variante DOCKER / AF_XDP (PF, pas de VF).
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

HOSTNAME   = os.environ.get("HOSTNAME_RX") or os.environ.get("HOSTNAME") or "mtlrx"
N_VIDEO    = int(os.environ.get("VIDEO_COUNT") or 1)
IFACE      = os.environ.get("IFACE") or "ens1f0np0"
LCORES     = os.environ.get("LCORES") or "1,2,3"
V_RING     = max(2, int(os.environ.get("RING") or 8))   # ring du pipeline (réglage) ; mtl_rx borne ≤8
WIDTH      = int(os.environ.get("WIDTH") or 1280)     # défaut/simu (réel = lu du SDP)
HEIGHT     = int(os.environ.get("HEIGHT") or 720)
FPS        = float(os.environ.get("FPS") or 25)
CHROMA     = str(os.environ.get("CHROMA") or "422")
BIT_DEPTH  = int(os.environ.get("BIT_DEPTH") or 10)
MTL_RX     = os.environ.get("MTL_RX_BIN") or "/usr/local/bin/mtl_rx"
HDR        = 64
SDP_DIR    = "/tmp"

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


# Résolution courante par slot (pour la simu + la taille IDENT), suit le SDP live.
_slot_res = [[WIDTH, HEIGHT] for _ in range(N_VIDEO)]

metrics = [{"idx": i, "essence": "video", "fps": 0.0, "frame_index": 0, "mode": "init"}
           for i in range(N_VIDEO)]
metrics_lock = threading.Lock()

# ─── Plan de contrôle par slot (:8082 /gen, /ident) — identique receiver_2110 ──────
# gen        : force la mire locale (simu) sur ce slot, même si un SDP est actif.
# ident      : incrustation 3 lignes (nom · source · format) en haut à droite, taille réglable.
_ctl = [{"gen": False, "ident": False, "ident_size": 0, "info": None} for _ in range(N_VIDEO)]
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


# ─── :8080 métriques (format get_metrics) ────────────────────────────
class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        with metrics_lock:
            recs = [dict(m) for m in metrics]
        # fps agrégé = premier slot actif (compat get_metrics qui lit .fps top-level)
        top_fps = next((m["fps"] for m in recs if m.get("mode") == "mtl"), recs[0]["fps"] if recs else 0.0)
        payload = {"fps": top_fps, "receivers": recs}
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
        return self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path.rstrip("/") != "/nmos/subscribe":
            return self._json(404, {"error": "not found"})
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
        except Exception as e:
            return self._json(400, {"error": str(e)})
        essence = body.get("essence", "video")
        idx     = int(body.get("receiver_index") or 0)
        enabled = bool(body.get("enabled"))
        sdp     = body.get("sdp")
        if isinstance(sdp, list):          # SMPTE 2022-7 : on garde la leg 0 en v1
            sdp = sdp[0] if sdp else None
        # v1 : vidéo uniquement (le slot lit nmos_recv_v_{idx}.sdp)
        if essence != "video":
            return self._json(200, {"ok": True, "note": "audio ignoré en v1"})
        path = os.path.join(SDP_DIR, "nmos_recv_v_{}.sdp".format(idx))
        try:
            if enabled and sdp:
                with open(path, "w") as f:
                    f.write(sdp)
            else:
                if os.path.exists(path):
                    os.remove(path)
        except Exception as e:
            return self._json(500, {"error": str(e)})
        return self._json(200, {"ok": True})

    def log_message(self, *a): pass


# ─── :8082 contrôle à chaud : /gen (générateur simu) + /ident (incrustation) ─────
class ControlHandler(BaseHTTPRequestHandler):
    def _json(self, code, obj):
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(obj).encode())

    def do_POST(self):
        path = self.path.rstrip("/")
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
        except Exception as e:
            return self._json(400, {"error": str(e)})
        try:
            idx = int(body.get("idx", 0))
        except Exception:
            idx = -1
        if not (0 <= idx < N_VIDEO):
            return self._json(400, {"error": "idx hors limites"})
        if path == "/gen":          # bascule générateur simu (force la mire sur ce slot)
            with _ctl_lock:
                _ctl[idx]["gen"] = bool(body.get("enabled"))
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
        if fr:
            num = int(fr.group(1)); den = int(fr.group(2)) if fr.group(2) else 1
            info["fps"] = round(num / den, 2)
        if "interlace" in params:
            info["interlaced"] = True
    info.setdefault("width", WIDTH)
    info.setdefault("height", HEIGHT)
    info.setdefault("fps", FPS)
    return info


def _launch_mtl_rx(idx, info):
    shm = "/dev/shm/{}_{}".format(HOSTNAME, idx)
    stats = "/tmp/mtl_rx_{}.json".format(idx)
    try:
        os.remove(stats)
    except OSError:
        pass
    args = [MTL_RX, "--pmd", "af_xdp", "--iface", IFACE,
            "--mcast", info["mcast"], "--udp_port", str(info["port"]),
            "--payload_type", str(info["pt"]),
            "--width", str(info["width"]), "--height", str(info["height"]),
            "--fps", str(info["fps"]),
            "--shm", shm, "--ring", str(V_RING), "--hdr", str(HDR),
            "--bit_depth", str(BIT_DEPTH),   # conforme au pipeline MXL (force8 → 8)
            "--ident_file", _ident_file(idx),  # incrustation IDENT live (Partie B)
            "--lcores", LCORES, "--stats_file", stats]
    if info.get("interlaced"):
        args.append("--interlaced")
    print("mtl_rx launch idx={}: {}".format(idx, " ".join(args)), flush=True)
    return subprocess.Popen(args), stats


# ─── Simulation (mire numpy, mêmes en-têtes shm) — fallback sans SDP ou GÉN forcé ──
def _simu_frame(mm, fi, idx, lay):
    w, h = lay["w"], lay["h"]
    base = np.full((h, w), _BLACK, dtype=np.dtype(_DT))
    col = (fi * 8) % w
    base[:, col:min(col + 8, w)] = _WHITE
    neutral = np.full((lay["uv_h"], lay["uv_w"]), _NEUTRAL, dtype=np.dtype(_DT)).tobytes()
    off = HDR + (fi % V_RING) * lay["vf"]
    mm[off:off + lay["y"]] = base.tobytes()
    mm[off + lay["y"]:off + lay["y"] + lay["uv"]] = neutral
    mm[off + lay["y"] + lay["uv"]:off + lay["y"] + 2 * lay["uv"]] = neutral
    _overlay_simu(mm, off, idx, lay)     # incrustation IDENT (coût nul si off)
    mm[0:16] = struct.pack("QQ", fi, time.time_ns())


def _open_shm(path, size):
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o666)
    os.ftruncate(fd, size)
    mm = mmap.mmap(fd, size)
    os.close(fd)
    return mm


def _read_stats(stats_path):
    try:
        with open(stats_path) as f:
            d = json.load(f)
        return float(d.get("fps", 0.0)), int(d.get("frame_index", 0))
    except Exception:
        return None


_procs = {}  # idx → Popen (pour le teardown SIGTERM)


def video_slot(idx):
    """Bascule SIMU ↔ mtl_rx selon la présence d'un SDP NMOS. Un seul producteur du shm à la fois."""
    shm_path = "/dev/shm/{}_{}".format(HOSTNAME, idx)
    sdp_path = "{}/nmos_recv_v_{}.sdp".format(SDP_DIR, idx)
    proc = None
    stats = None
    cur_key = None
    sim_mm = None
    sim_res = None
    fi = 0
    prev_mode = None
    while True:
        gen = _ctl[idx]["gen"]   # GÉN forcé → mire locale même si un SDP est actif
        # On parse TOUJOURS le SDP pour connaître la RÉSOLUTION LIVE (même sous GÉN) → la simu
        # garde la même taille de shm que mtl_rx ⇒ pas de casse côté consommateurs au basculement.
        sdp = _parse_sdp(sdp_path) if os.path.exists(sdp_path) else None
        cur_w, cur_h = (sdp["width"], sdp["height"]) if sdp else (WIDTH, HEIGHT)
        if _slot_res[idx] != [cur_w, cur_h]:
            _slot_res[idx] = [cur_w, cur_h]
            _update_ident(idx)               # la taille IDENT suit la résolution
        info = None if gen else sdp
        if info:
            key = (info["mcast"], info["port"], info["pt"], info["width"],
                   info["height"], info["fps"], info["interlaced"])
            if key != cur_key or (proc and proc.poll() is not None):
                if sim_mm is not None:
                    sim_mm.close(); sim_mm = None; sim_res = None
                if proc:
                    proc.terminate()
                    try: proc.wait(timeout=3)
                    except Exception: proc.kill()
                proc, stats = _launch_mtl_rx(idx, info)
                _procs[idx] = proc
                cur_key = key
            _ctl[idx]["info"] = info
            if prev_mode != "mtl":
                _update_ident(idx); prev_mode = "mtl"
            st = _read_stats(stats) if stats else None
            with metrics_lock:
                metrics[idx]["mode"] = "mtl"
                if st:
                    metrics[idx]["fps"] = st[0]
                    metrics[idx]["frame_index"] = st[1]
            time.sleep(1.0)
        else:
            if proc:
                proc.terminate()
                try: proc.wait(timeout=3)
                except Exception: proc.kill()
                proc = None; cur_key = None; _procs.pop(idx, None)
            lay = _layout(cur_w, cur_h)
            # (Ré)ouvre le shm à la RÉSOLUTION COURANTE (même taille que mtl_rx → pas de resize
            # visible par les consommateurs ; un resize n'arrive que si la source change de res).
            if sim_mm is None or sim_res != (cur_w, cur_h):
                if sim_mm is not None:
                    sim_mm.close()
                sim_mm = _open_shm(shm_path, lay["total"]); sim_res = (cur_w, cur_h)
            _ctl[idx]["info"] = sdp if gen else None
            if prev_mode != "simu":
                _update_ident(idx); prev_mode = "simu"
            _simu_frame(sim_mm, fi, idx, lay)
            fi += 1
            if fi % 25 == 0:
                with metrics_lock:
                    metrics[idx]["mode"] = "simu"
                    metrics[idx]["fps"] = 25.0
                    metrics[idx]["frame_index"] = fi
            time.sleep(1.0 / 25)


def _cleanup(*a):
    # Teardown gracieux : terminer les mtl_rx enfants. (Le détachement XDP est garanti côté
    # orchestrateur par `ip link set <iface> xdp off` — MtlManager n'a pas le temps de le faire.)
    for p in list(_procs.values()):
        try: p.terminate()
        except Exception: pass
    raise SystemExit(0)


signal.signal(signal.SIGTERM, _cleanup)
signal.signal(signal.SIGINT, _cleanup)

for _i in range(N_VIDEO):
    threading.Thread(target=video_slot, args=(_i,), daemon=True).start()

print("receiver_2110_mtl (docker/af_xdp) : {}v iface={} lcores={} ring={} mtl_rx={}".format(
    N_VIDEO, IFACE, LCORES, V_RING, MTL_RX), flush=True)

while True:
    time.sleep(3600)

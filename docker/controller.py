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
N_VIDEO    = int(os.environ.get("RX_COUNT") or os.environ.get("VIDEO_COUNT") or 1)   # slots RX vidéo
N_TX       = int(os.environ.get("TX_COUNT") or 0)                                     # slots TX (senders)
N_AUDIO    = int(os.environ.get("AUDIO_COUNT") or 0)                                   # slots RX audio (st30)
N_ANC      = int(os.environ.get("ANC_COUNT") or 0)                                     # slots RX ANC (st40)
A_CHANNELS = 8
A_RING     = max(2, int(os.environ.get("AUDIO_RING") or 100))   # ring shm audio (chunks 1ms)
# Ptime audio (ST 2110-30) par DÉFAUT (ms) — repli quand le SDP n'a pas d'a=ptime. Réglable par
# installation (setting mtl_audio_ptime → env AUDIO_PTIME). Le SDP a=ptime PRIME (auto par entrée).
A_PTIME_DEF = float(os.environ.get("AUDIO_PTIME") or 1.0)
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
        # Senders TX actifs (câblés) : SDP exposé pour le transportfile NMOS (manifest_href).
        with _tx_lock:
            senders = []
            for i in range(N_TX):
                t = _tx[i]
                if not (t["enabled"] and t["shm_in"]):
                    continue
                lat = t.get("lat_ms")
                inputs_lat = {t["shm_in"]: lat} if lat is not None else {}
                if t["mcast"] and t["udp_port"]:
                    senders.append({"tx_idx": i, "essence": "video", "sdp": _tx_sdp(i, t),
                                    "inputs_latency_ms": inputs_lat})
                if t.get("anc_mcast") and t.get("anc_port"):
                    senders.append({"tx_idx": i, "essence": "anc", "sdp": _anc_sdp(i, t),
                                    "inputs_latency_ms": inputs_lat})
        payload = {"fps": top_fps, "receivers": recs, "senders": senders}
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
            if isinstance(sdp, list):            # SMPTE 2022-7 : on garde la leg 0 en v1
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
                if "mcast"    in body: t["mcast"]    = body.get("mcast") or None
                if "udp_port" in body: t["udp_port"] = int(body.get("udp_port") or 0)
                if "pt"       in body: t["pt"]       = int(body.get("pt") or 96)
                if "shm_in"   in body: t["shm_in"]   = (body.get("shm_in") or "").strip() or None
                if "audios" in body:
                    t["audios"] = [{"mcast": a.get("mcast") or None,
                                    "port": int(a.get("port") or 0),
                                    "pt": int(a.get("pt") or 97)}
                                   for a in (body.get("audios") or [])[:2]]
                if "anc_mcast"   in body: t["anc_mcast"]   = body.get("anc_mcast") or None
                if "anc_port"    in body: t["anc_port"]    = int(body.get("anc_port") or 0)
                if "anc_pt"      in body: t["anc_pt"]      = int(body.get("anc_pt") or 97)
                # scan/field_order : ne clobber QUE si fournis (le câblage :8082/input porte le
                # format réel de la source ; un push /tx de dest seule ne doit pas les écraser).
                if "scan"        in body: t["scan"]        = "i" if str(body.get("scan")).lower() == "i" else "p"
                if "field_order" in body: t["field_order"] = "bff" if str(body.get("field_order")).lower() == "bff" else "tff"
                for k_in, k_st in (("width","w"),("height","h"),("fps","fps"),("bit_depth","bd"),("ring","ring")):
                    if body.get(k_in):
                        t[k_st] = (float(body[k_in]) if k_st == "fps" else int(body[k_in]))
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
                st = {"tx{}_shm".format(i): (_tx[i]["shm_in"] or "") for i in range(N_TX)}
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
            if body.get("essence", "video") != "video":
                # L'audio TX suit automatiquement la vidéo : shm audio dérivé du shm vidéo câblé
                # (mtl_0 → mtl_audio_0). Pas de câblage audio séparé.
                return self._json(200, {"ok": True, "note": "audio TX suit la vidéo (shm dérivé)"})
            try: slot = int(body.get("slot", 0))
            except Exception: slot = -1
            if not (0 <= slot < N_TX):
                return self._json(400, {"error": "slot TX hors limites"})
            shm = (body.get("shm") or "").strip() or None
            fmt = body.get("format") or {}
            with _tx_lock:
                t = _tx[slot]
                t["shm_in"] = shm
                t["enabled"] = bool(shm)            # n'émet que si câblé
                if fmt.get("width"):     t["w"] = int(fmt["width"])
                if fmt.get("height"):    t["h"] = int(fmt["height"])
                if fmt.get("bit_depth"): t["bd"] = int(fmt["bit_depth"])
                if fmt.get("fps"):       t["fps"] = float(fmt["fps"])
                # Mode de balayage de la SOURCE → passthrough entrelacé (émis tel quel en 2110-20).
                if "scan" in fmt:        t["scan"] = "i" if str(fmt.get("scan")).lower() == "i" else "p"
                if fmt.get("field_order"): t["field_order"] = "bff" if str(fmt["field_order"]).lower() == "bff" else "tff"
            return self._json(200, {"ok": True})

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
_tx = [{"enabled": False, "mcast": None, "udp_port": 0, "pt": 96, "shm_in": None,
        "w": WIDTH, "h": HEIGHT, "fps": FPS, "bd": BIT_DEPTH, "ring": V_RING,
        "scan": "p", "field_order": "tff",   # passthrough entrelacé : suit le format de la source câblée
        "audios": [],       # liste de {mcast, port, pt} — jusqu'à 2 flux 2110-30
        "anc_mcast": None, "anc_port": 0, "anc_pt": 97,
        "lat_ms": None}     # âge du signal depuis la capture originale (ts_ns header SHM)
       for _ in range(N_TX)]
_tx_lock = threading.Lock()


def _tx_lat_monitor():
    """Thread passif : lit ts_ns (offset 8, uint64) dans le header SHM de chaque slot Tx actif
    et calcule l'âge du signal depuis sa capture originale. Ne lit que 16 octets par slot (très
    léger). Expose le résultat dans _tx[i]["lat_ms"] pour les métriques :8080."""
    while True:
        with _tx_lock:
            slots = [(i, t["shm_in"]) for i, t in enumerate(_tx) if t["enabled"] and t["shm_in"]]
        now_ns = time.time_ns()
        for idx, shm_name in slots:
            path = shm_name if shm_name.startswith("/") else "/dev/shm/" + shm_name
            try:
                with open(path, "rb") as f:
                    f.seek(8)
                    raw = f.read(8)
                if len(raw) == 8:
                    ts_ns = struct.unpack("<Q", raw)[0]
                    if ts_ns > 0:
                        lat_ms = round((now_ns - ts_ns) / 1_000_000, 1)
                        with _tx_lock:
                            _tx[idx]["lat_ms"] = lat_ms if 0 < lat_ms < 30_000 else None
            except Exception:
                with _tx_lock:
                    _tx[idx]["lat_ms"] = None
        time.sleep(0.1)   # 10 Hz — header uniquement, coût négligeable


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
    return {"kind": "video", "role": "tx",
            "mcast": t["mcast"], "udp_port": t["udp_port"], "payload_type": t["pt"],
            "width": t["w"], "height": t["h"], "fps": t["fps"],
            # Passthrough du balayage : on ré-émet en entrelacé si la source câblée l'est.
            "interlaced": (t.get("scan") == "i"), "field_order": t.get("field_order") or "tff",
            "bit_depth": t["bd"], "ring": t["ring"], "hdr": HDR,
            "targets": [{"idx": idx, "shm": shm, "stats": "/tmp/mtl_tx{}.json".format(idx)}]}


_FR = {25.0: "25", 50.0: "50", 24.0: "24", 30.0: "30", 60.0: "60", 100.0: "100", 120.0: "120",
       23.98: "24000/1001", 29.97: "30000/1001", 59.94: "60000/1001"}

def _fps_str(fps):
    f = round(float(fps or 25), 2)
    return _FR.get(f) or (str(int(f)) if float(f).is_integer() else "{}/1001".format(int(round(f + 1))))

def _tx_sdp(i, t):
    """SDP ST 2110-20 d'un slot TX. Transport = 422-10 (depth=10 même si le shm est en 8 bits).
    sip = IP du PF (source des paquets). L'orchestrateur ajoute ts-refclk (PTP) au fetch.
    Entrelacé : flag `interlace` dans le fmtp (passthrough) ; exactframerate = cadence reçue
    (fps de la source câblée), conservée telle quelle pour rester aligné sur le flux source."""
    scan = "interlace; " if t.get("scan") == "i" else ""
    return (
        "v=0\r\n"
        "o=- 0 0 IN IP4 {sip}\r\n"
        "s={hn} TX{i}\r\n"
        "t=0 0\r\n"
        "m=video {port} RTP/AVP {pt}\r\n"
        "c=IN IP4 {mcast}/64\r\n"
        "a=source-filter: incl IN IP4 {mcast} {sip}\r\n"
        "a=rtpmap:{pt} raw/90000\r\n"
        "a=fmtp:{pt} sampling=YCbCr-4:2:2; width={w}; height={h}; exactframerate={fr}; depth=10; "
        "{scan}colorimetry=BT709; PM=2110GPM; SSN=ST2110-20:2017; TP=2110TPN;\r\n"
        "a=mediaclk:direct=0\r\n"
    ).format(sip=SIP or "0.0.0.0", hn=HOSTNAME, i=i,
             port=int(t.get("udp_port") or 0), pt=int(t.get("pt") or 96),
             mcast=t.get("mcast") or "0.0.0.0", w=int(t.get("w") or 1920),
             h=int(t.get("h") or 1080), fr=_fps_str(t.get("fps")), scan=scan)

def _anc_sdp(i, t):
    """SDP ST 2110-40 (ANC / SMPTE 291) d'un slot TX. dest = anc_mcast/anc_port du slot."""
    return (
        "v=0\r\n"
        "o=- 0 0 IN IP4 {sip}\r\n"
        "s={hn} TX{i} ANC\r\n"
        "t=0 0\r\n"
        "m=video {port} RTP/AVP {pt}\r\n"
        "c=IN IP4 {mcast}/64\r\n"
        "a=source-filter: incl IN IP4 {mcast} {sip}\r\n"
        "a=rtpmap:{pt} smpte291/90000\r\n"
        "a=fmtp:{pt} TP=2110TPN; SSN=ST2110-40:2018;\r\n"
        "a=mediaclk:direct=0\r\n"
    ).format(sip=SIP or "0.0.0.0", hn=HOSTNAME, i=i,
             port=int(t.get("anc_port") or 0), pt=int(t.get("anc_pt") or 97),
             mcast=t.get("anc_mcast") or "0.0.0.0")


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
    # [index, write_ts, media_ts] — la simu (mire) n'a pas d'horloge média → media_ts=0 (off16),
    # sinon une valeur RX périmée resterait au basculement RX→simu (les consos retombent au nominal).
    mm[0:24] = struct.pack("QQQ", fi, time.time_ns(), 0)


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

def _read_stats_raw(stats_path):
    """Lit le stats json complet (fps/frame_index + timecode/df pour l'ANC), ou None si absent."""
    try:
        with open(stats_path) as f:
            return json.load(f)
    except Exception:
        return None


def _write_config(sessions):
    """Écrit le config lu par le DAEMON mtl_rx : device params (cap des files = N_VIDEO) + sessions
    désirées. Le daemon détecte le changement de mtime et RÉCONCILIE à chaud — aucune relance."""
    with open(_CONFIG_PATH, "w") as f:
        json.dump({"pmd": "af_xdp", "iface": IFACE, "lcores": LCORES, "sip": SIP,
                   "rx_queues": max(1, N_VIDEO + N_AUDIO + N_ANC), "tx_queues": max(1, N_TX * 3 + 1),
                   "sessions": sessions}, f)


def _launch_mtl():
    """(Re)lance le daemon mtl_rx. Purge d'abord le XDP ET les règles ntuple résiduels : au 1er
    lancement (ou après un crash / `docker rm -f`) une instance précédente a pu laisser un programme
    XDP accroché (`native xdp dev init fail -5`) ET/OU des règles fdir sur le matériel (« socket add
    flow fail » → session muette). On repart d'une interface propre."""
    global _mtl_proc, _last_launch
    _xdp_off()
    _flush_ntuple()
    _mtl_proc = subprocess.Popen([MTL_RX, "--config", _CONFIG_PATH])
    _last_launch = time.time()
    print("mtl_rx daemon (re)lancé", flush=True)


def _manager_loop():
    """Calcule l'ensemble RX voulu (SDP actifs, groupés par source pour le fan-out) et RÉÉCRIT le
    config. Le daemon mtl_rx — lancé UNE fois et MAINTENU en vie (mtl_init à vie) — réconcilie les
    sessions à chaud : plus de kill/relance, ptp4l ne faute qu'au 1er lancement. On ne relance QUE
    si le daemon meurt (crash), avec backoff + purge XDP."""
    global _mtl_proc, _cur_sig, _fail_streak
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
        with _tx_lock:
            for i in range(N_TX):
                t = _tx[i]
                if t["enabled"] and t["mcast"] and t["udp_port"] and t["shm_in"]:
                    sessions.append(_tx_session(i, t))
                # TX audio : jusqu'à 2 flux, shm audio dérivé de la vidéo câblée.
                for ai, acfg in enumerate(t.get("audios") or []):
                    if not acfg.get("mcast") or not acfg.get("port"):
                        continue
                    ashm = _derive_audio_shm(t["shm_in"], ai)
                    if t["enabled"] and ashm:
                        if not ashm.startswith("/"):
                            ashm = "/dev/shm/" + ashm
                        sessions.append(_audio_tx_session(i * 2 + ai, acfg, ashm))
                # TX ANC : suit la vidéo (shm ANC dérivé) si une dest ANC est réglée sur le slot.
                dshm = _derive_anc_shm(t["shm_in"])
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
    """Écrit la mire de simu dans le shm du slot TANT QU'IL N'EST PAS servi par mtl_rx (_live).
    Quand le slot est live, mtl_rx possède le shm ; on ne fait que relayer ses stats sur :8080."""
    shm_path = "/dev/shm/{}_{}".format(HOSTNAME, idx)
    sim_mm = None; sim_res = None; fi = 0
    while True:
        if _live[idx]:
            if sim_mm is not None:
                sim_mm.close(); sim_mm = None; sim_res = None
            st = _read_stats("/tmp/mtl_v{}.json".format(idx))
            with metrics_lock:
                metrics[idx]["mode"] = "mtl"
                if st:
                    metrics[idx]["fps"] = st[0]; metrics[idx]["frame_index"] = st[1]
            time.sleep(0.5)
            continue
        w, h = _slot_res[idx]
        lay = _layout(w, h)
        # même taille de shm que mtl_rx (résolution live) → pas de resize visible au basculement.
        if sim_mm is None or sim_res != (w, h):
            if sim_mm is not None: sim_mm.close()
            sim_mm = _open_shm(shm_path, lay["total"]); sim_res = (w, h)
        _simu_frame(sim_mm, fi, idx, lay)
        fi += 1
        if fi % 25 == 0:
            with metrics_lock:
                metrics[idx]["mode"] = "simu"; metrics[idx]["fps"] = 25.0; metrics[idx]["frame_index"] = fi
        time.sleep(1.0 / 25)


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

print("receiver_2110_mtl (docker/af_xdp) multi-session : {}v iface={} lcores={} ring={}".format(
    N_VIDEO, IFACE, LCORES, V_RING), flush=True)

while True:
    time.sleep(3600)

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
V_RING     = min(int(os.environ.get("RING") or 8), 8)
WIDTH      = int(os.environ.get("WIDTH") or 1280)     # défaut/simu (réel = lu du SDP)
HEIGHT     = int(os.environ.get("HEIGHT") or 720)
FPS        = float(os.environ.get("FPS") or 25)
CHROMA     = str(os.environ.get("CHROMA") or "422")
BIT_DEPTH  = int(os.environ.get("BIT_DEPTH") or 10)
MTL_RX     = os.environ.get("MTL_RX_BIN") or "/usr/local/bin/mtl_rx"
HDR        = 64
SDP_DIR    = "/tmp"

# ─── Layout shm (simu) — identique à receiver_2110 ──────────────────
_DEEP    = BIT_DEPTH >= 10
_BPS     = 2 if _DEEP else 1
_DT      = "<u2" if _DEEP else "u1"
_NEUTRAL = 1 << (BIT_DEPTH - 1)
_BLACK   = 16 << (BIT_DEPTH - 8) if _DEEP else 16
_WHITE   = 235 << (BIT_DEPTH - 8) if _DEEP else 235
_CW = {"420": 2, "422": 2, "444": 1}.get(CHROMA, 2)
_CH = {"420": 2, "422": 1, "444": 1}.get(CHROMA, 1)
UV_W = WIDTH // _CW
UV_H = HEIGHT // _CH
Y_SIZE       = WIDTH * HEIGHT * _BPS
UV_SIZE      = UV_W * UV_H * _BPS
V_FRAME_SIZE = Y_SIZE + 2 * UV_SIZE
V_TOTAL_SIZE = HDR + V_RING * V_FRAME_SIZE

metrics = [{"idx": i, "essence": "video", "fps": 0.0, "frame_index": 0, "mode": "init"}
           for i in range(N_VIDEO)]
metrics_lock = threading.Lock()


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


threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8080), MetricsHandler).serve_forever(),
                 daemon=True).start()
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8081), AgentHandler).serve_forever(),
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
            "--lcores", LCORES, "--stats_file", stats]
    if info.get("interlaced"):
        args.append("--interlaced")
    print("mtl_rx launch idx={}: {}".format(idx, " ".join(args)), flush=True)
    return subprocess.Popen(args), stats


# ─── Simulation (fallback sans SDP) — mire numpy, mêmes en-têtes shm ──
def _simu_frame(mm, fi):
    base = np.full((HEIGHT, WIDTH), _BLACK, dtype=np.dtype(_DT))
    col = (fi * 8) % WIDTH
    base[:, col:min(col + 8, WIDTH)] = _WHITE
    neutral = np.full((UV_H, UV_W), _NEUTRAL, dtype=np.dtype(_DT)).tobytes()
    off = HDR + (fi % V_RING) * V_FRAME_SIZE
    mm[off:off + Y_SIZE] = base.tobytes()
    mm[off + Y_SIZE:off + Y_SIZE + UV_SIZE] = neutral
    mm[off + Y_SIZE + UV_SIZE:off + Y_SIZE + 2 * UV_SIZE] = neutral
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
    sdp_path = "{}/nmos_recv_v_{}.sdp".format(SDP_DIR, idx)
    proc = None
    stats = None
    cur_key = None
    sim_mm = None
    fi = 0
    while True:
        info = _parse_sdp(sdp_path) if os.path.exists(sdp_path) else None
        if info:
            key = (info["mcast"], info["port"], info["pt"], info["width"],
                   info["height"], info["fps"], info["interlaced"])
            if key != cur_key or (proc and proc.poll() is not None):
                if sim_mm is not None:
                    sim_mm.close(); sim_mm = None
                if proc:
                    proc.terminate()
                    try: proc.wait(timeout=3)
                    except Exception: proc.kill()
                proc, stats = _launch_mtl_rx(idx, info)
                _procs[idx] = proc
                cur_key = key
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
            if sim_mm is None:
                sim_mm = _open_shm("/dev/shm/{}_{}".format(HOSTNAME, idx), V_TOTAL_SIZE)
            _simu_frame(sim_mm, fi)
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

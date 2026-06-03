# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.
#
# receiver_2110_mtl — wrapper Python déployé par l'agent (C3a).
# Rôle : à l'activation NMOS IS-05, le service NMOS pousse le SDP dans le container
# (/tmp/nmos_recv_v_{{idx}}.sdp). Le wrapper le détecte, le PARSE (mcast/port/PT/format) et
# lance le binaire C `mtl_rx` (libmtl/DPDK) qui reçoit le flux ST 2110 et l'écrit en ZÉRO-COPIE
# dans le ring /dev/shm/{{hostname}}_{{idx}} (mtl_rx possède/mmap le shm lui-même). Sans SDP :
# mire de SIMULATION (numpy). Expose fps:8080 (agrégé des stats mtl_rx) + control:8082.
#
# Les paramètres DPDK (VF PCI, IP de la VF sur le réseau 2110, lcores) sont injectés dans CONFIG
# par l'orchestrateur au déploiement (C3b). En leur absence → simulation uniquement.

import json, mmap, os, re, signal, struct, subprocess, threading, time
import numpy as np
from http.server import HTTPServer, BaseHTTPRequestHandler

CONFIG          = {config}
HOSTNAME        = "{hostname}"
PLUGIN_VERSION  = "{plugin_version}"

def _as_bool(v):
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on")
    return bool(v)

N_VIDEO   = int(CONFIG.get("video_count") or 0)
N_AUDIO   = int(CONFIG.get("audio_count") or 0)
WIDTH     = int(CONFIG.get("width") or 1280)     # défaut/simu (réel = lu du SDP)
HEIGHT    = int(CONFIG.get("height") or 720)
CHROMA    = str(CONFIG.get("chroma") or "422")
BIT_DEPTH = int(CONFIG.get("bit_depth") or 8)

# Paramètres DPDK injectés par l'orchestrateur (vides → simu only)
VF_PCI  = str(CONFIG.get("vf_pci") or "")
VF_SIP  = str(CONFIG.get("vf_sip") or "")

def _auto_lcores():
    # lcores = cpus du cpuset réel du container (≥4 imposé au déploiement), moins 1 cœur
    # laissé aux threads de contrôle DPDK (sinon « Failed to create thread for interrupt »).
    try:
        cpus = sorted(os.sched_getaffinity(0))
    except Exception:
        cpus = list(range(os.cpu_count() or 1))
    if len(cpus) > 1:
        cpus = cpus[:-1]
    return ",".join(str(c) for c in cpus[:3])

LCORES  = str(CONFIG.get("lcores") or "") or _auto_lcores()
# Binaire mtl_rx : poussé/prébuildé par le déploiement (C3b) ; surchargeable pour les tests.
MTL_RX  = str(CONFIG.get("mtl_rx_bin") or "/opt/script/mtl_rx")
V_RING  = min(int(CONFIG.get("shm_video_ring", 8) or 8), 8)   # MTL st20 : ring ≤ 8
A_RING  = int(CONFIG.get("shm_audio_ring", 100) or 100)
HDR     = 64
SDP_DIR = "/tmp"

# ─── Layout shm (simu) — identique à receiver_2110 ──────────────────
_DEEP    = BIT_DEPTH >= 10
_BPS     = 2 if _DEEP else 1
_DT      = "<u2" if _DEEP else "u1"
_NEUTRAL = 1 << (BIT_DEPTH - 1)
_BLACK   = 16 << (BIT_DEPTH - 8) if _DEEP else 16
_WHITE   = 235 << (BIT_DEPTH - 8) if _DEEP else 235
_CW = {{"420": 2, "422": 2, "444": 1}}.get(CHROMA, 2)
_CH = {{"420": 2, "422": 1, "444": 1}}.get(CHROMA, 1)
UV_W = WIDTH // _CW
UV_H = HEIGHT // _CH
Y_SIZE       = WIDTH * HEIGHT * _BPS
UV_SIZE      = UV_W * UV_H * _BPS
V_FRAME_SIZE = Y_SIZE + 2 * UV_SIZE
V_TOTAL_SIZE = HDR + V_RING * V_FRAME_SIZE

metrics = [{{"idx": i, "essence": "video", "fps": 0.0, "frame_index": 0, "mode": "init"}}
           for i in range(N_VIDEO)]
metrics_lock = threading.Lock()


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        with metrics_lock:
            payload = {{"receivers": [dict(m) for m in metrics]}}
        self.wfile.write(json.dumps(payload).encode())
    def log_message(self, *a): pass


class ControlHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{{"ok": true, "note": "receiver_2110_mtl"}}')
    def log_message(self, *a): pass


threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8080), MetricsHandler).serve_forever(),
                 daemon=True).start()
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8082), ControlHandler).serve_forever(),
                 daemon=True).start()


def _ensure_mtl_rx():
    """Build mtl_rx depuis /opt/script/mtl_rx.c si présent et binaire absent (C3b le poussera)."""
    if os.path.exists(MTL_RX):
        return True
    src = "/opt/script/mtl_rx.c"
    if os.path.exists(src):
        env = dict(os.environ)
        env["PKG_CONFIG_PATH"] = ("/usr/local/lib/x86_64-linux-gnu/pkgconfig:"
                                  "/usr/local/lib/pkgconfig:" + env.get("PKG_CONFIG_PATH", ""))
        cc = subprocess.run(
            "cc -O2 -o {{}} {{}} $(pkg-config --cflags --libs mtl) -lpthread -lm".format(MTL_RX, src),
            shell=True, env=env, capture_output=True, text=True)
        if cc.returncode != 0:
            print("mtl_rx build fail:", cc.stderr[:400], flush=True)
        return cc.returncode == 0
    return False


# ─── Parseur SDP minimal (ST 2110-20) ───────────────────────────────
def _parse_sdp(path):
    """Renvoie un dict {{mcast,port,pt,width,height,fps,interlaced}} ou None."""
    try:
        txt = open(path).read()
    except Exception:
        return None
    info = {{"interlaced": False}}
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
    info.setdefault("fps", 25.0)
    return info


def _launch_mtl_rx(idx, info):
    shm = "/dev/shm/{{}}_{{}}".format(HOSTNAME, idx)
    stats = "/tmp/mtl_rx_{{}}.json".format(idx)
    try:
        os.remove(stats)
    except OSError:
        pass
    args = [MTL_RX, "--pci", VF_PCI, "--sip", VF_SIP,
            "--mcast", info["mcast"], "--udp_port", str(info["port"]),
            "--payload_type", str(info["pt"]),
            "--width", str(info["width"]), "--height", str(info["height"]),
            "--fps", str(info["fps"]),
            "--shm", shm, "--ring", str(V_RING), "--hdr", str(HDR),
            "--lcores", LCORES, "--stats_file", stats]
    if info.get("interlaced"):
        args.append("--interlaced")
    print("mtl_rx launch idx={{}}: {{}}".format(idx, " ".join(args)), flush=True)
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


def video_slot(idx):
    """Bascule SIMU ↔ mtl_rx selon la présence d'un SDP NMOS. Un seul producteur du shm à la fois."""
    sdp_path = "{{}}/nmos_recv_v_{{}}.sdp".format(SDP_DIR, idx)
    proc = None
    stats = None
    cur_key = None
    sim_mm = None
    fi = 0
    can_mtl = bool(VF_PCI and LCORES)
    while True:
        info = _parse_sdp(sdp_path) if (can_mtl and os.path.exists(sdp_path) and _ensure_mtl_rx()) else None
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
                cur_key = key
            st = _read_stats(stats) if stats else None
            with metrics_lock:
                metrics[idx]["mode"] = "mtl"
                if st:
                    metrics[idx]["fps"] = st[0]
                    metrics[idx]["frame_index"] = st[1]
            time.sleep(1.0)
        else:
            # pas de source → simulation
            if proc:
                proc.terminate()
                try: proc.wait(timeout=3)
                except Exception: proc.kill()
                proc = None; cur_key = None
            if sim_mm is None:
                sim_mm = _open_shm("/dev/shm/{{}}_{{}}".format(HOSTNAME, idx), V_TOTAL_SIZE)
            _simu_frame(sim_mm, fi)
            fi += 1
            if fi % 25 == 0:
                with metrics_lock:
                    metrics[idx]["mode"] = "simu"
                    metrics[idx]["fps"] = 25.0
                    metrics[idx]["frame_index"] = fi
            time.sleep(1.0 / 25)


def _cleanup(*a):
    raise SystemExit(0)
signal.signal(signal.SIGTERM, _cleanup)

for _i in range(N_VIDEO):
    threading.Thread(target=video_slot, args=(_i,), daemon=True).start()

print("receiver_2110_mtl {{}} : {{}}v/{{}}a, vf={{}} lcores={{}} ring={{}} mtl_rx={{}} (C3a)".format(
    PLUGIN_VERSION, N_VIDEO, N_AUDIO, VF_PCI or "-", LCORES or "-", V_RING, MTL_RX), flush=True)

while True:
    time.sleep(3600)

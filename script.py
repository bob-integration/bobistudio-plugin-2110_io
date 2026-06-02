# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

# receiver_2110_mtl — wrapper Python déployé par l'agent.
# Rôle : créer le ring shared memory AU MÊME FORMAT que receiver_2110 (profondeur de bits et
# tailles de ring pilotées par les réglages MXL, injectés dans CONFIG), exposer fps:8080 +
# control:8082, et — en Phase C — lancer le binaire C `mtl_rx` (libmtl/DPDK) qui écrira en
# zéro-copie (st20p external frames) dans ce ring. Tant que la RX MTL n'est pas câblée, on
# tourne en SIMULATION (mire numpy) afin de valider shm + métriques + intégration NMOS.

import mmap, os, struct, time, threading, json
import numpy as np
from http.server import HTTPServer, BaseHTTPRequestHandler

# ─── Config injectée (contrat plugin) ───────────────────────────────
CONFIG          = {config}
HOSTNAME        = "{hostname}"
PLUGIN_VERSION  = "{plugin_version}"

def _as_bool(v):
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on")
    return bool(v)

N_VIDEO   = int(CONFIG.get("video_count") or 0)
N_AUDIO   = int(CONFIG.get("audio_count") or 0)
WIDTH     = int(CONFIG.get("width") or 1280)
HEIGHT    = int(CONFIG.get("height") or 720)
FPS       = float(CONFIG.get("fps") or 25) or 25.0
CHROMA    = str(CONFIG.get("chroma") or "422")
BIT_DEPTH = int(CONFIG.get("bit_depth") or 8)

# Layout shm vidéo — IDENTIQUE à receiver_2110 (interop pipeline). Profondeur 8 → uint8,
# 10/12 → uint16 little-endian. Ring et profondeur viennent des réglages (CONFIG), pas en dur.
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
V_HEADER_SIZE = 64
V_RING_SIZE   = int(CONFIG.get("shm_video_ring", 10) or 10)
Y_SIZE        = WIDTH * HEIGHT * _BPS
UV_SIZE       = UV_W * UV_H * _BPS
V_FRAME_SIZE  = Y_SIZE + 2 * UV_SIZE
V_TOTAL_SIZE  = V_HEADER_SIZE + V_RING_SIZE * V_FRAME_SIZE

# Layout shm audio — IDENTIQUE à receiver_2110 (PCM L24 / 48k / 8ch, chunks 1ms)
A_SAMPLE_RATE      = 48000
A_CHANNELS         = 8
A_BYTES_PER_SAMPLE = 3
A_SAMPLES_PER_CHUNK = A_SAMPLE_RATE // 1000
A_CHUNK_SIZE  = A_SAMPLES_PER_CHUNK * A_CHANNELS * A_BYTES_PER_SAMPLE
A_HEADER_SIZE = 64
A_RING_SIZE   = int(CONFIG.get("shm_audio_ring", 100) or 100)
A_TOTAL_SIZE  = A_HEADER_SIZE + A_RING_SIZE * A_CHUNK_SIZE

metrics = (
    [{{"idx": i, "essence": "video", "fps": 0.0, "frame_index": 0}} for i in range(N_VIDEO)]
    + [{{"idx": i, "essence": "audio", "fps": 0.0, "chunk_index": 0}} for i in range(N_AUDIO)]
)
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
    # Stub de parité avec receiver_2110 (:8082/gen) — étendu en Phase C (bascule simu↔RX MTL).
    def do_POST(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{{"ok": true, "note": "stub MTL phase A"}}')
    def log_message(self, *a): pass


threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8080), MetricsHandler).serve_forever(),
                 daemon=True).start()
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", 8082), ControlHandler).serve_forever(),
                 daemon=True).start()


def _open_shm(path, size):
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o666)
    os.ftruncate(fd, size)
    mm = mmap.mmap(fd, size)
    os.close(fd)
    return mm


# ─── PHASE C (à venir) ───────────────────────────────────────────────
# Lancer ici le binaire C `mtl_rx` : il recevra le PCI de la VF + le multicast/port/format
# (issus de l'activation NMOS IS-05) + le chemin shm, et écrira en ZÉRO-COPIE via st20p
# external frames directement dans le ring ci-dessous (mêmes offsets/headers). En attendant,
# SIMULATION numpy pour valider le format shm, les métriques et l'enregistrement NMOS.

def _video_sim(idx):
    mm = _open_shm(f"/dev/shm/{{HOSTNAME}}_{{idx}}", V_TOTAL_SIZE)
    base = np.full((HEIGHT, WIDTH), _BLACK, dtype=np.dtype(_DT))
    neutral = np.full((UV_H, UV_W), _NEUTRAL, dtype=np.dtype(_DT)).tobytes()
    interval = 1.0 / FPS
    fi = 0
    start = time.time()
    nxt = start
    while True:
        frame = base.copy()
        col = (fi * 8) % WIDTH
        frame[:, col:min(col + 8, WIDTH)] = _WHITE   # barre verticale mobile
        off = V_HEADER_SIZE + (fi % V_RING_SIZE) * V_FRAME_SIZE
        mm[off:off + Y_SIZE] = frame.tobytes()
        mm[off + Y_SIZE:off + Y_SIZE + UV_SIZE] = neutral
        mm[off + Y_SIZE + UV_SIZE:off + Y_SIZE + 2 * UV_SIZE] = neutral
        mm[0:16] = struct.pack("QQ", fi, time.time_ns())
        fi += 1
        if fi % 25 == 0:
            with metrics_lock:
                el = time.time() - start
                if el > 0:
                    metrics[idx]["fps"] = round(fi / el, 1)
                    metrics[idx]["frame_index"] = fi
        nxt += interval
        d = nxt - time.time()
        if d > 0:
            time.sleep(d)
        else:
            nxt = time.time()


def _audio_sim(idx):
    moff = N_VIDEO + idx
    mm = _open_shm(f"/dev/shm/{{HOSTNAME}}_audio_{{idx}}", A_TOTAL_SIZE)
    silence = bytes(A_CHUNK_SIZE)
    interval = 0.001
    ci = 0
    start = time.time()
    nxt = start
    while True:
        off = A_HEADER_SIZE + (ci % A_RING_SIZE) * A_CHUNK_SIZE
        mm[off:off + A_CHUNK_SIZE] = silence
        mm[0:24] = struct.pack("QQIHH", ci, time.time_ns(),
                               A_SAMPLES_PER_CHUNK, A_CHANNELS, A_BYTES_PER_SAMPLE)
        ci += 1
        if ci % 1000 == 0:
            with metrics_lock:
                el = time.time() - start
                if el > 0:
                    metrics[moff]["fps"] = round(ci / el, 1)
                    metrics[moff]["chunk_index"] = ci
        nxt += interval
        d = nxt - time.time()
        if d > 0:
            time.sleep(d)
        else:
            nxt = time.time()


for _i in range(N_VIDEO):
    threading.Thread(target=_video_sim, args=(_i,), daemon=True).start()
for _i in range(N_AUDIO):
    threading.Thread(target=_audio_sim, args=(_i,), daemon=True).start()

print(f"receiver_2110_mtl {{PLUGIN_VERSION}} : {{N_VIDEO}}v/{{N_AUDIO}}a "
      f"{{WIDTH}}x{{HEIGHT}} {{BIT_DEPTH}}bit ring v={{V_RING_SIZE}}/a={{A_RING_SIZE}} "
      f"— SIMULATION (RX MTL en Phase C)", flush=True)

while True:
    time.sleep(3600)

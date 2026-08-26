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

import hashlib, json, mmap, os, re, signal, ssl, struct, subprocess, threading, time, zlib
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

# Base des ports du contrôleur (--network host) : :BASE métriques (get_metrics), :BASE+1 contrat
# agent (/nmos/subscribe, /status), :BASE+2 contrôle à chaud (/gen, /input…). Défaut 8080 → moteur
# mono-nœud STRICTEMENT inchangé. Une SONDE (probe_2110) déployée sur le MÊME nœud qu'un moteur (banc
# loopback : générateur port A + sonde port B) reçoit un offset (docker_driver -e CONTROLLER_PORT_BASE)
# pour ne pas entrer en conflit de ports avec le moteur (les 3 :808x sont en dur sinon).
PORT_BASE     = int(os.environ.get("CONTROLLER_PORT_BASE") or 8080)
PORT_METRICS  = PORT_BASE       # rapport / métriques (contrat get_metrics)
PORT_AGENT    = PORT_BASE + 1   # contrat agent : /nmos/subscribe (SDP IS-05), /status, /tx, /pin
PORT_CONTROL  = PORT_BASE + 2   # contrôle à chaud : /gen, /ident, /input, /state…
HOSTNAME   = os.environ.get("HOSTNAME_RX") or os.environ.get("HOSTNAME") or "mtlrx"

# ── NUMÉROTATION PUBLIQUE : le 0 n'existe pas ────────────────────────────────────────────────
# ⚠ MIROIR EXACT de `app/numerotation.py:numero()`, qui fait foi et documente la règle. Ce
# fichier tourne DANS l'image du moteur : il ne peut pas importer `app`. Une divergence entre
# les deux ne casserait rien au démarrage — le moteur nommerait simplement ses flux autrement
# que l'orchestrateur ne les cherche, et tout le câblage tomberait en silence.
#
# `idx` reste l'indice de tableau 0-based (slots, pools) ; SEULE la mise en chaîne est décalée :
# noms de flux, libellés RX/TX, noms de session SDP. Les graines de SSRC (`_ssrc(...)`) NE
# passent PAS par ici : les laisser sur l'indice brut préserve l'identité RTP à la bascule.
def _num(idx):
    """Indice de tableau (0-based) → numéro PUBLIC (1-based)."""
    return int(idx) + 1

N_VIDEO    = int(os.environ.get("RX_COUNT") or os.environ.get("VIDEO_COUNT") or 1)   # slots RX vidéo
N_TX       = int(os.environ.get("TX_COUNT") or 0)                                     # slots TX (senders)
N_AUDIO    = int(os.environ.get("AUDIO_COUNT") or 0)                                   # slots RX audio (st30)
N_ANC      = int(os.environ.get("ANC_COUNT") or 0)                                     # slots RX ANC (st40)
A_CHANNELS = 8
A_RING     = max(2, int(os.environ.get("AUDIO_RING") or 100))   # ring shm audio (chunks 1ms)
# Ptime audio (ST 2110-30) par DÉFAUT (ms) — repli quand le SDP n'a pas d'a=ptime. Réglable par
# installation (setting mtl_audio_ptime → env AUDIO_PTIME). Le SDP a=ptime PRIME (auto par entrée).
A_PTIME_DEF = float(os.environ.get("AUDIO_PTIME") or 1.0)
# Ptime audio autorisés (ST 2110-30 → enum ST30_PTIME_*, cf. mtl_rx.c:to_st30_ptime). Une sortie TX
# peut déclarer le SIEN (par-sortie) ; toute valeur hors de ce set retombe sur A_PTIME_DEF (le global).
A_PTIME_VALID = (0.125, 0.25, 0.333, 1.0, 4.0)
def _coerce_ptime(v):
    """ptime (ms) validé contre le set ST30 → valeur canonique, ou None si absent/invalide
    (l'appelant replie alors sur A_PTIME_DEF). Rétro-compatible : une sortie sans ptime → None → défaut."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    for p in A_PTIME_VALID:
        if abs(f - p) < 0.01:
            return p
    return None
def _tx_ptime(acfg):
    """ptime effectif d'une sortie audio TX : acfg['ptime'] si valide, sinon défaut global A_PTIME_DEF."""
    p = _coerce_ptime((acfg or {}).get("ptime"))
    return p if p is not None else A_PTIME_DEF
ACTIVE_RX   = int(os.environ.get("ACTIVE_RX_COUNT") or min(6, max(1, N_VIDEO)))
ACTIVE_TX_C = int(os.environ.get("ACTIVE_TX_COUNT") or min(6, max(0, N_TX)))
_tx_budget_warned = False
# Plafond de files TX sous pacing RL matériel (port E810 dpdk), APRÈS le patch libmtl
# `patch_tm_hierarchy.py` (arbre TM ramifié, 0.39.6). Le patch attache jusqu'à
# MT_MAX_RL_ITEMS=128 feuilles réparties sur P nœuds queue-group (P×8), la file de contrôle
# comprise (nb_tx_q = tx_queues+1). Capacité RÉELLE mesurée au banc dl360-1 (2026-07-07,
# loopback E810, DDP comms 1.3.63) = >8 confirmé, ≥16 senders RL tenus (cf. docs/chantiers/DPDK_NARROW.md
# §Capacité RL réelle E810). Bornage conservateur : la file de contrôle + marge.
RL_TX_QUEUES_CAP = int(os.environ.get("RL_TX_QUEUES_CAP") or 63)
_cpu_last_usec = None
_cpu_last_time = None
_bw_last = {}
_xdp_sessions_active = 0
# Sessions AF-XDP actives PAR PORT physique ({iface: count}). Le moteur est le SEUL à savoir le vrai
# compte par port : une session libmtl = une file, MÊME en fan-out (1 multicast → N slots = 1 session).
# Compter les flux côté orchestrateur sur-compterait. Mis à jour avec _xdp_sessions_active.
_xdp_active_per_iface = {}
# Ventilation RX/TX des sessions actives PAR PORT ({iface: count}) — socle DPDK narrow : le budget
# TX pertinent devient les sessions RL par port (cap RL_TX_QUEUES_CAP), le RX les files RSS. Mis à
# jour avec _xdp_active_per_iface (même règle role=='tx' que _write_config). Exposés :8080 (bloc
# `rl` + nic.ports[].{rx,tx}_sessions_active) pour la supervision Sources/Destinations.
_tx_active_per_iface = {}
_rx_active_per_iface = {}
# Sessions TX IGNORÉES au-delà du cap RL (cf. _emit_tx dans la boucle de réconciliation) — sur-
# capacité surfacée à l'UI (badge SUR-CAPACITÉ) au lieu d'un simple log.
_tx_sessions_dropped = 0
# Files RÉSERVÉES au dernier mtl_init (rx_queues/tx_queues passés au lancement). Distinct de
# `_rx/_tx_queues_alloc` qui suit la DEMANDE courante (recalculée à chaque _write_config) : le daemon
# ne relit PAS rx_queues après mtl_init → la réservation est FIGÉE jusqu'au prochain (re)lancement.
# C'est le « plafond à chaud » : au-delà, créer une session échoue tant qu'on n'a pas relancé.
_rx_queues_reserved = 0
_tx_queues_reserved = 0
# Réservation PAR PORT figée au dernier mtl_init (liste de {iface,rx_queues,tx_queues}). Sert à
# détecter, dans la boucle de réconciliation, qu'un port a besoin de PLUS de files que ce que le
# daemon a réservé au lancement → relance ciblée (cf. _ports_need_relaunch). Sans ça, les sessions
# ajoutées à chaud au-delà du budget gelé se créent mais n'obtiennent aucune file XDP → 0 fps.
_ports_reserved = []
# Demande RÉELLE par port (sessions effectives, SANS headroom ni plancher) — sert à décider la
# relance : on compare la demande réelle à la RÉSERVE (qui, elle, inclut headroom + plancher).
# Indispensable pour que le headroom crée une VRAIE marge : sinon (headroom des deux côtés de la
# comparaison) il s'annule et toute hausse de demande au-delà du boot relancerait le daemon →
# teardown des flux RX → gel des consommateurs aval (multiviews).
_ports_demand = []
# Headroom de files PAR PORT auto : marge pré-réservée à CHAQUE (re)lancement pour absorber des
# ajouts à chaud (audio/ANC/récepteurs) SANS relance. Compromis capacité↔souplesse — le HW E810
# offre 96 combined/port, la marge est quasi gratuite. Surchargeable par env (0 = comportement
# strict d'avant). Au-delà du headroom, la boucle relance le daemon (debounce) pour ne jamais
# rester muet (le bug historique : plafond figé ~8 = 2 files/port × 4 ports).
_DEF_RX_HEADROOM = 4
_DEF_TX_HEADROOM = 2
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


_mtl_ports_cache = {"t": 0.0, "data": None}

def _mtl_ports_read():
    """Contrat /tmp/mtl_ports.json (cf. docs/chantiers/DPDK_NARROW.md) : stats I/O PAR PORT écrites toutes les
    ~2 s par le daemon mtl_rx (source mtl_get_port_stats — compteurs CUMULÉS). Remplace
    ethtool -S pour un port en PMD DPDK (l'iface kernel a disparu en vfio). Cache court.
    Renvoie {"ts":…, "ports":[…]} ou None (daemon pas encore lancé / fichier absent)."""
    now = time.monotonic()
    if now < _mtl_ports_cache["t"] + 0.5:
        return _mtl_ports_cache["data"]
    data = None
    try:
        with open("/tmp/mtl_ports.json") as f:
            data = json.load(f)
    except Exception:
        pass
    _mtl_ports_cache["t"] = now
    _mtl_ports_cache["data"] = data
    return data


def _mtl_port_entry(iface):
    """Entrée du contrat mtl_ports.json pour le port `iface` (clé 'port' = ifname|BDF), ou None."""
    d = _mtl_ports_read()
    for p in (d or {}).get("ports") or []:
        if p.get("port") in (iface, _port_bdf(iface)):
            return p
    return None


def _nic_bps_mtl(iface):
    """Débit RX/TX d'un port PMD DPDK : delta sur les compteurs CUMULÉS du contrat mtl_ports.json.

    Le data-plane écrit ce fichier ~toutes les 2 s (compteurs + un `ts` en SECONDES FLOTTANTES de
    l'horloge monotone, même snapshot que les octets — cf. mtl_rx.c:write_port_stats). Le piège à
    éviter : recalculer le débit au rythme des requêtes :8080 (0,5 s) donne un aliasing — la plupart
    des lectures tombent sur un fichier inchangé (Δoctets=0 → 0 Gbps) et celle juste après une
    écriture voit ~2 s de trafic divisées par ~0,5 s (débit ×4). D'où le débit qui « saute » 0 / pic
    sur une sortie 2110 pourtant CBR.

    Correctif : on ne recalcule QUE lorsqu'une NOUVELLE écriture est apparue (`ts` avancé), avec
    Δt = Δts. Comme les octets et `ts` viennent du MÊME snapshot, `Δoctets/Δts` est le débit moyen
    RÉEL de l'intervalle, indépendant du rythme de poll. Entre deux écritures on conserve le dernier
    débit (jamais 0 fantôme). Un intervalle réellement sans trafic (ts avance, octets non) → 0 correct.

    0.59.0 : PLUS D'EMA. Le lissage α=0,5 ne compensait que la quantification de l'ancien `ts`
    entier (±1 s sur ~2 s = ±50 % d'erreur) ; avec un Δts exact la valeur instantanée EST le débit
    moyen de l'intervalle — lisser ne ferait plus que retarder l'affichage d'un vrai changement.
    État partagé `_bw_last[iface]`."""
    cap = 100.0   # vfio : /sys/class/net/<if>/speed n'existe plus → capacité E810 par défaut
    data = _mtl_ports_read()          # lecture cachée (~0,5 s) du contrat, atomique côté data-plane
    ent = None
    if data:
        for p in (data.get("ports") or []):
            if p.get("port") in (iface, _port_bdf(iface)):
                ent = p; break
    if ent is None:
        _bw_last[iface] = {"ts": None, "rx": None, "tx": None, "rx_gbps": None, "tx_gbps": None}
        return None, None, cap
    ts = data.get("ts")
    rx = int(ent.get("rx_bytes") or 0)
    tx = int(ent.get("tx_bytes") or 0)
    last = _bw_last.get(iface)
    # Par défaut : CONSERVER le dernier débit (ni 0 fantôme, ni pic dû à un Δt d'échantillonnage court).
    rx_gbps = (last or {}).get("rx_gbps")
    tx_gbps = (last or {}).get("tx_gbps")
    have_ref = bool(last and last.get("rx") is not None and last.get("ts") is not None and ts is not None)
    if have_ref and (ts < last["ts"] or rx < last["rx"] or tx < last["tx"]):
        # ts/compteurs qui reculent = redémarrage du daemon (remis à 0) → ré-armer, pas de débit.
        _bw_last[iface] = {"ts": ts, "rx": rx, "tx": tx, "rx_gbps": None, "tx_gbps": None}
        return None, None, cap
    if have_ref and ts > last["ts"]:
        # Nouvelle écriture du data-plane : Δt = Δts (débit vrai, poll-indépendant, sans lissage).
        # Octets figés sur l'intervalle → 0 (flux à l'arrêt), affiché tel quel.
        dts = ts - last["ts"]
        rx_gbps = round((rx - last["rx"]) * 8 / dts / 1e9, 2)
        tx_gbps = round((tx - last["tx"]) * 8 / dts / 1e9, 2)
        _bw_last[iface] = {"ts": ts, "rx": rx, "tx": tx, "rx_gbps": rx_gbps, "tx_gbps": tx_gbps}
        return rx_gbps, tx_gbps, cap
    if not have_ref:
        # Première mesure (ou ts/octets indisponibles) : poser la référence, débit pas encore calculable.
        _bw_last[iface] = {"ts": ts, "rx": rx, "tx": tx, "rx_gbps": rx_gbps, "tx_gbps": tx_gbps}
        return rx_gbps, tx_gbps, cap
    # Même écriture relue (ts inchangé) → garder référence ET dernier débit.
    return rx_gbps, tx_gbps, cap


def _nic_bps(iface):
    """Débit RX/TX via ethtool -S (compteurs matériels, inclut AF_XDP zero-copy).
    sysfs statistics/tx_bytes ne compte PAS le trafic AF_XDP → toujours 0 pour MTL.
    Cache PAR INTERFACE (`_bw_last[iface]`) : multi-NIC = un état de delta par port,
    sinon les compteurs de deux ports écrasent mutuellement leur référence → débits faux."""
    if _port_pmd(iface) == "dpdk":
        return _nic_bps_mtl(iface)   # port vfio : plus d'ethtool → stats MTL (contrat)
    now = time.monotonic()
    try:
        cap = int(open(f"/sys/class/net/{iface}/speed").read().strip()) / 1000
    except Exception:
        cap = 100.0
    last = _bw_last.get(iface)
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
        _bw_last[iface] = {"rx": None, "tx": None, "t": now, "rx_gbps": None, "tx_gbps": None}
        return None, None, cap
    if rx is None or tx is None:
        _bw_last[iface] = {"rx": None, "tx": None, "t": now, "rx_gbps": None, "tx_gbps": None}
        return None, None, cap
    rx_gbps = tx_gbps = None
    if last and last.get("rx") is not None and now > last.get("t", 0) + 0.5:
        dt = now - last["t"]
        if dt > 0:
            rx_gbps = round((rx - last["rx"]) * 8 / dt / 1e9, 2)
            tx_gbps = round((tx - last["tx"]) * 8 / dt / 1e9, 2)
    _bw_last[iface] = {"rx": rx, "tx": tx, "t": now, "rx_gbps": rx_gbps, "tx_gbps": tx_gbps}
    return rx_gbps, tx_gbps, cap


def _nic_link(iface):
    """État/vitesse du lien d'un port : operstate ('up'/'down') + speed Mb/s (sysfs)."""
    up = None
    try:
        up = open(f"/sys/class/net/{iface}/operstate").read().strip() == "up"
    except Exception:
        pass
    return up


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
# Multi-NIC : liste des interfaces média (CSV `IFACES`, aligné avec `SIPS`). IFACE = 1ʳᵉ NIC,
# conservée pour tout le code mono-NIC existant (purge XDP/ntuple, steering PTP, sip des SDP).
# Mono-NIC → liste à un élément (= IFACE) → strictement iso-comportement.
IFACES     = [s.strip() for s in (os.environ.get("IFACES") or IFACE).split(",") if s.strip()] or [IFACE]
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

# IP source PAR NIC (CSV `SIPS`, aligné sur IFACES) ; manquante pour une NIC → auto-détection
# locale. Le 1ᵉʳ élément reste piloté par `SIP` (rétro-compat). Émis dans config.ports[].sip.
_sips_env = [s.strip() for s in (os.environ.get("SIPS") or "").split(",")]
SIPS = []
for _i, _if in enumerate(IFACES):
    _s = _sips_env[_i] if (_i < len(_sips_env) and _sips_env[_i]) else ""
    SIPS.append(_s or _detect_iface_ip(_if))
if SIPS:
    SIPS[0] = SIP or SIPS[0]

# ── MASQUE + PASSERELLE PAR NIC (CSV `NETMASKS` / `GATEWAYS`, alignés sur IFACES) ──────────────
# Un port bindé vfio-pci n'a plus de netdev kernel : libmtl porte TOUTE sa couche 3 et ne recevait
# jusqu'ici que l'adresse. Sans masque elle ignore ce qui est sur le lien ; sans passerelle elle ne
# peut joindre aucun unicast hors sous-réseau. Un plant micro-segmenté en /30 avec une passerelle
# PAR fabric (A/B) l'exige. Vides → rien n'est émis, comportement strictement inchangé.
_masks_env = [s.strip() for s in (os.environ.get("NETMASKS") or "").split(",")]
_gws_env   = [s.strip() for s in (os.environ.get("GATEWAYS") or "").split(",")]
NETMASKS = [(_masks_env[_i] if _i < len(_masks_env) else "") for _i in range(len(IFACES))]
GATEWAYS = [(_gws_env[_i] if _i < len(_gws_env) else "") for _i in range(len(IFACES))]

# ── PMD PAR PORT (chantier DPDK, cf. docs/chantiers/DPDK_NARROW.md) ── `PORT_PMDS`/`PORT_BDFS` = CSV alignés
# sur IFACES, émis par l'orchestrateur SEULEMENT si ≥1 port est en vfio-pci (node_interfaces.pmd
# ='dpdk', BDF dans PORT_BDFS). Absents → tous les ports en af_xdp → STRICTEMENT iso-comportement
# (règle anti-régression n°1 : tout le code dpdk est gaté par `_port_pmd(...) == "dpdk"`).
_pmds_env = [s.strip() for s in (os.environ.get("PORT_PMDS") or "").split(",")]
_bdfs_env = [s.strip() for s in (os.environ.get("PORT_BDFS") or "").split(",")]
PORT_PMDS = [(_pmds_env[_i] if _i < len(_pmds_env) and _pmds_env[_i] else "af_xdp")
             for _i in range(len(IFACES))]
PORT_BDFS = [(_bdfs_env[_i] if _i < len(_bdfs_env) else "") for _i in range(len(IFACES))]
# CLASSE 2110-21 PAR PORT (#26) : `PORT_PROFILES` = CSV aligné sur IFACES (narrow|narrow_linear|
# wide), émis par l'orchestrateur avec les autres clés dpdk. Absent/vide → narrow (défaut strict) →
# ops.transport_pacing=NARROW côté moteur = comportement historique (memset).
_profs_env = [s.strip().lower() for s in (os.environ.get("PORT_PROFILES") or "").split(",")]
PORT_PROFILES = [((_profs_env[_i] if _i < len(_profs_env) and _profs_env[_i] in
                   ("narrow", "narrow_linear", "wide") else "narrow")) for _i in range(len(IFACES))]
_HAS_DPDK = any(p == "dpdk" for p in PORT_PMDS)
# PTP INTERNE libmtl actif = même prédicat que mtl_rx.c (ENGINE_PTP=libmtl ∧ au moins un port dpdk ;
# la variable est posée par l'orchestrateur, docker_driver._has_dpdk_pf). Quand il vaut True, le
# moteur EST l'horloge du nœud — d'où le maintien du daemon même sans session (cf. _manager_loop).
_ENGINE_PTP_ON = _HAS_DPDK and (os.environ.get("ENGINE_PTP") or "").strip().lower() == "libmtl"

def _port_pmd(ifn):
    """PMD du port `ifn` : 'af_xdp' (défaut, chemin actuel) ou 'dpdk' (vfio-pci)."""
    return PORT_PMDS[IFACES.index(ifn)] if ifn in IFACES else "af_xdp"

def _rl_is_active():
    """Vrai si le pacing RL (rate-limiter matériel, socle narrow) est le mécanisme TX effectif :
    ≥1 port dpdk ET MTL_PACING rl/auto (auto → RL sur E810 dpdk). MÊME gate que le clamp
    RL_TX_QUEUES_CAP de _write_config — tsc/tsc_narrow ne construisent aucune hiérarchie TM
    donc ne sont jamais bornés (le cap ne les concerne pas)."""
    _pacing = (os.environ.get("MTL_PACING") or "auto").strip().lower()
    return _HAS_DPDK and _pacing in ("rl", "auto")

def _port_bdf(ifn):
    """BDF PCI du port `ifn` ('' si af_xdp / inconnu)."""
    return PORT_BDFS[IFACES.index(ifn)] if ifn in IFACES else ""

def _port_profile(ifn):
    """Classe 2110-21 DEMANDÉE pour le port `ifn` : narrow|narrow_linear|wide (défaut narrow)."""
    return PORT_PROFILES[IFACES.index(ifn)] if ifn in IFACES else "narrow"


def _pacing_materiel(ifn):
    """Vrai si l'émission de ce port est cadencée par le LIMITEUR MATÉRIEL de la carte (RL).
    C'est la seule mécanique qui permet de TENIR la classe narrow (cf. `_rl_is_active`)."""
    return _port_pmd(ifn) == "dpdk" and _rl_is_active()


def _port_profile_effectif(ifn):
    """Classe 2110-21 RÉELLEMENT TENABLE par ce port — c'est ELLE qui part dans le SDP.

    Le profil décide de ce qu'on ANNONCE au récepteur (`TP=2110TPN|TPNL|TPW`), et un récepteur
    strict applique la fenêtre correspondante. Sans limiteur matériel (chemin AF-XDP), le pacing
    est logiciel : on ne peut pas GARANTIR narrow, donc l'annoncer est une promesse qu'on ne tient
    pas — c'est le récepteur qui paie la différence, en paquets rejetés hors plage.

    Règle : `wide` dès que le pacing matériel est absent, SAUF si le site a explicitement demandé
    une classe (PORT_PROFILES) — un site qui a mesuré sa régularité reste libre de déclarer narrow.
    """
    demande = _port_profile(ifn)
    if _pacing_materiel(ifn):
        return demande
    explicite = (ifn in IFACES and IFACES.index(ifn) < len(_profs_env)
                 and _profs_env[IFACES.index(ifn)] in ("narrow", "narrow_linear", "wide"))
    return demande if explicite else "wide"


# Classe 2110-21 → jeton `TP=` du SDP (ST 2110-21 §7). UNE seule table, pour que la session et la
# déclaration ne puissent plus diverger.
_TP_SDP = {"narrow": "2110TPN", "narrow_linear": "2110TPNL", "wide": "2110TPW"}


def _tp_sdp(ifn):
    """Jeton `TP=` à annoncer pour une sortie émise sur le port `ifn` (vide → port primaire)."""
    return _TP_SDP.get(_port_profile_effectif(ifn or (IFACES[0] if IFACES else "")), "2110TPW")

# Ports encore sur le chemin kernel/AF-XDP : SEULS concernés par la plomberie kernel (purge XDP,
# ntuple, restriction RSS PTP, contrôle d'IP). Un port dpdk n'a PLUS d'iface kernel (vfio-pci) —
# toute commande ip/ethtool y échouerait — et MTL y gère lui-même ses joins IGMP (PMD DPDK :
# MT_DRV_F_MCAST_IN_DP absent → mt_mcast émet ses membership reports, cf. lib/src/mt_mcast.c).
_AFXDP_IFACES = [ifn for _i, ifn in enumerate(IFACES) if PORT_PMDS[_i] != "dpdk"]

# Répartition des sessions sur les ports (multi-NIC). Par défaut le moteur RÉPARTIT automatiquement
# ses sessions sur les ports du RÉSEAU PRIMAIRE (modulo slot → stable + équilibré, sans flapping) ;
# un slot peut être ÉPINGLÉ sur un port précis (RX_PINS/_tx[i]['iface'], poussé par l'orchestrateur,
# mutable à chaud via :8081/pin). En RX (AF-XDP/IGMP) une session ne reçoit que sur le port qui a
# rejoint le groupe → on ne répartit qu'entre ports d'un MÊME réseau (le primaire).
# PORT_NETS = network_id par port (aligné sur IFACES) ; PRIMARY_NET = réseau primaire.
_PORT_NETS = [s.strip() for s in (os.environ.get("PORT_NETS") or "").split(",")]
_PRIMARY_NET = (os.environ.get("PRIMARY_NET") or "").strip()
# _auto_ports = ports candidats à la répartition auto = ceux du réseau primaire (repli : tous les
# ports si réseau inconnu / un seul réseau déclaré).
if _PRIMARY_NET and len(_PORT_NETS) == len(IFACES):
    _auto_ports = [IFACES[i] for i in range(len(IFACES)) if _PORT_NETS[i] == _PRIMARY_NET] or list(IFACES)
else:
    _auto_ports = list(IFACES)

def _parse_iface_map(envname):
    try:
        m = json.loads(os.environ.get(envname) or "{}")
        return {int(k): str(v) for k, v in m.items() if v in IFACES}
    except Exception:
        return {}
RX_PINS = _parse_iface_map("RX_PINS")   # {slot: ifname} ; mutable à chaud (:8081/pin)
_pins_lock = threading.Lock()

def _parse_port_reserve():
    """PORT_RESERVE (env JSON {iface:{rx,tx,hr}}) = réserve de files par interface réglée par
    l'opérateur (node_interfaces → Réglages Réseau). Sert de PLANCHER de réserve par port dans
    _write_config (capacité « à chaud » prévisible). Clés absentes → plancher par défaut du moteur.
    Restreint aux IFACES connues. Vide → comportement par défaut (rétro-compat)."""
    out = {}
    try:
        m = json.loads(os.environ.get("PORT_RESERVE") or "{}")
        for k, v in (m or {}).items():
            if k in IFACES and isinstance(v, dict):
                e = {}
                for kk in ("rx", "tx", "hr"):
                    if v.get(kk) is not None:
                        e[kk] = max(0, int(v[kk]))
                if e:
                    out[k] = e
    except Exception as _e:
        print("PORT_RESERVE invalide, ignoré: {}".format(_e), flush=True)
    return out
PORT_RESERVE = _parse_port_reserve()

def _auto_iface(idx):
    """Port auto pour un slot non épinglé : modulo stable sur les ports du réseau primaire."""
    return _auto_ports[int(idx) % len(_auto_ports)] if _auto_ports else IFACE

def _rx_iface(idx):
    with _pins_lock:
        pin = RX_PINS.get(int(idx))
    return pin if pin in IFACES else _auto_iface(idx)

def _tx_iface(idx, pin=None):
    """Port TX : épinglage poussé via /tx (_tx[i]['iface']), sinon répartition auto."""
    return pin if pin in IFACES else _auto_iface(idx)

# ─── SMPTE 2022-7 : appariement red/blue des ports ─────────────────────────────────────
# PORT_PAIRS (env "ifA:ifB[,ifC:ifD…]", émis par l'orchestrateur depuis node_interfaces
# pair_group/pair_role) : pour une session dual-leg, le leg redondant part/écoute sur l'iface
# APPARIÉE à celle du leg primaire. Map bidirectionnelle (le primaire peut être red ou blue,
# selon épinglage/répartition). Sans paire déclarée → _pair_iface rend "" → mono-leg partout.
def _parse_port_pairs():
    out = {}
    for tok in (os.environ.get("PORT_PAIRS") or "").split(","):
        a, _, b = tok.strip().partition(":")
        if a in IFACES and b in IFACES and a != b:
            out[a], out[b] = b, a
    return out
PORT_PAIRS = _parse_port_pairs()

def _pair_iface(iface):
    """Iface du leg redondant 2022-7 appariée à `iface` ('' si pas de paire déclarée)."""
    return PORT_PAIRS.get(iface or IFACE, "")

def _leg2(sess, iface, mcast2, port2):
    """Greffe le leg redondant 2022-7 sur un dict de session mtl_rx. Le daemon passe la
    session en num_leg=2 dès que iface2+mcast2+udp_port2 sont présents (parse_session_into) ;
    sans paire déclarée ou sans leg1 alloué → dict inchangé (mono-leg, iso-comportement)."""
    if2 = _pair_iface(iface)
    if if2 and mcast2 and port2:
        sess["iface2"], sess["mcast2"], sess["udp_port2"] = if2, mcast2, int(port2)
    return sess


# ─── Garde-fou IP des ports (post-mortem Horace 2026-07) ──────────────────────────────
# En AF-XDP, libmtl fige au mtl_init l'IP PRIMAIRE détectée de chaque port et joint les groupes
# multicast par ADRESSE (ip_mreq.imr_interface, résolue par le noyau) : une IP média dupliquée ou
# déplacée sur l'hôte fait partir les joins IGMP sur la MAUVAISE NIC → slot RX définitivement muet
# alors que fdir/queue sont posés au bon endroit, et le gel survit aux réalignements (qui reposaient
# l'IP fautive). Ce contrôle détecte les trois dérives (sip absent, sip non primaire, sip dupliqué)
# et les expose sur :8080 (nic.ip_warnings) + stdout. Après correction sur l'hôte, un redéploiement
# du moteur reste REQUIS : le daemon vivant garde le sip périmé figé en mémoire.
_ip_warnings = []

def _check_port_ips():
    global _ip_warnings
    try:
        out = subprocess.run(["ip", "-4", "-o", "addr", "show"],
                             capture_output=True, text=True, timeout=5).stdout
    except Exception:
        return
    addrs = {}                                   # iface → [ip, …] (ordre noyau = primaire d'abord)
    for ln in out.splitlines():
        p = ln.split()
        if len(p) >= 4 and p[2] == "inet":
            addrs.setdefault(p[1], []).append(p[3].split("/")[0])
    warns = []
    for i, ifn in enumerate(IFACES):
        if _port_pmd(ifn) == "dpdk":
            continue   # port vfio : l'iface kernel n'existe plus, joins IGMP gérés par MTL (PMD DPDK)
        sip = SIPS[i] if i < len(SIPS) else ""
        if not sip:
            continue
        mine = addrs.get(ifn) or []
        if sip not in mine:
            warns.append("sip {} absent de {} — joins IGMP impossibles sur ce port".format(sip, ifn))
        elif mine[0] != sip:
            warns.append("{} : IP primaire {} ≠ sip {} — MTL joindra via {}".format(
                ifn, mine[0], sip, mine[0]))
        for other, ips in addrs.items():
            if other != ifn and sip in ips:
                warns.append("IP {} (sip de {}) DUPLIQUÉE sur {} — joins IGMP déroutés : "
                             "purger l'IP puis redéployer le moteur".format(sip, ifn, other))
    if warns != _ip_warnings:
        for w in warns:
            print("⚠ IP ports: " + w, flush=True)
        if not warns and _ip_warnings:
            print("IP ports: anomalies résolues (redéployer le moteur si le daemon tournait déjà)",
                  flush=True)
    _ip_warnings = warns


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

# a=source-filter (SSM) dans les SDP TX — désactivable (SDP_SOURCE_FILTER=0). Bonne pratique
# 2110 sur un fabric SSM-capable (défaut ON), mais sur un switch L2 en IGMP snooping pur le
# join SSM (S,G) du receiver est enregistré sans jamais être forwardé → 0 Mbps silencieux ;
# dans ce cas l'omettre fait retomber les receivers sur un join (*,G) qui, lui, est livré.
SDP_SOURCE_FILTER = str(os.environ.get("SDP_SOURCE_FILTER") or "1").lower() not in ("0", "false", "off")

def _sf_line(mcast, sip):
    """Ligne a=source-filter d'une section média, ou '' si désactivée (fabric non-SSM)."""
    if not SDP_SOURCE_FILTER:
        return ""
    return "a=source-filter:incl IN IP4 {} {}\r\n".format(mcast or "0.0.0.0", sip or "0.0.0.0")

def _sip_of_iface(ifn):
    """IP source de la NIC `ifn` (SIPS aligné sur IFACES), '' si inconnue."""
    try:
        return SIPS[IFACES.index(ifn)] if ifn in IFACES else ""
    except Exception:
        return ""

def _tx_leg_sips(i, t):
    """(sip_leg0, sip_leg1) d'un slot TX : chaque leg 2022-7 annonce l'IP de SA NIC
    (leg1 = interface appariée). Un SDP dont les deux sections portent la même source
    ferait croire à une double émission sur un seul port — et casserait les récepteurs
    SSM (IGMPv3 source-specific) sur le leg secondaire."""
    ifn0 = _tx_iface(i, t.get("iface"))
    sip0 = _sip_of_iface(ifn0) or SIP or "0.0.0.0"
    ifn1 = _pair_iface(ifn0)
    sip1 = (_sip_of_iface(ifn1) if ifn1 else "") or sip0
    return sip0, sip1

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
_SCALE   = (1 << (BIT_DEPTH - 8)) if _DEEP else 1   # 8-bit ref → profondeur courante (barres de couleur)
_CW = {"420": 2, "422": 2, "444": 1}.get(CHROMA, 2)
_CH = {"420": 2, "422": 1, "444": 1}.get(CHROMA, 1)
# Sous-échantillonnage chroma (hx, hy) PAR fmt — la sonde gamut (#25) lit le chroma réel de chaque
# flux (RX/TX arbitraire), pas seulement CHROMA local. (Défini ici, pas dans bobimxl.)
_CHROMA = {"420": (2, 2), "422": (2, 1), "444": (1, 1)}


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
    """Writer MXL vidéo planar : index sur la grille TAI (bobimxl.Writer.next_index, le point
    unique de calcul de la flotte) → grille continue avec le live RX.
    `interlace`=interlaced_tff/bff → libmxl dimensionne chaque grain à 1 CHAMP (½h) et double la
    cadence ; le producteur écrit 2 grains-champs/trame aux index CHAMP (cf. _txgen_loop)."""
    n, d = _fps_rational(fps)
    return bobimxl.Writer(_mxl(), name, w, h, chroma=CHROMA, bit_depth=BIT_DEPTH,
                          fps_num=n, fps_den=d, interlace=interlace)


# Barres de couleur 100% (Y, Cb, Cr en 8 bits, centre chroma 128) → mises à l'échelle bit-depth.
_COLORBARS = [(235, 128, 128), (210, 16, 146), (170, 166, 16), (145, 54, 34),
              (106, 202, 222), (63, 102, 240), (32, 240, 118)]   # blanc jaune cyan vert magenta rouge bleu


def _field_test(fi, f, w, fh):
    """Mire de TEST CHROMA + APPARIEMENT DE CHAMP (entrelacé). f = 0 (1er champ/TOP en tff) / 1 (2e).
    Plans de CHAMP (fh = h/2). Une BANDE LARGE multicolore (barres 100%) qui avance LENTEMENT, dont la
    position dépend de la TRAME (fi) → les 2 champs d'une même trame ont la bande au MÊME X.
    - Émission OK = bande NETTE, couleurs franches, AUCUNE traînée derrière les bords colorés en
      mouvement (test de smearing/traînée de chroma 4:2:2 en entrelacé).
    - Champs mal appariés = bande dédoublée/peigne. Marqueur haut-gauche clair(champ0)/sombre(champ1)."""
    dt = np.dtype(_DT)
    GRAY = (_BLACK + _WHITE) // 2
    uv_w, uv_h = w // _CW, fh // _CH
    STRIPE = 48
    bx = (fi * 6) % w                                # LENT : 6 px/trame ; lié à la TRAME (même X / champ)
    # rangées-modèles (gris partout + bande de barres au début), puis roll à la position bx
    yrow  = np.full(w, GRAY, dtype=dt)
    cbrow = np.full(uv_w, _NEUTRAL, dtype=dt)
    crrow = np.full(uv_w, _NEUTRAL, dtype=dt)
    for i, (by, bcb, bcr) in enumerate(_COLORBARS):
        x0, x1 = i * STRIPE, (i + 1) * STRIPE
        yrow[x0:x1] = by * _SCALE
        cbrow[x0 // _CW:x1 // _CW] = bcb * _SCALE
        crrow[x0 // _CW:x1 // _CW] = bcr * _SCALE
    yrow  = np.roll(yrow, bx)
    cbrow = np.roll(cbrow, bx // _CW)
    crrow = np.roll(crrow, bx // _CW)
    y  = np.tile(yrow, (fh, 1))
    cb = np.tile(cbrow, (uv_h, 1))
    cr = np.tile(crrow, (uv_h, 1))
    y[0:max(2, fh // 8), 0:max(2, w // 8)] = _WHITE if f == 0 else _BLACK   # marqueur de champ (luma)
    return y, cb, cr


# ─── Réutilisation de slot du ring MXL (mire STATIQUE) ────────────────────────────────────────
# `Writer.open_grain()` rend une vue ZÉRO-COPIE sur un slot du ring : le contenu écrit lors d'une
# rotation précédente y est ENCORE PRÉSENT. Pour une mire statique — le cas de TOUT slot TX
# provisionné non câblé, qui émet le fallback noir — remplir le grain à chaque trame est donc une
# recopie d'environ 4 Mo pour un résultat OCTET POUR OCTET IDENTIQUE.
#
# Mesuré le 2026-07-26 sur dl360-1 : 6 générateurs × 4,15 Mo × 50 fps ≈ 1,24 Go/s de memcpy en
# Python pour une image CONSTANTE — et ils ne tenaient que 37,5 fps sur les 50 demandés (le fil,
# lui, restait nominal : libmtl répète la trame, cf. mtl_rx.c « trame (gelée) émise »).
#
# On mémorise donc, PAR ADRESSE de slot (robuste : aucune hypothèse sur la géométrie du ring), la
# signature du contenu écrit + un ÉCHANTILLON d'octets témoins. La cadence de grains ne change PAS
# (50 grains/s, indices qui avancent normalement) : seul le remplissage redondant disparaît, après
# une rotation complète du ring.
#
# GARDE (pas d'échec silencieux) : l'échantillon est REVÉRIFIÉ à chaque réutilisation. Si MXL
# réinitialisait un slot entre deux grains, on le VERRAIT — on remplirait normalement et on le
# dirait une fois dans les logs — au lieu d'émettre du vide en silence.
_TXGEN_ECHANT_N = 64                 # octets témoins par slot (comparaison négligeable par trame)
_TXGEN_MOTIFS_DYNAMIQUES = ("moving", "field_test")   # contenu different à chaque trame
_txgen_ring_volatil = set()          # slots TX ayant déjà signalé un ring non conservatif


def _grain_echantillon(view):
    pas = max(1, view.size // _TXGEN_ECHANT_N)
    return bytes(view[::pas][:_TXGEN_ECHANT_N])


def _grain_empreinte(view):
    """Empreinte du grain ENTIER. Coûteuse (une lecture pleine trame) — réservée à la
    VÉRIFICATION PROFONDE, faite UNE seule fois par slot (cf. _grain_reutilisable)."""
    return hashlib.blake2b(view, digest_size=16).digest()


def _grain_reutilisable(cache, view, sig, idx):
    """Le slot qu'on vient d'ouvrir contient-il DÉJÀ exactement ce contenu ?

    Deux niveaux de preuve, parce que sauter le remplissage repose sur une HYPOTHÈSE (le ring
    conserve les octets entre deux rotations) et qu'une hypothèse fausse émettrait du vide en
    silence :
      1. À la PREMIÈRE réutilisation d'un slot : comparaison de l'empreinte du grain ENTIER. Coût
         payé une fois par slot (≈ 8 slots par flux), donc négligeable, et il PROUVE l'hypothèse sur
         le matériel réel plutôt que de la supposer.
      2. Ensuite : 64 octets témoins à chaque trame — assez pour voir un slot réinitialisé, pour un
         coût nul.
    Échec de l'un ou l'autre ⇒ on remplit normalement ET on le DIT (une fois par slot TX)."""
    ent = cache.get(view.ctypes.data)
    if ent is None or ent[0] != sig:
        return False
    sig_mem, echant, empreinte = ent
    if empreinte is not None:                     # vérification PROFONDE, une seule fois
        ok = (_grain_empreinte(view) == empreinte)
        if ok:
            cache[view.ctypes.data] = (sig_mem, echant, None)   # prouvé : on passe au contrôle léger
        else:
            _signaler_ring_volatil(idx, "empreinte du grain entier")
            return False
        return True
    if _grain_echantillon(view) != echant:
        _signaler_ring_volatil(idx, "octets témoins")
        return False
    return True


def _signaler_ring_volatil(idx, quoi):
    if idx not in _txgen_ring_volatil:
        _txgen_ring_volatil.add(idx)
        print("txgen idx={}: le ring MXL ne conserve PAS le contenu entre deux grains ({} en "
              "défaut) — remplissage systématique, optimisation mire statique inopérante"
              .format(idx, quoi), flush=True)


def _noter_grain(cache, view, sig):
    cache[view.ctypes.data] = (sig, _grain_echantillon(view), _grain_empreinte(view))


def _fill_grain_planes(view, lay, y, cb, cr):
    """Écrit Y|Cb|Cr (numpy _DT) dans la vue uint8 d'un grain MXL (zéro-copie, planar contigu)."""
    yb, uvb = lay["y"], lay["uv"]
    view[0:yb].view(_DTNP).reshape(lay["h"], lay["w"])[:] = y
    view[yb:yb + uvb].view(_DTNP).reshape(lay["uv_h"], lay["uv_w"])[:] = cb
    view[yb + uvb:yb + 2 * uvb].view(_DTNP).reshape(lay["uv_h"], lay["uv_w"])[:] = cr


# Résolution courante par slot (pour la simu + la taille IDENT), suit le SDP live.
_slot_res = [[WIDTH, HEIGHT] for _ in range(N_VIDEO)]

# `numero` = le numéro que voit l'OPÉRATEUR (1-based), publié À CÔTÉ de `idx` qui reste
# l'indice de tableau (et la clé machine de /nmos/subscribe). Sans lui, chaque lecteur — l'UI,
# un diagnostic, un humain qui lit ce JSON — doit se souvenir d'ajouter 1 : 26 sites le font
# dans le dépôt, et l'oublier a coûté une matinée sur « Tx1 » (l'UI disait 1, le SDP disait 0).
metrics = [{"idx": i, "numero": _num(i), "essence": "video", "fps": 0.0, "frame_index": 0,
            "mode": "init"} for i in range(N_VIDEO)]
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
    l1 = "{} · RX{}".format(HOSTNAME, _num(idx))
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
    return "/dev/shm/{}_{}_ident".format(HOSTNAME, _num(idx))


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
    """Stats du sender TX idx écrites par mtl_rx (fps réel + late = trames ayant raté leur epoch).
    `fps_source`/`repeats` : répétition de trame VISIBLE (fps mélange trames uniques et rejouées) —
    n'existent dans le fichier que pour les cibles TX VIDÉO (cf. mtl_rx.c write_stats) ; None sinon,
    à ne PAS confondre avec 0 (une source qui avance à fps_source=0 n'a émis QUE des répétitions)."""
    try:
        with open("/tmp/mtl_tx{}.json".format(idx)) as f:
            d = json.load(f)
        fps_source = d.get("fps_source")
        return (float(d.get("fps", 0.0)), int(d.get("late", 0)),
                float(fps_source) if fps_source is not None else None,
                int(d["repeats"]) if "repeats" in d else None)
    except Exception:
        return None, None, None, None


# ─── :8080 métriques (format get_metrics) ────────────────────────────
# ─── Présence signal (audit A5) : noir / gel / silence ────────────────────────────────
# Détection de CONTENU — l'existant (rx/tx_stalled, watchdog) ne couvre que le TRANSPORT
# (frame_index/head figé) : une image noire ou figée dont les grains avancent est invisible.
# Coût borné : SIGNAL_ROWS lignes du plan Y (≈60 Ko) par flux toutes les SIGNAL_SAMPLE_S —
# négligeable devant la bande passante mémoire des nœuds (memory-bound). Exposé :8080
# (`signal` par entrée receivers[]/senders[]) ; l'orchestrateur (metrics.py) alerte à transition.
SIGNAL_SAMPLE_S   = float(os.environ.get("SIGNAL_SAMPLE_S") or 2.0)
SIGNAL_HOLD_S     = float(os.environ.get("SIGNAL_HOLD_S") or 5.0)     # persistance noir/gel avant flag
SIGNAL_BLACK_Y    = int(os.environ.get("SIGNAL_BLACK_Y") or 0)        # 0 = auto : (16+6) << (bd-8)
SIGNAL_SILENCE_DB = float(os.environ.get("SIGNAL_SILENCE_DB") or -60.0)
SIGNAL_SILENCE_S  = float(os.environ.get("SIGNAL_SILENCE_S") or 10.0)
SIGNAL_ROWS       = 16                                                 # lignes Y échantillonnées
# Paliers CONTENU supplémentaires (#25) — gamut/niveaux illégaux (vidéo) + loudness R128 (audio).
SIGNAL_GAMUT_PCT  = float(os.environ.get("SIGNAL_GAMUT_PCT") or 2.0)   # % pixels hors-gamut avant flag
SIGNAL_GAMUT_TOL  = float(os.environ.get("SIGNAL_GAMUT_TOL") or 0.02)  # tolérance RGB (fraction de plage)
# Nombre de relevés moyennés pour le % hors-gamut. Nécessaire depuis que l'origine des lignes
# tourne (cf. _sig_video_probe) : sans lissage, un défaut localisé ferait osciller la mesure d'un
# relevé à l'autre. 8 relevés ≈ 16 s à la cadence vidéo — assez pour stabiliser, assez court pour
# rester réactif face au délai de persistance (SIGNAL_HOLD_S).
SIGNAL_GAMUT_AVG  = max(1, int(os.environ.get("SIGNAL_GAMUT_AVG") or 8))
SIGNAL_LOUD_MS    = float(os.environ.get("SIGNAL_LOUD_MS") or 400.0)   # fenêtre loudness momentané (R128 « M »)
SIGNAL_LOUD_TARGET= float(os.environ.get("SIGNAL_LOUD_TARGET") or -23.0)  # cible EBU R128 (LUFS)
SIGNAL_LOUD_TOL   = float(os.environ.get("SIGNAL_LOUD_TOL") or 2.0)    # ± LU avant flag « hors cible »
# SATURATION (écrêtage). Le pic est DÉJÀ calculé pour le silence : le comparer à l'autre extrémité
# ne coûte rien. Contrairement au loudness, un écrêtage n'a pas besoin d'un programme — c'est un
# défaut à l'instant où il se produit.
# ⚠ C'est un HOLD, pas un état instantané, et c'est indispensable : la sonde ne regarde que 400 ms
# toutes les 2 s (20 % de l'audio), et l'orchestrateur exige 3 relevés concordants avant d'alerter.
# Un drapeau vrai « seulement pendant la fenêtre où l'écrêtage a été vu » ne survivrait jamais au
# débounce. On tient donc le drapeau HOLD secondes après le dernier écrêtage constaté.
# ⚠ Détection par ÉCHANTILLONNAGE, jamais exhaustive : un écrêtage bref tombant entre deux fenêtres
# n'est pas vu. Attraper tout imposerait un suivi de pic continu dans mtl_rx, pas dans cette sonde.
SIGNAL_CLIP_DB    = float(os.environ.get("SIGNAL_CLIP_DB") or -0.1)    # dBFS : pic ≥ seuil = écrêtage
SIGNAL_CLIP_HOLD_S= float(os.environ.get("SIGNAL_CLIP_HOLD_S") or 30.0) # maintien du drapeau
# Cadence de la sonde AUDIO, distincte de la vidéo. Elle doit être < A_RING (100 ms) pour que la
# lecture séquentielle ne perde jamais d'échantillon : à 20 Hz on lit 50 ms d'avance sur un tampon
# qui en tient 100, ce qui laisse de la marge pour un ordonnancement irrégulier. Coût mesuré sur le
# nœud : 46 µs par flux et par relevé, sans copie — 3 % d'un cœur à 64 flux.
SIGNAL_AUDIO_S    = float(os.environ.get("SIGNAL_AUDIO_S") or 0.05)

_signal_rx = {}      # idx slot RX → {"black","frozen"[,"silence"]} — lu par MetricsHandler
_signal_tx = {}      # idx slot TX → {"black","frozen"} (contenu du shm d'entrée câblé)
# Sondes ACTIVES par slot RX : {idx: {"video":bool, "gamut":bool, "audio":bool}}. Poussé par
# l'orchestrateur depuis les réglages par source (POST /probes). Slot ABSENT = tout est calculé —
# le défaut ne doit jamais éteindre une surveillance par omission.
_sig_probes = {}
_sig_lock  = threading.Lock()
_sig_state = {}      # ("rx"|"tx", idx) → état interne {name, vr, ar, vidx, crcs, *_since, fmt}


# DÉCROCHAGE DE GÉNÉRATION (le sampler est un lecteur LONGUE DURÉE). Quand un flux MXL est
# détruit puis recréé SOUS LE MÊME NOM — ce qui arrive sans redémarrer quoi que ce soit, dès qu'on
# change la source d'un slot : mtl_rx fusionne/dégroupe ses sessions (deux slots sur le même
# multicast = UNE session à N cibles) et recrée les flux concernés — notre Reader reste collé à la
# génération MORTE. Ses grains restent LISIBLES (index figé) : aucun SIGBUS, aucune exception,
# donc la réouverture « sur exception » ci-dessous ne se déclenche JAMAIS. Le sampler renvoie alors
# « inconnu » à vie sur ce slot : plus aucune alerte noir/gel, et aucune résolution non plus
# (mesuré en prod le 2026-07-26 : 3 slots décrochés pendant des heures, image parfaite à l'écran).
# Détecteur conforme à la spec : `now_tai − lastWriteTime` croît alors que l'horloge avance
# (maintenu par mtl_rx sur la vidéo ; l'audio n'a que head_index, cf. _sig_audio_probe).
# Parade : lâcher NOTRE handle → garbage_collect() → rouvrir. Un flux encore référencé DANS notre
# Instance n'est pas collecté et la réouverture retombe sur l'orphelin → au 2ᵉ échec consécutif on
# rouvre sur une Instance DÉDIÉE (cache vierge, résolution sur disque).
SIGNAL_STALE_MS = float(os.environ.get("SIGNAL_STALE_MS") or 5000.0)


def _sig_close(st):
    for k in ("vr", "ar"):
        try:
            if st.get(k) is not None:
                st[k].close()
        except Exception:
            pass
        st[k] = None
    own = st.pop("own_inst", None)      # Instance dédiée (escalade) : fermée AVEC ses readers,
    if own is not None:                 # sinon elle retient la génération qu'on veut lâcher.
        try: own.close()
        except Exception: pass


def _sig_instance(st):
    """Instance sur laquelle (r)ouvrir les readers de ce slot : la globale, ou une Instance DÉDIÉE
    au-delà de 2 décrochages consécutifs (cf. commentaire SIGNAL_STALE_MS). Vidéo et audio d'un
    même slot partagent l'Instance : c'est la même source, et une seule Instance de secours suffit."""
    if max(st.get("stale_v", 0), st.get("stale_a", 0)) < 2:
        return _mxl()
    own = st.get("own_inst")
    if own is None:
        own = bobimxl.Instance(_MXL_DOMAIN)
        st["own_inst"] = own
    return own


def _sig_reopen(st, name, motif, kind="v"):
    """Reconnexion d'un lecteur décroché : close (NOS handles) → GC → réouverture paresseuse au
    passage suivant. Tracé : c'est un événement rare qui signe une recréation de flux amont."""
    key = "stale_" + kind
    st[key] = st.get(key, 0) + 1
    _sig_close(st)
    try:
        _mxl().garbage_collect()
    except Exception:
        pass
    st.pop("vidx", None); st.pop("crcs", None); st.pop("ahead", None)
    st.pop("black_since", None); st.pop("frozen_since", None); st.pop("gamut_since", None)
    st.pop("sil_since", None); st.pop("ahead_t", None)
    n = st[key]
    if n <= 3 or n % 100 == 0:      # source réellement morte → on retente sans inonder le journal
        print("sampler: {} — lecteur PÉRIMÉ ({}), reconnexion (tentative {}){}".format(
            name, motif, n, " → Instance DÉDIÉE" if n >= 2 else ""), flush=True)


class _SkipGamut(Exception):
    """Sonde gamut désarmée sur ce slot — sortie propre du bloc de calcul, pas une erreur."""


def _gamut_arme(st):
    """Le gamut est-il armé sur le slot que décrit `st` ? Le gamut est la partie CHÈRE de la sonde
    vidéo (0,59 ms par relevé contre 46 µs pour l'audio) : il doit pouvoir s'éteindre SEUL, sans
    désarmer noir et gel qui, eux, ne coûtent qu'un CRC et une moyenne."""
    idx = st.get("slot_idx")
    if idx is None:
        return True
    with _sig_lock:
        pr = _sig_probes.get(idx)
    return True if pr is None else bool(pr.get("gamut", True))


# ⚠ LA MATRICE NE DOIT PAS ÊTRE CODÉE EN DUR, ET ELLE L'ÉTAIT. `_gamut_illegal_pct` calculait
# le RVB avec les coefficients BT.709 en littéral (1.5748 / 0.1873 / 0.4681 / 1.8556) et ne
# lisait JAMAIS la colorimétrie déclarée par la source. Sur un flux BT.601 ou BT.2020, le
# pourcentage publié était donc faux — et faux EN SILENCE, sans qu'aucun garde-fou ne bronche,
# alors que `gamut_pct` est montré à l'exploitant et sert à régler un seuil.
# C'est une règle explicite du projet (« AUCUNE constante de format vidéo codée en dur ») que
# le reste du produit respecte : le scope dérive ses coefficients de (Kr,Kb) et REFUSE de
# tracer quand la colorimétrie manque.
KRKB_SIG = {"601": (0.299, 0.114), "bt601": (0.299, 0.114), "smpte170m": (0.299, 0.114),
            "709": (0.2126, 0.0722), "bt709": (0.2126, 0.0722),
            "2020": (0.2627, 0.0593), "bt2020": (0.2627, 0.0593),
            "bt2100": (0.2627, 0.0593)}


def _colorimetrie_declaree(idx):
    """Colorimétrie DÉCLARÉE par la source pour le récepteur `idx`, ou None.

    Lue dans le SDP que NMOS a déposé — c'est la seule source d'autorité disponible ici. Rendre
    None est un REFUS : mieux vaut ne pas publier de pourcentage que d'en publier un calculé
    avec la mauvaise matrice, parce que le second a exactement l'aplomb du premier."""
    path = os.path.join(SDP_DIR, "nmos_recv_v_{}.sdp".format(idx))
    try:
        mt = os.path.getmtime(path)
    except OSError:
        return None
    ent = _COLOR_CACHE.get(idx)
    if ent and ent[0] == mt:
        return ent[1]
    val = None
    try:
        with open(path) as f:
            m = re.search(r"colorimetry=([A-Za-z0-9]+)", f.read())
        if m:
            val = m.group(1)
    except OSError:
        val = None
    _COLOR_CACHE[idx] = (mt, val)
    return val


_COLOR_CACHE = {}


def _gamut_illegal_pct(y, u, v, bd, colorimetrie=None):
    """Fraction (%) de pixels hors-gamut RGB legal sur des lignes Y/Cb/Cr échantillonnées (10-bit
    limited, matrice BT.709 HD). Couvre AUSSI les niveaux illégaux (super-noir/super-blanc → RGB<0
    ou >1). `u`/`v` = plans chroma 4:2:2 (demi-largeur) suréchantillonnés ×2 pour matcher Y. Coût
    borné : mêmes SIGNAL_ROWS lignes que noir/gel. Retourne un float (0..100)."""
    s = float((235 - 16) << (bd - 8))                      # plage luma legal (876 en 10-bit)
    cmid = float(128 << (bd - 8))                          # milieu chroma (512 en 10-bit)
    crange = float((240 - 16) << (bd - 8))                 # plage chroma pleine (896 en 10-bit)
    yb = float(16 << (bd - 8))                             # noir legal (64 en 10-bit)
    yn = (y.astype(np.float32) - yb) / s
    cb = (u.astype(np.float32) - cmid) / crange
    cr = (v.astype(np.float32) - cmid) / crange
    # Coefficients DÉRIVÉS de (Kr,Kb), jamais tabulés. `2·(1−Kr)` etc. : c'est la définition,
    # et elle vaut pour les trois colorimétries au lieu d'une seule.
    kk = KRKB_SIG.get(str(colorimetrie or "").strip().lower())
    if not kk:
        return None                                    # REFUS, pas un défaut silencieux
    kr, kb = kk
    kg = 1.0 - kr - kb
    if kg <= 0:
        return None
    r = yn + (2 * (1 - kr)) * cr
    g = yn - (2 * (1 - kb) * kb / kg) * cb - (2 * (1 - kr) * kr / kg) * cr
    b = yn + (2 * (1 - kb)) * cb
    tol = SIGNAL_GAMUT_TOL
    bad = ((r < -tol) | (r > 1 + tol) | (g < -tol) | (g > 1 + tol) |
           (b < -tol) | (b > 1 + tol))
    return 100.0 * float(bad.mean()) if bad.size else 0.0


# ─── Loudness R128 (BS.1770 K-weighting) via FFT ───────────────────────────────────────
# numpy seul (pas de scipy) : on n'a pas besoin du signal filtré, seulement de son mean-square
# (l'énergie), donc on applique |H_K(f)|² à la DSP et on somme (Parseval). Exact à l'énergie près
# des effets de bord (négligeables sur une fenêtre 400 ms). Coefficients 48 kHz (audio site fixe).
_KW_CACHE = {}

def _kweight_pw(n):
    """Poids spectraux |H_K(f)|² × facteur one-sided pour une rfft de taille `n` @48 kHz (caché)."""
    pw = _KW_CACHE.get(n)
    if pw is not None:
        return pw
    f = np.fft.rfftfreq(n, 1.0 / 48000.0)
    w = 2.0 * np.pi * f / 48000.0
    z1 = np.exp(-1j * w); z2 = z1 * z1
    # Étage 1 : shelving haute fréquence (pré-filtre tête) — coeffs BS.1770 @48 kHz.
    h1 = (1.53512485958697 - 2.69169618940638 * z1 + 1.19839281085285 * z2) / \
         (1.0 - 1.69065929318241 * z1 + 0.73248077421585 * z2)
    # Étage 2 : passe-haut RLB.
    h2 = (1.0 - 2.0 * z1 + 1.0 * z2) / (1.0 - 1.99004745483398 * z1 + 0.99007225036621 * z2)
    mag2 = np.abs(h1 * h2) ** 2
    one = np.full(mag2.shape, 2.0)                 # bins repliés comptés ×2…
    one[0] = 1.0
    if n % 2 == 0:
        one[-1] = 1.0                              # …sauf DC et Nyquist
    pw = mag2 * one
    _KW_CACHE[n] = pw
    return pw

def _loudness_lufs(block):
    """Loudness momentané R128 (LUFS) d'un bloc audio (n, channels) float32, canaux L/R (poids 1).
    Retourne un float (LUFS) ou None si bloc trop court."""
    n = block.shape[0]
    if n < 1024:
        return None
    pw = _kweight_pw(n)
    z = 0.0
    for ch in range(min(2, block.shape[1])):       # programme stéréo L/R (poids 1.0 chacun)
        x = np.ascontiguousarray(block[:, ch])
        X = np.fft.rfft(x)
        z += float(np.sum((np.abs(X) ** 2) * pw)) / (float(n) * float(n))  # mean-square K-pondéré
    if z <= 1e-12:
        return -70.0                               # plancher de gate absolu R128
    return -0.691 + 10.0 * float(np.log10(z))


def _sig_video_probe(st, name, now, idx=None):
    """Sonde noir/gel/gamut du flux MXL vidéo `name` → dict {black,frozen,gamut,gamut_pct} ou None
    si illisible OU si
    aucun grain neuf depuis le dernier passage (transport figé = déjà couvert par rx/tx_stalled,
    on ne double pas l'alarme). Reader rouvert sur exception (flux recréé par le producteur —
    motif multiview) ; garbage_collect pour se rattacher à la génération vivante."""
    if idx is not None:
        st["sig_idx"] = idx           # pour retrouver le SDP, donc la colorimétrie déclarée
    try:
        r = st.get("vr")
        if r is None:
            r = bobimxl.Reader(_sig_instance(st), name)
            st["vr"] = r
            st.pop("vidx", None); st.pop("crcs", None); st.pop("fmt", None)
            st.pop("black_since", None); st.pop("frozen_since", None)
        # DÉCROCHAGE : lastWriteTime figé alors que l'horloge avance ⇒ on lit une génération morte
        # (ou la source s'est arrêtée — dans les deux cas rouvrir est le bon réflexe et sans effet
        # de bord). Testé AVANT la lecture : sur l'orphelin, get_latest() rend un grain et masque
        # tout. Cf. SIGNAL_STALE_MS.
        _lw = r.last_write_time()
        if _lw and (bobimxl.now_tai() - _lw) / 1e6 > SIGNAL_STALE_MS:
            _sig_reopen(st, name, "aucune écriture depuis %.1f s"
                        % ((bobimxl.now_tai() - _lw) / 1e9), "v")
            return None
        g = r.get_latest()
        if g is None:
            return None
        gidx, _gi, view = g
        prev_idx = st.get("vidx")
        st["vidx"] = gidx
        if prev_idx is not None and gidx != prev_idx:
            st["stale_v"] = 0        # grain neuf → lecteur sain (désarme l'escalade)
        if prev_idx is None or gidx == prev_idx:
            # 1er passage (pas de référence) ou aucun grain neuf → état contenu inconnu
            st.pop("black_since", None); st.pop("frozen_since", None)
            return None
        fmt = st.get("fmt") or r.format()
        if not fmt:
            return None
        st["fmt"] = fmt
        w, h, bd = int(fmt["width"]), int(fmt["height"]), int(fmt.get("bit_depth") or 8)
        dt = np.uint16 if bd > 8 else np.uint8
        step = max(1, h // SIGNAL_ROWS)
        y = np.frombuffer(view, dtype=dt, count=w * h).reshape(h, w)
        rows = np.ascontiguousarray(y[::step])
        mean = float(rows.mean())
        crc = zlib.crc32(rows.tobytes())
        # Gamut / niveaux illégaux (#25) : lire les MÊMES lignes des plans Cb/Cr (4:2:2, demi-
        # largeur, hy=1), suréchantillonner ×hx pour matcher Y, matricer BT.709 → % hors-gamut RGB.
        # Isolé dans son propre try : un plan chroma illisible ne doit pas fermer le reader vidéo.
        gam_pct = None
        try:
            if not _gamut_arme(st):
                raise _SkipGamut            # sonde désarmée sur ce slot : on ne calcule pas
            hx, hy = _CHROMA.get(str(fmt.get("chroma") or "422"), (2, 1))
            cw, ch_ = w // hx, h // hy
            it = np.dtype(dt).itemsize
            u = np.frombuffer(view, dtype=dt, count=cw * ch_, offset=w * h * it).reshape(ch_, cw)
            v = np.frombuffer(view, dtype=dt, count=cw * ch_,
                              offset=(w * h + cw * ch_) * it).reshape(ch_, cw)
            # ── ORIGINE TOURNANTE (et NON les lignes du gel) ──────────────────────────────────
            # 17 lignes sur 1080, mais pleine largeur : 32 640 pixels. Pour estimer une PROPORTION
            # c'est un très gros échantillon (±0,15 % à 2σ sur une valeur de 2 %), et le
            # DÉCLENCHEMENT de l'alarme est déjà fiable avec une origine fixe — mesuré sur images
            # de synthèse : diffus, bandeau, synthé et logo sont tous classés correctement.
            # Ce que l'origine fixe rate, c'est la JUSTESSE du chiffre publié, parce qu'elle ne
            # croise qu'une partie d'un défaut localisé : un synthé occupant 11,1 % de l'image est
            # annoncé à 5,88 % (facteur 2), un logo de 0,58 % à 0,92 %. Or `gamut_pct` est montré à
            # l'exploitant et sert à régler un seuil — il doit dire vrai.
            # En décalant l'origine à chaque relevé, la moyenne converge vers la proportion réelle
            # de l'image ENTIÈRE (mesuré : 11,18 % contre 11,11 % de vérité), à coût identique.
            # ⚠ SURTOUT PAS sur `rows` : ces lignes-là servent au CRC du GEL. Les faire tourner
            # rendrait chaque trame différente de la précédente et le gel ne serait PLUS JAMAIS
            # détecté. Le gel garde son échantillon fixe, le gamut a le sien, mobile.
            goff = int(st.get("gam_off", 0)) % step
            st["gam_off"] = (goff + 1) % step
            yr = np.ascontiguousarray(y[goff::step])               # lignes Y (h/step, w)
            cstep = max(1, ch_ // SIGNAL_ROWS)
            coff = (goff // hy) % cstep
            ur = np.repeat(u[coff::cstep], hx, axis=1)[:, :w]      # chroma suréchantillonné → largeur w
            vr = np.repeat(v[coff::cstep], hx, axis=1)[:, :w]
            n = min(yr.shape[0], ur.shape[0], vr.shape[0])
            gam_pct = _gamut_illegal_pct(yr[:n], ur[:n], vr[:n], bd,
                                         _colorimetrie_declaree(st.get("sig_idx")))
            if gam_pct is None:
                # Colorimétrie non déclarée ou inconnue : on n'a pas de matrice, donc pas de
                # mesure. On sort SANS rien publier plutôt que de lisser un chiffre calculé
                # avec la mauvaise matrice — c'est ce que faisait la version précédente, en
                # BT.709 quoi qu'annonce la source.
                raise _SkipGamut()
            # LISSAGE, conséquence directe de l'origine tournante : d'un relevé à l'autre on ne
            # regarde plus les mêmes lignes, donc la mesure oscille (sur un synthé : 5,88 % à
            # 12,50 % d'un relevé au suivant). Seuiller là-dessus recréerait l'alarme qui bat qu'on
            # corrige partout ailleurs. La moyenne sur N relevés ramène l'écart à ±0,1 % et
            # converge vers la proportion réelle de l'image entière.
            _gh = st.setdefault("gam_hist", [])
            _gh.append(gam_pct)
            del _gh[:-SIGNAL_GAMUT_AVG]
            gam_pct = sum(_gh) / len(_gh)
        except _SkipGamut:
            gam_pct = None
        except Exception:
            gam_pct = None
        thr = SIGNAL_BLACK_Y or ((16 + 6) << (bd - 8))    # noir nominal 16 (8 bits) + marge
        if mean < thr:
            st.setdefault("black_since", now)
        else:
            st.pop("black_since", None)
        # Gel : crc identique à l'un des 2 précédents (2 : un flux ENTRELACÉ alterne les champs —
        # comparer au seul dernier crc raterait un gel dont les 2 champs diffèrent) sur des grains
        # QUI AVANCENT, pendant SIGNAL_HOLD_S.
        crcs = st.get("crcs") or []
        if crc in crcs:
            st.setdefault("frozen_since", now)
        else:
            st.pop("frozen_since", None)
        st["crcs"] = (crcs + [crc])[-2:]
        if gam_pct is not None and gam_pct >= SIGNAL_GAMUT_PCT:
            st.setdefault("gamut_since", now)
        else:
            st.pop("gamut_since", None)
        res = {"black":  ("black_since" in st and now - st["black_since"] >= SIGNAL_HOLD_S),
               "frozen": ("frozen_since" in st and now - st["frozen_since"] >= SIGNAL_HOLD_S)}
        if gam_pct is not None:
            res["gamut"] = ("gamut_since" in st and now - st["gamut_since"] >= SIGNAL_HOLD_S)
            res["gamut_pct"] = round(gam_pct, 2)
        return res
    except Exception:
        _sig_close(st)
        try:
            _mxl().garbage_collect()
        except Exception:
            pass
        return None


def _sig_audio_probe(st, name, now):
    """Sonde silence du flux MXL audio `name` → bool ou None (illisible / head figé — l'absence
    de flux est un problème de transport, déjà couvert). Fraîcheur via head_index (lastWriteTime
    jamais bumpé par les writers audio, cf. multiview)."""
    try:
        ar = st.get("ar")
        if ar is None:
            ar = bobimxl.AudioReader(_sig_instance(st), name)
            st["ar"] = ar
            st.pop("ahead", None); st.pop("sil_since", None); st.pop("ahead_t", None)
        head = int(ar.head_index())
        prev = st.get("ahead")
        st["ahead"] = head
        if prev is None or head < 0 or head == prev:
            st.pop("sil_since", None)
            # Même décrochage que la vidéo, mais l'audio n'a PAS de lastWriteTime (les writers ne le
            # bumpent pas) : le seul signe est un head_index qui ne bouge plus. Au-delà du seuil, on
            # reconnecte (une source réellement muette a quand même un head qui AVANCE — le writer
            # comble en silence ; un head figé n'est donc pas « du silence », c'est un décrochage).
            _t0 = st.setdefault("ahead_t", now)
            if now - _t0 > SIGNAL_STALE_MS / 1000.0:
                _sig_reopen(st, name, "head figé %d depuis %.1f s" % (head, now - _t0), "a")
            return None
        st["ahead_t"] = now
        st["stale_a"] = 0            # head qui avance → lecteur sain (désarme l'escalade)
        # ── LECTURE SÉQUENTIELLE, SANS TROU ───────────────────────────────────────────────────
        # « Ne rien laisser passer quand la saturation est armée » n'est pas une question de
        # cadence mais de CONTINUITÉ : on suit un curseur et on lit TOUT ce qui est arrivé depuis
        # le relevé précédent (read_from est fait pour ça, cf. son docstring). Un pic peut tenir en
        # un seul échantillon — une fenêtre « la plus récente » en raterait par construction.
        #
        # ⚠ L'ancienne version demandait 400 ms (fenêtre R128 « M ») à `read_latest` sur un ring qui
        # n'en contient que 100 (A_RING = 100 chunks de 1 ms). La lecture échouait, la sonde sortait
        # par `r is None`, et NI le silence NI le loudness n'ont jamais été publiés — vérifié en
        # prod : 12 sessions audio vivantes à 1000 chunks/s, `silence` absent des 15 slots qui
        # publient un signal. Les 400 ms n'existaient que pour le loudness, retiré des alarmes.
        nres = head - int(st["acur"]) if st.get("acur") is not None else 0
        cur = st.get("acur")
        if cur is None or nres <= 0 or nres > A_RING:
            # Premier passage, ou retard supérieur à la profondeur du ring : les échantillons
            # manquants sont ÉCRASÉS, on ne peut pas les inventer. On le DIT (un trou silencieux
            # ferait croire à une couverture qu'on n'a pas) et on repart du présent.
            if cur is not None and nres > A_RING:
                st["gap"] = int(st.get("gap", 0)) + 1
                print("sampler: {} — {} échantillon(s) NON ANALYSÉS (retard > ring de {} ms) : "
                      "couverture incomplète".format(name, nres - A_RING, A_RING), flush=True)
            st["acur"] = head
            return None
        r = ar.read_from(cur, min(nres, A_RING))
        if r is None or not r.size:
            # Échec de lecture : ne JAMAIS sortir en silence — c'est ce qui a masqué le bug
            # ci-dessus pendant toute la vie de cette sonde.
            if not st.get("rd_warn"):
                st["rd_warn"] = True
                print("sampler: {} — lecture audio IMPOSSIBLE ({} samples depuis {}) : silence et "
                      "saturation NE SERONT PAS calculés".format(name, min(nres, A_RING), cur),
                      flush=True)
            return None
        st.pop("rd_warn", None)
        st["acur"] = cur + int(r.shape[0])
        # Pic sans tableau temporaire : `np.abs(r)` allouerait une copie complète à chaque relevé
        # (mesuré sur le nœud : 4,2× le coût du calcul utile).
        peak_db = 20.0 * float(np.log10(max(max(float(r.max()), -float(r.min())), 1e-6)))
        if peak_db < SIGNAL_SILENCE_DB:
            st.setdefault("sil_since", now)
        else:
            st.pop("sil_since", None)
        silence = "sil_since" in st and now - st["sil_since"] >= SIGNAL_SILENCE_S
        # Saturation : même pic, autre extrémité. GLOBAL sur les 8 canaux, comme le silence — une
        # analyse par canal suppose de savoir QUELS canaux sont censés porter du son, ce qui est une
        # question d'intention et fera l'objet d'un plugin dédié (décision 2026-07-28).
        if peak_db >= SIGNAL_CLIP_DB:
            st["clip_t"] = now
        _ct = st.get("clip_t")
        out = {"silence": silence,
               "clip": bool(_ct is not None and now - _ct < SIGNAL_CLIP_HOLD_S),
               "peak_db": round(peak_db, 1)}
        # Loudness RETIRÉ : il n'a de sens que rapporté à un PROGRAMME (début, fin, intégré R128),
        # la fenêtre est désormais variable, et sa FFT coûtait plus cher que tout le reste de la
        # sonde. Un plugin dédié fera la mesure, armé au début d'un programme et coupé à la fin.
        return out
    except Exception:
        _sig_close(st)
        try:
            _mxl().garbage_collect()
        except Exception:
            pass
        return None


def _sig_drop(key):
    st = _sig_state.pop(key, None)
    if st:
        _sig_close(st)


_sig_bus = threading.Event()   # SIGBUS pendant une lecture MXL (flux recréé) → purge des readers


def _signal_loop():
    """Thread sampler : slots RX live (flux vidéo + audio associé) et slots TX câblés (contenu
    du shm d'entrée). Publie _signal_rx/_signal_tx (consommés par MetricsHandler)."""
    # ── DEUX CADENCES, PAS UNE ────────────────────────────────────────────────────────────────
    # L'audio doit être lu SANS TROU (cf. _sig_audio_probe) : le ring ne retient que A_RING ms, donc
    # tout intervalle plus long qu'A_RING perd des échantillons DÉFINITIVEMENT. La vidéo, elle, n'a
    # aucune raison d'aller si vite — comparer des CRC de trames et estimer un % hors-gamut à 0,5 Hz
    # suffit largement, et c'est le calcul coûteux (0,59 ms par slot contre 46 µs pour l'audio).
    # On tourne donc au pas de l'AUDIO et on ne fait la vidéo qu'un tick sur N.
    tick_s = max(0.02, min(SIGNAL_AUDIO_S, SIGNAL_SAMPLE_S))
    n_video = max(1, int(round(SIGNAL_SAMPLE_S / tick_s)))
    it = 0
    while True:
        time.sleep(tick_s)
        it += 1
        faire_video = (it % n_video) == 0
        now = time.monotonic()
        if _sig_bus.is_set():          # SIGBUS vu (producteur a recréé un flux) → repartir à neuf
            _sig_bus.clear()
            for k in list(_sig_state):
                _sig_drop(k)
            try:
                _mxl().garbage_collect()
            except Exception:
                pass
        try:
            for idx in range(N_VIDEO):
                key = ("rx", idx)
                if not _live[idx]:
                    _sig_drop(key)
                    with _sig_lock:
                        _signal_rx.pop(idx, None)
                    continue
                st = _sig_state.setdefault(key, {})
                st["slot_idx"] = idx
                # Sondes armées sur CE slot (poussées par l'orchestrateur, cf. POST /probes).
                # Absent = tout calculer : le défaut ne doit jamais éteindre une surveillance.
                with _sig_lock:
                    pr = _sig_probes.get(idx)
                v_on = True if pr is None else bool(pr.get("video", True) or pr.get("gamut", True))
                a_on = True if pr is None else bool(pr.get("audio", True))
                # Vidéo : un tick sur n_video. Le résultat précédent est CONSERVÉ entre deux
                # calculs (il décrit un état, pas un événement) — sinon les drapeaux image
                # clignoteraient au rythme de la cadence audio.
                vres = _sig_video_probe(st, "{}_{}".format(HOSTNAME, _num(idx)), now, idx) \
                    if (faire_video and v_on) else st.get("vres_last")
                if faire_video and v_on and vres is not None:
                    st["vres_last"] = vres
                sres = None
                if a_on and idx < N_AUDIO and _audio_live[idx]:
                    sres = _sig_audio_probe(st, "{}_audio_{}".format(HOSTNAME, _num(idx)), now)
                elif not a_on:
                    st.pop("acur", None)          # sonde désarmée → le curseur repart proprement
                    st.pop("sres_last", None)
                if sres is not None:
                    st["sres_last"] = sres
                else:
                    sres = st.get("sres_last")   # une lecture vide ne doit pas effacer l'état connu
                sig = {}
                if vres is not None:
                    sig.update(vres)          # black/frozen/gamut/gamut_pct
                if sres is not None:
                    sig.update(sres)          # silence/clip/peak_db
                with _sig_lock:
                    if sig:
                        _signal_rx[idx] = sig
                    else:
                        _signal_rx.pop(idx, None)
            with _tx_lock:
                # SUR LE CÂBLE RÉEL, PAS SUR `shm_in`. Un slot provisionné sans câble bascule en GÉN
                # et `shm_in` pointe alors vers le générateur INTERNE (`*_txgen_<i>`) : sonder ça
                # revient à mesurer notre propre repli, dont le noir constant est noir et figé PAR
                # CONSTRUCTION → « image noire/figée détectée » en permanence sur une sortie que
                # personne n'a câblée (12 alertes le 2026-07-26, dont une par oscillation).
                # `cable_shm` est justement le câblage réel, tenu à part de `shm_in` pour cette
                # raison. Pas de câble ⇒ pas de sortie à surveiller ⇒ aucun champ `signal` publié
                # (l'orchestrateur traite l'absence comme « inconnu » : il n'alerte ni ne résout).
                # Le RX a toujours eu sa garde équivalente (`_live[idx]`) ; seul le TX en manquait.
                tx_in = {i: (_tx[i].get("cable_shm") or "") for i in range(N_TX)
                         if _tx[i]["enabled"] and _tx[i].get("cable_shm")}
            for i in range(N_TX):
                key = ("tx", i)
                name = (tx_in.get(i) or "").rsplit("/", 1)[-1]
                if not name:
                    _sig_drop(key)
                    with _sig_lock:
                        _signal_tx.pop(i, None)
                    continue
                st = _sig_state.setdefault(key, {})
                if st.get("name") != name:      # recâblage → repartir d'un état neuf
                    _sig_close(st)
                    st = {"name": name}
                    _sig_state[key] = st
                vres = _sig_video_probe(st, name, now, idx)
                with _sig_lock:
                    if vres is not None:
                        _signal_tx[i] = dict(vres)     # black/frozen/gamut(+pct)
                    else:
                        _signal_tx.pop(i, None)
        except Exception as e:
            print("signal sampler err: {}".format(e), flush=True)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        with metrics_lock:
            recs = [dict(m) for m in metrics]
        # Présence signal (audit A5) : noir/gel/silence par slot, publiés par _signal_loop.
        with _sig_lock:
            for m in recs:
                s = _signal_rx.get(m.get("idx"))
                if s:
                    m["signal"] = dict(s)
            sig_tx = {i: dict(s) for i, s in _signal_tx.items()}
        # Receivers ANC (2110-40) : pas de simu (n'existent que si abonnés) → lus à la volée depuis
        # leur stats json (fps + frame_index + timecode ATC) et exposés en essence "anc".
        for idx in range(N_ANC):
            d = _read_stats_raw("/tmp/mtl_anc{}.json".format(idx))
            if d is None:
                continue
            rec = {"idx": idx, "numero": _num(idx), "essence": "anc", "fps": float(d.get("fps", 0.0)),
                   "frame_index": int(d.get("frame_index", 0)),
                   "mode": "mtl" if float(d.get("fps", 0.0)) > 0 else "idle"}
            if d.get("timecode"):
                rec["timecode"] = d["timecode"]; rec["df"] = bool(d.get("df"))
            recs.append(rec)
        # Receivers AUDIO (2110-30), même principe que l'ANC juste au-dessus : ils n'existent que si
        # abonnés, donc l'absence du stats json vaut « pas de session » — c'est le seul critère.
        #
        # Ils MANQUAIENT à `receivers[]`, alors que le moteur les servait bel et bien : `rl` comptait
        # 12 sessions RX pour 6 vidéo, les six flux MXL `*_audio_*` étaient écrits, mais aucun
        # consommateur du contrat :8080 ne pouvait le savoir. Conséquence directe : le voyant de
        # signal de la page Câbles affichait « pas de signal » sur des sorties audio parfaitement
        # alimentées — un indicateur qui se trompe dans le sens rassurant, le pire des deux.
        #
        # `fps` porte ici des paquets par seconde (≈1000 à ptime 1 ms), pas des trames : c'est la
        # grandeur que publie mtl_rx pour l'audio, on la relaie TELLE QUELLE (le contrat interdit de
        # dériver ici, cf. CLAUDE.md).
        for idx in range(N_AUDIO):
            d = _read_stats_raw("/tmp/mtl_a{}.json".format(idx))
            if d is None:
                continue
            recs.append({"idx": idx, "numero": _num(idx), "essence": "audio", "fps": float(d.get("fps", 0.0)),
                         "frame_index": int(d.get("frame_index", 0)),
                         "late": int(d.get("late", 0) or 0),
                         "rx_latency_ms": d.get("rx_latency_ms"),
                         "mode": "mtl" if float(d.get("fps", 0.0)) > 0 else "idle"})
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
                    tx_fps, tx_late, tx_fps_source, tx_repeats = _read_tx_stats(i)
                    with _tx_gen_lock:
                        _id_on, _id_sz = _tx_gen[i]["ident"], _tx_gen[i]["ident_size"]
                    entry = {"tx_idx": i, "idx": i, "numero": _num(i), "essence": "video",
                             "fps": tx_fps, "fps_nominal": float(t.get("fps") or 0),
                             "late": tx_late, "sdp": _tx_sdp(i, t),
                             "ident": _id_on, "ident_size": _id_sz,
                             "inputs_latency_ms": inputs_lat}
                    # Répétition de trame VISIBLE (cf. _read_tx_stats) : relayés TELS QUELS, aucune
                    # dérivation ici (repeats_per_s etc. reste au consommateur, cf. contrat CLAUDE.md).
                    if tx_fps_source is not None:
                        entry["fps_source"] = tx_fps_source
                    if tx_repeats is not None:
                        entry["repeats"] = tx_repeats
                    if i in sig_tx:
                        entry["signal"] = sig_tx[i]
                    senders.append(entry)
                # Senders AUDIO (2110-30) : un SDP par flux audio configuré (dest mcast+port).
                for ai, acfg in enumerate(t.get("audios") or []):
                    if acfg.get("mcast") and acfg.get("port"):
                        senders.append({"tx_idx": i, "idx": i, "numero": _num(i),
                                        "essence": "audio", "audio_idx": ai, "audio_numero": _num(ai),
                                        "sdp": _aud_sdp(i, ai, acfg),
                                        # ptime effectif de CETTE sortie (par-sortie ou défaut) — l'UI
                                        # peut l'offrir par-sortie ; ptime_default reste le repli global.
                                        "ptime": _tx_ptime(acfg), "ptime_default": A_PTIME_DEF,
                                        "inputs_latency_ms": inputs_lat})
                if t.get("anc_mcast") and t.get("anc_port"):
                    senders.append({"tx_idx": i, "idx": i, "numero": _num(i), "essence": "anc",
                                    "sdp": _anc_sdp(i, t),
                                    "inputs_latency_ms": inputs_lat})
        model_label, aggregate_gbps = _nic_model(IFACE)
        hw_q = _nic_hw_queues(IFACE)
        # Stats PAR PORT physique (multi-NIC). Débit mesuré par iface (ethtool -S, cache par port) ;
        # files RX/TX = dernière allocation du config (`_ports_alloc`). L'agrégat NIC (`rx_gbps`/
        # `tx_gbps`) devient la SOMME des ports — la tuile « globale » de l'UI cessait sinon de
        # refléter le moteur (elle ne montrait que le 1ᵉʳ port). Repli mono-NIC : une entrée = IFACE.
        _qmap = {p["iface"]: p for p in _ports_alloc}
        nic_ports = []
        for _if in IFACES:
            _rxg, _txg, _cap = _nic_bps(_if)
            _q = _qmap.get(_if, {})
            _is_dpdk = _port_pmd(_if) == "dpdk"
            # Port dpdk : pas d'iface kernel → ethtool -l sans objet (et le plafond de files
            # AF-XDP ne s'applique pas au PMD DPDK).
            _hwq = None if _is_dpdk else (_nic_hw_queues(_if) if _if != IFACE else hw_q)
            _pent = {"iface": _if, "sip": _q.get("sip", ""),
                     "rx_gbps": _rxg, "tx_gbps": _txg,
                     "port_capacity_gbps": _cap, "link_up": _nic_link(_if),
                     "rx_queues": _q.get("rx_queues"), "tx_queues": _q.get("tx_queues"),
                     # Sessions AF-XDP LIVE sur ce port (exact, fan-out compris) + plafond HW du port.
                     "active": _xdp_active_per_iface.get(_if, 0),
                     "hw_max_combined": (_hwq["max"] if _hwq else None),
                     "primary": _if in _auto_ports,
                     # Ce que le SDP de ce port ANNONCE, et si la mécanique qui le tient est là.
                     # Sans ces deux champs, une déclaration non tenable n'est visible nulle part
                     # (c'est ainsi qu'on a annoncé narrow en AF-XDP pendant des mois).
                     "profile": _port_profile_effectif(_if),
                     "pacing_hw": _pacing_materiel(_if)}
            if _is_dpdk:
                # Clés ADDITIVES (les consommateurs actuels ignorent les clés inconnues) :
                # budget="dpdk" = le plafond de files AF-XDP (16/48) est SANS OBJET sur ce port ;
                # mtl_stats = relais brut du contrat /tmp/mtl_ports.json (source des rx/tx_gbps).
                _pent["pmd"] = "dpdk"
                _pent["bdf"] = _port_bdf(_if)
                _pent["budget"] = "dpdk"
                # Supervision socle narrow : sessions RL TX live / cap RL du port (la limite dure,
                # cf. docs/chantiers/DPDK_NARROW.md §7) + sessions RX (files RSS dimensionnées à la demande).
                _pent["rl_tx_cap"] = RL_TX_QUEUES_CAP if _rl_is_active() else None
                _pent["tx_sessions_active"] = _tx_active_per_iface.get(_if, 0)
                _pent["rx_sessions_active"] = _rx_active_per_iface.get(_if, 0)
                _ment = _mtl_port_entry(_if)
                if _ment:
                    _pent["mtl_stats"] = _ment
            nic_ports.append(_pent)
        port_cap = nic_ports[0]["port_capacity_gbps"] if nic_ports else 100.0
        def _sum_gbps(key):
            vals = [p[key] for p in nic_ports if p[key] is not None]
            return round(sum(vals), 2) if vals else None
        payload = {"fps": top_fps, "receivers": recs, "senders": senders,
                   "nic": {"rx_gbps": _sum_gbps("rx_gbps"), "tx_gbps": _sum_gbps("tx_gbps"),
                            "port_capacity_gbps": port_cap,
                            "aggregate_gbps": aggregate_gbps,
                            "model": model_label,
                            "ports": nic_ports,
                            "ip_warnings": _ip_warnings},
                   "xdp": {"allocated":           _rx_queues_alloc + _tx_queues_alloc,
                            "reserved":            _rx_queues_reserved + _tx_queues_reserved,
                            "active":              _xdp_sessions_active,
                            "hw_max_combined":     hw_q["max"]          if hw_q else None,
                            "hw_current_combined": hw_q["current"]       if hw_q else None,
                            "hw_xdp_available":    hw_q["xdp_available"] if hw_q else None},
                   # Socle DPDK narrow : le budget TX pertinent = sessions RL par port (cap
                   # RL_TX_QUEUES_CAP, la limite dure de la carte — docs/chantiers/DPDK_NARROW.md §7), le RX = files
                   # RSS (rx_queues_alloc). tx_dropped = sessions demandées au-delà du cap, IGNORÉES
                   # par la boucle de réconciliation → sur-capacité à surfacer côté UI. Bloc émis
                   # inconditionnellement (active=False sur un nœud af_xdp/tsc → l'UI garde la barre
                   # « Queues XDP » historique).
                   "rl": {"active":          _rl_is_active(),
                           "pacing":          (os.environ.get("MTL_PACING") or "auto").strip().lower(),
                           "tx_cap_per_port": RL_TX_QUEUES_CAP if _rl_is_active() else None,
                           "tx_sessions":     sum(_tx_active_per_iface.values()),
                           "rx_sessions":     sum(_rx_active_per_iface.values()),
                           "tx_dropped":      _tx_sessions_dropped,
                           "rx_queues_alloc": _rx_queues_alloc,
                           "tx_queues_alloc": _tx_queues_alloc}}
        # État PTP interne libmtl (mtl_ports.json:ptp, socle DPDK) → l'orchestrateur (1) construit
        # a=ts-refclk:ptp du SDP TX quand ptp4l kernel est absent (gm_identity/domain/locked) et
        # (2) alimente l'onglet « Réseau 2110 - PTP » (synced/locked + offset_ns CORRIGÉ +
        # path_delay_ns + raw_delta_ns + grandmaster).
        # Relais du dict COMPLET (synced/locked/offset_ns/path_delay_ns/raw_delta_ns/domain/
        # gm_identity) + alias gm_id (hex clock id,
        # "" si inconnu). Bloc ABSENT si le moteur ne fait pas de PTP (ENGINE_PTP off / af_xdp) →
        # payload sans "ptp" (rétro-compat : l'orchestrateur retombe sur pmc/ptp4l kernel).
        _pj = _mtl_ports_read() or {}
        _ptp = _pj.get("ptp") if isinstance(_pj, dict) else None
        if isinstance(_ptp, dict):
            _ptp = dict(_ptp)
            _ptp.setdefault("gm_id", _ptp.get("gm_identity", ""))
            payload["ptp"] = _ptp
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

        if route == "/pin":                      # ── Épinglage de port d'un slot (multi-NIC, À CHAUD)
            role = body.get("role", "rx")
            try:
                idx = int(body.get("idx") or 0)
            except Exception:
                return self._json(400, {"error": "idx invalide"})
            ifn = (body.get("iface") or "").strip()   # "" / null → retour à la répartition auto
            if ifn and ifn not in IFACES:
                return self._json(400, {"error": "port inconnu: {}".format(ifn)})
            if role == "tx":
                if not (0 <= idx < N_TX):
                    return self._json(400, {"error": "idx TX hors limites"})
                with _tx_lock:
                    _tx[idx]["iface"] = ifn or None
            else:
                with _pins_lock:
                    if ifn:
                        RX_PINS[idx] = ifn
                    else:
                        RX_PINS.pop(idx, None)
            # _manager_loop re-tague au tour suivant → reconcile déplace la session (≤0,5 s).
            return self._json(200, {"ok": True, "iface": ifn or None,
                                    "auto": _auto_iface(idx) if not ifn else None})

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
                if "provisioned" in body: t["provisioned"] = bool(body.get("provisioned"))
                if "mcast"     in body: t["mcast"]     = body.get("mcast") or None
                if "udp_port"  in body: t["udp_port"]  = int(body.get("udp_port") or 0)
                if "mcast2"    in body: t["mcast2"]    = body.get("mcast2") or None
                if "udp_port2" in body: t["udp_port2"] = int(body.get("udp_port2") or 0)
                if "pt"        in body: t["pt"]        = int(body.get("pt") or 96)
                if "iface"     in body: t["iface"]     = (body.get("iface") or "").strip() or None
                if "shm_in"    in body: t["shm_in"]    = (body.get("shm_in") or "").strip() or None
                # Rythme d'émission : 0 = émission alignée epoch (défaut) ; >0 = grille d'émission
                # décalée de N µs après l'epoch nominal (mode tranche — l'image part dès que ses
                # premières tranches sont prêtes). Timestamp RTP inchangé, TROFF déclaré au SDP.
                if "epoch_shift_us" in body:
                    try: t["epoch_shift_us"] = max(0, int(body.get("epoch_shift_us") or 0))
                    except Exception: t["epoch_shift_us"] = 0
                # BRIDAGE D'AVANCE : plafond de trames prêtes que le worker TX s'autorise devant
                # celle que la lib émet (0 = désactivé). ⚠ Ce relais est INDISPENSABLE : sans lui
                # le réglage part de l'orchestrateur, arrive ici, et n'atteint jamais mtl_rx — le
                # bridage reste silencieusement inactif (`adv_wait_ms` à 0,0 quel que soit le
                # plafond demandé). C'est la même dette que celle déjà signalée pour `ring` sur
                # les sessions TX pleine trame.
                if "advance" in body:
                    try: t["advance"] = max(0, int(body.get("advance") or 0))
                    except Exception: t["advance"] = 0
                if "publish_lead_us" in body:
                    try: t["publish_lead_us"] = max(0, int(body.get("publish_lead_us") or 0))
                    except Exception: t["publish_lead_us"] = 0
                if "serve_newest" in body:
                    try: t["serve_newest"] = 1 if int(body.get("serve_newest") or 0) else 0
                    except Exception: t["serve_newest"] = 0
                if "audios" in body:
                    t["audios"] = [{"mcast": a.get("mcast") or None,
                                    "port": int(a.get("port") or 0),
                                    "pt": int(a.get("pt") or 97),
                                    "mcast2": a.get("mcast2") or None,
                                    "port2": int(a.get("port2") or 0),
                                    # ptime PAR-SORTIE (ms) : None si non fourni → repli A_PTIME_DEF au rendu.
                                    "ptime": _coerce_ptime(a.get("ptime"))}
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
                        # ptime effectif par sortie audio (par-sortie si déclaré, sinon défaut global).
                        _acfg = (_tx[i].get("audios") or [])
                        st["tx_audio{}_ptime".format(i * N_AUD_PER_TX + ai)] = (
                            _tx_ptime(_acfg[ai]) if ai < len(_acfg) else A_PTIME_DEF)
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
                _tx_gen_apply(i)   # câbler/décâbler l'audio (ré)active un slot audio-seul (enabled)
                return self._json(200, {"ok": True})
            if essence == "data":
                # Câblage ANC indépendant : slot = index du slot TX. shm vide = décâble.
                try: i = int(body.get("slot", -1))
                except Exception: i = -1
                if not (0 <= i < N_TX):
                    return self._json(400, {"error": "slot ANC TX hors limites"})
                with _tx_lock:
                    _tx[i]["anc_cable_shm"] = (body.get("shm") or "").strip() or None
                _tx_gen_apply(i)   # câbler/décâbler l'ANC (ré)active un slot ANC-seul (enabled)
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
        if path == "/gen":          # bascule générateur simu : mire VIDÉO ou TONALITÉ audio (essence)
            if body.get("essence") == "audio":   # générateur de tonalité de la simu RX (VU-mètres)
                with _sim_tone_lock:
                    tn = _sim_tone[idx]
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
            with _ctl_lock:
                _ctl[idx]["gen"] = bool(body.get("enabled"))
                if "pattern" in body:
                    _ctl[idx]["pattern"] = str(body["pattern"])
            return self._json(200, {"ok": True})
        if path == "/probes":       # QUELLES sondes de présence signal calculer sur ce slot
            # Gating du COÛT, pas seulement de l'alerte. L'orchestrateur sait quels drapeaux
            # l'exploitant a armés par source ; sans le lui dire, le moteur calcule tout pour tout
            # le monde — 0,03 % d'un cœur par source pour le gamut, 0,09 % par flux pour l'audio.
            # Décocher doit vouloir dire « ne le calcule même pas ».
            # ⚠ DÉFAUT = TOUT CALCULER. Un moteur qui n'a jamais reçu de configuration, ou qui
            # redémarre avant que l'orchestrateur ne repousse la sienne, doit surveiller comme
            # avant — une surveillance qui s'éteint toute seule au premier redémarrage serait
            # exactement l'échec silencieux qu'on corrige partout ailleurs.
            try:
                i = int(body.get("slot", body.get("idx", -1)))
            except Exception:
                i = -1
            if not (0 <= i < N_VIDEO):
                return self._json(400, {"error": "slot hors bornes"})
            m = body.get("probes")
            with _sig_lock:
                if m is None:
                    _sig_probes.pop(i, None)          # retour au défaut : tout est calculé
                else:
                    _sig_probes[i] = {k: bool(v) for k, v in dict(m).items()
                                      if k in ("video", "gamut", "audio")}
            return self._json(200, {"ok": True, "slot": i, "probes": _sig_probes.get(i)})

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


# ─── mTLS du contrat agent (:8081) ──────────────────────────────────
# Matériel TLS bind-monté par l'orchestrateur au `docker run` (-v <hostdir>:/etc/bobi-tls:ro),
# généré et signé par le CA du contrôleur (cf. app/ca.py côté orchestrateur). Convention
# IDENTIQUE au chemin conteneur standard : /etc/bobi-tls/{cert,key,ca}.pem.
#   cert+key présents        → :8081 en HTTPS (cert serveur signé CA)
#   + ca.pem présent         → mTLS (CERT_REQUIRED : le client contrôleur doit présenter son cert)
#   absents ou wrap en échec → HTTP clair (repli rétro-compat, jamais de crash)
# Ce code est baké dans l'image (environnement isolé) : ssl stdlib uniquement, pas d'import app.ca.
_TLS_DIR  = "/etc/bobi-tls"
_TLS_CERT = os.path.join(_TLS_DIR, "cert.pem")
_TLS_KEY  = os.path.join(_TLS_DIR, "key.pem")
_TLS_CA   = os.path.join(_TLS_DIR, "ca.pem")

def _agent_tls_context():
    """Construit le SSLContext depuis /etc/bobi-tls/ si présent, sinon None (→ HTTP clair).
    Toute erreur (cert illisible…) → None (repli, jamais de crash)."""
    if not (os.path.exists(_TLS_CERT) and os.path.exists(_TLS_KEY)):
        return None, "HTTP clair"
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=_TLS_CERT, keyfile=_TLS_KEY)
        if os.path.exists(_TLS_CA):
            ctx.load_verify_locations(cafile=_TLS_CA)
            ctx.verify_mode = ssl.CERT_REQUIRED
            return ctx, "HTTPS/mTLS (client cert requis)"
        return ctx, "HTTPS (pas de CA → client non vérifié)"
    except Exception as e:
        print(f"[8081] contexte TLS invalide ({e}) → repli HTTP clair", flush=True)
        return None, "HTTP clair (repli après échec TLS)"

def _serve_agent():
    ctx, mode = _agent_tls_context()
    srv = HTTPServer(("0.0.0.0", PORT_AGENT), AgentHandler)
    if ctx is not None:
        try:
            srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
        except Exception as e:
            print(f"[{PORT_AGENT}] wrap TLS échoué ({e}) → repli HTTP clair", flush=True)
            mode = "HTTP clair (repli après échec wrap)"
    print(f"[{PORT_AGENT}] contrat agent servi en {mode}", flush=True)
    srv.serve_forever()

threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT_METRICS), MetricsHandler).serve_forever(),
                 daemon=True).start()
threading.Thread(target=_serve_agent, daemon=True).start()
threading.Thread(target=lambda: HTTPServer(("0.0.0.0", PORT_CONTROL), ControlHandler).serve_forever(),
                 daemon=True).start()


# ─── Parseur SDP minimal (ST 2110-20) ───────────────────────────────
def _sdp_leg2(txt, media="video"):
    """2ᵉ section m=<media> d'un SDP dual-leg (SMPTE 2022-7, a=group:DUP / greffe NMOS) →
    (mcast2, port2) ou None. Chaque section porte son propre c= ; on lit celui de la 2ᵉ."""
    ms = list(re.finditer(r"^m=%s\s+(\d+)\s+RTP/AVP\s+\d+" % media, txt, re.M))
    if len(ms) < 2:
        return None
    seg = txt[ms[1].start(): ms[2].start() if len(ms) > 2 else len(txt)]
    c = re.search(r"^c=IN\s+IP4\s+([0-9.]+)", seg, re.M)
    return (c.group(1), int(ms[1].group(1))) if c else None

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
    leg2 = _sdp_leg2(txt, "video")
    if leg2:
        info["mcast2"], info["port2"] = leg2
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
    info = {"port": int(m.group(1)), "pt": int(m.group(2)), "mcast": c.group(1),
            "channels": A_CHANNELS, "ptime": ptime}
    leg2 = _sdp_leg2(txt, "audio")
    if leg2:
        info["mcast2"], info["port2"] = leg2
    return info

def _audio_session(idx, info, iface=IFACE):
    """Session RX audio st30 → /dev/shm/{hn}_audio_{idx} (L24 8ch BE, écrit tel quel par mtl_rx)."""
    return _leg2({"kind": "audio", "role": "rx", "iface": iface,
            "mcast": info["mcast"], "udp_port": info["port"], "payload_type": info["pt"],
            "channels": info.get("channels", A_CHANNELS), "ptime": info.get("ptime", A_PTIME_DEF),
            "ring": A_RING, "hdr": HDR,
            "targets": [{"idx": idx, "shm": "/dev/shm/{}_audio_{}".format(HOSTNAME, _num(idx)),
                         "stats": "/tmp/mtl_a{}.json".format(idx)}]},
            iface, info.get("mcast2"), info.get("port2"))

def _derive_audio_shm(video_shm, idx=0):
    """shm vidéo câblé → shm audio associé : 'host_0' + idx=1 → 'host_audio_1'."""
    m = re.match(r"^(.*)_(\d+)$", (video_shm or "").strip())
    return "{}_audio_{}".format(m.group(1), idx) if m else None

def _audio_tx_session(idx, acfg, shm_in, iface=IFACE):
    """Session TX audio st30 : émet le shm audio d'entrée (BE passthrough) vers la dest audio."""
    return _leg2({"kind": "audio", "role": "tx", "iface": iface,
            "mcast": acfg["mcast"], "udp_port": acfg["port"], "payload_type": acfg.get("pt", 97),
            "ssrc": _ssrc("{}:tx:a:{}".format(HOSTNAME, idx)),
            "channels": A_CHANNELS, "ptime": _tx_ptime(acfg), "ring": A_RING, "hdr": HDR,
            "targets": [{"idx": idx, "shm": shm_in, "stats": "/tmp/mtl_atx{}.json".format(idx)}]},
            iface, acfg.get("mcast2"), acfg.get("port2"))


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
    info = {"port": int(m.group(1)), "pt": int(m.group(2)), "mcast": c.group(1)}
    leg2 = _sdp_leg2(txt, "video")
    if leg2:
        info["mcast2"], info["port2"] = leg2
    return info

def _anc_session(idx, info, iface=IFACE, fps=None):
    """Session RX ANC st40 → /dev/shm/{hn}_anc_{idx} (meta+udw sérialisés par mtl_rx).

    `fps` = cadence de la VIDÉO du même slot. Elle sert de `grain_rate` au flux MXL et de grille
    TAI (`s->mrate` dans mtl_rx). On ne la transmettait PAS : le moteur retombait alors sur son
    littéral `jdbl(j, "fps", 25.0)`, pensé pour le pacing d'ÉMISSION, et le flux ANC d'une entrée
    1080p50 était annoncé — et cadencé — à 25/1. Mesuré le 2026-08-07 : vidéo 50,0 grains/s,
    ANC 25,2. L'ANC d'une entrée suit la cadence de SA vidéo, pas une constante."""
    return _leg2({"kind": "data", "role": "rx", "iface": iface,
            "mcast": info["mcast"], "udp_port": info["port"], "payload_type": info["pt"],
            "fps": float(fps) if fps else FPS,
            "ring": 8, "hdr": HDR,
            "targets": [{"idx": idx, "shm": "/dev/shm/{}_anc_{}".format(HOSTNAME, _num(idx)),
                         "stats": "/tmp/mtl_anc{}.json".format(idx)}]},
            iface, info.get("mcast2"), info.get("port2"))

def _derive_anc_shm(video_shm):
    """shm vidéo câblé → shm ANC associé : 'mtl_0' → 'mtl_anc_0' (None si pas de _N final)."""
    m = re.match(r"^(.*)_(\d+)$", (video_shm or "").strip())
    return "{}_anc_{}".format(m.group(1), m.group(2)) if m else None

def _anc_tx_session(idx, t, shm_in, iface=IFACE):
    """Session TX ANC st40 : ré-émet le shm ANC d'entrée (passthrough) vers la dest ANC du slot."""
    return _leg2({"kind": "data", "role": "tx", "iface": iface,
            "mcast": t["anc_mcast"], "udp_port": t["anc_port"], "payload_type": t.get("anc_pt", 97),
            "ssrc": _ssrc("{}:tx:anc:{}".format(HOSTNAME, idx)),
            "fps": t.get("fps") or FPS, "ring": 8, "hdr": HDR,
            "targets": [{"idx": idx, "shm": shm_in, "stats": "/tmp/mtl_anctx{}.json".format(idx)}]},
            iface, t.get("anc_mcast2"), t.get("anc_port2"))


# ─── Gestionnaire de sessions central ───────────────────────────────
# UN SEUL mtl_rx multi-session (un mtl_init = un PF) sert TOUTES les sessions actives. Le manager
# recalcule l'ensemble actif (slots avec SDP, pas GÉN) et (re)lance mtl_rx avec un config JSON
# quand cet ensemble change. Les slots inactifs sont écrits en simu par leur _simu_loop.
_CONFIG_PATH = "/tmp/mtl_config.json"
_mtl_proc = None
_mtl_lock = threading.Lock()
_cur_sig = None
_live = [False] * N_VIDEO     # slot vidéo idx actuellement servi par mtl_rx ?
_audio_live = [False] * N_AUDIO   # slot audio idx servi par mtl_rx (SDP 2110-30 actif) ? — INDÉPENDANT
                                  # de la vidéo : un slot peut avoir la vidéo live SANS audio reçu.
_last_launch = 0.0            # horodatage du dernier (re)lancement de mtl_rx
_fail_streak = 0             # échecs rapides consécutifs (backoff)
_sig_changed_at = 0.0        # horodatage du dernier changement de config (debounce de relance budget)
_RELAUNCH_SETTLE_S = 3.0     # délai de stabilité config avant relance pour cause de budget de files

# ─── Watchdog RX auto-guérison (post-mortem Horace 2026-07) ────────────────────────────
# Un groupe RX abonné dont AUCUNE image n'arrive (frame_index figé) pendant _WD_STALL_S est
# « bounced » : sa session est omise du config le temps d'un cycle daemon (_WD_BOUNCE_S) puis
# ré-émise → le daemon libère la session (leave IGMP + fdir put) et la recrée (nouvelle pose fdir,
# nouveau join). Transforme les gels définitifs (« socket add flow fail » → init_hw -5 sans retry,
# join parti sur la mauvaise NIC, règle fdir perdue) en trous de quelques secondes, et sert de
# vérification post-subscribe : un abonnement qui n'accroche pas est retenté automatiquement.
# Backoff exponentiel par groupe (source réellement absente → re-tentative au plus toutes les
# _WD_MAX_S, le leave/join périodique est sans danger). MTL_RX_WATCHDOG_S=0 désactive.
_WD_STALL_S  = float(os.environ.get("MTL_RX_WATCHDOG_S") or 15.0)
_WD_BOUNCE_S = 2.0           # durée d'omission de la session (≫ période de poll du daemon)
_WD_MAX_S    = 300.0         # plafond du backoff par groupe
_WD_GRACE_S  = 20.0          # pas de bounce juste après un (re)lancement du daemon (mtl_init lent)
# ★ PLAFOND D'ESSAIS À FROID (bobi.studio) : nombre de recréations tentées sur un groupe qui n'a
# JAMAIS rien reçu. Recréer une session coûte un `rte_tm_hierarchy_commit` qui ARRÊTE ET REDÉMARRE
# LE PORT ENTIER (pacing RL) — et une session TX vivante peut y perdre ses mbufs définitivement
# (cf. patch_tx_builder_famine_recovery). MESURÉ le 2026-07-28 : six entrées sans signal faisaient
# recréer leurs sessions en boucle, et CHAQUE passage tuait la sortie TX du même port, muette
# ensuite pour toujours. Le watchdog sert à débloquer un flux qui RECEVAIT et s'est figé (join IGMP
# sur la mauvaise carte, cf. mémoire Horace) : quelques essais suffisent à couvrir ce cas au
# démarrage. Au-delà, sans le moindre grain jamais reçu, la cause est en AMONT (pas d'émetteur,
# mauvais groupe, pas de route) — insister ne répare rien et casse le voisin.
_WD_COLD_MAX_N = int(os.environ.get("MTL_RX_WATCHDOG_COLD_MAX") or 2)
_wd_state = {}               # (mcast,port) → état watchdog ; touché par le seul thread manager
_ip_check_at = 0.0           # prochain contrôle périodique _check_port_ips (manager loop)

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
        "provisioned": False,                 # slot PRÉ-CRÉÉ (session + feuille RL) même SANS source →
                                              # silencieux (0 Gb/s) jusqu'à câblage. Le contenu se route
                                              # ensuite à chaud par swap de source (mtl_rx découple
                                              # source↔session : pas de re-création → pas de dé-lock PTP).
        "fallback_mode": "black",             # repli automatique sans câble : "none"|"black"|"bars"
        "w": WIDTH, "h": HEIGHT, "fps": FPS, "bd": BIT_DEPTH, "ring": V_RING,
        "scan": "p", "field_order": "tff",   # passthrough entrelacé : suit le format de la source câblée
        "audios": [],       # liste de {mcast, port, pt, mcast2, port2} — jusqu'à 2 flux 2110-30
        "audio_cable_shm": [None] * N_AUD_PER_TX,  # shm audio CÂBLÉ par sous-flux ai (indépendant de la vidéo)
        "anc_cable_shm": None,                     # shm ANC CÂBLÉ (indépendant de la vidéo)
        "anc_mcast": None, "anc_port": 0, "anc_pt": 97,
        "anc_mcast2": None, "anc_port2": 0,  # leg1 ANC (SMPTE 2022-7)
        "iface": None,      # épinglage de port (multi-NIC) ; None = répartition auto
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

# Générateur de TONALITÉ de SIMULATION (RX) par slot vidéo — pendant audio de la mire vidéo simu.
# Quand le slot N'EST PAS servi par mtl_rx (simu) et que le générateur audio est activé (UI →
# /gen essence=audio), on écrit une sinusoïde dans son FLUX AUDIO ({hn}_audio_{idx}), que les
# consommateurs (multiview…) lisent pour leurs VU-mètres. Même forme/ruptage que _tx_tone.
_sim_tone = [_default_tone() for _ in range(N_VIDEO)]
_sim_tone_lock = threading.Lock()


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
                    if lat is None:
                        # Âge aberrant = flux recréé sous le même nom, notre Reader est resté sur la
                        # génération morte (aucune exception : ses grains restent lisibles). Sans ce
                        # drop, la latence de ce slot restait « inconnue » à VIE.
                        _drop(nm)
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
    propre. Coût : refaute ptp4l ~15 s (auto-recovery), acceptable au (re)lancement.
    Multi-NIC : purge CHAQUE PF média (sinon une 2ᵉ NIC garde un XDP résiduel → init en boucle).
    Ports dpdk exclus : plus d'iface kernel (vfio-pci) → rien à détacher."""
    for nic in _AFXDP_IFACES:
        try:
            subprocess.run(["ip", "link", "set", nic, "xdp", "off"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
        except Exception as e:
            print("xdp off échoué ({}):".format(nic), e, flush=True)


def _flush_ntuple():
    """Purge les règles ntuple (flow director) résiduelles de l'interface AVANT chaque (re)lancement
    de mtl_rx. Un arrêt NON gracieux (SIGKILL via `docker rm -f`, ou crash) laisse les règles fdir
    sur le MATÉRIEL (elles survivent au conteneur) ; la création d'un nouveau flow pour le même
    5-tuple échoue alors (« socket add flow fail » → init_hw fail -5) → session muette sans retry.
    On repart d'une table de flow propre — mtl_init réinstalle les règles voulues. Sûr : l'interface
    (PF E810) est dédiée à MTL sur ce nœud. Multi-NIC : purge CHAQUE PF média.
    Ports dpdk exclus : ethtool sans objet sur un port vfio (fdir géré par le PMD ice DPDK)."""
    for nic in _AFXDP_IFACES:
        try:
            out = subprocess.run(["ethtool", "-n", nic], capture_output=True, text=True, timeout=5).stdout
            ids = re.findall(r"Filter:\s*(\d+)", out)
            for rid in ids:
                subprocess.run(["ethtool", "-N", nic, "delete", rid],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            if ids:
                print("ntuple purge ({}): {} règle(s) résiduelle(s) supprimée(s) avant mtl_init".format(
                    nic, len(ids)), flush=True)
        except Exception as e:
            print("ntuple purge échouée ({}):".format(nic), e, flush=True)


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
        # Multi-NIC : restreindre RSS sur CHAQUE PF média (chacune peut porter du PTP en coexistence).
        # Ports dpdk exclus : pas d'iface kernel, le PTP du nœud vit sur un AUTRE port (Phase 1).
        for nic in _AFXDP_IFACES:
            try:
                rc = subprocess.run(["ethtool", "-X", nic, "equal", str(PTP_KERNEL_QUEUE + 1)],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=5)
                if rc.returncode == 0:
                    print("PTP coexistence ({} {}): RSS restreint à la queue {} (noyau/ptp4l) ; "
                          "média via fdir libmtl (≥1)".format(tag, nic, PTP_KERNEL_QUEUE), flush=True)
                else:
                    print("RSS restrict ({} {}) échoué: {}".format(
                        tag, nic, (rc.stderr or b'').decode()[:150]), flush=True)
            except Exception as e:
                print("RSS restrict ({} {}) échoué: {}".format(tag, nic, e), flush=True)
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
            "shm": "/dev/shm/{}_{}".format(HOSTNAME, _num(idx)),
            "stats": "/tmp/mtl_v{}.json".format(idx),
            "ident_file": _ident_file(idx)}


def _video_session(info, idxs, iface=IFACE):
    """Une session = un flux réseau décodé UNE fois (un flow RX), fan-out vers tous les slots
    `idxs` qui demandent cette même source (mcast:port). Évite le conflit AF_XDP « même 5-tuple,
    2 files RX » : un seul flow, recopie interne par mtl_rx vers chaque cible. `iface` = NIC qui
    porte physiquement ce mcast (multi-NIC ; tous les idxs d'un groupe = même réseau)."""
    # Ordre de champ : pas porté par le SDP 2110-20 → défaut par résolution (1080i=TFF, 576i=BFF),
    # même règle que le helper orchestrateur. mtl_rx s'en sert pour la parité du merge RX.
    fo = "bff" if 0 < int(info.get("height") or 0) <= 576 else "tff"
    return _leg2({"kind": "video", "iface": iface,
            "mcast": info["mcast"], "udp_port": info["port"], "payload_type": info["pt"],
            "width": info["width"], "height": info["height"], "fps": info["fps"],
            "interlaced": bool(info.get("interlaced")), "field_order": fo, "bit_depth": BIT_DEPTH,
            "ring": V_RING, "hdr": HDR,
            "targets": [_video_target(i) for i in idxs]},
            iface, info.get("mcast2"), info.get("port2"))


def _tx_session(idx, t, iface=IFACE):
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
    return _leg2({"kind": "video", "role": "tx", "iface": iface,
            "mcast": t["mcast"], "udp_port": t["udp_port"], "payload_type": t["pt"],
            "ssrc": _ssrc("{}:tx:v:{}".format(HOSTNAME, idx)),
            "width": t["w"], "height": t["h"], "fps": _fps,
            # Passthrough du balayage : on ré-émet en entrelacé si la source câblée l'est.
            "interlaced": (t.get("scan") == "i"), "field_order": t.get("field_order") or "tff",
            "bit_depth": t["bd"], "ring": t["ring"], "hdr": HDR,
            # Rythme d'émission (mode tranche) : >0 = grille d'émission décalée de N µs (dans la
            # signature de session mtl_rx → changement = recréation propre de la session).
            "epoch_shift_us": int(t.get("epoch_shift_us") or 0),
            # Plafond d'avance du worker (0 = désactivé). Comme `epoch_shift_us`, il entre dans la
            # SIGNATURE de session : le changer recrée proprement la session TX — bref silence à
            # l'antenne, à ne pas faire en direct sans prévenir.
            "advance": int(t.get("advance") or 0),
            "publish_lead_us": int(t.get("publish_lead_us") or 0),
            "serve_newest": int(t.get("serve_newest") or 0),
            # ident_file TOUJOURS présent (sig stable → toggle IDENT sans recréer la session) ;
            # le fichier n'existe que quand l'IDENT est actif (mtl_rx libère le patch sinon).
            # static_frame TOUJOURS présent (comme ident_file : signature de session stable → passer
            # du repli statique au câble ne recrée AUCUNE session, c'est un simple swap de source).
            # Le fichier n'existe que quand le slot relève réellement du mode statique.
            "targets": [{"idx": idx, "shm": shm, "stats": "/tmp/mtl_tx{}.json".format(idx),
                         "ident_file": _tx_ident_file(idx),
                         "static_frame": _tx_static_file(idx)}]},
            iface, t.get("mcast2"), t.get("udp_port2"))


_SDP_ORIGIN_ID = int(time.time())   # o= sess-id : fixe pour la durée du process (identifie CETTE
                                     # instance moteur), jamais 0 — RFC4566 attend un identifiant
                                     # de session réellement unique, certains récepteurs (Blackmagic)
                                     # s'appuient dessus pour détecter un SDP re-servi vs neuf.

def _sdp_origin():
    """Ligne o= : sess-id fixe au process (unicité de session), sess-version = horodatage courant
    (signale un SDP fraîchement (re)généré à chaque fetch, cf. RFC4566 §5.2)."""
    return "{} {}".format(_SDP_ORIGIN_ID, int(time.time()))


def _ssrc(label):
    """SSRC RFC3550 stable (dérivé du label, ex. 'HOST:tx:v:0') pour un slot TX. Fixe (≠0) afin que
    le SDP annoncé (a=ssrc) et le SSRC réellement émis par mtl_rx matchent — certains récepteurs
    (ex. Blackmagic) valident les paquets RTP entrants contre le a=ssrc du SDP et rejettent le flux
    si le SDP n'en porte pas (libmtl tire alors un SSRC aléatoire par session, invérifiable a priori).
    Masqué à 31 bits : json-c (mtl_rx) lit ce champ en int signé côté parsing du config."""
    return zlib.crc32(label.encode()) & 0x7fffffff


_FR = {25.0: "25", 50.0: "50", 24.0: "24", 30.0: "30", 60.0: "60", 100.0: "100", 120.0: "120",
       23.98: "24000/1001", 29.97: "30000/1001", 59.94: "60000/1001"}

def _fps_str(fps):
    f = round(float(fps or 25), 2)
    return _FR.get(f) or (str(int(f)) if float(f).is_integer() else "{}/1001".format(int(round(f + 1))))

def _tx_sdp(i, t):
    """SDP ST 2110-20 d'un slot TX. Si mcast2/udp_port2 présents (SMPTE 2022-7),
    génère un unique SDP avec deux sections m=video (leg0 + leg1)."""
    sip, sip1 = _tx_leg_sips(i, t)
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
    # TCS/RANGE : optionnels selon ST 2110-20 mais plusieurs récepteurs Blackmagic refusent de
    # locker un flux dont le SDP ne les déclare pas explicitement (repli implicite ambigu côté
    # device). Valeurs par défaut de la norme (SDR / NARROW) déclarées en toutes lettres.
    # TP (classe 2110-21) : DÉDUIT du port de sortie, jamais écrit en dur. C'était le cas jusqu'en
    # 0.80.0 — `TP=2110TPN` littéral dans la chaîne de format — et la classe réellement appliquée à
    # la session (`ops.transport_pacing`) vivait ailleurs : rien ne reliait les deux. On pouvait
    # donc émettre en wide en annonçant narrow, ce qui est précisément la promesse que le récepteur
    # fait payer. La même valeur alimente maintenant la session ET la déclaration.
    # depth : la PROFONDEUR RÉELLEMENT ÉMISE (`t["bd"]`, celle de la session mtl_rx), jamais une
    # constante. C'était `depth=10` littéral jusqu'en 0.80.2 — sur un site câblé en 8 bits de bout
    # en bout (murs en bit_depth=8, sessions TX en BIT_DEPTH=8) le SDP annonçait donc du 10 bits
    # qu'on n'émettait pas. En 4:2:2 le groupe de 2 pixels pèse 4 octets en 8 bits contre 5 en
    # 10 bits : un récepteur qui CROIT la déclaration attend 25 % de données en plus par trame et
    # ne peut que se tromper — mal parser, refuser le flux, ou compter la différence en paquets
    # manquants. Constaté à Horace le 2026-08-06 ; ce récepteur-là déduit la structure réelle des
    # paquets et tolère l'écart, mais c'est de la chance, pas de la conformité.
    # Même défaut, même correctif que `TP=` juste en dessous : la valeur qui alimente la SESSION
    # alimente la DÉCLARATION.
    bd = int(t.get("bd") or BIT_DEPTH)
    fmtp  = ("sampling=YCbCr-4:2:2; width={w}; height={h}; exactframerate={fr}; depth={bd}; "
             "{scan}TCS=SDR; colorimetry=BT709; RANGE=NARROW; "
             "PM=2110GPM; SSN=ST2110-20:2017; TP={tp};").format(
             w=w, h=h, fr=fr, bd=bd, scan=scan, tp=_tp_sdp(t.get("iface") or ""))
    dual = bool(t.get("mcast2") and t.get("udp_port2"))
    # SSRC fixe (même valeur que ops.port.ssrc côté mtl_rx, cf. _tx_session) : certains récepteurs
    # (Blackmagic) valident le SSRC des paquets RTP contre le a=ssrc annoncé et rejettent le flux
    # si le SDP n'en porte pas.
    ssrc = _ssrc("{}:tx:v:{}".format(HOSTNAME, i))
    ssrc_line = "a=ssrc:{} cname:{}\r\n".format(ssrc, HOSTNAME)
    # TROFF (ST 2110-21) : déclaré SEULEMENT en émission décalée (epoch_shift_us > 0). Valeur en
    # ticks d'horloge média (90 kHz) = TROFF standard (43/1125 × période trame ≈ 3440/fps ticks)
    # + décalage de grille. Sans décalage on n'émet pas la ligne (défaut normatif implicite).
    _shift_us = int(t.get("epoch_shift_us") or 0)
    troff_line = ("a=troff:{}\r\n".format(int(round(3440.0 / _fps + _shift_us * 0.09)))
                  if _shift_us > 0 else "")
    leg0 = (
        "m=video {port} RTP/AVP {pt}\r\n"
        "c=IN IP4 {mcast}/255\r\n"
        "{mid}"
        "{sfilter}"
        "a=rtpmap:{pt} raw/90000\r\n"
        "a=fmtp:{pt} {fmtp}\r\n"
        "{troff}"
        "{refclk}"
        "a=mediaclk:direct=0\r\n"
        "{ssrc}"
    ).format(port=int(t.get("udp_port") or 0), pt=pt, mcast=t.get("mcast") or "0.0.0.0",
             sfilter=_sf_line(t.get("mcast"), sip),
             fmtp=fmtp, troff=troff_line, refclk=_LOCALMAC_REFCLK, ssrc=ssrc_line,
             mid="a=mid:PRIMARY\r\n" if dual else "")
    grp = "a=group:DUP PRIMARY SECONDARY\r\n" if dual else ""
    sdp = "v=0\r\no=- {origin} IN IP4 {sip}\r\ns={hn} TX{i}\r\nt=0 0\r\n{grp}".format(
          origin=_sdp_origin(), sip=sip, hn=HOSTNAME, i=_num(i), grp=grp) + leg0
    if dual:
        leg1 = (
            "m=video {port} RTP/AVP {pt}\r\n"
            "c=IN IP4 {mcast}/255\r\n"
            "a=mid:SECONDARY\r\n"
            "{sfilter}"
            "a=rtpmap:{pt} raw/90000\r\n"
            "a=fmtp:{pt} {fmtp}\r\n"
            "{troff}"
            "{refclk}"
            "a=mediaclk:direct=0\r\n"
            "{ssrc}"
        ).format(port=int(t["udp_port2"]), pt=pt, mcast=t["mcast2"],
                 sfilter=_sf_line(t["mcast2"], sip1), fmtp=fmtp, troff=troff_line,
                 refclk=_LOCALMAC_REFCLK, ssrc=ssrc_line)
        sdp += leg1
    return sdp

def _anc_sdp(i, t):
    """SDP ST 2110-40 (ANC) d'un slot TX. Dual-section si anc_mcast2/anc_port2 présents (2022-7)."""
    sip, sip1 = _tx_leg_sips(i, t)
    pt   = int(t.get("anc_pt") or 97)
    dual = bool(t.get("anc_mcast2") and t.get("anc_port2"))
    ssrc = _ssrc("{}:tx:anc:{}".format(HOSTNAME, i))
    ssrc_line = "a=ssrc:{} cname:{}\r\n".format(ssrc, HOSTNAME)
    leg0 = (
        "m=video {port} RTP/AVP {pt}\r\n"
        "c=IN IP4 {mcast}/255\r\n"
        "{mid}"
        "{sfilter}"
        "a=rtpmap:{pt} smpte291/90000\r\n"
        "a=fmtp:{pt} TP={tp}; SSN=ST2110-40:2018;\r\n"
        "{refclk}"
        "a=mediaclk:direct=0\r\n"
        "{ssrc}"
    ).format(port=int(t.get("anc_port") or 0), pt=pt,
             mcast=t.get("anc_mcast") or "0.0.0.0", tp=_tp_sdp(t.get("iface") or ""),
             sfilter=_sf_line(t.get("anc_mcast"), sip),
             refclk=_LOCALMAC_REFCLK, ssrc=ssrc_line,
             mid="a=mid:PRIMARY\r\n" if dual else "")
    grp = "a=group:DUP PRIMARY SECONDARY\r\n" if dual else ""
    sdp = "v=0\r\no=- {origin} IN IP4 {sip}\r\ns={hn} TX{i} ANC\r\nt=0 0\r\n{grp}".format(
          origin=_sdp_origin(), sip=sip, hn=HOSTNAME, i=_num(i), grp=grp) + leg0
    if dual:
        leg1 = (
            "m=video {port} RTP/AVP {pt}\r\n"
            "c=IN IP4 {mcast}/255\r\n"
            "a=mid:SECONDARY\r\n"
            "{sfilter}"
            "a=rtpmap:{pt} smpte291/90000\r\n"
            "a=fmtp:{pt} TP={tp}; SSN=ST2110-40:2018;\r\n"
            "{refclk}"
            "a=mediaclk:direct=0\r\n"
            "{ssrc}"
        ).format(port=int(t["anc_port2"]), pt=pt, mcast=t["anc_mcast2"],
                 tp=_tp_sdp(t.get("iface") or ""),
                 sfilter=_sf_line(t["anc_mcast2"], sip1),
                 refclk=_LOCALMAC_REFCLK, ssrc=ssrc_line)
        sdp += leg1
    return sdp

def _aud_sdp(i, ai, acfg):
    """SDP ST 2110-30 d'un flux audio TX (L24 / 48 kHz / 8 ch). Dual-section si mcast2/port2
    présents (SMPTE 2022-7 : group:DUP + a=mid:). ts-refclk:localmac (upgrade PTP côté orchestrateur)."""
    sip, sip1 = _tx_leg_sips(i, _tx[i] if 0 <= i < len(_tx) else {})
    pt  = int(acfg.get("pt") or 97)
    # ptime PAR-SORTIE (acfg['ptime']) prime ; repli sur le défaut global. Le SDP émis doit matcher
    # exactement la session TX (mtl_rx.c:to_st30_ptime) sinon le récepteur droppe (« pkt len mismatch »).
    ptime = _tx_ptime(acfg)
    ptime_s = ("%g" % ptime)
    dual = bool(acfg.get("mcast2") and acfg.get("port2"))
    ssrc = _ssrc("{}:tx:a:{}".format(HOSTNAME, i * 2 + ai))
    ssrc_line = "a=ssrc:{} cname:{}\r\n".format(ssrc, HOSTNAME)
    def _leg(mcast, port, mid, leg_sip):
        return (
            "m=audio {port} RTP/AVP {pt}\r\n"
            "c=IN IP4 {mcast}/255\r\n"
            "{mid}"
            "{sfilter}"
            "a=rtpmap:{pt} L24/48000/{ch}\r\n"
            "a=fmtp:{pt} channel-order=SMPTE2110.(U{ch:02d})\r\n"
            "a=ptime:{ptime}\r\n"
            "{refclk}"
            "a=mediaclk:direct=0\r\n"
            "{ssrc}"
        ).format(port=int(port or 0), pt=pt, mcast=mcast or "0.0.0.0",
                 sfilter=_sf_line(mcast, leg_sip),
                 ch=A_CHANNELS, ptime=ptime_s, refclk=_LOCALMAC_REFCLK, mid=mid, ssrc=ssrc_line)
    grp = "a=group:DUP PRIMARY SECONDARY\r\n" if dual else ""
    sdp = "v=0\r\no=- {origin} IN IP4 {sip}\r\ns={hn} TX{i} AUDIO{ai}\r\nt=0 0\r\n{grp}".format(
          origin=_sdp_origin(), sip=sip, hn=HOSTNAME, i=_num(i), ai=_num(ai), grp=grp)
    sdp += _leg(acfg.get("mcast"), acfg.get("port"), "a=mid:PRIMARY\r\n" if dual else "", sip)
    if dual:
        sdp += _leg(acfg.get("mcast2"), acfg.get("port2"), "a=mid:SECONDARY\r\n", sip1)
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
_ports_alloc = []      # dernier `ports` du config (iface/sip/rx_queues/tx_queues PAR NIC) — :8080 nic.ports


_cfg_stamp = 0    # dernier mtime entier posé sur le config (strictement croissant, cf. fin de fn)

def _write_config(sessions):
    """Écrit le config lu par le DAEMON mtl_rx : device params + sessions désirées. Le daemon détecte
    le changement de mtime et RÉCONCILIE à chaud — aucune relance.

    `rx_queues`/`tx_queues` (= `mtl_init` rx/tx_queues_cnt, 1 file AF-XDP par session libmtl) sont
    dimensionnés au NOMBRE RÉEL de sessions de ce config. AVANT : forfait `ACTIVE_RX*3` (réserve
    1 vidéo+1 audio+1 ANC par slot) → sur-réservation qui plafonnait à ~16 sessions (48 files HW / 3)
    même en vidéo-seule. Maintenant : exact → une RX vidéo-seule peut monter jusqu'aux ~48 files HW.
    `MTL_RX_QUEUE_HEADROOM`/`MTL_TX_QUEUE_HEADROOM` (env, défaut 0) pré-réservent des files pour
    ajouter audio/ANC à chaud SANS réinit mtl (compromis capacité ↔ souplesse dynamique)."""
    global _rx_queues_alloc, _tx_queues_alloc, _ports_alloc, _ports_demand
    # Défaut = headroom non nul (cf. _DEF_*_HEADROOM) pour absorber les ajouts à chaud sans relance ;
    # une valeur d'env explicite (même 0) prime.
    _env_rx = os.environ.get("MTL_RX_QUEUE_HEADROOM")
    _env_tx = os.environ.get("MTL_TX_QUEUE_HEADROOM")
    hr_rx = max(0, int(_env_rx)) if _env_rx not in (None, "") else _DEF_RX_HEADROOM
    hr_tx = max(0, int(_env_tx)) if _env_tx not in (None, "") else _DEF_TX_HEADROOM
    # Plancher de réserve PAR PORT auto = budget ACTIF configuré (ACTIVE_RX / ACTIVE_TX_C) réparti sur
    # les ports auto. On réserve dès le 1er mtl_init de quoi tenir TOUT le budget actif → s'abonner
    # jusqu'à ACTIVE_RX ne déclenche AUCUNE relance (donc aucun teardown des flux RX → pas de gel des
    # multiviews aval). Le daemon ne relance que si la demande dépasse ce plancher+headroom (cas
    # anormal / dépassement du budget). 1 file XDP ≈ 9,7 Mio d'UMEM — réserver le budget actif est
    # peu coûteux devant la RAM du nœud, et bien plus sûr que relancer à chaque ajout.
    _n_auto = max(1, len(_auto_ports))
    floor_rx = (ACTIVE_RX + _n_auto - 1) // _n_auto if ACTIVE_RX > 0 else 0
    floor_tx = (ACTIVE_TX_C + _n_auto - 1) // _n_auto if ACTIVE_TX_C > 0 else 0
    # Files PAR NIC : multi-NIC = chaque PF a son budget de files AF-XDP indépendant (48/E810). On
    # compte les sessions par iface (RX = non-'tx' : vidéo sans 'role', audio/ANC role='rx'). Une
    # session sans clé `iface` (config legacy) compte sur la NIC primaire. Le headroom (pré-réserve
    # pour ajouter à chaud sans réinit mtl) est PAR PORT (orchestrateur l'a déjà divisé par le nombre
    # de ports auto), appliqué à CHAQUE port candidat à la répartition auto (= _auto_ports).
    rx_per, tx_per = {}, {}
    for s in sessions:
        nic = s.get("iface") or IFACE
        d = tx_per if s.get("role") == "tx" else rx_per
        d[nic] = d.get(nic, 0) + 1
        # 2022-7 : une session dual-leg (iface2) consomme une file sur CHAQUE NIC (libmtl
        # alloue un flow/queue par session-port) → compter le leg redondant sur sa NIC.
        nic2 = s.get("iface2")
        if nic2 and nic2 != nic:
            d[nic2] = d.get(nic2, 0) + 1
    ports = []     # RÉSERVE écrite au daemon = max(demande, plancher) + marge
    demand = []    # DEMANDE réelle (sessions effectives) — base de la décision de relance
    for i, nic in enumerate(IFACES):
        _hr = nic in _auto_ports          # plancher auto seulement sur les ports d'auto-répartition
        d_rx = rx_per.get(nic, 0)
        d_tx = tx_per.get(nic, 0)
        # Plancher/marge : override OPÉRATEUR par interface (PORT_RESERVE, réglé Réglages → Réseau)
        # si présent, sinon plancher AUTO (budget actif réparti, ports auto uniquement).
        _ov = PORT_RESERVE.get(nic) or {}
        fl_rx = _ov["rx"] if "rx" in _ov else (floor_rx if _hr else 0)
        fl_tx = _ov["tx"] if "tx" in _ov else (floor_tx if _hr else 0)
        mg_rx = _ov["hr"] if "hr" in _ov else (hr_rx if _hr else 0)
        mg_tx = hr_tx if _hr else 0
        rq = max(d_rx, fl_rx) + mg_rx
        tq = max(d_tx, fl_tx) + mg_tx
        # Plafond RL/E810 (chantier DPDK). HISTORIQUE (0.39.1) : le shaper RL (rte_tm) du PMD ice
        # refusait de committer la hiérarchie au-delà de nb_tx_q=8 (« ice_tx_queue_start: Failed to
        # add lan txq ») car libmtl accrochait TOUTES ses files-feuilles sous UN SEUL nœud QG
        # (fan-out 8) → on bornait tx_queues ≤ 7. Le patch libmtl `patch_tm_hierarchy.py` (0.39.6)
        # RAMIFIE l'arbre TM (P nœuds QG, chacun ≤8 feuilles) → nb_qps = P×8 jusqu'à
        # MT_MAX_RL_ITEMS=128 → le mur des 8 est LEVÉ (>8 senders RL/port ET création en rafale).
        # Le cap ne concerne QUE le mécanisme RL (arbre TM) : tsc/tsc_narrow ne construisent AUCUNE
        # hiérarchie → JAMAIS bornés. On re-gate donc sur le pacing (narrow-wins : "rl"/"auto" =
        # RL device-wide sur E810 dpdk ; "tsc"/"tsc_narrow"/"tsn" = pas de cap). Nouveau plafond =
        # capacité réelle du patch mesurée au banc (dl360-1 2026-07-07, cf. docs/chantiers/DPDK_NARROW.md), + la
        # file de contrôle (nb_tx_q = tx_queues+1). Sans objet en AF_XDP (budget 48 files).
        _pacing = (os.environ.get("MTL_PACING") or "auto").strip().lower()
        _rl_active = _pacing in ("rl", "auto")   # auto → RL sur port E810 dpdk (TM supporté)
        if _HAS_DPDK and _port_pmd(nic) == "dpdk" and _rl_active:
            tq = min(tq, RL_TX_QUEUES_CAP)
        _pe = {"iface": nic, "sip": SIPS[i] if i < len(SIPS) else "",
               "rx_queues": max(1, rq), "tx_queues": max(1, tq)}
        # Classe 2110-21 : émise INCONDITIONNELLEMENT depuis 0.80.0. Auparavant la clé n'était
        # écrite que sur un nœud dpdk ; un nœud 100 % AF-XDP n'en émettait AUCUNE et mtl_rx
        # retombait sur son défaut `ST21_PACING_NARROW` → le SDP annonçait `TP=2110TPN` SANS que
        # personne ne l'ait décidé, alors que le limiteur matériel — la seule mécanique qui tient
        # narrow — était absent. On déclarait donc une régularité qu'on ne pouvait pas tenir.
        # (Le config d'un nœud af_xdp n'est plus octet-identique à avant : c'est l'objet du
        # correctif, et la valeur écrite est celle qui part dans le SDP.)
        _pe["profile"] = _port_profile_effectif(nic)
        # Masque/passerelle du port : émis SEULEMENT s'ils sont déclarés (une clé absente laisse
        # mtl_rx sur son comportement historique, adresse seule).
        if i < len(NETMASKS) and NETMASKS[i]:
            _pe["netmask"] = NETMASKS[i]
        if i < len(GATEWAYS) and GATEWAYS[i]:
            _pe["gateway"] = GATEWAYS[i]
        # PMD par port (chantier DPDK) : clés émises SEULEMENT si ≥1 port dpdk sur le nœud.
        if _HAS_DPDK:
            _pe["pmd"] = _port_pmd(nic)
            _pe["bdf"] = _port_bdf(nic)
        ports.append(_pe)
        demand.append({"iface": nic, "rx_queues": d_rx, "tx_queues": d_tx})
    # Totaux (exposés :8080 xdp.allocated + réservation au lancement du daemon).
    _rx_queues_alloc = sum(p["rx_queues"] for p in ports)
    _tx_queues_alloc = sum(p["tx_queues"] for p in ports)
    _ports_alloc = ports     # réservation par port (stats :8080 nic.ports + figée au lancement)
    _ports_demand = demand   # demande réelle par port (décision de relance vs _ports_reserved)
    with open(_CONFIG_PATH, "w") as f:
        # `ports` = source de vérité (mtl_rx le lit en priorité). iface/sip/rx_queues/tx_queues
        # scalaires = repli rétro-compat (1ʳᵉ NIC) pour un mtl_rx antérieur au multi-port.
        # Le "pmd" GLOBAL reste "af_xdp" (repli rétro-compat) ; le PMD réel est PAR PORT
        # (ports[].pmd/bdf, émis seulement si ≥1 port dpdk — cf. plus haut).
        _cfg = {"pmd": "af_xdp", "lcores": LCORES, "quota_mbs": QUOTA_MBS,
                "ports": ports,
                "iface": IFACE, "sip": SIP,
                "rx_queues": ports[0]["rx_queues"], "tx_queues": ports[0]["tx_queues"],
                "sessions": sessions}
        # Pacing TX 2110-21 (mtl_init_params.pacing, niveau device) : "auto"|"rl"|"tsc".
        # RL (rate-limit matériel) = prérequis du profil narrow — sans objet en AF-XDP (TSC).
        # Clé émise seulement si un port est dpdk ou si l'opérateur force MTL_PACING.
        if _HAS_DPDK or os.environ.get("MTL_PACING"):
            _cfg["pacing"] = (os.environ.get("MTL_PACING") or "auto").strip()
        json.dump(_cfg, f)
    # mtime ENTIER strictement croissant : un mtl_rx antérieur au compare-nanosecondes détecte le
    # changement sur (long)st_mtime — deux écritures dans la même seconde (rafale de commutations)
    # étaient invisibles → daemon figé sur un état intermédiaire (vu au banc de churn Horace).
    global _cfg_stamp
    _cfg_stamp = max(int(time.time()), _cfg_stamp + 1)
    try:
        os.utime(_CONFIG_PATH, (_cfg_stamp, _cfg_stamp))
    except Exception:
        pass


def _launch_mtl():
    """(Re)lance le daemon mtl_rx. Purge d'abord le XDP ET les règles ntuple résiduels : au 1er
    lancement (ou après un crash / `docker rm -f`) une instance précédente a pu laisser un programme
    XDP accroché (`native xdp dev init fail -5`) ET/OU des règles fdir sur le matériel (« socket add
    flow fail » → session muette). On repart d'une interface propre."""
    global _mtl_proc, _last_launch, _rx_queues_reserved, _tx_queues_reserved, _ports_reserved
    # Si un daemon est ENCORE VIVANT (relance pour cause de budget de files, PAS un crash), il faut le
    # TERMINER d'abord — sinon deux mtl_rx se disputent la même NIC/XDP (queues TX fatales « not dpdk
    # user pmd », RX muet). Sur le chemin crash, poll() != None → ce bloc est un no-op.
    if _mtl_proc is not None and _mtl_proc.poll() is None:
        try:
            _mtl_proc.terminate()
            try:
                _mtl_proc.wait(timeout=8)
            except Exception:
                _mtl_proc.kill(); _mtl_proc.wait(timeout=5)
        except Exception as _e:
            print("mtl_rx: arrêt du daemon précédent: {}".format(_e), flush=True)
        time.sleep(0.5)   # laisse le noyau détacher le programme XDP du daemon sortant
    _xdp_off()
    _flush_ntuple()
    _check_port_ips()   # les IP lues MAINTENANT par mtl_init sont figées à vie du daemon
    _mtl_proc = subprocess.Popen([MTL_RX, "--config", _CONFIG_PATH])
    _last_launch = time.time()
    # Fige la réservation effective = ce que le config porte À CET INSTANT (lu par mtl_init au boot du
    # daemon). _write_config a déjà posé _rx/_tx_queues_alloc + _ports_alloc juste avant dans la boucle.
    _rx_queues_reserved = _rx_queues_alloc
    _tx_queues_reserved = _tx_queues_alloc
    _ports_reserved = [dict(p) for p in _ports_alloc]   # réservation PAR PORT figée (détection de croissance)
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
        _publier_trame_statique(idx)    # retire un fichier de repli devenu sans objet
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
    elif user_gen or fallback != "none":
        with _tx_gen_lock:
            _tx_gen[idx]["enabled"] = True
            if not user_gen:
                _tx_gen[idx]["pattern"] = fallback   # le repli impose sa mire
        # TRAME STATIQUE quand c'est possible (mire fixe, progressif) : le moteur ré-émet lui-même
        # une trame publiée une fois, sans producteur ni flux MXL — cadence nominale et coût nul.
        # Sinon (motif animé, entrelacé) on garde le générateur MXL historique.
        if _tx_static_applicable(idx):
            with _tx_lock:
                _tx[idx]["shm_in"] = None            # pas de source vivante : le C sert le fichier
                _tx[idx]["enabled"] = True
            with _tx_gen_lock:
                _tx_gen[idx]["enabled"] = False      # le thread txgen n'a plus lieu d'être
        else:
            shm_name = "/dev/shm/{}_txgen_{}".format(HOSTNAME, _num(idx))
            with _tx_lock:
                _tx[idx]["shm_in"] = shm_name
                _tx[idx]["enabled"] = True
    else:
        # Pas de source VIDÉO (ni câble, ni gen, ni repli). Le slot reste néanmoins ACTIF s'il porte
        # une source AUDIO ou ANC câblée : c'est un slot audio-seul / ANC-seul. `enabled` gouverne les
        # gates d'émission audio/ANC (cf. boucle de build TX) — sans ce test, un slot sans vidéo ne
        # pourrait JAMAIS émettre d'audio ni d'ANC seuls. La session VIDÉO, elle, exige toujours une
        # destination + un shm vidéo (elle n'est donc pas créée ici : shm_in reste None).
        with _tx_lock:
            _tx[idx]["shm_in"] = None
            has_aud = any(_tx[idx].get("audio_cable_shm") or [])
            has_anc = bool(_tx[idx].get("anc_cable_shm"))
            _tx[idx]["enabled"] = bool(has_aud or has_anc)
        with _tx_gen_lock:
            _tx_gen[idx]["enabled"] = False
    # Publication (ou RETRAIT) de la trame statique, dans TOUS les cas : le prédicat est autonome,
    # donc un slot qu'on vient de câbler voit son fichier de repli disparaître, et un slot décâblé
    # le voit apparaître — sans qu'aucune branche ci-dessus n'ait à y penser.
    _publier_trame_statique(idx)


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
    return ["{} · TX{}".format(HOSTNAME, _num(idx)),
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
    lines = ["{} TX{}".format(HOSTNAME, _num(idx)), "{} · {}".format(mode_label, pat_name), "{}:{}".format(mcast_s, port_s)]
    return _render_patch_lines(lines, size)


def _tx_ident_file(idx):
    return "/dev/shm/{}_tx{}_ident".format(HOSTNAME, _num(idx))


# ─── Trame STATIQUE d'un slot TX non câblé (noir de repli, mire) ──────────────────────────────
# Le contenu d'un slot en GÉN ne change QUE sur évènement : motif, ident, format. Le produire 50
# fois par seconde via un flux MXL et un thread Python n'a aucun sens — et coûtait cher (≈4 Mo
# recopiés par trame et par slot, cf. 0.62.0). On le rend UNE fois et on publie le résultat dans un
# fichier ; `mtl_rx` le charge sur changement de mtime et le ré-émet lui-même, sans producteur, sans
# reader, sans attente de grain — donc à la cadence NOMINALE (cf. video_tx_*_thread, mode statique).
#
# Le fichier est BYTE-IDENTIQUE au payload d'un grain MXL (planar Y|Cb|Cr, même profondeur) : les
# deux chemins TX du moteur le consomment avec leur code existant, sans connaissance de format
# nouvelle d'aucun côté, et la profondeur se déduit de la TAILLE comme pour un grain.
#
# Contrat identique à celui de l'IDENT : écriture atomique par rename, relecture sur mtime.
def _tx_static_file(idx):
    return "/dev/shm/{}_tx{}_static".format(HOSTNAME, _num(idx))


def _tx_static_applicable(idx):
    """Ce slot peut-il être servi par une trame statique plutôt que par un producteur vivant ?

    NON si : câblé (c'est de la vidéo qui bouge), entrelacé (le mode statique du moteur est
    progressif seulement — limite assumée, cf. mtl_rx.c), ou motif ANIMÉ. Ces cas gardent le
    générateur MXL historique : la bascule est explicite, jamais un trou silencieux.

    Le prédicat se calcule depuis les SOURCES DE VÉRITÉ (câble, balayage, user_enabled,
    fallback_mode) et surtout PAS depuis `_tx_gen[idx]["enabled"]`, qui n'est plus que l'état du
    thread générateur — précisément ce que le mode statique éteint."""
    if idx >= ACTIVE_TX_C:
        return False
    with _tx_lock:
        cable = (_tx[idx].get("cable_shm") or "")
        entrelace = (_tx[idx].get("scan") == "i")
        fallback = _tx[idx].get("fallback_mode") or "none"
    with _tx_gen_lock:
        user_gen = _tx_gen[idx]["user_enabled"]
        motif = (_tx_gen[idx].get("pattern") or "black") if user_gen else fallback
    if cable or entrelace or not (user_gen or fallback != "none"):
        return False
    return motif not in _TXGEN_MOTIFS_DYNAMIQUES


def _publier_trame_statique(idx):
    """Rend la trame du slot (motif + ident incrusté) et la publie pour `mtl_rx`. Renvoie le chemin
    du fichier, ou None si le slot ne relève pas du mode statique (le fichier est alors RETIRÉ, ce
    qui rebascule proprement le moteur sur son producteur)."""
    fpath = _tx_static_file(idx)
    if not _tx_static_applicable(idx):
        try:
            if os.path.exists(fpath):
                os.remove(fpath)
        except OSError as e:
            print("tx static remove err idx={}: {}".format(idx, e), flush=True)
        return None
    with _tx_lock:
        w, h = int(_tx[idx]["w"] or WIDTH), int(_tx[idx]["h"] or HEIGHT)
    with _tx_gen_lock:
        motif = _tx_gen[idx].get("pattern") or "black"
        user_ident = _tx_gen[idx]["ident"]
    lay = _layout(w, h)
    try:
        y, cb, cr = _get_pattern(motif, 0, lay)
        buf = np.empty(lay["y"] + 2 * lay["uv"], dtype=np.uint8)
        _fill_grain_planes(buf, lay, y, cb, cr)
        # IDENT user actif → c'est le moteur qui l'incruste (overlay C sur la trame émise) : ne pas
        # le brûler ici, on aurait le libellé en double à l'écran.
        if not user_ident:
            _overlay_patch(buf, 0, _txgen_ident_patch(idx), lay)
        tmp = fpath + ".tmp"
        with open(tmp, "wb") as f:
            f.write(buf.tobytes())
        os.replace(tmp, fpath)      # publication atomique (mtl_rx lit le mtime)
        return fpath
    except Exception as e:
        print("tx static publish err idx={}: {}".format(idx, e), flush=True)
        return None


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
    # L'IDENT user gouverne aussi le libellé AUTO brûlé dans la trame statique (sans quoi on
    # afficherait les deux) → republier la trame du slot s'il est en mode statique.
    _publier_trame_statique(idx)


def _ports_need_relaunch():
    """True si la DEMANDE RÉELLE d'un port (_ports_demand : sessions effectives) dépasse ce que le
    daemon a RÉSERVÉ au dernier mtl_init (_ports_reserved = max(demande, plancher actif) + headroom).
    mtl_init ne relit pas rx/tx_queues à chaud → au-delà de la réserve, une session se crée mais
    n'obtient aucune file XDP (0 fps) ; on relance alors pour étendre la réserve.

    On compare la DEMANDE (sans headroom) à la RÉSERVE (avec headroom + plancher) — sinon le headroom,
    présent des deux côtés, s'annulerait et toute hausse de demande au-delà du boot relancerait le
    daemon (teardown des flux RX → gel des consommateurs). Avec le plancher dimensionné au budget
    ACTIF, s'abonner jusqu'à ACTIVE_RX ne franchit jamais la réserve → aucune relance en usage normal."""
    if not _ports_reserved:
        return False
    res = {p["iface"]: p for p in _ports_reserved}
    for p in _ports_demand:
        r = res.get(p["iface"])
        # Port nouvellement apparu, ou demande RX/TX réelle au-delà de la réserve figée → relance.
        if r is None or p["rx_queues"] > r["rx_queues"] or p["tx_queues"] > r["tx_queues"]:
            return True
    return False


def _manager_loop():
    """Calcule l'ensemble RX voulu (SDP actifs, groupés par source pour le fan-out) et RÉÉCRIT le
    config. Le daemon mtl_rx — lancé UNE fois et MAINTENU en vie (mtl_init à vie) — réconcilie les
    sessions à chaud : plus de kill/relance, ptp4l ne faute qu'au 1er lancement. On relance si le
    daemon meurt (crash, backoff + purge XDP) OU si la demande de files dépasse la réserve gelée du
    dernier mtl_init (sinon plafond muet ~8 = 2 files/port × 4 ports) — relance DEBOUNCÉE (config
    stable depuis _RELAUNCH_SETTLE_S) pour regrouper une rafale d'abonnements en une seule réinit."""
    global _mtl_proc, _cur_sig, _fail_streak, _xdp_sessions_active, _xdp_active_per_iface, _sig_changed_at, _ip_check_at
    global _tx_budget_warned, _tx_sessions_dropped, _tx_active_per_iface, _rx_active_per_iface
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

        # Watchdog RX : bounce des groupes abonnés qui ne reçoivent RIEN (cf. constantes _WD_*).
        _wd_now = time.time()
        if _WD_STALL_S > 0:
            for _k in [k for k in _wd_state if k not in groups]:
                del _wd_state[_k]                            # groupe désabonné → oubli
            _wd_daemon_ok = (_mtl_proc is not None and _mtl_proc.poll() is None
                             and (_wd_now - _last_launch) > _WD_GRACE_S)
            for key, g in list(groups.items()):
                # anchor = départ du timer de stall (abonnement / dernier bounce) ; prog = dernière
                # VRAIE progression (frame_index avance). Le backoff ne se réarme que sur prog —
                # réarmer sur anchor faisait retomber le délai à 15 s après chaque bounce (le
                # groupe paraissait « frais ») → une source absente re-tentait toutes les 15 s
                # au lieu de 30 s → 5 min.
                st = _wd_state.setdefault(key, {"fi": {}, "anchor": {}, "prog": {},
                                                "delay": _WD_STALL_S, "until": 0.0, "n": 0})
                if _wd_now < st["until"]:                    # bounce en cours → session omise
                    del groups[key]
                    continue
                # Suivi PAR SLOT (pas le max du groupe) : dans un groupe fan-out, une cible morte
                # pendant que sa sœur reçoit serait invisible au max — vu au banc de churn (1/50).
                for _i in [i for i in st["fi"] if i not in g["idxs"]]:
                    for _d in ("fi", "anchor", "prog"):
                        st[_d].pop(_i, None)                         # cible retirée du groupe
                with metrics_lock:
                    fis = {i: (metrics[i].get("frame_index") or 0) for i in g["idxs"]}
                for i, fi in fis.items():
                    if fi > 0 and fi != st["fi"].get(i):
                        st["fi"][i] = fi
                        st["anchor"][i] = st["prog"][i] = _wd_now    # cette cible avance
                    elif i not in st["anchor"]:
                        st["anchor"][i] = _wd_now                    # 1ʳᵉ vue de cette cible
                stalled = [i for i in g["idxs"] if (_wd_now - st["anchor"][i]) >= st["delay"]]
                if not stalled:
                    if st["n"] and all((_wd_now - st["prog"].get(i, 0)) < _WD_STALL_S
                                       for i in g["idxs"]):
                        st["delay"] = _WD_STALL_S; st["n"] = 0       # vraie reprise → reset backoff
                        st.pop("cold_logged", None)                  # réarme l'abandon à froid :
                        # ce groupe a reçu pour de bon, il MÉRITE de nouveau des tentatives s'il
                        # se fige un jour (et un nouveau message si on doit re-renoncer).
                elif _wd_daemon_ok:
                    # ★ Groupe JAMAIS ALIMENTÉ : aucun de ses slots n'a jamais progressé (`prog`
                    # vide). Après _WD_COLD_MAX_N essais, on ARRÊTE — recréer coûte un commit RL
                    # (arrêt/redémarrage du port) qui tue les sorties TX de la même carte, pour
                    # une panne qui n'est pas de notre ressort. On le DIT une fois, plutôt que de
                    # renoncer en silence : le groupe reste visible comme « sans signal ».
                    _jamais_recu = not any(st["prog"].get(i) for i in g["idxs"])
                    if _jamais_recu and st["n"] >= _WD_COLD_MAX_N:
                        if not st.get("cold_logged"):
                            st["cold_logged"] = True
                            print("watchdog RX: {}:{} slot(s) {} n'ont JAMAIS reçu d'image après {} "
                                  "tentative(s) → abandon des recréations (elles arrêtent le port et "
                                  "tuent les sorties TX de la même carte). Cause en amont : émetteur "
                                  "absent, mauvais groupe ou route manquante.".format(
                                      key[0], key[1], g["idxs"], st["n"]), flush=True)
                        # ⚠ SURTOUT PAS de `del groups[key]` ici : dans le chemin de recréation il
                        # signifie « omettre ce tour pour recréer au suivant », mais ici il
                        # SUPPRIMERAIT la session — définitivement, puisqu'on repasse à chaque tour.
                        # Vécu : les 6 entrées démontées (config 10 → 4, rx_sessions=0), plus rien
                        # n'écoutait si une source revenait. Abandonner la RECRÉATION, ce n'est pas
                        # démonter l'écoute : la session reste en place et recevra si ça arrive.
                        continue
                    worst = max(_wd_now - st["anchor"][i] for i in stalled)
                    st["until"] = _wd_now + _WD_BOUNCE_S
                    st["n"]    += 1
                    st["delay"] = min(st["delay"] * 2, _WD_MAX_S)
                    for i in stalled:
                        st["anchor"][i] = _wd_now                    # repart pour un délai complet
                    print("watchdog RX: {}:{} slot(s) {} sans image depuis {:.0f}s (groupe {}) → "
                          "recréation de session (tentative #{}, prochain essai dans {:.0f}s)".format(
                              key[0], key[1], stalled, worst, g["idxs"], st["n"], st["delay"]),
                          flush=True)
                    del groups[key]

        sessions = [_video_session(g["info"], g["idxs"], _rx_iface(g["idxs"][0])) for g in groups.values()]
        # Cadence VIDÉO par slot, pour les sessions ANC : l'ANC d'une entrée est la donnée
        # ancillaire de SA vidéo, elle doit tourner sur la même grille. Construit après la
        # finalisation de `groups` (des entrées ont pu en être retirées juste au-dessus).
        _fps_slot = {}
        for g in groups.values():
            _f = (g.get("info") or {}).get("fps")
            if _f:
                for _i in g["idxs"]:
                    _fps_slot[_i] = float(_f)
        active = set(t["idx"] for s in sessions if s["kind"] == "video" for t in s["targets"])
        for idx in range(N_VIDEO):
            _live[idx] = idx in active

        # Sessions RX audio (2110-30) : SDP audio actif → écrit /dev/shm/{hn}_audio_{idx} en L24 BE.
        # _audio_live[idx] pilote le générateur de tonalité simu (_simu_audio_loop) : tonalité écrite
        # SEULEMENT si aucun audio RÉEL n'écrit le flux (indépendant de la liveness vidéo).
        # PRÉCÉDENCE du GÉNÉRATEUR : si la tonalité est activée sur un slot, elle ÉCRASE la source
        # réelle — on NE crée PAS la session RX audio (mtl_rx cesse d'écrire le flux) et _audio_live
        # passe à False → _simu_audio_loop prend la main (évite le conflit double-writer sur le shm).
        for idx in range(N_AUDIO):
            apath = "{}/nmos_recv_a_{}.sdp".format(SDP_DIR, idx)
            ainfo = _parse_sdp_audio(apath) if os.path.exists(apath) else None
            with _sim_tone_lock:
                gen_on = bool(_sim_tone[idx]["enabled"]) if idx < len(_sim_tone) else False
            _audio_live[idx] = bool(ainfo) and not gen_on
            if ainfo and not gen_on:
                sessions.append(_audio_session(idx, ainfo, _rx_iface(idx)))

        # Sessions RX ANC (2110-40) : SDP smpte291 actif → écrit /dev/shm/{hn}_anc_{idx} + timecode.
        for idx in range(N_ANC):
            dpath = "{}/nmos_recv_anc_{}.sdp".format(SDP_DIR, idx)
            dinfo = _parse_sdp_anc(dpath) if os.path.exists(dpath) else None
            if dinfo:
                sessions.append(_anc_session(idx, dinfo, _rx_iface(idx),
                                             fps=_fps_slot.get(idx)))

        # Sessions TX : un slot émet s'il est activé, a une destination et un shm d'entrée câblé.
        # Plafonné à ACTIVE_TX_C (budget de queues partagé RX+TX) — les slots provisionnés au-delà
        # ne créent aucune session (cf. _tx_gen_apply qui les force déjà à enabled=False).
        # Budget de sessions TX = RL_TX_QUEUES_CAP (files RL/port de la carte − contrôle ; injecté en env
        # par docker_driver depuis la BIBLIOTHÈQUE DE CARTES, défaut 63 = E810-C). On BORNE le nombre de
        # sessions TX émises (vidéo+audio+ANC) DESSUS : sinon demande > réserve → le daemon relance en
        # boucle pour agrandir un budget que le HW ne donnera jamais. Sessions au-delà = IGNORÉES (loggé).
        _ntx_q = 0; _tx_dropped = 0
        def _emit_tx(sess):
            nonlocal _ntx_q, _tx_dropped
            if _ntx_q < RL_TX_QUEUES_CAP:
                sessions.append(sess); _ntx_q += 1
            else:
                _tx_dropped += 1
        with _tx_lock:
            for i in range(min(N_TX, ACTIVE_TX_C)):
                t = _tx[i]
                # Émission de la session vidéo TX : soit CÂBLÉE (enabled + source), soit PRÉ-PROVISIONNÉE
                # (session/feuille RL créée sans source → silencieuse ; mtl_rx tolère shm_in vide et
                # route le contenu à chaud par swap). Destination (mcast+port) toujours requise.
                if t["mcast"] and t["udp_port"] and ((t["enabled"] and t["shm_in"]) or t.get("provisioned")):
                    _emit_tx(_tx_session(i, t, _tx_iface(i, t.get("iface"))))
                # TX audio : priorité TONALITÉ (gen autonome) > mire/repli (GEN vidéo) > câblé.
                # SANS aucune de ces sources, la session est émise QUAND MÊME, source VIDE : le
                # moteur produit alors du SILENCE lui-même (audio_tx_thread, chemin `muet`). Émettre
                # du silence plutôt que rien garde la session et sa feuille RL vivantes → câbler
                # l'audio plus tard reste un swap de source, sans recréation ni commit RL. Et le
                # silence ne justifie AUCUN producteur : c'est une absence, pas un signal.
                _acable = t.get("audio_cable_shm") or []
                for ai, acfg in enumerate(t.get("audios") or []):
                    if not acfg.get("mcast") or not acfg.get("port"):
                        continue
                    _tone_on = bool(_tx_tone[i][ai]["enabled"]) if ai < len(_tx_tone[i]) else False
                    if _tone_on or (not t.get("cable_shm") and _tx_gen[i]["enabled"]):
                        ashm = "/dev/shm/{}_audio_txgen_{}_{}".format(HOSTNAME, _num(i), _num(ai))
                    else:
                        ashm = _acable[ai] if ai < len(_acable) else None
                    if t["enabled"]:
                        if ashm and not ashm.startswith("/"):
                            ashm = "/dev/shm/" + ashm
                        _emit_tx(_audio_tx_session(i * 2 + ai, acfg, ashm or "",
                                                   _tx_iface(i, t.get("iface"))))
                # TX ANC : câblage INDÉPENDANT (anc_cable_shm). NON câblé ⇒ pas de session.
                dshm = t.get("anc_cable_shm")
                if t["enabled"] and t.get("anc_mcast") and t.get("anc_port") and dshm:
                    if not dshm.startswith("/"):
                        dshm = "/dev/shm/" + dshm
                    _emit_tx(_anc_tx_session(i, t, dshm, _tx_iface(i, t.get("iface"))))
        _tx_sessions_dropped = _tx_dropped   # sur-capacité RL surfacée :8080 (bloc rl.tx_dropped)
        if _tx_dropped and not _tx_budget_warned:
            _tx_budget_warned = True
            print("mtl_rx: {} session(s) TX au-delà du cap RL de la carte ({} sessions/port, "
                  "RL_TX_QUEUES_CAP) — IGNORÉES. Réduire les sorties actives (limite carte, cf. §7)."
                  .format(_tx_dropped, RL_TX_QUEUES_CAP), flush=True)
        elif not _tx_dropped:
            _tx_budget_warned = False

        # 1) config : réécrit dès qu'il change → le daemon réconcilie à chaud (aucune relance/faute PTP)
        sig = json.dumps(sessions, sort_keys=True)
        if sig != _cur_sig:
            with _mtl_lock:
                _write_config(sessions)
            _cur_sig = sig
            _sig_changed_at = time.time()
            _xdp_sessions_active = len(sessions)
            _api, _tpi, _rpi = {}, {}, {}
            for _s in sessions:
                _ifc = _s.get("iface") or IFACE
                _api[_ifc] = _api.get(_ifc, 0) + 1
                # Ventilation RX/TX par port (même règle role=='tx' que _write_config) — le TX est
                # la métrique bornée par le cap RL (socle narrow), le RX consomme des files RSS.
                _d = _tpi if _s.get("role") == "tx" else _rpi
                _d[_ifc] = _d.get(_ifc, 0) + 1
                # 2022-7 : le leg redondant consomme sa file/feuille RL sur SA NIC (cf. _write_config)
                # → compté dans les ventilations RX/TX (comparées au cap PAR PORT). `_api` (compteur
                # AF-XDP historique) garde sa sémantique existante (sessions, pas legs).
                _ifc2 = _s.get("iface2")
                if _ifc2 and _ifc2 != _ifc:
                    _d[_ifc2] = _d.get(_ifc2, 0) + 1
            _xdp_active_per_iface = _api
            _tx_active_per_iface = _tpi
            _rx_active_per_iface = _rpi
            print("mtl_rx config: {} session(s)".format(len(sessions)), flush=True)

        # 2) cycle de vie : lancé 1× au 1er besoin, maintenu en vie ; relancé si crash OU si la demande
        #    de files dépasse la réserve gelée du dernier mtl_init (debounce : config stable depuis
        #    _RELAUNCH_SETTLE_S pour regrouper une rafale d'abonnements en UNE seule réinit).
        dead = (_mtl_proc is not None and _mtl_proc.poll() is not None)
        need_budget = (_mtl_proc is not None and not dead
                       and _ports_need_relaunch()
                       and (time.time() - _sig_changed_at) >= _RELAUNCH_SETTLE_S)
        # HORLOGE AU REPOS (socle DPDK) : sur un port full-PF DPDK il n'y a plus de netdev kernel,
        # donc plus de ptp4l — la SEULE horloge du nœud est le client PTP interne de libmtl, qui
        # n'existe que tant que mtl_rx tourne (mtl_init). Tant que le daemon n'était lancé qu'au 1ᵉʳ
        # abonnement, un moteur au repos laissait le nœud SANS AUCUNE référence de temps (alertes
        # « horloge absente » permanentes, et le 1ᵉʳ flux câblé devait attendre la convergence PTP
        # au lieu de trouver une horloge déjà disciplinée). mtl_rx accepte parfaitement 0 session :
        # c'est un daemon `mtl_init` à vie + réconciliation à chaud, et sa boucle publie les stats
        # (dont le bloc `ptp` de mtl_ports.json) indépendamment des sessions.
        # En AF-XDP on garde le comportement HISTORIQUE : le CNI n'y a pas de PTP interne (l'horloge
        # vient de ptp4l noyau) → lancer le daemon à vide attacherait le XDP et réserverait des files
        # pour rien, sans aucune horloge à la clé.
        garder_horloge = _ENGINE_PTP_ON and not sessions
        with _mtl_lock:
            if _mtl_proc is None and (sessions or garder_horloge):
                if garder_horloge:
                    print("mtl_rx: démarrage à vide (0 session) — le moteur porte l'horloge PTP du "
                          "nœud (socle DPDK, pas de ptp4l noyau)", flush=True)
                _launch_mtl()                               # 1er lancement (mtl_init → 1 seule faute PTP)
            elif need_budget and sessions:
                print("mtl_rx: demande de files > réserve ({} rx / {} tx alloc vs {} / {} réservé) → "
                      "relance pour réserver le nouveau budget".format(
                          _rx_queues_alloc, _tx_queues_alloc,
                          _rx_queues_reserved, _tx_queues_reserved), flush=True)
                _launch_mtl()                               # réinit pour étendre la réserve de files
            elif dead and (sessions or garder_horloge):
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
        # Contrôle périodique des IP de ports (dérive à chaud : IP retirée/dupliquée par l'hôte).
        # 600 s et non 60 : une dérive d'IP de port est RARE (elle vient d'une action hôte), et ce
        # contrôle lance un subprocess `ip -4 -o addr show`. 10 min suffisent largement. Historique :
        # cette période était le suspect n°1 du « hoquet ~60 s » du moteur — innocentée (le hoquet
        # était un artefact de mesure, cf. mtl_rx.c write_stats / dt monotone), mais l'allègement
        # reste bon à prendre.
        if time.time() >= _ip_check_at:
            _ip_check_at = time.time() + 600.0
            _check_port_ips()
        time.sleep(0.5)


def _simu_loop(idx):
    """Écrit la mire de simu dans le FLUX MXL du slot TANT QU'IL N'EST PAS servi par mtl_rx (_live).
    Quand le slot est live, mtl_rx possède le flux ; on ne fait que relayer ses stats sur :8080."""
    name = "{}_{}".format(HOSTNAME, _num(idx))
    writer = None; sim_res = None; fi = 0
    def _close():
        nonlocal writer, sim_res
        if writer is not None:
            try: writer.close()
            except Exception: pass
        writer = None; sim_res = None
    while True:
        with _ctl_lock:
            gen_on = bool(_ctl[idx]["gen"])
        # On NE génère la mire QUE si le générateur est explicitement actif (gen) et le slot non live.
        # Symétrie avec l'audio (_simu_audio_loop gate sur _sim_tone.enabled) : un slot non abonné
        # SANS générateur n'écrit RIEN dans le shm (mode "idle"), au lieu d'une mire noire/barres
        # parasite à un format par défaut trompeur.
        if _live[idx] or not gen_on or not _HAS_MXL:
            if writer is not None:
                _close()
            if _live[idx]:
                d = _read_stats_raw("/tmp/mtl_v{}.json".format(idx))
                with metrics_lock:
                    # Échec de création de session RX (budget lcores…) : mtl_rx écrit {error} dans le
                    # stats. On le remonte (mode="error" + rx_error) au lieu de prétendre « mtl » à tort.
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
                            # SONDE 2110-21 : surfacer le verdict du timing parser (mtl_v{idx}.json)
                            # sur :8080 — c'est ce que la page /probe (static/probe.js) lit à plat par
                            # receiver. Ces clés N'EXISTENT que si TIMING_PARSER=1 (mtl_rx les écrit) :
                            # un moteur 2110_io normal (parser off) ne les a pas → receiver inchangé.
                            # Verdict `compliant` ABSOLU fiable seulement avec PTP ; sans grandmaster,
                            # lire Cinst + vrx_span (invariants à la dérive, cf. docs/reference/PROBE_2110.md).
                            for _tk in ("compliant", "failed_cause", "cinst_max", "cinst_avg",
                                        "vrx_max", "vrx_min", "vrx_avg", "vrx_span",
                                        "fpt", "latency", "late"):
                                if _tk in d:
                                    metrics[idx][_tk] = d[_tk]
                                else:
                                    metrics[idx].pop(_tk, None)
            else:
                # Non abonné ET générateur off (ou MXL indispo) → rien généré (cf. symétrie audio).
                with metrics_lock:
                    metrics[idx]["mode"] = "idle"
                    metrics[idx].pop("rx_error", None)
                    metrics[idx]["fps"] = 0.0
                    metrics[idx]["rx_latency_ms"] = None
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
    name = "{}_txgen_{}".format(HOSTNAME, _num(idx))
    writer = None; res = None; fi = 0; patch = None; patch_age = 0
    next_t = None   # échéance absolue (monotone) du prochain GRAIN → pacing exact (compense le calcul)
    last_tai = -1   # entrelacé : dernier index TRAME TAI écrit (genlock PTP, anti-doublon/anti-saut)
    slots = {}      # adresse de slot du ring → (signature, échantillon) — cf. _grain_reutilisable
    patch_sig = None  # signature du CONTENU du patch ident (≠ compteur : cf. plus bas)
    def _close():
        nonlocal writer, res
        if writer is not None:
            try: writer.close()
            except Exception: pass
        writer = None; res = None
        # Le ring disparaît avec le writer : ses adresses peuvent être recyclées par un AUTRE flux.
        # Repartir d'un cache vide est OBLIGATOIRE (sinon on « reconnaîtrait » la mémoire d'autrui).
        slots.clear()
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
                # ENTRELACÉ GENLOCK PTP : 2 grains-CHAMPS par trame, aux index CHAMP de la GRILLE TAI
                # (frame_tai×2 + champ). On suit l'index TAI (writer.next_index, cadence trame) AU LIEU
                # d'un compteur monotone → la mire est verrouillée sur le PTP comme le TX → fluide (plus
                # de répétition/saut dû à la dérive txgen(monotone)↔TX(PTP)). Dédoublonnage : on n'écrit
                # qu'aux NOUVEAUX index TAI (même trame → on attend la suivante).
                frame_tai = int(writer.next_index())
                if frame_tai <= last_tai:
                    time.sleep(0.004); continue   # même trame TAI → attendre la suivante
                last_tai = frame_tai
                fh = h // 2
                layf = _layout(w, fh)
                full = None if pat == "field_test" else _get_pattern(pat, frame_tai, lay)
                for fld in (0, 1):
                    _, gi, view = writer.open_grain(index=frame_tai * 2 + fld)
                    # Champ d'une mire statique : le contenu ne dépend que du motif et de la parité.
                    sig = (pat, w, fh, fld, frame_tai if pat in _TXGEN_MOTIFS_DYNAMIQUES else None)
                    if not _grain_reutilisable(slots, view, sig, idx):
                        if pat == "field_test":
                            yy, cbb, crr = _field_test(frame_tai, fld, w, fh)
                        else:
                            yy, cbb, crr = full[0][fld::2], full[1][fld::2], full[2][fld::2]
                        _fill_grain_planes(view, layf, yy, cbb, crr)
                        _noter_grain(slots, view, sig)
                    writer.commit(gi)
            else:
                # IDENT user actif → mtl_rx incrustera l'IDENT sur la mire au passage du feeder TX ;
                # on n'ajoute PAS le libellé auto de la mire (évite le doublon à l'écran).
                with _tx_gen_lock:
                    user_ident = _tx_gen[idx]["ident"]
                if user_ident:
                    patch = None; patch_sig = None
                elif patch_age <= 0 or patch is None:
                    patch = _txgen_ident_patch(idx); patch_age = int(fps)  # recalcul 1× par seconde
                    # Signature du CONTENU, pas un compteur de recalcul : l'ident est re-rendu
                    # chaque seconde mais son texte change rarement. Un compteur ferait réécrire
                    # TOUT le ring 1×/s pour un patch rigoureusement identique.
                    patch_sig = (hash((patch[0].tobytes(), patch[1], patch[2])) if patch else None)
                else:
                    patch_age -= 1
                _, gi, view = writer.open_grain()
                # Le contenu ne dépend QUE du motif, de la géométrie et du patch ident — donc
                # constant tant que les trois le sont (mire noire d'un slot non câblé : toujours).
                # `_get_pattern` n'est appelé QUE si le slot doit réellement être réécrit : pour un
                # motif dynamique il ALLOUE une trame entière à chaque appel.
                sig = (pat, lay["w"], lay["h"], patch_sig,
                       fi if pat in _TXGEN_MOTIFS_DYNAMIQUES else None)
                if not _grain_reutilisable(slots, view, sig, idx):
                    y_arr, cb_arr, cr_arr = _get_pattern(pat, fi, lay)
                    _fill_grain_planes(view, lay, y_arr, cb_arr, cr_arr)
                    _overlay_patch(view, 0, patch, lay)
                    _noter_grain(slots, view, sig)
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
    name = "{}_audio_txgen_{}_{}".format(HOSTNAME, _num(idx), _num(ai))
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
                                             sample_rate=SR)
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


def _simu_audio_loop(idx):
    """Tonalité de SIMULATION sur le FLUX MXL audio du slot ({hn}_audio_{idx}) TANT QUE le slot
    n'est PAS servi par mtl_rx (live) ET que le générateur audio est activé (_sim_tone[idx]).
    Donne un signal aux VU-mètres des consommateurs (multiview…) quand l'entrée est en mire.
    Pendant RX de _txgen_audio_loop : mêmes buffers 1 s (ruptage 0,9 ON / 0,1 OFF), float32 8ch."""
    SR = 48000
    N = SR // 1000                                # 48 éch. = 1 ms
    name = "{}_audio_{}".format(HOSTNAME, _num(idx))
    writer = None; fi = 0
    sig = None; buf_on = None; buf_off = None
    def _close():
        nonlocal writer
        if writer is not None:
            try: writer.close()
            except Exception: pass
        writer = None
    while True:
        with _sim_tone_lock:
            tone = dict(_sim_tone[idx])
        # AUDIO live (mtl_rx possède le flux audio = SDP 2110-30 actif) ou MXL absent ou tonalité off
        # → veille, flux fermé. NB : gating sur la liveness AUDIO, PAS vidéo — un slot à vidéo live
        # mais sans audio reçu DOIT pouvoir servir la tonalité générée (sinon « ni source ni gén »).
        audio_live = _audio_live[idx] if idx < len(_audio_live) else False
        if audio_live or not _HAS_MXL or not tone.get("enabled"):
            if writer is not None: _close()
            sig = None
            time.sleep(0.1)
            continue
        try:
            if writer is None:
                writer = bobimxl.AudioWriter(_mxl(), name, channels=A_CHANNELS,
                                             sample_rate=SR)
            active = (tone.get("active") or [])
            rupted = (tone.get("rupted") or [])
            freq = int(tone.get("freq") or 1000)
            level = float(tone.get("level_db") if tone.get("level_db") is not None else -18.0)
            on_mask  = [bool(active[c]) if c < len(active) else False for c in range(A_CHANNELS)]
            off_mask = [on_mask[c] and not (c < len(rupted) and bool(rupted[c])) for c in range(A_CHANNELS)]
            nsig = (freq, level, tuple(on_mask), tuple(off_mask))
            if nsig != sig:
                buf_on  = _build_tone_second(freq, level, on_mask)
                buf_off = _build_tone_second(freq, level, off_mask)
                sig = nsig
            c = fi % 1000                            # position dans la seconde (ruptage)
            src = buf_on if c < 900 else buf_off
            chunk = src[c * N:(c + 1) * N]
            writer.write(chunk)                      # (48,8) float32 → index TAI interne
        except Exception as e:
            print("simu audio err idx={}: {}".format(idx, e), flush=True)
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
    if _i < N_AUDIO:        # tonalité de simu sur le flux audio dérivé du slot (VU-mètres consommateurs)
        threading.Thread(target=_simu_audio_loop, args=(_i,), daemon=True).start()
threading.Thread(target=_manager_loop, daemon=True).start()
# Présence signal (audit A5) : sampler noir/gel/silence. SIGBUS intercepté (motif multiview) :
# la lecture d'un flux MXL recréé/tronqué par son producteur lèverait sinon un SIGBUS fatal —
# le handler ne fait que marquer ; le sampler rouvre ses readers sur exception au tour suivant.
if _HAS_MXL:
    try:
        signal.signal(signal.SIGBUS, lambda *_a: _sig_bus.set())
    except (ValueError, OSError):
        pass
    threading.Thread(target=_signal_loop, daemon=True).start()
# Générateur TX : un thread vidéo + 2 threads audio par slot TX (restent en veille si gen off).
for _i in range(N_TX):
    threading.Thread(target=_txgen_loop, args=(_i,), daemon=True).start()
    for _ai in range(2):
        threading.Thread(target=_txgen_audio_loop, args=(_i, _ai), daemon=True).start()

print("2110_io (docker/af_xdp) multi-session : {}v iface={} lcores={} ring={}".format(
    N_VIDEO, IFACE, LCORES, V_RING), flush=True)

# Classe 2110-21 annoncée par port : la tracer AU DÉMARRAGE, et dire quand elle est dégradée et
# pourquoi. Une annonce non tenable qui ne se voit dans aucun journal est indétectable côté
# exploitation — c'est exactement comment `TP=2110TPN` a survécu sur des nœuds sans limiteur.
for _if in IFACES:
    _eff = _port_profile_effectif(_if)
    if _eff != _port_profile(_if):
        print("2110-21 {} : classe ANNONCÉE ramenée à '{}' (demandée '{}') — pas de limiteur "
              "matériel sur ce port (pmd={}, pacing={}), narrow ne serait pas tenable".format(
                  _if, _eff, _port_profile(_if), _port_pmd(_if),
                  (os.environ.get("MTL_PACING") or "auto").strip().lower()), flush=True)
    else:
        print("2110-21 {} : classe annoncée '{}' (limiteur matériel : {})".format(
            _if, _eff, "oui" if _pacing_materiel(_if) else "non"), flush=True)

while True:
    time.sleep(3600)

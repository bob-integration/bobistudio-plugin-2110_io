#!/usr/bin/env python3
"""Banc de capacité multi-TX du moteur 2110_io — critère « combien de senders narrow RL tenus ».

Empile des senders sur le VRAI moteur (`controller.py`), UN par UN, via le plan de contrôle
`:8081/tx` (spec complète du sender) — le MÊME chemin que l'orchestrateur. Contrairement à
`mtl_rx --config` en brut (banc 2026-07-10, cf. docs/chantiers/DPDK_NARROW.md §7), on ne balance PAS une rafale de
`st20p_tx_create` : chaque activation passe par le reconcile fichier + debounce du contrôleur, ce qui
évite le commit-storm RL qui accumulait les `st20_tx_queue_fatal_error` → backstop « TX FIGÉ ».

Chaque sender émet sa propre mire txgen (source AUTO-GÉNÉRÉE). À chaque palier on vérifie que (a) le
NOUVEAU sender verrouille (fps>0 sur :8080 sous --lock-timeout) ET (b) TOUS les senders déjà vivants
le restent (aucun ne retombe à fps=0 : c'est la cascade de fatal_error).

⚠ LIMITE MESURÉE (banc dl360-1, 2026-07-10) — cet outil ne mesure PAS proprement le plafond RL :
  • La mire txgen est un thread PYTHON par slot (rendu 1080p + write MXL). Sous GIL, sur les qq lcores
    du moteur, elle NE SOUTIENT PAS 50 fps au-delà de ~2 senders : à 16 slots, `TX_st20p frame get try
    10 succ 0-1` (la SOURCE est affamée, pas le TX) → fps s'effondre à ~1-2. Le vrai moteur tient « 16
    TX » car nourri par des FLUX MXL CÂBLÉS produits en C, pas par des mires Python.
  • L'ajout INCRÉMENTAL d'un sender à un moteur VIF déclenche `mt_dev_tx_queue_fatal_error` + relance
    `mtl_init` (dès le 3ᵉ) ; chaque relance re-discipline le PHC → re-lock PTP ~1,5-2 min (bien plus que
    --lock-timeout). Le moteur RÉCUPÈRE (1-2 senders prouvés stables 50 fps / 2,23 Gb/s chacun), mais
    tous les senders blippent le temps du re-lock.
  → Un vrai banc capacité doit (i) nourrir les senders par de VRAIS flux MXL câblés (C, non Python) et
    (ii) idéalement disposer d'un GM (re-lock PTP quasi instantané). CF. docs/chantiers/DPDK_NARROW.md §7.
Cet outil reste utile pour VALIDER 1-2 senders propres et OBSERVER la dynamique fatal_error/relance.

PRÉREQUIS conteneur : lancer le moteur avec TX_COUNT et ACTIVE_TX_COUNT ≥ --max (sinon les slots
au-delà de ACTIVE_TX_COUNT restent muets par garde-fou budget de files — cf. _tx_gen_apply). En socle
DPDK narrow RL le « mur des 8 » est levé (0.39.6, jusqu'à 63 senders/port) — viser large.

À lancer sur un moteur de BANC (dl360-1), pas sur la prod live. Le banc désactive tout en sortant
(même sur Ctrl-C).

Exemple (dl360-1, rampe 1→40 senders 1080p50 10-bit narrow) :
  tx_scale_bench.py --engine 192.0.2.251 --max 40 --format 1920x1080x50 \\
    --base-mcast 239.100.1.1 --base-port 20000 --settle 8 --stagger 2

Sortie : une ligne par palier (n senders → tx Gb/s, late) + verdict final. Code retour 0 = plafond
--max atteint sans échec, 1 = un palier a échoué (le plafond est le dernier palier stable)."""

import argparse, json, sys, time, urllib.request


def _post(engine, port, path, body):
    req = urllib.request.Request("http://{}:{}{}".format(engine, port, path),
                                 data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)


def _metrics(engine):
    with urllib.request.urlopen("http://{}:8080/".format(engine), timeout=5) as r:
        return json.load(r)


def _mcast_at(base, i):
    """base = 'a.b.c.d' → incrémente les 32 bits de i (chaque sender a un groupe distinct)."""
    parts = [int(x) for x in base.split(".")]
    v = (parts[0] << 24 | parts[1] << 16 | parts[2] << 8 | parts[3]) + i
    return "{}.{}.{}.{}".format((v >> 24) & 255, (v >> 16) & 255, (v >> 8) & 255, v & 255)


def _enable(engine, port8081, idx, mcast, udp_port, fmt):
    """Provisionne + active le sender idx sur sa mire txgen (source auto-générée)."""
    body = {"idx": idx, "enabled": True, "mcast": mcast, "udp_port": udp_port,
            "gen_enabled": True, "gen_pattern": "bars", "fallback_mode": "none",
            "width": fmt["w"], "height": fmt["h"], "fps": fmt["fps"], "bit_depth": fmt["bd"],
            "scan": "i" if fmt["interlaced"] else "p"}
    _post(engine, port8081, "/tx", body)


def _disable(engine, port8081, idx):
    _post(engine, port8081, "/tx", {"idx": idx, "enabled": False, "gen_enabled": False,
                                    "fallback_mode": "none"})


def _live_video_fps(engine):
    """{idx slot TX → fps} pour les senders VIDÉO qui rapportent un fps (session mtl_rx vivante)."""
    out = {}
    for s in _metrics(engine).get("senders", []):
        if s.get("essence") == "video" and s.get("fps") is not None:
            out[s["tx_idx"]] = s["fps"]
    return out


def _wait_live(engine, want_idx, prev_idx, timeout):
    """Attend que TOUS les slots (nouveaux + précédents) soient SIMULTANÉMENT vivants (fps>0).
    Tolère les blips transitoires d'une relance mtl_init/PTP (le moteur relance pour étendre la
    réserve de files quand la demande dépasse le budget gelé → tous les senders re-lockent le PTP) :
    on ne déclare un échec qu'à l'EXPIRATION du timeout, pas sur un creux intermédiaire.
    Retourne (ok, fps_map, nouveaux_muets, precedents_morts)."""
    t0 = time.time()
    fps = {}
    all_idx = list(want_idx) + list(prev_idx)
    while time.time() - t0 < timeout:
        try:
            fps = _live_video_fps(engine)
        except Exception:
            time.sleep(0.5); continue
        if all((fps.get(i) or 0) > 0 for i in all_idx):
            break
        time.sleep(0.5)
    new_dead  = [i for i in want_idx if (fps.get(i) or 0) <= 0]
    prev_dead = [i for i in prev_idx if (fps.get(i) or 0) <= 0]
    return (not new_dead and not prev_dead), fps, new_dead, prev_dead


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True, help="IP du moteur (:8080 métriques / :8081 contrôle)")
    ap.add_argument("--control-port", type=int, default=8081,
                    help="port du plan de contrôle /tx (BASE+1 si CONTROLLER_PORT_BASE décalé)")
    ap.add_argument("--max", type=int, default=32, help="nombre de senders visé (plafond de rampe)")
    ap.add_argument("--start", type=int, default=1, help="premier palier (nb de senders)")
    ap.add_argument("--step", type=int, default=1, help="incrément de senders par palier")
    ap.add_argument("--format", default="1920x1080x50",
                    help="WxHxfps[:i] — fps = cadence IMAGE (25 pour 1080i50) ; « :i » = entrelacé")
    ap.add_argument("--bit-depth", type=int, default=10)
    ap.add_argument("--base-mcast", default="239.100.1.1", help="groupe du 1er sender (incrémenté)")
    ap.add_argument("--base-port", type=int, default=20000, help="port UDP (commun à tous les groupes)")
    ap.add_argument("--stagger", type=float, default=2.0,
                    help="délai entre deux activations d'un même palier (laisse le reconcile grouper)")
    ap.add_argument("--settle", type=float, default=8.0,
                    help="attente après un palier avant vérif (≥ _RELAUNCH_SETTLE_S du contrôleur)")
    ap.add_argument("--lock-timeout", type=float, default=75.0,
                    help="délai max de verrouillage par palier. Un ajout qui dépasse la réserve de "
                         "files gelée déclenche une relance mtl_init (~15 s) + re-lock PTP (~30-40 s) "
                         "qui blippe TOUS les senders — le palier doit tolérer ce cycle avant verdict.")
    ap.add_argument("--warmup-timeout", type=float, default=90.0,
                    help="délai du 1er palier : démarrage à froid = mtl_init + lock PTP (~30-60 s)")
    a = ap.parse_args()

    f = a.format.split(":")
    dims = f[0].split("x")
    fmt = {"w": int(dims[0]), "h": int(dims[1]), "fps": float(dims[2]), "bd": a.bit_depth,
           "interlaced": len(f) > 1 and f[1] == "i"}

    enabled = []          # slots activés jusqu'ici
    last_ok = 0           # dernier palier entièrement stable
    failure = None
    try:
        n = a.start
        while n <= a.max:
            # Active les slots manquants pour atteindre n senders, étalés.
            new = list(range(len(enabled), n))
            for idx in new:
                _enable(a.engine, a.control_port, idx, _mcast_at(a.base_mcast, idx), a.base_port, fmt)
                enabled.append(idx)
                if idx != new[-1]:
                    time.sleep(a.stagger)
            time.sleep(a.settle)

            tmo = a.warmup_timeout if n == a.start else a.lock_timeout
            ok, fps, bad_new, dead_prev = _wait_live(
                a.engine, new, list(range(len(enabled) - len(new))), tmo)
            try:
                nic = _metrics(a.engine).get("nic", {})
                txg = nic.get("tx_gbps")
            except Exception:
                txg = None
            live_n = sum(1 for i in range(n) if (fps.get(i) or 0) > 0)
            print("palier {:>3} senders → {} vivants, tx {} Gb/s{}".format(
                n, live_n, txg if txg is not None else "?",
                "" if ok else "  ⚠ ÉCHEC"), flush=True)

            if not ok:
                failure = {"palier": n, "nouveaux_muets": bad_new, "precedents_morts": dead_prev,
                           "tx_gbps": txg}
                break
            last_ok = n
            n += a.step
    finally:
        for idx in enabled:
            try:
                _disable(a.engine, a.control_port, idx)
            except Exception:
                pass

    print()
    if failure:
        cause = []
        if failure["nouveaux_muets"]:
            cause.append("nouveau(x) sender(s) jamais verrouillé(s): {}".format(failure["nouveaux_muets"]))
        if failure["precedents_morts"]:
            cause.append("cascade — sender(s) déjà vivant(s) retombé(s) à 0: {}".format(
                failure["precedents_morts"]))
        print("VERDICT : PLAFOND = {} senders narrow stables (tx {} Gb/s au palier {}).".format(
            last_ok, failure["tx_gbps"], failure["palier"]))
        print("  échec au palier {} — {}".format(failure["palier"], " ; ".join(cause)))
        print("  interpréter : plafond ~= BP 100G / débit_sender = limite de bande passante ;")
        print("                plafond < BP mais cascade fatal_error = limite files/arbre RL.")
        print("  (rappel : ACTIVE_TX_COUNT du conteneur doit être ≥ {} pour ce test)".format(a.max))
        return 1
    print("VERDICT : OK — {} senders narrow tenus jusqu'au plafond de rampe (--max).".format(last_ok))
    print("  aucun échec : le plafond réel est AU-DELÀ de {}. Relancer avec --max plus haut.".format(a.max))
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Banc de churn RX du moteur 2110_io — critère d'acceptation « robustesse commutation ».

Commute en rafale des slots RX (subscribe/unsubscribe via :8081/nmos/subscribe) et vérifie que
CHAQUE abonnement verrouille (fps > 0 sur :8080) sous --lock-timeout. Un slot qui ne verrouille
jamais = échec (c'est le symptôme « slot figé » du post-mortem Horace 2026-07). À lancer sur des
slots LIBRES avec des groupes que le banc possède (ex. les txgen du moteur lui-même en boucle
switch) — ne pas pointer les slots de production.

Modes :
  --mode serial : slots un par un, N tours (churn doux, mesure du temps de lock par slot)
  --mode storm  : tous les slots (dé)abonnés d'un coup, N tours (le cas qui gelait : reshuffle)

Sources : --source mcast:port:width:height:fps[:i] (répétable ; « :i » = entrelacé, fps = cadence
IMAGE, ex. 25 pour 1080i50). Le SDP minimal est généré (join (*,G), sans source-filter). Les slots
tournent sur les sources en round-robin, décalés à chaque tour → chaque slot change de groupe.

Exemple (Horace, slots libres 15-19 sur les txgen du moteur) :
  churn_bench.py --engine 192.0.2.201 --slots 15,16,17,18,19 --rounds 10 --mode storm \\
    --source 239.141.1.2:2120:1920:1080:25:i --source 239.141.1.3:2120:1920:1080:25:i \\
    --source 239.141.1.4:2120:1920:1080:25:i --source 239.141.1.5:2120:1920:1080:25:i
Sortie : une ligne par (tour, slot) + verdict final. Code retour 0 = tous verrouillés à chaque
tour, 1 = au moins un échec. Le banc désabonne tout en sortant (même sur Ctrl-C)."""

import argparse, json, sys, time, urllib.request


def _post(engine, body):
    req = urllib.request.Request("http://{}:8081/nmos/subscribe".format(engine),
                                 data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as r:
        return json.load(r)


def _metrics(engine):
    with urllib.request.urlopen("http://{}:8080/".format(engine), timeout=5) as r:
        return json.load(r)


def _sdp(src):
    m, p, w, h, f = src["mcast"], src["port"], src["width"], src["height"], src["fps"]
    fmtp = ("sampling=YCbCr-4:2:2; depth=8; width={w}; height={h}; exactframerate={f}; "
            "colorimetry=BT709; PM=2110GPM; SSN=ST2110-20:2017; {itl}TP=2110TPN;").format(
                w=w, h=h, f=f, itl="interlace; " if src["interlaced"] else "")
    return ("v=0\r\no=- 1 1 IN IP4 0.0.0.0\r\ns=churn_bench {m}\r\nt=0 0\r\n"
            "m=video {p} RTP/AVP 96\r\nc=IN IP4 {m}/255\r\n"
            "a=rtpmap:96 raw/90000\r\na=fmtp:96 {fmtp}\r\n"
            "a=mediaclk:direct=0\r\n").format(m=m, p=p, fmtp=fmtp)


def _sub(engine, slot, src):
    _post(engine, {"essence": "video", "receiver_index": slot, "enabled": True, "sdp": _sdp(src)})


def _unsub(engine, slot):
    _post(engine, {"essence": "video", "receiver_index": slot, "enabled": False})


def _wait_locks(engine, slots, timeout):
    """Attend fps>0 pour tous les slots ; retourne {slot: t_lock | None}."""
    t0 = time.time()
    locks = {s: None for s in slots}
    while time.time() - t0 < timeout and any(v is None for v in locks.values()):
        try:
            recs = _metrics(engine)["receivers"]
        except Exception:
            time.sleep(0.5)
            continue
        now = time.time() - t0
        for s in slots:
            if locks[s] is None and s < len(recs) and (recs[s].get("fps") or 0) > 0:
                locks[s] = round(now, 1)
        time.sleep(0.5)
    return locks


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", required=True, help="IP du moteur (:8080/:8081)")
    ap.add_argument("--slots", required=True, help="slots RX à churner, CSV (ex. 15,16,17)")
    ap.add_argument("--source", action="append", required=True,
                    help="mcast:port:width:height:fps[:i] (répétable)")
    ap.add_argument("--rounds", type=int, default=5)
    ap.add_argument("--mode", choices=("serial", "storm"), default="storm")
    ap.add_argument("--lock-timeout", type=float, default=25.0,
                    help="délai max de verrouillage (s) — laisser au watchdog le temps d'UN retry")
    a = ap.parse_args()

    slots = [int(s) for s in a.slots.split(",")]
    srcs = []
    for s in a.source:
        f = s.split(":")
        srcs.append({"mcast": f[0], "port": int(f[1]), "width": int(f[2]), "height": int(f[3]),
                     "fps": f[4], "interlaced": len(f) > 5 and f[5] == "i"})

    failures = []
    try:
        for rnd in range(a.rounds):
            assign = {s: srcs[(i + rnd) % len(srcs)] for i, s in enumerate(slots)}
            if a.mode == "storm":
                for s in slots:
                    _unsub(a.engine, s)
                time.sleep(1.0)
                for s, src in assign.items():
                    _sub(a.engine, s, src)
                locks = _wait_locks(a.engine, slots, a.lock_timeout)
                for s in slots:
                    ok = locks[s] is not None
                    print("tour {:>2} slot {:>2} → {} ({})".format(
                        rnd + 1, s, "lock {}s".format(locks[s]) if ok else "ÉCHEC (jamais verrouillé)",
                        assign[s]["mcast"]), flush=True)
                    if not ok:
                        failures.append((rnd + 1, s, assign[s]["mcast"]))
            else:
                for s in slots:
                    src = assign[s]
                    _unsub(a.engine, s)
                    time.sleep(0.5)
                    _sub(a.engine, s, src)
                    lk = _wait_locks(a.engine, [s], a.lock_timeout)[s]
                    print("tour {:>2} slot {:>2} → {} ({})".format(
                        rnd + 1, s, "lock {}s".format(lk) if lk is not None else
                        "ÉCHEC (jamais verrouillé)", src["mcast"]), flush=True)
                    if lk is None:
                        failures.append((rnd + 1, s, src["mcast"]))
    finally:
        for s in slots:
            try:
                _unsub(a.engine, s)
            except Exception:
                pass

    n = a.rounds * len(slots)
    if failures:
        print("\nVERDICT : ÉCHEC — {}/{} commutations non verrouillées : {}".format(
            len(failures), n, failures))
        return 1
    print("\nVERDICT : OK — {}/{} commutations verrouillées sous {}s".format(n, n, a.lock_timeout))
    return 0


if __name__ == "__main__":
    sys.exit(main())

# 2110_io — the Bobi.Studio ST 2110 engine

*[Version française](README.md)*

A **dual-role** SMPTE ST 2110 engine: reception **and** transmission in a single instance, on
the same network card, through the [Media Transport Library](https://github.com/OpenVisualCloud/Media-Transport-Library)
(MTL/DPDK) in kernel-bypass. `st20p` / `st30p` streams are written and read **zero-copy** into
the [MXL](https://github.com/dmf-mxl/mxl) shared-memory bus.

A component of [Bobi.Studio](https://github.com/bob-integration/bobistudio).

---

## What it does

- **RX and TX on one card** — a single instance per node carries every receive and transmit
  slot: video, audio and ANC. That is what keeps the NIC count down to one.
- **Composable flows**: each RX or TX slot is wired to the MXL bus independently, without
  redeploying the engine.
- **Narrow and wide sender classes** (ST 2110-21), with the SDP's `TP=` following the class
  actually applied.
- **Field-native interlaced chain**, with no detour through an intermediate progressive frame.
- **NMOS IS-04 / IS-05**: every slot registers as a Receiver or a Sender, and connects from any
  controller.
- **ST 2110 measurements published** on its metrics port: `fpt`, `VRX`, `Cinst`, the compliance
  verdict and PTP state — enough to check a chain without an external analyser.

Qualified hardware is the **Intel E810** NIC, in PF, DPDK or AF-XDP. It is the only one it has
ever run on.

---

## The libmtl patches

The image applies a set of patches to the Media Transport Library at build time. They address
behaviour that, in broadcast production, shows on air: frequency discipline of the PHC,
stopping transmission at the right moment for true 2022-7 hitless, TX reset without dropping a
frame, the Router Alert option for IGMP.

They live in `docker/`, one script per patch, each stating what it inserts and why. Worth
knowing if you are comparing versions: the image does not carry a stock `libmtl`.

---

## Using it

This repository is a **plugin** of Bobi.Studio, mounted at `plugins/2110_io/`. It is deployed
from the orchestrator onto an enrolled node — never by hand. `help.md` is the help article
rendered inside the product: it covers slot sizing, the oversubscription traps and how to read
the counters.

It is not usable on its own: it reads its configuration and wiring from the orchestrator.

---

## Benches

```bash
python3 tools/churn_bench.py --engine <ip> --slots 15,16,17 --rounds 10
python3 tools/tx_scale_bench.py
```

Run by hand against a live engine — they need a real card and real streams, so they have no
place in continuous integration.

---

## Licence

GPL-3.0-or-later — see [LICENSE](LICENSE). Copyright © 2026 BOBI SAS, France.

The Media Transport Library is published by Intel under BSD-3-Clause; the patches in `docker/`
apply to its sources at build time and do not redistribute it.

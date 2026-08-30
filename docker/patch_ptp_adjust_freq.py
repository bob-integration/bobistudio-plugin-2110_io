#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# ACTIVE L'ASSERVISSEMENT EN FRÉQUENCE DU PHC — la branche existe, elle n'était jamais compilée.
#
# ── Ce qui a été mesuré (banc dl360-1, 2026-08-30) ─────────────────────────────────────────────
# Offset corrigé au GM : moyenne |dérive| 726 ns, pic 1,26 µs. Delta BRUT (PHC↔GM) : 1,7 à 2,5 µs.
# Le commentaire de libmtl annonce pourtant « ~±30 ns ». Deux mesures indépendantes convergent vers
# une erreur de RYTHME d'environ 7 ppm : celle-ci, et la dérive de `fpt` relevée le 2026-08-27
# (+7,00 µs/s sur TOUS les récepteurs, y compris notre propre TX relu en boucle interne).
#
# ── La cause ───────────────────────────────────────────────────────────────────────────────────
# `mt_ptp.c` sait asservir la FRÉQUENCE du PHC — `ptp_timesync_adjust_freq()` + le servo PI avec sa
# machine d'états JUMP/LOCKED. Tout ce code est derrière `#ifdef MTL_HAS_DPDK_TIMESYNC_ADJUST_FREQ`,
# et ce macro n'est défini NULLE PART dans le dépôt libmtl (ni meson.build, ni en-tête). Vérifié
# aussi sur notre image construite : `libmtl.so` ne contient aucun symbole `timesync_adjust_freq`.
# La branche compilée est donc le `#else` : `ptp_timesync_adjust_time()` SEUL, c'est-à-dire un SAUT
# DE PHASE à chaque message Sync et jamais une correction de rythme.
#
# D'où la dent de scie : l'erreur de fréquence se reconstitue entre deux Sync.
#     7 ppm × 125 ms (logSync −3)  =  875 ns   ← et on mesure 726 ns de moyenne, 1,26 µs de pic.
# Le modèle prédit la mesure. C'est aussi pourquoi le verrou servo strict (< 100 ns en continu) ne
# s'arme jamais : on a longtemps mis ce non-verrouillage sur le compte de l'E810, alors que c'est le
# servo qui signalait qu'il n'avait pas convergé.
#
# ── Pourquoi c'est activable ───────────────────────────────────────────────────────────────────
#  · Le PMD `ice` de DPDK IMPLÉMENTE `timesync_adjust_freq` (table d'ops de ice_ethdev.c).
#  · Les UNITÉS concordent : libmtl passe `ppb * 65.536` = ppm × 2^16, et ice divise par
#    `1000000ULL << 16` — c'est bien du « scaled ppm » des deux côtés, rien à convertir.
#  · `ptp_timesync_adjust_freq()` est défini DANS le même #ifdef : activer le macro amène le helper,
#    aucun symbole manquant.
#  · Repli intégré : `if (ret) ptp_timesync_adjust_time(ptp, delta);` — si l'ajustement de fréquence
#    échoue, on retombe sur le comportement actuel. Le pire cas est donc l'état d'aujourd'hui.
#
# ⚠ CE PATCH CHANGE LE COMPORTEMENT DE L'HORLOGE, pas seulement une métrique : il met en service le
# servo PI (JUMP puis LOCKED). À VALIDER AU BANC avant toute prod — mesurer l'offset sur au moins
# une heure, et vérifier qu'aucun saut de phase brutal ne traverse les flux en cours.

import sys

F = "lib/src/mt_ptp.c"
MARK = "bobi.studio: active l'asservissement en frequence"

c = open(F).read()
if MARK in c:
    print("patch PTP adjust_freq : déjà appliqué"); sys.exit(0)

ANCRE = "#ifdef MTL_HAS_DPDK_TIMESYNC_ADJUST_FREQ"
if ANCRE not in c:
    print("patch PTP adjust_freq : ERREUR — ancre '%s' introuvable "
          "(la branche a peut-être été retirée en amont : NE PAS forcer)" % ANCRE)
    sys.exit(1)
if "rte_eth_timesync_adjust_freq" not in c:
    print("patch PTP adjust_freq : ERREUR — appel rte_eth_timesync_adjust_freq absent")
    sys.exit(1)

# Défini AVANT la première occurrence, donc avant les trois usages (helper, servo, garde phc2sys).
c = c.replace(ANCRE,
              "/* %s : la branche freq existe mais son macro n'est defini nulle part en amont.\n"
              " * Sans lui, le servo ne fait que SAUTER la phase a chaque Sync et l'erreur de\n"
              " * rythme (~7 ppm mesures sur E810) se reconstitue entre deux — d'ou une dent de\n"
              " * scie de ~875 ns a logSync -3. Le PMD ice implemente l'op et les unites\n"
              " * concordent (scaled ppm des deux cotes). */\n"
              "#define MTL_HAS_DPDK_TIMESYNC_ADJUST_FREQ 1\n\n" % MARK
              + ANCRE, 1)
open(F, "w").write(c)
print("patch PTP adjust_freq : OK (asservissement en fréquence du PHC activé)")

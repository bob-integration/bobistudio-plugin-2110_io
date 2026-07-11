#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# EXPORTE UN GETTER D'ÉTAT « PTP STABLE » (mt_bobi_ptp_stable) POUR LE BACKSTOP TX DE mtl_rx.
#
# ── Pourquoi (G4 2026-07-10/11, dl360-1) ───────────────────────────────────────────────────────
# Avec ENGINE_PTP=libmtl, le démarrage du train de pacing TX attend le PTP stable
# (`mt_ptp_wait_stable`, st_tx_video_session.c:372, timeout 180 s) et ÉCHOUE au timeout
# (`if (ret < 0) return ret`) : GM absent/lent → AUCUNE frame TX. Le backstop « TX FIGÉ » de
# mtl_rx.c (5 s) confondait cette attente légitime avec un wedge de queue et redémarrait le daemon
# en boucle — chaque restart remettant la convergence PTP à zéro. Une grâce à durée FIXE ne couvre
# pas le cas « GM absent au boot » (la boucle revient à l'expiration). La bonne sémantique est un
# gate sur l'ÉTAT : backstop suspendu tant que le PTP interne n'est pas synchrone.
#
# ── Le fix ─────────────────────────────────────────────────────────────────────────────────────
# Ajouter à mt_ptp.c une fonction NON statique (liée dans libmtl.so, appelée par mtl_rx via extern) :
# `mt_bobi_ptp_stable(impl, port)` → true si le PTP interne est inactif (pas de gate) OU RÉELLEMENT
# LOCKÉ. Critère = `ptp->locked` : libmtl le met à true quand le max delta reste continûment sous
# 100 ns (mt_ptp.c « Be considered as locked while the max delta is continuously below 100ns »), et
# ne le remet à false qu'au (ré)init du port. ⚠ NE PAS utiliser `delta_result_cnt > 5` (critère de
# `mt_ptp_wait_stable`) : ce compteur atteint >5 DÈS LA CONVERGENCE (offset encore ~ms), donc le
# backstop se réarmerait AVANT le lock et retuerait le daemon en boucle (mesuré G4 2026-07-11 03h,
# 0.39.15 : offset 37 s→0,5 ms « not locked », cnt 16, backstop tire à 32 s). Champs lus :
# `ptp->active`, `ptp->locked` (struct mt_ptp_impl, mt_main.h). Ajout pur en fin de fichier.

import sys

F = "lib/src/mt_ptp.c"
MARK = "bobi.studio: getter PTP stable"

c = open(F).read()
if MARK in c:
    print("patch PTP stable getter : déjà appliqué"); sys.exit(0)

for needle in ("struct mt_ptp_impl* ptp = mt_get_ptp(impl, port);",
               "ptp->locked = true", "ptp->active"):
    if needle not in c:
        print("patch PTP stable getter : ERREUR — ancre '%s' introuvable" % needle)
        sys.exit(1)

c += """
/* %s : état de synchro du PTP interne, pour le backstop « TX FIGÉ » de mtl_rx.
 * true = pas de gate (PTP interne inactif) OU RÉELLEMENT lockÉ (ptp->locked : max delta < 100 ns
 * en continu ; posé par le servo, remis à false seulement au (ré)init du port). PAS delta_result_cnt
 * (>5 dès la convergence → réarmerait le backstop avant le lock). Appelé depuis mtl_rx via extern. */
bool mt_bobi_ptp_stable(struct mtl_main_impl* impl, enum mtl_port port) {
  struct mt_ptp_impl* ptp = mt_get_ptp(impl, port);
  if (!ptp || !ptp->active) return true;
  return ptp->locked;
}
""" % MARK

open(F, "w").write(c)
print("patch PTP stable getter : OK (mt_bobi_ptp_stable ajouté à mt_ptp.c)")

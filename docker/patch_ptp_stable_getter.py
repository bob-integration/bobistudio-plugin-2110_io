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
# `mt_bobi_ptp_stable(impl, port)` → true si le PTP interne est inactif (pas de gate) ou s'il est
# connecté avec ≥ 6 mesures de delta (même critère de stabilité que `mt_ptp_wait_stable`,
# `delta_result_cnt > 5`). Champs lus : `ptp->active`, `ptp->connected`, `ptp->delta_result_cnt`
# (struct mt_ptp_impl, mt_main.h). Aucune modification du code existant : ajout pur en fin de
# fichier.

import sys

F = "lib/src/mt_ptp.c"
MARK = "bobi.studio: getter PTP stable"

c = open(F).read()
if MARK in c:
    print("patch PTP stable getter : déjà appliqué"); sys.exit(0)

for needle in ("struct mt_ptp_impl* ptp = mt_get_ptp(impl, port);",
               "delta_result_cnt", "->connected"):
    if needle not in c:
        print("patch PTP stable getter : ERREUR — ancre '%s' introuvable" % needle)
        sys.exit(1)

c += """
/* %s : état de synchro du PTP interne, pour le backstop « TX FIGÉ » de mtl_rx.
 * true = pas de gate (PTP interne inactif) OU synchrone (connecté + >5 mesures de delta,
 * même critère que mt_ptp_wait_stable). Appelé depuis mtl_rx via extern (symbole exporté). */
bool mt_bobi_ptp_stable(struct mtl_main_impl* impl, enum mtl_port port) {
  struct mt_ptp_impl* ptp = mt_get_ptp(impl, port);
  if (!ptp || !ptp->active) return true;
  return ptp->connected && (ptp->delta_result_cnt > 5);
}
""" % MARK

open(F, "w").write(c)
print("patch PTP stable getter : OK (mt_bobi_ptp_stable ajouté à mt_ptp.c)")

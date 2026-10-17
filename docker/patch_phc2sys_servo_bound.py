#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh, APRÈS
# patch_ptp_adjust_freq).
#
# BORNE LE SERVO phc2sys DE libmtl (CLOCK_REALTIME ← PHC, `ENGINE_PHC2SYS=1`).
#
# ── Ce qui a été mesuré (dl360-1) ──────────────────────────────────────────────────────────────
# 2026-10-16 23:4x : REALTIME à +396 s de l'UTC, tick 10122 (+1,22 %).
# 2026-10-17 23:12 : REALTIME à −455 s de l'UTC (−492 s par rapport au TAI attendu), tick 9840
#                    (−1,6 %), −16 ms/s. Le PTP PHC↔GM, lui, restait verrouillé (±50 ns).
# Dans les deux cas le journal du moteur répétait `ptp_adj_system_clock_freq(0), adj system time
# freq fail -1` à chaque cycle, et seule une relance du moteur sortait de l'état.
#
# ── Le mécanisme (lu dans mt_ptp.c, SHA épinglé) ───────────────────────────────────────────────
# `phc2sys_adjust` → `pi_sample` : UN saut de phase (count 3) au démarrage, puis uniquement de la
# fréquence — ppb = 0,3·offset + drift ; drift += 0,7·offset — SANS borne, SANS anti-windup, SANS
# nouveau saut. `ptp_adj_system_clock_freq` traduit en tick + freq et appelle `clock_adjtime`. Le
# noyau REFUSE (EINVAL) un tick hors de [900000/USER_HZ ; 1100000/USER_HZ] (±10 %) : l'erreur est
# seulement journalisée, l'horloge garde la dernière vitesse ACCEPTÉE, l'offset croît, l'intégrateur
# continue d'intégrer → la demande ne peut plus revenir dans la plage. Emballement définitif.
# Il suffit pour l'amorcer d'un écart de quelques ms : à la relance du 17/10 23:15, une excursion
# du PHC de −3,9 ms en phase de convergence a fait demander un tick de 9446 (−5,5 %) — et 19 « freq
# fail » sont sortis en 2 s avant que la demande ne revienne dans la plage. Un écart deux fois plus
# grand ne revient pas.
#
# ── Ce que ce patch change (et rien d'autre) ──────────────────────────────────────────────────
#  1. BORNE : la demande est ramenée dans ce que le noyau accepte (tick ∈ [900000/hz ; 1100000/hz],
#     soit ±10 % ; sans ADJ_TICK, ±500 ppm). Elle n'est donc plus jamais refusée pour cause de plage.
#  2. ANTI-WINDUP : quand la commande est bornée OU refusée, l'intégration du cycle est annulée
#     (drift ramené à sa valeur d'avant pi_sample) — l'intégrateur ne court plus après une
#     correction qui n'est pas appliquée.
#  3. RE-SAUT : servo verrouillé (count ≥ 4) et |offset| > 1 ms → saut de phase
#     (`ptp_adj_system_clock_time`), vitesse noyau remise au nominal, servo réinitialisé (il refait
#     la même acquisition qu'au démarrage : 2 mesures, estimation, saut, verrouillage). Le test est
#     fait AVANT pi_sample : l'écart hors norme n'est jamais intégré.
#     Pourquoi 1 ms : (a) c'est ~3 000× l'écart d'un servo sain (verrouillé = < 300 ns au sens de
#     libmtl ; mesuré ±50 ns sur dl360-1) — jamais atteint en régime ; (b) au-delà, la correction
#     linéaire (0,7·offset par cycle) demande > 700 ppm, hors de la plage ADJ_FREQUENCY du noyau
#     (±500 ppm) : c'est exactement là que le servo sort de son domaine linéaire ; (c) c'est 1/20
#     d'une trame à 50 Hz : un saut de 1 ms ne déplace un index de grain MXL (dérivé de CLOCK_TAI =
#     REALTIME sur ce nœud) que d'au plus un.
#  4. JOURNAL (anglais, warn) : un re-saut, l'entrée en saturation et la sortie de saturation, avec
#     l'offset — au plus une ligne par 10 s et par événement, avec le compte des cycles tus.
#
# Le servo du PHC (ptp->servo, même pi_sample) n'est PAS touché : la carte accepte de grandes
# corrections, c'est la borne du NOYAU qui rend le servo REALTIME non linéaire.

import sys

F = "lib/src/mt_ptp.c"
FH = "lib/src/mt_main.h"
MARK = "bobi.studio: phc2sys servo borne"

c = open(F).read()
if MARK in c:
    print("patch phc2sys servo bound : déjà appliqué"); sys.exit(0)


def unique(texte, ancre, ou):
    n = texte.count(ancre)
    if n != 1:
        print("patch phc2sys servo bound : ERREUR — ancre vue %d fois dans %s (il en faut "
              "EXACTEMENT une) : %r" % (n, ou, ancre[:90]))
        sys.exit(1)


# ── 1) champs d'état dans mt_phc2sys_impl ─────────────────────────────────────────────────────
h = open(FH).read()
ANCRE_H = "  bool locked;\n  uint16_t stat_sync_keep;\n};"
unique(h, ANCRE_H, FH)
h = h.replace(ANCRE_H,
              "  bool locked;\n  uint16_t stat_sync_keep;\n"
              "  /* %s : etat du bornage (saturation, re-saut, journal limite) */\n"
              "  bool bobi_saturated;\n"
              "  uint64_t bobi_sat_cnt;\n"
              "  uint64_t bobi_sat_log_ns;\n"
              "  uint64_t bobi_step_cnt;\n"
              "  uint64_t bobi_step_log_ns;\n"
              "};" % MARK, 1)
open(FH, "w").write(h)

# ── 2) ptp_adj_system_clock_freq rend son statut ──────────────────────────────────────────────
SIG = "static void ptp_adj_system_clock_freq(struct mt_ptp_impl* ptp, double ppb) {"
unique(c, SIG, F)
c = c.replace(SIG, "static int ptp_adj_system_clock_freq(struct mt_ptp_impl* ptp, double ppb) {", 1)

FIN = ("          __func__, ptp->port);\n  }\n}\n\n"
       "static void phc2sys_adjust(struct mt_ptp_impl* ptp) {")
unique(c, FIN, F)

HELPERS = r'''
/* %(mark)s — cf. plugins/2110_io/docker/patch_phc2sys_servo_bound.py.
 * Seuil de re-saut : au-dela, la correction lineaire sort de la plage ADJ_FREQUENCY du noyau. */
#define BOBI_PHC2SYS_STEP_NS (1000 * 1000)
#define BOBI_PHC2SYS_LOG_PERIOD_NS (10ull * NS_PER_S)

/* Plage de frequence (ppb) que clock_adjtime ACCEPTE, avec la conversion tick + freq de
 * ptp_adj_system_clock_freq : kernel/time/ntp.c refuse (EINVAL) un tick hors de
 * [900000/USER_HZ ; 1100000/USER_HZ]. Les bornes sont des multiples exacts de 1e3*hz, donc
 * round(borne/1e3/hz) tombe pile sur le tick extreme et le reliquat freq vaut 0. Sans ADJ_TICK,
 * le noyau ecrete ADJ_FREQUENCY a +-500 ppm sans refuser. */
static void bobi_phc2sys_ppb_bounds(struct mt_ptp_impl* ptp, double* lo, double* hi) {
  long hz = ptp->phc2sys.realtime_hz;
  long nominal = ptp->phc2sys.realtime_nominal_tick;
  if (nominal && hz > 0) {
    *lo = 1e3 * hz * (double)(900000 / hz - nominal);
    *hi = 1e3 * hz * (double)(1100000 / hz - nominal);
  } else {
    *lo = -500000.0;
    *hi = 500000.0;
  }
}

/* Re-saut : servo deja verrouille et |offset| au-dela du seuil -> saut de phase, vitesse noyau au
 * nominal, servo reinitialise (meme acquisition qu'au demarrage). Appele AVANT pi_sample : l'ecart
 * hors norme n'est jamais integre. Rend true si le saut a eu lieu. */
static bool bobi_phc2sys_restep(struct mt_ptp_impl* ptp, int64_t offset) {
  struct mt_phc2sys_impl* p = &ptp->phc2sys;
  if (p->servo.count < 4) return false; /* acquisition : le JUMP amont s'en charge */
  if (offset < BOBI_PHC2SYS_STEP_NS && offset > -BOBI_PHC2SYS_STEP_NS) return false;

  ptp_adj_system_clock_time(ptp, -offset);
  memset(&p->servo, 0, sizeof(p->servo));
  ptp_adj_system_clock_freq(ptp, 0.0);
  p->bobi_saturated = false;
  p->bobi_sat_cnt = 0;
  p->bobi_step_cnt++;

  uint64_t now = mt_get_monotonic_time();
  if (!p->bobi_step_log_ns || now - p->bobi_step_log_ns >= BOBI_PHC2SYS_LOG_PERIOD_NS) {
    warn("%%s(%%d), phc2sys: CLOCK_REALTIME offset %%" PRId64
         " ns beyond step threshold %%d ns: stepped, frequency reset to nominal, servo "
         "restarted (%%" PRIu64 " steps since engine start)\n",
         __func__, ptp->port, offset, BOBI_PHC2SYS_STEP_NS, p->bobi_step_cnt);
    p->bobi_step_log_ns = now;
  }
  return true;
}

/* Etat LOCKED : applique la demande BORNEE ; si elle a du etre bornee ou si le noyau l'a refusee,
 * l'integration du cycle est annulee (anti-windup). */
static void bobi_phc2sys_apply_freq(struct mt_ptp_impl* ptp, double want, int64_t offset,
                                    double drift_before) {
  struct mt_phc2sys_impl* p = &ptp->phc2sys;
  double lo, hi, cmd = want;
  bobi_phc2sys_ppb_bounds(ptp, &lo, &hi);
  if (cmd > hi) cmd = hi;
  if (cmd < lo) cmd = lo;
  bool clamped = (cmd != want);
  int ret = ptp_adj_system_clock_freq(ptp, cmd);
  uint64_t now = mt_get_monotonic_time();

  if (clamped || ret < 0) {
    p->servo.drift = drift_before;
    p->bobi_sat_cnt++;
    if (!p->bobi_saturated || now - p->bobi_sat_log_ns >= BOBI_PHC2SYS_LOG_PERIOD_NS) {
      warn("%%s(%%d), phc2sys: CLOCK_REALTIME frequency demand %%.0f ppb %%s (kernel range "
           "%%.0f..%%.0f ppb), integrator held, offset %%" PRId64 " ns, %%" PRIu64
           " saturated cycles\n",
           __func__, ptp->port, want, ret < 0 ? "REFUSED by kernel" : "clamped", lo, hi,
           offset, p->bobi_sat_cnt);
      p->bobi_sat_log_ns = now;
    }
    p->bobi_saturated = true;
  } else if (p->bobi_saturated) {
    warn("%%s(%%d), phc2sys: CLOCK_REALTIME frequency demand back within kernel range after "
         "%%" PRIu64 " saturated cycles (demand %%.0f ppb, offset %%" PRId64 " ns)\n",
         __func__, ptp->port, p->bobi_sat_cnt, want, offset);
    p->bobi_saturated = false;
    p->bobi_sat_cnt = 0;
  }
}

''' % {"mark": MARK}

c = c.replace(FIN,
              "          __func__, ptp->port);\n  }\n  return ret;\n}\n" + HELPERS +
              "static void phc2sys_adjust(struct mt_ptp_impl* ptp) {", 1)

# ── 3) phc2sys_adjust : re-saut avant pi_sample, commande bornée en LOCKED ─────────────────────
SAMPLE = "    ppb = pi_sample(&ptp->phc2sys.servo, offset, t_sys, &state);\n"
unique(c, SAMPLE, F)
c = c.replace(SAMPLE,
              "    /* %s : re-saut AVANT integration, drift memorise pour l'anti-windup */\n"
              "    double bobi_drift_before = ptp->phc2sys.servo.drift;\n"
              "    if (bobi_phc2sys_restep(ptp, offset)) {\n"
              "      ppb = 0.0;\n"
              "      state = UNLOCKED; /* deja corrige : rien d'autre a appliquer ce cycle */\n"
              "    } else {\n"
              "      ppb = pi_sample(&ptp->phc2sys.servo, offset, t_sys, &state);\n"
              "    }\n" % MARK, 1)

LOCKED = "      case LOCKED:\n        ptp_adj_system_clock_freq(ptp, -ppb);\n"
unique(c, LOCKED, F)
c = c.replace(LOCKED,
              "      case LOCKED:\n"
              "        bobi_phc2sys_apply_freq(ptp, -ppb, offset, bobi_drift_before);\n", 1)

# Contrôle de sortie : plus aucun appel direct non borné en état LOCKED.
if "ptp_adj_system_clock_freq(ptp, -ppb)" in c:
    print("patch phc2sys servo bound : ERREUR — appel non borné encore présent"); sys.exit(1)

open(F, "w").write(c)
print("patch phc2sys servo bound : OK (borne noyau + anti-windup + re-saut > 1 ms)")

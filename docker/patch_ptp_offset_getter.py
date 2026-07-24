#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# EXPORTE L'OFFSET PTP interne de libmtl (mt_bobi_ptp_offset) POUR LES MÉTRIQUES :8080.
#
# ── Pourquoi (banc PTP DPDK 2026-07) ───────────────────────────────────────────────────────────
# En socle full-PF DPDK le port E810 est en vfio-pci : ptp4l kernel N'EXISTE PAS, le PTP est fait
# DANS libmtl (ENGINE_PTP=libmtl). L'onglet « Réglages → Réseau → Réseau 2110 - PTP » reste donc
# VIDE (rien à lire via pmc/ptp4l). On expose déjà le LOCK (mt_bobi_ptp_stable) et le GRANDMASTER
# (mt_bobi_ptp_gm) ; il MANQUE l'OFFSET numérique pour tracer le verrouillage/la dérive (graphe).
# L'offset n'existe que dans le LOG moteur (« system clock offset max N, locked ») — bon pour la
# qualif one-shot (nic_qualify) mais mauvais pour un graphe live. On ajoute donc un getter dédié.
#
# ── Le fix ─────────────────────────────────────────────────────────────────────────────────────
# Ajouter à mt_ptp.c une fonction NON statique (liée dans libmtl.so, appelée par mtl_rx via extern) :
# `mt_bobi_ptp_offset(impl, port, out_ns)` → remplit out_ns avec le MAX delta de la fenêtre de stats
# courante (`ptp->stat_delta_max`, l'offset servo NIC-PHC↔GM — c'est CE champ qui alimente la ligne
# « offset max » du log, cf. mt_ptp.c ptp_print_port_stats). true SSI le PTP interne est actif ET a
# mesuré au moins un delta dans la fenêtre (`stat_delta_cnt > 0`) — garde-fou contre la valeur
# sentinelle INT_MIN posée par ptp_stat_clear entre deux fenêtres. Émis MÊME quand non verrouillé
# (stat_delta_max reflète la convergence). Ajout pur en fin de fichier.

import sys

F = "lib/src/mt_ptp.c"
MARK = "bobi.studio: export offset PTP"

c = open(F).read()
if MARK in c:
    print("patch PTP offset getter : déjà appliqué"); sys.exit(0)

for needle in ("struct mt_ptp_impl* ptp = mt_get_ptp(impl, port);",
               "stat_delta_max", "stat_delta_cnt",
               "stat_correct_delta_sum", "stat_correct_delta_cnt",
               "stat_path_delay_sum", "stat_path_delay_cnt",
               "correct_delta /= 2;"):
    if needle not in c:
        print("patch PTP offset getter : ERREUR — ancre '%s' introuvable" % needle)
        sys.exit(1)

# ── Offset SIGNÉ instantané ─────────────────────────────────────────────────────────────────────
# stat_correct_delta_sum est accumulé en labs() → sa moyenne est l'AMPLITUDE (toujours positive),
# PAS l'offset. Le « offset from master » de ptp4l est SIGNÉ et oscille autour de 0. On stocke donc
# le DERNIER correct_delta signé dans la struct (champ ajouté à mt_main.h) et on l'expose tel quel.
FH = "lib/src/mt_main.h"
h = open(FH).read()
if "bobi_last_correct_delta" not in h:
    if "int64_t stat_correct_delta_sum;" not in h:
        print("patch PTP offset getter : ERREUR — ancre struct 'int64_t stat_correct_delta_sum;' introuvable dans mt_main.h")
        sys.exit(1)
    h = h.replace(
        "int64_t stat_correct_delta_sum;",
        "int64_t stat_correct_delta_sum;\n"
        "  int64_t bobi_last_correct_delta; /* bobi.studio: dernier correct_delta SIGNÉ (offset from master instantané, ns) */\n"
        "  bool bobi_has_correct_delta;     /* bobi.studio: true dès la 1re mesure (garde-fou du getter) */",
        1)
    open(FH, "w").write(h)

# mt_ptp.c : capter la valeur signée là où correct_delta est calculé (juste après « correct_delta /= 2; »).
c = c.replace(
    "correct_delta /= 2;",
    "correct_delta /= 2;\n"
    "  ptp->bobi_last_correct_delta = correct_delta; /* bobi.studio: offset signé instantané */\n"
    "  ptp->bobi_has_correct_delta = true;",
    1)

c += """
/* %s : trois getters PTP internes (ns) pour les métriques :8080 quand le ptp4l kernel est absent —
 * socle DPDK. TOUS non-statiques (liés dans libmtl.so, appelés par mtl_rx via extern). true SSI le
 * PTP est actif ET a au moins une mesure dans la fenêtre de stats courante (cnt > 0 ; entre deux
 * fenêtres ptp_stat_clear pose des sentinelles → cnt=0 = valeur inconnue). Aucun n'est gaté sur
 * ptp->locked : les valeurs doivent tracer la CONVERGENCE, y compris avant le lock.
 *
 * ⚠ TROIS mesures DISTINCTES, ne pas confondre (banc PTP DPDK 2026-07, dl360-1) :
 *  · mt_bobi_ptp_offset        = stat_delta_max        = delta BRUT (offset PHC↔GM AVANT correction
 *                                logicielle). C'est le champ qui pilote ptp->locked (< 100 ns continu).
 *                                Sur E810 DPDK il reste ~1,3 µs (discipline HW du PHC non convergente,
 *                                MTL corrige en LOGICIEL via le coefficient) → mauvais pour l'UI
 *                                « offset », bon comme diagnostic « pourquoi pas de lock strict ».
 *  · mt_bobi_ptp_correct_offset = correct_delta SIGNÉ instantané = offset CORRIGÉ (path-delay +
 *                                coefficient), l'équivalent du « offset from master » de ptp4l : SIGNÉ,
 *                                oscille autour de 0 (~±30 ns). C'EST la valeur à afficher comme offset.
 *                                (⚠ PAS la moyenne de |correct_delta| : stat_correct_delta_sum est
 *                                accumulé en labs() → ce serait l'AMPLITUDE, toujours positive, pas
 *                                l'offset. On lit donc bobi_last_correct_delta, capté signé au calcul.)
 *  · mt_bobi_ptp_path_delay     = moy(path_delay)      = mean path delay (~168 ns) — le champ manquant
 *                                de l'onglet PTP. TOUJOURS positif (délai de transit physique). */
bool mt_bobi_ptp_offset(struct mtl_main_impl* impl, enum mtl_port port, int64_t* out_ns) {
  struct mt_ptp_impl* ptp = mt_get_ptp(impl, port);
  if (!ptp || !ptp->active || ptp->stat_delta_cnt == 0) return false;
  if (out_ns) *out_ns = ptp->stat_delta_max;
  return true;
}
bool mt_bobi_ptp_correct_offset(struct mtl_main_impl* impl, enum mtl_port port, int64_t* out_ns) {
  struct mt_ptp_impl* ptp = mt_get_ptp(impl, port);
  if (!ptp || !ptp->active || !ptp->bobi_has_correct_delta) return false;
  if (out_ns) *out_ns = ptp->bobi_last_correct_delta;
  return true;
}
bool mt_bobi_ptp_path_delay(struct mtl_main_impl* impl, enum mtl_port port, int64_t* out_ns) {
  struct mt_ptp_impl* ptp = mt_get_ptp(impl, port);
  if (!ptp || !ptp->active || ptp->stat_path_delay_cnt == 0) return false;
  if (out_ns) *out_ns = ptp->stat_path_delay_sum / ptp->stat_path_delay_cnt;
  return true;
}
""" % MARK

open(F, "w").write(c)
print("patch PTP offset getter : OK (offset brut + correct_offset + path_delay ajoutés à mt_ptp.c)")

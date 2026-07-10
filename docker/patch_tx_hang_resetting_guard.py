#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# SUPPRIME LE fatal_error / relance à la CRÉATION d'une session RL TX (blip PTP de toute la flotte).
#
# ── Cause racine (banc dl360-1, 2026-07-10) ───────────────────────────────────────────────────
# Créer une session sender RL appelle dev_tx_queue_set_rl_rate → rte_tm_hierarchy_commit, que le
# PMD ice implémente en STOPPANT tout le port (dev_started=0) ~100 ms→1 s le temps de recommitter
# l'arbre traffic-manager. libmtl encadre ce stop par inf->resetting=true/false (PORT-WIDE).
#   • patch_rx_resetting_guard : rend le stop SURVIVABLE (mt_rxq_burst/mt_txq_burst renvoient 0 au
#     lieu de segfault sur un port not-ready). MAIS il ne fait que renvoyer 0…
#   • …et ce 0 fait tomber les AUTRES sessions TX déjà vivantes dans video_trs_burst_fail
#     (st_video_transmitter.c) : mt_txq_burst()==0 → burst_fail. Si le stop dure > le seuil de hang
#     (tx_hang_detect_time_thresh, défaut NS_PER_S = 1000 ms), il déclenche st20_tx_queue_fatal_error
#     → masque/réassigne la queue → et le backstop « TX FIGÉ » de mtl_rx.c (_exit) → le contrôleur
#     relance mtl_init → RE-LOCK PTP → TOUTE la flotte TX blippe ~2 min. Symptôme : chaque ajout de
#     sender (dès le 3ᵉ) fait cligner tous les autres.
#
# ── Le patch : rendre le détecteur de hang TX conscient de `resetting` ────────────────────────
# En tête de video_trs_burst_fail (et de son jumeau audio st_audio_trs_burst_fail), si le port est
# en reset transitoire (rte_tm_hierarchy_commit d'une AUTRE session en cours), ce n'est PAS un hang
# de CETTE session : on repousse le repère de succès (last_burst_succ_time_tsc) et on « skip » les
# paquets courants SANS déclencher le fatal. La fenêtre de commit ne s'accumule donc plus contre le
# seuil de 1000 ms ; l'émission reprend seule à la fin du commit. Le vrai détecteur de wedge (lien
# mort, ring XSK plein hors reset) reste INTACT — il ne fautait que sur le reset transitoire.
# Complémentaire de patch_rx_resetting_guard (segfault) et patch_tm_hierarchy (mur des 8).
#
# Idempotent + fail-fast : ancre introuvable (source MTL changée) ⇒ échec du build.
import sys

MARK = "/* bobi.studio: TX hang guard resetting */"

# ---------------------------------------------------------------- 1) vidéo : video_trs_burst_fail
FV = "lib/src/st2110/st_video_transmitter.c"
v = open(FV).read()

if MARK in v:
    print("patch TX hang resetting guard : déjà appliqué (%s)" % FV)
else:
    OLD_V = (
        "static uint16_t video_trs_burst_fail(struct mtl_main_impl* impl,\n"
        "                                     struct st_tx_video_session_impl* s,\n"
        "                                     enum mtl_session_port s_port, uint16_t nb_pkts) {\n"
        "  uint64_t cur_tsc = mt_get_tsc(impl);\n"
    )
    NEW_V = (
        "static uint16_t video_trs_burst_fail(struct mtl_main_impl* impl,\n"
        "                                     struct st_tx_video_session_impl* s,\n"
        "                                     enum mtl_session_port s_port, uint16_t nb_pkts) {\n"
        "  " + MARK + "\n"
        "  /* Port temporairement stoppé (dev_started=0) pendant rte_tm_hierarchy_commit — l'attache\n"
        "   * d'un shaper RL par une AUTRE session sender fige tout le port. mt_txq_burst renvoie 0\n"
        "   * (garde resetting) → on arrive ici, mais ce n'est PAS un hang de cette session : ne pas\n"
        "   * déclencher st20_tx_queue_fatal_error. On repousse le repère de succès pour que la\n"
        "   * fenêtre de commit ne s'accumule pas contre tx_hang_detect_time_thresh ; l'émission\n"
        "   * reprend seule à la fin du commit. Le vrai détecteur de wedge reste actif hors reset. */\n"
        "  if (rte_atomic32_read(&mt_if(impl, mt_port_logic2phy(s->port_maps, s_port))->resetting)) {\n"
        "    s->last_burst_succ_time_tsc[s_port] = mt_get_tsc(impl);\n"
        "    return nb_pkts;\n"
        "  }\n"
        "  uint64_t cur_tsc = mt_get_tsc(impl);\n"
    )
    if OLD_V not in v:
        print("ERREUR: ancre 'video_trs_burst_fail' introuvable dans %s (source MTL modifiée ?)" % FV,
              file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_V, NEW_V, 1)
    open(FV, "w").write(v)
    print("patch TX hang resetting guard : appliqué (%s)" % FV)

# ---------------------------------------------------------------- 2) audio : st_audio_trs_burst_fail
FA = "lib/src/st2110/st_audio_transmitter.c"
a = open(FA).read()

if MARK in a:
    print("patch TX hang resetting guard : déjà appliqué (%s)" % FA)
    sys.exit(0)

OLD_A = (
    "static uint16_t st_audio_trs_burst_fail(struct mtl_main_impl* impl,\n"
    "                                        struct st_tx_audio_sessions_mgr* mgr,\n"
    "                                        enum mtl_port port) {\n"
    "  uint64_t cur_tsc = mt_get_tsc(impl);\n"
)
NEW_A = (
    "static uint16_t st_audio_trs_burst_fail(struct mtl_main_impl* impl,\n"
    "                                        struct st_tx_audio_sessions_mgr* mgr,\n"
    "                                        enum mtl_port port) {\n"
    "  " + MARK + "  /* port déjà physique ici */\n"
    "  if (rte_atomic32_read(&mt_if(impl, port)->resetting)) {\n"
    "    mgr->last_burst_succ_time_tsc[port] = mt_get_tsc(impl);\n"
    "    return 1;\n"
    "  }\n"
    "  uint64_t cur_tsc = mt_get_tsc(impl);\n"
)
if OLD_A not in a:
    print("ERREUR: ancre 'st_audio_trs_burst_fail' introuvable dans %s (source MTL modifiée ?)" % FA,
          file=sys.stderr)
    sys.exit(1)
a = a.replace(OLD_A, NEW_A, 1)
open(FA, "w").write(a)
print("patch TX hang resetting guard : appliqué (%s)" % FA)

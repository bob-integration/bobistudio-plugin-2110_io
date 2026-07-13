#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# DÉCLENCHE la récupération native (purge rings + nouvelle queue + reset mempool + WAIT_FRAME)
# quand un commit RL TX a fait perdre les mbufs d'une session TX DÉJÀ VIVANTE.
#
# ── Cause racine (diagnostiquée, cf. mémoire mtl-tx-frozen-uint64-race + reliquat) ────────────
# Sur le PMD ice en pacing ratelimit (RL), créer une session TX déclenche dev_tx_queue_set_rl_rate
# → rte_tm_hierarchy_commit qui STOPPE/redémarre tout le port (cf. patch_rx_resetting_guard,
# patch_tx_hang_resetting_guard, patch_tm_hierarchy — même famille de commit). Une session TX déjà
# vivante peut y perdre DÉFINITIVEMENT les mbufs de son mempool hdr : rte_pktmbuf_alloc_bulk (vidéo,
# st_tx_video_session.c) / rte_pktmbuf_alloc (audio, st_tx_audio_session.c) échoue alors pour
# toujours (stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL = -207) → session morte EN SILENCE.
#
# Le hang detector natif (video_trs_burst_fail / st_audio_trs_burst_fail, cf.
# patch_tx_hang_resetting_guard) ne vit QUE dans le chemin burst (rte_eth_tx_burst) — or un builder
# qui échoue à l'ALLOC ne construit aucun paquet et n'atteint donc JAMAIS le burst : ce chemin ne
# tourne plus une fois la famine installée, le hang detector ne se déclenche donc jamais. La
# récupération elle-même (st20_tx_queue_fatal_error / st_audio_queue_fatal_error : purge rings +
# nouvelle queue + reset mempool + retour à ST21_TX_STAT_WAIT_FRAME) EXISTE et fonctionne déjà —
# il faut juste la DÉCLENCHER depuis le chemin BUILDER quand la famine persiste.
#
# ── Le patch : tracker de famine au point d'échec d'alloc, déclenché après 2 s ────────────────
# Un compteur bobi_alloc_fail_first_tsc (par session TX) est posé au 1er échec d'alloc consécutif
# et comparé à mt_get_tsc(impl) à chaque échec suivant :
#   - si le port est en commit transitoire (inf->resetting, même flag que patch_rx_resetting_guard/
#     patch_tx_hang_resetting_guard) → PAS de famine réelle, le compteur est simplement réarmé à 0
#     (le commit n'est pas encore fini, ne pas compter ce temps-là) ;
#   - sinon, au-delà de 2 s d'échecs consécutifs HORS reset → famine confirmée (mempool
#     réellement épuisé, pas juste un commit en cours) → déclenche la récupération native.
# Le compteur est remis à 0 dès qu'une alloc réussit (mempool redevenu sain) — vidéo : au succès du
# burst TX (video_trs_burst, st_video_transmitter.c, même point que last_burst_succ_time_tsc) ;
# audio : au succès de l'alloc elle-même dans le builder (st_tx_audio_session.c) — le hang detector
# audio natif est au niveau du MANAGER (mgr->last_burst_succ_time_tsc[port], PAS par session : cf.
# struct st_tx_audio_sessions_mgr), donc son point de succès de burst (st_audio_trs_burst,
# st_audio_transmitter.c) n'a PAS de pointeur de session — itérer mgr->sessions[] à CHAQUE paquet
# émis avec succès serait un coût chemin chaud inutile pour un cas rare. Resetter au succès de
# l'alloc locale est strictement équivalent (l'alloc est précisément ce qu'on surveille) et reste
# dans le fichier du builder, sans toucher st_audio_transmitter.c.
#
# Pas de compteur de tentatives : si la récupération réussit, le pool est plein et l'alloc repart
# (le compteur retombe à 0 au prochain succès) ; si elle échoue, libmtl marque déjà la session dead
# (unrecoverable, cf. st20_tx_queue_fatal_error / st_audio_queue_fatal_error existants).
#
# Idempotent + fail-fast : ancre introuvable (source MTL changée) ⇒ échec du build.
import sys

MARK = "/* bobi.studio: TX builder famine recovery */"
FAMINE_THRESH_NS = "2000000000ULL"  # 2 s. NS_PER_S n'est PAS visible depuis ces .c (grep vérifié
                                    # sur le SHA épinglé : seul mt_main.h l'UTILISE sans le définir
                                    # dans ce sous-arbre) → constante littérale, conservateur.

# ================================================================== 1) st_header.h : les 2 champs
FH = "lib/src/st2110/st_header.h"
h = open(FH).read()

if MARK in h:
    print("patch TX builder famine recovery : déjà appliqué (%s)" % FH)
else:
    # 1a) struct st_tx_video_session_impl : champ tracker vidéo (zmalloc-é au create → init 0)
    OLD_VH = (
        "  int idx; /* index for current tx_session */\n"
        "  uint64_t advice_sleep_us;\n"
        "  int recovery_idx;\n"
        "\n"
        "  struct st_tx_video_session_handle_impl* st20_handle;\n"
    )
    NEW_VH = (
        "  int idx; /* index for current tx_session */\n"
        "  uint64_t advice_sleep_us;\n"
        "  int recovery_idx;\n"
        "  " + MARK + "\n"
        "  /* horodatage (tsc) du 1er échec d'alloc consécutif dans le builder ; 0 = sain.\n"
        "   * Struct zmalloc-ée à la création de session → init garantie à 0. */\n"
        "  uint64_t bobi_alloc_fail_first_tsc;\n"
        "\n"
        "  struct st_tx_video_session_handle_impl* st20_handle;\n"
    )
    if OLD_VH not in h:
        print("ERREUR: ancre 'st_tx_video_session_impl (recovery_idx/st20_handle)' introuvable "
              "dans %s (source MTL modifiée ?)" % FH, file=sys.stderr)
        sys.exit(1)
    h = h.replace(OLD_VH, NEW_VH, 1)

    # 1b) struct st_tx_audio_session_impl : champ tracker audio (même garantie zmalloc)
    OLD_AH = (
        "  int recovery_idx;\n"
        "  bool active;\n"
        "  struct st_tx_audio_sessions_mgr* mgr;\n"
    )
    NEW_AH = (
        "  int recovery_idx;\n"
        "  bool active;\n"
        "  " + MARK + "\n"
        "  /* horodatage (tsc) du 1er échec d'alloc consécutif dans le builder ; 0 = sain. */\n"
        "  uint64_t bobi_alloc_fail_first_tsc;\n"
        "  struct st_tx_audio_sessions_mgr* mgr;\n"
    )
    if OLD_AH not in h:
        print("ERREUR: ancre 'st_tx_audio_session_impl (recovery_idx/active/mgr)' introuvable "
              "dans %s (source MTL modifiée ?)" % FH, file=sys.stderr)
        sys.exit(1)
    h = h.replace(OLD_AH, NEW_AH, 1)

    open(FH, "w").write(h)
    print("patch TX builder famine recovery : appliqué (%s)" % FH)

# ========================================================== 2) vidéo : builder + reset au succès
FV = "lib/src/st2110/st_tx_video_session.c"
v = open(FV).read()

if MARK in v:
    print("patch TX builder famine recovery : déjà appliqué (%s)" % FV)
else:
    # 2a) site d'échec d'alloc du builder whole-frame (tv_tasklet_frame, PAS le st22 ~l.2483 qui a
    # une indentation à 4 espaces — cet ancrage est à 2 espaces, donc distinct).
    OLD_VB = (
        "  ret = rte_pktmbuf_alloc_bulk(hdr_pool_p, pkts, bulk);\n"
        "  if (ret < 0) {\n"
        "    dbg(\"%s(%d), pkts alloc fail %d\\n\", __func__, idx, ret);\n"
        "    s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "    return MTL_TASKLET_ALL_DONE;\n"
        "  }\n"
    )
    NEW_VB = (
        "  ret = rte_pktmbuf_alloc_bulk(hdr_pool_p, pkts, bulk);\n"
        "  if (ret < 0) {\n"
        "    dbg(\"%s(%d), pkts alloc fail %d\\n\", __func__, idx, ret);\n"
        "    s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "    " + MARK + "\n"
        "    /* Mempool hdr épuisé pour de bon (perte au commit RL d'une AUTRE session, cf.\n"
        "     * patch_rx_resetting_guard) : le hang detector natif (video_trs_burst_fail) ne vit\n"
        "     * que dans le chemin burst, jamais atteint ici puisqu'on échoue avant de construire\n"
        "     * un paquet. On track nous-mêmes la durée de famine et on déclenche la récupération\n"
        "     * existante (st20_tx_queue_fatal_error) après un seuil, HORS fenêtre de commit. */\n"
        "    if (rte_atomic32_read(&mt_if(impl, mt_port_logic2phy(s->port_maps,\n"
        "                                                        MTL_SESSION_PORT_P))->resetting)) {\n"
        "      /* commit RL en cours ailleurs sur ce port : pas une vraie famine, ne pas compter */\n"
        "      s->bobi_alloc_fail_first_tsc = 0;\n"
        "    } else {\n"
        "      uint64_t bobi_now_tsc = mt_get_tsc(impl);\n"
        "      if (!s->bobi_alloc_fail_first_tsc) {\n"
        "        s->bobi_alloc_fail_first_tsc = bobi_now_tsc;\n"
        "      } else if ((bobi_now_tsc - s->bobi_alloc_fail_first_tsc) > " + FAMINE_THRESH_NS + ") {\n"
        "        err(\"%s(%d), bobi: builder famine (mempool exhausted) — triggering queue fatal \"\n"
        "            \"recovery\\n\", __func__, idx);\n"
        "        s->bobi_alloc_fail_first_tsc = 0;\n"
        "        for (int bobi_sp = 0; bobi_sp < s->ops.num_port; bobi_sp++) {\n"
        "          st20_tx_queue_fatal_error(impl, s, bobi_sp);\n"
        "        }\n"
        "      }\n"
        "    }\n"
        "    return MTL_TASKLET_ALL_DONE;\n"
        "  }\n"
    )
    if OLD_VB not in v:
        print("ERREUR: ancre 'tv_tasklet_frame pkts alloc fail' introuvable dans %s "
              "(source MTL modifiée ?)" % FV, file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_VB, NEW_VB, 1)
    open(FV, "w").write(v)
    print("patch TX builder famine recovery : appliqué (%s)" % FV)

# ------------------------------------------------------------------------------------------------
# 3) vidéo : reset du tracker au succès du burst (même point que last_burst_succ_time_tsc, dans
#    st_video_transmitter.c) — un burst qui réussit prouve que le mempool est de nouveau sain.
FT = "lib/src/st2110/st_video_transmitter.c"
t = open(FT).read()

if MARK in t:
    print("patch TX builder famine recovery : déjà appliqué (%s)" % FT)
else:
    OLD_VT = (
        "  s->last_burst_succ_time_tsc[s_port] = mt_get_tsc(impl);\n"
        "  return tx;\n"
        "}\n"
    )
    NEW_VT = (
        "  s->last_burst_succ_time_tsc[s_port] = mt_get_tsc(impl);\n"
        "  " + MARK + " s->bobi_alloc_fail_first_tsc = 0;\n"
        "  return tx;\n"
        "}\n"
    )
    if OLD_VT not in t:
        print("ERREUR: ancre 'video_trs_burst succès' introuvable dans %s "
              "(source MTL modifiée ?)" % FT, file=sys.stderr)
        sys.exit(1)
    t = t.replace(OLD_VT, NEW_VT, 1)
    open(FT, "w").write(t)
    print("patch TX builder famine recovery : appliqué (%s)" % FT)

# ================================================================== 4) audio : builder + reset
FA = "lib/src/st2110/st_tx_audio_session.c"
a = open(FA).read()

if MARK in a:
    print("patch TX builder famine recovery : déjà appliqué (%s)" % FA)
else:
    # 4a) 1er site d'échec d'alloc du builder audio whole-frame (tx_audio_session_tasklet_frame) —
    # les 3 autres sites -STI_FRAME_PKT_ALLOC_FAIL du fichier (rte_pktmbuf_alloc(chain_pool)/
    # (hdr_pool_r)/rte_pktmbuf_copy) partagent le même mempool logique et suivent immédiatement ;
    # couvrir le premier suffit à détecter la famine du pool hdr, sans dupliquer le patch 4×.
    OLD_AB = (
        "  pkt = rte_pktmbuf_alloc(hdr_pool_p);\n"
        "  if (!pkt) {\n"
        "    dbg(\"%s(%d), pkt alloc fail\\n\", __func__, idx);\n"
        "    s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "    return MTL_TASKLET_ALL_DONE;\n"
        "  }\n"
    )
    NEW_AB = (
        "  pkt = rte_pktmbuf_alloc(hdr_pool_p);\n"
        "  if (!pkt) {\n"
        "    dbg(\"%s(%d), pkt alloc fail\\n\", __func__, idx);\n"
        "    s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "    " + MARK + "\n"
        "    /* Même famine que côté vidéo (perte de mempool au commit RL d'une AUTRE session) :\n"
        "     * le hang detector natif audio (st_audio_trs_burst_fail) vit au niveau MANAGER\n"
        "     * (mgr->last_burst_succ_time_tsc[port]) et n'est jamais atteint tant que le builder\n"
        "     * échoue avant de construire un paquet. Récupération : st_audio_queue_fatal_error,\n"
        "     * qui opère déjà au niveau mgr/port (purge + nouvelle queue + reset mempool pour\n"
        "     * TOUTES les sessions du mgr sur ce port). */\n"
        "    if (rte_atomic32_read(&mt_if(impl, mt_port_logic2phy(s->port_maps,\n"
        "                                                        MTL_SESSION_PORT_P))->resetting)) {\n"
        "      s->bobi_alloc_fail_first_tsc = 0;\n"
        "    } else {\n"
        "      uint64_t bobi_now_tsc = mt_get_tsc(impl);\n"
        "      if (!s->bobi_alloc_fail_first_tsc) {\n"
        "        s->bobi_alloc_fail_first_tsc = bobi_now_tsc;\n"
        "      } else if ((bobi_now_tsc - s->bobi_alloc_fail_first_tsc) > " + FAMINE_THRESH_NS + ") {\n"
        "        err(\"%s(%d), bobi: builder famine (mempool exhausted) — triggering queue fatal \"\n"
        "            \"recovery\\n\", __func__, idx);\n"
        "        s->bobi_alloc_fail_first_tsc = 0;\n"
        "        for (int bobi_sp = 0; bobi_sp < s->ops.num_port; bobi_sp++) {\n"
        "          st_audio_queue_fatal_error(impl, s->mgr,\n"
        "                                     mt_port_logic2phy(s->port_maps, bobi_sp));\n"
        "        }\n"
        "      }\n"
        "    }\n"
        "    return MTL_TASKLET_ALL_DONE;\n"
        "  }\n"
    )
    if OLD_AB not in a:
        print("ERREUR: ancre 'tx_audio_session_tasklet_frame pkt alloc fail' introuvable dans %s "
              "(source MTL modifiée ?)" % FA, file=sys.stderr)
        sys.exit(1)
    a = a.replace(OLD_AB, NEW_AB, 1)

    # 4b) reset du tracker AU SUCCÈS DE L'ALLOC LOCALE (voir en-tête : le hang detector audio natif
    # est au niveau mgr/port, sans pointeur de session, donc son point de succès de burst
    # (st_audio_trs_burst, st_audio_transmitter.c) ne peut pas resetter un compteur PAR SESSION sans
    # itérer mgr->sessions[] à chaque paquet émis — coût chemin chaud pour un cas rare. Resetter dès
    # que l'alloc réussit est strictement équivalent : c'est exactement l'événement surveillé.
    OLD_AS = (
        "  pkt = rte_pktmbuf_alloc(hdr_pool_p);\n"
        "  if (!pkt) {\n"
    )
    NEW_AS = (
        "  pkt = rte_pktmbuf_alloc(hdr_pool_p);\n"
        "  " + MARK + " if (pkt) s->bobi_alloc_fail_first_tsc = 0;\n"
        "  if (!pkt) {\n"
    )
    if a.count(OLD_AS) != 1:
        print("ERREUR: ancre 'pkt = rte_pktmbuf_alloc(hdr_pool_p) (reset succès)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FA, a.count(OLD_AS)), file=sys.stderr)
        sys.exit(1)
    a = a.replace(OLD_AS, NEW_AS, 1)

    open(FA, "w").write(a)
    print("patch TX builder famine recovery : appliqué (%s)" % FA)

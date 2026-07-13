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
# vivante peut y perdre DÉFINITIVEMENT les mbufs de ses mempools (hdr, chain OU redondant) :
# rte_pktmbuf_alloc_bulk (vidéo, st_tx_video_session.c) / rte_pktmbuf_alloc (audio,
# st_tx_audio_session.c) échoue alors pour toujours (stat_build_ret_code =
# -STI_FRAME_PKT_ALLOC_FAIL = -207, ou les variantes _CHAIN_FAIL/_R_FAIL) → session morte EN
# SILENCE. Le hang detector natif (video_trs_burst_fail / st_audio_trs_burst_fail, cf.
# patch_tx_hang_resetting_guard) ne vit QUE dans le chemin burst (rte_eth_tx_burst) — or un builder
# qui échoue à l'ALLOC ne construit aucun paquet et n'atteint donc JAMAIS le burst : ce chemin ne
# tourne plus une fois la famine installée, le hang detector ne se déclenche donc jamais. La
# récupération elle-même (st20_tx_queue_fatal_error / st_audio_queue_fatal_error : purge rings +
# nouvelle queue + reset mempool + retour à ST21_TX_STAT_WAIT_FRAME) EXISTE et fonctionne déjà —
# il faut juste la DÉCLENCHER depuis le chemin BUILDER quand la famine persiste.
#
# ── Le patch (v2 = 0.44.1, retours du banc moteur 140 sur la v1 seuil 2 s) ────────────────────
# Un helper statique PAR FICHIER, bobi_famine_check(impl, s), appelé dans TOUTES les branches
# d'échec d'alloc du builder frame (vidéo st20 : hdr + chain + redondant ; audio : hdr + chain
# pkt_rtp + hdr_r + pktmbuf_copy) — la v1 ne couvrait que le pool hdr, laissant des sessions en
# build ret -207 permanent quand c'est le pool CHAIN (ou R) qui est vide. Logique du helper, sur
# le tracker par session bobi_alloc_fail_first_tsc (champ ajouté à st_header.h, zmalloc → init 0) :
#   - port en commit transitoire (inf->resetting, même flag que patch_rx_resetting_guard) →
#     tracker = 0 (le commit n'est pas fini, ne pas compter ce temps-là) ;
#   - sinon, tracker posé au 1er échec consécutif ; au-delà de 30 s d'échecs HORS reset (la v1 à
#     2 s déclenchait en pleine création séquentielle des sessions au cold-batch → cascade de
#     8-10 récupérations, chacune = nouvelle queue = commit TM = stop de port ice qui refait des
#     victimes, jusqu'à tuer le chemin RX PTP du daemon ; le cas cible est une session qui
#     resterait morte des HEURES, 30 s de latence de guérison est très acceptable) ;
#   - ET garde-fou GLOBAL anti-tempête : une statique par fichier (bobi_*_last_recovery_tsc) —
#     au plus UNE récupération par 10 s et par essence (vidéo/audio), posée AVANT d'appeler la
#     récupération. Sérialise les commits de récupération ; une famine bloquée par le garde-fou
#     GARDE son tracker et repart à la passe suivante. Races bénignes entre threads acceptées
#     (au pire 2 récupérations proches).
# Déclenchement : err() explicite + tracker = 0 + st20_tx_queue_fatal_error (vidéo, chaque port
# de la session) / st_audio_queue_fatal_error (audio, niveau MANAGER/port — purge + nouvelle
# queue + reset mempool de TOUTES les sessions du mgr sur ce port).
#
# Reset du tracker (retour à sain) :
#   - vidéo : au succès du burst TX (video_trs_burst, st_video_transmitter.c, même point que
#     last_burst_succ_time_tsc) — un burst qui part prouve que TOUS les pools ont fourni ;
#   - audio : à la FIN DE BUILD RÉUSSIE d'un paquet (st_tx_mbuf_set_idx(pkt, ...), première
#     instruction après les 4 branches d'alloc — atteinte SEULEMENT si toutes ont réussi).
#     La v1 resettait au succès de l'alloc hdr seule : si c'est le pool chain/R qui est vide,
#     hdr réussit à chaque passe et réarmait le tracker → jamais de déclenchement (le TROU de
#     couverture vu au banc). NB : le point `s->stat_build_ret_code = 0;`
#     (tx_audio_sessions_tasklet, ~l.1609) NE CONVIENT PAS : c'est le pré-reset INCONDITIONNEL
#     exécuté à chaque passe AVANT d'appeler le builder — y resetter le tracker le neutraliserait
#     totalement. Le point équivalent de fin de build réussie est st_tx_mbuf_set_idx (l.851,
#     unique dans le fichier).
#
# Pas de compteur de tentatives : si la récupération réussit, le pool est plein et l'alloc repart
# (le tracker retombe à 0 au prochain succès) ; si elle échoue, libmtl marque déjà la session dead
# (unrecoverable, cf. st20_tx_queue_fatal_error / st_audio_queue_fatal_error existants).
#
# Idempotent + fail-fast : ancre introuvable (source MTL changée) ⇒ échec du build.
import sys

MARK = "/* bobi.studio: TX builder famine recovery */"
FAMINE_THRESH_NS = "30000000000ULL"   # 30 s (v1 : 2 s → tempête au cold-batch, cf. en-tête)
RECOVERY_GUARD_NS = "10000000000ULL"  # garde-fou global : 1 récupération / 10 s / essence
# NS_PER_S n'est PAS visible depuis ces .c (grep vérifié sur le SHA épinglé : seul mt_main.h
# l'UTILISE sans le définir dans ce sous-arbre) → constantes littérales, conservateur.

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

# ============================================== 2) vidéo : helper + 3 branches d'échec (st20)
FV = "lib/src/st2110/st_tx_video_session.c"
v = open(FV).read()

if MARK in v:
    print("patch TX builder famine recovery : déjà appliqué (%s)" % FV)
else:
    # 2a) helper statique injecté juste avant tv_tasklet_frame (le builder frame st20).
    OLD_VF = (
        "static int tv_tasklet_frame(struct mtl_main_impl* impl,\n"
        "                            struct st_tx_video_session_impl* s) {\n"
    )
    NEW_VF = (
        MARK + "\n"
        "/* Garde-fou GLOBAL anti-tempête : au plus une récupération vidéo / 10 s. Une\n"
        " * récupération = une nouvelle queue = un commit TM = un stop du port ice qui peut\n"
        " * refaire des victimes ; des récupérations en cascade tuent le chemin RX PTP du\n"
        " * daemon (banc moteur 140). Races bénignes entre lcores acceptées (au pire 2\n"
        " * récupérations proches). */\n"
        "static uint64_t bobi_video_last_recovery_tsc;\n"
        "\n"
        "/* Famine d'alloc dans le builder (mempool hdr/chain/R épuisé par le commit RL d'une\n"
        " * AUTRE session) : le hang detector natif (video_trs_burst_fail) ne vit que dans le\n"
        " * chemin burst, jamais atteint quand on échoue AVANT de construire un paquet. On\n"
        " * track la durée de famine hors fenêtre resetting et on déclenche la récupération\n"
        " * existante (st20_tx_queue_fatal_error) après le seuil. */\n"
        "static inline void bobi_famine_check(struct mtl_main_impl* impl,\n"
        "                                     struct st_tx_video_session_impl* s) {\n"
        "  if (rte_atomic32_read(\n"
        "          &mt_if(impl, mt_port_logic2phy(s->port_maps, MTL_SESSION_PORT_P))->resetting)) {\n"
        "    /* commit RL en cours ailleurs sur ce port : pas une vraie famine, ne pas compter */\n"
        "    s->bobi_alloc_fail_first_tsc = 0;\n"
        "    return;\n"
        "  }\n"
        "  uint64_t bobi_now_tsc = mt_get_tsc(impl);\n"
        "  if (!s->bobi_alloc_fail_first_tsc) {\n"
        "    s->bobi_alloc_fail_first_tsc = bobi_now_tsc;\n"
        "  } else if ((bobi_now_tsc - s->bobi_alloc_fail_first_tsc) > " + FAMINE_THRESH_NS + ") {\n"
        "    if ((bobi_now_tsc - bobi_video_last_recovery_tsc) <= " + RECOVERY_GUARD_NS + ")\n"
        "      return; /* garde-fou global : tracker conservé, retentera à la passe suivante */\n"
        "    bobi_video_last_recovery_tsc = bobi_now_tsc;\n"
        "    err(\"%s(%d), bobi: builder famine (mempool exhausted) — triggering queue fatal \"\n"
        "        \"recovery\\n\", __func__, s->idx);\n"
        "    s->bobi_alloc_fail_first_tsc = 0;\n"
        "    for (int bobi_sp = 0; bobi_sp < s->ops.num_port; bobi_sp++) {\n"
        "      st20_tx_queue_fatal_error(impl, s, bobi_sp);\n"
        "    }\n"
        "  }\n"
        "}\n"
        "\n"
        "static int tv_tasklet_frame(struct mtl_main_impl* impl,\n"
        "                            struct st_tx_video_session_impl* s) {\n"
    )
    if OLD_VF not in v:
        print("ERREUR: ancre 'tv_tasklet_frame (déclaration)' introuvable dans %s "
              "(source MTL modifiée ?)" % FV, file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_VF, NEW_VF, 1)

    # 2b) branche hdr (STI_FRAME_PKT_ALLOC_FAIL) du chemin st20 — indentation 2 espaces, distincte
    # du bloc st22 homonyme (~l.2483, indentation 4 espaces).
    OLD_VB1 = (
        "  ret = rte_pktmbuf_alloc_bulk(hdr_pool_p, pkts, bulk);\n"
        "  if (ret < 0) {\n"
        "    dbg(\"%s(%d), pkts alloc fail %d\\n\", __func__, idx, ret);\n"
        "    s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "    return MTL_TASKLET_ALL_DONE;\n"
        "  }\n"
    )
    NEW_VB1 = (
        "  ret = rte_pktmbuf_alloc_bulk(hdr_pool_p, pkts, bulk);\n"
        "  if (ret < 0) {\n"
        "    dbg(\"%s(%d), pkts alloc fail %d\\n\", __func__, idx, ret);\n"
        "    s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "    bobi_famine_check(impl, s);\n"
        "    return MTL_TASKLET_ALL_DONE;\n"
        "  }\n"
    )
    # 2c) branche chain (STI_FRAME_PKT_ALLOC_CHAIN_FAIL) st20 — idem, indentation discriminante.
    OLD_VB2 = (
        "  if (!s->tx_no_chain) {\n"
        "    ret = rte_pktmbuf_alloc_bulk(chain_pool, pkts_chain, bulk);\n"
        "    if (ret < 0) {\n"
        "      dbg(\"%s(%d), pkts chain alloc fail %d\\n\", __func__, idx, ret);\n"
        "      rte_pktmbuf_free_bulk(pkts, bulk);\n"
        "      s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_CHAIN_FAIL;\n"
        "      return MTL_TASKLET_ALL_DONE;\n"
        "    }\n"
        "  }\n"
    )
    NEW_VB2 = (
        "  if (!s->tx_no_chain) {\n"
        "    ret = rte_pktmbuf_alloc_bulk(chain_pool, pkts_chain, bulk);\n"
        "    if (ret < 0) {\n"
        "      dbg(\"%s(%d), pkts chain alloc fail %d\\n\", __func__, idx, ret);\n"
        "      rte_pktmbuf_free_bulk(pkts, bulk);\n"
        "      s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_CHAIN_FAIL;\n"
        "      bobi_famine_check(impl, s);\n"
        "      return MTL_TASKLET_ALL_DONE;\n"
        "    }\n"
        "  }\n"
    )
    # 2d) branche redondante (STI_FRAME_PKT_ALLOC_R_FAIL) st20 — idem.
    OLD_VB3 = (
        "  if (send_r) {\n"
        "    ret = rte_pktmbuf_alloc_bulk(hdr_pool_r, pkts_r, bulk);\n"
        "    if (ret < 0) {\n"
        "      dbg(\"%s(%d), pkts_r alloc fail %d\\n\", __func__, idx, ret);\n"
        "      rte_pktmbuf_free_bulk(pkts, bulk);\n"
        "      if (!s->tx_no_chain) rte_pktmbuf_free_bulk(pkts_chain, bulk);\n"
        "      s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_R_FAIL;\n"
        "      return MTL_TASKLET_ALL_DONE;\n"
        "    }\n"
        "  }\n"
    )
    NEW_VB3 = (
        "  if (send_r) {\n"
        "    ret = rte_pktmbuf_alloc_bulk(hdr_pool_r, pkts_r, bulk);\n"
        "    if (ret < 0) {\n"
        "      dbg(\"%s(%d), pkts_r alloc fail %d\\n\", __func__, idx, ret);\n"
        "      rte_pktmbuf_free_bulk(pkts, bulk);\n"
        "      if (!s->tx_no_chain) rte_pktmbuf_free_bulk(pkts_chain, bulk);\n"
        "      s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_R_FAIL;\n"
        "      bobi_famine_check(impl, s);\n"
        "      return MTL_TASKLET_ALL_DONE;\n"
        "    }\n"
        "  }\n"
    )
    for name, old in (("tv_tasklet_frame hdr alloc fail", OLD_VB1),
                      ("tv_tasklet_frame chain alloc fail", OLD_VB2),
                      ("tv_tasklet_frame R alloc fail", OLD_VB3)):
        if v.count(old) != 1:
            print("ERREUR: ancre '%s' non-unique ou introuvable dans %s (comptée %d fois, "
                  "source MTL modifiée ?)" % (name, FV, v.count(old)), file=sys.stderr)
            sys.exit(1)
    v = v.replace(OLD_VB1, NEW_VB1, 1)
    v = v.replace(OLD_VB2, NEW_VB2, 1)
    v = v.replace(OLD_VB3, NEW_VB3, 1)
    open(FV, "w").write(v)
    print("patch TX builder famine recovery : appliqué (%s)" % FV)

# ------------------------------------------------------------------------------------------------
# 3) vidéo : reset du tracker au succès du burst (même point que last_burst_succ_time_tsc, dans
#    st_video_transmitter.c) — un burst qui réussit prouve que TOUS les pools ont fourni (le
#    paquet a été construit puis émis) : couvre hdr, chain ET R.
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

# ========================================= 4) audio : helper + 4 branches d'échec + reset build-ok
FA = "lib/src/st2110/st_tx_audio_session.c"
a = open(FA).read()

if MARK in a:
    print("patch TX builder famine recovery : déjà appliqué (%s)" % FA)
else:
    # 4a) helper statique injecté juste avant tx_audio_session_tasklet_frame (builder frame).
    OLD_AF = (
        "static int tx_audio_session_tasklet_frame(struct mtl_main_impl* impl,\n"
        "                                          struct st_tx_audio_session_impl* s) {\n"
    )
    NEW_AF = (
        MARK + "\n"
        "/* Garde-fou GLOBAL anti-tempête : au plus une récupération audio / 10 s (cf. le\n"
        " * jumeau vidéo dans st_tx_video_session.c — même cascade de commits au banc). */\n"
        "static uint64_t bobi_audio_last_recovery_tsc;\n"
        "\n"
        "/* Même famine que côté vidéo (mempool hdr/chain/R épuisé par le commit RL d'une AUTRE\n"
        " * session) : le hang detector natif audio (st_audio_trs_burst_fail) vit au niveau\n"
        " * MANAGER (mgr->last_burst_succ_time_tsc[port]) et n'est jamais atteint tant que le\n"
        " * builder échoue avant de construire un paquet. Récupération : st_audio_queue_fatal_error,\n"
        " * qui opère déjà au niveau mgr/port (purge + nouvelle queue + reset mempool pour TOUTES\n"
        " * les sessions du mgr sur ce port). */\n"
        "static inline void bobi_famine_check(struct mtl_main_impl* impl,\n"
        "                                     struct st_tx_audio_session_impl* s) {\n"
        "  if (rte_atomic32_read(\n"
        "          &mt_if(impl, mt_port_logic2phy(s->port_maps, MTL_SESSION_PORT_P))->resetting)) {\n"
        "    /* commit RL en cours ailleurs sur ce port : pas une vraie famine, ne pas compter */\n"
        "    s->bobi_alloc_fail_first_tsc = 0;\n"
        "    return;\n"
        "  }\n"
        "  uint64_t bobi_now_tsc = mt_get_tsc(impl);\n"
        "  if (!s->bobi_alloc_fail_first_tsc) {\n"
        "    s->bobi_alloc_fail_first_tsc = bobi_now_tsc;\n"
        "  } else if ((bobi_now_tsc - s->bobi_alloc_fail_first_tsc) > " + FAMINE_THRESH_NS + ") {\n"
        "    if ((bobi_now_tsc - bobi_audio_last_recovery_tsc) <= " + RECOVERY_GUARD_NS + ")\n"
        "      return; /* garde-fou global : tracker conservé, retentera à la passe suivante */\n"
        "    bobi_audio_last_recovery_tsc = bobi_now_tsc;\n"
        "    err(\"%s(%d), bobi: builder famine (mempool exhausted) — triggering queue fatal \"\n"
        "        \"recovery\\n\", __func__, s->idx);\n"
        "    s->bobi_alloc_fail_first_tsc = 0;\n"
        "    for (int bobi_sp = 0; bobi_sp < s->ops.num_port; bobi_sp++) {\n"
        "      st_audio_queue_fatal_error(impl, s->mgr,\n"
        "                                 mt_port_logic2phy(s->port_maps, bobi_sp));\n"
        "    }\n"
        "  }\n"
        "}\n"
        "\n"
        "static int tx_audio_session_tasklet_frame(struct mtl_main_impl* impl,\n"
        "                                          struct st_tx_audio_session_impl* s) {\n"
    )
    if OLD_AF not in a:
        print("ERREUR: ancre 'tx_audio_session_tasklet_frame (déclaration)' introuvable dans %s "
              "(source MTL modifiée ?)" % FA, file=sys.stderr)
        sys.exit(1)
    a = a.replace(OLD_AF, NEW_AF, 1)

    # 4b) branche 1 : hdr (dbg « pkt alloc fail » + STI_FRAME_PKT_ALLOC_FAIL — distincte du site
    # RTP ~l.989 (err + STI_RTP_PKT_ALLOC_FAIL) et du site ~l.2961 (goto out)).
    OLD_AB1 = (
        "  pkt = rte_pktmbuf_alloc(hdr_pool_p);\n"
        "  if (!pkt) {\n"
        "    dbg(\"%s(%d), pkt alloc fail\\n\", __func__, idx);\n"
        "    s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "    return MTL_TASKLET_ALL_DONE;\n"
        "  }\n"
    )
    NEW_AB1 = (
        "  pkt = rte_pktmbuf_alloc(hdr_pool_p);\n"
        "  if (!pkt) {\n"
        "    dbg(\"%s(%d), pkt alloc fail\\n\", __func__, idx);\n"
        "    s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "    bobi_famine_check(impl, s);\n"
        "    return MTL_TASKLET_ALL_DONE;\n"
        "  }\n"
    )
    # 4c) branche 2 : chain pkt_rtp.
    OLD_AB2 = (
        "    if (!pkt_rtp) {\n"
        "      err(\"%s(%d), pkt_rtp alloc fail\\n\", __func__, idx);\n"
        "      rte_pktmbuf_free(pkt);\n"
        "      s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "      return MTL_TASKLET_ALL_DONE;\n"
        "    }\n"
    )
    NEW_AB2 = (
        "    if (!pkt_rtp) {\n"
        "      err(\"%s(%d), pkt_rtp alloc fail\\n\", __func__, idx);\n"
        "      rte_pktmbuf_free(pkt);\n"
        "      s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "      bobi_famine_check(impl, s);\n"
        "      return MTL_TASKLET_ALL_DONE;\n"
        "    }\n"
    )
    # 4d) branche 3 : hdr_r (chemin chain, send_r).
    OLD_AB3 = (
        "      if (!pkt_r) {\n"
        "        err(\"%s(%d), rte_pktmbuf_alloc redundant fail\\n\", __func__, idx);\n"
        "        rte_pktmbuf_free(pkt);\n"
        "        rte_pktmbuf_free(pkt_rtp);\n"
        "        s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "        return MTL_TASKLET_ALL_DONE;\n"
        "      }\n"
    )
    NEW_AB3 = (
        "      if (!pkt_r) {\n"
        "        err(\"%s(%d), rte_pktmbuf_alloc redundant fail\\n\", __func__, idx);\n"
        "        rte_pktmbuf_free(pkt);\n"
        "        rte_pktmbuf_free(pkt_rtp);\n"
        "        s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "        bobi_famine_check(impl, s);\n"
        "        return MTL_TASKLET_ALL_DONE;\n"
        "      }\n"
    )
    # 4e) branche 4 : pktmbuf_copy redondant (chemin no_chain, send_r).
    OLD_AB4 = (
        "      if (!pkt_r) {\n"
        "        err(\"%s(%d), rte_pktmbuf_copy redundant fail\\n\", __func__, idx);\n"
        "        rte_pktmbuf_free(pkt);\n"
        "        s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "        return MTL_TASKLET_ALL_DONE;\n"
        "      }\n"
    )
    NEW_AB4 = (
        "      if (!pkt_r) {\n"
        "        err(\"%s(%d), rte_pktmbuf_copy redundant fail\\n\", __func__, idx);\n"
        "        rte_pktmbuf_free(pkt);\n"
        "        s->stat_build_ret_code = -STI_FRAME_PKT_ALLOC_FAIL;\n"
        "        bobi_famine_check(impl, s);\n"
        "        return MTL_TASKLET_ALL_DONE;\n"
        "      }\n"
    )
    for name, old in (("audio hdr alloc fail", OLD_AB1),
                      ("audio pkt_rtp alloc fail", OLD_AB2),
                      ("audio hdr_r alloc fail", OLD_AB3),
                      ("audio pktmbuf_copy fail", OLD_AB4)):
        if a.count(old) != 1:
            print("ERREUR: ancre '%s' non-unique ou introuvable dans %s (comptée %d fois, "
                  "source MTL modifiée ?)" % (name, FA, a.count(old)), file=sys.stderr)
            sys.exit(1)
    a = a.replace(OLD_AB1, NEW_AB1, 1)
    a = a.replace(OLD_AB2, NEW_AB2, 1)
    a = a.replace(OLD_AB3, NEW_AB3, 1)
    a = a.replace(OLD_AB4, NEW_AB4, 1)

    # 4f) reset du tracker à la FIN DE BUILD RÉUSSIE : st_tx_mbuf_set_idx(pkt, ...) est la 1ʳᵉ
    # instruction après les 4 branches d'alloc, atteinte SEULEMENT si toutes ont réussi (unique
    # dans le fichier). Voir en-tête pour le rejet de `s->stat_build_ret_code = 0;` (~l.1609,
    # pré-reset inconditionnel de tx_audio_sessions_tasklet, exécuté AVANT le builder).
    OLD_AS = (
        "  st_tx_mbuf_set_idx(pkt, s->st30_pkt_idx);\n"
        "  st_tx_mbuf_set_tsc(pkt, pacing->tsc_time_cursor);\n"
    )
    NEW_AS = (
        "  " + MARK + " s->bobi_alloc_fail_first_tsc = 0; /* build complet = tous pools sains */\n"
        "  st_tx_mbuf_set_idx(pkt, s->st30_pkt_idx);\n"
        "  st_tx_mbuf_set_tsc(pkt, pacing->tsc_time_cursor);\n"
    )
    if a.count(OLD_AS) != 1:
        print("ERREUR: ancre 'st_tx_mbuf_set_idx (reset build-ok)' non-unique ou introuvable "
              "dans %s (comptée %d fois, source MTL modifiée ?)" % (FA, a.count(OLD_AS)),
              file=sys.stderr)
        sys.exit(1)
    a = a.replace(OLD_AS, NEW_AS, 1)

    open(FA, "w").write(a)
    print("patch TX builder famine recovery : appliqué (%s)" % FA)

#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
# ⚠ DOIT s'appliquer APRÈS patch_tx_builder_famine_recovery.py (les ancres de st_header.h sont
#   son état de SORTIE — fail-fast sinon).
#
# ═══ LE MODE DE MORT TRAITÉ ICI : `build ret -203` PERSISTANT ═══════════════════════════════════
# DISTINCT de la famine `-207` (STI_FRAME_PKT_ALLOC_FAIL, mempool vidé) que traite le filet
# étage 0 (patch_tx_builder_famine_recovery). -203 = STI_FRAME_APP_GET_FRAME_BUSY
# (lib/src/st2110/st_err.h:35) : ce n'est PAS l'alloc de mbufs qui échoue, c'est
# `ops->get_next_frame()` — le FEEDER APPLICATIF — qui ne rend plus aucune trame
# (st_tx_video_session.c:1876). Le filet famine ne le voit pas : ses points d'appel sont les
# branches d'échec d'ALLOC, jamais atteintes ici (on échoue AVANT, en amont du builder).
#
# ── Constaté au banc (moteur 140, nœud 30, 2× : slots 0 et 2 ; log du 2026-07-14 17:10) ────────
#   17:10:02  bobi_famine_check(0) … triggering queue fatal recovery      (voisines, -207)
#   17:10:12  TX_VIDEO_SESSION(9,0:bobi_mtl_vtx_sl): build ret -203       ← la victime
#   17:10:22  … -203 … 17:10:32 … -203 … (indéfiniment, 0 fps, source fraîche 3,3 ms)
# La victime n'a JAMAIS de ligne de récupération : elle ne fait pas de famine mempool, donc le
# filet 0.48.0 ne s'accroche nulle part. Elle est morte DÉFINITIVEMENT, en antenne.
#
# ── LE MÉCANISME, PROUVÉ (fichier:ligne, SHA épinglé 32b1b4e9) ─────────────────────────────────
# 1. Une trame vidéo TX n'est RENDUE à l'application que par le refcount des mbufs :
#    tv_tasklet_frame incrémente `frame->refcnt` (st_tx_video_session.c:1906) et chaque mbuf de
#    charge utile prend une ref extbuf sur `frame->sh_info` (:1255). Quand la DERNIÈRE ref tombe
#    (mbuf libéré après émission), DPDK appelle `tv_frame_free_cb` (:114) → `tv_notify_frame_done`
#    (:91) → `ops.notify_frame_done` = le callback de l'app. **C'est le SEUL chemin de retour.**
# 2. Le commit TM (`rte_tm_hierarchy_commit` d'une AUTRE session) stoppe le PORT ENTIER ;
#    `ice_reset_tx_queue` memset le sw_ring SANS free → les mbufs déjà postés dans les
#    descripteurs TX sont PERDUS sans être libérés. C'est la MÊME fuite qui vide les mempools
#    (-207, cf. TX_LAYOUTS.md « cause première de la vidange » : pools d'origine alloués et vides).
# 3. Conséquence côté trames : la ref extbuf de ces mbufs perdus n'est JAMAIS rendue ⇒
#    `tv_frame_free_cb` n'est JAMAIS appelé ⇒ `frame->refcnt` reste à 1 **POUR TOUJOURS** ⇒ le
#    framebuffer n'est JAMAIS rendu à l'app. L'app (mtl_rx.c) n'a plus de slot libre à remplir,
#    donc plus rien à donner ⇒ `get_next_frame` rend -EBUSY à chaque passe ⇒ **-203 permanent**.
#    Le filet famine ne peut RIEN : cette session n'échoue jamais à allouer.
# 4. Pourquoi le nettoyage de trames EXISTANT ne sauve pas : libmtl SAIT rendre les trames
#    piégées — `st20_tx_queue_fatal_error` contient exactement ce bloc (:4095-4107, « stop frame
#    %u » : tv_notify_frame_done + dec refcnt + rte_mbuf_ext_refcnt_set(sh_info, 0)). Mais il
#    n'est atteignable QUE par une récupération de QUEUE, elle-même déclenchée par une famine
#    d'ALLOC (-207) ou par le hang detector de burst. Une session dont le mempool tient encore
#    (assez de mbufs libres) mais dont les TRAMES sont piégées ne déclenche NI l'un NI l'autre :
#    **le seul chemin de guérison est inaccessible depuis le seul symptôme qu'elle produit.**
#    ⇒ ce n'est PAS un flag jamais reclearé (famille `resetting`) : c'est un **compteur de
#    références jamais rendu**, et un chemin de guérison sans déclencheur.
#
# ── LE FIX : un rappel des trames orphelines, SANS toucher à la queue ──────────────────────────
# Sur -203 CONTINU pendant BUSY_THRESH_NS (2 s) hors fenêtre `inf->resetting`, on scanne les
# trames de la session :
#   - au moins une a `refcnt != 0` ⇒ elle est piégée (à 50 fps, une trame en vol > 2 s est une
#     impossibilité physique ; l'app, elle, ne rend rien) → on la RÉCUPÈRE exactement comme le
#     fait st20_tx_queue_fatal_error : `tv_notify_frame_done` + `rte_atomic32_dec(refcnt)` +
#     `rte_mbuf_ext_refcnt_set(&sh_info, 0)`. L'app retrouve son slot, le feeder repart.
#   - AUCUNE trame en vol ⇒ ce n'est PAS un piégeage, c'est une famine APPLICATIVE légitime
#     (slot silencieux, pas de source câblée) → **on ne fait RIEN** (et on le DIT une fois, cf.
#     « pas de repli silencieux » : un slot muet volontaire ne doit pas devenir un log en boucle).
# Coût : ZÉRO commit TM, zéro nouvelle queue, zéro mempool. C'est le point clé — la récupération
# de queue (étage 0) est une opération PERTURBATRICE (elle re-stoppe le port et refait des
# victimes) ; ici on ne touche QUE la comptabilité de trames de la session concernée.
#
# ⚠ CONTEXTE D'EXÉCUTION (le piège n°1 de ce chantier — un appel bloquant depuis une tasklet a
# déjà figé un lcore 10 h, cf. l'en-tête de patch_tx_builder_famine_recovery) : ce helper est
# appelé depuis `tv_tasklet_frame`, sous le spinlock de session pris par `tvs_tasklet_handler`
# (tx_video_session_try_get). Il n'appelle RIEN de bloquant : pas de spinlock, pas de mutex de
# session, pas de queue, pas de mempool. `tv_notify_frame_done` est exactement ce que le chemin
# NORMAL (tv_frame_free_cb, appelé depuis ce même lcore à chaque trame émise) exécute déjà — le
# contexte est identique, à l'octet près. Aucune nouvelle classe de verrou n'est introduite.
#
# ── Ce que ce patch NE couvre PAS (assumé, dit explicitement) ──────────────────────────────────
#   - l'AUDIO (st30) : `st_tx_audio_session.c:745` produit le même -203, et la même fuite de
#     mbufs peut y piéger des trames. Non traité ici : côté audio le builder tourne sous
#     `mgr->mutex[sidx]` et le callback applicatif y serait appelé sous spinlock — il faut le
#     même différé « poser une demande / consommer en fin de tasklet » que la v4 du filet famine.
#     À faire si le banc montre un st30 muet après commit. **Non observé à ce jour.**
#   - la FUITE elle-même (mbufs perdus au stop de port). Ce patch la RATTRAPE, il ne la supprime
#     pas. Seul l'étage 4 (matrice de queues pré-shapées, plus aucun commit) la supprimerait.
#
# Idempotent + fail-fast : ancre introuvable ⇒ échec du build.
import sys

MARK = "/* bobi.studio: TX inflight frame reclaim */"
BUSY_THRESH_NS = "2000000000ULL"   # 2 s de -203 continu hors resetting (cf. en-tête)

# ============================================================ 1) st_header.h : les 2 champs
FH = "lib/src/st2110/st_header.h"
h = open(FH).read()

if MARK in h:
    print("patch TX inflight frame reclaim : déjà appliqué (%s)" % FH)
else:
    # Ancre = l'état de SORTIE de patch_tx_builder_famine_recovery (champ bobi_alloc_fail_first_tsc
    # inséré avant st20_handle). Garantit l'ordre d'application des deux patchs.
    OLD_VH = (
        "  uint64_t bobi_alloc_fail_first_tsc;\n"
        "\n"
        "  struct st_tx_video_session_handle_impl* st20_handle;\n"
    )
    NEW_VH = (
        "  uint64_t bobi_alloc_fail_first_tsc;\n"
        "  " + MARK + "\n"
        "  /* horodatage (tsc) du 1er -203 (get_next_frame busy) consécutif ; 0 = sain.\n"
        "   * Struct zmalloc-ée à la création de session → init garantie à 0. */\n"
        "  uint64_t bobi_get_frame_busy_first_tsc;\n"
        "  /* 1 = verdict « famine applicative, aucune trame piégée » déjà loggé pour cet\n"
        "   * épisode (anti-spam : un slot silencieux est un état NORMAL et permanent). */\n"
        "  uint8_t bobi_get_frame_busy_diag;\n"
        "\n"
        "  struct st_tx_video_session_handle_impl* st20_handle;\n"
    )
    if h.count(OLD_VH) != 1:
        print("ERREUR: ancre 'st_tx_video_session_impl (bobi_alloc_fail_first_tsc/st20_handle)' "
              "non-unique ou introuvable dans %s (comptée %d fois — patch_tx_builder_famine_"
              "recovery.py doit être appliqué AVANT celui-ci)" % (FH, h.count(OLD_VH)),
              file=sys.stderr)
        sys.exit(1)
    h = h.replace(OLD_VH, NEW_VH, 1)
    open(FH, "w").write(h)
    print("patch TX inflight frame reclaim : appliqué (%s)" % FH)

# ================================ 2) st_tx_video_session.c : helper + détection + reset du tracker
FV = "lib/src/st2110/st_tx_video_session.c"
v = open(FV).read()

if MARK in v:
    print("patch TX inflight frame reclaim : déjà appliqué (%s)" % FV)
else:
    # 2a) le helper, injecté juste avant tv_tasklet_frame (déclaration inchangée par le patch
    # famine, qui insère ses propres helpers AVANT elle). tv_notify_frame_done est défini plus
    # haut dans le fichier (l.91) → visible.
    OLD_VF = (
        "static int tv_tasklet_frame(struct mtl_main_impl* impl,\n"
        "                            struct st_tx_video_session_impl* s) {\n"
    )
    NEW_VF = (
        MARK + "\n"
        "/* Rappel des trames PIÉGÉES « inflight » — mode de mort DISTINCT de la famine mempool.\n"
        " *\n"
        " * -203 (STI_FRAME_APP_GET_FRAME_BUSY) = get_next_frame ne rend plus rien. Deux causes,\n"
        " * qu'on DISCRIMINE par le refcnt des trames :\n"
        " *   (a) refcnt != 0 sur au moins une trame ⇒ ses mbufs de charge utile ont été perdus\n"
        " *       sans free au stop de port du commit TM (ice_reset_tx_queue memset le sw_ring) :\n"
        " *       la ref extbuf sur sh_info n'est jamais rendue ⇒ tv_frame_free_cb n'est JAMAIS\n"
        " *       appelé ⇒ la trame n'est JAMAIS rendue à l'app ⇒ l'app n'a plus de slot libre et\n"
        " *       ne peut plus rien fournir ⇒ -203 PERMANENT (session morte en silence, en\n"
        " *       antenne). À 50 fps, une trame en vol depuis > 2 s est une impossibilité.\n"
        " *   (b) aucune trame en vol ⇒ famine APPLICATIVE légitime (slot silencieux, pas de\n"
        " *       source câblée) : rien à réparer, on n'y touche pas.\n"
        " *\n"
        " * Cas (a) : on rend les trames exactement comme le fait déjà st20_tx_queue_fatal_error\n"
        " * (« stop frame »), mais SANS récupération de queue — donc ZÉRO commit TM, zéro stop de\n"
        " * port, zéro nouvelle victime. Le chemin de guérison existait, il n'avait simplement\n"
        " * aucun déclencheur atteignable depuis ce symptôme (le filet famine ne s'accroche qu'aux\n"
        " * branches d'échec d'ALLOC, jamais parcourues ici).\n"
        " *\n"
        " * ⚠ Appelé sous le spinlock de session de tvs_tasklet_handler : RIEN de bloquant ici.\n"
        " * tv_notify_frame_done est exactement ce que le chemin normal (tv_frame_free_cb) exécute\n"
        " * depuis ce même lcore à chaque trame émise — contexte identique. */\n"
        "static inline void bobi_get_frame_busy_check(struct mtl_main_impl* impl,\n"
        "                                             struct st_tx_video_session_impl* s) {\n"
        "  if (!st20_is_frame_type(s->ops.type)) return; /* RTP-level : pas de trames */\n"
        "  if (rte_atomic32_read(\n"
        "          &mt_if(impl, mt_port_logic2phy(s->port_maps, MTL_SESSION_PORT_P))->resetting)) {\n"
        "    /* commit RL en cours sur ce port : l'app peut légitimement patiner, ne pas compter */\n"
        "    s->bobi_get_frame_busy_first_tsc = 0;\n"
        "    return;\n"
        "  }\n"
        "  uint64_t bobi_now_tsc = mt_get_tsc(impl);\n"
        "  if (!s->bobi_get_frame_busy_first_tsc) {\n"
        "    s->bobi_get_frame_busy_first_tsc = bobi_now_tsc;\n"
        "    return;\n"
        "  }\n"
        "  if ((bobi_now_tsc - s->bobi_get_frame_busy_first_tsc) <= " + BUSY_THRESH_NS + ") return;\n"
        "  /* ré-arme AVANT d'agir : le scan ci-dessous ne doit tourner qu'une fois par fenêtre\n"
        "   * (ce helper est appelé à CHAQUE passe de tasklet, chemin très chaud). */\n"
        "  s->bobi_get_frame_busy_first_tsc = bobi_now_tsc;\n"
        "\n"
        "  int bobi_orphans = 0;\n"
        "  for (int bobi_i = 0; bobi_i < s->st20_frames_cnt; bobi_i++) {\n"
        "    struct st_frame_trans* bobi_f = &s->st20_frames[bobi_i];\n"
        "    if (!rte_atomic32_read(&bobi_f->refcnt)) continue;\n"
        "    err(\"%s(%d), bobi: inflight frame %d trapped > 2s while get_frame busy — mbufs lost \"\n"
        "        \"at port stop (TM commit); reclaiming it (no queue recovery)\\n\",\n"
        "        __func__, s->idx, bobi_i);\n"
        "    tv_notify_frame_done(s, bobi_i);\n"
        "    rte_atomic32_dec(&bobi_f->refcnt);\n"
        "    rte_mbuf_ext_refcnt_set(&bobi_f->sh_info, 0);\n"
        "    s->port_user_stats.common.stat_recoverable_error++;\n"
        "    bobi_orphans++;\n"
        "  }\n"
        "  if (bobi_orphans) {\n"
        "    s->st20_pkt_idx = 0;\n"
        "    s->st20_frame_stat = ST21_TX_STAT_WAIT_FRAME;\n"
        "    s->bobi_get_frame_busy_diag = 0;\n"
        "    err(\"%s(%d), bobi: %d inflight frame(s) reclaimed, tx should resume\\n\", __func__,\n"
        "        s->idx, bobi_orphans);\n"
        "  } else if (!s->bobi_get_frame_busy_diag) {\n"
        "    /* Pas de repli SILENCIEUX : on dit ce qu'on constate et pourquoi on n'agit pas.\n"
        "     * Une fois par épisode (réarmé au 1er get_next_frame réussi) — un slot volontairement\n"
        "     * silencieux est un état normal et PERMANENT, il ne doit pas spammer le log. */\n"
        "    s->bobi_get_frame_busy_diag = 1;\n"
        "    info(\"%s(%d), bobi: get_frame busy > 2s but NO frame in flight — app starvation \"\n"
        "         \"(silent slot / no source), nothing to reclaim\\n\", __func__, s->idx);\n"
        "  }\n"
        "}\n"
        "\n"
        "static int tv_tasklet_frame(struct mtl_main_impl* impl,\n"
        "                            struct st_tx_video_session_impl* s) {\n"
    )
    if v.count(OLD_VF) != 1:
        print("ERREUR: ancre 'tv_tasklet_frame (déclaration)' non-unique ou introuvable dans %s "
              "(comptée %d fois, source MTL modifiée ?)" % (FV, v.count(OLD_VF)), file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_VF, NEW_VF, 1)

    # 2b) le point de DÉTECTION : la branche -203 de tv_tasklet_frame (st20 ; le jumeau st22 a sa
    # propre branche, non couverte — st22 n'est pas utilisé par le moteur).
    OLD_VB = (
        "      if (ret < 0) { /* no frame ready from app */\n"
        "        if (s->stat_user_busy_first) {\n"
        "          s->port_user_stats.stat_user_busy++;\n"
        "          s->stat_user_busy_first = false;\n"
        "          dbg(\"%s(%d), get_next_frame fail %d\\n\", __func__, idx, ret);\n"
        "        }\n"
        "        s->stat_build_ret_code = -STI_FRAME_APP_GET_FRAME_BUSY;\n"
        "        return MTL_TASKLET_ALL_DONE;\n"
        "      }\n"
    )
    NEW_VB = (
        "      if (ret < 0) { /* no frame ready from app */\n"
        "        if (s->stat_user_busy_first) {\n"
        "          s->port_user_stats.stat_user_busy++;\n"
        "          s->stat_user_busy_first = false;\n"
        "          dbg(\"%s(%d), get_next_frame fail %d\\n\", __func__, idx, ret);\n"
        "        }\n"
        "        s->stat_build_ret_code = -STI_FRAME_APP_GET_FRAME_BUSY;\n"
        "        " + MARK + "\n"
        "        bobi_get_frame_busy_check(impl, s);\n"
        "        return MTL_TASKLET_ALL_DONE;\n"
        "      }\n"
    )
    if v.count(OLD_VB) != 1:
        print("ERREUR: ancre 'tv_tasklet_frame (branche -203 get_next_frame)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FV, v.count(OLD_VB)), file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_VB, NEW_VB, 1)

    # 2c) reset du tracker : au 1er get_next_frame RÉUSSI (l'app rend de nouveau des trames).
    OLD_VS = (
        "      s->stat_user_busy_first = true;\n"
        "      /* all check fine */\n"
        "      rte_atomic32_inc(&frame->refcnt);\n"
    )
    NEW_VS = (
        "      s->stat_user_busy_first = true;\n"
        "      " + MARK + "\n"
        "      s->bobi_get_frame_busy_first_tsc = 0; /* l'app rend des trames : sain */\n"
        "      s->bobi_get_frame_busy_diag = 0;\n"
        "      /* all check fine */\n"
        "      rte_atomic32_inc(&frame->refcnt);\n"
    )
    if v.count(OLD_VS) != 1:
        print("ERREUR: ancre 'tv_tasklet_frame (get_next_frame succès)' non-unique ou introuvable "
              "dans %s (comptée %d fois, source MTL modifiée ?)" % (FV, v.count(OLD_VS)),
              file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_VS, NEW_VS, 1)

    open(FV, "w").write(v)
    print("patch TX inflight frame reclaim : appliqué (%s)" % FV)

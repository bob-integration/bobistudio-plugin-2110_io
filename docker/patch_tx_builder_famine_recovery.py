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
# ── v4 = 0.48.0 : ★ AUTO-DEADLOCK DU LCORE — la VRAIE raison pour laquelle le filet ne tient pas
#    (mesuré au banc 2026-07-14, moteur 140, gdb + télémétrie DPDK — les v1/v2/v3 se trompaient) ─
# Constat brut sur le moteur 140 (image 0.47.0, 10 h de vie) : slots TX [0,0,0,0,50,50], 420 ×
# `build ret -207` / 10 min, UNE SEULE ligne `bobi: builder famine` dans TOUT le log (à T+90 s),
# ZÉRO ligne `zombie` (le filet v3 n'a jamais tourné une seule fois), et TOUS les mempools encore
# nommés `..._HDR_0_..` (recovery_idx = 0 ⇒ AUCUNE session n'a jamais été re-mempool-isée).
#
# Backtrace gdb du lcore fautif (thread `mtl_sch_0`, état R, 100 % CPU, figé depuis 10 h) :
#     #0  st_audio_queue_fatal_error
#     #1  tx_audio_session_tasklet_frame   ← bobi_famine_check, inliné
#     #2  tx_audio_sessions_tasklet
#     #3  sch_tasklet_func
# Télémétrie DPDK au même instant : TV_M0S1P0_HDR_0_33 → size 2047, common_pool_count = 1.
#
# Cause EXACTE, une ligne de code : `tx_audio_sessions_tasklet` prend la session courante avec
# `tx_audio_session_try_get(mgr, sidx)` = `rte_spinlock_trylock(&mgr->mutex[sidx])` et GARDE ce
# spinlock pendant TOUT l'appel au builder (relâché seulement au `exit: tx_audio_session_put`).
# Or `st_audio_queue_fatal_error` (st_tx_audio_session.c ~l.2703) reboucle sur TOUTES les sessions
# du mgr avec `tx_audio_session_get(mgr, sidx)` = `rte_spinlock_lock(&mgr->mutex[sidx])` — la
# variante BLOQUANTE. `rte_spinlock_t` n'est PAS récursive : dès que la boucle atteint la session
# que le tasklet appelant tient déjà, le lcore SPINNE SUR SON PROPRE VERROU, POUR TOUJOURS.
# ⇒ appeler st_audio_queue_fatal_error DEPUIS le builder audio (ce que faisaient les v1/v2/v3) est
#   un AUTO-DEADLOCK garanti dès la 1ʳᵉ famine audio.
# Le call site NATIF de libmtl (st_audio_transmitter.c:58, hang detector burst) est un tasklet
# DIFFÉRENT qui ne tient aucun mutex de session → upstream est sain ; c'est bien NOTRE point
# d'appel qui est illégal. Le jumeau VIDÉO est sain lui aussi : st20_tx_queue_fatal_error ne prend
# AUCUN mutex de session (il ne touche que `s`) → appelable depuis le builder vidéo.
#
# Tout le tableau clinique en découle, sans rien d'autre :
#   - le lcore sch_0 est mort ⇒ les 4 sessions vidéo + les sessions audio de mgr 0 (mêmes sch) ne
#     sont plus jamais buildées ⇒ fps 0 ; `stat_build_ret_code` reste FIGÉ à sa dernière valeur
#     (-207) et le thread de stats la réimprime toutes les 10 s ⇒ les « 420 -207 / 10 min » ne sont
#     PAS 420 famines, c'est UNE famine figée réimprimée en boucle ;
#   - bobi_famine_check n'est plus jamais atteint ⇒ une seule ligne `builder famine` à vie ;
#   - bobi_zombie_retry (v3) vit DANS la boucle manager du même lcore mort ⇒ ne tourne jamais ⇒
#     0 ligne `zombie`. Le filet v3 n'était pas « trop conservateur », il était INATTEIGNABLE ;
#   - le deadlock survient AVANT la boucle de re-mempool ⇒ recovery_idx reste 0 partout ;
#   - les sessions vidéo des sch 1 et 2 (lcores vivants) tournent à 50 fps sans incident.
# ⇒ Les « 3 tentatives puis plus jamais » de la v3 étaient une lecture erronée : il n'y a jamais
#   eu 3 tentatives, il y a eu UN appel qui n'est jamais revenu.
#
# ── Le fix v4 : RECOVERY AUDIO DIFFÉRÉE, HORS SPINLOCK ────────────────────────────────────────
# On ne peut PAS appeler st_audio_queue_fatal_error sous le mutex de session. On le fait donc
# depuis le seul endroit du même lcore où AUCUN mutex de session n'est tenu : la FIN de
# `tx_audio_sessions_tasklet`, après que la boucle a fait tous ses `tx_audio_session_put`.
#   - `bobi_famine_check` (audio) ne fait plus que POSER une demande : un drapeau par port sur le
#     mgr (`bobi_famine_pending[MTL_PORT_MAX]`, ajouté à struct st_tx_audio_sessions_mgr) ;
#   - `bobi_zombie_retry` (audio) fait pareil (une session !active pose la demande, rien de plus) ;
#   - un CONSOMMATEUR en fin de tasklet applique le garde-fou global (1 récupération / 10 s,
#     `bobi_audio_last_recovery_tsc`, inchangé) puis appelle st_audio_queue_fatal_error. Si le
#     garde-fou bloque, la demande est CONSERVÉE et rejouée au tick suivant → retry indéfini, sans
#     compteur max : une session morte pour toujours est bien pire qu'une récupération qui retente.
# Poseur et consommateur tournent sur le MÊME lcore (le tasklet du mgr) → pas de course, un simple
# uint8_t suffit (pas d'atomique).
# La protection anti-tempête est intégralement conservée là où elle sert (fenêtre de boot) : le
# débit de commits TM reste borné à 1/10 s par essence. Seul change le fait que la file de demandes
# ne se vide jamais tant qu'une session est en famine.
#
# Le chemin VIDÉO est laissé TEL QUEL (v3) : pas de mutex de session pris par
# st20_tx_queue_fatal_error ⇒ appel direct depuis le builder légal, et bobi_zombie_retry vidéo
# est légal pour la même raison.
#
# Le fix (bobi_zombie_retry, v3) : `s->active=false` reste nécessaire pour la sûreté (ne pas
# tenter de tx sur une queue NULL) mais n'est plus un aller simple. Chaque boucle manager
# (tvs_tasklet_handler / tx_audio_sessions_tasklet), juste au point du `goto exit` existant,
# appelle bobi_zombie_retry sur toute session `!active` : elle retente st20_tx_queue_fatal_error /
# st_audio_queue_fatal_error, throttlée par le MÊME garde-fou global 10 s déjà en place
# (bobi_*_last_recovery_tsc) — pas un nouveau timer séparé. Raisonnement : une résurrection =
# une nouvelle queue = un commit TM, donc EXACTEMENT le même risque de tempête qu'une récupération
# initiale ; réutiliser le garde-fou existant borne le débit de commits au même régime déjà validé
# (1/10 s, toutes sessions confondues) SANS jamais abandonner — tant qu'au moins une session est
# morte, la boucle manager la retente indéfiniment à chaque passage du garde-fou. Pas de compteur
# de tentatives max : "une session morte pour toujours est bien pire qu'une récupération qui
# retente". Sur succès (queue + mempool ré-acquis), `s->active` repasse explicitement à `true`
# (le code existant ne le faisait jamais, car il ne s'attendait qu'à des sessions déjà vivantes).
#
# Fix connexe nécessaire : le garde d'entrée de st20_tx_queue_fatal_error / st_audio_queue_fatal_
# error (`if (!s->queue[s_port]) return -EIO;` / `if (!mgr->queue[port]) return -EIO;`) supposait
# TOUJOURS une queue actuellement valide (vrai pour les appels historiques : builder famine sur
# session vivante, ou hang detector burst) — sur une résurrection la queue est justement NULL
# depuis l'échec précédent ; sans le fix, bobi_zombie_retry rappellerait la fonction qui bail-out
# immédiatement sans même retenter l'acquisition. Passage en bloc conditionnel (démonte SI présente,
# sinon rien à démonter) au lieu d'un bail-out.
#
# Le fix n'annule PAS le garde-fou anti-tempête là où il sert encore (fenêtre de boot, cascade de
# commits pendant la création séquentielle des 6 sessions) : il reste la MÊME variable statique
# partagée, donc au plus 1 récupération/résurrection par 10 s sur toute la durée de vie du
# processus — seule la fenêtre TEMPORELLE change (indéfinie au lieu de s'arrêter après le 1er échec).
#
# Pas de compteur de tentatives : si la récupération réussit, le pool est plein et l'alloc repart
# (le tracker retombe à 0 au prochain succès) ; si elle échoue, la session reste candidate à
# bobi_zombie_retry à la prochaine passe du garde-fou global (10 s) — plus jamais de mise à mort
# définitive côté bobi.
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

    # 1c) struct st_tx_audio_sessions_mgr : file de demandes de récupération famine par port
    # (v4). Posée par le builder (qui tient un spinlock de session) et consommée en FIN de
    # tx_audio_sessions_tasklet (hors spinlock) — cf. l'en-tête : appeler
    # st_audio_queue_fatal_error sous mgr->mutex[sidx] est un auto-deadlock du lcore.
    # Poseur et consommateur = le MÊME lcore (le tasklet du mgr) → uint8_t suffit.
    OLD_MH = (
        "  /* the last burst succ time(tsc) */\n"
        "  uint64_t last_burst_succ_time_tsc[MTL_PORT_MAX];\n"
        "  uint64_t tx_hang_detect_time_thresh;\n"
        "\n"
        "  struct st_tx_audio_session_impl* sessions[ST_SCH_MAX_TX_AUDIO_SESSIONS];\n"
    )
    NEW_MH = (
        "  /* the last burst succ time(tsc) */\n"
        "  uint64_t last_burst_succ_time_tsc[MTL_PORT_MAX];\n"
        "  uint64_t tx_hang_detect_time_thresh;\n"
        "  " + MARK + "\n"
        "  /* v4 : demande de récupération famine en attente, par port. Posée par le builder\n"
        "   * (bobi_famine_check / bobi_zombie_retry, qui tiennent mgr->mutex[sidx]) et consommée\n"
        "   * en fin de tx_audio_sessions_tasklet, HORS de tout spinlock de session — appeler\n"
        "   * st_audio_queue_fatal_error sous mgr->mutex[sidx] fait spinner le lcore sur son\n"
        "   * propre verrou (rte_spinlock_t non récursive) : auto-deadlock, cf. en-tête. */\n"
        "  uint8_t bobi_famine_pending[MTL_PORT_MAX];\n"
        "\n"
        "  struct st_tx_audio_session_impl* sessions[ST_SCH_MAX_TX_AUDIO_SESSIONS];\n"
    )
    if h.count(OLD_MH) != 1:
        print("ERREUR: ancre 'st_tx_audio_sessions_mgr (hang_detect/sessions)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FH, h.count(OLD_MH)), file=sys.stderr)
        sys.exit(1)
    h = h.replace(OLD_MH, NEW_MH, 1)

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

    # 2e) helper bobi_zombie_retry (v3/0.47.0) : résurrection d'une session tuée par
    # bobi_famine_check — cf. en-tête. Injecté juste avant tvs_tasklet_handler (la boucle
    # manager qui fait `if (!s->active) goto exit;` juste après) ; st20_tx_queue_fatal_error
    # est déjà déclaré via st_video_transmitter.h (inclus en tête de ce .c) donc visible ici
    # bien qu'il soit défini plus bas dans le fichier (~l.4048).
    OLD_VZ_DECL = "static int tvs_tasklet_handler(void* priv) {\n"
    NEW_VZ_DECL = (
        MARK + "\n"
        "/* v3 : résurrection zombie */\n"
        "/* Résurrection d'une session tuée par bobi_famine_check (mt_txq_get / tv_mempool_init\n"
        " * ayant échoué PENDANT la récupération elle-même — cf. en-tête du patch). Sans ce\n"
        " * hook, une famine qui survit à la 1ʳᵉ tentative de récupération tue la session pour\n"
        " * toujours et en SILENCE : tvs_tasklet_handler ne rappelle plus jamais son builder\n"
        " * (`if (!s->active) goto exit;` juste en dessous) donc bobi_famine_check lui-même\n"
        " * n'est plus jamais atteint. Une session morte pour toujours est bien pire qu'une\n"
        " * récupération qui retente. Réutilise le MÊME garde-fou global que la récupération\n"
        " * initiale (bobi_video_last_recovery_tsc / 10 s) : une résurrection = une nouvelle\n"
        " * queue = un commit TM, donc le même risque de tempête — au plus UNE tentative / 10 s\n"
        " * toutes sessions dead confondues, mais qui ne s'arrête JAMAIS tant qu'au moins une\n"
        " * session reste morte (pas de compteur de tentatives max). */\n"
        "static inline void bobi_zombie_retry(struct mtl_main_impl* impl,\n"
        "                                     struct st_tx_video_session_impl* s) {\n"
        "  uint64_t bobi_now_tsc = mt_get_tsc(impl);\n"
        "  if ((bobi_now_tsc - bobi_video_last_recovery_tsc) <= " + RECOVERY_GUARD_NS + ") return;\n"
        "  bobi_video_last_recovery_tsc = bobi_now_tsc;\n"
        "  info(\"%s(%d), bobi: zombie session — retrying famine recovery\\n\", __func__, s->idx);\n"
        "  for (int bobi_sp = 0; bobi_sp < s->ops.num_port; bobi_sp++) {\n"
        "    if (!s->queue[bobi_sp]) st20_tx_queue_fatal_error(impl, s, bobi_sp);\n"
        "  }\n"
        "}\n"
        "\n"
        "static int tvs_tasklet_handler(void* priv) {\n"
    )
    if v.count(OLD_VZ_DECL) != 1:
        print("ERREUR: ancre 'tvs_tasklet_handler (déclaration)' non-unique ou introuvable "
              "dans %s (comptée %d fois, source MTL modifiée ?)" % (FV, v.count(OLD_VZ_DECL)),
              file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_VZ_DECL, NEW_VZ_DECL, 1)

    # 2f) hook dans la boucle manager : sur session inactive, retenter avant d'abandonner le tick.
    OLD_VZ_HOOK = (
        "  for (int sidx = 0; sidx < mgr->max_idx; sidx++) {\n"
        "    s = tx_video_session_try_get(mgr, sidx);\n"
        "    if (!s) continue;\n"
        "    if (!s->active) goto exit;\n"
        "\n"
        "    if (time_measure) tsc_s = mt_get_tsc(impl);\n"
    )
    NEW_VZ_HOOK = (
        "  for (int sidx = 0; sidx < mgr->max_idx; sidx++) {\n"
        "    s = tx_video_session_try_get(mgr, sidx);\n"
        "    if (!s) continue;\n"
        "    if (!s->active) {\n"
        "      " + MARK + "\n"
        "      bobi_zombie_retry(impl, s);\n"
        "      if (!s->active) goto exit;\n"
        "    }\n"
        "\n"
        "    if (time_measure) tsc_s = mt_get_tsc(impl);\n"
    )
    if v.count(OLD_VZ_HOOK) != 1:
        print("ERREUR: ancre 'tvs_tasklet_handler (boucle, goto exit)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FV, v.count(OLD_VZ_HOOK)), file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_VZ_HOOK, NEW_VZ_HOOK, 1)

    # 2g) st20_tx_queue_fatal_error : capturer l'état AVANT mutation (sert à ne pas re-notifier
    # ST_EVENT_FATAL_ERROR à chaque tentative de résurrection ratée — seulement au tout 1er échec
    # — et à savoir en fin de fonction si on ranime une session ou si elle était déjà vivante).
    # Puis : le garde d'entrée `if (!s->queue[s_port]) return -EIO;` supposait TOUJOURS une queue
    # actuellement valide (vrai pour les appels historiques : builder famine sur session vivante,
    # hang detector burst) — sur une résurrection (bobi_zombie_retry) la queue est justement NULL
    # depuis l'échec précédent : sans ce fix, on bail-out immédiatement sans retenter
    # l'acquisition. Passage en bloc conditionnel (démonte SI présente, sinon rien à démonter).
    OLD_VQ1 = (
        "  if (!s->queue[s_port]) {\n"
        "    err(\"%s(%d,%d), no queue\\n\", __func__, s_port, idx);\n"
        "    return -EIO;\n"
        "  }\n"
        "\n"
        "  /* clear all tx ring buffer */\n"
        "  if (s->packet_ring) mt_ring_dequeue_clean(s->packet_ring);\n"
        "  for (uint8_t i = 0; i < s->ops.num_port; i++) {\n"
        "    if (s->ring[i]) mt_ring_dequeue_clean(s->ring[i]);\n"
        "  }\n"
        "  /* clean the queue done mbuf */\n"
        "  mt_txq_done_cleanup(s->queue[s_port]);\n"
        "\n"
        "  mt_txq_fatal_error(s->queue[s_port]);\n"
        "  mt_txq_put(s->queue[s_port]);\n"
        "  s->queue[s_port] = NULL;\n"
    )
    NEW_VQ1 = (
        "  " + MARK + "\n"
        "  /* v3 : résurrection zombie */\n"
        "  bool bobi_was_active = s->active;\n"
        "\n"
        "  if (s->queue[s_port]) {\n"
        "    /* clear all tx ring buffer */\n"
        "    if (s->packet_ring) mt_ring_dequeue_clean(s->packet_ring);\n"
        "    for (uint8_t i = 0; i < s->ops.num_port; i++) {\n"
        "      if (s->ring[i]) mt_ring_dequeue_clean(s->ring[i]);\n"
        "    }\n"
        "    /* clean the queue done mbuf */\n"
        "    mt_txq_done_cleanup(s->queue[s_port]);\n"
        "\n"
        "    mt_txq_fatal_error(s->queue[s_port]);\n"
        "    mt_txq_put(s->queue[s_port]);\n"
        "    s->queue[s_port] = NULL;\n"
        "  }\n"
        "  /* bobi: sinon (queue déjà NULL) — résurrection après échec précédent, rien à\n"
        "   * démonter, on retente directement l'acquisition ci-dessous. */\n"
    )
    if v.count(OLD_VQ1) != 1:
        print("ERREUR: ancre 'st20_tx_queue_fatal_error (guard queue)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FV, v.count(OLD_VQ1)), file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_VQ1, NEW_VQ1, 1)

    # 2h) branche get-new-txq : ne notifier FATAL_ERROR qu'au 1er échec (bobi_was_active), pas à
    # chaque résurrection ratée ; la session reste candidate à bobi_zombie_retry (pas un aveu
    # d'abandon définitif côté commentaire).
    OLD_VQ2 = (
        "  s->queue[s_port] = mt_txq_get(impl, port, &flow);\n"
        "  if (!s->queue[s_port]) {\n"
        "    err(\"%s(%d,%d), get new txq fail\\n\", __func__, s_port, idx);\n"
        "    s->port_user_stats.common.stat_unrecoverable_error++;\n"
        "    s->active = false; /* mark current session to dead */\n"
        "    if (s->ops.notify_event) s->ops.notify_event(s->ops.priv, ST_EVENT_FATAL_ERROR, NULL);\n"
        "    return -EIO;\n"
        "  }\n"
    )
    NEW_VQ2 = (
        "  s->queue[s_port] = mt_txq_get(impl, port, &flow);\n"
        "  if (!s->queue[s_port]) {\n"
        "    err(\"%s(%d,%d), get new txq fail%s\\n\", __func__, s_port, idx,\n"
        "        bobi_was_active ? \"\" : \" (bobi: zombie retry still starved, will retry)\");\n"
        "    s->port_user_stats.common.stat_unrecoverable_error++;\n"
        "    s->active = false; /* bobi: reste candidate à bobi_zombie_retry (jamais définitif) */\n"
        "    if (bobi_was_active && s->ops.notify_event)\n"
        "      s->ops.notify_event(s->ops.priv, ST_EVENT_FATAL_ERROR, NULL);\n"
        "    return -EIO;\n"
        "  }\n"
    )
    if v.count(OLD_VQ2) != 1:
        print("ERREUR: ancre 'st20_tx_queue_fatal_error (get new txq fail)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FV, v.count(OLD_VQ2)), file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_VQ2, NEW_VQ2, 1)

    # 2i) branche reset-mempool : même traitement (notify throttlé au 1er échec seul).
    OLD_VQ3 = (
        "  ret = tv_mempool_init(impl, s->mgr, s);\n"
        "  if (ret < 0) {\n"
        "    err(\"%s(%d,%d), reset mempool fail\\n\", __func__, s_port, idx);\n"
        "    s->port_user_stats.common.stat_unrecoverable_error++;\n"
        "    s->active = false; /* mark current session to dead */\n"
        "    if (s->ops.notify_event) s->ops.notify_event(s->ops.priv, ST_EVENT_FATAL_ERROR, NULL);\n"
        "    return ret;\n"
        "  }\n"
    )
    NEW_VQ3 = (
        "  ret = tv_mempool_init(impl, s->mgr, s);\n"
        "  if (ret < 0) {\n"
        "    err(\"%s(%d,%d), reset mempool fail%s\\n\", __func__, s_port, idx,\n"
        "        bobi_was_active ? \"\" : \" (bobi: zombie retry still starved, will retry)\");\n"
        "    s->port_user_stats.common.stat_unrecoverable_error++;\n"
        "    s->active = false; /* bobi: reste candidate à bobi_zombie_retry (jamais définitif) */\n"
        "    if (bobi_was_active && s->ops.notify_event)\n"
        "      s->ops.notify_event(s->ops.priv, ST_EVENT_FATAL_ERROR, NULL);\n"
        "    return ret;\n"
        "  }\n"
    )
    if v.count(OLD_VQ3) != 1:
        print("ERREUR: ancre 'st20_tx_queue_fatal_error (reset mempool fail)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FV, v.count(OLD_VQ3)), file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_VQ3, NEW_VQ3, 1)

    # 2j) succès : si on ranimait une session (bobi_was_active == false), repasser ->active à
    # true explicitement — le code existant ne le faisait jamais (il ne s'attendait qu'à des
    # appels sur des sessions déjà vivantes). Sans ce set, tvs_tasklet_handler ne rappellerait
    # toujours pas le builder malgré une queue/mempool parfaitement fonctionnels.
    OLD_VQ4 = (
        "  /* point to next frame */\n"
        "  s->st20_pkt_idx = 0;\n"
        "  s->st20_frame_stat = ST21_TX_STAT_WAIT_FRAME;\n"
        "  s->port_user_stats.common.stat_recoverable_error++;\n"
        "  if (s->ops.notify_event)\n"
        "    s->ops.notify_event(s->ops.priv, ST_EVENT_RECOVERY_ERROR, NULL);\n"
        "\n"
        "  return 0;\n"
        "}\n"
    )
    NEW_VQ4 = (
        "  /* point to next frame */\n"
        "  s->st20_pkt_idx = 0;\n"
        "  s->st20_frame_stat = ST21_TX_STAT_WAIT_FRAME;\n"
        "  s->port_user_stats.common.stat_recoverable_error++;\n"
        "  if (!bobi_was_active) {\n"
        "    info(\"%s(%d,%d), bobi: zombie session revived, resuming tx\\n\", __func__, s_port,\n"
        "         idx);\n"
        "    s->active = true; /* bobi: résurrection réussie (cf. bobi_zombie_retry) */\n"
        "  }\n"
        "  if (s->ops.notify_event)\n"
        "    s->ops.notify_event(s->ops.priv, ST_EVENT_RECOVERY_ERROR, NULL);\n"
        "\n"
        "  return 0;\n"
        "}\n"
    )
    if v.count(OLD_VQ4) != 1:
        print("ERREUR: ancre 'st20_tx_queue_fatal_error (retour succès)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FV, v.count(OLD_VQ4)), file=sys.stderr)
        sys.exit(1)
    v = v.replace(OLD_VQ4, NEW_VQ4, 1)

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
        " * jumeau vidéo dans st_tx_video_session.c — même cascade de commits au banc).\n"
        " * Appliqué par le CONSOMMATEUR en fin de tx_audio_sessions_tasklet (v4). */\n"
        "static uint64_t bobi_audio_last_recovery_tsc;\n"
        "\n"
        "/* Même famine que côté vidéo (mempool hdr/chain/R épuisé par le commit RL d'une AUTRE\n"
        " * session) : le hang detector natif audio (st_audio_trs_burst_fail) vit au niveau\n"
        " * MANAGER (mgr->last_burst_succ_time_tsc[port]) et n'est jamais atteint tant que le\n"
        " * builder échoue avant de construire un paquet.\n"
        " *\n"
        " * ⚠⚠ v4 — NE JAMAIS appeler st_audio_queue_fatal_error ICI. Ce builder tourne sous\n"
        " * mgr->mutex[sidx] (pris par tx_audio_sessions_tasklet via tx_audio_session_try_get et\n"
        " * gardé jusqu'au tx_audio_session_put), et st_audio_queue_fatal_error reboucle sur\n"
        " * TOUTES les sessions du mgr avec tx_audio_session_get() = rte_spinlock_lock() BLOQUANT.\n"
        " * rte_spinlock_t n'est pas récursive ⇒ le lcore spinne sur son PROPRE verrou, pour\n"
        " * toujours (mesuré au banc : `mtl_sch_0` figé 10 h, gdb #0 st_audio_queue_fatal_error,\n"
        " * toutes les sessions de ce sch mortes). On se contente donc de POSER une demande ; le\n"
        " * consommateur en fin de tasklet (hors spinlock) fait le travail. */\n"
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
        "    /* Poser la demande (idempotent) — pas de garde-fou ici, il est appliqué au moment\n"
        "     * de la consommation. Le tracker est CONSERVÉ : tant que le build ne repasse pas,\n"
        "     * la demande est reposée à chaque tick (et le consommateur la rejoue dès que le\n"
        "     * garde-fou global l'autorise) ⇒ jamais d'abandon. */\n"
        "    for (int bobi_sp = 0; bobi_sp < s->ops.num_port; bobi_sp++) {\n"
        "      s->mgr->bobi_famine_pending[mt_port_logic2phy(s->port_maps, bobi_sp)] = 1;\n"
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

    # 4g) helper bobi_zombie_retry (v3/0.47.0, jumeau audio de 2e) : injecté juste avant
    # tx_audio_sessions_tasklet. st_audio_queue_fatal_error est déjà déclaré via
    # st_audio_transmitter.h (inclus en tête de ce .c).
    OLD_AZ_DECL = "static int tx_audio_sessions_tasklet(void* priv) {\n"
    NEW_AZ_DECL = (
        MARK + "\n"
        "/* v4 : résurrection zombie — POSE DE DEMANDE uniquement */\n"
        "/* Jumeau audio de bobi_zombie_retry (st_tx_video_session.c) : une session marquée\n"
        " * !active (récupération précédente ayant échoué à ré-acquérir queue/mempool) n'est plus\n"
        " * jamais buildée par la boucle manager (`if (!s->active) goto exit;`), donc\n"
        " * bobi_famine_check ne peut plus rien poser pour elle — c'est ICI qu'on repose la\n"
        " * demande, indéfiniment, tant qu'elle reste morte. Une session morte pour toujours est\n"
        " * bien pire qu'une récupération qui retente.\n"
        " * ⚠ v4 : comme bobi_famine_check, ce point tourne SOUS mgr->mutex[sidx] ⇒ interdiction\n"
        " * absolue d'appeler st_audio_queue_fatal_error ici (auto-deadlock du lcore, cf. en-tête).\n"
        " * On ne fait que poser le drapeau ; le consommateur en fin de tasklet (hors spinlock)\n"
        " * applique le garde-fou global et exécute la récupération. */\n"
        "static inline void bobi_zombie_retry(struct st_tx_audio_sessions_mgr* mgr,\n"
        "                                     struct st_tx_audio_session_impl* s) {\n"
        "  for (int bobi_sp = 0; bobi_sp < s->ops.num_port; bobi_sp++) {\n"
        "    mgr->bobi_famine_pending[mt_port_logic2phy(s->port_maps, bobi_sp)] = 1;\n"
        "  }\n"
        "}\n"
        "\n"
        "static int tx_audio_sessions_tasklet(void* priv) {\n"
    )
    if a.count(OLD_AZ_DECL) != 1:
        print("ERREUR: ancre 'tx_audio_sessions_tasklet (déclaration)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FA, a.count(OLD_AZ_DECL)), file=sys.stderr)
        sys.exit(1)
    a = a.replace(OLD_AZ_DECL, NEW_AZ_DECL, 1)

    # 4h) hook dans la boucle manager (jumeau de 2f) : une session morte repose la demande.
    OLD_AZ_HOOK = (
        "  for (int sidx = 0; sidx < mgr->max_idx; sidx++) {\n"
        "    s = tx_audio_session_try_get(mgr, sidx);\n"
        "    if (!s) continue;\n"
        "    if (!s->active) goto exit;\n"
        "    if (time_measure) tsc_s = mt_get_tsc(impl);\n"
    )
    NEW_AZ_HOOK = (
        "  for (int sidx = 0; sidx < mgr->max_idx; sidx++) {\n"
        "    s = tx_audio_session_try_get(mgr, sidx);\n"
        "    if (!s) continue;\n"
        "    if (!s->active) {\n"
        "      " + MARK + "\n"
        "      bobi_zombie_retry(mgr, s); /* pose la demande ; exécutée en fin de tasklet */\n"
        "      goto exit;\n"
        "    }\n"
        "    if (time_measure) tsc_s = mt_get_tsc(impl);\n"
    )
    if a.count(OLD_AZ_HOOK) != 1:
        print("ERREUR: ancre 'tx_audio_sessions_tasklet (boucle, goto exit)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FA, a.count(OLD_AZ_HOOK)), file=sys.stderr)
        sys.exit(1)
    a = a.replace(OLD_AZ_HOOK, NEW_AZ_HOOK, 1)

    # 4h-bis) ★ v4 : LE CONSOMMATEUR. Fin de tx_audio_sessions_tasklet, APRÈS la boucle — donc
    # après tous les tx_audio_session_put : aucun mgr->mutex[] n'est tenu par ce lcore, l'appel
    # à st_audio_queue_fatal_error (qui les prend TOUS en bloquant) est enfin légal. C'est le
    # seul point du lcore où c'est vrai. Garde-fou global anti-tempête appliqué ICI (1/10 s) ;
    # une demande bloquée par le garde-fou est CONSERVÉE et rejouée au tick suivant → retry
    # indéfini, jamais d'abandon.
    OLD_ACONS = (
        "  exit:\n"
        "    tx_audio_session_put(mgr, sidx);\n"
        "  }\n"
        "\n"
        "  return pending;\n"
        "}\n"
        "\n"
        "static int tx_audio_sessions_mgr_uinit_hw(struct st_tx_audio_sessions_mgr* mgr,\n"
    )
    NEW_ACONS = (
        "  exit:\n"
        "    tx_audio_session_put(mgr, sidx);\n"
        "  }\n"
        "\n"
        "  " + MARK + "\n"
        "  /* v4 : consommation DIFFÉRÉE des demandes de récupération famine (posées par\n"
        "   * bobi_famine_check / bobi_zombie_retry, qui tournent sous mgr->mutex[sidx] et ne\n"
        "   * PEUVENT donc pas appeler st_audio_queue_fatal_error eux-mêmes — celle-ci reprend en\n"
        "   * BLOQUANT le mutex de chaque session du mgr : l'appeler sous l'un d'eux fait spinner\n"
        "   * le lcore sur son propre verrou, définitivement (rte_spinlock_t non récursive ; banc\n"
        "   * 2026-07-14 : `mtl_sch_0` figé 10 h, toutes les sessions de ce sch mortes). Ici, la\n"
        "   * boucle ci-dessus a rendu tous les verrous → l'appel est légal. */\n"
        "  {\n"
        "    uint64_t bobi_now_tsc = mt_get_tsc(impl);\n"
        "    for (int bobi_p = 0; bobi_p < MTL_PORT_MAX; bobi_p++) {\n"
        "      if (!mgr->bobi_famine_pending[bobi_p]) continue;\n"
        "      if ((bobi_now_tsc - bobi_audio_last_recovery_tsc) <= " + RECOVERY_GUARD_NS + ")\n"
        "        break; /* garde-fou global : demande CONSERVÉE, rejouée au prochain tick */\n"
        "      bobi_audio_last_recovery_tsc = bobi_now_tsc;\n"
        "      mgr->bobi_famine_pending[bobi_p] = 0;\n"
        "      err(\"%s(%d,%d), bobi: builder famine (mempool exhausted) — triggering queue \"\n"
        "          \"fatal recovery\\n\", __func__, mgr->idx, bobi_p);\n"
        "      st_audio_queue_fatal_error(impl, mgr, bobi_p);\n"
        "    }\n"
        "  }\n"
        "\n"
        "  return pending;\n"
        "}\n"
        "\n"
        "static int tx_audio_sessions_mgr_uinit_hw(struct st_tx_audio_sessions_mgr* mgr,\n"
    )
    if a.count(OLD_ACONS) != 1:
        print("ERREUR: ancre 'tx_audio_sessions_tasklet (fin de boucle / return pending)' "
              "non-unique ou introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FA, a.count(OLD_ACONS)), file=sys.stderr)
        sys.exit(1)
    a = a.replace(OLD_ACONS, NEW_ACONS, 1)

    # 4i) st_audio_queue_fatal_error : même fix de garde d'entrée que 2g (queue mgr NULL =
    # résurrection après échec précédent du txq mgr, pas une erreur — rare en pratique côté
    # audio car le niveau mort est presque toujours la session, pas le mgr, mais gardé par
    # symétrie/robustesse).
    OLD_AQ1 = (
        "  if (!mgr->queue[port]) {\n"
        "    err(\"%s(%d,%d), no queue\\n\", __func__, idx, port);\n"
        "    return -EIO;\n"
        "  }\n"
        "\n"
        "  /* clean mbuf in the ring as we will free the mempool then */\n"
        "  if (mgr->ring[port]) mt_ring_dequeue_clean(mgr->ring[port]);\n"
        "  /* clean the queue done mbuf */\n"
        "  mt_txq_done_cleanup(mgr->queue[port]);\n"
        "\n"
        "  mt_txq_fatal_error(mgr->queue[port]);\n"
        "  mt_txq_put(mgr->queue[port]);\n"
        "  mgr->queue[port] = NULL;\n"
    )
    NEW_AQ1 = (
        "  " + MARK + "\n"
        "  /* v3 : résurrection zombie */\n"
        "  if (mgr->queue[port]) {\n"
        "    /* clean mbuf in the ring as we will free the mempool then */\n"
        "    if (mgr->ring[port]) mt_ring_dequeue_clean(mgr->ring[port]);\n"
        "    /* clean the queue done mbuf */\n"
        "    mt_txq_done_cleanup(mgr->queue[port]);\n"
        "\n"
        "    mt_txq_fatal_error(mgr->queue[port]);\n"
        "    mt_txq_put(mgr->queue[port]);\n"
        "    mgr->queue[port] = NULL;\n"
        "  }\n"
        "  /* bobi: sinon (queue mgr déjà NULL) — résurrection après échec précédent du txq\n"
        "   * mgr, rien à démonter, on retente directement l'acquisition en fin de fonction. */\n"
    )
    if a.count(OLD_AQ1) != 1:
        print("ERREUR: ancre 'st_audio_queue_fatal_error (guard queue mgr)' non-unique ou "
              "introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FA, a.count(OLD_AQ1)), file=sys.stderr)
        sys.exit(1)
    a = a.replace(OLD_AQ1, NEW_AQ1, 1)

    # 4j) boucle par-session (reset mempool) : capturer l'état avant mutation + ranimer
    # explicitement sur succès si la session était zombie (jumeau audio de 2j, mais ICI
    # niveau session — le mgr lui n'a pas de notion "active", cf. 4i pour son propre retour).
    OLD_AQ2 = (
        "    s->recovery_idx++;\n"
        "    tx_audio_session_mempool_free(s);\n"
        "    ret = tx_audio_session_mempool_init(impl, mgr, s);\n"
        "    if (ret < 0) {\n"
        "      err(\"%s(%d,%d), init mempool fail %d for session %d\\n\", __func__, idx, port, ret,\n"
        "          sidx);\n"
        "      s->port_user_stats.common.stat_unrecoverable_error++;\n"
        "      s->active = false; /* mark current session to dead */\n"
        "    } else {\n"
        "      s->port_user_stats.common.stat_recoverable_error++;\n"
        "    }\n"
    )
    NEW_AQ2 = (
        "    bool bobi_sess_was_active = s->active;\n"
        "    s->recovery_idx++;\n"
        "    tx_audio_session_mempool_free(s);\n"
        "    ret = tx_audio_session_mempool_init(impl, mgr, s);\n"
        "    if (ret < 0) {\n"
        "      err(\"%s(%d,%d), init mempool fail %d for session %d%s\\n\", __func__, idx, port,\n"
        "          ret, sidx, bobi_sess_was_active ? \"\" : \" (bobi: zombie retry still starved)\");\n"
        "      s->port_user_stats.common.stat_unrecoverable_error++;\n"
        "      s->active = false; /* bobi: reste candidate à bobi_zombie_retry (jamais définitif) */\n"
        "    } else {\n"
        "      s->port_user_stats.common.stat_recoverable_error++;\n"
        "      if (!bobi_sess_was_active) {\n"
        "        info(\"%s(%d,%d), bobi: zombie session %d revived, resuming tx\\n\", __func__,\n"
        "             idx, port, sidx);\n"
        "        s->active = true; /* bobi: résurrection réussie (cf. bobi_zombie_retry) */\n"
        "      }\n"
        "    }\n"
    )
    if a.count(OLD_AQ2) != 1:
        print("ERREUR: ancre 'st_audio_queue_fatal_error (boucle mempool par-session)' "
              "non-unique ou introuvable dans %s (comptée %d fois, source MTL modifiée ?)"
              % (FA, a.count(OLD_AQ2)), file=sys.stderr)
        sys.exit(1)
    a = a.replace(OLD_AQ2, NEW_AQ2, 1)

    open(FA, "w").write(a)
    print("patch TX builder famine recovery : appliqué (%s)" % FA)

# ============================== 5) mt_dev.c : une queue TX rendue redevient RÉUTILISABLE (v4)
# ── 2ᵉ mur, mesuré au banc 0.48.0 (une fois l'auto-deadlock levé) ──────────────────────────────
# Le filet tournait enfin (récupérations + résurrections en boucle, plus aucun lcore figé), mais
# les sessions ne revenaient pas : `st20_tx_queue_fatal_error(0,1), get new txq fail`, en boucle,
# avec des queue_id qui MONTENT sans jamais redescendre (24, 25, 26, …) → épuisement des files TX.
#
# Cause (lue dans le SHA épinglé) : `mt_dev_tx_queue_fatal_error()` (mt_dev.c ~l.1741) ne fait
# RIEN d'autre que `tx_queue->fatal_error = true;` + un err() — aucun reset matériel, c'est une
# pure MISE EN QUARANTAINE. Et `mt_dev_put_tx_queue()` ne remet QUE `active = false` : le drapeau
# fatal_error n'est JAMAIS effacé. Or `mt_dev_get_tx_queue()` skippe toute queue marquée
# (`if (tx_queue->active || tx_queue->fatal_error) continue;`). ⇒ CHAQUE récupération BRÛLE
# définitivement une file TX matérielle, sur une ressource bornée (nb_tx_q, mur HW E810 = 64).
# Un filet qui retente indéfiniment épuise donc les 64 files en quelques minutes, puis toutes les
# sessions meurent pour de bon (`get new txq fail`) : le filet se sabotait lui-même.
#
# NB : ceci ré-explique a posteriori une observation déjà consignée (docs/chantiers/DPDK_NARROW.md §7) — « les
# st20_tx_queue_fatal_error s'accumulent sous la rafale de commits RL → backstop TX FIGÉ → exit
# (q33 à tx_q=34, q42 à tx_q=44) ; ce n'est PAS le mur des leaves RL ». C'était bien un épuisement
# de files par quarantaine cumulative, mal attribué à l'époque.
#
# ── Le fix : `put` rend la file au pool RÉUTILISABLE ───────────────────────────────────────────
# `mt_dev_put_tx_queue()` efface fatal_error en même temps que active. Justification : put() n'est
# appelé que sur des chemins de démontage, et la file est INTÉGRALEMENT ré-initialisée par le
# get() suivant (dev_tx_queue_set_rl_rate → rte_tm_hierarchy_commit → stop/start du port). Garder
# une quarantaine PERMANENTE sur une ressource bornée transforme toute récupération répétée en
# épuisement garanti — un mode de défaillance bien pire que de re-servir une file qui aurait
# éventuellement besoin d'un nouveau reset (auquel cas elle sera re-marquée, re-rendue, et le
# garde-fou global borne le rythme à 1 commit / 10 s).
# Portée : cela change aussi la sémantique du hang detector natif (quarantaine → recyclage). C'est
# DÉLIBÉRÉ : sur le socle narrow/RL, la cause dominante d'un burst bloqué est le stop de port du
# commit TM, pas une file matériellement morte — quarantiner était le mauvais réflexe et coûtait
# une file à chaque fois.
FDV = "lib/src/dev/mt_dev.c"
dv = open(FDV).read()

if MARK in dv:
    print("patch TX builder famine recovery : déjà appliqué (%s)" % FDV)
else:
    OLD_PUT = (
        "  tx_queue->active = false;\n"
        "  mt_pthread_mutex_unlock(&inf->tx_queues_mutex);\n"
        "\n"
        "  info(\"%s(%d), q %d\\n\", __func__, port, queue_id);\n"
        "  return 0;\n"
        "}\n"
    )
    NEW_PUT = (
        "  tx_queue->active = false;\n"
        "  " + MARK + "\n"
        "  /* v4 : une file RENDUE redevient RÉUTILISABLE. mt_dev_tx_queue_fatal_error() ne fait\n"
        "   * que poser ce drapeau (aucun reset HW) et mt_dev_get_tx_queue() skippe toute file\n"
        "   * marquée : sans cet effacement, CHAQUE récupération brûle définitivement une des\n"
        "   * (max 64) files TX du port, et un filet qui retente épuise le pool en quelques\n"
        "   * minutes (`get new txq fail` → sessions mortes pour de bon). La file est de toute\n"
        "   * façon entièrement ré-initialisée par le get() suivant (set_rl_rate → commit TM →\n"
        "   * stop/start du port). Cf. l'en-tête du patch. */\n"
        "  tx_queue->fatal_error = false;\n"
        "  mt_pthread_mutex_unlock(&inf->tx_queues_mutex);\n"
        "\n"
        "  info(\"%s(%d), q %d\\n\", __func__, port, queue_id);\n"
        "  return 0;\n"
        "}\n"
    )
    if dv.count(OLD_PUT) != 1:
        print("ERREUR: ancre 'mt_dev_put_tx_queue (active=false)' non-unique ou introuvable dans "
              "%s (comptée %d fois, source MTL modifiée ?)" % (FDV, dv.count(OLD_PUT)),
              file=sys.stderr)
        sys.exit(1)
    dv = dv.replace(OLD_PUT, NEW_PUT, 1)
    open(FDV, "w").write(dv)
    print("patch TX builder famine recovery : appliqué (%s)" % FDV)

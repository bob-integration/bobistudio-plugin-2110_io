#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio libmtl 0.50.0 — LA FUITE DE MBUFS AU COMMIT TM, À LA SOURCE.
#
# ── Ce que ce patch corrige (et où la fuite était VRAIMENT) ────────────────────────────────────
# Hypothèse de chantier (docs/reference/TX_LAYOUTS.md, étage 4) : « le stop de port du commit TM perd les mbufs
# postés dans les descripteurs TX sans les libérer (memset de ice_reset_tx_queue) ». Cette
# hypothèse est FAUSSE sur DPDK 26.03 (la version buildée dans cette image) :
#   drivers/net/intel/ice/ice_rxtx.c:1196  ci_txq_release_all_mbufs(txq, false);   <-- FREE
#   drivers/net/intel/ice/ice_rxtx.c:1197  ice_reset_tx_queue(txq);               <-- memset APRÈS
#   drivers/net/intel/common/tx.h:360-373  boucle rte_pktmbuf_free_seg() sur TOUT le sw_ring
#   drivers/net/intel/ice/ice_ethdev.c:2886 ice_dev_stop() stoppe CHAQUE queue TX
# ⇒ le PMD ice libère bien les mbufs postés au HW. Rien à patcher côté DPDK.
#
# La fuite est DANS NOTRE PROPRE PATCH `patch_tx_hang_resetting_guard.py` (présent depuis 0.43) :
#   st_video_transmitter.c  video_trs_burst_fail() : `if (resetting) { ... return nb_pkts; }`
#   st_audio_transmitter.c  st_audio_trs_burst_fail() : `if (resetting) { ... return 1; }`
# Rendre nb_pkts = MENTIR à l'appelant (« ces paquets ont été émis »). Les appelants —
# video_burst_packet() (st_video_transmitter.c:157 : `if (tx < bulk)` … donc rien n'est
# ré-empilé quand tx == bulk), la branche trs_inflight du tasklet RL (`trs_inflight_num -= tx`)
# et st_audio_trs_session_tasklet() (`trs->inflight[port] = NULL`) — LÂCHENT alors les pointeurs
# SANS rte_pktmbuf_free : fuite nette de mbufs, à chaque burst, pendant TOUTE la fenêtre de
# commit (port stoppé 100 ms – 1 s). C'est la source UNIQUE des deux morts collatérales :
#   -207 (STI_FRAME_PKT_ALLOC_FAIL)     : mempool hdr de la session vidé (~1280 mbufs/session
#                                          mesurés au banc) → famine du builder ;
#   -203 (STI_FRAME_APP_GET_FRAME_BUSY) : chaque mbuf de charge utile perdu tenait une ref extbuf
#                                          sur frame->sh_info (st_tx_video_session.c:1255) → la
#                                          dernière libération (tv_frame_free_cb → notify_frame_done)
#                                          n'arrive JAMAIS → trame jamais rendue → -EBUSY à vie.
# (Le filet 0.48.0 rattrape le premier, le filet 0.49.0 le second : deux symptômes, une cause.)
#
# ── Le fix : ne plus mentir. `return 0` = « queue pleine, réessaie » ───────────────────────────
# Retourner 0 est la sémantique NORMALE d'un burst qui n'a rien pu émettre, et TOUS les appelants
# savent déjà la traiter — c'est le chemin emprunté chaque fois que la NIC est pleine :
#   * video_burst_packet()  : `if (tx < bulk)` → les mbufs restants sont empilés dans trs_inflight ;
#   * tasklet RL branche trs_inflight : `trs_inflight_num -= 0` → gardés, ré-essayés ;
#   * pads : `if (tx < 1) trs_pad_inflight_num++` → la référence du pad est conservée ;
#   * audio : `if (!n) { trs->inflight[port] = pkt; }` → gardé, ré-essayé.
#   * précédent amont : le transmetteur ANC (st_ancillary_transmitter.c:64-66) fait EXACTEMENT
#     ça depuis toujours (mt_txq_burst → 0 ⇒ pkt gardé en inflight) — et l'ANC, lui, n'a JAMAIS
#     fui au commit.
# Aucun mbuf n'est détruit, aucune trame n'est complétée prématurément, aucun free/refcount
# acrobatique : les paquets bâtis pendant la fenêtre de commit sont simplement ré-émis au
# redémarrage du port. Rien de bloquant n'est appelé (on RETIRE du code, on n'en ajoute pas dans
# le chemin chaud) — piège n°1 du chantier (appel bloquant sous spinlock) hors de portée.
# La ligne `last_burst_succ_time_tsc = now` du patch d'origine est CONSERVÉE : c'est elle (et
# elle seule) qui empêche le hang detector de prendre la fenêtre de commit pour un wedge.
#
# ── Observabilité (pas de repli silencieux) ────────────────────────────────────────────────────
# Compteurs par session/port : nombre de bursts DIFFÉRÉS + tsc du 1er. Au premier burst réussi
# après la fenêtre, une ligne unique par épisode :
#   bobi: reset window over after <N> ms, <B> burst(s) deferred, 0 mbuf lost
# ⇒ le nombre de mbufs perdus par commit doit être 0 ; les filets 0.48/0.49 ne doivent plus
# se déclencher (ils restent en ceinture pour les autres pertes possibles : link down, reset PMD).
#
# Idempotent, fail-fast (chaque ancre doit être trouvée exactement une fois).
# DOIT s'appliquer APRÈS patch_tx_hang_resetting_guard (ses ancres = l'état de sortie de
# celui-ci) et après famine/reclaim (qui ajoutent des lignes dans les mêmes fonctions).
import pathlib
import sys

ROOT = pathlib.Path(".")


def patch_file(rel, edits):
    p = ROOT / rel
    if not p.exists():
        print("ERREUR: %s introuvable (cwd attendu = /src/MTL)" % rel, file=sys.stderr)
        sys.exit(1)
    src = p.read_text()
    for old, new, tag in edits:
        if new in src:
            print("patch reset-no-drop: %s / %s déjà appliqué" % (rel, tag))
            continue
        cnt = src.count(old)
        if cnt != 1:
            print("ERREUR: ancre '%s' trouvée %d fois dans %s (attendu 1) — libmtl ou un patch "
                  "amont a changé" % (tag, cnt, rel), file=sys.stderr)
            sys.exit(1)
        src = src.replace(old, new, 1)
        print("patch reset-no-drop: %s / %s OK" % (rel, tag))
    p.write_text(src)


# ─────────────────────────────────────────────────────────── 1) compteurs (st_header.h)
patch_file(
    "lib/src/st2110/st_header.h",
    [
        (
            "  /* 1 = verdict « famine applicative, aucune trame piégée » déjà loggé pour cet\n"
            "   * épisode (anti-spam : un slot silencieux est un état NORMAL et permanent). */\n"
            "  uint8_t bobi_get_frame_busy_diag;\n",
            "  /* 1 = verdict « famine applicative, aucune trame piégée » déjà loggé pour cet\n"
            "   * épisode (anti-spam : un slot silencieux est un état NORMAL et permanent). */\n"
            "  uint8_t bobi_get_frame_busy_diag;\n"
            "  /* bobi.studio: TX reset no-drop */\n"
            "  /* fenêtre de commit TM en cours : bursts DIFFÉRÉS (aucun mbuf perdu) + tsc du 1er. */\n"
            "  uint64_t bobi_reset_defer_bursts[MTL_SESSION_PORT_MAX];\n"
            "  uint64_t bobi_reset_defer_first_tsc[MTL_SESSION_PORT_MAX];\n",
            "champs vidéo",
        ),
        (
            "  uint8_t bobi_famine_pending[MTL_PORT_MAX];\n",
            "  uint8_t bobi_famine_pending[MTL_PORT_MAX];\n"
            "  /* bobi.studio: TX reset no-drop */\n"
            "  uint64_t bobi_reset_defer_bursts[MTL_PORT_MAX];\n"
            "  uint64_t bobi_reset_defer_first_tsc[MTL_PORT_MAX];\n",
            "champs audio (mgr)",
        ),
    ],
)

# ─────────────────────────────────────────────────── 2) vidéo (st_video_transmitter.c)
patch_file(
    "lib/src/st2110/st_video_transmitter.c",
    [
        (
            "  if (rte_atomic32_read(&mt_if(impl, mt_port_logic2phy(s->port_maps, s_port))->resetting)) {\n"
            "    s->last_burst_succ_time_tsc[s_port] = mt_get_tsc(impl);\n"
            "    return nb_pkts;\n"
            "  }\n",
            "  if (rte_atomic32_read(&mt_if(impl, mt_port_logic2phy(s->port_maps, s_port))->resetting)) {\n"
            "    s->last_burst_succ_time_tsc[s_port] = mt_get_tsc(impl);\n"
            "    /* bobi.studio: TX reset no-drop (0.50.0) — NE PLUS PRÉTENDRE AVOIR ÉMIS.\n"
            "     * Retourner nb_pkts faisait « consommer » les mbufs par l'appelant\n"
            "     * (video_burst_packet : `if (tx < bulk)` ne ré-empile rien quand tx == bulk ;\n"
            "     * branche trs_inflight : `trs_inflight_num -= tx`) SANS jamais les libérer :\n"
            "     * fuite nette de mbufs à chaque burst pendant toute la fenêtre de commit TM\n"
            "     * (port stoppé) ⇒ mempool hdr vidé (-207) ET ref extbuf jamais rendue sur la\n"
            "     * trame en vol (-203 permanent). C'était LA source des deux morts collatérales.\n"
            "     * 0 = « rien émis, queue pleine » : sémantique déjà gérée par TOUS les appelants\n"
            "     * (les paquets restent en trs_inflight / dans le ring et sont ré-émis au\n"
            "     * redémarrage du port). Le repère de succès ci-dessus reste poussé pour que le\n"
            "     * hang detector ne prenne pas la fenêtre de commit pour un wedge. */\n"
            "    if (!s->bobi_reset_defer_first_tsc[s_port])\n"
            "      s->bobi_reset_defer_first_tsc[s_port] = mt_get_tsc(impl);\n"
            "    s->bobi_reset_defer_bursts[s_port]++;\n"
            "    return 0;\n"
            "  }\n",
            "video_trs_burst_fail: return 0",
        ),
        (
            "  s->last_burst_succ_time_tsc[s_port] = mt_get_tsc(impl);\n"
            "  /* bobi.studio: TX builder famine recovery */ s->bobi_alloc_fail_first_tsc = 0;\n"
            "  return tx;\n",
            "  s->last_burst_succ_time_tsc[s_port] = mt_get_tsc(impl);\n"
            "  /* bobi.studio: TX builder famine recovery */ s->bobi_alloc_fail_first_tsc = 0;\n"
            "  /* bobi.studio: TX reset no-drop — fin d'épisode : une ligne, chiffrée. */\n"
            "  if (unlikely(s->bobi_reset_defer_bursts[s_port])) {\n"
            "    uint64_t bobi_ms =\n"
            "        (mt_get_tsc(impl) - s->bobi_reset_defer_first_tsc[s_port]) / NS_PER_MS;\n"
            "    warn(\"%s(%d,%d), bobi: reset window over after %\" PRIu64 \" ms, %\" PRIu64\n"
            "         \" burst(s) deferred, 0 mbuf lost\\n\",\n"
            "         __func__, s->idx, s_port, bobi_ms, s->bobi_reset_defer_bursts[s_port]);\n"
            "    s->bobi_reset_defer_bursts[s_port] = 0;\n"
            "    s->bobi_reset_defer_first_tsc[s_port] = 0;\n"
            "  }\n"
            "  return tx;\n",
            "video_trs_burst: log de fin d'épisode",
        ),
    ],
)

# ─────────────────────────────────────────────────── 3) audio (st_audio_transmitter.c)
patch_file(
    "lib/src/st2110/st_audio_transmitter.c",
    [
        (
            "  /* bobi.studio: TX hang guard resetting */  /* port déjà physique ici */\n"
            "  if (rte_atomic32_read(&mt_if(impl, port)->resetting)) {\n"
            "    mgr->last_burst_succ_time_tsc[port] = mt_get_tsc(impl);\n"
            "    return 1;\n"
            "  }\n",
            "  /* bobi.studio: TX hang guard resetting */  /* port déjà physique ici */\n"
            "  if (rte_atomic32_read(&mt_if(impl, port)->resetting)) {\n"
            "    mgr->last_burst_succ_time_tsc[port] = mt_get_tsc(impl);\n"
            "    /* bobi.studio: TX reset no-drop (0.50.0) — voir st_video_transmitter.c.\n"
            "     * Retourner 1 (« émis ») faisait lâcher le mbuf par le tasklet\n"
            "     * (`trs->inflight[port] = NULL` / pkt dequeué non ré-empilé) SANS free :\n"
            "     * même fuite que côté vidéo, sur le mempool audio. 0 = gardé en inflight,\n"
            "     * ré-émis à la fin du commit. */\n"
            "    if (!mgr->bobi_reset_defer_first_tsc[port])\n"
            "      mgr->bobi_reset_defer_first_tsc[port] = mt_get_tsc(impl);\n"
            "    mgr->bobi_reset_defer_bursts[port]++;\n"
            "    return 0;\n"
            "  }\n",
            "st_audio_trs_burst_fail: return 0",
        ),
        (
            "  uint16_t tx = mt_txq_burst(mgr->queue[port], &pkt, 1);\n"
            "  if (!tx) return st_audio_trs_burst_fail(impl, mgr, port);\n"
            "  mgr->last_burst_succ_time_tsc[port] = mt_get_tsc(impl);\n"
            "  return tx;\n",
            "  uint16_t tx = mt_txq_burst(mgr->queue[port], &pkt, 1);\n"
            "  if (!tx) return st_audio_trs_burst_fail(impl, mgr, port);\n"
            "  mgr->last_burst_succ_time_tsc[port] = mt_get_tsc(impl);\n"
            "  /* bobi.studio: TX reset no-drop — fin d'épisode : une ligne, chiffrée. */\n"
            "  if (unlikely(mgr->bobi_reset_defer_bursts[port])) {\n"
            "    uint64_t bobi_ms =\n"
            "        (mt_get_tsc(impl) - mgr->bobi_reset_defer_first_tsc[port]) / NS_PER_MS;\n"
            "    warn(\"%s(%d,%d), bobi: reset window over after %\" PRIu64 \" ms, %\" PRIu64\n"
            "         \" burst(s) deferred, 0 mbuf lost\\n\",\n"
            "         __func__, mgr->idx, port, bobi_ms, mgr->bobi_reset_defer_bursts[port]);\n"
            "    mgr->bobi_reset_defer_bursts[port] = 0;\n"
            "    mgr->bobi_reset_defer_first_tsc[port] = 0;\n"
            "  }\n"
            "  return tx;\n",
            "st_audio_trs_burst: log de fin d'épisode",
        ),
    ],
)

print("patch reset-no-drop (0.50.0) : appliqué")

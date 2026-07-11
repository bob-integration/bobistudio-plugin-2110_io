#!/usr/bin/env python3
# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
#
# Patch bobi.studio pour libmtl (appliqué au build de bobi-mtl, AVANT ./build.sh).
#
# EPOCH-SHIFT TX : fenêtre d'émission d'une session TX décalée APRÈS l'epoch nominal,
# timestamp RTP restant sur l'epoch NOMINAL (= TROFF ST 2110-21 vu comme FPT côté récepteur).
#
# ── Pourquoi (chantier latence, 2026-07-11) ───────────────────────────────────────────────────
# Comportement historique libmtl : l'émission d'une trame ne peut commencer qu'à la frontière
# d'epoch (+tr_offset). Une chaîne interne dont la trame est prête ~5 ms après l'epoch paie donc
# +1 trame entière de latence (« attendre l'image suivante »). ST 2110-21 autorise pourtant un
# TROFF déclaré : émettre la fenêtre de la session `shift` ns APRÈS l'epoch, le RTP timestamp
# restant sur l'epoch nominal — le récepteur mesure simplement un FPT ≈ shift. Le TROFF est
# déclaré dans le SDP côté contrôleur (hors de ce patch).
#
# ── Convention d'activation (PAS de nouveau champ d'API publique) ─────────────────────────────
# ops.rtp_timestamp_delta_us NÉGATIF ⇒ epoch-shift de −delta µs. Un delta POSITIF conserve la
# sémantique amont intacte (stamp avancé, grille d'émission inchangée). Delta 0 ⇒ strictement
# aucun changement de logique.
#
# ── Mécanique / flux de signes (vérifié sur le SHA épinglé 32b1b4e9) ──────────────────────────
# 1) st_header.h : champ int64_t bobi_epoch_shift_ns dans struct st_tx_video_pacing (défaut 0).
# 2) tv_init_pacing (st_tx_video_session.c) : shift = −delta si delta<0 (clampé à frame_time/2 :
#    au-delà, l'arrondi de required_tai dans calc_frame_count_since_epoch changerait d'epoch).
# 3) tai_from_frame_count : + bobi_epoch_shift_ns. C'est le POINT UNIQUE : tous les consommateurs
#    suivent d'un bloc —
#      • transmission_start_time (l.64) = tai_from_frame_count + tr_offset − vrx·trs → fenêtre
#        d'émission décalée SANS toucher tr_offset (grossir tr_offset rendrait frame_idle_time
#        négatif à 1080p50 : ~0,04 ms de marge) ;
#      • tv_sync_pacing (l.692) : time_to_tx_ns = start_time_tai − cur_tai calculé sur la grille
#        DÉCALÉE → pas de clamp à 0 systématique ni d'epoch « raté » à chaque trame ;
#      • curseurs tsc/ptp (pacing_set_mbuf_time_stamp) → chemins RL (trs_target_tsc, warm-up
#        video_trs_rl_warm_up + stat_trans_troffset_mismatch) ET TSC/PTP du transmitter :
#        tous dérivent des curseurs, donc décalés ensemble ;
#      • chemins de STAMP : tv_update_rtp_time_stamp (l.737), tv_build_rtp (l.~1365),
#        tv_build_rtp_chain (l.~1438) font tai_for_rtp_ts (= ptp_time_cursor décalé, ou
#        tai_from_frame_count décalé si ST20_TX_FLAG_RTP_TIMESTAMP_EPOCH) + delta_ns.
#        delta = −shift ⇒ stamp = (nominal + shift) − shift = NOMINAL. Le mécanisme de stamp
#        existant fait le travail — on ne le re-patche PAS.
#    Émission = nominal + shift ; stamp = nominal ; FPT récepteur ≈ shift. CQFD.
#    L'entrelacé suit : la grille est par CHAMP (frame_time = période champ), shift uniforme.
# 4) calc_frame_count_since_epoch reste en comptage d'epochs NOMINAL (cur_tai/frame_time) —
#    correct : shift < frame_time/2 garantit que la fin d'émission de l'epoch N reste avant la
#    frontière de comptage de N+1.
#
# ── Hors périmètre (documenté) ────────────────────────────────────────────────────────────────
# • ST20_TX_FLAG_USER_TIMESTAMP : le stamp vient de l'utilisateur ; delta négatif le reculerait
#   sans référence à la grille — ne pas combiner (le moteur bobi n'utilise pas ce flag).
# • st30 audio (st_tx_audio_session.c) : delta négatif y recule le stamp SANS décaler la grille
#   → ne pas poser de delta négatif sur l'audio (mtl_rx.c ne le fait que sur la vidéo).
#
# Idempotent + fail-fast : ancre introuvable (source MTL changée) ⇒ échec du build.
import sys

MARK = "/* bobi.studio: EPOCH-SHIFT TX */"

# ---------------------------------------------------------------- 1) champ pacing : st_header.h
FH = "lib/src/st2110/st_header.h"
h = open(FH).read()

if MARK in h:
    print("patch epoch shift : déjà appliqué (%s)" % FH)
else:
    OLD_H = (
        "  uint64_t tsc_time_frame_start; /* start tsc time for frame start */\n"
        "};\n"
    )
    NEW_H = (
        "  uint64_t tsc_time_frame_start; /* start tsc time for frame start */\n"
        "  " + MARK + "\n"
        "  /* Décalage (ns, >=0) de la grille d'epoch de la session APRÈS l'epoch nominal (0 =\n"
        "   * comportement amont strictement inchangé). Posé par tv_init_pacing depuis un\n"
        "   * ops.rtp_timestamp_delta_us NÉGATIF (convention bobi) ; appliqué au point unique\n"
        "   * tai_from_frame_count → fenêtre d'émission ET acceptation d'epoch décalées ENSEMBLE ;\n"
        "   * le stamp RTP retombe sur l'epoch NOMINAL via ce même delta négatif (chemins de\n"
        "   * stamp existants, non patchés). Le récepteur voit FPT ≈ shift (TROFF ST 2110-21). */\n"
        "  int64_t bobi_epoch_shift_ns;\n"
        "};\n"
    )
    if OLD_H not in h:
        print("ERREUR: ancre 'struct st_tx_video_pacing' introuvable dans %s (source MTL modifiée ?)" % FH,
              file=sys.stderr)
        sys.exit(1)
    h = h.replace(OLD_H, NEW_H, 1)
    open(FH, "w").write(h)
    print("patch epoch shift : appliqué (%s)" % FH)

# ------------------------------------------- 2) init + grille décalée : st_tx_video_session.c
FC = "lib/src/st2110/st_tx_video_session.c"
c = open(FC).read()

if MARK in c:
    print("patch epoch shift : déjà appliqué (%s)" % FC)
    sys.exit(0)

# 2a) tai_from_frame_count : + shift (point unique — voir flux de signes en tête de fichier).
#     Le cast (uint64_t) explicite est EXACTEMENT la conversion implicite d'origine (return
#     double → uint64_t) : shift nul ⇒ logique octet-identique. On caste AVANT d'ajouter le
#     shift pour ne pas réintroduire la perte de précision double que nextafter contourne.
OLD_TAI = (
    "  return nextafter(frame_count * pacing->frame_time, INFINITY);\n"
    "}\n"
)
NEW_TAI = (
    "  " + MARK + "\n"
    "  /* Grille d'epoch décalée de bobi_epoch_shift_ns. Émission = nominal + shift ;\n"
    "   * stamp = (nominal + shift) + delta avec delta = −shift ⇒ stamp = NOMINAL.\n"
    "   * Cast avant l'addition : même conversion double→uint64_t que l'amont (0 = identique). */\n"
    "  return (uint64_t)nextafter(frame_count * pacing->frame_time, INFINITY) +\n"
    "         (uint64_t)pacing->bobi_epoch_shift_ns;\n"
    "}\n"
)
if OLD_TAI not in c:
    print("ERREUR: ancre 'tai_from_frame_count' introuvable dans %s (source MTL modifiée ?)" % FC,
          file=sys.stderr)
    sys.exit(1)
c = c.replace(OLD_TAI, NEW_TAI, 1)

# 2b) tv_init_pacing : dérive le shift du delta négatif (une seule fois, frame_time juste posé).
OLD_INIT = (
    "  double frame_time = (double)1000000000.0 * s->fps_tm.den / s->fps_tm.mul;\n"
    "  pacing->frame_time = frame_time;\n"
)
NEW_INIT = (
    "  double frame_time = (double)1000000000.0 * s->fps_tm.den / s->fps_tm.mul;\n"
    "  pacing->frame_time = frame_time;\n"
    "  " + MARK + "\n"
    "  /* Convention bobi : ops.rtp_timestamp_delta_us NÉGATIF ⇒ « émission décalée » — la grille\n"
    "   * d'émission recule de −delta APRÈS l'epoch nominal (via tai_from_frame_count), et ce même\n"
    "   * delta négatif, appliqué par les chemins de stamp EXISTANTS, ramène le timestamp RTP sur\n"
    "   * l'epoch nominal. Delta POSITIF : sémantique amont intacte (grille inchangée). Clamp à\n"
    "   * frame_time/2 : au-delà, l'arrondi de required_tai (calc_frame_count_since_epoch)\n"
    "   * changerait d'epoch. */\n"
    "  pacing->bobi_epoch_shift_ns = 0;\n"
    "  if (s->ops.rtp_timestamp_delta_us < 0) {\n"
    "    pacing->bobi_epoch_shift_ns =\n"
    "        -(int64_t)s->ops.rtp_timestamp_delta_us * (int64_t)NS_PER_US;\n"
    "    if (pacing->bobi_epoch_shift_ns > (int64_t)(frame_time / 2)) {\n"
    "      warn(\"%s[%02d], bobi epoch shift %\" PRId64 \" ns > frame_time/2, clamp\\n\", __func__,\n"
    "           idx, pacing->bobi_epoch_shift_ns);\n"
    "      pacing->bobi_epoch_shift_ns = (int64_t)(frame_time / 2);\n"
    "    }\n"
    "    info(\"%s[%02d], bobi epoch shift %\" PRId64\n"
    "         \" ns (émission décalée, stamp RTP sur l'epoch nominal)\\n\",\n"
    "         __func__, idx, pacing->bobi_epoch_shift_ns);\n"
    "  }\n"
)
if OLD_INIT not in c:
    print("ERREUR: ancre 'tv_init_pacing frame_time' introuvable dans %s (source MTL modifiée ?)" % FC,
          file=sys.stderr)
    sys.exit(1)
c = c.replace(OLD_INIT, NEW_INIT, 1)

open(FC, "w").write(c)
print("patch epoch shift : appliqué (%s)" % FC)

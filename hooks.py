# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Hooks orchestrateur (in-process) du plugin receiver_2110_mtl.

Même contrat de topologie/ports que receiver_2110 (mêmes shm produits) — le pipeline aval
(mixer, monitoring…) ne voit aucune différence. La normalisation des comptes/slots réutilise
celle du receiver classique ; les params spécifiques ffmpeg n'existent pas ici (ingest MTL)."""

from app.scripts import normalize_receiver_params


def before_deploy(params, context):
    """Normalise comptes vidéo/audio + slots de simulation (réutilise le receiver 2110).
    Signature (params, context) — identique au hook before_deploy de receiver_2110."""
    return normalize_receiver_params(params)


def topology_ports(hostname, params, ctx):
    nv = int(params.get("video_count") or 0)
    na = int(params.get("audio_count") or 0)
    video_fmt = {
        "width":  int(params.get("width") or 0),
        "height": int(params.get("height") or 0),
        "fps":    float(params.get("fps") or 0),
        "chroma": str(params.get("chroma") or "422"),
        "bit_depth": int(params.get("bit_depth") or 8),
    }
    audio_fmt = {"sample_rate": 48000, "channels": 8, "bit_depth": 24}
    produces  = [{"shm": f"{hostname}_{i}", "kind": "video", "format": video_fmt} for i in range(nv)]
    produces += [{"shm": f"{hostname}_audio_{i}", "kind": "audio", "format": audio_fmt} for i in range(na)]
    return {"produces": produces, "consumes": []}

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
    """Normalise comptes vidéo/audio + slots de simulation (réutilise le receiver 2110), puis
    auto-alloue la destination (mcast/port) de chaque slot TX — un mcast distinct par slot pour
    éviter le conflit de flow sur la même 5-uplet. Le shm d'entrée (tx{i}_shm) vient du câblage."""
    params = normalize_receiver_params(params)
    vmid = int(context.get("vmid", 0))
    n_tx = int(params.get("tx_count") or 0)
    slots = [dict(t or {}) for t in (params.get("tx_slots") or [])]
    while len(slots) < n_tx:
        slots.append({})
    for i, t in enumerate(slots[:n_tx]):
        t.setdefault("multicast_ip", f"239.10.30.{(vmid + i) % 254 + 1}")
        t.setdefault("dest_port", 5000)
        t.setdefault("payload_type", 96)
    params["tx_slots"] = slots[:n_tx]
    return params


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
    # Slots TX (émetteurs) = ports d'ENTRÉE câblables → destinations MXL à droite sur la page Câbles.
    # Le shm câblé est persisté à plat dans deploy_config sous tx{i}_shm (state_field du manifeste).
    consumes = []
    for i in range(int(params.get("tx_count") or 0)):
        shm = params.get(f"tx{i}_shm") or ""
        port = {"kind": "video", "slot": i, "label": f"Émetteur 2110-20 #{i + 1}", "shm": shm}
        if not shm:
            port["disconnected"] = True
        consumes.append(port)
    return {"produces": produces, "consumes": consumes}


def produced_flow_count(params, ctx):
    return int(params.get("video_count") or 0) + int(params.get("audio_count") or 0)


def produced_shms(hostname, params, ctx):
    nv = int(params.get("video_count") or 0)
    na = int(params.get("audio_count") or 0)
    return ([f"{hostname}_{i}" for i in range(nv)]
            + [f"{hostname}_audio_{i}" for i in range(na)])


def control_action(action, body, params, ctx):
    """Identique à receiver_2110 : bascule gen (simu par slot) / ident (incrustation).
    Retourne {params, hot_endpoint, hot_body} (hot-wire :8082) ou None si action inconnue."""
    vmid = ctx.get("vmid")
    if action == "gen":
        essence = body.get("essence")
        if essence not in ("video", "audio"):
            raise ValueError("essence invalide (video|audio)")
        enabled = bool(body.get("enabled", False))
        try: idx = int(body.get("idx", -1))
        except (TypeError, ValueError): idx = -1
        key = "sim_audio_slots" if essence == "audio" else "sim_video_slots"
        slots = [dict(s or {}) for s in (params.get(key) or [])]
        if not (0 <= idx < len(slots)):
            raise ValueError(f"slot {essence} #{idx} hors limites")
        slots[idx]["enabled"] = enabled
        params = dict(params); params[key] = slots
        if enabled:
            params["sim_master"] = True
        params = normalize_receiver_params(params)
        norm_slots = params.get(key) or []
        slot_cfg = norm_slots[idx] if 0 <= idx < len(norm_slots) else {"enabled": enabled}
        hot_body = {"essence": essence, "idx": idx}
        hot_body.update(slot_cfg or {})
        return {"params": params, "hot_endpoint": "/gen", "hot_body": hot_body,
                "vmid": vmid, "essence": essence, "idx": idx, "enabled": enabled}

    if action == "ident":
        try: idx = int(body.get("idx", -1))
        except (TypeError, ValueError): idx = -1
        slots = [dict(s or {}) for s in (params.get("sim_video_slots") or [])]
        if not (0 <= idx < len(slots)):
            raise ValueError(f"slot vidéo #{idx} hors limites")
        if "enabled" in body:
            slots[idx]["ident"] = bool(body["enabled"])
        if "size" in body:
            try: slots[idx]["ident_size"] = max(0, int(body["size"] or 0))
            except (TypeError, ValueError): pass
        params = dict(params); params["sim_video_slots"] = slots
        params = normalize_receiver_params(params)
        norm = params.get("sim_video_slots") or []
        cur = norm[idx] if 0 <= idx < len(norm) else {}
        enabled = bool(cur.get("ident")); size = int(cur.get("ident_size") or 0)
        return {"params": params, "hot_endpoint": "/ident",
                "hot_body": {"idx": idx, "enabled": enabled, "size": size},
                "vmid": vmid, "idx": idx, "ident": enabled, "size": size}

    return None


def source_shm(params, context):
    """Colonnes source/shm_out pour le dashboard (identique receiver_2110)."""
    nv = int(params.get("video_count", 0))
    na = int(params.get("audio_count", 0))
    hn = params.get("hostname", "mxl")
    if params.get("sim_master") or params.get("simulation"):
        parts  = (([f"{nv}v simu"] if nv else []) + ([f"{na}a simu"] if na else []))
        source = " + ".join(parts) or "simu"
    else:
        parts  = (([f"{nv} NMOS vidéo"] if nv else []) + ([f"{na} NMOS audio"] if na else []))
        source = " + ".join(parts) or "NMOS"
    shm_parts = []
    if nv: shm_parts.append(f"{hn}_0..{nv-1}" if nv > 1 else f"{hn}_0")
    if na: shm_parts.append(f"{hn}_audio_0..{na-1}" if na > 1 else f"{hn}_audio_0")
    return {"source": source, "shm": " · ".join(shm_parts) or "—"}

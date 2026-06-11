# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Hooks orchestrateur (in-process) du plugin 2110_io.

Même contrat de topologie/ports que receiver_2110 (mêmes shm produits) — le pipeline aval
(mixer, monitoring…) ne voit aucune différence. La normalisation des comptes/slots réutilise
celle du receiver classique ; les params spécifiques ffmpeg n'existent pas ici (ingest MTL)."""

from app.scripts import normalize_receiver_params


def before_deploy(params, context):
    """Normalise comptes vidéo/audio + slots de simulation (réutilise le receiver 2110), puis
    auto-alloue la destination (mcast/port) de chaque slot TX — un mcast distinct par slot pour
    éviter le conflit de flow sur la même 5-uplet. Le shm d'entrée (tx{i}_shm) vient du câblage."""
    params = normalize_receiver_params(params, settings=context.get("settings"))
    vmid = int(context.get("vmid", 0))
    n_tx = int(params.get("tx_count") or 0)
    # Nombre de flux audio PAR slot TX = ratio audio_count/tx_count (≥1). Chaque slot actif consomme
    # ainsi 1 vidéo + N audio + 1 ANC ; avec N=1 → 3 queues AF_XDP/slot, ce qui fait coller la maths
    # du budget (active*3) côté orchestrateur et évite la sur-souscription des queues.
    n_aud_per_tx = max(1, (int(params.get("audio_count") or 0) // n_tx) if n_tx else 1)
    slots = [dict(t or {}) for t in (params.get("tx_slots") or [])]
    while len(slots) < n_tx:
        slots.append({})
    for i, t in enumerate(slots[:n_tx]):
        t.setdefault("multicast_ip", f"239.10.30.{(vmid + i) % 254 + 1}")
        t.setdefault("dest_port", 5000)
        t.setdefault("payload_type", 96)
        # Audio TX : n_aud_per_tx flux — plages 239.10.40.x, 239.10.41.x, …
        base_a = (vmid * 2 + i) % 254 + 1
        audios_alloc = [
            {"multicast_ip": f"239.10.{40 + ai}.{base_a}", "dest_port": 5004 + i * 4 + ai * 2}
            for ai in range(n_aud_per_tx)
        ]
        # Complète depuis l'existant si présent, sinon alloue ; TRONQUE au ratio (un container
        # legacy provisionné avec 2 audios/slot repasse à 1 au prochain redéploiement → fin de la
        # sur-souscription « 2 audios par TX »).
        t["audios"] = ((t.get("audios") or []) + audios_alloc)[:n_aud_per_tx]
        # ANC TX (1 flux) : plage 239.10.50.x
        t.setdefault("anc_multicast_ip", f"239.10.50.{(vmid + i) % 254 + 1}")
        t.setdefault("anc_dest_port", 5008 + i * 2)
        # SMPTE 2022-7 — leg1 auto-allouée une seule fois (setdefault : ne pas écraser)
        if params.get("smpte_2022_7"):
            t.setdefault("multicast_ip_leg1", f"239.10.130.{(vmid + i) % 254 + 1}")
            t.setdefault("dest_port_leg1", t.get("dest_port") or 5000)
            audios = t.get("audios") or []
            for ai, a in enumerate(audios):
                a.setdefault("multicast_ip_leg1", f"239.10.{140 + ai}.{base_a}")
                a.setdefault("dest_port_leg1", a.get("dest_port") or 5004)
            t.setdefault("anc_multicast_ip_leg1", f"239.10.150.{(vmid + i) % 254 + 1}")
            t.setdefault("anc_dest_port_leg1", t.get("anc_dest_port") or 5008)
    params["tx_slots"] = slots[:n_tx]
    # active_rx_count / active_tx_count : combien de slots apparaissent dans NMOS.
    # setdefault → préservé lors des re-déploiements (l'opérateur a peut-être activé des slots).
    nv = int(params.get("video_count") or 0)
    params.setdefault("active_rx_count", min(8, nv))
    params["active_rx_count"] = max(0, min(int(params["active_rx_count"] or 0), nv))
    params.setdefault("active_tx_count", min(8, n_tx))
    params["active_tx_count"] = max(0, min(int(params["active_tx_count"] or 0), n_tx))
    return params


def topology_ports(hostname, params, ctx):
    nv = int(params.get("video_count") or 0)
    na = int(params.get("audio_count") or 0)
    nd = int(params.get("anc_count") or 0)
    video_fmt = {
        "width":  int(params.get("width") or 0),
        "height": int(params.get("height") or 0),
        "fps":    float(params.get("fps") or 0),
        "chroma": str(params.get("chroma") or "422"),
        "bit_depth": int(params.get("bit_depth") or 8),
        "scan":   str(params.get("scan") or "p"),
        "field_order": str(params.get("field_order") or ""),
    }
    audio_fmt = {"sample_rate": 48000, "channels": 8, "bit_depth": 24}
    produces  = [{"shm": f"{hostname}_{i}", "kind": "video", "format": video_fmt} for i in range(nv)]
    produces += [{"shm": f"{hostname}_audio_{i}", "kind": "audio", "format": audio_fmt} for i in range(na)]
    produces += [{"shm": f"{hostname}_anc_{i}", "kind": "data", "format": {"type": "smpte291"}} for i in range(nd)]
    # Slots TX (émetteurs) = ports d'ENTRÉE câblables → destinations MXL à droite sur la page Câbles.
    # Le shm câblé est persisté à plat dans deploy_config sous tx{i}_shm (state_field du manifeste).
    consumes = []
    txs = params.get("tx_slots") or []
    for i in range(int(params.get("tx_count") or 0)):
        shm = params.get(f"tx{i}_shm") or ""
        t = txs[i] if i < len(txs) else {}
        dest = "{}:{}".format(t.get("multicast_ip"), t.get("dest_port") or 5000) if t.get("multicast_ip") else ""
        port = {"kind": "video", "slot": i, "label": f"TX #{i + 1}",
                "shm": shm, "dest": dest}   # dest = destination 2110-20 (éditable à chaud)
        if not shm:
            port["disconnected"] = True
        consumes.append(port)
    return {"produces": produces, "consumes": consumes}


def produced_flow_count(params, ctx):
    return (int(params.get("video_count") or 0) + int(params.get("audio_count") or 0)
            + int(params.get("anc_count") or 0))


def produced_shms(hostname, params, ctx):
    nv = int(params.get("video_count") or 0)
    na = int(params.get("audio_count") or 0)
    nd = int(params.get("anc_count") or 0)
    return ([f"{hostname}_{i}" for i in range(nv)]
            + [f"{hostname}_audio_{i}" for i in range(na)]
            + [f"{hostname}_anc_{i}" for i in range(nd)])


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
        if essence == "video" and "pattern" in body:
            slots[idx]["pattern"] = str(body["pattern"])
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

    if action == "gen_tx":
        try: idx = int(body.get("idx", -1))
        except (TypeError, ValueError): idx = -1
        tx_slots = list(params.get("tx_slots") or [])
        if not (0 <= idx < len(tx_slots)):
            raise ValueError(f"slot TX #{idx} hors limites")
        slots = [dict(s or {}) for s in tx_slots]
        slots[idx]["gen_enabled"] = bool(body.get("enabled", False))
        if "pattern" in body:
            slots[idx]["gen_pattern"] = str(body["pattern"])
        params = dict(params); params["tx_slots"] = slots
        return {"params": params, "hot_endpoint": "/gen_tx",
                "hot_body": {"idx": idx, "enabled": slots[idx]["gen_enabled"],
                             "pattern": slots[idx].get("gen_pattern", "bars")},
                "vmid": vmid}

    return None


def source_shm(params, context):
    """Colonnes source/shm_out pour le dashboard (identique receiver_2110)."""
    nv = int(params.get("video_count", 0))
    na = int(params.get("audio_count", 0))
    nd = int(params.get("anc_count", 0))
    hn = params.get("hostname", "mxl")
    if params.get("sim_master") or params.get("simulation"):
        parts  = (([f"{nv}v simu"] if nv else []) + ([f"{na}a simu"] if na else []) + ([f"{nd} ANC"] if nd else []))
        source = " + ".join(parts) or "simu"
    else:
        parts  = (([f"{nv} NMOS vidéo"] if nv else []) + ([f"{na} NMOS audio"] if na else []) + ([f"{nd} NMOS ANC"] if nd else []))
        source = " + ".join(parts) or "NMOS"
    shm_parts = []
    if nv: shm_parts.append(f"{hn}_0..{nv-1}" if nv > 1 else f"{hn}_0")
    if na: shm_parts.append(f"{hn}_audio_0..{na-1}" if na > 1 else f"{hn}_audio_0")
    if nd: shm_parts.append(f"{hn}_anc_0..{nd-1}" if nd > 1 else f"{hn}_anc_0")
    return {"source": source, "shm": " · ".join(shm_parts) or "—"}

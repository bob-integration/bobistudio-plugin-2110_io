# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2026 BOBI SAS, France
# Auteur : Cyril Mazouer, pour le compte de BOBI SAS
# Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

"""Hooks orchestrateur (in-process) du plugin 2110_io.

Même contrat de topologie/ports que receiver_2110 (mêmes shm produits) — le pipeline aval
(mixer, monitoring…) ne voit aucune différence. La normalisation des comptes/slots réutilise
celle du receiver classique ; les params spécifiques ffmpeg n'existent pas ici (ingest MTL)."""

import re

from app.scripts import normalize_receiver_params


def _derive_audio_shm(video_shm, ai):
    """shm vidéo câblé → shm audio dérivé (miroir controller._derive_audio_shm) : 'host_0' + ai
    → 'host_audio_{ai}'. '' si pas de suffixe _N."""
    m = re.match(r"^(.*)_(\d+)$", (video_shm or "").strip())
    return "{}_audio_{}".format(m.group(1), ai) if m else ""


def _derive_anc_shm(video_shm):
    """shm vidéo câblé → shm ANC dérivé (miroir controller._derive_anc_shm) : 'mtl_0' → 'mtl_anc_0'."""
    m = re.match(r"^(.*)_(\d+)$", (video_shm or "").strip())
    return "{}_anc_{}".format(m.group(1), m.group(2)) if m else ""


def before_deploy(params, context):
    """Normalise comptes vidéo/audio + slots de simulation (réutilise le receiver 2110), puis
    auto-alloue la destination (mcast/port) de chaque slot TX — un mcast distinct par slot pour
    éviter le conflit de flow sur la même 5-uplet. Le shm d'entrée (tx{i}_shm) vient du câblage."""
    params = normalize_receiver_params(params, settings=context.get("settings"))
    vmid = int(context.get("vmid", 0))
    n_tx = int(params.get("tx_count") or 0)
    nv   = int(params.get("video_count") or 0)
    from app import io2110_flows as _iof
    # active_rx_count / active_tx_count : fenêtre de visibilité NMOS. POSÉS D'ABORD (avant de dériver
    # les flux) car la dérivation legacy s'appuie dessus. setdefault → préservés au re-déploiement.
    params.setdefault("active_rx_count", min(8, nv))
    params["active_rx_count"] = max(0, min(int(params["active_rx_count"] or 0), nv))
    params.setdefault("active_tx_count", min(8, n_tx))
    params["active_tx_count"] = max(0, min(int(params["active_tx_count"] or 0), n_tx))
    # Modèle de flux composables (« Option A ») : rx_flows/tx_flows font foi pour le GROUPEMENT et le
    # nombre d'audios/ANC par vidéo. Dérivés du legacy au 1er passage (container neuf via la palette),
    # puis PRÉSERVÉS — le hot add/remove (routes /flows/*) les édite. Ne jamais écraser une liste.
    if not params.get("rx_flows"):
        params["rx_flows"] = _iof.derive_rx_flows(params)
    if not params.get("tx_flows"):
        params["tx_flows"] = _iof.derive_tx_flows(params)
    params["rx_flows"] = _iof.normalize(params["rx_flows"])
    params["tx_flows"] = _iof.normalize(params["tx_flows"])
    tx_flows = params["tx_flows"]
    # active_rx_count / active_tx_count = nombre de flux VIDÉO actifs (les listes font foi). Tenus
    # synchrones car le budget de queues/lcores (docker_driver._auto_lcores, _mtl_active_caps) les lit.
    params["active_rx_count"] = min(nv, len([f for f in params["rx_flows"] if f["essence"] == "video"])) if nv else \
        len([f for f in params["rx_flows"] if f["essence"] == "video"])
    params["active_tx_count"] = min(n_tx, len([f for f in tx_flows if f["essence"] == "video"]))
    n_aud_per_tx = max(1, (int(params.get("audio_count") or 0) // n_tx) if n_tx else 1)  # défaut pool
    slots = [dict(t or {}) for t in (params.get("tx_slots") or [])]
    while len(slots) < n_tx:
        slots.append({})
    for i, t in enumerate(slots[:n_tx]):
        t.setdefault("multicast_ip", f"239.10.30.{(vmid + i) % 254 + 1}")
        t.setdefault("dest_port", 5000)
        t.setdefault("payload_type", 96)
        # Format GÉN par slot : défaut = format du moteur ; un override (page Destinations) est
        # préservé (setdefault). Ne sert qu'au générateur/mire : un slot CÂBLÉ suit sa source
        # (adapts_input via :8082/input), ce format est alors ignoré (cf. push_tx_slots).
        t.setdefault("width",  int(params.get("width") or 1920))
        t.setdefault("height", int(params.get("height") or 1080))
        t.setdefault("fps",    float(params.get("fps") or 25))
        t.setdefault("scan",   str(params.get("scan") or "p"))
        # Audios du slot = flux audio ATTACHÉS (tx_flows), N quelconque (plus de cap à 2). Les
        # mcast/port sont alloués par idx FLAT de flux → uniques même au-delà de 2 audios/slot.
        # Un slot vidéo-seul (0 flux audio attaché) n'a aucune destination audio. Les slots SANS
        # flux vidéo (pool inactif) ne sont pas touchés ici (activation = ajout de flux, route /flows).
        aud_idxs = _iof.tx_slot_audio_idxs(tx_flows, i)
        if aud_idxs or any(f["essence"] == "video" and f["idx"] == i for f in tx_flows):
            existing = t.get("audios") or []
            audios = []
            for ai, aidx in enumerate(aud_idxs):
                a = dict(existing[ai]) if ai < len(existing) else {}
                a.setdefault("multicast_ip", f"239.10.{40 + (aidx % 8)}.{(vmid * 2 + aidx) % 254 + 1}")
                a.setdefault("dest_port", 5004 + aidx * 2)
                # Générateur de tonalité par sous-flux audio — défaut OFF, 1 kHz / -18 dBFS.
                a.setdefault("tone", {"enabled": False, "freq": 1000, "level_db": -18.0,
                                      "active": [True] * 8, "rupted": [False] * 8})
                audios.append(a)
            t["audios"] = audios
        # ANC TX (1 flux par slot) : plage 239.10.50.x
        t.setdefault("anc_multicast_ip", f"239.10.50.{(vmid + i) % 254 + 1}")
        t.setdefault("anc_dest_port", 5008 + i * 2)
        # SMPTE 2022-7 — leg1 auto-allouée une seule fois (setdefault : ne pas écraser)
        if params.get("smpte_2022_7"):
            t.setdefault("multicast_ip_leg1", f"239.10.130.{(vmid + i) % 254 + 1}")
            t.setdefault("dest_port_leg1", t.get("dest_port") or 5000)
            for ai, a in enumerate(t.get("audios") or []):
                aidx = aud_idxs[ai] if ai < len(aud_idxs) else ai
                a.setdefault("multicast_ip_leg1", f"239.10.{140 + (aidx % 8)}.{(vmid * 2 + aidx) % 254 + 1}")
                a.setdefault("dest_port_leg1", a.get("dest_port") or 5004)
            t.setdefault("anc_multicast_ip_leg1", f"239.10.150.{(vmid + i) % 254 + 1}")
            t.setdefault("anc_dest_port_leg1", t.get("anc_dest_port") or 5008)
    params["tx_slots"] = slots[:n_tx]
    # Pool d'entrées audio TX (capacité contrôleur) : ≥ legacy (n_tx × défaut) ET ≥ plus haut idx de
    # flux audio + 1 (réserve les slots pour le hot-add). tx_audio_count = repeat du manifeste.
    max_aidx = max([f["idx"] for f in tx_flows if f["essence"] == "audio"] + [-1])
    params["tx_audio_count"] = max(n_tx * n_aud_per_tx, max_aidx + 1)
    # Câblage shm audio/ANC : un flux audio/ANC dont la vidéo est câblée mais sans câble explicite
    # hérite du shm DÉRIVÉ (suit la vidéo) → pas de sortie muette. setdefault : ne jamais écraser.
    for i in range(n_tx):
        vshm = (params.get(f"tx{i}_shm") or "").strip()
        for ai, aidx in enumerate(_iof.tx_slot_audio_idxs(tx_flows, i)):
            params.setdefault(f"tx_audio{aidx}_shm", _derive_audio_shm(vshm, ai))
        params.setdefault(f"tx_anc{i}_shm", _derive_anc_shm(vshm))
    return params


def topology_ports(hostname, params, ctx):
    from app import io2110_flows as _iof
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
    # « Option A » : les ports produits (RX) reflètent les flux ACTIFS (rx_flows) et portent un
    # `group` = id de la vidéo d'attache (ou la vidéo elle-même) → la page Câbles regroupe la vidéo
    # et ses audios/ANC ; un flux indépendant a son propre groupe.
    rx_flows = _iof.active_flows(params, "rx")
    _fmt = {"video": video_fmt, "audio": audio_fmt, "anc": {"type": "smpte291"}}
    _shmf = {"video": f"{hostname}_{{}}", "audio": f"{hostname}_audio_{{}}", "anc": f"{hostname}_anc_{{}}"}
    _kindf = {"video": "video", "audio": "audio", "anc": "data"}
    produces = []
    for f in rx_flows:
        ess = f["essence"]
        produces.append({"shm": _shmf[ess].format(f["idx"]), "kind": _kindf[ess],
                         "format": _fmt[ess],
                         "group": f["id"] if ess == "video" else (f.get("attached_to") or f["id"])})
    # Slots TX (émetteurs) = ports d'ENTRÉE câblables → destinations MXL à droite sur la page Câbles.
    # Pilotés par tx_flows : par slot vidéo, le port vidéo + ses audios attachés (N) + son ANC.
    consumes = []
    tx_flows = _iof.active_flows(params, "tx")
    txs = params.get("tx_slots") or []
    for vf in [x for x in tx_flows if x["essence"] == "video"]:
        i = vf["idx"]
        t = txs[i] if i < len(txs) else {}
        shm = params.get(f"tx{i}_shm") or ""
        dest = "{}:{}".format(t.get("multicast_ip"), t.get("dest_port") or 5000) if t.get("multicast_ip") else ""
        port = {"kind": "video", "slot": i, "label": f"TX #{i + 1}", "shm": shm, "dest": dest,
                "group": vf["id"]}
        if not shm:
            port["disconnected"] = True
        consumes.append(port)
        # Ports AUDIO (2110-30) — un par flux audio attaché ; slot = idx FLAT du flux (= tx_audio{idx}_shm)
        aud_idxs = _iof.tx_slot_audio_idxs(tx_flows, i)
        audios = t.get("audios") or []
        for ai, ap in enumerate(aud_idxs):
            ashm = params.get(f"tx_audio{ap}_shm") or ""
            a = audios[ai] if ai < len(audios) else {}
            adest = "{}:{}".format(a.get("multicast_ip"), a.get("dest_port") or 5004) if a.get("multicast_ip") else ""
            alabel = f"TX #{i + 1} AUD" + (f" {ai + 1}" if len(aud_idxs) > 1 else "")
            aport = {"kind": "audio", "slot": ap, "label": alabel, "shm": ashm, "dest": adest, "group": vf["id"]}
            if not ashm:
                aport["disconnected"] = True
            consumes.append(aport)
        # Port ANC (2110-40) — présent si un flux ANC est attaché ; slot = i (espace propre au kind data)
        if _iof.tx_slot_has_anc(tx_flows, i):
            dshm = params.get(f"tx_anc{i}_shm") or ""
            ddest = "{}:{}".format(t.get("anc_multicast_ip"), t.get("anc_dest_port") or 5008) if t.get("anc_multicast_ip") else ""
            dport = {"kind": "data", "slot": i, "label": f"TX #{i + 1} ANC", "shm": dshm, "dest": ddest,
                     "group": vf["id"]}
            if not dshm:
                dport["disconnected"] = True
            consumes.append(dport)
    return {"produces": produces, "consumes": consumes}


def wire_followers(kind, shm, slot, params, ctx):
    """Câblage VIDÉO d'une sortie TX → l'audio (×N) et l'ANC SUIVENT, en réutilisant les shm RÉELS
    produits par la source (ctx['producer_produces']) — PAS une dérivation par nom (le player produit
    'p1'/'p1_audio'/'p1_anc_0', pas 'p1_0'/'p1_audio_0'). Politique « toujours resuivre » : (re)câbler
    la vidéo (re)pointe l'audio/ANC ; décâbler (shm='' / pas de producteur) → followers vidés (silence).
    Appariement par PROGRAMME : la vidéo câblée a un rang v parmi les vidéos de la source ; chaque vidéo
    porte k = nb_audio/nb_vidéo flux audio (et k' data). Renvoie [{essence, slot, shm, state_field}] ou
    None si non applicable. Un câble audio/ANC direct (kind audio/data) ne déclenche AUCUN follower."""
    if kind != "video" or slot is None:
        return None
    try:
        i = int(slot)
    except (TypeError, ValueError):
        return None
    n_tx = int(params.get("tx_count") or 0)
    if not (0 <= i < n_tx):
        return None
    # « Option A » : les audios/ANC qui SUIVENT la vidéo = les flux attachés à ce slot (tx_flows),
    # par leur idx FLAT (= tx_audio{idx}_shm). Plus de pas fixe homogène.
    from app import io2110_flows as _iof
    tx_flows = _iof.active_flows(params, "tx")
    aud_idxs = _iof.tx_slot_audio_idxs(tx_flows, i)
    has_anc = _iof.tx_slot_has_anc(tx_flows, i)
    produces = ctx.get("producer_produces") or []     # [{essence, shm}] de la source (vide → décâblage)
    vids = [p["shm"] for p in produces if (p.get("essence") or "video") == "video"]
    auds = [p["shm"] for p in produces if p.get("essence") == "audio"]
    dats = [p["shm"] for p in produces if p.get("essence") == "data"]
    v = vids.index(shm) if shm in vids else 0
    # k flux audio / k' data PAR vidéo (groupage par programme) ; repli : tout si non divisible.
    ka = (len(auds) // len(vids)) if vids and len(auds) % len(vids) == 0 else 0
    kd = (len(dats) // len(vids)) if vids and len(dats) % len(vids) == 0 else 0
    prog_a = auds[v * ka:(v + 1) * ka] if ka else auds
    prog_d = dats[v * kd:(v + 1) * kd] if kd else dats
    followers = []
    for ai, ap in enumerate(aud_idxs):
        followers.append({"essence": "audio", "slot": ap,
                          "shm": (prog_a[ai] if ai < len(prog_a) else ""),
                          "state_field": f"tx_audio{ap}_shm"})
    if has_anc:
        followers.append({"essence": "data", "slot": i,
                          "shm": (prog_d[0] if prog_d else ""),
                          "state_field": f"tx_anc{i}_shm"})
    return followers


def produced_flow_count(params, ctx):
    from app import io2110_flows as _iof
    return len(_iof.active_flows(params, "rx"))


def produced_shms(hostname, params, ctx):
    from app import io2110_flows as _iof
    _shmf = {"video": f"{hostname}_{{}}", "audio": f"{hostname}_audio_{{}}", "anc": f"{hostname}_anc_{{}}"}
    return [_shmf[f["essence"]].format(f["idx"]) for f in _iof.active_flows(params, "rx")]


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

    if action == "ident_tx":
        try: idx = int(body.get("idx", -1))
        except (TypeError, ValueError): idx = -1
        tx_slots = list(params.get("tx_slots") or [])
        if not (0 <= idx < len(tx_slots)):
            raise ValueError(f"slot TX #{idx} hors limites")
        slots = [dict(s or {}) for s in tx_slots]
        if "enabled" in body:
            slots[idx]["ident"] = bool(body["enabled"])
        if "size" in body:
            try: slots[idx]["ident_size"] = max(0, int(body["size"] or 0))
            except (TypeError, ValueError): pass
        params = dict(params); params["tx_slots"] = slots
        enabled = bool(slots[idx].get("ident")); size = int(slots[idx].get("ident_size") or 0)
        return {"params": params, "hot_endpoint": "/ident_tx",
                "hot_body": {"idx": idx, "enabled": enabled, "size": size},
                "vmid": vmid, "idx": idx, "ident": enabled, "size": size}

    if action == "tone_tx":
        # Générateur de tonalité (1 kHz/-18 dBFS, canaux + ruptage) d'un sous-flux audio TX.
        try: idx = int(body.get("idx", -1)); ai = int(body.get("ai", -1))
        except (TypeError, ValueError): idx = ai = -1
        tx_slots = list(params.get("tx_slots") or [])
        if not (0 <= idx < len(tx_slots)):
            raise ValueError(f"slot TX #{idx} hors limites")
        slots = [dict(s or {}) for s in tx_slots]
        audios = [dict(a or {}) for a in (slots[idx].get("audios") or [])]
        if not (0 <= ai < len(audios)):
            raise ValueError(f"flux audio #{ai} hors limites sur le slot TX #{idx}")
        tone = dict(audios[ai].get("tone") or
                    {"enabled": False, "freq": 1000, "level_db": -18.0,
                     "active": [True] * 8, "rupted": [False] * 8})
        if "enabled" in body:
            tone["enabled"] = bool(body["enabled"])
        if "freq" in body:
            try: tone["freq"] = max(20, min(20000, int(body["freq"] or 1000)))
            except (TypeError, ValueError): pass
        if "level_db" in body:
            try: tone["level_db"] = max(-60.0, min(0.0, float(body["level_db"])))
            except (TypeError, ValueError): pass
        if isinstance(body.get("active"), list):
            tone["active"] = [bool(x) for x in body["active"][:8]] + [False] * max(0, 8 - len(body["active"]))
        if isinstance(body.get("rupted"), list):
            tone["rupted"] = [bool(x) for x in body["rupted"][:8]] + [False] * max(0, 8 - len(body["rupted"]))
        audios[ai]["tone"] = tone
        slots[idx] = dict(slots[idx]); slots[idx]["audios"] = audios
        params = dict(params); params["tx_slots"] = slots
        return {"params": params, "hot_endpoint": "/tone_tx",
                "hot_body": {"idx": idx, "ai": ai, "enabled": tone["enabled"],
                             "freq": tone["freq"], "level_db": tone["level_db"],
                             "active": tone["active"], "rupted": tone["rupted"]},
                "vmid": vmid, "idx": idx, "ai": ai, "enabled": tone["enabled"]}

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

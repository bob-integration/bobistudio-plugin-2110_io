// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

// Carte de contrôle 2110_io. Enregistrée sur window.MXLPlugins["2110_io"].
// mount(el, vmid, ctx) est appelée par le shell Sources après injection de control.html.
// Données par-vmid : GET /api/nmos/receivers/<vmid>/detail. Générateur par slot :
// POST /api/nmos/receivers/<vmid>/gen/<essence>/<idx>. Auto-refresh interne (5 s).
window.MXLPlugins = window.MXLPlugins || {};
window.MXLPlugins["2110_io"] = {
  _timers: {},

  mount(el, vmid, ctx){
    const body = el.querySelector('.rx-body') || el;
    const toast = (ctx && ctx.toast) || (()=>{});

    // Toute action moteur DISRUPTIVE (relance mtl_init / recréation → coupure de TOUS les flux) est
    // bloquée par le serveur (HTTP 409 + needs_confirm + reason). On confirme explicitement puis on
    // ré-émet avec confirm:true. Les ops à chaud passent normalement (jamais de 409). Retourne la
    // Response (ou null si l'utilisateur annule la confirmation).
    const mtlMutate = async (url, payload) => {
      payload = payload || {};
      let r = await fetch(url, {method:'POST', headers:{'Content-Type':'application/json'},
                                body: JSON.stringify(payload)});
      if (r.status === 409) {
        const j = await r.json().catch(()=>({}));
        if (j && j.needs_confirm) {
          if (!confirm((j.reason || 'Cette opération coupera brièvement TOUS les flux du moteur.')
                       + '\n\nContinuer ?')) return null;
          r = await fetch(url, {method:'POST', headers:{'Content-Type':'application/json'},
                                body: JSON.stringify(Object.assign({}, payload, {confirm:true}))});
        }
      }
      return r;
    };

    const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
      '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    }[c]));

    const VIDEO_PATTERNS = {
      bars:     'Barres SMPTE',
      gradient: 'Dégradé gris',
      black:    'Fond noir',
      moving:   'Barre animée',
    };

    // SDP actif courant par index de flux vidéo (rempli au rendu, lu par la modale).
    const _sdpByIdx = {};

    function fmtFps(v){
      if (v == null) return '<span style="color:var(--text-muted)">—</span>';
      const n = Number(v);
      const col = n >= 24 ? 'var(--status-running-fg)' : n > 0 ? '#e8a33d' : 'var(--status-stopped-fg)';
      return `<span style="color:${col}">${n.toFixed(1)}</span>`;
    }
    // Taille IDENT (10..120 px) → angle du rotatif (course 270°, de -135° à +135°).
    function _identAngle(v){ v = Math.max(10, Math.min(120, Number(v) || 12)); return -135 + (v - 10) / 110 * 270; }
    function fmtVideoFormat(o){
      if (o.width && o.height) {
        const sc = (o.scan === 'i') ? 'i' : 'p';      // p ou i TOUJOURS affiché (notation broadcast)
        const fpsTxt = (o.fps != null) ? String(Number(o.fps)).replace(/\.0$/, '') : '';
        // Résolution + scan + fps fusionnés : « 1920×1080p25 ». fps coloré selon l'état.
        const res = `${o.width}×${o.height}${sc}<span style="color:${Number(o.fps) >= 24 ? 'var(--status-running-fg)' : Number(o.fps) > 0 ? '#e8a33d' : 'var(--status-stopped-fg)'}">${fpsTxt}</span>`;
        // Tout ce que le SDP donne en plus : chroma, profondeur, colorimétrie, transfert(HDR), range.
        const chroma = o.chroma ? String(o.chroma).replace(/^(\d)(\d)(\d)$/, '$1:$2:$3') : '';
        const extra = [
          chroma,
          o.bit_depth ? o.bit_depth + 'b' : '',
          o.colorimetry || '',
          (o.tcs && String(o.tcs).toUpperCase() !== 'SDR') ? 'HDR(' + o.tcs + ')' : '',
          (o.range && String(o.range).toUpperCase() === 'FULL') ? 'full' : '',
        ].filter(Boolean).join(' · ');
        const tip = `Format vidéo (SDP) : ${o.width}×${o.height}${sc}${fpsTxt}${extra ? ' · ' + extra.replace(/<[^>]+>/g,'') : ''}`;
        return `<span style="color:var(--text-muted)" title="${esc(tip)}">${res}${extra ? ' · ' + extra : ''}</span>`;
      }
      return `${fmtFps(o.fps)} fps`;
    }
    function stateBadge(active, stalled){
      if (active && stalled)
        // Abonné (IS-05) mais aucun flux ne remonte : création RX ratée (budget lcores) ou pas de trafic.
        return `<span class="badge" style="background:rgba(251,146,60,0.7); color:#3d1500" title="Abonné (IS-05) mais aucun flux ne remonte : création RX échouée (budget lcores du nœud) ou pas de trafic réseau (source/switch).">⚠ sans flux</span>`;
      return active
        ? `<span class="badge" style="background:var(--status-running-bg); color:var(--status-running-fg)">subscribed</span>`
        : `<span class="badge" style="background:var(--border-soft); color:var(--text-muted)">idle</span>`;
    }
    function genTooltip(r, isAudio, genOn){
      const g = r.gen || {};
      const on = (genOn != null) ? genOn : (isAudio ? r.simulated : r.generating);
      const hint = `<div style="margin-top:7px; padding-top:6px; border-top:1px solid var(--border-soft);
          color:${on ? '#e8a33d' : 'var(--text-muted)'}; font-size:0.92em">
          👆 Cliquer pour ${on ? 'désactiver' : 'activer'} le générateur</div>`;
      if (isAudio) {
        const freq  = (g.freq != null) ? `${g.freq} Hz` : '—';
        const level = (g.level_db != null) ? `${g.level_db} dBFS` : '—';
        const active = g.active || [], rupted = g.rupted || [];
        const nChans = Math.max(active.length, rupted.length, 8);
        const chans = [];
        for (let i = 0; i < nChans; i++) {
          const on = active[i], rup = on && rupted[i];
          const cls = rup ? 'gt-ch rup' : (on ? 'gt-ch on' : 'gt-ch');
          const tip = !on ? 'muet' : (rup ? 'actif + ruptures' : 'actif');
          chans.push(`<span class="${cls}" title="canal ${i+1} — ${tip}">${i+1}</span>`);
        }
        const nOn = active.filter(Boolean).length;
        return `<div class="gen-tip"><div class="gen-tip-inner">
          <h5>⚙ Générateur sine local</h5>
          <div class="gt-row"><span>Fréquence</span><span>${esc(freq)}</span></div>
          <div class="gt-row"><span>Niveau</span><span>${esc(level)}</span></div>
          <div class="gt-row"><span>Canaux actifs</span><span>${nOn} / ${nChans}</span></div>
          <div class="gt-chans">${chans.join('')}</div>
          <div style="margin-top:6px; color:var(--text-muted); font-size:0.92em">
            Vert = actif · Rouge = ruptures · Grisé = muet</div>
          ${hint}
        </div></div>`;
      }
      return '';
    }
    // Couples multicast:port lus dans le SDP (une entrée par section m=). Couvre le
    // SDP manuel (transport_params vides) et le multi-flux DUP/2022-7. Ignore le
    // SECONDARY nul (c=0.0.0.0).
    function flowsFromSdp(sdp){
      const out = [];
      if (!sdp) return out;
      let port = null;
      sdp.split(/\r?\n/).forEach(l => {
        const m = l.match(/^m=\w+\s+(\d+)/);
        if (m){ port = m[1]; return; }
        const c = l.match(/^c=IN IP4\s+([0-9.]+)/);
        if (c){
          if (c[1] !== '0.0.0.0') out.push(`${c[1]}:${port ?? '?'}`);
          port = null;
        }
      });
      return out;
    }

    function rowReceiver(r){
      // Préfère le SDP (toutes les jambes / DUP) ; repli sur les transport_params IS-05.
      let flows = flowsFromSdp(r.sdp);
      if (!flows.length && r.multicast_ip) flows = [`${r.multicast_ip}:${r.destination_port ?? '?'}`];
      // Une adresse multicast par ligne (lisibilité des flux DUP/2022-7) : chaque
      // couple reste insécable (pas de coupure mid-octet), tronqué en ellipse si trop long.
      const net = flows.length
        ? `<span class="net-flows">${flows.map(f => `<span class="flow-addr">${esc(f)}</span>`).join('')}</span>`
        : '<span style="color:var(--text-muted)">—</span>';
      const mxl = r.shm_path ? esc(r.shm_path) : '<span style="color:var(--text-muted)">—</span>';
      const isAudio = r.essence === 'audio';
      const isAnc   = r.essence === 'anc';
      const ess = isAudio ? 'audio' : 'video';
      // Nommage : « Vidéo 1 » ; « Audio 1-1 » = vidéo 1, 1ʳᵉ piste audio de cette vidéo ; « ANC 1 ».
      const tag = isAnc
        ? `ANC ${(r.video_idx != null ? r.video_idx : r.idx) + 1}`
        : isAudio
        ? ((r.video_idx != null && r.audio_sub_idx != null)
            ? `Audio ${r.video_idx + 1}-${r.audio_sub_idx + 1}`
            : `Audio ${r.idx + 1}`)
        : `Vidéo ${r.idx + 1}`;
      // Bouton générateur : data-* lus par délégation (pas d'onclick global). Pas de GÉN pour l'ANC.
      // Placeholder vide quand absent : la grille .flow-row place par position → sans cet espace
      // réservé, le badge SDP se décalerait dans une colonne de gauche (désalignement audio/ANC vs vidéo).
      const _genPat = (r.gen && r.gen.pattern) || 'bars';
      const _patOpts = Object.entries(VIDEO_PATTERNS).map(([k,v]) =>
        `<option value="${k}"${k===_genPat?' selected':''}>${esc(v)}</option>`).join('');
      // VIDÉO : le badge GÉN reflète l'état LIVE du moteur (mire réellement émise, mode='simu') —
      // honnête, ≠ config. AUDIO : inchangé (config `simulated`, déjà clair). Le clic toggle le gen.
      const _genOn = isAudio ? !!r.simulated : !!r.generating;
      const genIcon = isAnc ? '<span class="gen-wrap"></span>' : `<span class="gen-wrap">
          <span class="gen-badge ${_genOn ? 'on' : 'off'}" role="button" tabindex="0"
                data-essence="${ess}" data-idx="${r.idx}" data-enable="${_genOn ? '0' : '1'}">GÉN</span>
          ${(!isAudio && _genOn) ? `<span class="gen-pat-wrap"><select class="gen-pat-sel" data-essence="video" data-idx="${r.idx}">${_patOpts}</select></span>` : ''}
          ${genTooltip(r, isAudio, _genOn)}
        </span>`;
      // IDENT : incrustation 3 lignes (nom/source/format) — slots vidéo uniquement.
      // IDENT : badge marche/arrêt + petit rotatif compact pour la taille du texte
      // (glisser ↕ ou molette) — reste dans sa colonne, ne décale pas le badge GÉN.
      const identSz = r.ident_size || Math.max(12, Math.round((r.height || 720) / 28));
      // Placeholder vide pour audio/ANC (pas d'IDENT) → réserve la colonne 3 de la grille,
      // sinon le badge SDP (colonne 4) remonte et n'est plus aligné avec celui de la vidéo.
      const identCtl = (isAudio || isAnc) ? '<span class="ident-wrap"></span>' : `<span class="ident-wrap">
          <span class="ident-badge ${r.ident ? 'on' : 'off'}" role="button" tabindex="0"
                data-idx="${r.idx}" data-enable="${r.ident ? '0' : '1'}"
                title="Incrustation 3 lignes (nom · source/multicast · format), fond noir, haut-droite">IDENT</span>
          ${r.ident ? `<span class="ident-knob" role="slider" tabindex="0"
                aria-valuemin="10" aria-valuemax="120" aria-valuenow="${identSz}"
                data-idx="${r.idx}" data-val="${identSz}"
                title="Taille du texte IDENT — glisser ↕ ou molette">
                <span class="ident-knob-dial" style="transform:rotate(${_identAngle(identSz)}deg)"></span></span>
              <span class="ident-size-val">${identSz}px</span>` : ''}
        </span>`;
      // SDP (vidéo, audio ET ANC) : badge ouvrant une modale d'affichage/édition.
      // Le SDP n'est PAS inliné dans le HTML (multiligne) — on le garde en cache par
      // (essence:idx) (_sdpByIdx, scope mount), la modale le relit. L'audio (2110-30) et
      // l'ANC (2110-40) partagent la même chaîne d'abonnement manuel IS-05 que la vidéo
      // (manual_subscribe côté backend, essence transmise) — sans ça, pas d'abonnement
      // audio/ANC sans contrôleur NMOS externe → shm non alimenté → streamer muet.
      const sdpEss = isAnc ? 'anc' : isAudio ? 'audio' : 'video';
      _sdpByIdx[sdpEss + ':' + r.idx] = r.sdp || '';
      const sdpCtl = `<span class="sdp-wrap">
          <span class="sdp-badge ${r.sdp ? 'on' : 'off'}" role="button" tabindex="0"
                data-essence="${sdpEss}" data-idx="${r.idx}"
                title="Afficher / coller le SDP (abonnement NMOS manuel)">SDP</span>
        </span>`;
      const rateCell = isAnc
        ? (() => {
            // ANC 2110-40 : on ne cherche pas à afficher un timecode. On confirme la réception
            // et on précise le type de métadata SI on le connaît (timecode ATC décodé, sinon SMPTE 291).
            const flowing = Number(r.fps) > 0;
            const col = flowing ? 'var(--status-running-fg)' : (r.sdp ? 'var(--text-muted)' : 'var(--status-stopped-fg)');
            let txt = '—', tip = 'Aucun abonnement ANC sur ce slot';
            if (r.sdp) {
              const type = r.timecode ? 'timecode (SMPTE ST 12M)' : 'SMPTE ST 291 (type non décodé)';
              txt = flowing ? ('reçu' + (r.timecode ? ' · timecode' : '')) : 'abonné';
              tip = flowing ? `ANC 2110-40 reçu · ${type}` : 'ANC 2110-40 abonné · aucun paquet reçu';
            }
            return `<span style="color:${col}" title="${tip}">${esc(txt)}</span>`;
          })()
        : isAudio
        ? (() => {
            // Format AUDIO 2110-30 lu du SDP : « 48kHz / L24 / 8ch ». Affiché dès l'abonnement
            // (r.sdp), même sans flux ; couleur = état de réception.
            const flowing = Number(r.fps) > 0;
            const col = flowing ? 'var(--status-running-fg)' : (r.sdp ? 'var(--text-muted)' : 'var(--status-stopped-fg)');
            let txt = '— / —', tip = 'Aucun abonnement audio sur ce slot';
            if (r.sdp && r.sample_rate) {
              const khz = (r.sample_rate / 1000).toString().replace(/\.0$/, '');
              const ch  = r.channels || 1;
              txt = `${khz}kHz / L${r.bit_depth || 24} / ${ch}ch`;
              tip = flowing ? `${esc(txt)} — flux actif` : `${esc(txt)} — abonné, aucun chunk reçu`;
            }
            return `<span style="color:${col}" title="${tip}">${esc(txt)}</span>`;
          })()
        : (() => {
            // VIDÉO : ni abonné (IS-05) ni générateur actif → pas de signal. On n'affiche PAS un
            // format par défaut trompeur (le moteur ne génère plus rien par défaut, cf. _simu_loop).
            if (!r.active && !r.generating) {
              return `<span style="color:var(--status-stopped-fg)" title="Slot non abonné — aucun flux ni générateur (sortie vide)">non abonnée</span>`;
            }
            return `<span title="format vidéo${r.generating ? ' (mire générée)' : ''}">${fmtVideoFormat(r)}</span>`;
          })();
      const rowCls = isAnc ? 'flow-anc' : isAudio ? 'flow-audio' : 'flow-video';
      return `<div class="flow-row ${rowCls}">
        <span class="flow-tag ${isAnc ? 'd' : isAudio ? 'a' : 'v'}">${tag}</span>
        ${genIcon}
        ${identCtl}
        ${sdpCtl}
        <span>${stateBadge(r.active, r.rx_stalled)}</span>
        ${rateCell}
        <span class="net-addr" title="entrée 2110"><span class="net-arrow">↘</span>${portSelector('rx', r.idx, r.port)}${net}</span>
        <span class="mxl-path" title="sortie MXL (shared memory)">→ ${mxl}</span>
      </div>`;
    }
    function groupEnsembles(receivers){
      const videos = receivers.filter(x => x.essence === 'video');
      const audios = receivers.filter(x => x.essence === 'audio');
      const ancs   = receivers.filter(x => x.essence === 'anc');
      const groups = videos.map(v => ({video: v, audios: [], ancs: []}));
      const byVid = {};
      groups.forEach(g => { byVid[g.video.idx] = g; });
      // « Option A » : chaque audio/ANC suit SA vidéo (video_idx) ; video_idx null = flux INDÉPENDANT
      // (regroupé à part). Un container non migré sans flux retombe sur l'ancien 1:1 (video_idx = idx).
      const indep = {video: null, audios: [], ancs: [], independent: true};
      audios.forEach(a => {
        const g = (a.video_idx != null) ? byVid[a.video_idx] : null;
        (g || indep).audios.push(a);
      });
      ancs.forEach(d => {
        const g = (d.video_idx != null) ? byVid[d.video_idx] : null;
        (g || indep).ancs.push(d);
      });
      if (indep.audios.length || indep.ancs.length) groups.push(indep);
      return groups;
    }

    function renderTXSection(senders){
      // Groupe les senders TX par slot (tx_idx), puis affiche vidéo + audios + ANC.
      if (!senders || !senders.length) return '';
      const bySlot = {};
      senders.forEach(s => {
        const ti = s.tx_idx != null ? s.tx_idx : 0;
        (bySlot[ti] = bySlot[ti] || []).push(s);
      });
      const slotIds = Object.keys(bySlot).sort((a,b) => Number(a) - Number(b));
      if (!slotIds.length) return '';
      const rows = slotIds.map(ti => {
        const sl = bySlot[ti];
        const vid  = sl.find(s => s.essence === 'video');
        const auds = sl.filter(s => s.essence === 'audio').sort((a,b) => (a.audio_idx||0) - (b.audio_idx||0));
        const anc  = sl.find(s => s.essence === 'anc');
        const fmtDest = (s) => {
          if (!s) return '<span style="color:var(--text-muted)">non configuré</span>';
          const fp = fmtFps(s.fps);
          const dest = s.multicast_ip ? `${s.multicast_ip}:${s.destination_port ?? '?'}` : '—';
          return `<span style="color:var(--text-muted)">${esc(dest)}</span> ${fp} fps`;
        };
        const lines = [];
        if (vid) lines.push(`<div class="flow-row" style="gap:8px;align-items:center">
          <span class="badge" style="background:var(--bg-input,var(--bg));border:1px solid var(--border)">2110-20</span>
          ${fmtDest(vid)}</div>`);
        auds.forEach((a,i) => lines.push(`<div class="flow-row" style="gap:8px;align-items:center">
          <span class="badge" style="background:var(--bg-input,var(--bg));border:1px solid var(--border)">2110-30 #${i+1}</span>
          ${fmtDest(a)}</div>`));
        if (anc) lines.push(`<div class="flow-row" style="gap:8px;align-items:center">
          <span class="badge" style="background:var(--bg-input,var(--bg));border:1px solid var(--border)">2110-40</span>
          ${fmtDest(anc)}</div>`);
        const _txPort = (vid && vid.port) || (auds[0] && auds[0].port) || (anc && anc.port) || null;
        return `<div class="ens">
          <div class="ens-title">Slot TX #${Number(ti) + 1}${portSelector('tx', Number(ti), _txPort)}</div>
          ${lines.join('')}
        </div>`;
      }).join('');
      return `<div class="meta" style="margin-top:10px;padding-top:8px;border-top:1px solid var(--border-soft);font-weight:600">Sorties (TX)</div>`
           + rows;
    }

    function _nicBar(gbps, estGbps, cap, label) {
      const val = gbps != null ? gbps : estGbps;
      const isEst = gbps == null && estGbps != null;
      if (val == null) return '';
      const pct = Math.min(100, Math.round(val / cap * 100));
      const col = pct > 80 ? 'var(--status-stopped-fg,#f87171)' : pct > 60 ? '#e8a33d' : 'var(--status-running-fg,#22c55e)';
      return `<div class="nic-bar-wrap">
        <span class="nic-bar-lbl">${label}</span>
        <span class="nic-bar-val${isEst ? ' nic-bar-est' : ''}" style="color:${col}">${isEst ? '~' : ''}${val.toFixed(1)} / ${cap} Gbps (${pct}%)${isEst ? ' (estimation)' : ''}</span>
        <div class="nic-bar-track"><div class="nic-bar-fill" style="width:${pct}%;background:${col}"></div></div>
      </div>`;
    }

    function _nicHeader(model, aggregateGbps) {
      if (!model) return '';
      // Agrégat = somme des vitesses de lien réelles des ports de la carte (ex. 4×10 = 40G).
      const agg = (aggregateGbps > 0)
        ? ` · <span class="nic-shared">agrégé ${aggregateGbps}G</span>` : '';
      return `<div class="nic-model-lbl">${esc(model)}${agg}</div>`;
    }

    // Couleur stable d'un réseau média (pastille red/blue de la bande de ports + badges de slot).
    function _netColor(network) {
      const s = String(network || '·');
      let h = 0;
      for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) & 0xffffff;
      return `hsl(${h % 360},62%,55%)`;
    }
    // Chip de port (côté RX) : nom + réseau + barre de charge RX (mesurée/estimée) + files/flux.
    function _portChip(p) {
      const col   = _netColor(p.network);
      const cap   = p.port_capacity_gbps || 100;
      const val   = p.rx_gbps != null ? p.rx_gbps : p.rx_estimated_gbps;
      const isEst = p.rx_gbps == null && p.rx_estimated_gbps != null;
      const pct   = val != null ? Math.min(100, Math.round(val / cap * 100)) : 0;
      const bcol  = pct > 80 ? 'var(--status-stopped-fg,#f87171)' : pct > 60 ? '#e8a33d' : 'var(--status-running-fg,#22c55e)';
      const down  = p.link_up === false;
      return `<div class="io2110-portchip${down ? ' down' : ''}" style="border-left-color:${col}">
        <div class="pc-top"><span class="pc-name" style="color:${col}">${esc(p.iface)}</span>
          ${p.primary ? '<span class="pc-prim">PRIM</span>' : ''}
          <span class="pc-net">${esc(p.network || '')}</span>
          ${down ? '<span class="pc-down" title="Lien physique down">⚠ lien</span>' : ''}</div>
        <div class="pc-load"><span class="pc-loadval${isEst ? ' est' : ''}">${
          val != null ? (isEst ? '~' : '') + val.toFixed(1) + ' / ' + cap + ' G' : '—'}</span>
          <div class="pc-track"><div class="pc-fill" style="width:${pct}%;background:${bcol}"></div></div></div>
        <div class="pc-meta">${p.rx_flow_count != null ? p.rx_flow_count + ' flux' : ''}${
          p.rx_queues != null ? ' · ' + p.rx_queues + ' files' : ''}</div>
      </div>`;
    }
    // Badge état PTP d'un port (SLAVE/MASTER/PASSIVE/LISTENING/FAULTY…) — couleur par état.
    function _ptpBadge(state){
      if (!state) return '';
      const M = {SLAVE:['SLAVE','var(--status-running-fg,#22c55e)'], MASTER:['MASTER','#60a5fa'],
        GRAND_MASTER:['GRAND MASTER','#60a5fa'], PRE_MASTER:['PRE-MASTER','#60a5fa'],
        PASSIVE:['PASSIVE','var(--text-muted)'], LISTENING:['LISTENING','#e8a33d'],
        UNCALIBRATED:['UNCAL','#e8a33d'], FAULTY:['FAULTY','var(--status-stopped-fg,#f87171)'],
        DISABLED:['DISABLED','var(--text-muted)'], INITIALIZING:['INIT','var(--text-muted)']};
      const [lbl,c] = M[state] || [state, 'var(--text-muted)'];
      return `<span class="pc-ptp" style="color:${c};border-color:${c}" title="État PTP du port : ${esc(state)}">⏱ ${lbl}</span>`;
    }
    // Bande de ports + bouton « Par NIC ». Mono-port (<2 ports) → '' (UI agrégée inchangée). Le détail
    // déplié montre PAR PORT, en barres PLEINE LARGEUR : débit RX + Queues XDP multi-segments
    // (live/planifié/réservé/libre + repère plafond, comme la globale) sur le budget du PORT + état PTP.
    function _nicPortStrip(ports) {
      if (!ports || ports.length < 2) return '';
      const strip = ports.map(_portChip).join('');
      const detail = _nicOpen ? `<div class="io2110-portdetail">${
        ports.map(p => {
          const col = _netColor(p.network);
          const cap = p.port_capacity_gbps || 100;
          const rxBar = _nicBar(p.rx_gbps, p.rx_estimated_gbps, cap, 'RX');
          const xdpBar = (p.xdp_hw && p.xdp_reserved != null)
            ? _xdpBar(p.xdp_active || 0, p.xdp_planned, p.xdp_reserved, p.xdp_hw)
            : '';
          const flows = p.rx_flow_count != null ? `<span class="pc-meta">Flux RX : ${p.rx_flow_count}</span>` : '';
          return `<div class="io2110-portcard" style="border-left-color:${col}">
            <h5><span style="color:${col}">${esc(p.iface)}</span>${p.primary ? '<span class="pc-prim">PRIM</span>' : ''}
              ${p.network ? `<span class="pc-net">${esc(p.network)}</span>` : ''}${_ptpBadge(p.ptp_state)}
              <span class="pc-meta" style="margin-left:auto">${p.link_up === false ? '⚠ lien down' : (p.link_up ? 'lien up' : '')}</span></h5>
            ${rxBar}${xdpBar}${flows}</div>`;
        }).join('')}</div>` : '';
      return `<div class="io2110-nicbar"><div class="io2110-portstrip">${strip}</div>
        <button class="io2110-nictoggle${_nicOpen ? ' on' : ''}"
          title="Afficher / masquer le détail par port physique">${_nicOpen ? '▾' : '▸'} Par NIC</button>
        </div>${detail}`;
    }

    let _cachedEnsembles = [];
    let _nicOpen          = false;  // multi-NIC : détail « Par NIC » déplié (état local de la carte)
    let _lastNicPorts     = [];     // dernier nic_ports reçu (pour reconstruire la bande au toggle)
    let _cachedMeta      = '';
    let _cachedTxHtml    = '';
    let _cachedVideoCount = 0;  // capacité totale déployée
    let _cachedActiveRx   = 0;  // slots simultanés autorisés (active_rx_count)
    let _nodePorts        = [];  // ports média du nœud (multi-NIC) ; [] = mono-port → pas de sélecteur

    // Sélecteur de PORT (NIC) d'un slot — multi-NIC seulement (≥2 ports). « Auto » = répartition
    // automatique (badge du port effectif courant) ; un port précis = épinglage. POST /api/mtl/<vmid>/pin.
    function portSelector(role, idx, port){
      if (!_nodePorts || _nodePorts.length < 2) return '';
      const cur = (port && port.pinned) ? port.iface : '';   // '' = Auto
      const eff = (port && port.iface) || '';
      const opts = [`<option value=""${cur===''?' selected':''}>Auto${eff?` (${esc(eff)})`:''}</option>`]
        .concat(_nodePorts.map(p =>
          `<option value="${esc(p.ifname)}"${cur===p.ifname?' selected':''}>${esc(p.ifname)}${p.network?` · ${esc(p.network)}`:''}</option>`)).join('');
      return `<span class="port-wrap"><select class="port-sel" data-role="${role}" data-idx="${idx}"
                title="Port (NIC) de ce slot — Auto = répartition automatique entre les ports du réseau">${opts}</select></span>`;
    }

    // Ligne de flux audio/ANC avec bouton de retrait granulaire (« Option A »).
    function _rmWrap(html, fid){
      return `<div class="io2110-flowrow">${html}`
           + (fid ? `<button class="io2110-flowrm" data-fid="${esc(fid)}" title="Retirer ce flux">✕</button>` : '')
           + `</div>`;
    }

    // Barre « Queues XDP » multi-segments (B2+). active=sessions LIVE, planned=flux provisionnés (≥active),
    // reserved=plafond mtl_init, hw=files NIC. PLEIN=live · HACHURÉ=planifié (réagit aux ajouts) · PÂLE
    // ancré au marqueur (mangé de droite→gauche)=réservé libre · TRAIT=plafond à chaud. Tout en % des HW.
    function _xdpBar(active, planned, reserved, hw, scope){
      active   = Math.max(0, active || 0);
      planned  = Math.max(active, planned || active);
      reserved = Math.max(0, reserved || 0);
      const pend  = Math.max(0, planned - active);          // planifié pas encore live
      const hot   = Math.max(0, reserved - active);         // ajoutables à chaud (planifié inclus)
      const freeQ = Math.max(0, reserved - planned);        // réservé NON réclamé (zone pâle)
      const overQ = Math.max(0, planned - reserved);        // planifié AU-DELÀ du plafond (ambre)
      const pct   = v => Math.min(100, Math.max(0, v / hw * 100));
      const col   = (active >= reserved) ? 'var(--status-stopped-fg,#f87171)'
                  : (hot <= 1 ? '#e8a33d' : 'var(--status-running-fg,#22c55e)');
      const aPct = pct(active), rPct = pct(reserved), planPct = pct(planned);
      const hotL = aPct, hotW = Math.max(0, Math.min(planPct, rPct) - aPct);   // planifié À CHAUD (≤ réservé)
      const ovrL = Math.max(aPct, rPct), ovrW = Math.max(0, planPct - ovrL);   // planifié au-delà (ambre)
      const freeL = pct(Math.min(Math.max(active, planned), reserved));
      const freeW = Math.max(0, rPct - freeL);
      const txt = (overQ
        ? `${active} live · +${pend} planifié dont ${overQ} > réservé (${reserved}) → redéploiement`
        : `${active} live · +${pend} planifié · ${freeQ} libre / ${hw} files`) + (scope || '');
      return `<div class="nic-bar-wrap">
        <span class="nic-bar-lbl">Queues XDP</span>
        <span class="nic-bar-val" style="color:${overQ ? '#e8a33d' : col}">${txt}</span>
        <div class="nic-xdp-track">
          <div class="nic-xdp-free"    style="left:${freeL}%;width:${freeW}%"></div>
          <div class="nic-xdp-pending" style="left:${hotL}%;width:${hotW}%;background-color:${col}"></div>
          <div class="nic-xdp-over"    style="left:${ovrL}%;width:${ovrW}%"></div>
          <div class="nic-xdp-active"  style="width:${aPct}%;background:${col}"></div>
          <div class="nic-xdp-mark"    style="left:${rPct}%" title="Plafond à chaud : ${reserved} files réservées à mtl_init — au-delà, redéploiement requis"></div>
        </div>
      </div>`;
    }

    function _renderBody() {
      const ens = _cachedEnsembles;
      const inner = ens.length === 0
        ? `<div class="meta" style="padding:8px 0">Aucun receiver actif</div>`
        : ens.map((g, i) => {
            const rows = [];
            if (g.video) rows.push(rowReceiver(g.video));
            g.audios.forEach(a => rows.push(_rmWrap(rowReceiver(a), a.flow_id)));
            (g.ancs || []).forEach(d => rows.push(_rmWrap(rowReceiver(d), d.flow_id)));
            if (g.independent) {
              // Flux indépendants (audio/ANC non rattachés à une vidéo).
              return `<div class="ens ens-indep">
                <div class="ens-title">Flux indépendants</div>
                ${rows.join('')}
              </div>`;
            }
            const titleParts = ['1 vidéo'];
            if (g.audios.length) titleParts.push(`${g.audios.length} audio`);
            if ((g.ancs || []).length) titleParts.push(`${g.ancs.length} ANC`);
            const vfid = g.video ? (g.video.flow_id || '') : '';
            // Ajout/retrait granulaire par vidéo : + Audio / + ANC rattachés ; ✕ retire la source entière.
            const ctrls = vfid ? `<div class="io2110-flowctrls">
              <button class="io2110-addflow" data-ess="audio" data-att="${esc(vfid)}">+ Audio</button>
              <button class="io2110-addflow" data-ess="anc" data-att="${esc(vfid)}">+ ANC</button>
              <button class="io2110-flowrm io2110-rmgrp" data-fid="${esc(vfid)}" title="Retirer cette source">✕ source</button>
            </div>` : '';
            return `<div class="ens">
              <div class="ens-title">Ensemble #${i + 1} — ${titleParts.join(' + ')}</div>
              ${rows.join('')}
              ${ctrls}
            </div>`;
          }).join('');
      const ensVideoCount = ens.filter(g => g.video).length;
      // Headroom = pool pré-provisionné (video_count). Au-delà → augmenter le pool (redéploiement).
      const moreBtn = ensVideoCount < _cachedVideoCount
        ? `<button class="io2110-more-btn">+ Ajouter une source</button>`
        : '';
      const delBtn = ensVideoCount > 0
        ? `<button class="io2110-more-btn io2110-del-rx">− Retirer la dernière source</button>`
        : '';
      // Remède famine : ≥1 source abonnée mais sans flux (rx_stalled) → bouton de réalignement des
      // files (redéploiement du moteur). Disruptif → passe par mtlMutate (confirmation serveur).
      const _anyStalled = recvs.some(r => r.rx_stalled);
      const realignBtn = _anyStalled
        ? `<button class="io2110-more-btn io2110-realign" title="Une ou plusieurs sources sont abonnées mais ne reçoivent aucun flux. Redéployer le moteur réaligne les files (coupure brève de TOUS les flux).">⟳ Redéployer pour réaligner les files</button>`
        : '';
      // Création d'un flux INDÉPENDANT (audio/ANC sans vidéo d'attache).
      const indepAdd = `<div class="io2110-flowctrls io2110-indepadd">
        <span class="meta">Indépendant :</span>
        <button class="io2110-addflow" data-ess="audio" data-att="">+ Audio</button>
        <button class="io2110-addflow" data-ess="anc" data-att="">+ ANC</button>
      </div>`;
      body.innerHTML = _cachedMeta + _nicPortStrip(_lastNicPorts) + inner + indepAdd + moreBtn + delBtn + realignBtn + _cachedTxHtml;
      const realignEl = body.querySelector('.io2110-realign');
      if (realignEl) {
        realignEl.onclick = async () => {
          realignEl.disabled = true;
          try {
            const r = await mtlMutate(`/api/mtl/${vmid}/realign`, {});
            if (r && !r.ok) { const j = await r.json().catch(()=>({})); toast(j.error || 'Erreur réalignement', 'error'); }
          } catch(e) { toast('Erreur réseau', 'error'); }
          await refresh();
        };
      }
      // Ajout granulaire de flux (rattaché si data-att, sinon indépendant).
      body.querySelectorAll('.io2110-addflow').forEach(b => b.onclick = async () => {
        b.disabled = true;
        try {
          const r = await mtlMutate(`/api/mtl/${vmid}/flows/add`,
            {role: 'rx', essence: b.dataset.ess, attached_to: b.dataset.att || null});
          if (r && !r.ok) { const j = await r.json().catch(()=>({})); toast(j.error || 'Erreur ajout flux', 'error'); }
        } catch(e) { toast('Erreur réseau', 'error'); }
        await refresh();
      });
      // Retrait granulaire d'un flux (par id ; une vidéo retire aussi ses audios/ANC attachés).
      body.querySelectorAll('.io2110-flowrm').forEach(b => b.onclick = async () => {
        b.disabled = true;
        try {
          const r = await fetch(`/api/mtl/${vmid}/flows/remove`, {
            method: 'POST', headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({id: b.dataset.fid}),
          });
          const j = await r.json().catch(()=>({}));
          if (!r.ok) toast(j.error || 'Erreur retrait flux', 'error');
          else if (j.note) toast(j.note, 'info');
        } catch(e) { toast('Erreur réseau', 'error'); }
        await refresh();
      });
      const moreEl = body.querySelector('.io2110-more-btn:not(.io2110-del-rx)');
      if (moreEl) {
        moreEl.onclick = async () => {
          moreEl.disabled = true;
          try {
            const r = await mtlMutate(`/api/mtl/${vmid}/activate`, {kind: 'rx'});
            if (r && !r.ok) { const j = await r.json().catch(()=>({})); toast(j.error || 'Erreur activation RX', 'error'); }
          } catch(e) { toast('Erreur réseau', 'error'); }
          await refresh();
        };
      }
      const delEl = body.querySelector('.io2110-del-rx');
      if (delEl) {
        delEl.onclick = async () => {
          delEl.disabled = true;
          try {
            const r = await fetch(`/api/mtl/${vmid}/deactivate`, {
              method: 'POST', headers: {'Content-Type': 'application/json'},
              body: JSON.stringify({kind: 'rx'}),
            });
            if (!r.ok) { const j = await r.json().catch(()=>({})); toast(j.error || 'Erreur retrait RX', 'error'); }
          } catch(e) { toast('Erreur réseau', 'error'); }
          await refresh();
        };
      }
    }

    async function refresh(){
      let c, cs;
      try { c = await (await fetch(`/api/nmos/receivers/${vmid}/detail`)).json(); }
      catch(e){ body.innerHTML = '<div class="meta">Détail NMOS indisponible.</div>'; return; }
      try { const cd = await (await fetch(`/api/nmos/senders/${vmid}/detail`)).json();
            cs = (cd && cd.length) ? cd[0] : null; }
      catch(e){ cs = null; }
      const recvs = (c && c.receivers) || [];
      _nodePorts = (c && c.ports) || [];
      const activeCount = recvs.filter(x => x.active).length;
      _cachedVideoCount = (c && c.video_count) || recvs.length;
      _cachedActiveRx   = (c && c.active_rx_count) || _cachedVideoCount;
      _cachedEnsembles = groupEnsembles(recvs);
      const _nicPortCap = (c && c.nic_port_capacity_gbps) || 100;
      const _nicAgg     = (c && c.nic_aggregate_gbps)     || 100;
      const _nicH   = _nicHeader((c && c.nic_model) || '', _nicAgg);
      // Barre RX AGRÉGÉE : nic_rx_gbps = somme de tous les ports → dénominateur = agrégat de la
      // carte (somme des vitesses, ex. 40G), PAS la capacité d'un seul port (10G). Le détail par
      // port (chacun vs sa propre capacité) est dans la bande « Par NIC ».
      const _nicRxBar = _nicBar(
        (c && c.nic_rx_gbps != null) ? c.nic_rx_gbps : null,
        (c && c.nic_rx_estimated_gbps != null) ? c.nic_rx_estimated_gbps : null,
        _nicAgg, 'RX');
      const _xdpAlloc    = c && c.xdp_allocated;
      const _xdpAct      = (c && c.xdp_active) ?? 0;
      const _xdpReserved = c && c.xdp_reserved;
      const _xdpPlanned  = (c && c.xdp_planned) ?? _xdpAct;
      // active/reserved/planned ET le budget HW sont désormais des SOMMES 4 ports fournies par le
      // backend (xdp_hw_max_combined agrégé) — plus de rustine × _nPorts ici. Barre « toutes NIC ».
      const _nPorts      = ((c && c.nic_ports) || []).length;
      const _xdpHwMax    = c && c.xdp_hw_max_combined;
      const _xdpScope    = _nPorts > 1 ? ' · toutes NIC' : '';
      const _hasB2 = (_xdpReserved != null) && (_xdpHwMax != null) && _xdpHwMax > 0;
      let _nicXdpBar = '';
      if (_hasB2) {
        _nicXdpBar = _xdpBar(_xdpAct, _xdpPlanned, _xdpReserved, _xdpHwMax, _xdpScope);
      } else if (_xdpAlloc != null) {
        // Repli image pré-A2 (pas de `reserved`) : ancien rendu allocated/HW.
        const _xdpDen     = _xdpHwMax || _xdpAlloc;
        const _xdpUsedPct = _xdpDen ? Math.min(100, Math.round(_xdpAlloc / _xdpDen * 100)) : 0;
        const _xdpOver    = (_xdpHwMax != null) && (_xdpAlloc > _xdpHwMax);
        const _xdpColU = (_xdpOver || _xdpUsedPct >= 100) ? 'var(--status-stopped-fg,#f87171)' : _xdpUsedPct > 85 ? '#e8a33d' : 'var(--status-running-fg,#22c55e)';
        const _xdpTxt  = (_xdpHwMax != null)
          ? (_xdpOver ? `${_xdpAlloc} / ${_xdpHwMax} files — SUR-CAPACITÉ (${_xdpAct} actives)`
                      : `${_xdpAlloc} / ${_xdpHwMax} files (${_xdpUsedPct}%)`)
          : `${_xdpAct} / ${_xdpAlloc} sessions`;
        _nicXdpBar = `<div class="nic-bar-wrap">
          <span class="nic-bar-lbl">Queues XDP</span>
          <span class="nic-bar-val" style="color:${_xdpColU}">${_xdpTxt}</span>
          <div class="nic-bar-track"><div class="nic-bar-fill" style="width:${_xdpUsedPct}%;background:${_xdpColU}"></div></div>
        </div>`;
      }
      _lastNicPorts = (c && c.nic_ports) || [];   // mémorisé pour reconstruire la bande au toggle (sans refetch)
      _cachedMeta = `<div class="meta rx-meta">IP : ${esc((c && c.ip) || '—')} — ${recvs.length} / ${_cachedVideoCount} sources · ${activeCount} abonné${activeCount > 1 ? 's' : ''}</div>${_nicH}${_nicRxBar}${_nicXdpBar}`;
      _cachedTxHtml    = renderTXSection(cs && cs.senders);
      _renderBody();
    }

    // Délégation : clic sur un badge IDENT → bascule l'incrustation de ce slot vidéo.
    async function onClickIdent(e){
      const badge = e.target.closest('.ident-badge');
      if (!badge || !body.contains(badge)) return;
      if (badge.classList.contains('busy')) return;
      const idx = parseInt(badge.dataset.idx, 10);
      const enable = badge.dataset.enable === '1';
      badge.classList.add('busy');
      try {
        const resp = await fetch(`/api/containers/${vmid}/control/ident`, {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({idx, enabled: enable}),
        });
        if (!resp.ok) { const j = await resp.json().catch(()=>({})); throw new Error(j.error || ('HTTP ' + resp.status)); }
        setTimeout(refresh, 600);
      } catch(err) {
        toast('Échec du basculement IDENT : ' + err.message, 'error');
      } finally {
        badge.classList.remove('busy');
      }
    }
    // Taille IDENT via un petit rotatif (glisser ↕ ou molette) : aperçu live, POST throttlé.
    // La taille s'applique à chaud côté container (sans respawn).
    const _identThrottle = {};
    async function _postIdentSize(idx, size){
      try {
        await fetch(`/api/containers/${vmid}/control/ident`, {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({idx, size}),
        });
      } catch(err) { toast('Échec taille IDENT : ' + err.message, 'error'); }
    }
    function _setKnob(knob, val){
      val = Math.max(10, Math.min(120, val));
      knob.dataset.val = val; knob.setAttribute('aria-valuenow', val);
      const dial = knob.querySelector('.ident-knob-dial');
      if (dial) dial.style.transform = `rotate(${_identAngle(val)}deg)`;
      const lbl = knob.parentElement.querySelector('.ident-size-val');
      if (lbl) lbl.textContent = val + 'px';
      return val;
    }
    function _schedIdent(idx, val){
      clearTimeout(_identThrottle[idx]);
      _identThrottle[idx] = setTimeout(() => _postIdentSize(idx, val), 120);
    }
    let knobDrag = null;
    function onKnobDown(e){
      const k = e.target.closest('.ident-knob');
      if (!k || !body.contains(k)) return;
      knobDrag = {k, idx: parseInt(k.dataset.idx, 10), startY: e.clientY, startVal: parseInt(k.dataset.val, 10) || 12};
      try { k.setPointerCapture(e.pointerId); } catch(_){}
      e.preventDefault();
    }
    function onKnobMove(e){
      if (!knobDrag) return;
      // glisser vers le haut = augmenter ; pas de 2 px, ~0.7 px de valeur par px souris.
      const v = _setKnob(knobDrag.k, Math.round((knobDrag.startVal + (knobDrag.startY - e.clientY) * 0.7) / 2) * 2);
      _schedIdent(knobDrag.idx, v);
    }
    function onKnobUp(e){
      if (!knobDrag) return;
      try { knobDrag.k.releasePointerCapture(e.pointerId); } catch(_){}
      knobDrag = null;
    }
    function onKnobWheel(e){
      const k = e.target.closest('.ident-knob');
      if (!k || !body.contains(k)) return;
      e.preventDefault();
      const idx = parseInt(k.dataset.idx, 10);
      const v = _setKnob(k, (parseInt(k.dataset.val, 10) || 12) + (e.deltaY < 0 ? 2 : -2));
      _schedIdent(idx, v);
    }

    // Délégation : clic sur un badge GÉN → bascule le générateur de ce slot.
    async function onClick(e){
      const badge = e.target.closest('.gen-badge');
      if (!badge || !body.contains(badge)) return;
      if (badge.classList.contains('busy')) return;
      const essence = badge.dataset.essence;
      const idx = parseInt(badge.dataset.idx, 10);
      const enable = badge.dataset.enable === '1';
      badge.classList.add('busy');
      try {
        const resp = await fetch(`/api/containers/${vmid}/control/gen`, {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({essence, idx, enabled: enable}),
        });
        if (!resp.ok) { const j = await resp.json().catch(()=>({})); throw new Error(j.error || ('HTTP ' + resp.status)); }
        badge.classList.toggle('on', enable);
        badge.classList.toggle('off', !enable);
        badge.dataset.enable = enable ? '0' : '1';
        setTimeout(refresh, 2500);   // le redéploiement côté container est asynchrone
      } catch(err) {
        toast('Échec du basculement du générateur : ' + err.message, 'error');
      } finally {
        badge.classList.remove('busy');
      }
    }
    // Délégation : changement de pattern dans le <select> du tooltip GÉN.
    async function onPatternChange(e){
      const sel = e.target.closest('.gen-pat-sel');
      if (!sel || !body.contains(sel)) return;
      const essence = sel.dataset.essence;
      const idx = parseInt(sel.dataset.idx, 10);
      const pattern = sel.value;
      try {
        const resp = await fetch(`/api/containers/${vmid}/control/gen`, {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({essence, idx, enabled: true, pattern}),
        });
        if (!resp.ok) { const j = await resp.json().catch(()=>({})); throw new Error(j.error || ('HTTP ' + resp.status)); }
      } catch(err) {
        toast('Échec du changement de mire : ' + err.message, 'error');
      }
    }
    // Délégation : toggle « Par NIC » — déplie/replie le détail des ports (sans refetch).
    function onNicToggle(e){
      const btn = e.target.closest('.io2110-nictoggle');
      if (!btn || !body.contains(btn)) return;
      _nicOpen = !_nicOpen;
      _renderBody();
    }
    // Délégation : changement de PORT (NIC) d'un slot — épinglage / retour à l'auto (multi-NIC).
    async function onPortChange(e){
      const sel = e.target.closest('.port-sel');
      if (!sel || !body.contains(sel)) return;
      const role = sel.dataset.role;
      const idx = parseInt(sel.dataset.idx, 10);
      const iface = sel.value;   // '' = Auto (répartition)
      sel.disabled = true;
      try {
        const resp = await fetch(`/api/mtl/${vmid}/pin`, {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({role, idx, iface}),
        });
        if (!resp.ok) { const j = await resp.json().catch(()=>({})); throw new Error(j.error || ('HTTP ' + resp.status)); }
        setTimeout(refresh, 700);   // laisse reconcile déplacer la session
      } catch(err) {
        toast('Échec de l\'épinglage de port : ' + err.message, 'error');
      } finally {
        sel.disabled = false;
      }
    }

    // ── SDP : ouverture de la modale d'affichage / abonnement manuel ──────────
    function _closeSdpModal(){ const m = document.getElementById('rx-sdp-modal'); if (m) m.remove(); }
    async function _sdpApply(essence, idx, enable){
        const ta  = document.getElementById('rx-sdp-ta');
        const st  = document.getElementById('rx-sdp-status');
        const sdp = ta ? ta.value.trim() : '';
        if (enable && !sdp){ if (st){ st.textContent = 'SDP vide'; st.style.color = 'var(--status-stopped-fg)'; } return; }
        if (st){ st.textContent = enable ? 'abonnement…' : 'désabonnement…'; st.style.color = 'var(--text-muted)'; }
        try {
            const resp = await fetch(`/api/nmos/receivers/${vmid}/${idx}/sdp`, {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ sdp, enabled: enable, essence }),
            });
            const j = await resp.json().catch(()=>({}));
            if (!resp.ok) throw new Error(j.error || ('HTTP ' + resp.status));
            _closeSdpModal();
            toast(enable ? 'Abonnement SDP appliqué' : 'Désabonné', 'info');
            setTimeout(refresh, 1500);   // l'agent redéploie ffmpeg de façon asynchrone
        } catch(err){
            if (st){ st.textContent = '✕ ' + err.message; st.style.color = 'var(--status-stopped-fg)'; }
        }
    }
    function _openSdpModal(essence, idx){
        _closeSdpModal();
        const cur = _sdpByIdx[essence + ':' + idx] || '';
        const essLabel = essence === 'anc' ? 'ANC' : essence === 'audio' ? 'Audio' : 'Vidéo';
        const modal = document.createElement('div');
        modal.id = 'rx-sdp-modal';
        modal.style.cssText = 'position:fixed; inset:0; background:rgba(0,0,0,.55); z-index:9999;'
            + 'display:flex; align-items:center; justify-content:center';
        modal.innerHTML = `
            <div style="background:var(--bg-card); border:1px solid var(--border); border-radius:8px;
                        padding:16px; width:640px; max-width:95vw">
                <div style="font-weight:600; margin-bottom:8px">SDP — ${essLabel} ${idx + 1} (#${vmid})</div>
                <div style="color:var(--text-muted); font-size:0.82em; margin-bottom:8px">
                    SDP actif reçu via NMOS. Coller/éditer puis « Appliquer » pour un abonnement
                    manuel immédiat (sans contrôleur externe). « Se désabonner » coupe le flux.
                </div>
                <textarea id="rx-sdp-ta" rows="12" spellcheck="false"
                    style="width:100%; box-sizing:border-box; font-family:var(--font-mono);
                           font-size:0.82em; background:var(--bg-input, var(--bg));
                           color:var(--text-primary); border:1px solid var(--border);
                           border-radius:4px; padding:8px; resize:vertical">${esc(cur)}</textarea>
                <div style="display:flex; gap:8px; align-items:center; margin-top:12px; justify-content:flex-end">
                    <span id="rx-sdp-status" style="margin-right:auto; font-size:0.85em"></span>
                    <button class="btn" id="rx-sdp-close">Fermer</button>
                    <button class="btn btn-red" id="rx-sdp-unsub">Se désabonner</button>
                    <button class="btn btn-green" id="rx-sdp-apply">Appliquer</button>
                </div>
            </div>`;
        document.body.appendChild(modal);
        modal.addEventListener('click', e => { if (e.target === modal) _closeSdpModal(); });
        document.getElementById('rx-sdp-close').addEventListener('click', _closeSdpModal);
        document.getElementById('rx-sdp-apply').addEventListener('click', () => _sdpApply(essence, idx, true));
        document.getElementById('rx-sdp-unsub').addEventListener('click', () => _sdpApply(essence, idx, false));
        document.getElementById('rx-sdp-ta').focus();
    }
    function onClickSdp(e){
        const badge = e.target.closest('.sdp-badge');
        if (!badge || !body.contains(badge)) return;
        _openSdpModal(badge.dataset.essence || 'video', parseInt(badge.dataset.idx, 10));
    }

    body.addEventListener('click', onClick);
    body.addEventListener('click', onClickIdent);
    body.addEventListener('click', onClickSdp);
    body.addEventListener('click', onNicToggle);
    body.addEventListener('change', onPatternChange);
    body.addEventListener('change', onPortChange);
    body.addEventListener('pointerdown', onKnobDown);
    body.addEventListener('pointermove', onKnobMove);
    body.addEventListener('pointerup', onKnobUp);
    body.addEventListener('wheel', onKnobWheel, {passive: false});

    refresh();
    if (this._timers[vmid]) clearInterval(this._timers[vmid]);
    this._timers[vmid] = setInterval(refresh, 5000);
  },

  unmount(vmid){
    if (vmid != null) { clearInterval(this._timers[vmid]); delete this._timers[vmid]; }
    else { Object.values(this._timers).forEach(clearInterval); this._timers = {}; }
  }
};

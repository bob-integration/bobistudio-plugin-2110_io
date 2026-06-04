// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.

// Carte de contrôle receiver_2110_mtl (identique à receiver_2110). Enregistrée sur window.MXLPlugins["receiver_2110_mtl"].
// mount(el, vmid, ctx) est appelée par le shell Sources après injection de control.html.
// Données par-vmid : GET /api/nmos/receivers/<vmid>/detail. Générateur par slot :
// POST /api/nmos/receivers/<vmid>/gen/<essence>/<idx>. Auto-refresh interne (5 s).
window.MXLPlugins = window.MXLPlugins || {};
window.MXLPlugins["receiver_2110_mtl"] = {
  _timers: {},

  mount(el, vmid, ctx){
    const body = el.querySelector('.rx-body') || el;
    const toast = (ctx && ctx.toast) || (()=>{});

    const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
      '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    }[c]));

    const VIDEO_PATTERNS = {
      testsrc2:   'testsrc2 — couleurs animées',
      testsrc:    'testsrc — barres + timer',
      smptebars:  'barres SMPTE',
      rgbtestsrc: 'mire RGB',
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
      const fps = `${fmtFps(o.fps)} fps`;
      if (o.width && o.height) {
        const sc = (o.scan === 'i') ? 'i' : '';
        return `<span style="color:var(--text-muted)">${o.width}×${o.height}${sc}</span> · ${fps}`;
      }
      return fps;
    }
    function stateBadge(active){
      return active
        ? `<span class="badge" style="background:var(--status-running-bg); color:var(--status-running-fg)">subscribed</span>`
        : `<span class="badge" style="background:var(--border-soft); color:var(--text-muted)">idle</span>`;
    }
    function genTooltip(r, isAudio){
      const g = r.gen || {};
      const hint = `<div style="margin-top:7px; padding-top:6px; border-top:1px solid var(--border-soft);
          color:${r.simulated ? '#e8a33d' : 'var(--text-muted)'}; font-size:0.92em">
          👆 Cliquer pour ${r.simulated ? 'désactiver' : 'activer'} le générateur</div>`;
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
        return `<div class="gen-tip">
          <h5>⚙ Générateur sine local</h5>
          <div class="gt-row"><span>Fréquence</span><span>${esc(freq)}</span></div>
          <div class="gt-row"><span>Niveau</span><span>${esc(level)}</span></div>
          <div class="gt-row"><span>Canaux actifs</span><span>${nOn} / ${nChans}</span></div>
          <div class="gt-chans">${chans.join('')}</div>
          <div style="margin-top:6px; color:var(--text-muted); font-size:0.92em">
            Vert = actif · Rouge = ruptures · Grisé = muet</div>
          ${hint}
        </div>`;
      }
      const pat = g.pattern || 'testsrc2';
      const patLabel = VIDEO_PATTERNS[pat] || pat;
      const res = (r.width && r.height) ? `${r.width}×${r.height}${r.scan === 'i' ? 'i' : ''}` : '—';
      return `<div class="gen-tip">
        <h5>⚙ Générateur de mire local</h5>
        <div class="gt-row"><span>Mire</span><span>${esc(patLabel)}</span></div>
        <div class="gt-row"><span>Résolution</span><span>${esc(res)}</span></div>
        ${hint}
      </div>`;
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
      const genIcon = isAnc ? '<span class="gen-wrap"></span>' : `<span class="gen-wrap">
          <span class="gen-badge ${r.simulated ? 'on' : 'off'}" role="button" tabindex="0"
                data-essence="${ess}" data-idx="${r.idx}" data-enable="${r.simulated ? '0' : '1'}">GÉN</span>
          ${genTooltip(r, isAudio)}
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
            const flowing = Number(r.fps) > 0;
            const tc = r.timecode || '--:--:--:--';
            const col = flowing ? 'var(--status-running-fg)' : 'var(--status-stopped-fg)';
            const tip = flowing
              ? (r.timecode ? 'ANC 2110-40 actif · timecode ATC (SMPTE 12-1)' + (r.df ? ' · drop-frame' : '')
                            : 'ANC 2110-40 actif · pas de timecode ATC dans le flux')
              : 'Aucun paquet ANC reçu sur ce slot';
            return `<span style="color:${col};font-family:var(--font-mono,ui-monospace,monospace);letter-spacing:.5px" title="${tip}">${esc(tc)}${r.df ? ' DF' : ''}</span>`;
          })()
        : isAudio
        ? (() => {
            const flowing = Number(r.fps) > 0;
            const col = flowing ? 'var(--status-running-fg)' : 'var(--status-stopped-fg)';
            const txt = flowing ? '48K / L24' : '— / —';
            const tip = flowing ? '48 kHz / L24 / 8 canaux — flux actif' : 'Aucun chunk audio reçu sur ce slot';
            return `<span style="color:${col}" title="${tip}">${txt}</span>`;
          })()
        : `<span title="format vidéo">${fmtVideoFormat(r)}</span>`;
      const rowCls = isAnc ? 'flow-anc' : isAudio ? 'flow-audio' : 'flow-video';
      return `<div class="flow-row ${rowCls}">
        <span class="flow-tag ${isAnc ? 'd' : isAudio ? 'a' : 'v'}">${tag}</span>
        ${genIcon}
        ${identCtl}
        ${sdpCtl}
        <span>${stateBadge(r.active)}</span>
        ${rateCell}
        <span class="net-addr" title="entrée 2110"><span class="net-arrow">↘</span>${net}</span>
        <span class="mxl-path" title="sortie MXL (shared memory)">→ ${mxl}</span>
      </div>`;
    }
    function groupEnsembles(receivers){
      const videos = receivers.filter(x => x.essence === 'video');
      const audios = receivers.filter(x => x.essence === 'audio');
      const ancs   = receivers.filter(x => x.essence === 'anc');
      if (videos.length === 0)
        return audios.map(a => ({video: null, audios: [a], ancs: []}))
                     .concat(ancs.map(d => ({video: null, audios: [], ancs: [d]})));
      const groups = videos.map(v => ({video: v, audios: [], ancs: []}));
      const byVid = {};
      groups.forEach(g => { byVid[g.video.idx] = g; });
      // Associe chaque audio/ANC à SA vidéo (video_idx) ; fallback : idx (ancien 1:1).
      audios.forEach(a => {
        const vi = (a.video_idx != null) ? a.video_idx : a.idx;
        (byVid[vi] || groups[groups.length - 1]).audios.push(a);
      });
      ancs.forEach(d => {
        const vi = (d.video_idx != null) ? d.video_idx : d.idx;
        (byVid[vi] || groups[groups.length - 1]).ancs.push(d);
      });
      return groups;
    }

    async function refresh(){
      let c;
      try { c = await (await fetch(`/api/nmos/receivers/${vmid}/detail`)).json(); }
      catch(e){ body.innerHTML = '<div class="meta">Détail NMOS indisponible.</div>'; return; }
      const recvs = (c && c.receivers) || [];
      const ensembles = groupEnsembles(recvs);
      const activeCount = recvs.filter(x => x.active).length;
      const inner = ensembles.length === 0
        ? `<div class="meta" style="padding:8px 0">Aucun receiver actif</div>`
        : ensembles.map((g, i) => {
            const rows = [];
            if (g.video) rows.push(rowReceiver(g.video));
            g.audios.forEach(a => rows.push(rowReceiver(a)));
            (g.ancs || []).forEach(d => rows.push(rowReceiver(d)));
            const titleParts = [];
            if (g.video) titleParts.push('1 vidéo');
            if (g.audios.length) titleParts.push(`${g.audios.length} audio`);
            if ((g.ancs || []).length) titleParts.push(`${g.ancs.length} ANC`);
            return `<div class="ens">
              <div class="ens-title">Ensemble #${i} — ${titleParts.join(' + ')}</div>
              ${rows.join('')}
            </div>`;
          }).join('');
      body.innerHTML =
        `<div class="meta rx-meta">IP : ${esc((c && c.ip) || '—')} — ${recvs.length} receiver(s) NMOS · ${activeCount}/${recvs.length} actif(s)</div>`
        + inner;
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

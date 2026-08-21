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

    // i18n (catalogue plugin.2110_io.*). `window.t` rend la CLÉ BRUTE si elle manque : on teste,
    // et on retombe alors sur le français passé en 2e argument — jamais un code à l'écran.
    const T = (k, repli) => {
      const v = window.t ? window.t(k) : k;
      return (v && v !== k) ? v : (repli !== undefined ? repli : k);
    };

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
          if (!confirm((j.reason || T('plugin.2110_io.disruptive_confirm', 'Cette opération coupera brièvement TOUS les flux du moteur.'))
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

/* Le lecteur de flux (cadence, état) est un fichier partagé, chargé par layout.html. S'il
     * manque — gabarit servi depuis le cache après un ajout, fichier non déployé — la vue entière
     * s'arrêtait sur la première cellule. Un point commun ne doit pas pouvoir emporter tout ce qui
     * s'appuie dessus : à défaut, on rend une cellule qui DIT qu'elle est dégradée (jamais un
     * repli discret qui ferait croire à un affichage normal) et le reste de la ligne survit. */
    const FLUX = () => window.IOFlux || {
      circule: o => Number(o && o.fps) > 0,
      cadence: () => '<span style="color:var(--status-warning-fg)" title="Lecteur de flux non chargé (io_flux.js) : cette valeur est indisponible, pas nulle.">?</span>',
      badge: (t, l) => '<span class="badge" title="Lecteur de flux non chargé (io_flux.js).">' + l + '</span>',
    };

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
    // Taille IDENT (10..120 px) ramenée à 0..1 pour le tracé du catalogue.
    const IDENT_MIN = 10, IDENT_MAX = 120;
    const _identClamp = v => Math.max(IDENT_MIN, Math.min(IDENT_MAX, Number(v) || IDENT_MIN));
    const _ident01 = v => (_identClamp(v) - IDENT_MIN) / (IDENT_MAX - IDENT_MIN);
    // Tracé du rotatif — DÉLÉGUÉ au catalogue (MXLControls.knobSvg). `kind` vient de la classe
    // `.ctl-knob--*` quand on a l'élément sous la main ; à la construction du HTML il n'existe pas
    // encore, d'où l'arc explicite. Repli silencieux si le catalogue n'est pas chargé : on rend un
    // cadran vide plutôt que de casser toute la carte.
    function _identSvg(v, def, kind){
      if (!window.MXLControls) return '';
      return window.MXLControls.knobSvg(kind || 'arc', _ident01(v), def == null ? null : _ident01(def));
    }
    function fmtVideoFormat(o){
      if (o.width && o.height) {
        const sc = (o.scan === 'i') ? 'i' : 'p';      // p ou i TOUJOURS affiché (notation broadcast)
        // La cadence reste dans la chaîne — c'est celle que la source ANNONCE dans son SDP — mais
        // elle n'est plus COLORÉE (2026-08-06) : la couleur disait la santé d'une mesure sur un
        // nombre qui est une déclaration. La cadence réellement reçue a désormais son propre
        // badge à côté, et c'est leur ÉCART qui est intéressant : annoncer 50 et livrer 25 se voit
        // maintenant, alors qu'un seul chiffre coloré le cachait.
        const fpsTxt = (o.fps_sdp != null) ? String(Number(o.fps_sdp)).replace(/\.0$/, '')
                     : (o.fps != null) ? String(Number(o.fps)).replace(/\.0$/, '') : '';
        const res = `${o.width}×${o.height}${sc}${fpsTxt}`;
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
    // État d'un flux REÇU. Il ne disait qu'« abonné ou non » : sur une piste audio, un flux
    // souscrit dont plus aucun échantillon n'arrivait affichait toujours « subscribed », et le
    // silence ne se lisait qu'à la COULEUR du format à côté — un état porté par la seule teinte,
    // donc invisible pour qui la distingue mal. L'arrivée réelle est désormais dite en toutes
    // lettres. Vocabulaire commun aux deux sens (reçoit ↔ émet), rendu par le socle partagé.
    function stateBadge(r){
      const F = FLUX();
      const estVideo = r.essence !== 'audio' && r.essence !== 'anc';
      if (!r.active) return F.badge('inactif', T('plugin.2110_io.st_inactive', 'inactif'),
        T('plugin.2110_io.st_inactive_tip', "Aucun abonnement sur ce flux : rien n'est demandé à la source."));
      if (estVideo && r.rx_stalled)
        return F.badge('alerte', T('plugin.2110_io.st_noflux', '⚠ sans flux'),
          T('plugin.2110_io.st_noflux_tip', 'Abonné (IS-05) mais aucun flux ne remonte : création RX échouée (budget lcores du nœud) ou pas de trafic réseau (source/switch).'));
      if (F.circule(r)) return F.badge('ok', T('plugin.2110_io.st_receiving', 'reçoit'),
        T('plugin.2110_io.st_receiving_tip', 'Abonné, et les données arrivent.'));
      return F.badge('attente', estVideo ? T('plugin.2110_io.st_subscribed', 'abonné')
                                         : T('plugin.2110_io.st_mute', 'muet'),
        estVideo ? T('plugin.2110_io.st_subscribed_tip', 'Abonné, mais aucune image ne remonte encore.')
                 : T('plugin.2110_io.st_mute_tip', "Abonné, mais aucun échantillon n'arrive : la source est silencieuse ou absente."));
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

    function rowReceiver(r, groupVideo){
      // Préfère le SDP (toutes les jambes / DUP) ; repli sur les transport_params IS-05.
      let flows = flowsFromSdp(r.sdp);
      if (!flows.length && r.multicast_ip) flows = [`${r.multicast_ip}:${r.destination_port ?? '?'}`];
      // Une adresse multicast par ligne (lisibilité des flux DUP/2022-7) : chaque
      // couple reste insécable (pas de coupure mid-octet), tronqué en ellipse si trop long.
      const net = flows.length
        ? flows.map(f => `<span><span class="io-flow-addr">${esc(f)}</span></span>`).join('')
        : '<span style="color:var(--text-muted)">—</span>';
      const mxl = r.shm_path ? esc(r.shm_path) : '<span style="color:var(--text-muted)">—</span>';
      const isAudio = r.essence === 'audio';
      const isAnc   = r.essence === 'anc';
      const ess = isAudio ? 'audio' : 'video';
      // Nommage d'AFFICHAGE uniforme avec les slots TX (« Tx #n ») : « Rx #1 » ; « Rx #1 AUD 1 » =
      // Rx 1, 1ʳᵉ piste audio ; « Rx #1 ANC ». Index 1-based. Le nom technique du flux (shm) reste
      // visible dans la colonne MXL — jamais comme nom du flux.
      // Tag PAR ESSENCE, identique aux sorties (2026-08-06) : le numéro de l'ensemble est déjà
      // dans le titre juste au-dessus, le répéter sur chaque ligne volait la largeur du libellé.
      // Un flux audio INDÉPENDANT (sans vidéo d'attache) garde son numéro : il n'a pas de titre
      // d'ensemble pour le porter.
      const tag = isAnc ? 'ANC'
        : isAudio
        ? ((r.video_idx != null && r.audio_sub_idx != null)
            ? `AUD ${r.audio_sub_idx + 1}`
            : `AUD #${r.idx + 1}`)
        : 'VIDÉO';
      // LIBELLÉ DE LA SOURCE, sous le tag : le nom que l'exploitant reconnaît (UMD reçu par TSL,
      // nom d'antenne…). Le NIVEAU affiché est celui choisi dans la barre de navigation — on
      // n'en impose aucun, un même flux porte plusieurs noms selon le métier. La règle de repli
      // et l'héritage parent→audio/ANC vivent dans window.SourceLabels (partagés avec /labels).
      const _shm = String(r.shm_path || '').replace(/^\/dev\/shm\//, '');
      let _lab = (window.SourceLabels && _shm) ? window.SourceLabels.labelOf(_shm)
                                              : { value: '', inherited: false, level: null };
      // Audio/ANC sans libellé propre → celui de la VIDÉO du même Rx. L'héritage de /labels ne
      // couvre pas ce cas : il exige que le parent soit un PRÉFIXE du nom (`player` → `player_audio`),
      // or le moteur nomme ses flux `<hôte>_0` et `<hôte>_audio_0` — des frères, pas un préfixe.
      // Ici on ne devine rien : le moteur nous donne lui-même le groupement (g.video).
      if (!_lab.value && groupVideo && window.SourceLabels) {
        const _vshm = String(groupVideo.shm_path || '').replace(/^\/dev\/shm\//, '');
        const _vlab = _vshm ? window.SourceLabels.labelOf(_vshm) : null;
        if (_vlab && _vlab.value) _lab = { value: _vlab.value, inherited: true, level: _vlab.level };
      }
      let _labTip = '';
      if (_lab.value && window.SourceLabels) {
        _labTip = 'Libellé « ' + window.SourceLabels.levelName(_lab.level) + ' »'
                + (_lab.level !== window.SourceLabels.level
                     ? ' — le niveau demandé est vide pour cette source' : '')
                + (_lab.inherited ? ' — hérité de la vidéo du même Rx' : '')
                + ' · niveau réglable en haut de page';
      }
      // Le libellé est un ÉLÉMENT DE GRILLE À PART (2ᵉ rangée, colonnes 1→4) et non un enfant du
      // tag : la colonne du tag fait 64 px, un nom d'antenne y était tronqué dès quelques
      // caractères. Sous GÉN et IDENT il dispose de ~230 px, et la place était libre.
      const labelLine = _lab.value
        ? `<small class="io-flow-lab${
                  _lab.level !== (window.SourceLabels || {}).level ? ' fallback' : ''}"
                  title="${esc(_lab.value + (_labTip ? ' — ' + _labTip : ''))}">${esc(_lab.value)}</small>`
        : '';
      // Bouton générateur : data-* lus par délégation (pas d'onclick global). Pas de GÉN pour l'ANC.
      // Placeholder vide quand absent : la grille .flow-row place par position → sans cet espace
      // réservé, le badge SDP se décalerait dans une colonne de gauche (désalignement audio/ANC vs vidéo).
      const _genPat = (r.gen && r.gen.pattern) || 'bars';
      const _patOpts = Object.entries(VIDEO_PATTERNS).map(([k,v]) =>
        `<option value="${k}"${k===_genPat?' selected':''}>${esc(v)}</option>`).join('');
      // VIDÉO : le badge GÉN reflète l'état LIVE du moteur (mire réellement émise, mode='simu') —
      // honnête, ≠ config. AUDIO : inchangé (config `simulated`, déjà clair). Le clic toggle le gen.
      const _genOn = isAudio ? !!r.simulated : !!r.generating;
      // GÉN : geste d'EXPLOITATION à accrochage (mettre une mire à l'antenne) → poussoir du
      // catalogue, `.ctl-push--led`. C'est un vrai <button role="switch"> : Entrée et Espace
      // l'actionnent, le focus se voit, l'état est annoncé. Le <span role="button" tabindex="0">
      // d'avant n'écoutait que le clic — annoncé actionnable, il ne l'était qu'à la souris.
      const genIcon = isAnc ? '<span class="io-flow-gen gen-wrap"></span>' : `<span class="io-flow-gen gen-wrap">
          <button type="button" class="ctl-push ctl-push--led io-gen" role="switch"
                aria-pressed="${_genOn}"
                data-essence="${ess}" data-idx="${r.idx}" data-enable="${_genOn ? '0' : '1'}"><span
                class="ctl-led"></span>GÉN</button>
          ${(!isAudio && _genOn) ? `<span class="gen-pat-wrap"><select class="ctl-select io-patsel" data-essence="video" data-idx="${r.idx}">${_patOpts}</select></span>` : ''}
          ${genTooltip(r, isAudio, _genOn)}
        </span>`;
      // IDENT : incrustation 3 lignes (nom/source/format) — slots vidéo uniquement.
      // IDENT : badge marche/arrêt + petit rotatif compact pour la taille du texte
      // (glisser ↕ ou molette) — reste dans sa colonne, ne décale pas le badge GÉN.
      // Taille AUTOMATIQUE, dérivée de la hauteur de l'image : c'est à la fois la valeur servie
      // quand l'exploitant n'a rien réglé, et le DÉFAUT vers lequel le rotatif revient. Les deux
      // sont calculés au même endroit — deux formules voisines auraient fini par diverger, et le
      // repère de l'arc aurait alors désigné une valeur que la remise à zéro n'atteint pas.
      const identDef = _identClamp(Math.max(12, Math.round((r.height || 720) / 28)));
      const identSz  = _identClamp(r.ident_size || identDef);
      // Placeholder vide pour audio/ANC (pas d'IDENT) → réserve la colonne 3 de la grille,
      // sinon le bouton SDP (colonne 4) remonte et n'est plus aligné avec celui de la vidéo.
      // Le rotatif de taille IDENT vient du catalogue : `.ctl-knob--arc`, tracé par MXLControls.
      // Il était en AIGUILLE et dessiné à la main — or la règle de parc du 2026-07-26 veut que
      // TOUS les rotatifs du produit soient en arc, pour qu'un réglage se lise pareil partout.
      // Le défaut est matérialisé par le repère de l'arc : on voit d'un coup d'œil si on s'en
      // écarte, ce qui n'existait pas avec l'aiguille.
      const identCtl = (isAudio || isAnc) ? '<span class="io-flow-ident ident-wrap"></span>' : `<span class="io-flow-ident ident-wrap">
          <button type="button" class="ctl-push ctl-push--led io-ident" role="switch"
                aria-pressed="${!!r.ident}" data-idx="${r.idx}" data-enable="${r.ident ? '0' : '1'}"
                title="Incrustation 3 lignes (nom · source/multicast · format), fond noir, haut-droite"><span
                class="ctl-led"></span>IDENT</button>
          ${r.ident ? `<span class="ctl-knob ctl-knob--arc io-identknob"
                data-idx="${r.idx}" data-min="${IDENT_MIN}" data-max="${IDENT_MAX}" data-step="2"
                data-val="${identSz}" data-def="${identDef}" data-unit="px">
                <button type="button" class="ctl-knob-hit" role="slider"
                  aria-label="${esc(T('js.io2110.ident_size_aria', 'Taille du texte IDENT'))}" aria-valuemin="${IDENT_MIN}" aria-valuemax="${IDENT_MAX}"
                  aria-valuenow="${identSz}" aria-valuetext="${identSz}px"
                  title="Taille du texte IDENT — glisser ↕, molette, flèches (Maj = pas large). Entrée = taille automatique.">${
                  _identSvg(identSz, identDef)}</button>
                <span class="ctl-knob-val">${identSz}px</span></span>` : ''}
        </span>`;
      // SDP (vidéo, audio ET ANC) : badge ouvrant une modale d'affichage/édition.
      // Le SDP n'est PAS inliné dans le HTML (multiligne) — on le garde en cache par
      // (essence:idx) (_sdpByIdx, scope mount), la modale le relit. L'audio (2110-30) et
      // l'ANC (2110-40) partagent la même chaîne d'abonnement manuel IS-05 que la vidéo
      // (manual_subscribe côté backend, essence transmise) — sans ça, pas d'abonnement
      // audio/ANC sans contrôleur NMOS externe → shm non alimenté → streamer muet.
      const sdpEss = isAnc ? 'anc' : isAudio ? 'audio' : 'video';
      _sdpByIdx[sdpEss + ':' + r.idx] = r.sdp || '';
      // SDP : il n'y a RIEN à activer ici. Le clic OUVRE une fenêtre, et le témoin vert n'est pas
      // ce que le clic bascule — c'est un constat (un SDP est posé, ou non). D'où un bouton de
      // COMMANDE (`.btn .btn-sm`), l'ellipsis « … » qui annonce une fenêtre, `aria-haspopup` qui
      // l'annonce à qui n'a pas l'écran, et la LED du catalogue pour l'état. En faire un
      // `role="switch"` aurait fait annoncer « interrupteur, activé » sur un contrôle qui n'active
      // rien : le contrôle aurait menti sur sa propre nature.
      const sdpCtl = `<span class="io-flow-sdp sdp-wrap">
          <button type="button" class="btn btn-sm io-sdp" aria-haspopup="dialog"
                data-essence="${sdpEss}" data-idx="${r.idx}"
                title="${r.sdp ? 'Un SDP est posé sur ce slot' : 'Aucun SDP sur ce slot'} — afficher / coller (abonnement NMOS manuel)"><span
                class="ctl-led${r.sdp ? ' on' : ''}" style="--ctl-led-col:var(--status-running-fg)"></span>SDP…</button>
        </span>`;
      const rateCell = isAnc
        ? (() => {
            // ANC 2110-40 : on ne cherche pas à afficher un timecode. On confirme la réception
            // et on précise le type de métadata SI on le connaît (timecode ATC décodé, sinon SMPTE 291).
            // La colonne FORMAT dit ce que le flux TRANSPORTE, pas s'il arrive — ça, c'est le
            // badge d'état, désormais présent sur chaque essence. Elle disait les deux, et disait
            // l'arrivée par la couleur.
            if (!r.sdp) return '<span style="color:var(--text-muted)">—</span>';
            const type = r.timecode ? 'timecode (SMPTE ST 12M)' : 'SMPTE ST 291';
            return `<span title="Métadonnées ANC 2110-40 : ${esc(type)}">${esc(type)}</span>`;
          })()
        : isAudio
        ? (() => {
            // Format AUDIO 2110-30 lu du SDP : « 48kHz / L24 / 8ch ». Affiché dès l'abonnement
            // (r.sdp), même sans flux ; couleur = état de réception.
            if (!(r.sdp && r.sample_rate)) return '<span style="color:var(--text-muted)">— / —</span>';
            const khz = (r.sample_rate / 1000).toString().replace(/\.0$/, '');
            const txt = `${khz}kHz / L${r.bit_depth || 24} / ${r.channels || 1}ch`;
            return `<span title="Format audio annoncé par le SDP reçu">${esc(txt)}</span>`;
          })()
        : (() => {
            // VIDÉO : ni abonné (IS-05) ni générateur actif → pas de signal. On n'affiche PAS un
            // format par défaut trompeur (le moteur ne génère plus rien par défaut, cf. _simu_loop).
            if (!r.active && !r.generating) {
              return `<span style="color:var(--status-stopped-fg)" title="${esc(T('plugin.2110_io.not_subscribed_tip', 'Slot non abonné — aucun flux ni générateur (sortie vide)'))}">${esc(T('plugin.2110_io.not_subscribed', 'non abonnée'))}</span>`;
            }
            return `<span title="format vidéo${r.generating ? ' (mire générée)' : ''}">${fmtVideoFormat(r)}</span>`;
          })();
      // Cadence MESURÉE, face au format qui, lui, est déclaré. Rendue par le socle partagé — et
      // seulement pour la vidéo : sur une session audio, le compteur du moteur totalise des
      // CHUNKS de 1 ms, ce qui s'affichait « 1000.0 fps ». Le nombre était juste, son unité non.
      const fpsCell = FLUX().cadence(
        isAnc ? 'anc' : isAudio ? 'audio' : 'video', r.fps, null, {sens: 'rx'});
      const essCls = isAnc ? 'd' : isAudio ? 'a' : 'v';
      // Gabarit PARTAGÉ avec les sorties (`.io-flow`, static/css/base.css). Toute cellule est
      // posée même vide : c'est ce qui garde les colonnes alignées d'une ligne à l'autre.
      return `<div class="io-flow io-flow--${essCls}">
        <span class="io-flow-tag">${tag}</span>
        ${labelLine}
        ${genIcon}
        ${identCtl}
        ${sdpCtl}
        <span class="io-flow-state">${stateBadge(r)}</span>
        <span class="io-flow-fmt">${rateCell}</span>
        <span class="io-flow-fps">${fpsCell}</span>
        <span class="io-flow-net" title="adresses 2110 reçues">${net}</span>
        <span class="io-flow-mxl${r.shm_path ? '' : ' vide'}" title="nom du flux sur le bus MXL">${mxl}</span>
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
        if (vid) lines.push(`<div class="io-txsum">
          <span class="badge" style="background:var(--bg-input,var(--bg));border:1px solid var(--border)">2110-20</span>
          ${fmtDest(vid)}</div>`);
        auds.forEach((a,i) => lines.push(`<div class="io-txsum">
          <span class="badge" style="background:var(--bg-input,var(--bg));border:1px solid var(--border)">2110-30 #${i+1}</span>
          ${fmtDest(a)}</div>`));
        if (anc) lines.push(`<div class="io-txsum">
          <span class="badge" style="background:var(--bg-input,var(--bg));border:1px solid var(--border)">2110-40</span>
          ${fmtDest(anc)}</div>`);
        const _txPort = (vid && vid.port) || (auds[0] && auds[0].port) || (anc && anc.port) || null;
        return `<div class="ens ctl-dense">
          <div class="ens-title">Slot Tx #${Number(ti) + 1}${portSelector('tx', Number(ti), _txPort)}</div>
          ${lines.join('')}
        </div>`;
      }).join('');
      return `<div class="meta" style="margin-top:10px;padding-top:8px;border-top:1px solid var(--border-soft);font-weight:600">Sorties (TX)</div>`
           + rows;
    }

    // ─── Jauges de charge ────────────────────────────────────────────────────────────────────
    // `.ctl-gauge` du catalogue. Ce qui change au-delà de l'apparence :
    //  · les seuils ne sont plus des couleurs écrites à la main (#e8a33d, >60, >80) mais les états
    //    `warn`/`over` du composant, et le seuil d'alerte est MATÉRIALISÉ par un trait — on voit
    //    donc qu'on s'en approche avant qu'il se produise ;
    //  · une mesure ABSENTE prend l'état `na` (estompé, « — ») au lieu d'une barre à zéro, qui se
    //    lisait « tout va bien » alors qu'elle voulait dire « je ne sais pas ».
    // Les seuils d'exploitation eux-mêmes sont INCHANGÉS (alerte 60 %, critique 80 %) : cette
    // migration change la façon de le dire, pas ce qui est dit.
    const NIC_SEUIL = 60, NIC_CRIT = 80;
    function _nicEtat(pct){ return pct == null ? 'na' : pct >= NIC_CRIT ? 'over' : pct >= NIC_SEUIL ? 'warn' : ''; }
    // Couleur du texte chiffré, accordée à l'état de la jauge — par TOKEN, jamais en dur.
    function _nicCouleur(etat){
      return etat === 'over' ? 'var(--status-stopped-fg)'
           : etat === 'warn' ? 'var(--status-warning-fg)'
           : etat === 'na'   ? 'var(--text-muted)' : 'var(--status-running-fg)';
    }
    // La jauge est rendue SANS `.ctl-gauge-val` : la règle du composant veut que la valeur reste
    // affichée en permanence dès que ça alerte, or ici elle l'est déjà — en clair, à gauche, dans
    // tous les cas. Ajouter la bulle du composant afficherait deux fois le même chiffre.
    // `seuil === false` : grandeur dont l'approche du maximum ne se prévient pas (le plafond EST
    // le bout de la barre, ex. les sessions du limiteur matériel) → variante sans trait, plutôt
    // qu'un repère posé sur le bord qui n'apprendrait rien.
    function _gauge(pct, etat, titre, seuil){
      const p = Math.max(0, Math.min(1, (pct || 0) / 100));
      const cls = 'ctl-gauge' + (etat ? ' ' + etat : '') + (seuil === false ? ' ctl-gauge--sans-seuil' : '');
      return `<span class="${cls}" role="img"
        style="--ctl-gauge-w:100%;flex:1${seuil === false ? '' : `;--ctl-gauge-seuil:${NIC_SEUIL}%`}"
        aria-label="${esc(titre)}"><span class="ctl-gauge-fill" style="transform:scaleX(${p.toFixed(3)})"></span></span>`;
    }
    function _nicBar(gbps, estGbps, cap, label) {
      const val = gbps != null ? gbps : estGbps;
      const isEst = gbps == null && estGbps != null;
      if (val == null) return '';
      const pct = Math.min(100, Math.round(val / cap * 100));
      const etat = _nicEtat(pct);
      const txt = `${isEst ? '~' : ''}${val.toFixed(1)} / ${cap} Gbps (${pct}%)${isEst ? ' (estimation)' : ''}`;
      return `<div class="nic-bar-wrap">
        <span class="nic-bar-lbl">${label}</span>
        <span class="nic-bar-val${isEst ? ' nic-bar-est' : ''}" style="color:${_nicCouleur(etat)}">${txt}</span>
        ${_gauge(pct, etat, `${label} : ${txt}`)}
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
      // Aucune mesure ⇒ PAS de pourcentage. Ce port affichait auparavant une barre à zéro, qui se
      // lit « ce port ne reçoit rien » alors qu'elle voulait dire « je n'ai pas la mesure ».
      const pct   = val != null ? Math.min(100, Math.round(val / cap * 100)) : null;
      const etat  = _nicEtat(pct);
      const down  = p.link_up === false;
      return `<div class="io2110-portchip${down ? ' down' : ''}" style="border-left-color:${col}">
        <div class="pc-top"><span class="pc-name" style="color:${col}">${esc(p.iface)}</span>
          ${p.primary ? '<span class="pc-prim">PRIM</span>' : ''}
          <span class="pc-net">${esc(p.network || '')}</span>
          ${down ? '<span class="pc-down" title="Lien physique down">⚠ lien</span>' : ''}</div>
        <div class="pc-load"><span class="pc-loadval${isEst ? ' est' : ''}"${
          val == null ? ' style="color:var(--text-muted)"' : ''}>${
          val != null ? (isEst ? '~' : '') + val.toFixed(1) + ' / ' + cap + ' G' : '—'}</span>
          ${_gauge(pct, etat, val != null
              ? `${p.iface} : ${val.toFixed(1)} sur ${cap} Gbps`
              : `${p.iface} : débit non mesuré`)}</div>
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
      // SMPTE 2022-7 : les deux legs d'une paire sont visuellement liés (chip A ⇄ chip B).
      const seenPair = new Set();
      const strip = ports.map(p => {
        const pr = _pairOf(p.iface);
        if (!pr) return _portChip(p);
        if (seenPair.has(p.iface)) return '';
        const twin = pr[0] === p.iface ? pr[1] : pr[0];
        const tp = ports.find(x => x.iface === twin);
        seenPair.add(twin);
        return _portChip(p) + '<span class="io2110-pairlink" title="SMPTE 2022-7">⇄</span>' + (tp ? _portChip(tp) : '');
      }).join('');
      const detail = _nicOpen ? `<div class="io2110-portdetail">${
        ports.map(p => {
          const col = _netColor(p.network);
          const cap = p.port_capacity_gbps || 100;
          const rxBar = _nicBar(p.rx_gbps, p.rx_estimated_gbps, cap, 'RX');
          // Port DPDK (PF vfio, socle narrow) : plus de plafond AF-XDP (xdp_hw=null) — la métrique
          // pertinente = files RSS RX + sessions RL TX / cap RL du port. Sinon barre XDP historique.
          const xdpBar = (p.pmd === 'dpdk')
            ? (_rssRow(p.rx_queues) + (p.rl_tx_cap ? _rlBar(p.tx_sessions_active || 0, p.rl_tx_cap, 0) : ''))
            : ((p.xdp_hw && p.xdp_reserved != null)
               ? _xdpBar(p.xdp_active || 0, p.xdp_planned, p.xdp_reserved, p.xdp_hw)
               : '');
          const flows = p.rx_flow_count != null ? `<span class="pc-meta">Flux RX : ${p.rx_flow_count}</span>` : '';
          return `<div class="io2110-portcard" style="border-left-color:${col}">
            <h5><span style="color:${col}">${esc(p.iface)}</span>${p.primary ? '<span class="pc-prim">PRIM</span>' : ''}
              ${p.network ? `<span class="pc-net">${esc(p.network)}</span>` : ''}${_ptpBadge(p.ptp_state)}
              <span class="pc-meta" style="margin-left:auto">${p.link_up === false ? '⚠ lien down' : (p.link_up ? 'lien up' : '')}</span></h5>
            ${rxBar}${xdpBar}${flows}</div>`;
        }).join('')}</div>` : '';
      return `<div class="io2110-nicbar"><div class="io2110-portstrip">${strip}</div>
        <button type="button" class="btn btn-sm io-nictoggle" aria-expanded="${_nicOpen}"
          title="Afficher / masquer le détail par port physique">${_nicOpen ? '▾' : '▸'} Par NIC</button>
        </div>${detail}`;
    }

    let _cachedEnsembles = [];
    let _cachedRecvs      = [];  // dernier recvs reçu (refresh) — lu par _renderBody (fonction sœur)
    let _nicOpen          = false;  // multi-NIC : détail « Par NIC » déplié (état local de la carte)
    let _lastNicPorts     = [];     // dernier nic_ports reçu (pour reconstruire la bande au toggle)
    let _cachedMeta      = '';
    let _cachedTxHtml    = '';
    let _cachedVideoCount = 0;  // capacité totale déployée
    let _cachedActiveRx   = 0;  // slots simultanés autorisés (active_rx_count)
    let _nodePorts        = [];  // ports média du nœud (multi-NIC) ; [] = mono-port → pas de sélecteur
    let _pairs227         = [];  // paires red/blue actives (moteur en SMPTE 2022-7) : [[ifA, ifB], …]

    // Sélecteur de PORT (NIC) d'un slot — multi-NIC seulement (≥2 ports). « Auto » = répartition
    // automatique (badge du port effectif courant) ; un port précis = épinglage. POST /api/mtl/<vmid>/pin.
    // SMPTE 2022-7 : une session dual-leg occupe LES DEUX ports d'une paire → on propose la PAIRE
    // (« ifA ⇄ ifB », valeur = leg rouge), jamais ses membres séparément ; les ports non appariés
    // restent sélectionnables individuellement (aucune perte hors 2022-7).
    function _pairOf(ifn){ return _pairs227.find(pr => pr[0] === ifn || pr[1] === ifn); }
    // Le choix de PORT se fait par ENSEMBLE, pas par essence (harmonisé avec les sorties le
    // 2026-08-06). L'exploitant raisonne par SOURCE : recevoir la vidéo sur une carte pendant que
    // son audio arrive sur l'autre n'est pas un réglage qu'on cherche, c'est un accident qu'on
    // subit — et la page l'offrait ligne par ligne, donc trois fois par source, sans jamais dire
    // que les trois devaient concorder. `idxs` porte tous les flux de l'ensemble : le serveur les
    // épingle en UNE opération (une boucle de requêtes ici laisserait un ensemble à moitié posé).
    // Les flux INDÉPENDANTS (audio/ANC sans vidéo) sont chacun leur propre ensemble : ils gardent
    // donc leur sélecteur, sans exception à écrire.
    function portSelector(role, idx, port, idxs){
      if (!_nodePorts || _nodePorts.length < 2) return '';
      const cur = (port && port.pinned) ? port.iface : '';   // '' = Auto
      const eff = (port && port.iface) || '';
      const curPair = _pairOf(cur);
      const effPair = _pairOf(eff);
      const effLbl = effPair ? `${effPair[0]} ⇄ ${effPair[1]}` : eff;
      const opts = [`<option value=""${cur===''?' selected':''}>Auto${eff?` (${esc(effLbl)})`:''}</option>`];
      const seen = new Set();
      for (const p of _nodePorts){
        const pr = _pairOf(p.ifname);
        if (pr){
          const key = pr.join(':');
          if (seen.has(key)) continue;
          seen.add(key);
          const sel = curPair && curPair.join(':') === key;   // l'un OU l'autre membre épinglé
          opts.push(`<option value="${esc(pr[0])}"${sel?' selected':''}>${esc(pr[0])} ⇄ ${esc(pr[1])}${p.network?` · ${esc(p.network)}`:''}</option>`);
        } else {
          opts.push(`<option value="${esc(p.ifname)}"${cur===p.ifname?' selected':''}>${esc(p.ifname)}${p.network?` · ${esc(p.network)}`:''}</option>`);
        }
      }
      return `<span class="port-wrap"><select class="ctl-select io-portsel" data-role="${role}" data-idx="${idx}"
                data-idxs="${esc((idxs && idxs.length ? idxs : [idx]).join(','))}"
                title="${_pairs227.length ? 'Paire 2022-7 (les deux legs) ou port de cet ensemble' : 'Port (NIC) de cet ensemble — Auto = répartition automatique entre les ports du réseau'}">${opts.join('')}</select></span>`;
    }

    // Ligne de flux audio/ANC avec bouton de retrait granulaire (« Option A »).
    function _rmWrap(html, fid){
      return `<div class="io2110-flowrow">${html}`
           + (fid ? `<button type="button" class="btn btn-sm io2110-flowrm" data-fid="${esc(fid)}" title="Retirer ce flux">✕</button>` : '')
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

    // Barre « Sessions TX (RL) » — socle DPDK narrow : budget TX = sessions sur le rate-limiter
    // matériel (cap RL par port, limite dure de la carte — docs/chantiers/DPDK_NARROW.md §7). `dropped` =
    // sessions au-delà du cap IGNORÉES par le moteur → badge SUR-CAPACITÉ. Identique io2110.js.
    function _rlBar(active, cap, dropped, scope){
      active  = Math.max(0, active || 0);
      cap     = Math.max(1, cap || 1);
      dropped = Math.max(0, dropped || 0);
      const pct  = Math.min(100, Math.round(active / cap * 100));
      const over = dropped > 0 || active > cap;
      // Le plafond RL est une limite DURE de la carte : atteindre le cap est déjà l'alerte, le
      // dépasser est la faute. Pas de seuil intermédiaire à matérialiser ici.
      const etat = over ? 'over' : (active >= cap ? 'warn' : '');
      let txt = window.t('js.io2110.rl_val').replace('{act}', active).replace('{cap}', cap)
                + ` (${pct}%)`;
      if (over) txt = window.t('js.io2110.rl_overcap').replace('{n}', dropped || (active - cap))
                      + ' — ' + txt;
      return `<div class="nic-bar-wrap">
        <span class="nic-bar-lbl" title="${esc(window.t('js.io2110.rl_tip'))}">${esc(window.t('js.io2110.rl_sessions'))}</span>
        <span class="nic-bar-val" style="color:${_nicCouleur(etat)}">${esc(txt + (scope || ''))}</span>
        ${_gauge(pct, etat, esc(txt), false)}
      </div>`;
    }

    // Ligne « Files RX (RSS) » (socle DPDK) : files de réception réservées, dimensionnées à la
    // demande (pas de plafond AF-XDP sous vfio) — informatif, sans barre de saturation.
    function _rssRow(n, scope){
      if (n == null) return '';
      return `<div class="nic-bar-wrap">
        <span class="nic-bar-lbl" title="${esc(window.t('js.io2110.rx_rss_tip'))}">${esc(window.t('js.io2110.rx_rss'))}</span>
        <span class="nic-bar-val">${esc(window.t('js.io2110.rx_rss_val').replace('{n}', n) + (scope || ''))}</span>
      </div>`;
    }

    function _renderBody() {
      const ens = _cachedEnsembles;
      const inner = ens.length === 0
        ? `<div class="meta" style="padding:8px 0">${esc(T('plugin.2110_io.no_receiver', 'Aucun receiver actif'))}</div>`
        : ens.map((g, i) => {
            const rows = [];
            if (g.video) rows.push(rowReceiver(g.video));
            g.audios.forEach(a => rows.push(_rmWrap(rowReceiver(a, g.video), a.flow_id)));
            (g.ancs || []).forEach(d => rows.push(_rmWrap(rowReceiver(d, g.video), d.flow_id)));
            if (g.independent) {
              // Flux indépendants (audio/ANC non rattachés à une vidéo).
              // Flux indépendants : chacun EST son propre ensemble (aucune vidéo d'attache), donc
              // le port se choisit flux par flux — pas une exception à la règle, son application.
              return `<div class="ens ens-indep ctl-dense">
                <div class="ens-title">Flux indépendants${
                  g.audios.concat(g.ancs || []).map(x =>
                    `<span class="io-indepport" title="Port du flux ${esc(String(x.shm_path || '').replace(/^\/dev\/shm\//, '') || ('#' + x.idx))}">${
                      esc((x.essence || '').toUpperCase())} ${portSelector('rx', x.idx, x.port)}</span>`).join('')}</div>
                ${rows.join('')}
              </div>`;
            }
            const titleParts = ['1 vidéo'];
            if (g.audios.length) titleParts.push(`${g.audios.length} audio`);
            if ((g.ancs || []).length) titleParts.push(`${g.ancs.length} ANC`);
            const vfid = g.video ? (g.video.flow_id || '') : '';
            // Ajout/retrait granulaire par vidéo : + Audio / + ANC rattachés ; ✕ retire la source entière.
            const ctrls = vfid ? `<div class="io2110-flowctrls">
              <button type="button" class="btn btn-sm io2110-addflow" data-ess="audio" data-att="${esc(vfid)}">+ Audio</button>
              <button type="button" class="btn btn-sm io2110-addflow" data-ess="anc" data-att="${esc(vfid)}">+ ANC</button>
              <button type="button" class="btn btn-sm io2110-flowrm io2110-rmgrp" data-fid="${esc(vfid)}" title="Retirer cette source">✕ source</button>
            </div>` : '';
            // `ctl-dense` : contexte du catalogue. Une carte porte N lignes de trois contrôles ;
            // aux métriques de pupitre (poussoir 38 px), seize slots repoussent la carte hors de
            // l'écran. La cible tactile n'est pas sacrifiée pour autant (cf. controls.css).
            // Le port de l'ensemble : posé sur la vidéo ET sur tous ses flux attachés, en une
            // opération serveur. Le sélecteur porte l'index de la vidéo (référence) et la liste
            // complète dans `data-idxs`.
            const _ensIdxs = [g.video && g.video.idx]
              .concat(g.audios.map(a => a.idx), (g.ancs || []).map(d => d.idx))
              .filter(x => x != null);
            return `<div class="ens ctl-dense">
              <div class="ens-title">Rx #${i + 1} — ${titleParts.join(' + ')}${
                g.video ? portSelector('rx', g.video.idx, g.video.port, _ensIdxs) : ''}</div>
              ${rows.join('')}
              ${ctrls}
            </div>`;
          }).join('');
      const ensVideoCount = ens.filter(g => g.video).length;
      // Headroom = pool pré-provisionné (video_count). Au-delà → augmenter le pool (redéploiement).
      const moreBtn = ensVideoCount < _cachedVideoCount
        ? `<button class="io-addrow">+ Ajouter une source</button>`
        : '';
      const delBtn = ensVideoCount > 0
        ? `<button class="io-addrow io2110-del-rx">${esc(T('plugin.2110_io.del_last_source', '− Retirer la dernière source'))}</button>`
        : '';
      // Remède famine : ≥1 source abonnée mais sans flux (rx_stalled) → bouton de réalignement des
      // files (redéploiement du moteur). Disruptif → passe par mtlMutate (confirmation serveur).
      const _anyStalled = _cachedRecvs.some(r => r.rx_stalled);
      const realignBtn = _anyStalled
        ? `<button class="io-addrow io2110-realign" title="Une ou plusieurs sources sont abonnées mais ne reçoivent aucun flux. Redéployer le moteur réaligne les files (coupure brève de TOUS les flux).">⟳ Redéployer pour réaligner les files</button>`
        : '';
      // Création d'un flux INDÉPENDANT (audio/ANC sans vidéo d'attache).
      const indepAdd = `<div class="io2110-flowctrls io2110-indepadd">
        <span class="meta">Indépendant :</span>
        <button type="button" class="btn btn-sm io2110-addflow" data-ess="audio" data-att="">+ Audio</button>
        <button type="button" class="btn btn-sm io2110-addflow" data-ess="anc" data-att="">+ ANC</button>
      </div>`;
      body.innerHTML = _cachedMeta + _nicPortStrip(_lastNicPorts) + inner + indepAdd + moreBtn + delBtn + realignBtn + _cachedTxHtml;
      // Le bouton de remise au défaut des rotatifs est posé par le CATALOGUE, pas écrit ici : c'est
      // ce qui garantit qu'on ne l'oublie pas, et l'appel est idempotent (rien n'est doublé à
      // chaque rendu). Il n'appelle rien du plugin, il émet `ctl-knob-reset` (cf. onKnobReset).
      if (window.MXLControls) window.MXLControls.attachKnobGestures(body, 'Remettre à la taille automatique');
      const realignEl = body.querySelector('.io2110-realign');
      if (realignEl) {
        realignEl.onclick = async () => {
          realignEl.disabled = true;
          try {
            const r = await mtlMutate(`/api/mtl/${vmid}/realign`, {});
            if (r && !r.ok) { const j = await r.json().catch(()=>({})); toast(j.error || T('plugin.2110_io.err_realign', 'Erreur réalignement'), 'error'); }
          } catch(e) { toast(T('plugin.2110_io.err_network', 'Erreur réseau'), 'error'); }
          await refresh();
        };
      }
      // Ajout granulaire de flux (rattaché si data-att, sinon indépendant).
      body.querySelectorAll('.io2110-addflow').forEach(b => b.onclick = async () => {
        b.disabled = true;
        try {
          const r = await mtlMutate(`/api/mtl/${vmid}/flows/add`,
            {role: 'rx', essence: b.dataset.ess, attached_to: b.dataset.att || null});
          if (r && !r.ok) { const j = await r.json().catch(()=>({})); toast(j.error || T('plugin.2110_io.err_add_flow', 'Erreur ajout flux'), 'error'); }
        } catch(e) { toast(T('plugin.2110_io.err_network', 'Erreur réseau'), 'error'); }
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
          if (!r.ok) toast(j.error || T('plugin.2110_io.err_del_flow', 'Erreur retrait flux'), 'error');
          else if (j.note) toast(j.note, 'info');
        } catch(e) { toast(T('plugin.2110_io.err_network', 'Erreur réseau'), 'error'); }
        await refresh();
      });
      const moreEl = body.querySelector('.io-addrow:not(.io2110-del-rx):not(.io2110-realign)');
      if (moreEl) {
        moreEl.onclick = async () => {
          moreEl.disabled = true;
          try {
            const r = await mtlMutate(`/api/mtl/${vmid}/activate`, {kind: 'rx'});
            if (r && !r.ok) { const j = await r.json().catch(()=>({})); toast(j.error || T('plugin.2110_io.err_rx_enable', 'Erreur activation RX'), 'error'); }
          } catch(e) { toast(T('plugin.2110_io.err_network', 'Erreur réseau'), 'error'); }
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
            if (!r.ok) { const j = await r.json().catch(()=>({})); toast(j.error || T('plugin.2110_io.err_rx_remove', 'Erreur retrait RX'), 'error'); }
          } catch(e) { toast(T('plugin.2110_io.err_network', 'Erreur réseau'), 'error'); }
          await refresh();
        };
      }
    }

    async function refresh(){
      let c, cs;
      try { c = await (await fetch(`/api/nmos/receivers/${vmid}/detail`)).json(); }
      catch(e){ body.innerHTML = '<div class="meta">' + esc(T('plugin.2110_io.nmos_detail_na', 'Détail NMOS indisponible.')) + '</div>'; return; }
      try { const cd = await (await fetch(`/api/nmos/senders/${vmid}/detail`)).json();
            cs = (cd && cd.length) ? cd[0] : null; }
      catch(e){ cs = null; }
      const recvs = (c && c.receivers) || [];
      _cachedRecvs = recvs;
      _nodePorts = (c && c.ports) || [];
      _pairs227  = (c && c.smpte_2022_7 && c.port_pairs) || [];
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
      const _xdpScope    = _nPorts > 1 ? ' · ' + window.t('js.io2110.all_nics') : '';
      const _hasB2 = (_xdpReserved != null) && (_xdpHwMax != null) && _xdpHwMax > 0;
      // Socle DPDK narrow (rl_active) : la barre « Queues XDP » n'a plus de sens (PF vfio, pas de
      // plafond AF-XDP) → files RX (RSS) réservées + « Sessions TX (RL) » sur le cap RL agrégé.
      const _hasRL = !!(c && c.rl_active && c.rl_tx_cap_total);
      let _nicXdpBar = '';
      if (_hasRL) {
        _nicXdpBar = _rssRow(c.rl_rx_queues, _xdpScope)
                   + _rlBar(c.rl_tx_sessions, c.rl_tx_cap_total, c.rl_tx_dropped, _xdpScope);
      } else if (_hasB2) {
        _nicXdpBar = _xdpBar(_xdpAct, _xdpPlanned, _xdpReserved, _xdpHwMax, _xdpScope);
      } else if (_xdpAlloc != null) {
        // Repli image pré-A2 (pas de `reserved`) : ancien rendu allocated/HW.
        const _xdpDen     = _xdpHwMax || _xdpAlloc;
        const _xdpUsedPct = _xdpDen ? Math.min(100, Math.round(_xdpAlloc / _xdpDen * 100)) : 0;
        const _xdpOver    = (_xdpHwMax != null) && (_xdpAlloc > _xdpHwMax);
        // Seuil propre à ce repli : l'alerte est à 85 % des files, pas à 60 % comme un débit.
        const _xdpEtat = (_xdpOver || _xdpUsedPct >= 100) ? 'over' : _xdpUsedPct > 85 ? 'warn' : '';
        const _xdpTxt  = (_xdpHwMax != null)
          ? (_xdpOver ? `${_xdpAlloc} / ${_xdpHwMax} files — SUR-CAPACITÉ (${_xdpAct} actives)`
                      : `${_xdpAlloc} / ${_xdpHwMax} files (${_xdpUsedPct}%)`)
          : `${_xdpAct} / ${_xdpAlloc} sessions`;
        _nicXdpBar = `<div class="nic-bar-wrap">
          <span class="nic-bar-lbl">Queues XDP</span>
          <span class="nic-bar-val" style="color:${_nicCouleur(_xdpEtat)}">${_xdpTxt}</span>
          <span class="ctl-gauge${_xdpEtat ? ' ' + _xdpEtat : ''}" role="img" aria-label="${esc(_xdpTxt)}"
            style="--ctl-gauge-w:100%;flex:1;--ctl-gauge-seuil:85%"><span class="ctl-gauge-fill"
            style="transform:scaleX(${(Math.min(100, _xdpUsedPct) / 100).toFixed(3)})"></span></span>
        </div>`;
      }
      _lastNicPorts = (c && c.nic_ports) || [];   // mémorisé pour reconstruire la bande au toggle (sans refetch)
      _cachedMeta = `<div class="meta rx-meta">${esc(T('plugin.2110_io.meta_ip', 'IP :'))} ${esc((c && c.ip) || '—')} — `
        + `${esc(T('plugin.2110_io.meta_sources', '{n} / {total} sources').replace('{n}', recvs.length).replace('{total}', _cachedVideoCount))} · `
        + `${esc(T('plugin.2110_io.meta_subscribed', '{n} abonné(s)').replace('{n}', activeCount))}</div>${_nicH}${_nicRxBar}${_nicXdpBar}`;
      _cachedTxHtml    = renderTXSection(cs && cs.senders);
      _renderBody();
    }

    // Délégation : clic sur le poussoir IDENT → bascule l'incrustation de ce slot vidéo.
    // L'attente se dit par `disabled` + `aria-busy` (le poussoir refuse le geste ET l'annonce),
    // là où la classe `.busy` d'avant ne faisait que griser : au clavier, rien ne l'arrêtait.
    async function onClickIdent(e){
      const badge = e.target.closest('.io-ident');
      if (!badge || !body.contains(badge)) return;
      if (badge.disabled) return;
      const idx = parseInt(badge.dataset.idx, 10);
      const enable = badge.dataset.enable === '1';
      badge.disabled = true; badge.setAttribute('aria-busy', 'true');
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
        badge.disabled = false; badge.removeAttribute('aria-busy');
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
    function _schedIdent(idx, val){
      clearTimeout(_identThrottle[idx]);
      _identThrottle[idx] = setTimeout(() => _postIdentSize(idx, val), 120);
    }
    // Le GESTE du rotatif (glisser, molette, clavier, remise au défaut) est celui du CATALOGUE :
    // `MXLControls.attachKnobGestures` le lit dans les attributs du contrôle. Le plugin n'écoute
    // que le résultat — il sait ce que la valeur commande, le catalogue sait comment elle se règle.
    function onKnobInput(e){
      const k = e.target.closest('.io-identknob');
      if (!k || !body.contains(k)) return;
      _schedIdent(parseInt(k.dataset.idx, 10), e.detail.value);
    }

    // Délégation : clic sur le poussoir GÉN → bascule le générateur de ce slot.
    async function onClick(e){
      const badge = e.target.closest('.io-gen');
      if (!badge || !body.contains(badge)) return;
      if (badge.disabled) return;
      const essence = badge.dataset.essence;
      const idx = parseInt(badge.dataset.idx, 10);
      const enable = badge.dataset.enable === '1';
      badge.disabled = true; badge.setAttribute('aria-busy', 'true');
      try {
        const resp = await fetch(`/api/containers/${vmid}/control/gen`, {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({essence, idx, enabled: enable}),
        });
        if (!resp.ok) { const j = await resp.json().catch(()=>({})); throw new Error(j.error || ('HTTP ' + resp.status)); }
        badge.setAttribute('aria-pressed', String(enable));
        badge.dataset.enable = enable ? '0' : '1';
        setTimeout(refresh, 2500);   // le redéploiement côté container est asynchrone
      } catch(err) {
        toast('Échec du basculement du générateur : ' + err.message, 'error');
      } finally {
        badge.disabled = false; badge.removeAttribute('aria-busy');
      }
    }
    // Délégation : changement de pattern dans le <select> du tooltip GÉN.
    async function onPatternChange(e){
      const sel = e.target.closest('.io-patsel');
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
    // Une RÉVÉLATION, pas un interrupteur : d'où `aria-expanded` sur un bouton de commande, et
    // non un poussoir à accrochage. Le crochet est nommé `io-nictoggle` et NON `io2110-nictoggle`
    // à dessein : ce dernier est restylé par la feuille embarquée de static/io2110.js, injectée
    // APRÈS le socle sur la page /io — elle aurait silencieusement repris la main sur `.btn`.
    function onNicToggle(e){
      const btn = e.target.closest('.io-nictoggle');
      if (!btn || !body.contains(btn)) return;
      _nicOpen = !_nicOpen;
      _renderBody();
    }
    // Délégation : changement de PORT (NIC) d'un slot — épinglage / retour à l'auto (multi-NIC).
    async function onPortChange(e){
      const sel = e.target.closest('.io-portsel');
      if (!sel || !body.contains(sel)) return;
      const role = sel.dataset.role;
      const idx = parseInt(sel.dataset.idx, 10);
      // Tous les flux de l'ensemble, épinglés en une opération (cf. portSelector).
      const idxs = String(sel.dataset.idxs || idx).split(',').map(Number).filter(n => !isNaN(n));
      const iface = sel.value;   // '' = Auto (répartition)
      sel.disabled = true;
      try {
        const resp = await fetch(`/api/mtl/${vmid}/pin`, {
          method: 'POST', headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({role, idx, idxs, iface}),
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
    // Certains devices 2110 (ex. convertisseurs SDI→IP) exposent une vraie API NMOS IS-04/05 mais
    // n'émettent rien tant qu'on ne PATCH pas leur sender (master_enable) — l'abonnement RX ici est
    // manuel (SDP collé, pas de registry commun) donc ce PATCH ne part jamais tout seul de façon
    // fiable (mDNS/découverte peu fiables sur ce matériel). Bouton = déclenchement à la demande.
    async function _activateRemoteSender(essence, idx, btn){
        const st = document.getElementById('rx-sdp-status');
        const prevText = btn.textContent;
        btn.disabled = true; btn.textContent = 'Activation…';
        try {
            const resp = await fetch(`/api/nmos/receivers/${vmid}/${idx}/activate_sender`, {
                method: 'POST', headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ essence }),
            });
            const j = await resp.json().catch(()=>({}));
            if (st) {
                if (j.ok) {
                    st.textContent = j.already_active ? 'Sender distant déjà actif' : 'Sender distant activé (IS-05)';
                    st.style.color = 'var(--status-running-fg)';
                } else {
                    st.textContent = '✕ ' + (j.error || 'échec');
                    st.style.color = 'var(--status-stopped-fg)';
                }
            }
        } catch(err) {
            if (st){ st.textContent = '✕ ' + err.message; st.style.color = 'var(--status-stopped-fg)'; }
        } finally {
            btn.disabled = false; btn.textContent = prevText;
        }
    }
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
                    <button class="btn" id="rx-sdp-activate" title="Force le PATCH master_enable:true sur le sender NMOS distant correspondant à ce SDP (matériel qui ne s'active pas tout seul)">Activer IS-05 (sender distant)</button>
                    <button class="btn btn-red" id="rx-sdp-unsub">Se désabonner</button>
                    <button class="btn btn-green" id="rx-sdp-apply">Appliquer</button>
                </div>
            </div>`;
        document.body.appendChild(modal);
        modal.addEventListener('click', e => { if (e.target === modal) _closeSdpModal(); });
        document.getElementById('rx-sdp-close').addEventListener('click', _closeSdpModal);
        document.getElementById('rx-sdp-apply').addEventListener('click', () => _sdpApply(essence, idx, true));
        document.getElementById('rx-sdp-unsub').addEventListener('click', () => _sdpApply(essence, idx, false));
        document.getElementById('rx-sdp-activate').addEventListener('click', e => _activateRemoteSender(essence, idx, e.currentTarget));
        document.getElementById('rx-sdp-ta').focus();
    }
    function onClickSdp(e){
        const badge = e.target.closest('.io-sdp');
        if (!badge || !body.contains(badge)) return;
        _openSdpModal(badge.dataset.essence || 'video', parseInt(badge.dataset.idx, 10));
    }

    body.addEventListener('click', onClick);
    body.addEventListener('click', onClickIdent);
    body.addEventListener('click', onClickSdp);
    body.addEventListener('click', onNicToggle);
    body.addEventListener('change', onPatternChange);
    body.addEventListener('change', onPortChange);
    body.addEventListener('ctl-knob-input', onKnobInput);   // émis par le catalogue

    // Changement de niveau de libellé (barre de navigation) → re-rendu immédiat, sans attendre
    // le prochain tick de 5 s : le sélecteur doit répondre à la volée.
    if (this._onLabelLevel) document.removeEventListener('source-labels:change', this._onLabelLevel);
    this._onLabelLevel = () => refresh();
    document.addEventListener('source-labels:change', this._onLabelLevel);

    refresh();
    if (this._timers[vmid]) clearInterval(this._timers[vmid]);
    this._timers[vmid] = setInterval(refresh, 5000);
  },

  unmount(vmid){
    if (vmid != null) { clearInterval(this._timers[vmid]); delete this._timers[vmid]; }
    else { Object.values(this._timers).forEach(clearInterval); this._timers = {}; }
    if (this._onLabelLevel) {
      document.removeEventListener('source-labels:change', this._onLabelLevel);
      this._onLabelLevel = null;
    }
  }
};

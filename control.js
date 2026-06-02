// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Panneau de contrôle minimal (Phase A). Sera étendu en Phase C (sources/multicast MTL).
window.MXLPlugins = window.MXLPlugins || {};
window.MXLPlugins.receiver_2110_mtl = {
  mount(el, vmid, ctx) {
    const slots = el.querySelector('#mtl-recv-slots');
    if (slots) slots.textContent = 'Container #' + vmid + ' — métriques fps sur :8080.';
  },
  unmount() {}
};

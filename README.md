# 2110_io — moteur ST 2110 de Bobi.Studio

*[English version](README.en.md)*

Moteur **bi-rôle** SMPTE ST 2110 : réception **et** émission dans une seule instance, sur la
même carte réseau, via la [Media Transport Library](https://github.com/OpenVisualCloud/Media-Transport-Library)
(MTL/DPDK) en kernel-bypass. Les flux `st20p` / `st30p` sont écrits et lus en **zéro-copie**
dans le bus mémoire partagée [MXL](https://github.com/dmf-mxl/mxl).

Composant de [Bobi.Studio](https://github.com/bob-integration/bobistudio).

---

## Ce qu'il fait

- **RX et TX sur la même carte** — une instance par nœud porte tous les slots de réception et
  d'émission, vidéo, audio et ANC. C'est ce qui permet de n'immobiliser qu'une NIC.
- **Flux composables** : chaque slot RX ou TX se câble indépendamment vers le bus MXL, sans
  redéployer le moteur.
- **Classes d'émission narrow et wide** (ST 2110-21), avec le `TP=` du SDP qui suit la classe
  réellement appliquée.
- **Chaîne entrelacée champ-natif**, sans passage par une trame progressive intermédiaire.
- **NMOS IS-04 / IS-05** : chaque slot est enregistré comme Receiver ou Sender, et se connecte
  depuis n'importe quel contrôleur.
- **Mesure 2110 publiée** sur son port de métriques : `fpt`, `VRX`, `Cinst`, le verdict de
  conformité et l'état PTP — de quoi vérifier une chaîne sans analyseur externe.

Le matériel qualifié est la **NIC Intel E810**, en PF, DPDK ou AF-XDP. C'est la seule sur
laquelle il ait tourné.

---

## Les correctifs à libmtl

`docker/` contient **dix-neuf correctifs** appliqués à la Media Transport Library au moment de
construire l'image. Ce ne sont pas des ajustements de confort : chacun corrige un comportement
qui, en production broadcast, se voit à l'antenne. Quelques-uns, pour donner le ton :

| Correctif | Ce qu'il répare |
|---|---|
| `tx_reset_no_drop` | une réinitialisation TX ne doit pas jeter de trame |
| `afxdp_tx_link_drop` | vrai *hitless* 2022-7 : couper l'émission quand le lien tombe, pas après |
| `tx_builder_famine_recovery` | le constructeur de trames se relève d'une famine au lieu de rester bloqué |
| `ptp_adjust_freq` | asservissement en **fréquence** du PHC — le servo n'était jamais compilé |
| `ptp_gm_export` | exposer le delta PHC↔GM signé, seule mesure qui vaille une fois le PHC asservi |
| `rx_resetting_guard` / `tx_hang_resetting_guard` | ne pas confondre « en cours de reset » et « mort » |
| `igmp_router_alert` | l'option Router Alert, exigée par certains commutateurs pour l'IGMP |

Ils sont lisibles : chacun est un script Python qui décrit ce qu'il insère et pourquoi.

---

## L'utiliser

Ce dépôt est un **plugin** de Bobi.Studio, monté dans `plugins/2110_io/`. Il se déploie depuis
l'orchestrateur, sur un nœud enrôlé — jamais à la main. `help.md` est l'article d'aide rendu
dans le produit : il détaille le dimensionnement des slots, les pièges de sur-souscription et
la lecture des compteurs.

Il ne s'utilise pas seul : il lit sa configuration et son câblage dans l'orchestrateur.

---

## Bancs

```bash
python3 tools/churn_bench.py --engine <ip> --slots 15,16,17 --rounds 10
python3 tools/tx_scale_bench.py
```

À lancer à la main contre un moteur en marche — ils ont besoin d'une vraie carte et de vrais
flux, et n'ont donc pas leur place en intégration continue.

---

## Licence

GPL-3.0-or-later — voir [LICENSE](LICENSE). Copyright © 2026 BOBI SAS, France.

La Media Transport Library est publiée par Intel sous BSD-3-Clause ; les correctifs de
`docker/` s'appliquent à ses sources au moment du build et ne la redistribuent pas.

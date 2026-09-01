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

L'image applique à la Media Transport Library un jeu de correctifs, au moment du build. Ils
portent sur des comportements qui, en production broadcast, se voient à l'antenne : discipline
en fréquence du PHC, coupure d'émission au bon moment pour un vrai *hitless* 2022-7,
réinitialisation TX sans perte de trame, option Router Alert pour l'IGMP.

Ils vivent dans `docker/`, un script par correctif, et chacun décrit ce qu'il insère et
pourquoi. À savoir si vous comparez des versions : l'image n'embarque pas une `libmtl`
d'origine.

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

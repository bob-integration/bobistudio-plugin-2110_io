# I/O ST 2110 (moteur MTL)

Moteur ST 2110 **bi-rôle** (réception **et** émission) basé sur la **Media Transport Library**
(MTL/DPDK, kernel-bypass). Un binaire C natif (libmtl) écrit/lit les flux `st20p`/`st30p` en
**zéro-copie** dans le ring shared memory (MXL, `/dev/shm`). C'est la variante **haute performance**
de `receiver_2110`/`sender_2110` : mêmes shm produits, mais une seule instance fait RX + TX sur la
même NIC E810. Exposé en NMOS IS-04/05 (rôles receiver **et** sender).

## Déployer

`2110_io` est **Docker-only** : il se déploie sur un **nœud** (jamais en LXC), via
**Réglages → Nœuds & Matériel → Déploiement → Nœuds**. Le nœud doit être compatible MTL
(NIC **Intel E810** en PF, DPDK/AF-XDP) et avoir le pool SR-IOV initialisé (voir l'article *SR-IOV*).

1. Sélectionner le nœud et créer le conteneur `2110_io` (une seule instance par nœud suffit : elle
   porte tous les slots RX et TX).
2. Configurer le nombre de slots (vidéo / audio / ANC en réception, et slots TX en émission) et le
   nombre de slots **actifs** simultanés.
3. Déployer — le moteur démarre en **simulation par slot** tant qu'aucune source n'est activée.

## Réception (RX)

Chaque slot RX produit un shm consommable par tout le pipeline :

| Essence | shm produit |
|---------|-------------|
| Vidéo 2110-20 | `<hostname>_0`, `<hostname>_1`… |
| Audio 2110-30 | `<hostname>_audio_0`… |
| ANC 2110-40 | `<hostname>_anc_0`… |

Une source réelle est abonnée par un contrôleur NMOS (PATCH IS-05 avec un SDP) ou en activant un
slot. Sans source, le slot reste en **simulation** (mire / silence).

## Émission (TX)

Les **slots TX** lisent un shm du pipeline (câblé depuis la page **Câbles**, colonne Destinations)
et émettent en RTP multicast. Chaque slot TX émet de façon couplée :

- **Vidéo 2110-20** — le shm câblé ;
- **Audio 2110-30 ×2** — suit automatiquement le shm vidéo (`<hostname>_0` → `…_audio_0/1`) ;
- **ANC 2110-40** — suit automatiquement (`<hostname>_0` → `…_anc_0`).

Les adresses multicast/ports sont **auto-allouées** au déploiement (pool multicast cluster-unique,
cf. Réglages → Cluster & Réseau) et modifiables à chaud via `POST /api/mtl/<vmid>/tx/<slot>/dest`.
Les senders NMOS IS-04 correspondants sont enregistrés automatiquement.

## Boutons par slot (à chaud, port :8082)

| Bouton | Côté | Action |
|--------|------|--------|
| **GEN** | RX | Bascule un slot entre la source réseau et un **générateur** local (mire / sine audio) |
| **IDENT** | RX | Incruste 3 lignes (nom + multicast + format) sur un slot vidéo |
| **GEN TX** | TX | Active un générateur de mire sur un slot de sortie |
| **IDENT TX** | TX | Incruste l'IDENT sur un slot de sortie |
| **TONE TX** | TX | Injecte une tonalité 1000 Hz sur l'audio d'un slot de sortie |

Ces actions sont **instantanées** (pas de redéploiement).

## Options

- **SMPTE 2022-7** (dual-path redondant) : émission/réception sur deux ports physiques distincts
  (paire de VFs déclarée).
- **Repli TX sans signal** : ce qu'émet un slot TX quand sa source est absente — *Coupé*,
  *Noir + silence* (défaut) ou *Mire + 1000 Hz*.
- **Slots actifs** : `active_rx_count` / `active_tx_count` bornent le nombre de sessions simultanées
  (budget de files AF_XDP de la NIC — voir l'article *SR-IOV*). Sur-souscrire sature le scheduler et
  fait chuter la cadence : garder la somme dans le budget de la carte.

## PTP & genlock

En mode MTL, **MTL gère le PTP nativement** via le PHC de la NIC E810 (précision sub-100 ns) —
`ptp4l`/`phc2sys` ne sont pas utilisés pour ce conteneur. La réception est calée sur le flux entrant,
l'émission sur la grille PTP (voir les articles *PTP* et *Synchronisation (Genlock)*).

## Prérequis

- Nœud avec NIC **E810** (PF, DPDK/AF-XDP) et pool **SR-IOV** initialisé sur ce nœud.
- **PTP** verrouillé sur le réseau.
- Réseau 2110 (plan média) séparé, configuré par-nœud (Réglages → Nœuds & Matériel → Réseau hôte).

## Notes

- Les UUID NMOS sont stables (registre de niveau cluster) : un recreate/restore ne casse pas les
  abonnements des contrôleurs externes.
- Les shm produits sont identiques à ceux de `receiver_2110` → interchangeables côté consommateurs.

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

### Latence de sortie : deux réglages par slot

Chaque sortie porte deux sélecteurs, à côté de son format.

**Choix de la trame émise.** Le moteur prépare quelques trames d'avance pour ne jamais laisser
le fil à sec. Le réglage dit laquelle il sert.

| | |
|---|---|
| ⚡ **Trame la plus récente** *(défaut)* | Émet la dernière trame prête et libère celles qu'elle a dépassées |
| ⏱ Trame la plus ancienne | Comportement historique : émet dans l'ordre d'arrivée |

Servir la plus récente **rend environ une image de latence**. La raison est une question de
calendrier : une source publie sa trame peu après le début de son créneau, alors que le transport
ne vient la chercher qu'un peu avant la fin — la trame fraîche est donc prête à temps, mais en
servant la plus ancienne on émettait une trame périmée pendant qu'elle attendait. Mesuré sur banc
le 2026-08-12 : l'âge du contenu chez le récepteur passe de 62,4 à 42,4 ms, sans perte de cadence.

Revenir à « la plus ancienne » n'a de sens que si une source **irrégulière** provoque des
répétitions : les trames dépassées étant libérées sans être émises, une source qui hoquette peut
laisser le moteur sans rien de frais à envoyer. Les deux témoins sont `repeats` (l'antenne
répète-t-elle ?) et `skipped` (combien de trames périmées libérées) dans les statistiques du slot.

**Rythme d'émission.** « Image suivante » aligne l'émission sur l'instant nominal — c'est le
réglage d'interopérabilité stricte. « Décalée » fait partir l'image dès que ses premières tranches
sont prêtes, avec le décalage déclaré au SDP (TROFF) et l'horodatage inchangé, donc sans effet sur
la synchronisation son/image. Il exige un peu de marge de tampon chez le récepteur.

⚠ Ces deux réglages **recréent la session** du slot concerné : bref silence à l'antenne sur cette
sortie. À ne pas enchaîner pendant une émission.

### Le budget d'une image, pour les étages en amont

L'émetteur vient chercher le contenu à un instant fixe du créneau : environ **16,4 ms** après son
début, à 50 images/s. Tout ce qu'un étage amont publie avant part à l'image suivante ; tout ce qui
arrive après attend une image de plus. La chaîne dispose donc d'un **budget d'environ 16 ms par
image**, et un étage ne coûte rien tant qu'il y reste — puis une image pleine dès qu'il le
franchit. C'est un escalier, pas une pente.

Conséquence utile au moment de dimensionner une chaîne : ajouter un traitement léger est gratuit,
et c'est le cumul qui compte, pas le nombre d'étages. Mesuré sur cette installation : la pyramide
et la réplication RDMA ne consomment rien, un correcteur de couleur 2 ms (deux en cascade
restent additifs), un multiview de 2 à 11 ms selon sa configuration — c'est lui qui fait
généralement basculer le budget. Le détail et la règle de vérification par mur sont dans l'aide
du **Multiviewer**, section « Latence ».

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

## Diagnostiquer un port en mode DPDK/vfio

Quand une interface média est passée en `pmd=dpdk` (`node_interfaces`), le port est lié à
**vfio-pci** : il **disparaît de `/sys/class/net`**. Conséquences : plus d'`ethtool`, plus de
`tcpdump`, plus de compteurs kernel sur ce port. Les compteurs viennent alors du **moteur**
(daemon MTL → `/tmp/mtl_ports.json` dans le conteneur, relayé sur `:8080` sous
`nic.ports[i].mtl_stats`) ; l'onglet Serveurs du Monitoring continue d'afficher le débit du
port (source `mtl`), et l'agent-nœud ≥ 0.15.0 remonte l'interface avec `state:"vfio"` au lieu
de la faire disparaître.

### Table d'équivalences (geste kernel → geste vfio)

| Avant (netdev/AF-XDP) | Après (vfio/DPDK) |
|---|---|
| `ethtool -S <if> \| grep rx_queue_N` (deltas par file — discriminant du playbook gel RX) | Compteurs **par port** : `nic.ports[i].mtl_stats` sur `:8080` (`rx_packets`/`rx_bytes` qui avancent = le port reçoit) + stats **par session** MTL : `receivers[]` de `:8080` (`frame_index`/`fps` par slot = l'équivalent « ma file avance »). Dans le conteneur : `cat /tmp/mtl_ports.json` deux fois à quelques secondes d'écart et comparer. |
| `ethtool -S <if> \| grep -i drop/error` | `mtl_stats.rx_err`, `tx_err`, `rx_hw_dropped`, `rx_nombuf` (nombuf qui monte = manque de mbufs → moteur sous-dimensionné, pas le réseau). |
| `ethtool -i <if>` (driver/firmware) | `lspci -vvs <BDF>` sur l'hôte (le BDF est dans `node_interfaces.pci`) : `Kernel driver in use: vfio-pci` confirme le binding ; firmware visible via `lspci` ou en relançant temporairement le driver `ice`. |
| `ethtool -T <if>` (capacités PTP) | Le PHC de la carte reste visible via le **port frère** de la même carte (PHC partagé sur E810 bi-port) : `ethtool -T` sur l'autre port. |
| `ip -s link show <if>` (lien/compteurs) | Lien : `nic.ports[i].link_up` sur `:8080` (état vu par le moteur). Compteurs : `mtl_stats`. |
| `tcpdump -i <if>` | **Impossible** sur le port (le kernel ne le voit plus). Alternatives : **port mirror sur le switch** vers une machine d'analyse ; stats RX **par session** sur `:8080` (fps, `frame_index`, `rx_latency_ms`, signal noir/figé) pour localiser le flux en cause ; en dernier recours, repasser le port en `af_xdp` (rebind kernel) le temps du diagnostic. |
| Join IGMP visible côté kernel (`ip maddr`) | Les joins sont émis par **MTL lui-même** (plus par le kernel) : vérifier côté **switch** (table snooping IGMP) que le groupe est bien appris sur le port. |

### Symptômes et gestes de recovery (inchangés)

- **Alerte « port vfio muet »** (Monitoring) : compteurs `mtl_stats` figés alors que des sessions
  sont actives. Vérifier d'abord la source amont et le switch (snooping IGMP), puis **redémarrer le
  moteur** (Containers → restart du conteneur `2110_io`) — le restart re-crée les sessions MTL et
  ré-émet les joins, comme dans le playbook gel RX historique.
- **Un seul slot RX muet** (les autres vivent) : problème de flux/abonnement, pas de port → stats
  par session sur `:8080`, SDP/IGMP côté source, comme avant.
- **Tout le port muet** (`rx_packets` figé) : lien (`link_up`), câble/SFP, config switch, binding
  vfio (`lspci -k`). Le restart moteur reste le geste de recovery de référence.
- Les hugepages, le budget lcores et les plafonds de sessions se diagnostiquent **comme avant**
  (rien ne passait par ethtool).

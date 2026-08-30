// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) 2026 BOBI SAS, France
// Auteur : Cyril Mazouer, pour le compte de BOBI SAS
// Distribué sous licence GNU GPL v3 (ou ultérieure) ; voir le fichier LICENSE.
//
// mtl_rx — receiver ST 2110 MULTI-SESSION via Media Transport Library (libmtl/DPDK).
//
// UN SEUL mtl_init (un PF = un MtlManager/XDP) héberge N sessions de tout type sur la même carte :
//   - VIDEO (st20p, ST 2110-20) → ring /dev/shm/{hn}_{idx} (YUV422 planar, conv 10→8, overlay IDENT)
//   - AUDIO (st30p, ST 2110-30) → ring /dev/shm/{hn}_audio_{idx} (chunks 1ms, L24 8ch, BIG-ENDIAN
//     wire-native : on écrit le payload TEL QUEL, zéro conversion — le pipeline MXL audio est en BE)
//   - DATA  (st40, ST 2110-40 ANC) → point d'extension prêt (non implémenté : pas de consommateur).
//
// Les sessions sont décrites par un fichier de config JSON (--config), écrit par le contrôleur.
// Compat : sans --config, les args legacy construisent 1 session vidéo (tests manuels).
//
// Layout shm identique à receiver_2110 : header 64o [u64 index][u64 ns] puis ring de slots.

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <math.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>   /* strcasecmp (POSIX) — parse de MTL_LOG_LEVEL (mtl_log_level_env) */
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <mtl/st_pipeline_api.h>
#include <mtl/st30_pipeline_api.h>
#include <mtl/st40_pipeline_api.h>
#include <mtl/st40_api.h>         /* ANC RFC 8331 : st40_set/get_udw, add_parity_bits, calc_checksum */
#include <mtl/st_convert_api.h>   /* mode tranche : conversions SIMD RFC4175↔planar PAR BANDE */
#include <json-c/json.h>

/* ── MXL (Media eXchange Layer) ── le moteur produit/consomme des FLOWS MXL (grains vidéo/data,
 * samples audio) au lieu des rings shm maison. Voir bobimxl.py côté Python (mêmes conventions). */
#include <mxl/mxl.h>
#include <mxl/flow.h>
#include <mxl/time.h>
#include <mxl/rational.h>
#define OPENSSL_SUPPRESS_DEPRECATED   /* SHA1_* sont « deprecated » en OpenSSL 3 mais valides */
#include <openssl/sha.h>

#define MAX_FB    64
/* Registre de sessions libmtl (RX + TX, toutes essences). Une session = un (mcast,port) distinct
 * (les RX même-source sont fan-outés en UNE session à N cibles, cf. MAX_TG). DOIT couvrir le pire
 * cas ACTIF : N RX vidéo distincts + audio/ANC RX + slots TX. À 16 il était PILE rempli par 16 RX
 * vidéo → les sessions TX (traitées après) étaient rejetées (« registre plein ») → sortie TX muette.
 * `reg` est statique (BSS) ⇒ ~18 Kio/sess, l'agrandir est quasi gratuit. 128 couvre 40+ entrées
 * distinctes + audio + TX avec marge. */
#define MAX_SESS  128
#define MAX_TG    16   /* cibles (shm de sortie) par session : fan-out même-source → N slots */

/* TÉMOIN DE REPLI (bobi.studio) : au-delà de ce délai sans grain SOURCE frais, le WORKER (pas le
 * callback lcore) se met à publier lui-même un témoin (noir légal + carré qui alterne de position)
 * dans le ring TX normal, exactement comme le fait la branche statique — cf. video_tx_slice_thread.
 * Réglage à remonter dans les Réglages. */
#define SL_FALLBACK_AFTER_NS   (2ull * 1000000000ull)   /* 2 s sans grain frais ⇒ repli clignotant */
/* Demi-période d'alternance de la position du carré témoin (cycle complet ≈ 2×). Réglage à
 * remonter dans les Réglages. */
#define SL_FALLBACK_TOGGLE_NS  (500ull * 1000000ull)     /* ~500 ms par position */

/* ── ANC / ST 2110-40 (data) ── le grain MXL porte une charge utile ANC.
 *
 * FORMAT NORMATIF (depuis 2026-07-12) : **RFC 8331** → interopérable. Le format MAISON
 * historique ([u32 meta_num][u32 udw_fill][meta×16][udw]) n'était compris QUE de nous : un
 * consommateur MXL stock le parsait comme du RFC 8331, en déduisait « ANC count: 0 » et
 * concluait SANS ERREUR que le flux ne portait aucun ANC → PERTE SILENCIEUSE du timecode/tally
 * (banc croisé, cf. MXL_INTEROP.md). Contrairement au planar (vrai gain CPU sur des trames de
 * plusieurs Mo), ce format maison n'achetait RIEN : un grain ANC fait 4 Ko.
 *
 * Layout du grain (identique à bobimxl.anc_pack_rfc8331, validé octet pour octet contre libmtl) :
 *   [u16 Length BE][u8 ANC_Count][2 b F + 6 b rsvd][u16 rsvd]      ← en-tête 6 o (pas d'ESN :
 *                                                                     c'est un champ RTP)
 *   puis les paquets, chacun multiple de 4 o (l'alignement 32 b du RFC se compte depuis le
 *   payload RTP, qui portait 2 o d'ESN de plus → dans le grain : octets 6, 10, 14…) :
 *     [1 b C][11 b Line][12 b Hori][1 b S][7 b StreamNum]   (32 b)
 *     puis un flux de mots de 10 bits : DID, SDID, Data_Count, UDW×DC, Checksum_Word
 *     (parité SMPTE 291 + checksum via les primitives libmtl), bourré à 32 b.
 *
 * On LIT encore l'ancien format (ANC_FMT_BOBI_V1) le temps que la flotte se migre : le codage
 * est ANNONCÉ par le producteur dans son flowDef (`bobi_anc_format`). */
#define ANC_SLOT     8192u    /* taille d'un slot ANC (sérialisation bornée) */
#define ANC_MAX_UDW  4000u    /* buffer UDW max par frame (octets, 1 o = 1 UDW low8) */
#define ANC_HDR_BYTES 6u      /* en-tête du grain RFC 8331 (Length + ANC_Count + F/rsvd) */
/* Ancien format maison — LECTURE SEULE (compat flotte mixte, ne plus produire). */
struct anc_meta_rec { uint16_t did, sdid, line, hori, udw_size, udw_offset, c, s; };  /* 16 o */

/* Taille (octets) d'un paquet ANC RFC 8331 : 1er chunk 32 b + (3 + udw + checksum) mots de
 * 10 bits, arrondi au multiple de 4 o. */
static inline size_t anc_elem_bytes(uint16_t udw_size) {
  uint32_t bits = (uint32_t)(3 + udw_size + 1) * 10u;
  return 4u + ((bits + 31u) / 32u) * 4u;
}

/* Sérialise un frame st40 (meta[] + udw) en un grain RFC 8331. Renvoie les octets écrits, ou 0
 * si ça ne tient pas (le grain est alors laissé « 0 paquet », jamais de débordement). */
static size_t anc_pack_rfc8331(uint8_t* dst, size_t dst_size, const struct st40_frame_info* f) {
  if (dst_size < ANC_HDR_BYTES) return 0;
  uint32_t mn = f->meta_num; if (mn > ST40_MAX_META) mn = ST40_MAX_META;
  size_t off = ANC_HDR_BYTES;
  uint32_t written = 0;
  for (uint32_t m = 0; m < mn; m++) {
    const struct st40_meta* md = &f->meta[m];
    uint16_t n = md->udw_size;
    size_t need = anc_elem_bytes(n);
    if (off + need > dst_size) break;                  /* tronque proprement plutôt que déborder */
    memset(dst + off, 0, need);
    struct st40_rfc8331_payload_hdr* p = (struct st40_rfc8331_payload_hdr*)(dst + off);
    p->first_hdr_chunk.c = md->c;
    p->first_hdr_chunk.line_number = md->line_number;
    p->first_hdr_chunk.horizontal_offset = md->hori_offset;
    p->first_hdr_chunk.s = md->s;
    p->first_hdr_chunk.stream_num = md->stream_num;    /* enfin porté (l'ancien format le PERDAIT) */
    p->second_hdr_chunk.did = st40_add_parity_bits(md->did);
    p->second_hdr_chunk.sdid = st40_add_parity_bits(md->sdid);
    p->second_hdr_chunk.data_count = st40_add_parity_bits(n);
    p->swapped_first_hdr_chunk = htonl(p->swapped_first_hdr_chunk);
    p->swapped_second_hdr_chunk = htonl(p->swapped_second_hdr_chunk);
    /* UDW à partir de l'index 3 DANS second_hdr_chunk (0,1,2 = DID/SDID/DC) → bit 62, contigu. */
    const uint8_t* udw = f->udw_buff_addr + md->udw_offset;
    for (uint16_t i = 0; i < n; i++)
      st40_set_udw(i + 3, st40_add_parity_bits(udw[i]), (uint8_t*)&p->second_hdr_chunk);
    st40_set_udw(n + 3, st40_calc_checksum(3 + n, (uint8_t*)&p->second_hdr_chunk),
                 (uint8_t*)&p->second_hdr_chunk);
    off += need; written++;
  }
  size_t body = off - ANC_HDR_BYTES;
  dst[0] = (uint8_t)(body >> 8); dst[1] = (uint8_t)(body & 0xff);   /* Length (BE) */
  dst[2] = (uint8_t)written;                                        /* ANC_Count */
  dst[3] = 0; dst[4] = 0; dst[5] = 0;                               /* F=0 + réservés */
  return off;
}

/* Décode un grain RFC 8331 → meta[] + udw du frame st40 de TX. Renvoie le nombre de paquets. */
static uint32_t anc_unpack_rfc8331(const uint8_t* src, size_t src_size,
                                   struct st40_frame_info* f, uint32_t max_udw) {
  if (src_size < ANC_HDR_BYTES) return 0;
  size_t body = ((size_t)src[0] << 8) | src[1];
  uint32_t count = src[2];
  if (count > ST40_MAX_META) count = ST40_MAX_META;
  if (ANC_HDR_BYTES + body > src_size) return 0;                    /* Length incohérent */
  size_t off = ANC_HDR_BYTES;
  uint32_t fill = 0, n_out = 0;
  for (uint32_t m = 0; m < count; m++) {
    if (off + 8 > src_size) break;
    struct st40_rfc8331_payload_hdr p;
    memcpy(&p, src + off, sizeof(p));
    p.swapped_first_hdr_chunk = ntohl(p.swapped_first_hdr_chunk);
    p.swapped_second_hdr_chunk = ntohl(p.swapped_second_hdr_chunk);
    uint16_t n = p.second_hdr_chunk.data_count & 0xff;              /* 8 bits utiles (b8/b9=parité) */
    size_t need = anc_elem_bytes(n);
    if (off + need > src_size || fill + n > max_udw) break;
    struct st40_meta* md = &f->meta[n_out];
    md->c = p.first_hdr_chunk.c;
    md->line_number = p.first_hdr_chunk.line_number;
    md->hori_offset = p.first_hdr_chunk.horizontal_offset;
    md->s = p.first_hdr_chunk.s;
    md->stream_num = p.first_hdr_chunk.stream_num;                  /* préservé */
    md->did = p.second_hdr_chunk.did & 0xff;
    md->sdid = p.second_hdr_chunk.sdid & 0xff;
    md->udw_size = n;
    md->udw_offset = fill;
    /* Relire les UDW depuis le buffer SOURCE (l'en-tête local `p` a été byte-swappé). */
    const uint8_t* chunk = src + off + 4;                           /* &second_hdr_chunk */
    for (uint16_t i = 0; i < n; i++)
      f->udw_buff_addr[fill + i] = (uint8_t)(st40_get_udw(i + 3, (uint8_t*)chunk) & 0xff);
    fill += n; n_out++; off += need;
  }
  f->meta_num = n_out;
  f->udw_buffer_fill = fill;
  return n_out;
}

/* (anc_flow_is_rfc8331 est défini plus bas : il a besoin de l'instance MXL globale g_mxl.) */

/* Décodage de l'ANCIEN grain maison (lecture seule, flotte en migration). */
static uint32_t anc_unpack_bobi_v1(const uint8_t* src, size_t src_size,
                                   struct st40_frame_info* f, uint32_t max_udw) {
  if (src_size < 8) return 0;
  uint32_t mn = ((const uint32_t*)src)[0];
  uint32_t fill = ((const uint32_t*)src)[1];
  if (mn > ST40_MAX_META) return 0;
  if (fill > max_udw) fill = max_udw;
  if (8 + (size_t)mn * sizeof(struct anc_meta_rec) + fill > src_size) return 0;
  const struct anc_meta_rec* mr = (const struct anc_meta_rec*)(src + 8);
  for (uint32_t m = 0; m < mn; m++) {
    struct st40_meta* md = &f->meta[m];
    md->did = mr[m].did; md->sdid = mr[m].sdid;
    md->line_number = mr[m].line; md->hori_offset = mr[m].hori;
    md->udw_size = mr[m].udw_size; md->udw_offset = mr[m].udw_offset;
    md->c = mr[m].c; md->s = mr[m].s; md->stream_num = 0;   /* perdu par ce format */
  }
  if (fill && f->udw_buff_addr)
    memcpy(f->udw_buff_addr, src + 8 + (size_t)mn * sizeof(struct anc_meta_rec), fill);
  f->meta_num = mn;
  f->udw_buffer_fill = fill;
  return mn;
}

enum sess_kind { K_VIDEO, K_AUDIO, K_DATA };   /* DATA = ST 2110-40 ANC (passthrough + timecode) */
enum sess_role { ROLE_RX, ROLE_TX };           /* RX = wire→shm (receiver) ; TX = shm→wire (sender) */

static volatile int g_stop = 0;
static void on_signal(int s) { (void)s; g_stop = 1; }

/* bobi.studio: fenêtre de grâce du backstop « TX FIGÉ » après une création de session TX.
 * Créer un sender RL commit l'arbre traffic-manager (rte_tm_hierarchy_commit) → le PMD ice STOPPE
 * tout le port ~qq s → les sessions TX déjà vives ne transmettent plus le temps du commit (leur
 * alive_ns stagne). Ce N'EST PAS un wedge : la garde libmtl (patch_tx_hang_resetting_guard) évite le
 * fatal_error, mais le backstop process-wide de mtl_rx.c, lui, redémarrerait le daemon (→ re-lock PTP,
 * blip flotte, voire boucle car re-lock 2 min > seuil 5 s). On suspend donc le backstop TX_ADD_GRACE_NS
 * après tout create TX : l'émission reprend seule à la fin du commit. Le vrai détecteur (lien/queue
 * mort HORS ajout) reste actif dès la grâce écoulée. */
#define TX_ADD_GRACE_NS (20ull * 1000000000ull)
static uint64_t g_tx_add_grace_ns = 0;   /* mono_ns jusqu'auquel le backstop TX est suspendu */

/* bobi.studio: gate d'ÉTAT du backstop TX quand le PTP interne libmtl est actif (G4 2026-07-10/11).
 * Le train de pacing TX attend le PTP stable (mt_ptp_wait_stable, 180 s) et ÉCHOUE au timeout →
 * zéro frame tant que le GM n'est pas là. Une grâce à durée fixe ne couvre pas « GM absent au
 * boot » (la boucle de restart revient à l'expiration) : on suspend le backstop tant que le PTP
 * n'est PAS synchrone (getter exporté par patch_ptp_stable_getter.py — même critère de stabilité
 * que mt_ptp_wait_stable). GM absent → le daemon attend sans churn (log « not connected » 10 s) ;
 * lock → TX démarre et le backstop se réarme pour son vrai rôle (queue morte). */
extern bool mt_bobi_ptp_stable(void* impl, int port);
/* bobi.studio: grandmaster PTP interne libmtl (patch_ptp_gm_export) → publié dans mtl_ports.json
 * pour que l'orchestrateur construise a=ts-refclk:ptp du SDP TX quand ptp4l kernel est absent. */
extern bool mt_bobi_ptp_gm(void* impl, int port, unsigned char* out_id8, int* out_domain, int* out_utc);
/* bobi.studio: métriques PTP internes libmtl en ns (patch_ptp_offset_getter) → publiées dans
 * mtl_ports.json pour l'onglet « Réseau 2110 - PTP » quand ptp4l kernel est absent (socle DPDK).
 * TROIS mesures distinctes : offset BRUT (stat_delta, pilote ptp->locked, ~1,3 µs sur E810 DPDK car
 * discipline HW non convergente), offset CORRIGÉ (correct_delta ~31 ns = « offset from master » à
 * afficher) et mean path delay (~168 ns, le champ qui manquait). true SSI mesuré. */
extern bool mt_bobi_ptp_offset(void* impl, int port, long long* out_ns);
extern bool mt_bobi_ptp_correct_offset(void* impl, int port, long long* out_ns);
extern bool mt_bobi_ptp_path_delay(void* impl, int port, long long* out_ns);
/* Dernier delta PHC<->GM SIGNE : « offset from master » une fois le PHC asservi en frequence
 * (cf. patch_ptp_adjust_freq). L'offset corrige en LOGICIEL n'a plus d'objet dans ce regime. */
extern bool mt_bobi_ptp_last_delta(void* impl, int port, long long* out_ns);
static int g_engine_ptp = 0;             /* 1 = ENGINE_PTP=libmtl actif (PTP interne sur port DPDK) */

static uint64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* Horloge monotone (mesure d'écarts insensible aux sauts NTP/PTP de l'horloge système). */
static uint64_t mono_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* bobi.studio (0.44.2, volet 4) : les threads qui drainent le RX temps réel (audio_rx_thread en
 * particulier, mais aussi vidéo/ANC) partagent le CPU avec le controller Python + les lcores
 * busy-poll MTL sur un cpuset étroit → des hoquets d'ordonnancement CFS ≥4 ms font déborder le
 * pool RX audio (framebuff_cnt=4, cf. volet 2). SCHED_FIFO leur donne priorité sur le CFS (les
 * lcores busy-poll DPDK ne sont, eux, PAS gérés par ce processus — pur user-space EAL). Nécessite
 * CAP_SYS_NICE (conteneur --privileged l'a déjà, cf. docker_driver._build_run_cmd) ; sans elle,
 * pthread_setschedparam échoue EPERM → on continue en CFS (dégradé, pas fatal) et on logue UNE
 * SEULE fois pour tout le process (pas par session/thread, pour ne pas spammer si plusieurs
 * sessions RX démarrent sans la capacité). */
static volatile int g_schedfifo_warned = 0;
static void rt_thread_priority(const char* who) {
  struct sched_param sp; memset(&sp, 0, sizeof(sp));
  sp.sched_priority = 10;
  int e = pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp);   /* renvoie l'erreur, ne pose PAS errno */
  if (e != 0) {
    if (__sync_bool_compare_and_swap(&g_schedfifo_warned, 0, 1)) {
      fprintf(stderr, "mtl_rx: SCHED_FIFO refusé (%s, errno=%d %s) — threads en CFS, "
                       "prévoir CAP_SYS_NICE (+ ulimit rtprio) sur le conteneur moteur\n",
              who, e, strerror(e));
    }
  }
}

/* Timestamp média de la frame → TAI ns ABSOLU (instant de capture, horloge PTP commune A/V).
 * Le RX libmtl fournit en pratique tfmt=MEDIA_CLK : un compteur 32 bits qui WRAPPE (~47721 s à
 * 90 kHz, ~89478 s à 48 kHz). st10_media_clk_to_ns ne donne que la position DANS la fenêtre →
 * vidéo (90k) et audio (48k) ne sont alors PAS comparables. On reconstruit l'absolu en recollant
 * la fenêtre courante à partir de l'horloge PTP (mtl_ptp_read_time_raw = TAI ns), la même qui
 * cadence les media-clocks ST 2110-10. tfmt=TAI → renvoyé tel quel. */
static uint64_t media_ts_to_tai(mtl_handle st, enum st10_timestamp_fmt tfmt,
                                uint64_t ts, uint32_t rate) {
  if (tfmt == ST10_TIMESTAMP_FMT_TAI) return ts;
  uint64_t within = st10_media_clk_to_ns((uint32_t)ts, rate);     /* ns dans [0, window) */
  uint64_t window = (uint64_t)(((double)(1ULL << 32) * 1e9) / (double)rate);
  uint64_t ptp = mtl_ptp_read_time_raw(st);                       /* TAI ns courant */
  if (!window) return ptp;
  uint64_t tai = (ptp / window) * window + within;
  if      ((int64_t)(tai - ptp) >  (int64_t)(window / 2)) tai -= window;  /* fenêtre précédente */
  else if ((int64_t)(ptp - tai) >  (int64_t)(window / 2)) tai += window;  /* fenêtre suivante */
  return tai;
}

/* ═══ MXL ═══ instance globale (un domaine = un sous-rép. tmpfs partagé avec les consommateurs).
 * Domaine surchargeable par MXL_DOMAIN (isole un banc). Créée dans main(), à vie. */
static mxlInstance g_mxl = NULL;

/* Codage ANC ANNONCÉ par le producteur (`bobi_anc_format` du flowDef) → flotte MIXTE pendant la
 * migration. Champ absent = producteur pas encore migré → ancien format maison. (Défini ici et
 * pas dans le bloc ANC plus haut : a besoin de g_mxl.) */
static int anc_flow_is_rfc8331(const char* flow_id) {
  char buf[8192];
  size_t sz = sizeof(buf);
  if (mxlGetFlowDef(g_mxl, flow_id, buf, &sz) != MXL_STATUS_OK) return 0;
  buf[sizeof(buf) - 1] = 0;
  const char* k = strstr(buf, "bobi_anc_format");
  if (!k) return 0;
  const char* v = strstr(k, "rfc8331");
  return (v && (size_t)(v - k) < 40) ? 1 : 0;
}

/* ── Ports MTL (NIC) du device ── un mtl_init unique peut déclarer plusieurs NIC média
 * (MTL_PORT_MAX). Remplis à l'init, puis résolus PAR SESSION via le nom d'iface (multi-NIC :
 * chaque session vise la NIC qui porte physiquement son mcast — AF-XDP, pas d'auto-sélection). */
struct mtl_port_ent {
  char iface[64]; char portname[MTL_PORT_MAX_LEN];
  char pmd[16];   /* étiquette PMD du port ("af_xdp"|"dpdk"|"kernel") — publiée dans mtl_ports.json */
  enum st21_pacing profile;   /* CLASSE 2110-21 PAR PORT (#26) : cible VRX par session TX émise sur ce
                               * port. NARROW=0 (défaut memset) = comportement historique. C'est la
                               * manette produit (ops.transport_pacing par session), distincte du
                               * MÉCANISME device (mtl_init_params.pacing = st21_tx_pacing_way). */
};
static struct mtl_port_ent g_ports[MTL_PORT_MAX];
static int g_nports = 0;

/* Classe 2110-21 (profil d'émetteur) : chaîne config → enum st21_pacing. Défaut/inconnu = NARROW
 * (le plus strict, cf. DPDK_NARROW.md : une iface non configurée compte comme narrow). */
static enum st21_pacing parse_profile(const char* s) {
  if (!s || !s[0]) return ST21_PACING_NARROW;
  if (!strcmp(s, "wide"))                                   return ST21_PACING_WIDE;
  if (!strcmp(s, "narrow_linear") || !strcmp(s, "linear"))  return ST21_PACING_LINEAR;
  return ST21_PACING_NARROW;   /* "narrow" + repli */
}

/* Résout la CLASSE 2110-21 (ops.transport_pacing) de la session depuis sa NIC de sortie. iface vide
 * → port 0 (mono-NIC). iface inconnue → NARROW (le plus strict, jamais wide par accident). */
static enum st21_pacing resolve_profile(const char* iface) {
  if (g_nports <= 0) return ST21_PACING_NARROW;
  if (!iface || !iface[0]) return g_ports[0].profile;
  for (int k = 0; k < g_nports; k++)
    if (!strcmp(g_ports[k].iface, iface)) return g_ports[k].profile;
  return ST21_PACING_NARROW;
}

/* Nom de port MTL selon le PMD : native_af_xdp:<if> / kernel:<if> / <if> (PCI/dpdk). */
static void build_portname(const char* pmd, const char* iface, char out[MTL_PORT_MAX_LEN]) {
  if (!strcmp(pmd, "af_xdp"))      snprintf(out, MTL_PORT_MAX_LEN, "native_af_xdp:%s", iface);
  else if (!strcmp(pmd, "kernel")) snprintf(out, MTL_PORT_MAX_LEN, "kernel:%s", iface);
  else                             snprintf(out, MTL_PORT_MAX_LEN, "%s", iface);
}

/* Résout un nom d'iface vers le portname MTL initialisé. iface vide → port 0 (mono-NIC).
 * Renvoie l'index de port (≥0) ; -1 si l'iface est demandée mais inconnue du device → `out` reste
 * vide et l'appelant (reconcile) remonte `unknown_iface`. */
static int resolve_port(const char* iface, char out[MTL_PORT_MAX_LEN]) {
  out[0] = '\0';
  if (g_nports <= 0) return -1;
  if (!iface || !iface[0]) { snprintf(out, MTL_PORT_MAX_LEN, "%s", g_ports[0].portname); return 0; }
  for (int k = 0; k < g_nports; k++)
    if (!strcmp(g_ports[k].iface, iface)) { snprintf(out, MTL_PORT_MAX_LEN, "%s", g_ports[k].portname); return k; }
  return -1;
}

/* ── 2022-7 : état du LIEN physique par NIC ──────────────────────────────────────────────
 * Sans parade, émettre vers un lien mort est FATAL en AF_XDP : le driver ne draine plus le
 * ring XSK (`tx prod full`) → `st20_tx_queue_fatal_error` que libmtl ne sait pas récupérer
 * (« not dpdk user pmd, nothing to do ») ; pire, sa récupération interne (audio) recrée ses
 * mempools en boucle et FUIT des memzones DPDK jusqu'au plafond (2560) → TOUT le TX meurt,
 * même après rebranchement (banc 2026-07-06, nœud 30). La parade PRINCIPALE est dans libmtl
 * (patch bobi.studio patch_afxdp_tx_link_drop : port au lien mort ⇒ paquets jetés comme émis,
 * la session duale ne se fige jamais — vrai hitless). Ici on ne fait que JOURNALISER les
 * changements de lien (boucle principale) ; le backstop anti-wedge reste l'ultime filet. */
static int iface_carrier(const char* iface) {
  if (!iface || !iface[0]) return 1;
  char p[160]; snprintf(p, sizeof(p), "/sys/class/net/%s/carrier", iface);
  FILE* f = fopen(p, "r");
  if (!f) return 1;                    /* illisible (netns, nom exotique) → ne jamais geler à l'aveugle */
  int c = fgetc(f); fclose(f);
  return c == '0' ? 0 : 1;
}

/* Namespace UUIDv5 Bobi.Studio = uuid5(NAMESPACE_DNS, "mxl.bobi.studio") — DOIT être identique à
 * bobimxl._NS_BOBI (sinon les flux écrits ici ne sont pas trouvés par les consommateurs Python). */
static const uint8_t NS_BOBI[16] = {
  0xd4, 0xe7, 0x7c, 0xba, 0x0e, 0x52, 0x55, 0xd9, 0x82, 0xed, 0x72, 0x26, 0xb7, 0xbd, 0xa7, 0x57};

/* UUIDv5 (SHA-1) d'un NOM de flux → chaîne canonique (= bobimxl.flow_id). Le flowDef porte cet id ;
 * le lecteur ouvre le flux par ce même id. */
static void flow_id_str(const char* name, char out[37]) {
  SHA_CTX c; unsigned char h[20];
  SHA1_Init(&c);
  SHA1_Update(&c, NS_BOBI, 16);
  SHA1_Update(&c, name, strlen(name));
  SHA1_Final(h, &c);
  h[6] = (unsigned char)((h[6] & 0x0f) | 0x50);   /* version 5 */
  h[8] = (unsigned char)((h[8] & 0x3f) | 0x80);   /* variant RFC 4122 */
  snprintf(out, 37,
    "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
    h[0],h[1],h[2],h[3],h[4],h[5],h[6],h[7],h[8],h[9],h[10],h[11],h[12],h[13],h[14],h[15]);
}

/* Nom de flux = basename du shm_path historique (strip /dev/shm/). Le câblage orchestrateur
 * propage toujours des noms ({hn}_{idx}) → contrat inchangé. */
static const char* flow_name(const char* shm_path) {
  if (!strncmp(shm_path, "/dev/shm/", 9)) return shm_path + 9;
  return shm_path;
}

/* Cadence (double) → rational standard (grain_rate du flowDef + grille TAL). Miroir de to_st_fps :
 * les fps « drop » sont rendus en n/1001 pour une grille TAI exacte. */
static mxlRational fps_to_rational(double f) {
  mxlRational r = {25, 1};
  if      (fabs(f - 23.98) < 0.05) { r.numerator = 24000; r.denominator = 1001; }
  else if (fabs(f - 24.0)  < 0.05) { r.numerator = 24;    r.denominator = 1; }
  else if (fabs(f - 25.0)  < 0.05) { r.numerator = 25;    r.denominator = 1; }
  else if (fabs(f - 29.97) < 0.05) { r.numerator = 30000; r.denominator = 1001; }
  else if (fabs(f - 30.0)  < 0.05) { r.numerator = 30;    r.denominator = 1; }
  else if (fabs(f - 50.0)  < 0.05) { r.numerator = 50;    r.denominator = 1; }
  else if (fabs(f - 59.94) < 0.1)  { r.numerator = 60000; r.denominator = 1001; }
  else if (fabs(f - 60.0)  < 0.05) { r.numerator = 60;    r.denominator = 1; }
  else if (fabs(f - 100.0) < 0.05) { r.numerator = 100;   r.denominator = 1; }
  else if (fabs(f - 120.0) < 0.05) { r.numerator = 120;   r.denominator = 1; }
  return r;
}

/* Framebuffers suivis pour la ré-émission d'une trame STATIQUE en mode trame-entière (cf. struct
 * target : sf_fb_ptr). Généreux : la lib en expose typiquement 3-4. */
#define SF_FB_MAX 8

/* Une cible = un FLUX MXL de sortie. Une session en porte 1..N (fan-out même-source → N flux).
 * Chaque cible a son propre writer (RX/simu) ou reader (TX), son index et son propre IDENT. */
struct target {
  int      idx;            /* slot d'origine (pour le log) */
  char     shm_path[300], stats_path[300];   /* shm_path = NOM de flux (basename) après strip */
  mxlFlowWriter writer;    /* RX/simu : producteur du flux MXL (NULL si TX) */
  mxlFlowReader reader;    /* TX : lecteur du flux d'entrée câblé (NULL si RX ; créé paresseusement) */
  uint64_t index;          /* dernier index publié/lu (frame_index/chunk_index exposé en stats) */
  uint64_t recv;           /* compteur reçu (pour le débit) */
  uint64_t late;           /* TX vidéo : trames en retard (get_frame > 1,5 période = epoch raté) */
  uint64_t last_feed_ns;   /* TX vidéo : instant (monotone) du dernier get_frame réussi */
  uint64_t slot_wait_ns;   /* TX tranche : temps cumulé (ns) passé BLOQUÉ en attente d'un slot fb
                            * libre depuis le dernier feed — contre-pression de l'anneau (epoch-shift :
                            * la lib retient chaque trame `shift` plus tard), EXCLU du gap `late`. */
  /* Les deux compteurs ci-dessous sont l'ACCUMULATION de `slot_wait_ns`, que le calcul de `late`
   * remet à zéro à chaque trame. Sans eux, la contre-pression de l'anneau est mesurée mais
   * INVISIBLE — et c'est précisément l'inconnue qui bloque le diagnostic du plafond à ~38 fps pour
   * une source à 50 : worker affamé par la source, ou étranglé par l'anneau ? Publiés en
   * `slot_wait_ms` / `slot_wait_n` (write_stats), remis à zéro à chaque fenêtre de stats. */
  uint64_t slot_wait_cum_ns;
  uint64_t slot_wait_cnt;
  /* BRIDAGE D'AVANCE (`advance`) : temps passé à s'empêcher VOLONTAIREMENT de prendre de l'avance.
   * Compté SÉPARÉMENT de `slot_wait_ns`, et ce n'est pas un détail : confondre les deux détruirait
   * le seul compteur qui distingue « worker affamé par l'anneau » de « worker bridé exprès ».
   * Soustrait du gap `late` au même titre — sinon chaque bridage compterait comme un retard SOURCE. */
  uint64_t adv_wait_ns;
  uint64_t adv_wait_cum_ns;
  uint64_t adv_wait_cnt;
  uint64_t alive_ns;       /* TX (tous kinds) : dernier signe de vie du thread (get_frame OK ou
                            * attente de câblage). Figé session démarrée = queue TX morte (wedge). */
  int      dbg_depth_logged; /* TX vidéo : log one-shot grainSize/out_size/_src8 au 1er grain */
  uint64_t tx_src_idx;     /* TX vidéo : dernier index de grain SOURCE lu (détection flux figé) */
  uint64_t tx_src_idx_ns;  /* TX vidéo : instant (monotone) où tx_src_idx a changé pour la dernière fois */
  int      tx_src_idx_init; /* TX vidéo : tx_src_idx amorcé ? (0 au (ré)ouverture du reader) */
  /* bobi.studio: OBSERVABILITÉ tenue/source (write_stats) — instant (monotone) du dernier grain
   * SOURCE FRAIS réellement lu (posé par tx_reopen_if_stale, qui détecte déjà cet évènement pour la
   * reconnexion sur flux figé — même fait, deux consommateurs). Ne PAS recompter les rejeux de la
   * trame de tenue : libmtl ne les signale pas de façon fiable (cf. tx_sl_next_frame). */
  uint64_t last_fresh_ns;
  /* TX vidéo (bobi.studio) : compteur CUMULÉ de trames RÉPÉTÉES (gel d'image tranche, ou grain
   * source identique à la trame précédente en mode trame-entière). Trames UNIQUES = recv - repeats.
   * EXCLUT le mode statique (nominal, pas une dégradation) et la trame noire de repli (absence de
   * signal, pas une répétition) — cf. write_stats pour fps_source. */
  uint64_t repeats;
  int      anc_fmt_init;   /* TX ANC : codage du producteur déjà résolu ? (0 au (ré)ouverture) */
  int      anc_rfc8331;    /* TX ANC : 1 = producteur RFC 8331 (normatif), 0 = ancien format maison */
  uint64_t field_base;     /* TX entrelacé : index du 1er champ de la trame émise (MÊME trame pour les
                            * 2 champs → anti-peigne). Parité = TOP(pair) en TFF, BOTTOM(impair) en BFF. */
  /* RX vidéo : latence de réception (segment A = capture média → écriture shm), moyenne glissante
   * sur la fenêtre de stats. lat_sum en ns, lat_cnt = nb d'échantillons ; reset à chaque write_stats. */
  uint64_t lat_sum; uint32_t lat_cnt;
  /* RX audio (bobi.studio 0.44.2) : gap-fill silence quand le RX a droppé des trames (famine CPU
   * des threads de service, cf. volet 1 / CLAUDE.md). a_end = fin (index+n) du dernier chunk
   * RÉELLEMENT écrit (t->index reste le DÉBUT, pour compat stats) ; a_primed = 0 tant qu'aucun
   * chunk n'a encore été écrit (pas de gap-fill au tout premier chunk : rien à combler). */
  uint64_t a_end; int a_primed;
  uint64_t a_silence_filled;   /* échantillons de silence comblés (cumulatif, exposé si besoin) */
  uint64_t a_silence_log_ns;   /* throttle log gap-fill (mono_ns du dernier log, 0=jamais loggé) */
  char     ident_file[300]; int has_ident;
  uint8_t* ident_patch; int id_bw, id_bh; long id_mtime;
  /* bobi.studio: TRAME STATIQUE (slot TX provisionné SANS câble — noir de repli, mire, ardoise).
   * Le contenu ne change QUE sur évènement (changement de motif, d'ident, de format) : le rendre
   * 50 fois par seconde n'a aucun sens. Le contrôleur le rend UNE fois et publie le résultat dans
   * un fichier (écriture atomique, même contrat que `ident_file` : le C recharge sur le mtime) ;
   * ce thread ne fait plus que ré-emettre. Aucun reader, aucun flux MXL, aucune attente de grain →
   * la cadence est imposée par la seule libération des framebuffers, donc NOMINALE.
   * Le fichier est byte-identique au payload d'un grain MXL (planar Y|Cb|Cr) : les deux chemins TX
   * le consomment avec le code EXISTANT (sl_pack_band / copie+upshift), sans connaissance de format
   * supplémentaire d'aucun côté. La profondeur se déduit de la TAILLE, comme pour un grain. */
  char     static_frame[300];  /* chemin du fichier, ou "" (slot câblé / sans repli) */
  uint8_t* sf_buf; size_t sf_size; long sf_mtime;
  uint32_t sf_gen;             /* incrémenté à chaque (re)chargement → invalide les framebuffers */
  /* Chemin TRAME ENTIÈRE : les framebuffers sont rendus par la lib (adresses stables et peu
   * nombreuses) → même économie que le mode tranche, mémorisée par ADRESSE. */
  void*    sf_fb_ptr[SF_FB_MAX]; uint32_t sf_fb_stamp[SF_FB_MAX]; int sf_fb_n;
  /* timecode ATC (data/ANC) — dernier TC décodé, publié dans les stats */
  char     tc[16]; int tc_df; int tc_valid;
  /* bobi.studio: SWAP de SOURCE à chaud sans re-créer la session TX (découplage source↔session).
   * reconcile (writer unique) pose la nouvelle source via tx_set_source ; le thread TX la prend via
   * tx_take_source puis rouvre son reader. La source n'est PLUS dans compute_sig (TX) → changer de
   * source ne déclenche plus st20p_tx_create → pas de commit RL → pas de dé-lock PTP. Seqlock car la
   * source change rarement (routage) alors que le thread lit ~50 Hz. */
  char     want_shm[300];      /* source désirée (reconcile) */
  volatile uint32_t src_seq;   /* seqlock : pair = stable, impair = écriture en cours */
  uint32_t src_seq_seen;       /* dernière séquence consommée par le thread TX */
};

/* Une session = un flux réseau décodé UNE fois sur le mtl_handle partagé, fan-out vers ses cibles. */
struct sess {
  enum sess_kind kind;
  enum sess_role role;      /* RX (wire→shm) ou TX (shm→wire) */
  mtl_handle st;            /* partagé (mtl_init unique) */
  char portname[MTL_PORT_MAX_LEN];   /* leg primaire (NIC résolue depuis `iface`) */
  char iface[64];           /* nom de NIC du leg primaire ('' = port 0 / mono-NIC) */
  /* ── 2022-7 (leg redondant red/blue) — conçu mais NON émis par le socle (num_leg=1) ── */
  char portname_r[MTL_PORT_MAX_LEN]; /* leg redondant (vide = mono-leg) */
  char iface_r[64];         /* nom de NIC du leg redondant */
  char mcast_r[64]; int udp_port_r;  /* mcast/port de la 2ᵉ patte */
  int  num_leg;             /* 1 (socle) ou 2 (2022-7) */
  /* réseau */
  char mcast[64];
  int  udp_port, payload_type;
  uint32_t ssrc;            /* TX seulement : RFC3550 SSRC annoncé en a=ssrc du SDP (0=aléatoire) */
  /* décodage (partagé par toutes les cibles) */
  size_t   slotsize;       /* vidéo: framesize ; audio: 1152 (1 chunk 1ms) */
  int      ring, hdr;
  /* cibles (shm de sortie) */
  struct target tg[MAX_TG]; int ntg;
  pthread_t thread; int started;
  int      copy_mode;      /* vidéo: 1=memcpy (af_xdp) ; audio: toujours 1 */
  /* ── cycle de vie daemon (réconciliation à chaud) ── */
  volatile int stop;       /* arrêt PROPRE à cette session (thread boucle sur !stop) */
  int      used;           /* slot du registre occupé par une session vivante */
  int      seen;           /* marquage transitoire pendant un passage de réconciliation */
  char     sig[1024];      /* signature = identité+contenu ; un sig différent ⇒ recréer */
  /* ── vidéo ── */
  st20p_rx_handle vh;      /* RX */
  st20p_tx_handle vth;     /* TX */
  int      width, height, bit_depth, interlaced; double fps;
  int      tff;            /* entrelacé : 1=TFF (1080i), 0=BFF (576i) — parité des champs */
  int      epoch_shift_us; /* TX vidéo EPOCH-SHIFT (0=off) : fenêtre d'émission décalée de +shift µs
                              APRÈS l'epoch nominal, timestamp RTP restant sur l'epoch NOMINAL — le
                              récepteur mesure FPT ≈ shift (TROFF ST 2110-21, déclaré dans le SDP par
                              le contrôleur). Convention bobi libmtl (patch_epoch_shift) : porté par
                              ops.rtp_timestamp_delta_us NÉGATIF. Une chaîne interne à ~5 ms de phase
                              évite ainsi de payer +1 trame (« attendre l'image suivante »). */
  int      serve_newest;   /* TX vidéo TRANCHE : servir la trame la PLUS RÉCEMMENT publiée au lieu
                              de la plus ancienne (FIFO). 0 = désactivé (historique).
                              POURQUOI. MESURÉ le 2026-08-12 : la mire publie sa trame à ~6 ms dans
                              son créneau, libmtl vient la chercher à 16,4 ms du même créneau — elle
                              est donc DISPONIBLE À TEMPS pour l'époque suivante. Elle part pourtant
                              deux époques plus tard, parce que ce callback rend `sl_fb_cons`, la
                              plus ANCIENNE prête : on émet une trame périmée pendant qu'une plus
                              fraîche attend derrière.
                              ⚠ La tentative de juillet a échoué en JETANT des trames — elle jetait
                              celle que la lib allait servir (`drained` et `repeats` montaient au
                              même rythme). Ici on ne jette que des trames STRICTEMENT plus
                              anciennes que celle qu'on sert, et jamais celle en vol (`sl_fb_inflight`,
                              posée par ce même callback et libérée par notify_frame_done). */
  int      publish_lead_us; /* TX vidéo TRANCHE : FREIN TEMPOREL. Le worker attend d'être à
                              `publish_lead_us` µs de la prochaine époque avant d'aller chercher
                              le grain source. 0 = désactivé (comportement historique).
                              POURQUOI. MESURÉ le 2026-08-12 : une trame publiée attend 27 ms que
                              la lib vienne la chercher (`wait_pub_ms`) — celle-ci ne sollicite
                              qu'UNE FOIS PAR ÉPOQUE, et le worker, cadencé par la SOURCE, publie
                              juste après la sollicitation précédente. Les deux tournent à 50 Hz,
                              donc l'écart de phase établi au démarrage ne se résorbe jamais.
                              ⚠ CE N'EST PAS UN RETARD AJOUTÉ : l'instant d'émission est fixé par
                              la grille PTP, pas par la publication. Publier plus tard met du
                              contenu PLUS FRAIS dans le MÊME créneau. Le risque est de publier
                              TROP tard et de rater la sollicitation — la lib rediffuse alors la
                              trame de tenue (répétition à l'antenne). D'où un réglage, et deux
                              compteurs qui arbitrent : `repeats` et `wait_pub_ms`. */
  int      advance;        /* TX vidéo TRANCHE : nombre MAXIMAL de framebuffers prêts (stat=1) que
                              le worker s'autorise à avoir devant lui. 0 = désactivé (historique).
                              MESURÉ le 2026-08-12 : `depth_avg` 3,00 avec `slot_wait_ms` à 0,0 —
                              le worker n'attend jamais un slot, il a simplement trois trames
                              prêtes en permanence, niveau figé par le transitoire de démarrage
                              (deux débits égaux ne vident jamais une file).
                              ⛔ CE QU'ON EN ATTENDAIT EST RÉFUTÉ. On a cru que cette avance était
                              de la latence (`dist_avg` = `depth_avg` − 1 + « la lib sert la plus
                              ancienne » ⇒ 20 ms par trame stockée). MESURÉ ensuite : ramener la
                              file de 3 à 1 ne change PAS la latence (62,1 ms et +3 trames aux trois
                              paliers) et, à 1, le TX ne consomme plus qu'une trame source sur deux.
                              Le délai est dans l'ORDONNANCEMENT de l'émission, pas dans le stock —
                              cf. `epoch_shift_us`, seul levier qui déplace réellement l'aiguille.
                              LAISSER À 0. Le réglage n'est conservé que comme INSTRUMENT : c'est
                              lui qui a permis d'écarter la file comme cause. */
  size_t   src_framesize; int conv8;
  size_t   shm_slotsize;   /* taille d'un slot shm = TRAME PLEINE (≠ slotsize = taille CHAMP en
                              entrelacé, côté libmtl). 0 ⇒ open_shm retombe sur slotsize (audio/data). */
  enum st_frame_fmt out_fmt; int plane_h;   /* plane_h = hauteur de CHAMP (H/2) en entrelacé */
  uint8_t* tx_scratch;     /* TX entrelacé + IDENT : trame pleine de travail (overlay avant weave) */
  uint8_t* rx_scratch;     /* RX entrelacé : trame pleine où l'on weave les 2 champs avant OpenGrain */
  uint8_t* tx_frame;       /* TX entrelacé : trame pleine source (copie stable du grain) entre champs */
  /* ── MXL ── grille du flux (grain_rate vidéo/data, sample_rate audio) pour l'index TAI. */
  mxlRational mrate;       /* vidéo/data : cadence trame (grain_rate) */
  mxlRational srate;       /* audio : {48000,1} (sample_rate) */
  /* ── audio ── */
  st30p_rx_handle ah;      /* RX */
  st30p_tx_handle a_tx;    /* TX */
  int      channels;
  double   a_ptime;        /* ptime audio (ms) du SDP/réglage : 1.0, 0.125, 0.25… → ST30_PTIME_* */
  /* ── data / ANC (2110-40) ── */
  st40p_rx_handle d_rx;    /* RX */
  st40p_tx_handle d_tx;    /* TX */
  uint32_t max_udw;        /* taille du buffer UDW (octets) */
  /* ── Timing parser 2110-21 (RX vidéo, TIMING_PARSER=1 uniquement) ── conformité par session.
   * Le parser libmtl (st_rx_timing_parser.c) calcule Cinst/VRX/FPT/latency par TRAME et rend un
   * verdict FAILED|WIDE|NARROW + failed_cause. On accumule sur la fenêtre de write_stats (lockless,
   * même modèle que lat_sum/lat_cnt : accum côté video_rx_thread, lecture+reset côté write_stats).
   * Le verdict de fenêtre = le PIRE (enum min : failed<wide<narrow). Sans HW timestamp le parser
   * n'a pas de mesure fiable → capacité gardée strictement par tp_enabled (getenv TIMING_PARSER). */
  int      tp_enabled;         /* 1 = session RX vidéo avec parser actif (flag posé au create) */
  int      tp_worst;           /* pire verdict de la fenêtre (enum st_rx_tp_compliant) ; -1 = aucun échantillon */
  char     tp_cause[64];       /* failed_cause du pire verdict (vide si narrow) */
  int32_t  tp_cinst_max, tp_vrx_max, tp_vrx_min; /* extrêmes de la fenêtre */
  int32_t  tp_vrx_span_max;    /* pire amplitude INTRA-trame (vrx_max-vrx_min d'UNE trame) sur la fenêtre.
                                * Invariante au décalage/dérive d'horloge RX↔TX (différences intra-trame)
                                * → discriminant narrow/wide robuste quand la sonde n'est pas PTP-lockée
                                * sur l'émetteur (ex. banc loopback free-running). */
  double   tp_cinst_sum, tp_vrx_sum; /* Σ des moyennes/trame (÷ tp_cnt = moyenne de fenêtre) */
  int32_t  tp_fpt, tp_latency; /* dernières valeurs (ns) de la fenêtre */
  uint32_t tp_cnt;             /* nb de trames échantillonnées dans la fenêtre */
  /* ── TRANSPORT PAR SESSION (2026-08-27) — le trou que la sonde du scope documentait ──────
   * Jusqu'ici les seuls compteurs de paquets publiés étaient ceux du PORT (`mtl_stats`),
   * agrégés sur TOUTES les sessions : « 9 478 paquets jetés » ne désignait aucun flux, et un
   * afficheur qui l'attribuait à un flux mentait par confusion de granularité.
   * ★ LA DONNÉE ÉTAIT DÉJÀ SOUS LA MAIN. `st_frame` / `st20_rx_frame_meta` portent, PAR TRAME
   * ET PAR SESSION : `pkts_total` (paquets attendus, redondants exclus), `pkts_recv[port]`
   * (reçus sur chaque patte) et `status`. L'en-tête libmtl le dit lui-même : comparer
   * `pkts_recv[s_port]` à `pkts_total` EST l'indicateur de qualité du signal. Il n'y avait
   * donc rien à mesurer, seulement à ACCUMULER et à publier — même modèle lockless que le
   * bloc `tp` juste au-dessus (accum côté thread RX, lecture+reset côté write_stats).
   * `px_manque` = Σ(pkts_total − pkts_recv[P]) : les paquets qui ne sont PAS arrivés sur la
   * patte primaire. Avec le 2022-7 armé, la patte de secours les rattraperait — d'où le
   * compteur séparé `px_recv_r`, qui reste à zéro tant que `num_leg == 1`. */
  uint64_t px_pkts;            /* Σ pkts_total sur la fenêtre */
  uint64_t px_recv_p, px_recv_r; /* Σ pkts_recv[P] et [R] */
  uint32_t px_incomplete;      /* trames au statut != COMPLETE */
  uint32_t px_frames;          /* trames comptées */
  uint32_t px_pire;            /* pire manque sur UNE trame (paquets) */
  /* ── MODE TRANCHE (SLICE_MODE=1, vidéo PROGRESSIVE uniquement) — latence sous-trame ──
   * RX : API raw st20 (ST20_TYPE_SLICE_LEVEL) → conversion RFC4175→planar PAR BANDE + commit MXL
   * progressif (validSlices=1..N, cf. patch mxl-planar-slices) au fil des tranches reçues.
   * TX : raw st20 aussi → lecture mxlFlowReaderGetGrainSlice (réveil PAR TRANCHE) + remplissage
   * progressif du framebuffer, la lib interroge lines_ready (query_frame_lines_ready).
   * Env-gaté (défaut OFF = chemin whole-frame st20p inchangé) ; entrelacé/audio/data inchangés.
   * Convention validSlices (contrat producteur↔consommateur, cf. slice_bench) : k tranches
   * valides ⇔ lignes image [0, k·slice_lines) écrites SUR LES 3 PLANS (Y, Cb, Cr). */
  int slice_on;                /* session vidéo en mode tranche */
  int slice_lines;             /* lignes par tranche (RX : SLICE_LINES, doit diviser height) */
  st20_rx_handle sl_rx;        /* RX raw st20 (remplace vh en mode tranche) */
  st20_tx_handle sl_tx;        /* TX raw st20 (remplace vth en mode tranche) */
  pthread_mutex_t sl_mx; pthread_cond_t sl_cv;   /* réveil du worker par les callbacks lcore */
#define SL_Q 8                 /* trames en vol max (= framebuff_cnt clampé) */
  /* RX : trames en vol. Les callbacks lcore (non bloquants) posent frame+lignes reçues sous sl_mx ;
   * le worker convertit/commit. Champs « worker » hors verrou (seul le worker y touche). */
  struct sl_inflight {
    void*    frame;            /* framebuffer libmtl (RFC4175 BE10 packé) ; NULL = slot libre */
    uint64_t timestamp;        /* meta->timestamp (posé à la 1ʳᵉ tranche) */
    enum st10_timestamp_fmt tfmt;
    uint32_t recv_lines;       /* lignes reçues (mis à jour par les callbacks, sous sl_mx) */
    int      complete;         /* notify_frame_ready passé (trame close côté lib) */
    uint64_t seq;              /* ordre d'arrivée (le worker traite la plus ancienne d'abord) */
    /* état worker */
    uint32_t conv_lines;       /* lignes déjà converties/commitées */
    int      opened;           /* grains MXL ouverts (à la 1ʳᵉ tranche) */
    uint64_t fi, mts;          /* index de grain + timestamp média TAI */
    mxlGrainInfo gi[MAX_TG]; uint8_t* payload[MAX_TG];
  } sl_q[SL_Q];
  uint64_t sl_seq;             /* générateur d'ordre d'arrivée */
  uint32_t sl_drop;            /* trames jetées (pas de slot / OpenGrain KO) */
  /* TX : framebuffers raw. Le worker remplit progressivement ; la lib consomme (get_next_frame)
   * et interroge lines_ready. stat : 0=FREE (à remplir) 1=READY (en remplissage ou en vol). */
  /* `sf_stamp` : génération de trame STATIQUE déjà écrite dans ce framebuffer (0 = jamais). Les
   * framebuffers libmtl PERSISTENT d'un tour à l'autre : une fois la trame statique packée dedans,
   * la ré-émettre ne demande AUCUNE recopie — on ne fait que re-marquer le slot prêt.
   * `wit_stamp` : même principe pour le TÉMOIN DE REPLI publié par le worker (cf. SL_FALLBACK_AFTER_NS
   * dans video_tx_slice_thread) — génération du témoin (position du carré) déjà packée dans ce
   * framebuffer (0 = jamais). CHAMP DÉDIÉ, distinct de `sf_stamp` : un même slot peut successivement
   * porter une trame statique puis un témoin (changement de mode) — s'ils partageaient le compteur de
   * génération, une coïncidence de valeur ferait sauter le re-packing à tort.
   * `rep_stamp` : même principe pour la RÉPÉTITION QUASI SANS COPIE de la dernière image RÉELLE
   * packée (source en retard, mais pas encore morte — cf. sl_publish_repeat_or_witness), CHAMP DÉDIÉ
   * lui aussi : un slot qui a déjà reçu la bonne génération n'est jamais recopié. */
  /* stat : 0=FREE (le worker peut le remplir) 1=READY (rempli / en vol) 2=TENUE (dernière trame
   * intégralement émise, CONSERVÉE pour être re-servie si la source est en retard — le worker ne
   * doit jamais la reprendre, cf. HORLOGE DE SORTIE dans tx_sl_next_frame). */
  /* `pub_ns` : instant où le WORKER publie ce framebuffer. `start_ns` : instant où la LIB le
   * sollicite pour la première fois ensuite — donc où l'émission de CETTE trame commence.
   * Leur écart est le DERNIER segment aveugle de la chaîne TX. Tout le reste a été éliminé
   * par la mesure : source fraîche (src_age négatif), file sans effet, ordonnancement 7,6 ms. */
  struct sl_txfb { volatile int stat; volatile uint32_t lines_ready; uint32_t sf_stamp; uint32_t wit_stamp; uint32_t rep_stamp; uint64_t pub_ns; uint64_t start_ns; } sl_fb[SL_Q];
  uint16_t sl_fb_cnt, sl_fb_prod, sl_fb_cons;
  /* ── HORLOGE DE SORTIE (trame de tenue) ────────────────────────────────────────────────────────
   * Une sortie 2110 est un appareil à HORLOGE : elle doit présenter une trame à CHAQUE époque de la
   * grille PTP, quoi que fasse le producteur. Sans ça, `tx_sl_next_frame` rendait -EBUSY dès que le
   * worker n'avait rien de prêt : l'époque était perdue, et le worker rattrapait ensuite en paquetant
   * la trame entière d'un bloc → un grand trou suivi d'une rafale. MESURÉ contre un EVS Neuron :
   * PIT max 7077-12708 avec un producteur déficitaire, contre 818 sur une sortie sans producteur —
   * et il REFUSE au-delà de 1500. Un problème de SOURCE devenait un problème de SIGNAL.
   * `sl_hold_idx` = dernier framebuffer intégralement émis, gardé en stat=TENUE et re-servi tel quel
   * (aucune copie : les framebuffers libmtl persistent). `sl_hold_emitted` n'est écrit QUE par le
   * tasklet lcore — un seul écrivain par compteur, pas de course avec le worker (cf. write_stats). */
  uint16_t sl_hold_idx;        /* framebuffer de tenue (valide ssi sl_hold_valid) */
  int      sl_hold_valid;
  uint64_t sl_hold_emitted;    /* trames émises PAR REJEU de la trame de tenue (cumul) */
  uint64_t sl_hold_last_ns;    /* dernier rejeu COMPTÉ (anti-double-compte, cf. tx_sl_next_frame) */
  uint64_t sl_period_ns;       /* période nominale d'époque (1/fps) — borne du comptage de rejeu */
  /* ── DIAGNOSTIC DU SERVICE DE TRAMES (bobi.studio) ──────────────────────────────────────────────
   * Compteurs de ce que `tx_sl_next_frame` a RENDU à la lib, par branche. Écrits UNIQUEMENT par le
   * tasklet lcore (un seul écrivain), lus par write_stats. Ce ne sont PAS des trames émises — la lib
   * sollicite ce callback sans toujours émettre — mais c'est exactement ce qu'il faut pour répondre
   * « le callback est-il seulement appelé ? » et « quelle branche prend-il ? » quand une sortie se
   * tait. Faute de ces compteurs, le diagnostic d'une sortie muette se fait à l'aveugle (vécu). */
  uint64_t srv_fresh;          /* une trame FRAÎCHE du worker a été servie (témoin INCLUS : de son
                                * point de vue le callback ne voit qu'un slot READY normal) */
  uint64_t srv_hold;           /* la tenue « gel » a été servie */
  uint64_t srv_busy;           /* -EBUSY rendu : rien à servir du tout (ne doit plus arriver) */
  /* ── TÉMOIN DE REPLI (bobi.studio) ──────────────────────────────────────────────────────────────
   * Au-delà de SL_FALLBACK_AFTER_NS sans grain SOURCE frais (cf. t->last_fresh_ns), le WORKER
   * lui-même publie un témoin (noir légal + petit carré, cf. sl_fill_fallback_frame) dans le ring TX
   * normal — EXACTEMENT comme la branche statique du mode tranche (cf. video_tx_slice_thread) —
   * plutôt que de laisser la lib re-servir indéfiniment une paire de framebuffers réservés depuis le
   * callback lcore (1ʳᵉ tentative, ABANDONNÉE : mesuré en prod, dès que l'appli cesse de publier des
   * trames, libmtl arrête d'appeler tx_sl_next_frame et ne reprend jamais). Le carré ALTERNE de
   * position toutes les SL_FALLBACK_TOGGLE_NS — le déplacement est ce qui distingue un « gel » d'un
   * « plantage ». Un slot SANS câble (mode statique) n'a pas de source à perdre → ne bascule jamais
   * (cf. `statique` dans video_tx_slice_thread). */
  uint64_t sl_witness_switch_ns; /* mono_ns du dernier basculement de position (0 = pas encore basculé) */
  int      sl_witness_cur;      /* position courante du carré témoin : 0 (gauche) ou 1 (droite) */
  uint32_t sl_witness_gen;      /* génération du témoin courant (incrémentée à chaque bascule) — cf.
                                 * sl_txfb.wit_stamp (0 = jamais publié) */
  /* ── RÉPÉTITION QUASI SANS COPIE (bobi.studio) ──────────────────────────────────────────────────
   * Comble le TROU entre la trame de tenue (valide tant que le worker publie à cadence correcte) et
   * le témoin de repli (déclenché seulement après SL_FALLBACK_AFTER_NS) : une source qui livre trop
   * lentement pour (1) mais trop vite pour (2) faisait sortir le port à 0,01 Gb/s (mesuré). Chaque
   * tour qui obtient un slot libre DOIT publier — quand aucun grain frais n'est disponible, on
   * republie la DERNIÈRE IMAGE RÉELLE packée plutôt que rien : `sl_carrier_idx` désigne le slot qui la
   * porte, `sl_content_gen` sa génération (incrémentée à chaque nouveau packing réel). Un slot dont
   * `rep_stamp` est déjà à jour n'est jamais recopié (les framebuffers persistent) ; sinon, un
   * memcpy depuis le slot porteur — SEULEMENT s'il est libre (stat==0), jamais depuis un slot en vol. */
  uint16_t sl_carrier_idx;      /* slot portant la dernière image RÉELLE packée (valide ssi sl_carrier_valid) */
  int      sl_carrier_valid;
  uint32_t sl_content_gen;      /* génération de la dernière image réelle (0 = aucune encore) */
  uint64_t sl_wedge_ns;        /* TX : début (mono) du blocage « aucun slot libre » (watchdog anneau) */
  uint64_t sl_wedge_log_ns;    /* TX : throttle du log wedge — instant du dernier log émis (0 = session
                                * saine, le prochain wedge logue en entier). Reset par tx_sl_frame_done. */
  uint32_t sl_wedge_log_n;     /* TX : resyncs wedge survenus depuis le dernier log émis (agrégat) */
  uint64_t sl_hold_empty_kept; /* TX : promotions en tenue REFUSÉES parce que la trame était vide
                                * (cf. tx_sl_frame_done). Non nul = la lib a émis un framebuffer
                                * publié avant remplissage, et on vient d'éviter une sortie muette
                                * définitive. Doit rester à 0 depuis 0.82.0 (publication après la
                                * 1ʳᵉ bande) ; s'il grimpe, une autre fenêtre reste ouverte. */
  uint8_t* sl_scratch;         /* TX source 8-bit : bande planar10 up-shiftée avant pack SIMD */
  /* VENTILATION DU TOUR DE WORKER (ajouté le 2026-08-09). Une itération de video_tx_slice_thread
   * coûte : attente d'un slot libre (déjà mesurée par slot_wait_*) + ATTENTE DU GRAIN source +
   * PACKING. Les deux dernières étaient confondues, si bien qu'un worker publiant 37/s pour une
   * source à 50/s ne pouvait pas être attribué. Reset à chaque fenêtre de stats, comme slot_wait. */
  uint16_t sl_fb_inflight;     /* slot RENDU au dernier appel = trame en cours d'émission */
  int      sl_fb_inflight_ok;  /* sl_fb_inflight est-il renseigné ? */
  uint64_t sl_skipped;         /* trames périmées libérées sans être émises (serve_newest) */
  uint64_t sl_wait_pub_ns;     /* cumul publication → 1re sollicitation par la lib */
  uint64_t sl_wait_pub_cnt;
  uint64_t sl_wait_pub_max;
  uint64_t sl_emit_ns;         /* cumul 1re sollicitation → notify_frame_done */
  uint64_t sl_emit_cnt;
  int64_t  sl_lead_ns;         /* cumul de l'avance d'ordonnancement vue dans tx_sl_next_frame */
  uint64_t sl_lead_cnt;
  int64_t  sl_lead_max;
  int64_t  sl_srcage_ns;       /* cumul de l'ÂGE du contenu saisi par le TX (TAI − index×période) */
  uint64_t sl_srcage_cnt;
  int64_t  sl_srcage_max;
  uint64_t sl_getgrain_ns;     /* cumul du temps passé dans mxlFlowReaderGetGrainSlice (attente source) */
  uint64_t sl_getgrain_cnt;
  uint64_t sl_pack_ns;         /* cumul du temps de sl_pack_band (toutes bandes d'une trame) */
  uint64_t sl_pack_cnt;
  uint64_t sl_depth_sum, sl_depth_cnt; uint16_t sl_depth_max;  /* profondeur de file à la sollicitation */
  uint64_t sl_dist_sum;  uint16_t sl_dist_max;                  /* distance servi → prod (sens de l'anneau) */
  uint64_t sl_drained;         /* RÉSERVÉ : compteur du drain tenté le 2026-08-09 (servir la trame
                                * la plus récente et rendre les autres). Retiré le même jour — il
                                * produisait UNE RÉPÉTITION PAR TRAME DRAINÉE, donc il volait la
                                * trame courante au lieu d'en jeter une périmée. Reste publié à 0
                                * tant que le sens réel de l'anneau n'est pas établi (cf. dist_avg). */
};

/* Mode tranche demandé (SLICE_MODE=1) + géométrie de tranche (SLICE_LINES, défaut 36 lignes —
 * divise 1080/720/2160 ; 1080p → 30 tranches, réveil consommateur toutes les ~0,6 ms à 50p). */
static int slice_wanted(void) { const char* e = getenv("SLICE_MODE"); return e && atoi(e); }
static int slice_lines_env(void) {
  const char* e = getenv("SLICE_LINES");
  int v = e ? atoi(e) : 0;
  return v > 0 ? v : 36;
}

/* Niveau de log libmtl configurable (env MTL_LOG_LEVEL, posé par l'orchestrateur depuis
 * Réglages → MXL). DÉFAUT "warning" (silencieux) : à INFO, libmtl émet périodiquement un dump de
 * stats volumineux (bloc « END STATE » + SCH/xdp_queue) qui noie les logs du moteur et rend le
 * diagnostic illisible. Valeur absente ou inconnue → warning (sûr). Remonter à info/debug pour un
 * diagnostic ponctuel seulement. */
static enum mtl_log_level mtl_log_level_env(void) {
  const char* e = getenv("MTL_LOG_LEVEL");
  if (!e || !*e)                                            return MTL_LOG_LEVEL_WARNING;
  if (!strcasecmp(e, "debug"))                              return MTL_LOG_LEVEL_DEBUG;
  if (!strcasecmp(e, "info"))                               return MTL_LOG_LEVEL_INFO;
  if (!strcasecmp(e, "notice"))                             return MTL_LOG_LEVEL_NOTICE;
  if (!strcasecmp(e, "warning") || !strcasecmp(e, "warn"))  return MTL_LOG_LEVEL_WARNING;
  if (!strcasecmp(e, "error")   || !strcasecmp(e, "err"))   return MTL_LOG_LEVEL_ERR;
  if (!strcasecmp(e, "crit"))                               return MTL_LOG_LEVEL_CRIT;
  return MTL_LOG_LEVEL_WARNING;
}

/* Période du dump de stats libmtl (env MTL_STAT_DUMP_PERIOD, en secondes ; 0 ou hors bornes =
 * défaut de la lib). Bobi ne consomme PAS ce dump : en mode silencieux l'orchestrateur pose une
 * très grande période (dump neutralisé, plus aucune collecte périodique inutile), et en mode
 * diagnostic (info/debug/notice) il laisse 0 pour que le dump serve. */
static uint16_t mtl_dump_period_env(void) {
  const char* e = getenv("MTL_STAT_DUMP_PERIOD");
  int v = e ? atoi(e) : 0;
  return (v > 0 && v < 65536) ? (uint16_t)v : 0;
}

/* ── flowDef MXL (ressource Flow NMOS IS-04) — mêmes champs/conventions que bobimxl.build_*. ── */
static void jcomp(struct json_object* arr, const char* nm, int w, int h, int bd) {
  struct json_object* c = json_object_new_object();
  json_object_object_add(c, "name", json_object_new_string(nm));
  json_object_object_add(c, "width", json_object_new_int(w));
  json_object_object_add(c, "height", json_object_new_int(h));
  json_object_object_add(c, "bit_depth", json_object_new_int(bd));
  json_object_array_add(arr, c);
}
static struct json_object* jgrouphint(const char* name, const char* role) {
  struct json_object* tags = json_object_new_object();
  struct json_object* gh = json_object_new_array();
  char hint[420]; snprintf(hint, sizeof(hint), "%s:%s", name, role);
  json_object_array_add(gh, json_object_new_string(hint));
  json_object_object_add(tags, "urn:x-nmos:tag:grouphint/v1.0", gh);
  return tags;
}
static struct json_object* jrate(mxlRational r) {
  struct json_object* o = json_object_new_object();
  json_object_object_add(o, "numerator", json_object_new_int64(r.numerator));
  json_object_object_add(o, "denominator", json_object_new_int64(r.denominator));
  return o;
}

/* Vidéo planar (video/x-mxl-planar) : grain = somme des plans Y+Cb+Cr (octet-identique au shm
 * maison historique). 422 → Cb/Cr en demi-largeur, pleine hauteur. Renvoie une chaîne heap. */
static char* build_video_flowdef(struct sess* s, struct target* t) {
  const char* name = flow_name(t->shm_path);
  char id[37]; flow_id_str(name, id);
  struct json_object* o = json_object_new_object();
  json_object_object_add(o, "id", json_object_new_string(id));
  json_object_object_add(o, "tags", jgrouphint(name, "Video"));
  json_object_object_add(o, "format", json_object_new_string("urn:x-nmos:format:video"));
  json_object_object_add(o, "label", json_object_new_string(name));
  json_object_object_add(o, "media_type", json_object_new_string("video/x-mxl-planar"));
  json_object_object_add(o, "grain_rate", jrate(s->mrate));
  json_object_object_add(o, "frame_width", json_object_new_int(s->width));
  json_object_object_add(o, "frame_height", json_object_new_int(s->height));
  /* ENTRELACÉ NATIF « 1 grain = 1 CHAMP » (modèle SDK MXL) : interlace_mode=interlaced_tff/bff +
   * grain_rate = cadence TRAME (25/1 ou 30000/1001). libmxl double la cadence en interne (cadence
   * champ) et dimensionne chaque grain = 1 CHAMP (½ trame). L'identité du champ = parité de l'index
   * de grain (index pair = 1er champ ; tff ⇒ 1er=top). On écrit/lit donc des grains-champs ;
   * field_height reste pleine (libmxl fait le /2). */
  json_object_object_add(o, "interlace_mode", json_object_new_string(
      s->interlaced ? (s->tff ? "interlaced_tff" : "interlaced_bff") : "progressive"));
  /* MODE TRANCHE : slice_height (extension bobi.studio du patch mxl-planar-slices) → libmxl
   * publie le grain en N = height/slice_height tranches (totalSlices) → commit progressif
   * validSlices=1..N. Absent (whole-frame) → 1 tranche, comportement historique. */
  if (s->slice_on && s->role == ROLE_RX)
    json_object_object_add(o, "slice_height", json_object_new_int(s->slice_lines));
  json_object_object_add(o, "colorspace", json_object_new_string("BT709"));
  struct json_object* comps = json_object_new_array();
  jcomp(comps, "Y",  s->width,     s->height, s->bit_depth);
  jcomp(comps, "Cb", s->width / 2, s->height, s->bit_depth);
  jcomp(comps, "Cr", s->width / 2, s->height, s->bit_depth);
  json_object_object_add(o, "components", comps);
  char* out = strdup(json_object_to_json_string(o));
  json_object_put(o);
  return out;
}

/* Audio (audio/float32) : flux CONTINU de samples float32 par canal, 48 kHz. */
static char* build_audio_flowdef(struct sess* s, struct target* t) {
  const char* name = flow_name(t->shm_path);
  char id[37]; flow_id_str(name, id);
  struct json_object* o = json_object_new_object();
  json_object_object_add(o, "id", json_object_new_string(id));
  json_object_object_add(o, "tags", jgrouphint(name, "Audio"));
  json_object_object_add(o, "format", json_object_new_string("urn:x-nmos:format:audio"));
  json_object_object_add(o, "label", json_object_new_string(name));
  json_object_object_add(o, "media_type", json_object_new_string("audio/float32"));
  mxlRational sr = {48000, 1};
  json_object_object_add(o, "sample_rate", jrate(sr));
  json_object_object_add(o, "channel_count", json_object_new_int(s->channels));
  json_object_object_add(o, "bit_depth", json_object_new_int(32));
  char* out = strdup(json_object_to_json_string(o));
  json_object_put(o);
  return out;
}

/* Data/ANC (video/smpte291) : grain = payload ANC **RFC 8331** (normatif, interopérable).
 * `bobi_anc_format` (champ NON standard, ignoré par un SDK stock — même vecteur que slice_height)
 * ANNONCE le codage aux consommateurs → un lecteur pas encore migré, ou un producteur legacy
 * encore en vol, restent gérés (flotte MIXTE). Cf. bobimxl.build_data_flow_def. */
static char* build_data_flowdef(struct sess* s, struct target* t) {
  const char* name = flow_name(t->shm_path);
  char id[37]; flow_id_str(name, id);
  struct json_object* o = json_object_new_object();
  json_object_object_add(o, "id", json_object_new_string(id));
  json_object_object_add(o, "bobi_anc_format", json_object_new_string("rfc8331"));
  json_object_object_add(o, "tags", jgrouphint(name, "Data"));
  json_object_object_add(o, "format", json_object_new_string("urn:x-nmos:format:data"));
  json_object_object_add(o, "label", json_object_new_string(name));
  json_object_object_add(o, "media_type", json_object_new_string("video/smpte291"));
  json_object_object_add(o, "grain_rate", jrate(s->mrate));
  char* out = strdup(json_object_to_json_string(o));
  json_object_put(o);
  return out;
}

/* ── Lot de synchronisation RDMA (`maxSyncBatchSizeHint`) ────────────────────────────────────
 * Option de FLUX, posée à la CRÉATION, dans le JSON d'options (3ᵉ argument de
 * mxlCreateFlowWriter) — l'emplacement où ce fichier passait NULL depuis toujours.
 *
 * Elle fixe `slicesPerBatch` : combien de tranches l'initiateur RDMA accumule avant de
 * transférer. SON DÉFAUT VAUT `totalSlices`. Conséquence, mesurée au banc le 2026-08-09
 * (dell-1 → dl360-1, 1080p50 en 30 tranches, horodatage écrit dans chaque tranche) :
 *
 *     lot 30 (= défaut) → 1ʳᵉ bande lisible sur la réplique à 22,63 ms
 *     lot 2            → 0,54 ms
 *     lot 1            → 0,06 ms
 *
 * Autrement dit : SANS cette option, découper le flux ne rapporte RIEN sur le fil. Le moteur
 * publie ses 30 tranches (SLICE_MODE=1, slice_height) et l'initiateur les rassemble toutes avant
 * d'en transférer une seule — on paie le découpage sans en tirer la latence. C'est le cas en
 * production sur le flux RX répliqué dl360-1 → dell-1, constaté le 2026-08-10.
 *
 * MÊME CONTRAT QUE LE CÔTÉ PYTHON (`script_templates/bobimxl.py:_flow_options`) : variable
 * d'environnement MXL_SYNC_BATCH, vide/illisible/≤ 0 ⇒ NULL, donc comportement historique
 * octet-identique. Les deux implémentations doivent rester alignées — un producteur C et un
 * producteur Python sur le même domaine MXL ne doivent pas avoir des lots différents sans raison.
 *
 * ⚠ N'agit que sur les flux CRÉÉS ENSUITE : un flux existant garde le lot fixé à sa création.
 * Vérifiable avec `mxl-info -d /dev/shm/mxl -f <uuid>` → ligne `Sync batch size`.
 *
 * Renvoie une chaîne heap (à libérer par l'appelant) ou NULL. */
static char* sync_batch_opts(void) {
  const char* e = getenv("MXL_SYNC_BATCH");
  if (!e || !*e)
    return NULL;
  int n = atoi(e);
  if (n <= 0)
    return NULL;                 /* valeur absente de sens → défaut du SDK, jamais un lot bancal */
  struct json_object* o = json_object_new_object();
  json_object_object_add(o, "maxSyncBatchSizeHint", json_object_new_int(n));
  char* out = strdup(json_object_to_json_string(o));
  json_object_put(o);
  return out;
}

/* Crée le writer MXL d'une cible à partir d'un flowDef (RX/simu). Renvoie 0 si OK.
 * GC-AND-RETRY : l'id du flux ne dépend que du NOM du slot (pas de l'interlace_mode/grain_rate).
 * Quand un slot passe d'un producteur SIMU progressif (contrôleur) à un RX entrelacé (ce process),
 * un flux PÉRIMÉ de def DIFFÉRENTE squatte l'id → mxlCreateFlowWriter échoue (def incompatible).
 * Une fois l'ancien producteur libéré (le contrôleur ferme sa simu quand le slot devient live), un
 * garbage-collect récupère le flux orphelin → on réessaie (jusqu'à ~2 s). Cas sans transition de def
 * (progressif↔progressif : attache au flux simu existant) : succès au 1er essai, boucle non exécutée. */
static int open_writer(struct target* t, char* flowdef) {
  bool created = false;
  /* Le lot de synchronisation vaut pour TOUS les writers du moteur (vidéo, audio, ANC) : c'est
   * précisément pour ça qu'ils passent tous par ici — un seul point, pas un oubli par essence. */
  char* opts = sync_batch_opts();
  mxlStatus st = mxlCreateFlowWriter(g_mxl, flowdef, opts, &t->writer, NULL, &created);
  for (int attempt = 0; st != MXL_STATUS_OK && attempt < 10; attempt++) {
    mxlGarbageCollectFlows(g_mxl);          /* récupère le flux périmé une fois son producteur libéré */
    usleep(200000);                         /* laisse la simu du contrôleur se fermer (slot devenu live) */
    st = mxlCreateFlowWriter(g_mxl, flowdef, opts, &t->writer, NULL, &created);
  }
  free(opts);
  free(flowdef);
  if (st != MXL_STATUS_OK) {
    fprintf(stderr, "mtl_rx: mxlCreateFlowWriter(%s) -> %d\n", flow_name(t->shm_path), (int)st);
    return -1;
  }
  return 0;
}

/* Latence de réception (segment A) : Δ entre l'instant de CAPTURE média (media_ts, TAI) et
 * l'instant d'écriture shm (now_ns, REALTIME). Les deux horloges peuvent différer d'un nombre
 * ENTIER de secondes (offset TAI↔UTC, p.ex. 37 s ; ou 0 si le système est calé sur TAI). La vraie
 * latence étant << 1 s, on retire l'écart entier de secondes par arrondi → reste la part sub-seconde.
 * Accumule la moyenne glissante (fenêtre = write_stats). media_ts=0 (inconnu) → ignoré. */
static void accum_rx_latency(struct target* t, uint64_t media_ts) {
  if (!media_ts) return;
  int64_t d   = (int64_t)now_ns() - (int64_t)media_ts;
  int64_t off = (int64_t)llround((double)d / 1e9) * 1000000000LL;
  d -= off;
  if (d < 0) d = 0;                 /* garde-fou : jamais négatif après calibration */
  t->lat_sum += (uint64_t)d; t->lat_cnt++;
}
/* sampling de l'horloge média ST 2110-10 : 90 kHz vidéo, 48 kHz audio (pour st10_get_tai). */
#define MEDIA_CLK_VIDEO 90000u
#define MEDIA_CLK_AUDIO 48000u

/* ═══ VIDÉO ═══════════════════════════════════════════════════════════════════ */

static int frame_available(void* priv) { (void)priv; return 0; }

/* TRAME STATIQUE : recharge le fichier (écrit par le contrôleur) si son mtime OU sa taille change.
 * Même contrat que load_ident_patch — le contrôleur publie par rename atomique, on relit sur mtime.
 * Renvoie 0 si une trame est disponible en mémoire, -1 sinon (pas de fichier, ou illisible : le
 * slot reste alors silencieux, ce qui est visible, plutôt que d'émettre un buffer non initialisé).
 * `sf_gen` est incrémenté à chaque chargement RÉUSSI : c'est lui qui invalide les framebuffers déjà
 * remplis, sans quoi un changement de mire ne serait jamais visible à l'antenne. */
static int load_static_frame(struct target* t) {
  if (!t->static_frame[0]) return -1;
  struct stat st;
  if (stat(t->static_frame, &st) != 0 || st.st_size <= 0) return t->sf_buf ? 0 : -1;
  if (t->sf_buf && (long)st.st_mtime == t->sf_mtime && (size_t)st.st_size == t->sf_size) return 0;
  FILE* f = fopen(t->static_frame, "rb");
  if (!f) return t->sf_buf ? 0 : -1;
  uint8_t* buf = malloc((size_t)st.st_size);
  if (!buf) { fclose(f); return t->sf_buf ? 0 : -1; }
  size_t n = fread(buf, 1, (size_t)st.st_size, f);
  fclose(f);
  if (n != (size_t)st.st_size) { free(buf); return t->sf_buf ? 0 : -1; }
  free(t->sf_buf);
  t->sf_buf = buf; t->sf_size = n; t->sf_mtime = (long)st.st_mtime; t->sf_gen++;
  fprintf(stderr, "mtl_rx: trame statique chargée (%s, %zu octets) — slot ré-émet sans producteur\n",
          t->static_frame, n);
  return 0;
}

/* IDENT : recharge le patch (fichier écrit par le contrôleur) si mtime change. Coût ~nul si off.
 * Le patch est PAR CIBLE (chaque slot a son propre IDENT, même quand la source est partagée). */
static void load_ident_patch(struct target* t) {
  if (!t->has_ident) return;
  struct stat st;
  if (stat(t->ident_file, &st) != 0) {
    if (t->ident_patch) { free(t->ident_patch); t->ident_patch = NULL; }
    t->id_bw = t->id_bh = 0; t->id_mtime = 0; return;
  }
  if ((long)st.st_mtime == t->id_mtime && t->ident_patch) return;
  FILE* f = fopen(t->ident_file, "rb");
  if (!f) return;
  uint32_t hdr[2];
  if (fread(hdr, sizeof(uint32_t), 2, f) != 2) { fclose(f); return; }
  int bw = (int)hdr[0], bh = (int)hdr[1];
  if (bw <= 0 || bh <= 0 || bw > 8192 || bh > 8192) { fclose(f); return; }
  size_t n = (size_t)bw * bh;
  uint8_t* p = malloc(n);
  if (!p) { fclose(f); return; }
  if (fread(p, 1, n, f) != n) { free(p); fclose(f); return; }
  fclose(f);
  if (t->ident_patch) free(t->ident_patch);
  t->ident_patch = p; t->id_bw = bw; t->id_bh = bh; t->id_mtime = (long)st.st_mtime;
}

/* dst_bd = profondeur du BUFFER destination `dst` (≠ s->bit_depth quand TX : le buffer libmtl
 * est toujours 10-bit, cf. input_fmt PLANAR10LE). Le patch reste un plan Y 8-bit. */
static void overlay_ident(struct sess* s, struct target* t, uint8_t* dst, int dst_bd) {
  if (!t->ident_patch || t->id_bw <= 0) return;
  int W = s->width, H = s->height, bw = t->id_bw, bh = t->id_bh;
  if (bw > W || bh > H) return;
  int x0 = W - bw - 8, y0 = 8;
  x0 -= x0 & 1; y0 -= y0 & 1; if (x0 < 0) x0 = 0;
  int deep = (dst_bd >= 10), shift = dst_bd - 8;
  size_t ysz = (size_t)W * H, uvsz = (size_t)(W / 2) * H;
  int bps = deep ? 2 : 1;
  for (int r = 0; r < bh; r++) {
    const uint8_t* p = t->ident_patch + (size_t)r * bw;
    if (deep) { uint16_t* y = (uint16_t*)dst + (size_t)(y0 + r) * W + x0;
                for (int x = 0; x < bw; x++) y[x] = (uint16_t)p[x] << shift; }
    else      { uint8_t*  y = dst + (size_t)(y0 + r) * W + x0;
                for (int x = 0; x < bw; x++) y[x] = p[x]; }
  }
  int neutral = 1 << (dst_bd - 1);
  int ux0 = x0 / 2, ubw = bw / 2;
  uint8_t* uplane = dst + ysz * bps;
  uint8_t* vplane = uplane + uvsz * bps;
  for (int pl = 0; pl < 2; pl++) {
    uint8_t* plane = pl ? vplane : uplane;
    for (int r = 0; r < bh; r++) {
      if (deep) { uint16_t* row = (uint16_t*)plane + (size_t)(y0 + r) * (W / 2) + ux0;
                  for (int x = 0; x < ubw; x++) row[x] = (uint16_t)neutral; }
      else      { uint8_t*  row = plane + (size_t)(y0 + r) * (W / 2) + ux0;
                  for (int x = 0; x < ubw; x++) row[x] = (uint8_t)neutral; }
    }
  }
}

/* Entrelacé : pont entre un CHAMP libmtl (planar 422, 10-bit, plane_h = H/2 lignes, addr/linesize
 * par plan) et un slot shm TRAME PLEINE (planar 422 contigu Y|Cb|Cr, H lignes). Une ligne de champ
 * = une ligne sur deux de la trame, départ `parity` (0|1), stride 2.
 *   dir=0 : champ → trame (RX merge) ; dir=1 : trame → champ (TX split).
 *   shm8=1 : le shm est en 8 bits (conv8 RX / bit_depth 8 TX) → conversion 8↔10 par échantillon ;
 *            le buffer libmtl est TOUJOURS 10-bit (out/input_fmt YUV422PLANAR10LE).
 * 422 : plan 0 (Y) = W échantillons/ligne ; plans 1,2 (Cb,Cr) = W/2 ; tous H lignes en trame. */
static void field_weave(int W, int H, int shm8, struct st_frame* frame,
                        uint8_t* slot, int parity, int dir) {
  int fh = H / 2;
  int bps_shm = shm8 ? 1 : 2;
  size_t plane_off = 0;
  for (int p = 0; p < 3; p++) {
    int pw = p == 0 ? W : W / 2;                  /* échantillons/ligne du plan (422) */
    size_t shm_ls = (size_t)pw * bps_shm;          /* linesize shm (sans padding) */
    uint8_t* shm_p = slot + plane_off;
    uint8_t* fld_p = (uint8_t*)frame->addr[p];
    size_t fld_ls = frame->linesize[p];            /* linesize libmtl (peut avoir du padding) */
    for (int j = 0; j < fh; j++) {
      uint8_t* shm_l = shm_p + (size_t)(2 * j + parity) * shm_ls;
      uint8_t* fld_l = fld_p + (size_t)j * fld_ls;
      if (dir == 0) {                              /* RX : champ(10) → trame(shm) */
        if (shm8) { const uint16_t* sf = (const uint16_t*)fld_l;
                    for (int x = 0; x < pw; x++) shm_l[x] = (uint8_t)(sf[x] >> 2); }
        else memcpy(shm_l, fld_l, (size_t)pw * 2);
      } else {                                     /* TX : trame(shm) → champ(10) */
        if (shm8) { uint16_t* df = (uint16_t*)fld_l;
                    for (int x = 0; x < pw; x++) df[x] = (uint16_t)shm_l[x] << 2; }
        else memcpy(fld_l, shm_l, (size_t)pw * 2);
      }
    }
    plane_off += (size_t)pw * H * bps_shm;          /* plan suivant (trame pleine : H lignes) */
  }
}

/* parité (ligne de départ 0|1) d'un champ : second_field (lib) + ordre de champ (tff).
 * TFF : 1er champ→paires(0), 2e→impaires(1) ; BFF : inversé. DOIT être identique RX↔TX. */
static inline int field_parity(int second_field, int tff) {
  return (second_field ? 1 : 0) ^ (tff ? 0 : 1);
}

/* Capacité SONDE (analyseur 2110-21) gardée STRICTEMENT par l'env TIMING_PARSER=1 : défaut OFF →
 * aucun flag posé, chemin RX inchangé (le HW timestamp du mbuf et le parser ne sont demandés qu'ici).
 * Prérequis réel : PMD DPDK/vfio (le HW timestamp AF_XDP n'est pas fiable, cf. PROBE_2110.md). */
static int tp_wanted(void) {
  const char* e = getenv("TIMING_PARSER");
  return e && atoi(e);
}

/* Accumule le tp_meta d'une trame RX vidéo dans la fenêtre de stats de la session (lockless : appelé
 * seulement depuis video_rx_thread ; write_stats lit+reset). tp = frame->tp[port] (NULL si le parser
 * n'a pas encore de verdict pour cette trame — début de flux/trame incomplète). */
static void accum_tp(struct sess* s, const struct st20_rx_tp_meta* tp) {
  if (!tp) return;
  int v = (int)tp->compliant;                 /* FAILED=0 < WIDE=1 < NARROW=2 */
  if (s->tp_worst < 0 || v < s->tp_worst) {   /* garder le PIRE verdict + sa cause */
    s->tp_worst = v;
    snprintf(s->tp_cause, sizeof(s->tp_cause), "%s", tp->failed_cause);
  }
  if (s->tp_cnt == 0 || tp->cinst_max > s->tp_cinst_max) s->tp_cinst_max = tp->cinst_max;
  if (s->tp_cnt == 0 || tp->vrx_max   > s->tp_vrx_max)   s->tp_vrx_max   = tp->vrx_max;
  if (s->tp_cnt == 0 || tp->vrx_min   < s->tp_vrx_min)   s->tp_vrx_min   = tp->vrx_min;
  int32_t span = tp->vrx_max - tp->vrx_min;               /* amplitude VRX de CETTE trame */
  if (s->tp_cnt == 0 || span > s->tp_vrx_span_max) s->tp_vrx_span_max = span;
  s->tp_cinst_sum += tp->cinst_avg;
  s->tp_vrx_sum   += tp->vrx_avg;
  s->tp_fpt = tp->fpt; s->tp_latency = tp->latency;   /* dernières valeurs de la fenêtre */
  s->tp_cnt++;
}

/* Accumulateur de transport par session. Même contrat que `accum_tp` : appelé UNIQUEMENT
 * depuis le thread RX de la session, lu et remis à zéro par write_stats. */
static void accum_px(struct sess* s, uint32_t pkts_total, uint32_t recv_p, uint32_t recv_r,
                     int complete) {
  s->px_pkts   += pkts_total;
  s->px_recv_p += recv_p;
  s->px_recv_r += recv_r;
  if (!complete) s->px_incomplete++;
  /* ⚠ SIGNÉ AVANT DE SOUSTRAIRE. `pkts_recv` peut dépasser `pkts_total` (paquets redondants
   * comptés côté patte) ; en arithmétique non signée la différence deviendrait un nombre
   * astronomique et le compteur de pertes afficherait des milliards. */
  int64_t manque = (int64_t)pkts_total - (int64_t)recv_p;
  if (manque > 0 && (uint32_t)manque > s->px_pire) s->px_pire = (uint32_t)manque;
  s->px_frames++;
}

static void* video_rx_thread(void* arg) {
  struct sess* s = arg;
  rt_thread_priority("video_rx_thread");
  uint64_t frame_idx_latch = 0;   /* entrelacé : index TRAME latché sur le 1er champ (→ index champ = ×2+sf) */
  while (!s->stop) {
    struct st_frame* frame = st20p_rx_get_frame(s->vh);
    if (!frame) { usleep(1000); continue; }
    if (!frame->addr[0]) { st20p_rx_put_frame(s->vh, frame); continue; }
    if (s->tp_enabled) accum_tp(s, st_frame_tp_meta(frame, MTL_SESSION_PORT_P));
    /* ⚠ SANS GARDE `tp_enabled` : ces compteurs-là ne coûtent rien et ne demandent pas de
     * timestamp matériel, contrairement au parser de timing. Les gater sur la même condition
     * priverait de la mesure de transport tous les moteurs qui tournent sans TIMING_PARSER. */
    accum_px(s, frame->pkts_total, frame->pkts_recv[MTL_SESSION_PORT_P],
             frame->pkts_recv[MTL_SESSION_PORT_R],
             frame->status == ST_FRAME_STATUS_COMPLETE ||
             frame->status == ST_FRAME_STATUS_RECONSTRUCTED);
    /* Timestamp MÉDIA (capture) de la frame, en TAI ns — commun audio/vidéo via PTP (ST 2110-10).
     * st10_get_tai normalise TAI ou media-clk → ns (sampling 90 kHz vidéo). */
    uint64_t mts = media_ts_to_tai(s->st, frame->tfmt, frame->timestamp, MEDIA_CLK_VIDEO);
    /* af_xdp/copy_mode : décodage unique → fan-out vers chaque cible (slot shm).
     * Chaque cible a son propre ring/index + son propre IDENT. */
    /* CHAMP-NATIF : chaque get_frame = 1 CHAMP (entrelacé) ou 1 TRAME (progressif) → 1 grain.
     * Index : entrelacé = index TRAME × 2 + parité de champ (libmtl pose second_field). On LATCH
     * l'index trame sur le 1er champ — le 2e champ arrive ½ période trame plus tard, mxlTimestampToIndex
     * l'arrondirait mal. Index pair = 1er champ (= top en tff). Plus de weave : on écrit le champ tel
     * quel (≡ chemin progressif, juste à l'index champ). */
    int sf = frame->second_field ? 1 : 0;
    uint64_t fi;
    if (s->interlaced) {
      if (!sf) frame_idx_latch = mts ? mxlTimestampToIndex(&s->mrate, mts) : mxlGetCurrentIndex(&s->mrate);
      fi = frame_idx_latch * 2 + (uint64_t)sf;
    } else {
      fi = mts ? mxlTimestampToIndex(&s->mrate, mts) : mxlGetCurrentIndex(&s->mrate);
    }
    for (int ti = 0; ti < s->ntg; ti++) {
      struct target* t = &s->tg[ti];
      mxlGrainInfo gi; uint8_t* payload;
      if (mxlFlowWriterOpenGrain(t->writer, fi, &gi, &payload) != MXL_STATUS_OK) continue;
      size_t _ncp = s->slotsize;                   /* grain = 1 champ/trame = slotsize */
      if (gi.grainSize && (size_t)gi.grainSize < _ncp) _ncp = gi.grainSize;   /* garde-fou débordement */
      if (s->conv8) {                              /* 10→8 bits (planar10 libmtl → planar8 grain) */
        const uint16_t* src = (const uint16_t*)frame->addr[0];
        for (size_t k = 0; k < _ncp; k++) payload[k] = (uint8_t)(src[k] >> 2);
      } else {
        memcpy(payload, frame->addr[0], _ncp);
      }
      /* IDENT : positionné en espace TRAME → désactivé en entrelacé (champ = ½ hauteur). TODO champ-aware. */
      if (t->has_ident && !s->interlaced) { load_ident_patch(t); overlay_ident(s, t, payload, s->bit_depth); }
      gi.validSlices = gi.totalSlices;
      mxlFlowWriterCommitGrain(t->writer, &gi);
      accum_rx_latency(t, mts);
      t->index = fi; t->recv++;
    }
    st20p_rx_put_frame(s->vh, frame);
  }
  return NULL;
}

/* ═══ VIDÉO RX MODE TRANCHE (SLICE_MODE=1, progressive) ════════════════════════
 * API raw st20 (ST20_TYPE_SLICE_LEVEL) : libmtl notifie CHAQUE tranche reçue (notify_slice_ready,
 * contexte lcore tasklet, non bloquant). Les callbacks ne font que poser frame+lignes sous sl_mx
 * et signaler ; le worker (video_rx_slice_thread) convertit RFC4175 BE10 → planar PAR BANDE
 * (SIMD libmtl) directement dans le grain MXL (OpenGrain à la 1ʳᵉ tranche) et committe
 * validSlices=k au fil de l'eau → le consommateur get_slice se réveille PAR TRANCHE (~0,6 ms
 * après l'arrivée fil de la bande, vs ~20 ms trame pleine — banc slice_bench 2026-07-11). */

/* Slot en vol pour `frame` ; create=1 → en réclame un libre. Appelé sous sl_mx. */
static struct sl_inflight* sl_find(struct sess* s, void* frame, int create) {
  int n = s->sl_fb_cnt > 0 && s->sl_fb_cnt < SL_Q ? s->sl_fb_cnt : SL_Q;
  for (int i = 0; i < n; i++) if (s->sl_q[i].frame == frame) return &s->sl_q[i];
  if (!create) return NULL;
  for (int i = 0; i < n; i++)
    if (!s->sl_q[i].frame) {
      struct sl_inflight* e = &s->sl_q[i];
      memset(e, 0, sizeof(*e));
      e->frame = frame; e->seq = ++s->sl_seq;
      return e;
    }
  return NULL;
}

/* lcore tasklet : une tranche de plus est complète pour `frame` (meta->frame_recv_lines cumulées). */
static int rx_sl_slice_ready(void* priv, void* frame, struct st20_rx_slice_meta* meta) {
  struct sess* s = priv;
  pthread_mutex_lock(&s->sl_mx);
  struct sl_inflight* e = sl_find(s, frame, 1);
  if (e) {
    if (!e->timestamp) { e->timestamp = meta->timestamp; e->tfmt = meta->tfmt; }
    if (meta->frame_recv_lines > e->recv_lines) e->recv_lines = meta->frame_recv_lines;
    pthread_cond_signal(&s->sl_cv);
  }
  pthread_mutex_unlock(&s->sl_mx);
  return 0;
}

/* lcore tasklet : trame close côté lib (ownership → app ; st20_rx_put_framebuff au finalize).
 * Trame INCOMPLÈTE (perte fil, flag RECEIVE_INCOMPLETE_FRAME) : les tranches déjà commitées ne se
 * dé-committent pas → on finalise telle quelle (compteur sl_drop) plutôt que bloquer les readers. */
static int rx_sl_frame_ready(void* priv, void* frame, struct st20_rx_frame_meta* meta) {
  struct sess* s = priv;
  if (s->tp_enabled) accum_tp(s, meta->tp[MTL_SESSION_PORT_P]);
  accum_px(s, meta->pkts_total, meta->pkts_recv[MTL_SESSION_PORT_P],
           meta->pkts_recv[MTL_SESSION_PORT_R],
           meta->status == ST_FRAME_STATUS_COMPLETE ||
           meta->status == ST_FRAME_STATUS_RECONSTRUCTED);
  pthread_mutex_lock(&s->sl_mx);
  struct sl_inflight* e = sl_find(s, frame, 1);
  if (!e) { s->sl_drop++; pthread_mutex_unlock(&s->sl_mx); return -EIO; }   /* lib re-put le frame */
  if (!e->timestamp) { e->timestamp = meta->timestamp; e->tfmt = meta->tfmt; }
  if (st_is_frame_complete(meta->status)) e->recv_lines = (uint32_t)s->height;
  else s->sl_drop++;                       /* incomplète : commit en l'état (jamais de blocage lecteur) */
  e->complete = 1;
  pthread_cond_signal(&s->sl_cv);
  pthread_mutex_unlock(&s->sl_mx);
  return 0;
}

/* Copie de bande planar (fan-out cible 2..N : la conversion SIMD n'est faite qu'une fois). */
static void sl_copy_band(uint8_t* dst, const uint8_t* src, int W, int H, int conv8,
                         uint32_t l0, uint32_t nl) {
  size_t ybps = conv8 ? 1 : 2;                 /* octets/échantillon */
  size_t yln = (size_t)W * ybps, cln = (size_t)(W / 2) * ybps;
  size_t yoff = (size_t)l0 * yln, coff = (size_t)l0 * cln;
  size_t ypl = (size_t)H * yln, cpl = (size_t)H * cln;
  memcpy(dst + yoff, src + yoff, (size_t)nl * yln);                          /* Y  */
  memcpy(dst + ypl + coff, src + ypl + coff, (size_t)nl * cln);              /* Cb */
  memcpy(dst + ypl + cpl + coff, src + ypl + cpl + coff, (size_t)nl * cln);  /* Cr */
}

/* Worker : convertit les bandes disponibles de la trame en vol la plus ANCIENNE et committe
 * progressivement. Trame complète → commit final (validSlices=totalSlices) + put_framebuff. */
static void* video_rx_slice_thread(void* arg) {
  struct sess* s = arg;
  rt_thread_priority("video_rx_slice_thread");
  int W = s->width, H = s->height, L = s->slice_lines;
  size_t bpl_be = ((size_t)W / 2) * 5;         /* RFC4175 422-10 : pg2 = 2 px = 5 octets */
  while (!s->stop) {
    /* attendre du travail : la plus ancienne trame en vol avec des lignes non converties */
    pthread_mutex_lock(&s->sl_mx);
    struct sl_inflight* e = NULL;
    for (;;) {
      uint64_t best = UINT64_MAX; e = NULL;
      int n = s->sl_fb_cnt > 0 && s->sl_fb_cnt < SL_Q ? s->sl_fb_cnt : SL_Q;
      for (int i = 0; i < n; i++) {
        struct sl_inflight* q = &s->sl_q[i];
        if (!q->frame) continue;
        if ((q->recv_lines > q->conv_lines || q->complete) && q->seq < best) { best = q->seq; e = q; }
      }
      if (e || s->stop) break;
      struct timespec tw; clock_gettime(CLOCK_REALTIME, &tw);
      tw.tv_nsec += 50 * 1000000;
      if (tw.tv_nsec >= 1000000000) { tw.tv_sec++; tw.tv_nsec -= 1000000000; }
      pthread_cond_timedwait(&s->sl_cv, &s->sl_mx, &tw);
    }
    uint32_t recv = e ? e->recv_lines : 0;
    int complete = e ? e->complete : 0;
    pthread_mutex_unlock(&s->sl_mx);
    if (s->stop) break;
    if (!e) continue;

    if (!e->opened) {                          /* 1ʳᵉ bande : index TAI + OpenGrain (sous-trame) */
      e->mts = media_ts_to_tai(s->st, e->tfmt, e->timestamp, MEDIA_CLK_VIDEO);
      e->fi = e->mts ? mxlTimestampToIndex(&s->mrate, e->mts) : mxlGetCurrentIndex(&s->mrate);
      for (int ti = 0; ti < s->ntg; ti++)
        if (mxlFlowWriterOpenGrain(s->tg[ti].writer, e->fi, &e->gi[ti], &e->payload[ti]) != MXL_STATUS_OK)
          e->payload[ti] = NULL;
      e->opened = 1;
    }
    uint32_t upto = complete ? (uint32_t)H : recv - (recv % (uint32_t)L);
    if (upto > (uint32_t)H) upto = (uint32_t)H;
    if (upto > e->conv_lines) {
      uint32_t l0 = e->conv_lines, nl = upto - l0;
      int ref = -1;
      for (int ti = 0; ti < s->ntg; ti++) if (e->payload[ti]) { ref = ti; break; }
      if (ref >= 0) {
        const uint8_t* src = (const uint8_t*)e->frame + (size_t)l0 * bpl_be;
        uint8_t* pay = e->payload[ref];
        if (s->conv8) {
          uint8_t* y = pay + (size_t)l0 * W;
          uint8_t* b = pay + (size_t)W * H + (size_t)l0 * (W / 2);
          uint8_t* r = pay + (size_t)W * H * 3 / 2 + (size_t)l0 * (W / 2);
          st20_rfc4175_422be10_to_yuv422p8((struct st20_rfc4175_422_10_pg2_be*)src, y, b, r, W, (int)nl);
        } else {
          uint16_t* y = (uint16_t*)(pay + (size_t)l0 * W * 2);
          uint16_t* b = (uint16_t*)(pay + (size_t)W * H * 2 + (size_t)l0 * W);
          uint16_t* r = (uint16_t*)(pay + (size_t)W * H * 3 + (size_t)l0 * W);
          st20_rfc4175_422be10_to_yuv422p10le((struct st20_rfc4175_422_10_pg2_be*)src, y, b, r,
                                              (uint32_t)W, nl);
        }
        for (int ti = 0; ti < s->ntg; ti++) {
          if (ti == ref || !e->payload[ti]) continue;
          sl_copy_band(e->payload[ti], pay, W, H, s->conv8, l0, nl);
        }
        for (int ti = 0; ti < s->ntg; ti++) {
          if (!e->payload[ti]) continue;
          e->gi[ti].validSlices = (uint16_t)(upto / (uint32_t)L);
          if (complete && upto >= (uint32_t)H) e->gi[ti].validSlices = e->gi[ti].totalSlices;
          mxlFlowWriterCommitGrain(s->tg[ti].writer, &e->gi[ti]);   /* commit PROGRESSIF (1..N) */
        }
      }
      e->conv_lines = upto;
    }
    if (complete && e->conv_lines >= (uint32_t)H) {   /* finalize */
      for (int ti = 0; ti < s->ntg; ti++) {
        if (!e->payload[ti]) continue;
        accum_rx_latency(&s->tg[ti], e->mts);
        s->tg[ti].index = e->fi; s->tg[ti].recv++;
      }
      st20_rx_put_framebuff(s->sl_rx, e->frame);
      pthread_mutex_lock(&s->sl_mx);
      e->frame = NULL;                                /* libère le slot (callbacks sous sl_mx) */
      pthread_mutex_unlock(&s->sl_mx);
    }
  }
  return NULL;
}

/* ═══ AUDIO ═══════════════════════════════════════════════════════════════════ */
/* st30p délivre le payload L24 du fil = BIG-ENDIAN, entrelacé par échantillon (chs canaux × 3 o).
 * On le CONVERTIT en samples float32 par canal (contrat MXL audio/float32) et on l'écrit dans le
 * flux continu MXL, à l'INDEX SAMPLE TAI (mxlTimestampToIndex sur la grille 48 kHz) → MÊME grille
 * PTP que la vidéo (phase-lock A/V structurel). */
/* bobi.studio (0.44.2) : comble par du SILENCE (samples float32 = 0.0) le trou laissé par une
 * ou plusieurs trames RX droppées (framebuff pool empty, cf. volet 2/RX_AUDIO_SESSION back-
 * pressure) — sinon le span sauté reste LISIBLE côté consommateur avec du VIEUX contenu d'anneau
 * (bouillie audio). Écrit [t->a_end, idx) AVANT le chunk réel. Borné à ~250 ms : au-delà, le trou
 * dépasse la marge utile de l'anneau → pas de comblement (ré-ancrage franc sur idx, le trou reste
 * un vrai trou plutôt qu'un mur de silence disproportionné). No-op au tout premier chunk
 * (a_primed=0) et si aucun trou (idx <= a_end, cas normal). */
static void audio_gapfill_silence(struct sess* s, struct target* t, uint64_t idx) {
  if (!t->a_primed || idx <= t->a_end) return;
  uint64_t gap = idx - t->a_end;
  uint64_t max_gap_samples = (uint64_t)(0.25 * (double)s->srate.numerator / (double)s->srate.denominator);
  if (gap > max_gap_samples) return;   /* trou > ~250 ms : pas de comblement, ré-ancrage franc */
  mxlMutableWrappedMultiBufferSlice gslc; memset(&gslc, 0, sizeof(gslc));
  if (mxlFlowWriterOpenSamples(t->writer, t->a_end, gap, &gslc) != MXL_STATUS_OK) return;
  for (size_t c = 0; c < gslc.count; c++) {
    for (int f = 0; f < 2; f++) {          /* 2 fragments (wrap d'anneau), même modèle que le chunk réel */
      size_t fb = gslc.base.fragments[f].size;
      if (!fb) continue;
      void* dst = (uint8_t*)gslc.base.fragments[f].pointer + c * gslc.stride;
      memset(dst, 0, fb);                 /* 0x00000000 == 0.0f en IEEE754 : memset(0) valide */
    }
  }
  mxlFlowWriterCommitSamples(t->writer);
  t->a_silence_filled += gap;
  uint64_t tnow = mono_ns();
  if (!t->a_silence_log_ns || tnow - t->a_silence_log_ns > 60000000000ULL) {   /* throttle 1/min */
    fprintf(stderr, "mtl_rx[audio] %s: gap-fill silence %llu échantillon(s) (RX drop) — "
                     "total comblé=%llu\n",
            t->shm_path, (unsigned long long)gap, (unsigned long long)t->a_silence_filled);
    t->a_silence_log_ns = tnow;
  }
}

static void* audio_rx_thread(void* arg) {
  struct sess* s = arg;
  rt_thread_priority("audio_rx_thread");
  int chs = s->channels;
  while (!s->stop) {
    struct st30_frame* frame = st30p_rx_get_frame(s->ah);
    if (!frame) { usleep(500); continue; }
    const uint8_t* src = (const uint8_t*)frame->addr;
    size_t n = frame->data_size / (size_t)(chs * 3);   /* samples par canal dans ce chunk (~48) */
    if (!n) { st30p_rx_put_frame(s->ah, frame); continue; }
    uint64_t mts = media_ts_to_tai(s->st, frame->tfmt, frame->timestamp, MEDIA_CLK_AUDIO);
    for (int ti = 0; ti < s->ntg; ti++) {        /* fan-out (même source audio → N flux) */
      struct target* t = &s->tg[ti];
      uint64_t idx = mts ? mxlTimestampToIndex(&s->srate, mts)
                         : mxlGetCurrentIndex(&s->srate) - n;
      audio_gapfill_silence(s, t, idx);
      mxlMutableWrappedMultiBufferSlice slc; memset(&slc, 0, sizeof(slc));
      if (mxlFlowWriterOpenSamples(t->writer, idx, n, &slc) != MXL_STATUS_OK) continue;
      size_t stride = slc.stride;
      for (size_t c = 0; c < slc.count; c++) {
        size_t pos = 0;
        for (int f = 0; f < 2; f++) {            /* 2 fragments (wrap d'anneau) */
          size_t fb = slc.base.fragments[f].size;
          if (!fb) continue;
          float* dst = (float*)((uint8_t*)slc.base.fragments[f].pointer + c * stride);
          size_t cnt = fb / 4;
          for (size_t k = 0; k < cnt; k++) {
            const uint8_t* p = src + ((pos + k) * (size_t)chs + c) * 3;   /* L24 BE entrelacé */
            int32_t v = (int32_t)((uint32_t)p[0] << 16 | (uint32_t)p[1] << 8 | p[2]);
            if (v & 0x800000) v |= ~0xFFFFFF;    /* sign-extend 24→32 bits */
            dst[k] = (float)v / 8388608.0f;
          }
          pos += cnt;
        }
      }
      mxlFlowWriterCommitSamples(t->writer);
      t->index = idx; t->recv++;
      t->a_end = idx + n; t->a_primed = 1;
    }
    st30p_rx_put_frame(s->ah, frame);
  }
  return NULL;
}

/* ═══ communs ═════════════════════════════════════════════════════════════════ */

static enum st_fps to_st_fps(double f) {
  if (fabs(f - 23.98) < 0.05) return ST_FPS_P23_98;
  if (fabs(f - 24.0) < 0.05) return ST_FPS_P24;
  if (fabs(f - 25.0) < 0.05) return ST_FPS_P25;
  if (fabs(f - 29.97) < 0.05) return ST_FPS_P29_97;
  if (fabs(f - 30.0) < 0.05) return ST_FPS_P30;
  if (fabs(f - 50.0) < 0.05) return ST_FPS_P50;
  if (fabs(f - 59.94) < 0.1) return ST_FPS_P59_94;
  if (fabs(f - 60.0) < 0.05) return ST_FPS_P60;
  if (fabs(f - 100.0) < 0.05) return ST_FPS_P100;
  if (fabs(f - 120.0) < 0.05) return ST_FPS_P120;
  return ST_FPS_P25;
}

/* ptime audio (ms) → enum ST30_PTIME. DOIT matcher le flux (a=ptime du SDP) sinon « pkt len
 * mismatch » → tous les paquets droppés. Défaut 1 ms. framebuff_size reste 1 ms (multiple du
 * paquet pour 1/0.25/0.125 ms → chunk shm inchangé). */
static enum st30_ptime to_st30_ptime(double ms) {
  if (fabs(ms - 0.125) < 0.01) return ST30_PTIME_125US;
  if (fabs(ms - 0.25)  < 0.01) return ST30_PTIME_250US;
  if (fabs(ms - 0.333) < 0.02) return ST30_PTIME_333US;
  if (fabs(ms - 4.0)   < 0.05) return ST30_PTIME_4MS;
  return ST30_PTIME_1MS;
}

/* Crée le writer MXL de chaque cible RX/simu (le flowDef dépend du kind ; mrate/srate déjà posés). */
static int open_targets(struct sess* s) {
  for (int ti = 0; ti < s->ntg; ti++) {
    char* fd = (s->kind == K_AUDIO) ? build_audio_flowdef(s, &s->tg[ti])
             : (s->kind == K_DATA)  ? build_data_flowdef(s, &s->tg[ti])
             :                        build_video_flowdef(s, &s->tg[ti]);
    if (open_writer(&s->tg[ti], fd) != 0) return -1;
  }
  return 0;
}

/* Nettoyage d'un setup RX vidéo PARTIEL (échec après st20p_rx_create) : on NE DOIT JAMAIS orpheliner
 * la session libmtl. Une session st20p créée puis abandonnée continue de recevoir les paquets, remplit
 * son pool de framebuffers (jamais drainé car le thread RX n'a pas démarré) → « framebuff pool empty,
 * back-pressure » et 0 trame remontée. On libère writers MXL + handle st20p. */
static void _abort_video_setup(struct sess* s) {
  for (int ti = 0; ti < s->ntg; ti++)
    if (s->tg[ti].writer) { mxlReleaseFlowWriter(g_mxl, s->tg[ti].writer); s->tg[ti].writer = NULL; }
  if (s->vh) { st20p_rx_free(s->vh); s->vh = NULL; }
}

/* Setup RX vidéo MODE TRANCHE (raw st20, ST20_TYPE_SLICE_LEVEL). Prérequis vérifiés par
 * l'appelant : SLICE_MODE=1, progressive, height divisible par SLICE_LINES. */
static int setup_video_slice_rx(struct sess* s) {
  s->slice_on = 1;
  s->slice_lines = slice_lines_env();
  s->conv8 = (s->bit_depth == 8);
  if (s->ring < 2) s->ring = 2; if (s->ring > SL_Q) s->ring = SL_Q;
  s->src_framesize = ((size_t)s->width / 2) * 5 * (size_t)s->height;   /* RFC4175 BE10 packé */
  s->slotsize = (size_t)(s->conv8 ? 2 : 4) * (size_t)s->width * (size_t)s->height;  /* grain planar */
  s->shm_slotsize = s->slotsize;
  s->mrate = fps_to_rational(s->fps);
  pthread_mutex_init(&s->sl_mx, NULL); pthread_cond_init(&s->sl_cv, NULL);
  s->sl_fb_cnt = (uint16_t)s->ring;

  struct st20_rx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_v_sl";
  ops.priv = s;
  ops.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.ip_addr[MTL_SESSION_PORT_P]);
  snprintf(ops.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.ip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.payload_type = s->payload_type;
  ops.width = s->width; ops.height = s->height; ops.fps = to_st_fps(s->fps);
  ops.interlaced = false;
  ops.fmt = ST20_FMT_YUV_422_10BIT;
  ops.type = ST20_TYPE_SLICE_LEVEL;
  ops.slice_lines = (uint32_t)s->slice_lines;
  ops.framebuff_cnt = (uint16_t)s->ring;
  ops.notify_frame_ready = rx_sl_frame_ready;
  ops.notify_slice_ready = rx_sl_slice_ready;
  /* trame incomplète notifiée quand même : les tranches déjà commitées ne se dé-committent pas,
   * on FINALISE en l'état plutôt que laisser un grain partiel bloquer les lecteurs whole-frame. */
  ops.flags = ST20_RX_FLAG_RECEIVE_INCOMPLETE_FRAME;
  if (tp_wanted()) { ops.flags |= ST20_RX_FLAG_TIMING_PARSER_META; s->tp_enabled = 1; s->tp_worst = -1; }

  s->sl_rx = st20_rx_create(s->st, &ops);
  if (!s->sl_rx) {
    fprintf(stderr, "mtl_rx: st20_rx_create SLICE fail (video %s:%d)\n", s->mcast, s->udp_port);
    pthread_mutex_destroy(&s->sl_mx); pthread_cond_destroy(&s->sl_cv);
    return -1;
  }
  if (open_targets(s) != 0) {
    for (int ti = 0; ti < s->ntg; ti++)
      if (s->tg[ti].writer) { mxlReleaseFlowWriter(g_mxl, s->tg[ti].writer); s->tg[ti].writer = NULL; }
    st20_rx_free(s->sl_rx); s->sl_rx = NULL;
    pthread_mutex_destroy(&s->sl_mx); pthread_cond_destroy(&s->sl_cv);
    return -1;
  }
  int hi = 0;
  for (int ti = 0; ti < s->ntg; ti++) if (s->tg[ti].has_ident) hi = 1;
  fprintf(stderr, "mtl_rx[video RX SLICE] %dx%dp fps=%.2f pt=%d mc=%s:%d tranche=%d lignes (%d/trame)"
          " ring=%d%s → %d cible(s):", s->width, s->height, s->fps, s->payload_type, s->mcast,
          s->udp_port, s->slice_lines, s->height / s->slice_lines, s->ring,
          hi ? " [IDENT ignoré en mode tranche]" : "", s->ntg);
  for (int ti = 0; ti < s->ntg; ti++) fprintf(stderr, " %s", s->tg[ti].shm_path);
  fprintf(stderr, "\n");
  if (pthread_create(&s->thread, NULL, video_rx_slice_thread, s) != 0) {
    for (int ti = 0; ti < s->ntg; ti++)
      if (s->tg[ti].writer) { mxlReleaseFlowWriter(g_mxl, s->tg[ti].writer); s->tg[ti].writer = NULL; }
    st20_rx_free(s->sl_rx); s->sl_rx = NULL;
    pthread_mutex_destroy(&s->sl_mx); pthread_cond_destroy(&s->sl_cv);
    return -1;
  }
  s->started = 1;
  return 0;
}

static int setup_video(struct sess* s) {
  /* MODE TRANCHE (SLICE_MODE=1) : bascule env-gatée vers le chemin raw st20 par tranches.
   * Éligibilité : progressive + height divisible par SLICE_LINES (le flowdef MXL exige des
   * tranches égales). Inéligible → repli whole-frame silencieux (log). */
  if (slice_wanted() && !s->interlaced) {
    if (s->height % slice_lines_env() == 0) return setup_video_slice_rx(s);
    fprintf(stderr, "mtl_rx[slice] RX %s:%d inéligible (h=%d non divisible par %d) → whole-frame\n",
            s->mcast, s->udp_port, s->height, slice_lines_env());
  }
  s->copy_mode = 1;   /* af_xdp/kernel uniquement dans ce contexte */
  s->out_fmt = ST_FRAME_FMT_YUV422PLANAR10LE;   /* converter présent ; conv 10→8 nous-mêmes */
  s->conv8 = (s->bit_depth == 8);
  s->plane_h = s->interlaced ? s->height / 2 : s->height;
  if (s->ring > 8) s->ring = 8; if (s->ring < 2) s->ring = 2; if (s->ring > MAX_FB) s->ring = MAX_FB;

  struct st20p_rx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_v";
  ops.priv = s;
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.ip_addr[MTL_SESSION_PORT_P]);
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.ip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.port.payload_type = s->payload_type;
  ops.width = s->width; ops.height = s->height; ops.fps = to_st_fps(s->fps);
  ops.interlaced = s->interlaced ? true : false;
  ops.transport_fmt = ST20_FMT_YUV_422_10BIT;
  ops.output_fmt = ST_FRAME_FMT_YUV422PLANAR10LE;
  ops.device = ST_PLUGIN_DEVICE_AUTO;
  ops.framebuff_cnt = s->ring;
  ops.notify_frame_available = frame_available;
  /* af_xdp → copy_mode : frames internes MTL, on memcpy (pas d'ext-frame DMA). */
  /* SONDE 2110-21 (TIMING_PARSER=1) : demande le parser de timing par trame (Cinst/VRX/verdict).
   * Défaut OFF → flag jamais posé, comportement de prod inchangé. */
  if (tp_wanted()) { ops.flags |= ST20P_RX_FLAG_TIMING_PARSER_META; s->tp_enabled = 1; s->tp_worst = -1; }

  s->vh = st20p_rx_create(s->st, &ops);
  if (!s->vh) { fprintf(stderr, "mtl_rx: st20p_rx_create fail (video %s:%d)\n", s->mcast, s->udp_port); return -1; }
  s->src_framesize = st20p_rx_frame_size(s->vh);   /* = taille CHAMP si entrelacé (st_frame_size/2) */
  s->slotsize = s->conv8 ? s->src_framesize / 2 : s->src_framesize;
  /* Champ-natif : 1 grain = 1 CHAMP (= slotsize, déjà ½ trame en entrelacé côté libmtl) ou 1 trame
   * en progressif. Plus de ×2 (plus de weave). shm_slotsize == slotsize. */
  s->shm_slotsize = s->slotsize;
  s->mrate = fps_to_rational(s->fps);            /* grain_rate du flowDef + grille TAI vidéo */
  if (open_targets(s) != 0) { _abort_video_setup(s); return -1; }
  fprintf(stderr, "mtl_rx[video] %dx%d%s fps=%.2f pt=%d mc=%s:%d slot=%zu ring=%d → %d cible(s):",
          s->width, s->height, s->interlaced ? "i" : "p", s->fps, s->payload_type,
          s->mcast, s->udp_port, s->slotsize, s->ring, s->ntg);
  for (int ti = 0; ti < s->ntg; ti++) fprintf(stderr, " %s", s->tg[ti].shm_path);
  fprintf(stderr, "\n");
  if (pthread_create(&s->thread, NULL, video_rx_thread, s) != 0) { _abort_video_setup(s); return -1; }
  s->started = 1;
  return 0;
}

/* ═══ TX commun (lecture du flux d'entrée câblé via un reader MXL) ═════════════ */
/* bobi.studio: DÉCOUPLAGE source↔session — reconcile (writer unique) pose la source désirée ; le
 * thread TX la prend et rouvre son reader. Voir struct target (want_shm/src_seq). */
static void tx_set_source(struct target* t, const char* src) {
  t->src_seq++;                                 /* impair : écriture en cours */
  __sync_synchronize();
  snprintf(t->want_shm, sizeof(t->want_shm), "%s", src ? src : "");
  __sync_synchronize();
  t->src_seq++;                                 /* pair : publié */
}
/* Retourne 1 si la source a changé (⇒ le thread relâche son reader pour rouvrir sur t->shm_path). */
static int tx_take_source(struct target* t) {
  uint32_t seq = t->src_seq;
  if (seq == t->src_seq_seen) return 0;         /* rien de neuf (séquence paire déjà consommée) */
  char local[300]; uint32_t s0;
  do { s0 = t->src_seq & ~1u; __sync_synchronize();
       snprintf(local, sizeof(local), "%s", t->want_shm);
       __sync_synchronize();
  } while (s0 != t->src_seq);                    /* lecture torn (écriture concurrente) → retry */
  snprintf(t->shm_path, sizeof(t->shm_path), "%s", local);
  t->src_seq_seen = s0;
  return 1;
}

/* Crée (paresseusement) le reader MXL du flux d'entrée câblé. -1 si le flux n'existe pas encore
 * (le producteur peut démarrer après nous) → l'appelant réessaie. */
static int open_reader(struct target* t) {
  char id[37]; flow_id_str(flow_name(t->shm_path), id);
  if (mxlCreateFlowReader(g_mxl, id, NULL, &t->reader) != MXL_STATUS_OK) { t->reader = NULL; return -1; }
  t->tx_src_idx_init = 0;   /* nouveau reader → ré-amorce la détection de flux figé */
  t->anc_fmt_init = 0;      /* …et la résolution du codage ANC (le producteur a pu être migré) */
  return 0;
}

/* Reconnexion sur flux SOURCE figé : un producteur redéployé (ex. multiview) DÉTRUIT puis RECRÉE son
 * flux MXL SOUS LE MÊME NOM → l'ancien ring orphelin reste lisible et reader_latest/reader_field
 * renvoient indéfiniment le DERNIER grain (index figé, AUCUNE erreur → aucune libération) → la sortie
 * TX se fige sur la dernière trame jusqu'à un re-câble manuel. Si l'index source n'avance plus au-delà
 * de TX_REOPEN_STALE_NS, on libère le reader → open_reader le recrée sur la génération COURANTE du flux
 * (résolution par nom). Sans danger pour une source légitimement statique (elle relit son grain).
 * `idx` = index du grain courant ; `tnow` = mono_ns() ; à appeler APRÈS une lecture réussie. */
#define TX_REOPEN_STALE_NS 1000000000ULL   /* 1 s sans avancée d'index ⇒ flux périmé → reconnexion */
static void tx_reopen_if_stale(struct target* t, uint64_t idx, uint64_t tnow) {
  if (!t->tx_src_idx_init || idx != t->tx_src_idx) {
    t->tx_src_idx = idx; t->tx_src_idx_ns = tnow; t->tx_src_idx_init = 1;
    t->last_fresh_ns = tnow;   /* grain SOURCE neuf (cf. champ, pour "source_live" dans write_stats) */
    return;
  }
  if (tnow - t->tx_src_idx_ns > TX_REOPEN_STALE_NS) {
    /* Détacher le reader de l'ancien ring PUIS réclamer l'orphelin : le flux périmé a son writer
     * MORT (producteur redéployé) mais reste « vivant » dans le domaine tant qu'un reader y est
     * attaché et qu'aucun GC ne passe → une simple réouverture par nom retomberait DESSUS (c'est
     * pourquoi le re-câble immédiat ne suffisait qu'après un délai laissant le GC opérer). On force
     * donc le GC ici → open_reader (tour suivant) résout le NOUVEAU flux vivant du producteur. */
    mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL;
    mxlGarbageCollectFlows(g_mxl);
  }
}

/* Dernier grain dispo (NON bloquant) → 0 + (gi,payload), ou -1. Flux recréé (producteur redéployé)
 * → libère le reader pour forcer une recréation au tour suivant. */
/* ─── Lecture du grain SOURCE d'un TX vidéo trame-entière (0.80.3) ───────────────────────────────
 * ★ VISER LE GRAIN SUIVANT ET L'ATTENDRE, au lieu d'attraper la tête à l'instant du tour.
 *
 * Avant : `GetRuntimeInfo` puis `GetGrainNonBlocking(rt.headIndex)` — on prenait ce qui traînait, à
 * l'instant du tour d'émission. Or deux horloges à 50 Hz sont en jeu (le commit du producteur MXL et
 * le tour d'émission libmtl) et leur phase relative DÉRIVE lentement. Quand le tour tombe PENDANT le
 * commit du producteur, la tête n'a pas encore avancé → on relit le MÊME grain (répétition VISIBLE)
 * et au tour suivant la tête a avancé de deux → le grain intermédiaire est JETÉ, jamais émis.
 *
 * Mesuré à Horace les 6-7 août, cinq épisodes sur deux murs indépendants :
 *   • déclenchement à la MÊME phase à chaque fois — +0,062 grain (1,24 ms après une frontière),
 *     constant à 0,009 grain près (180 µs) sur 11 h et deux murs ;
 *   • 40 à 46 min par épisode, 13 à 16 trames rejouées/s au pic, récurrence ~6 h par mur ;
 *   • le producteur, lui, écrivait une suite d'index PARFAITE : zéro anomalie sur 1905 relevés
 *     pendant une montée, même phase de commit que le mur témoin. Ce n'est pas lui.
 *   • `fps_source = fps − répétitions/s` vérifié au centième → chaque répétition = un grain perdu.
 *
 * Après : on vise `tx_src_idx + 1` (le grain qu'on n'a pas encore émis) et on l'ATTEND avec
 * `mxlFlowReaderGetGrain`, qui rend dès le commit. Coût : le résidu de commit, quelques ms, et
 * seulement quand le tour tombe dans la fenêtre. AUCUNE latence permanente, AUCUN grain jeté.
 * (`head − 1` aurait donné 20 ms de marge — mais 20 ms de latence sur CHAQUE trame : exclu.)
 *
 * Deux propriétés du code d'origine sont conservées, et elles sont essentielles :
 *   1. RATTRAPAGE — au-delà de TX_CATCHUP_MAX grains de retard (ou si notre index est devant la
 *      tête, ce qui arrive après une réouverture), on saute à la tête. Jamais à la traîne.
 *   2. REPLI — si le grain visé ne vient pas dans le budget (producteur en retard ou mort), on relit
 *      la tête en non bloquant, c'est-à-dire l'ANCIEN comportement. Ne JAMAIS renvoyer -1 sur une
 *      simple attente expirée : l'appelant émettrait un `memset` — une trame NOIRE. Une répétition
 *      vaut toujours mieux qu'un noir.
 * Le correctif ne peut donc que faire mieux que l'ancien code, jamais pire.
 */
#define TX_WAIT_GRAIN_NS  5000000ULL   /* 5 ms : couvre la fenêtre de commit mesurée (~3 ms) en
                                        * laissant 15 ms des 20 ms de budget de trame. */
#define TX_CATCHUP_MAX    3            /* au-delà, on saute à la tête plutôt que rejouer l'histoire */

/* ─── Un grain n'est ÉMISSIBLE que COMPLET ────────────────────────────────────────────────────
 * `mxlFlowReaderGetGrain*` rend MXL_STATUS_OK dès que le grain est LISIBLE. Sur un flux TRANCHÉ
 * c'est vrai dès les premières tranches commitées : les bandes pas encore écrites portent alors
 * encore la trame PRÉCÉDENTE. Un TX qui accepte ce grain émet une image DÉCHIRÉE, avec une
 * frontière qui se DÉPLACE lentement — la phase entre l'horloge de commit du producteur et le
 * tour d'émission libmtl dérivant (cf. le pavé de reader_latest ci-dessus). C'est le « trait qui
 * balaye l'écran » signalé en exploitation le 2026-08-13/14.
 *
 * Le défaut était latent : il ne se voit que si le producteur commite SOUVENT par trame. Le
 * réglage `mxl_sync_batch=2` (target RDMA commitant toutes les 2 tranches, soit 15 fois par
 * trame) l'a rendu permanent ; le repasser au défaut du SDK l'a masqué sans le corriger.
 *
 * Sur un flux NON tranché `totalSlices == 1` → la garde est un no-op : aucun changement de
 * comportement, aucune latence ajoutée. */
static inline int grain_complet(const mxlGrainInfo* gi) {
  uint16_t total = gi->totalSlices ? gi->totalSlices : 1;
  return gi->validSlices >= total;
}

static int reader_latest(struct target* t, mxlGrainInfo* gi, uint8_t** payload) {
  mxlFlowRuntimeInfo rt;
  mxlStatus st = mxlFlowReaderGetRuntimeInfo(t->reader, &rt);
  if (st == MXL_STATUS_OK && rt.headIndex != MXL_UNDEFINED_INDEX) {
    uint64_t cible = rt.headIndex;
    if (t->tx_src_idx_init) {
      uint64_t suivant = t->tx_src_idx + 1;
      /* écart > 0 : on est en retard sur la tête ; == -1 : le suivant n'est pas encore publié
       * (cas NOMINAL quand le producteur est en train de le commiter) → c'est lui qu'on attend. */
      int64_t ecart = (int64_t)rt.headIndex - (int64_t)suivant;
      if (ecart >= -1 && ecart <= TX_CATCHUP_MAX) cible = suivant;
    }
    /* GetGrainSlice(…, VALID_SLICES_ALL) = attendre le grain COMPLET, pas seulement lisible.
     * Sur un flux non tranché c'est strictement l'ancien GetGrain (1 tranche = tout). */
    st = mxlFlowReaderGetGrainSlice(t->reader, cible, MXL_GRAIN_VALID_SLICES_ALL,
                                    TX_WAIT_GRAIN_NS, gi, payload);
    if (st == MXL_STATUS_OK && grain_complet(gi)) return 0;
    /* Budget épuisé : REPLI (cf. propriété 2 ci-dessus — ne JAMAIS renvoyer -1 sur une simple
     * attente expirée, l'appelant émettrait un memset, donc une trame NOIRE). Le repli doit être
     * COMPLET lui aussi : rejouer une trame entière vaut mieux qu'en émettre une déchirée. On
     * tente la tête, puis la trame d'avant. */
    for (int _r = 0; _r < 2; _r++) {
      uint64_t _c = rt.headIndex - (uint64_t)_r;
      if (_r && rt.headIndex == 0) break;
      if (_r == 0 && cible == rt.headIndex) continue;   /* déjà tenté à l'instant */
      st = mxlFlowReaderGetGrainNonBlocking(t->reader, _c, gi, payload);
      if (st == MXL_STATUS_OK && grain_complet(gi)) return 0;
    }
  }
  if (st == MXL_ERR_FLOW_NOT_FOUND || st == MXL_ERR_FLOW_INVALID) {
    mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL;
  }
  return -1;
}

/* Champ-natif TX — émet les 2 champs de la MÊME trame COMPLÈTE, dominance pilotée par `tff`.
 * Mesuré : la RX livre des grains-champs CONSÉCUTIFS réguliers (20 ms, 50/s, aucun saut). Donc on
 * émet simplement la dernière trame COMPLÈTE = (trame de tête − 1) → ses 2 champs sont garantis
 * écrits (anti-peigne), latence constante d'1 trame, AUCUN rattrapage/compteur → la cadence suit la
 * source sans dérive ni répétition (le ×2-catch-up de 0.28.2 saccadait). Grains : index TRAME×2 +
 * parité (pair = TOP, impair = BOTTOM). 1er champ (field==0) = TOP(pair) en TFF, BOTTOM(impair) en BFF.
 *  - field==0 : ancre field_base sur la trame de tête−1, parité selon tff.
 *  - field==1 : l'autre parité de la MÊME trame. */
static int reader_field(struct target* t, int field, int tff, mxlGrainInfo* gi, uint8_t** payload) {
  mxlFlowRuntimeInfo rt;
  mxlStatus st = mxlFlowReaderGetRuntimeInfo(t->reader, &rt);
  if (st == MXL_STATUS_OK && rt.headIndex != MXL_UNDEFINED_INDEX) {
    uint64_t head = rt.headIndex;
    uint64_t latest_frame = ((head & 1ULL) ? head - 1 : head) / 2;
    uint64_t target;
    if (field == 0) {
      uint64_t frame = latest_frame ? latest_frame - 1 : 0;   /* dernière trame COMPLÈTE */
      t->field_base = frame * 2 + (tff ? 0 : 1);              /* TOP(pair) en TFF, BOTTOM(impair) en BFF */
      target = t->field_base;
    } else {
      target = tff ? (t->field_base + 1) : (t->field_base - 1);   /* 2e champ, MÊME trame */
    }
    st = mxlFlowReaderGetGrainNonBlocking(t->reader, target, gi, payload);
    if (st == MXL_STATUS_OK && grain_complet(gi)) return 0;
    /* Champ incomplet (producteur tranché en cours de commit) : reculer d'UNE TRAME (même
     * parité, donc target − 2) plutôt que d'émettre un demi-champ neuf collé sur l'ancien. */
    if (st == MXL_STATUS_OK && target >= 2) {
      st = mxlFlowReaderGetGrainNonBlocking(t->reader, target - 2, gi, payload);
      if (st == MXL_STATUS_OK && grain_complet(gi)) return 0;
    }
  }
  if (st == MXL_ERR_FLOW_NOT_FOUND || st == MXL_ERR_FLOW_INVALID) {
    mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL;
  }
  return -1;
}

/* ═══ VIDÉO TX (flux MXL → wire, st20p_tx) ═════════════════════════════════════ */

/* Feeder TX : lit le DERNIER grain du flux MXL d'entrée câblé et l'émet. Up-shift 8→10 si le
 * pipeline est en 8 bits (transport 2110-20 = 422-10). Le reader est créé PARESSEUSEMENT (le
 * producteur peut démarrer après nous). Pacing assuré par ST20P_TX_FLAG_BLOCK_GET. */
static void* video_tx_thread(void* arg) {
  struct sess* s = arg;
  struct target* t = &s->tg[0];                 /* la cible TX = l'unique flux d'entrée câblé */
  size_t out_size = st20p_tx_frame_size(s->vth);   /* = taille CHAMP si entrelacé */
  uint64_t latched_fi = 0;                      /* entrelacé : index latché sur le 1er champ */
  /* Période nominale entre deux get_frame (BLOCK_GET pace à la cadence de la session ; en
   * entrelacé un get_frame = un CHAMP). Sert au compteur `late` : un get_frame qui revient
   * > 1,5 période après le précédent = la session a raté au moins un epoch (trame perdue). */
  uint64_t period_ns = s->fps > 0 ? (uint64_t)(1e9 / s->fps) : 0;
  if (s->interlaced) period_ns /= 2;
  while (!s->stop) {
    /* bobi.studio: source permutée à chaud par reconcile (sans re-créer la session) → rouvrir le reader
     * sur la nouvelle source. Source vidée ("") → open_reader échoue → thread muet (slot silencieux). */
    if (tx_take_source(t) && t->reader) {
      mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL; t->tx_src_idx_init = 0;
    }
    /* MODE STATIQUE (cf. video_tx_slice_thread) : PROGRESSIF seulement. En entrelacé il faudrait
     * servir le champ de la bonne parité à chaque get_frame ; tant que ce n'est pas fait, un slot
     * entrelacé garde son producteur — limite ASSUMÉE et explicite, pas un trou silencieux (le
     * contrôleur ne publie pas de `static_frame` pour un slot entrelacé). */
    int statique = (!t->shm_path[0] && t->static_frame[0] && !s->interlaced);
    if (!statique && !t->reader) {
      t->last_feed_ns = 0;   /* flux pas encore là : ne pas compter l'attente comme du retard */
      t->alive_ns = mono_ns();   /* attendre un câblage n'est pas un wedge */
      if (open_reader(t) != 0) { usleep(20000); continue; }
    }
    if (statique && t->reader) {   /* câble retiré → on lâche le reader et on passe en statique */
      mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL; t->tx_src_idx_init = 0;
    }
    if (statique && load_static_frame(t) != 0) {   /* pas (encore) de trame publiée → slot muet */
      t->alive_ns = mono_ns();
      usleep(20000); continue;
    }
    struct st_frame* frame = st20p_tx_get_frame(s->vth);   /* bloque → pacing à fps */
    if (!frame) { usleep(1000); continue; }
    uint64_t tnow = mono_ns();
    t->alive_ns = tnow;      /* la session transmet (frames libérées par MTL) */
    if (statique) {
      /* Remplissage UNE FOIS par framebuffer (adresses stables, cf. sf_fb_ptr) : après la première
       * rotation, ré-émettre une trame statique ne coûte plus aucune copie. */
      void* dst = frame->addr[0];
      int slot = -1;
      for (int k = 0; k < t->sf_fb_n; k++) if (t->sf_fb_ptr[k] == dst) { slot = k; break; }
      if (slot < 0 && t->sf_fb_n < SF_FB_MAX) {
        slot = t->sf_fb_n++; t->sf_fb_ptr[slot] = dst; t->sf_fb_stamp[slot] = 0;
      }
      if (slot < 0 || t->sf_fb_stamp[slot] != t->sf_gen) {
        /* dst = trame planaire TOUJOURS 10 bits (input_fmt PLANAR10LE) ; source 8 bits → up-shift,
         * exactement comme le chemin câblé (profondeur déduite de la TAILLE). */
        size_t exp8 = (size_t)2 * (size_t)s->width * (size_t)s->height;
        if (t->sf_size == exp8) {
          uint16_t* d16 = (uint16_t*)dst;
          for (size_t k = 0; k < t->sf_size; k++) d16[k] = (uint16_t)t->sf_buf[k] << 2;
        } else {
          memcpy(dst, t->sf_buf, out_size < t->sf_size ? out_size : t->sf_size);
        }
        if (slot >= 0) t->sf_fb_stamp[slot] = t->sf_gen;
      }
      st20p_tx_put_frame(s->vth, frame);
      t->index++; t->recv++;
      continue;                                  /* aucun `late` : il n'y a pas de source à rater */
    }
    if (period_ns && t->last_feed_ns) {
      /* NB epoch-shift : ici PAS de biais de contre-pression (contrairement au mode tranche,
       * cf. video_tx_slice_thread) — le shift retarde chaque libération d'un offset CONSTANT,
       * qui s'annule entre deux get_frame consécutifs ; la lecture source est non bloquante. */
      uint64_t gap = tnow - t->last_feed_ns;
      if (gap > period_ns + period_ns / 2) {
        uint64_t missed = (gap + period_ns / 2) / period_ns;   /* périodes écoulées (arrondi) */
        t->late += missed > 1 ? missed - 1 : 1;
      }
    }
    t->last_feed_ns = tnow;
    if (s->interlaced) {
      /* CHAMP-NATIF : 1 get_frame = 1 CHAMP à émettre (libmtl pose second_field). On lit le grain-champ
       * de même parité (reader_field) et on le copie directement dans le buffer libmtl (= 1 champ),
       * up-shift 8→10 si pipeline 8 bits. Plus de tx_frame/weave. */
      int sf = frame->second_field ? 1 : 0;
      uint8_t* dst = (uint8_t*)frame->addr[0];
      mxlGrainInfo gi; uint8_t* payload;
      if (reader_field(t, sf, s->tff, &gi, &payload) == 0) {
        /* MXL-NATIF : profondeur SOURCE dérivée du GRAIN, PAS de s->bit_depth (cf. branche
         * progressive). Détection robuste : grain = 1 CHAMP en entrelacé → taille 8-bit attendue
         * = 2·w·(h/2) ; 8-bit si grainSize == exp8 OU == moitié d'out_size (= champ 10-bit). */
        size_t _gs = (size_t)gi.grainSize;
        size_t _exp8 = (size_t)2 * (size_t)s->width * ((size_t)s->height / 2);   /* CHAMP */
        int _src8 = (_gs > 0 && (_gs == _exp8 || _gs * 2 == out_size));
        if (!t->dbg_depth_logged) {
          fprintf(stderr, "mtl_rx[video TX dbg i] shm=%s grainSize=%zu out_size=%zu exp8(2w·h/2)=%zu "
                  "gs2==out=%d gs==exp8=%d _src8=%d\n",
                  t->shm_path, _gs, out_size, _exp8,
                  (int)(_gs * 2 == out_size), (int)(_gs == _exp8), _src8);
          t->dbg_depth_logged = 1;
        }
        if (_src8) {
          uint16_t* d16 = (uint16_t*)dst;
          for (size_t k = 0; k < _gs; k++) d16[k] = (uint16_t)payload[k] << 2;   /* 8→10 */
        } else {
          memcpy(dst, payload, out_size < _gs ? out_size : _gs);
        }
        latched_fi = gi.index;
        /* répétition VISIBLE : reader_field a rendu le MÊME grain-champ que la trame précédente
         * (source figée) — tx_src_idx porte encore l'ancienne valeur, tx_reopen_if_stale ne l'a
         * pas encore mise à jour pour ce tour. */
        if (t->tx_src_idx_init && gi.index == t->tx_src_idx) t->repeats++;
        tx_reopen_if_stale(t, gi.index, tnow);   /* flux figé (producteur redéployé) → reconnexion */
        /* IDENT entrelacé : positionné en espace TRAME → différé (champ = ½ hauteur). TODO champ-aware. */
      } else {
        memset(dst, 0, out_size);                  /* pas de grain → champ neutre */
      }
      st20p_tx_put_frame(s->vth, frame);
      t->index = latched_fi; t->recv++;
    } else {
      mxlGrainInfo gi; uint8_t* payload;
      uint8_t* dst = (uint8_t*)frame->addr[0];
      if (reader_latest(t, &gi, &payload) == 0) {
        /* MXL-NATIF : profondeur SOURCE dérivée du GRAIN (flux auto-descriptif), PAS de s->bit_depth
         * (valeur poussée — au câblage à chaud la session TX défaute à bit_depth=10, donc une source
         * 8-bit comme la multiview était versée brute dans le buffer 10-bit → sortie 2110 VERTE).
         * Détection ROBUSTE immune au padding : 8-bit si grainSize == taille planar 8-bit attendue
         * (géométrie, 422 : Y+Cb+Cr = 2·w·h) OU == moitié d'out_size (buffer libmtl 10-bit). Une
         * vraie source 10-bit (grain == out_size == 2·exp8) ne déclenche aucune des deux → memcpy. */
        size_t _gs = (size_t)gi.grainSize;
        size_t _exp8 = (size_t)2 * (size_t)s->width * (size_t)s->height;
        int _src8 = (_gs > 0 && (_gs == _exp8 || _gs * 2 == out_size));
        if (!t->dbg_depth_logged) {
          fprintf(stderr, "mtl_rx[video TX dbg] shm=%s grainSize=%zu out_size=%zu exp8(2wh)=%zu "
                  "gs2==out=%d gs==exp8=%d _src8=%d\n",
                  t->shm_path, _gs, out_size, _exp8,
                  (int)(_gs * 2 == out_size), (int)(_gs == _exp8), _src8);
          t->dbg_depth_logged = 1;
        }
        if (_src8) {
          uint16_t* d16 = (uint16_t*)dst;
          for (size_t k = 0; k < _gs; k++) d16[k] = (uint16_t)payload[k] << 2;   /* 8→10 */
        } else {
          memcpy(dst, payload, out_size < _gs ? out_size : _gs);
        }
        /* IDENT : dst est une trame pleine planaire TOUJOURS 10-bit (input_fmt PLANAR10LE). */
        if (t->has_ident) { load_ident_patch(t); overlay_ident(s, t, dst, 10); }
        t->index = gi.index;
        /* répétition VISIBLE : même grain source que la trame précédente (source figée) — cf.
         * commentaire miroir de la branche entrelacée ci-dessus. */
        if (t->tx_src_idx_init && gi.index == t->tx_src_idx) t->repeats++;
        tx_reopen_if_stale(t, gi.index, tnow);   /* flux figé (producteur redéployé) → reconnexion */
      } else {
        memset(dst, 0, out_size);               /* pas de grain → trame neutre */
      }
      st20p_tx_put_frame(s->vth, frame);
      t->recv++;
    }
  }
  return NULL;
}

/* ═══ VIDÉO TX MODE TRANCHE (SLICE_MODE=1, progressive) ════════════════════════
 * API raw st20 (ST20_TYPE_SLICE_LEVEL) : le worker lit le grain SOURCE tranche par tranche
 * (mxlFlowReaderGetGrainSlice — réveil par commit du producteur), packe planar → RFC4175 BE10
 * PAR BANDE dans le framebuffer libmtl et publie lines_ready ; la lib émet au fil des lignes
 * prêtes (query_frame_lines_ready) → l'émission d'une trame commence AVANT sa fin d'écriture
 * amont (latence sous-trame). Le nombre de tranches vient du GRAIN SOURCE (totalSlices) : une
 * source whole-frame (totalSlices=1) dégrade proprement en trame pleine. */

/* lcore tasklet : la lib veut une trame à émettre → le prochain framebuffer publié (READY), ou, à
 * défaut, la TRAME DE TENUE (cf. sl_hold_idx). DEVENU FILET plutôt que mécanisme principal (bobi.studio) :
 * depuis que le WORKER tient l'INVARIANT « tout tour qui obtient un slot publie » (cf.
 * sl_publish_repeat_or_witness dans video_tx_slice_thread), il ne laisse plus jamais ce callback sans
 * rien à servir — ce -EBUSY ne devrait plus se produire en pratique. Conservé tel quel : il ne coûte
 * rien et couvre un cas qu'on n'aurait pas anticipé. */
static int tx_sl_next_frame(void* priv, uint16_t* next_frame_idx, struct st20_tx_frame_meta* meta) {
  struct sess* s = priv;
  /* AVANCE D'ORDONNANCEMENT DE LA LIB. Ce callback est appelé quand libmtl veut la PROCHAINE trame
   * à émettre, et `meta` porte l'ÉPOQUE qu'elle lui assigne. L'écart entre cette époque et l'instant
   * courant est le temps que la trame va passer À ATTENDRE SON TOUR DANS LA LIB, après que notre
   * worker l'a remise — segment jamais mesuré jusqu'ici, et le seul qui reste après avoir éliminé
   * la lecture de la source (`src_age_ms` NÉGATIF : on saisit du frais) et la profondeur de file
   * (la réduire ne change rien). C'est là que doivent se trouver les ~50 ms manquantes. */
  if (meta && s->sl_period_ns) {
    uint64_t _now = mtl_ptp_read_time_raw(s->st);
    int64_t _lead = (int64_t)meta->epoch * (int64_t)s->sl_period_ns - (int64_t)_now;
    if (_lead > -1000000000LL && _lead < 1000000000LL) {
      s->sl_lead_ns += _lead; s->sl_lead_cnt++;
      if (_lead > s->sl_lead_max) s->sl_lead_max = _lead;
    }
  }
  /* ★ LE CONSOMMATEUR DOIT SAUTER LES SLOTS RÉSERVÉS (stat=2), exactement comme le producteur.
   * Sans ça il avance d'un cran par trame consommée, tombe sur un tampon de repli — figé à un index
   * FIXE, lui — et s'y arrête DÉFINITIVEMENT : plus aucune trame fraîche n'est jamais consommée.
   * Régression introduite avec la paire de repli (0.71.0) et mesurée en prod : 1,5 fps, port à
   * 0,01 Gb/s. Elle n'existait pas tant que le seul slot réservé était la tenue, qui MIGRE vers les
   * slots du worker à chaque trame émise et libère donc toujours la place. Bornée à un tour
   * d'anneau, même raison que la garde du producteur. */
  for (uint16_t _k = 0; _k < s->sl_fb_cnt && s->sl_fb[s->sl_fb_cons].stat == 2; _k++)
    s->sl_fb_cons = (uint16_t)((s->sl_fb_cons + 1) % s->sl_fb_cnt);
  /* ★ OBSERVATION D'ORDRE (2026-08-09) — aucune influence sur le comportement.
   * Le drain tenté ce jour libérait, à chaque sollicitation, tous les slots prêts sauf « le plus
   * récent » ; résultat : `drained` et les répétitions montaient au MÊME rythme (11/s), donc chaque
   * trame jetée était précisément celle que la lib allait servir. Mon hypothèse sur le SENS de
   * l'anneau est donc fausse quelque part. On mesure au lieu de re-déduire :
   *   depth = nombre de slots prêts au moment de la sollicitation ;
   *   dist  = distance du slot SERVI au slot de production (`prod`), modulo l'anneau.
   * Si le servi est le plus ANCIEN, dist ≈ depth ; s'il est le plus RÉCENT, dist ≈ 1. */
  {
    uint16_t _d = 0;
    for (uint16_t _k = 0; _k < s->sl_fb_cnt; _k++) if (s->sl_fb[_k].stat == 1) _d++;
    uint16_t _dist = (uint16_t)((s->sl_fb_prod + s->sl_fb_cnt - s->sl_fb_cons) % s->sl_fb_cnt);
    s->sl_depth_sum += _d; s->sl_depth_cnt++;
    if (_d > s->sl_depth_max) s->sl_depth_max = _d;
    s->sl_dist_sum += _dist;
    if (_dist > s->sl_dist_max) s->sl_dist_max = _dist;
  }
  uint16_t c = s->sl_fb_cons;
  if (s->sl_fb[c].stat != 1) {
    /* Rien de frais : RE-SERT la dernière trame intégralement émise (gel, comportement historique —
     * un hoquet de moins de SL_FALLBACK_AFTER_NS ne doit rien changer à l'antenne). Au-delà de ce
     * délai, c'est le WORKER (video_tx_slice_thread) qui prend le relais en publiant lui-même un
     * témoin qui BOUGE dans le ring normal (slot stat=1) — cf. TÉMOIN DE REPLI ; ce callback n'a donc
     * plus qu'UNE seule branche de repli à connaître : la tenue. Elle n'est plus en vol
     * (notify_frame_done reçu) et le worker ne peut pas l'avoir reprise (stat=TENUE l'en exclut)
     * → aucune écriture concurrente, aucune copie. L'époque est tenue, le pacing reste régulier.
     * sl_hold_valid est désormais amorcé DÈS setup_video_slice_tx (noir légal) et PRÉSERVÉ par le
     * resync watchdog (cf. « anneau fb WEDGÉ ») : ce -EBUSY ne doit plus se produire en pratique.
     * Gardé comme filet défensif (perdre une époque vaut mieux que déréférencer un slot invalide). */
    uint64_t now = mono_ns();
    if (!s->sl_hold_valid) { s->srv_busy++; return -EBUSY; }
    *next_frame_idx = s->sl_hold_idx;
    s->srv_hold++;
    /* COMPTAGE DU REJEU — ni ici sans garde, ni dans notify_frame_done : mesuré, la lib SOLLICITE ce
     * callback plusieurs fois sans émettre (une sortie statique affichait 65 fps pour 2,19 Gb/s de
     * fil, soit 50) et ne RAPPELLE PAS frame_done quand elle re-sert la même trame (`fps` tombait
     * alors à 38 pour un fil à 50). Les deux points de comptage sont donc faux en sens opposés.
     * Ce qui est VRAI, c'est qu'une trame de tenue ne peut partir qu'UNE FOIS PAR ÉPOQUE : on borne
     * le comptage à une période nominale. La valeur est alors exacte à la période près, et jamais
     * au-dessus du nominal (`now` déjà calculé plus haut). */
    if (s->sl_period_ns && now - s->sl_hold_last_ns >= (s->sl_period_ns * 3) / 4) {
      s->sl_hold_last_ns = now;
      s->sl_hold_emitted++;
    }
    return 0;
  }
  if (s->serve_newest) {
    /* La plus RÉCEMMENT publiée parmi les slots prêts, en excluant celle en vol : elle est
     * encore stat=1 mais la lib est en train de l'émettre, la libérer produirait du déchirement. */
    int _best = -1;
    for (uint16_t _k = 0; _k < s->sl_fb_cnt; _k++) {
      if (s->sl_fb[_k].stat != 1) continue;
      if (s->sl_fb_inflight_ok && _k == s->sl_fb_inflight) continue;
      if (_best < 0 || s->sl_fb[_k].pub_ns > s->sl_fb[_best].pub_ns) _best = _k;
    }
    if (_best >= 0) {
      /* Les prêtes STRICTEMENT plus anciennes que celle qu'on sert sont périmées : personne ne
       * les émettra jamais (on vient de passer devant), et les laisser occuperait l'anneau
       * jusqu'au watchdog. On les libère — c'est la différence avec un drain aveugle : on ne
       * touche ni à celle qu'on sert, ni à celle en vol. */
      for (uint16_t _k = 0; _k < s->sl_fb_cnt; _k++) {
        if (_k == (uint16_t)_best || s->sl_fb[_k].stat != 1) continue;
        if (s->sl_fb_inflight_ok && _k == s->sl_fb_inflight) continue;
        if (s->sl_fb[_k].pub_ns < s->sl_fb[_best].pub_ns) {
          s->sl_fb[_k].stat = 0;
          s->sl_skipped++;
        }
      }
      *next_frame_idx = (uint16_t)_best;
      s->sl_fb_inflight = (uint16_t)_best; s->sl_fb_inflight_ok = 1;
      s->sl_fb_cons = (uint16_t)((_best + 1) % s->sl_fb_cnt);
      s->srv_fresh++;
      return 0;
    }
  }
  *next_frame_idx = c;
  s->sl_fb_inflight = c; s->sl_fb_inflight_ok = 1;
  s->sl_fb_cons = (uint16_t)((c + 1) % s->sl_fb_cnt);
  s->srv_fresh++;
  return 0;
}

/* lcore tasklet : trame émise → la trame qui vient de partir devient la TRAME DE TENUE (elle sera
 * re-servie si la suivante n'est pas prête), et la tenue PRÉCÉDENTE est rendue au worker. Une seule
 * trame est ainsi immobilisée en permanence pour le gel — le TÉMOIN DE REPLI, lui, transite par les
 * slots NORMAUX du worker (cf. video_tx_slice_thread), il n'a besoin d'aucune réservation ici. Un
 * re-service de la tenue rappelle ce callback avec le MÊME index : on ne touche alors à rien (elle
 * reste tenue, ses lines_ready restent complètes). */
/* Publication d'un framebuffer par le worker. Passe par un helper parce que QUATRE chemins
 * publient (trame fraîche, répétition, témoin, mode bande) : dater à la main à chaque site
 * invite l'oubli, et un site manquant fausserait la moyenne sans le dire. */
static inline void sl_mark_pub(struct sess* s, uint16_t idx) {
  s->sl_fb[idx].pub_ns = mono_ns();
  s->sl_fb[idx].start_ns = 0;
  __sync_synchronize();          /* contenu ET horodatage EN PLACE avant d'annoncer le slot */
  s->sl_fb[idx].stat = 1;        /* ⚠ l'affectation DIRECTE, pas le helper : on EST le helper */
}

static int tx_sl_frame_done(void* priv, uint16_t frame_idx, struct st20_tx_frame_meta* meta) {
  struct sess* s = priv; (void)meta;
  if (s->sl_fb[frame_idx].start_ns) {
    s->sl_emit_ns += mono_ns() - s->sl_fb[frame_idx].start_ns; s->sl_emit_cnt++;
  }
  if (s->sl_hold_valid && frame_idx == s->sl_hold_idx) {
    s->sl_wedge_log_ns = 0; s->sl_wedge_log_n = 0;
    pthread_mutex_lock(&s->sl_mx);
    pthread_cond_signal(&s->sl_cv);
    pthread_mutex_unlock(&s->sl_mx);
    return 0;                                   /* rejeu de la tenue : rien à libérer */
  }
  /* ★ NE JAMAIS PROMOUVOIR UNE TRAME VIDE EN TENUE — sortie muette DÉFINITIVE (banc 2026-07-29).
   *
   * MÉCANISME (corrigé après mesure — une première explication par le resync du watchdog était
   * FAUSSE : le compteur ci-dessous grimpe à ~8/s alors qu'aucun resync ne se produit).
   * Le worker du mode tranche publie le framebuffer PRÊT **avant** de l'avoir rempli — c'est le
   * principe même du slice : `lines_ready = 0; stat = 1;` puis remplissage bande par bande, la lib
   * émettant au fil des lignes. Il existe donc une fenêtre où un slot est READY avec ZÉRO ligne. Si
   * la lib s'en saisit dans cette fenêtre, elle n'émet rien, rappelle notify_frame_done, et ce slot
   * VIDE devenait la trame de tenue. Dès lors chaque époque re-servait une trame sans une seule
   * ligne : plus rien sur le fil, les autres slots s'accumulaient pleins et jamais consommés.
   * Définitif — ni décâblage, ni retour de la source, ni changement de port n'y changeaient rien ;
   * seul un redéploiement du moteur rétablissait l'émission (~90 s de coupure du nœud).
   * Signature instrumentée : ring.stat=[1,1,1,2] avec lines=[1080,1080,1080,0].
   *
   * La course est FRÉQUENTE (mesurée à ~8 fois par seconde pour 50 trames, soit ~16 %) : ce n'est
   * pas un cas limite, c'est le régime normal du mode tranche. Ce qui était rare, c'est qu'elle
   * tombe sur la promotion en tenue — et une seule suffisait à éteindre la sortie pour de bon.
   *
   * Une tenue périmée mais COMPLÈTE vaut infiniment mieux qu'une tenue fraîche mais vide : on garde
   * l'ancienne et on rend le slot au worker, qui le remplira. */
  if (!s->sl_fb[frame_idx].lines_ready) {
    s->sl_fb[frame_idx].stat = 0;               /* rendue au worker, PAS promue */
    s->sl_hold_empty_kept++;
    pthread_mutex_lock(&s->sl_mx);
    pthread_cond_signal(&s->sl_cv);
    pthread_mutex_unlock(&s->sl_mx);
    return 0;                                   /* la tenue précédente reste en place */
  }
  if (s->sl_hold_valid) {                       /* l'ancienne tenue redevient disponible */
    s->sl_fb[s->sl_hold_idx].lines_ready = 0;
    s->sl_fb[s->sl_hold_idx].stat = 0;
  }
  s->sl_hold_idx = frame_idx; s->sl_hold_valid = 1;
  s->sl_fb[frame_idx].stat = 2;                 /* TENUE : contenu complet, worker exclu */
  s->sl_wedge_log_ns = 0; s->sl_wedge_log_n = 0;   /* done reçu : session saine → réarme le throttle */
  pthread_mutex_lock(&s->sl_mx);
  pthread_cond_signal(&s->sl_cv);
  pthread_mutex_unlock(&s->sl_mx);
  return 0;
}

/* lcore tasklet : combien de lignes de frame_idx sont prêtes à partir ? */
static int tx_sl_lines_ready(void* priv, uint16_t frame_idx, struct st20_tx_slice_meta* meta) {
  struct sess* s = priv;
  /* PREMIÈRE sollicitation depuis la publication = début d'émission. On ne date qu'une fois
   * (start_ns remis à 0 à chaque publication) : la lib rappelle ensuite à chaque bande. */
  if (!s->sl_fb[frame_idx].start_ns && s->sl_fb[frame_idx].pub_ns) {
    uint64_t _n = mono_ns();
    s->sl_fb[frame_idx].start_ns = _n;
    uint64_t _w = _n - s->sl_fb[frame_idx].pub_ns;
    s->sl_wait_pub_ns += _w; s->sl_wait_pub_cnt++;
    if (_w > s->sl_wait_pub_max) s->sl_wait_pub_max = _w;
  }
  meta->lines_ready = (uint16_t)s->sl_fb[frame_idx].lines_ready;
  return 0;
}

/* Packe la bande [l0, l0+nl) du grain planar source vers RFC4175 BE10 dans le framebuffer.
 * Source 8-bit : up-shift 8→10 de la bande dans sl_scratch (alloué paresseusement) puis SIMD. */
static void sl_pack_band(struct sess* s, const uint8_t* pay, int src8,
                         uint32_t l0, uint32_t nl, uint8_t* fb) {
  int W = s->width, H = s->height;
  size_t bpl_be = ((size_t)W / 2) * 5;
  struct st20_rfc4175_422_10_pg2_be* dst =
      (struct st20_rfc4175_422_10_pg2_be*)(fb + (size_t)l0 * bpl_be);
  if (!src8) {
    uint16_t* y = (uint16_t*)(pay + (size_t)l0 * W * 2);
    uint16_t* b = (uint16_t*)(pay + (size_t)W * H * 2 + (size_t)l0 * W);
    uint16_t* r = (uint16_t*)(pay + (size_t)W * H * 3 + (size_t)l0 * W);
    st20_yuv422p10le_to_rfc4175_422be10(y, b, r, dst, (uint32_t)W, nl);
    return;
  }
  if (!s->sl_scratch) s->sl_scratch = malloc((size_t)4 * W * H);   /* pire cas : bande = trame */
  if (!s->sl_scratch) return;
  const uint8_t* y8 = pay + (size_t)l0 * W;
  const uint8_t* b8 = pay + (size_t)W * H + (size_t)l0 * (W / 2);
  const uint8_t* r8 = pay + (size_t)W * H * 3 / 2 + (size_t)l0 * (W / 2);
  uint16_t* ys = (uint16_t*)s->sl_scratch;
  uint16_t* bs = ys + (size_t)W * nl;
  uint16_t* rs = bs + (size_t)(W / 2) * nl;
  for (size_t k = 0; k < (size_t)W * nl; k++) ys[k] = (uint16_t)y8[k] << 2;
  for (size_t k = 0; k < (size_t)(W / 2) * nl; k++) {
    bs[k] = (uint16_t)b8[k] << 2; rs[k] = (uint16_t)r8[k] << 2;
  }
  st20_yuv422p10le_to_rfc4175_422be10(ys, bs, rs, dst, (uint32_t)W, nl);
}

/* Construit dans `fb` (framebuffer packé RFC4175 BE10) un fond NOIR LÉGAL + un petit carré témoin
 * gris clair, centré horizontalement sur `cx` (pixels), vers le bas de l'image — cf. TÉMOIN DE
 * REPLI. VOIE CHOISIE : on construit une bande planar 16 bits temporaire (Y|Cb|Cr, EXACTEMENT le
 * format déjà attendu par sl_pack_band côté src8=0, cf. l'amorçage de la trame de tenue) puis on la
 * packe UNE fois avec le pack SIMD existant — plutôt que de tracer le carré directement dans le
 * packé RFC4175 (un pgroup porte 2 pixels sur 5 octets à cheval Y/Cb/Cr : y écrire un rectangle
 * exigerait de réimplémenter un déballage/remballage pgroup rien que pour ce cas, pour un gain nul
 * puisque ce code ne tourne QU'AU SETUP, jamais sur le tasklet lcore). Aucune duplication du pack :
 * même fonction que le noir de tenue. */
#define SL_WITNESS_SQ 48   /* côté du carré témoin, en pixels (~48×48) */
static void sl_fill_fallback_frame(struct sess* s, uint8_t* fb, int cx) {
  int W = s->width, H = s->height;
  size_t npix = (size_t)W * H;
  uint16_t* buf = malloc((size_t)2 * npix * sizeof(uint16_t));   /* Y (npix) + Cb+Cr (npix) */
  if (!buf) return;
  uint16_t* y = buf;
  uint16_t* cbcr = buf + npix;
  for (size_t k = 0; k < npix; k++) y[k] = 64;     /* noir légal (comme l'amorçage de la tenue) */
  for (size_t k = 0; k < npix; k++) cbcr[k] = 512;  /* Cb=Cr=512 : achromatique, le carré n'y touche pas */
  int sq = SL_WITNESS_SQ;
  int cy = H - H / 8;                               /* ~1/8 de hauteur au-dessus du bas de l'image */
  int x0 = cx - sq / 2, y0 = cy - sq / 2;
  if (x0 < 0) x0 = 0; if (y0 < 0) y0 = 0;
  int x1 = x0 + sq; if (x1 > W) x1 = W;
  int y1 = y0 + sq; if (y1 > H) y1 = H;
  for (int yy = y0; yy < y1; yy++)
    for (int xx = x0; xx < x1; xx++)
      y[(size_t)yy * W + xx] = 700;                  /* gris clair légal (Y) */
  sl_pack_band(s, (uint8_t*)buf, 0, 0, (uint32_t)H, fb);
  free(buf);
}

/* Marque `idx` comme portant une image RÉELLE fraîchement packée (source vivante, grain relu, ou
 * trame statique valide) : nouvelle génération de contenu, `idx` devient le slot PORTEUR pour toute
 * répétition ultérieure ailleurs dans l'anneau (cf. sl_publish_repeat_or_witness). Coût nul : pas de
 * copie, juste deux compteurs et un stamp. */
static inline void sl_note_fresh_pack(struct sess* s, uint16_t idx) {
  s->sl_content_gen++;
  s->sl_carrier_idx = idx;
  s->sl_carrier_valid = 1;
  s->sl_fb[idx].rep_stamp = s->sl_content_gen;
}

/* Publie dans le slot `idx` (déjà acquis LIBRE, `fb` = son framebuffer) la meilleure alternative
 * disponible quand aucune trame fraîche n'a pu être obtenue ce tour — c'est ce qui tient l'INVARIANT
 * « tout tour qui obtient un slot publie exactement une trame » (cf. TROU DE COUVERTURE : une source
 * ~1 fps n'est couverte ni par la trame de tenue du callback ni par le témoin des 2 s, port mesuré à
 * 0,01 Gb/s). Ordre de préférence :
 *   1. répétition QUASI SANS COPIE de la dernière image réelle (`sl_carrier_idx`), si elle existe et
 *      si son slot porteur est LIBRE (jamais recopier depuis un slot en cours d'émission : risque de
 *      trame déchirée) et si `force_witness` ne l'exclut pas (source morte depuis trop longtemps) ;
 *   2. témoin de repli (noir + carré alterné), sinon.
 * `force_witness` doit valoir vrai quand la source est MORTE (au-delà de SL_FALLBACK_AFTER_NS) : au
 * bout d'un moment, rejouer indéfiniment une vieille image serait indiscernable d'un gel — le témoin
 * qui bouge est ce qui prouve la vie de la chaîne d'émission. */
static void sl_publish_repeat_or_witness(struct sess* s, struct target* t, uint16_t idx,
                                          uint8_t* fb, int force_witness, uint64_t now_ns) {
  int W = s->width, H = s->height;
  size_t bpl_be = ((size_t)W / 2) * 5;
  int can_repeat = !force_witness && s->sl_carrier_valid && s->sl_fb[s->sl_carrier_idx].stat == 0;
  if (can_repeat) {
    if (s->sl_fb[idx].rep_stamp != s->sl_content_gen) {
      if (idx != s->sl_carrier_idx) {
        uint8_t* src = st20_tx_get_framebuffer(s->sl_tx, s->sl_carrier_idx);
        if (src) memcpy(fb, src, bpl_be * (size_t)H);
      }
      s->sl_fb[idx].rep_stamp = s->sl_content_gen;
    }
  } else {
    /* Même schéma que le témoin historique : position alternée toutes les SL_FALLBACK_TOGGLE_NS,
     * empreinte `wit_stamp` pour ne repacker que si la position a changé depuis ce slot. */
    if (!s->sl_witness_switch_ns || now_ns - s->sl_witness_switch_ns >= SL_FALLBACK_TOGGLE_NS) {
      s->sl_witness_switch_ns = now_ns;
      s->sl_witness_cur ^= 1;
      s->sl_witness_gen++;
    }
    if (s->sl_fb[idx].wit_stamp != s->sl_witness_gen) {
      int cx = s->sl_witness_cur ? (W / 2 + 100) : (W / 2 - 100);
      sl_fill_fallback_frame(s, fb, cx);
      s->sl_fb[idx].wit_stamp = s->sl_witness_gen;
    }
  }
  s->sl_fb[idx].lines_ready = (uint32_t)H;   /* contenu EN PLACE avant d'annoncer le slot prêt */
  __sync_synchronize();
  sl_mark_pub(s, idx);
  s->sl_fb_prod = (uint16_t)((idx + 1) % s->sl_fb_cnt);
  t->index++; t->recv++;                     /* trame réellement émise (fps reste nominal) */
  t->alive_ns = mono_ns();
}

/* Worker TX tranche : vise toujours le grain de TÊTE du flux source (celui en cours d'écriture),
 * le suit tranche par tranche et remplit le framebuffer au fil de l'eau. */
static void* video_tx_slice_thread(void* arg) {
  struct sess* s = arg;
  struct target* t = &s->tg[0];                 /* la cible TX = l'unique flux d'entrée câblé */
  int W = s->width, H = s->height;
  size_t bpl_be = ((size_t)W / 2) * 5;
  uint64_t period_ns = s->fps > 0 ? (uint64_t)(1e9 / s->fps) : 20000000ull;
  uint64_t next_fi = 0; int fi_init = 0;
  while (!s->stop) {
    if (tx_take_source(t) && t->reader) {       /* source permutée à chaud → rouvrir le reader */
      mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL; fi_init = 0;
    }
    /* MODE STATIQUE : slot SANS câble mais avec une trame publiée par le contrôleur (noir de repli,
     * mire, ardoise). On ne dépend d'AUCUN producteur : ni reader, ni flux MXL, ni attente de grain.
     * La boucle n'est alors cadencée que par la libération des framebuffers (donc par l'émission
     * réelle) → cadence NOMINALE, là où un producteur externe imposait la sienne. Le câblage qui
     * survient est pris au tour suivant par tx_take_source : la session reste la MÊME (aucun
     * st20p_tx_create, aucun commit RL, aucun stop de port), la bascule se fait sur une frontière
     * de trame et le flux 2110 n'est pas interrompu. */
    int statique = (!t->shm_path[0] && t->static_frame[0]);
    /* SOURCE MORTE (bobi.studio, TÉMOIN DE REPLI) : slot CÂBLÉ dont plus aucun grain frais n'est
     * arrivé depuis SL_FALLBACK_AFTER_NS. `last_fresh_ns == 0` (producteur JAMAIS apparu) donne un
     * écart énorme au temps courant → considéré mort dès le départ, c'est voulu (rien à figer de
     * toute façon). Ne concerne QUE les slots câblés : un slot statique n'a aucune source à perdre. */
    uint64_t now_lf = mono_ns();
    int morte = !statique && t->shm_path[0] != 0 &&
                (now_lf - t->last_fresh_ns) > SL_FALLBACK_AFTER_NS;
    if (!statique && !t->reader) {
      t->last_feed_ns = 0; t->slot_wait_ns = 0; t->adv_wait_ns = 0;
      t->alive_ns = mono_ns();                  /* attendre un câblage n'est pas un wedge */
      fi_init = 0;
      /* NE PAS sauter le tour même si la réouverture échoue — le thread doit quand même publier
       * plus bas (répétition ou témoin, cf. le bloc `if (!t->reader)` après l'acquisition de slot) :
       * c'est le POINT LE PLUS IMPORTANT de ce mécanisme. La réouverture reste tentée à CHAQUE tour
       * (ici, avant tout) — dès qu'elle réussit ou qu'un grain frais est relu, la boucle reprend le
       * chemin normal ci-dessous, sans recréation de session. Un échec ne fait plus que ralentir le
       * rythme de réessai (usleep), il ne prive plus jamais ce tour de sa publication. */
      if (open_reader(t) != 0) usleep(20000);
    }
    if (statique && t->reader) {                /* on vient de perdre le câble → lâcher le reader */
      mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL; fi_init = 0;
    }
    /* HORLOGE DE SORTIE : la TRAME DE TENUE est immobilisée pour le rejeu — on ne la remplit jamais,
     * et surtout on ne l'ATTEND pas (l'attendre déclencherait le watchdog anneau au bout de 2 s alors
     * que ce slot est retenu volontairement). L'anneau fait 4 slots (1 réservé, cf. stat=2) : il en
     * reste 3 au worker. BOUCLE (pas un simple if) : même si un seul slot est réservé aujourd'hui, on
     * garde la boucle bornée à un tour d'anneau (cf. note ci-dessous). */
    /* BORNÉE : un seul slot porte stat=2 (la tenue), donc la boucle sort toujours dès la 1ʳᵉ
     * itération en pratique. Mais ce chemin a un historique de blocages définitifs — si un jour un
     * défaut marquait TOUS les slots réservés, une boucle non bornée ferait tourner ce thread à
     * 100 % de CPU en silence, sans watchdog pour le voir. Un tour d'anneau au plus : on préfère une
     * contre-pression visible à un thread qui part en vrille muet. */
    for (uint16_t _k = 0; _k < s->sl_fb_cnt && s->sl_fb[s->sl_fb_prod].stat == 2; _k++)
      s->sl_fb_prod = (uint16_t)((s->sl_fb_prod + 1) % s->sl_fb_cnt);
    /* ── BRIDAGE D'AVANCE (instrument, PAS un levier de latence) ──────────────────────────
     * Tant que `advance` trames sont DÉJÀ prêtes, on ne commence pas la suivante. Ce n'est pas
     * la disponibilité d'un slot qu'on régule, c'est la PROFONDEUR de la file — le slot visé
     * peut être libre et le bridage s'appliquer quand même.
     *
     * ⛔ RÉSULTAT (2026-08-12, banc apparié au contenu) : ça marche mécaniquement — la file
     * descend de 3 à 1 et `adv_wait_ms` monte à 1868 ms par fenêtre de 2 s — mais LA LATENCE NE
     * BOUGE PAS (62,1 ms, +3 trames, aux trois paliers), et à 1 le TX ne consomme plus qu'une
     * trame source sur deux. Garder 0. Ce qui déplace vraiment l'aiguille est `epoch_shift_us`.
     *
     * On ne draine pas non plus : l'essai du 2026-08-09 libérait tous les slots prêts sauf le
     * plus récent, et les répétitions montaient au rythme exact des purges — chaque trame jetée
     * était celle que la lib allait servir. */
    if (s->advance > 0 && !statique) {
      unsigned _occ = 0;
      for (uint16_t _k = 0; _k < s->sl_fb_cnt; _k++)
        if (s->sl_fb[_k].stat == 1) _occ++;
      if (_occ >= (unsigned)s->advance) {
        uint64_t _a0 = mono_ns();
        pthread_mutex_lock(&s->sl_mx);
        struct timespec _tw; clock_gettime(CLOCK_REALTIME, &_tw);
        _tw.tv_nsec += 2 * 1000000;        /* court : on veut reprendre dès qu'un slot part */
        if (_tw.tv_nsec >= 1000000000) { _tw.tv_sec++; _tw.tv_nsec -= 1000000000; }
        pthread_cond_timedwait(&s->sl_cv, &s->sl_mx, &_tw);
        pthread_mutex_unlock(&s->sl_mx);
        /* Un bridage VOLONTAIRE n'est pas un wedge : sans ce signe de vie, le watchdog de queue
         * TX conclurait à un thread mort au bout de 2 s de fonctionnement parfaitement nominal. */
        t->alive_ns = mono_ns();
        {
          uint64_t _w = t->alive_ns - _a0;
          t->adv_wait_ns += _w;
          t->adv_wait_cum_ns += _w;
          t->adv_wait_cnt++;
        }
        continue;
      }
    }
    /* attendre un framebuffer libre (la lib libère via notify_frame_done) */
    uint16_t idx = s->sl_fb_prod;
    if (s->sl_fb[idx].stat != 0) {
      uint64_t w0 = mono_ns();   /* chrono attente de slot (exclue du compteur `late`, cf. infra) */
      /* WATCHDOG ANNEAU (durcissement 0.40.1) : la lib peut ABANDONNER une trame sans
       * notify_frame_done (échec transmit sous churn de source, vu au banc mv 2026-07-11) →
       * le slot reste stat=1 pour toujours → deadlock (get_next_frame rend -EBUSY en boucle,
       * build ret -203, 0 fps jusqu'au restart daemon). RESYNC — tout FREE + cons recalé sur
       * prod (écritures u16/u32 alignées, la lib ne fait que LIRE ces champs dans cet état).
       *
       * ★ 0.49.0 — LE GATE `all_busy` ÉTAIT AVEUGLE AU MODE DE MORT DOMINANT (banc 2026-07-14,
       * moteur 140 : `bobi_mtl_vtx_sl` à 0 fps DÉFINITIF, `build ret -203` toutes les 10 s,
       * source fraîche). Quand le commit TM d'une AUTRE session stoppe le port, les mbufs de la
       * trame en vol sont perdus SANS free (ice_reset_tx_queue memset le sw_ring) → la ref
       * extbuf sur la trame n'est jamais rendue → libmtl n'appelle JAMAIS notify_frame_done pour
       * ce slot → il reste stat=1 pour toujours. Les AUTRES slots, eux, finissent d'être émis et
       * repassent stat=0 : `all_busy` est donc FAUX, le watchdog ne se déclenchait JAMAIS, et le
       * worker restait bloqué à vie sur ce seul slot (prod == slot piégé) — plus rien n'était
       * produit, donc la lib finissait par ne plus avoir un seul slot prêt : -203 permanent.
       * Le bon critère n'est PAS « la lib ne tient plus rien », c'est « MON slot de production
       * ne se libère pas » : à 50 fps un slot doit être rendu en ~20 ms, 2 s = anomalie certaine.
       * (Le filet libmtl `bobi_get_frame_busy_check` — patch_tx_frame_inflight_reclaim — traite
       * la CAUSE en rendant la trame piégée ; ce watchdog reste le filet de dernier recours côté
       * app, pour toute perte de notify_frame_done qui ne laisserait PAS de refcnt derrière.) */
#define SL_WEDGE_NS (2ull * 1000000000ull)
      if (!s->sl_wedge_ns) s->sl_wedge_ns = mono_ns();
      else if (mono_ns() - s->sl_wedge_ns > SL_WEDGE_NS) {
        unsigned busy = 0;
        for (uint16_t k = 0; k < s->sl_fb_cnt; k++)
          if (s->sl_fb[k].stat != 0) busy++;
        /* THROTTLE (0.41.1) : une session TX en échec permanent (ex. PKT_ALLOC_FAIL) wedge
         * puis resynce toutes les 2 s pour toujours → spam continu qui sature docker logs.
         * 1er wedge après une période saine : log complet ; ensuite au plus 1 log/min avec
         * l'agrégat des occurrences. Le throttle est réarmé par tx_sl_frame_done (un done
         * reçu = session redevenue saine), PAS par le resync lui-même (qui libère les slots). */
        uint64_t lnow = mono_ns();
        s->sl_wedge_log_n++;
        if (!s->sl_wedge_log_ns) {
          fprintf(stderr, "mtl_rx[video TX SLICE] %s:%d anneau fb WEDGÉ — slot prod %u jamais"
                  " libéré depuis %.1fs (%u/%u slots occupés) → resync (all FREE, cons=prod)"
                  " [logs throttlés à 1/min tant que pas de done]\n",
                  s->mcast, s->udp_port, (unsigned)idx, (lnow - s->sl_wedge_ns) / 1e9,
                  busy, (unsigned)s->sl_fb_cnt);
          s->sl_wedge_log_ns = lnow; s->sl_wedge_log_n = 0;
        } else if (lnow - s->sl_wedge_log_ns >= 60ull * 1000000000ull) {
          fprintf(stderr, "mtl_rx[video TX SLICE] %s:%d anneau fb toujours WEDGÉ — resync"
                  " (×%u depuis %.0f s, %u/%u slots occupés)\n",
                  s->mcast, s->udp_port, (unsigned)s->sl_wedge_log_n,
                  (lnow - s->sl_wedge_log_ns) / 1e9, busy, (unsigned)s->sl_fb_cnt);
          s->sl_wedge_log_ns = lnow; s->sl_wedge_log_n = 0;
        }
        for (uint16_t k = 0; k < s->sl_fb_cnt; k++) {
          if (s->sl_hold_valid && k == s->sl_hold_idx) continue;   /* cf. note ci-dessous : PRÉSERVÉ */
          s->sl_fb[k].lines_ready = 0;
          s->sl_fb[k].stat = 0;
        }
        /* Le resync REND tous les AUTRES slots au worker, mais PRÉSERVE la trame de TENUE (stat=2,
         * lines_ready intacts) — l'invalider ici rendrait la sortie MUETTE précisément quand ce
         * watchdog constate que plus rien n'avance côté producteur, soit le pire moment. Sans danger
         * de réécriture concurrente : le worker ne produit JAMAIS dans ce slot (la garde en tête de
         * boucle le saute, cf. `sl_fb_prod`), et le hold_idx courant n'est jamais celui qui vient de
         * se coincer (même garde). Elle se remplace normalement à la prochaine trame réellement émise
         * (tx_sl_frame_done). */
        s->sl_fb_cons = s->sl_fb_prod;
        s->sl_wedge_ns = mono_ns();          /* ré-arme (resync fait) */
      }
      pthread_mutex_lock(&s->sl_mx);
      if (s->sl_fb[idx].stat != 0 && !s->stop) {
        struct timespec tw; clock_gettime(CLOCK_REALTIME, &tw);
        tw.tv_nsec += 50 * 1000000;
        if (tw.tv_nsec >= 1000000000) { tw.tv_sec++; tw.tv_nsec -= 1000000000; }
        pthread_cond_timedwait(&s->sl_cv, &s->sl_mx, &tw);
      }
      pthread_mutex_unlock(&s->sl_mx);
      t->alive_ns = mono_ns();
      {
        uint64_t w = t->alive_ns - w0;
        t->slot_wait_ns += w;              /* contre-pression anneau : pas un retard SOURCE */
        t->slot_wait_cum_ns += w;          /* … et accumulation pour la publier (cf. champ) */
        t->slot_wait_cnt++;
      }
      continue;
    }
    s->sl_wedge_ns = 0;                      /* un slot s'est libéré : anneau vivant */
    uint8_t* fb = st20_tx_get_framebuffer(s->sl_tx, idx);
    if (!fb) { usleep(5000); continue; }
    if (statique) {
      if (load_static_frame(t) != 0) {        /* pas (encore) de trame publiée par le contrôleur */
        /* Aucune trame statique à packer : l'invariant exige quand même une publication (le slot
         * est acquis) → témoin de repli (aucune image réelle n'a jamais existé pour ce slot). Le
         * carrier éventuel (image réelle d'un câblage précédent) reste éligible via force_witness=0
         * — sl_publish_repeat_or_witness retombera de toute façon sur le témoin si aucun carrier
         * n'est valide. */
        sl_publish_repeat_or_witness(s, t, idx, fb, 0, now_lf);
        continue;                             /* aucun `late` : il n'y a pas de source à rater */
      }
      /* Packing UNE SEULE FOIS par framebuffer : les buffers libmtl persistent d'un tour à l'autre,
       * donc ré-émettre une trame statique ne coûte plus rien après la première rotation. `sf_gen`
       * change à chaque re-publication du fichier → tous les buffers sont re-packés. */
      if (s->sl_fb[idx].sf_stamp != t->sf_gen) {
        /* Profondeur SOURCE déduite de la TAILLE, comme pour un grain MXL (flux auto-descriptif). */
        int src8 = (t->sf_size == (size_t)2 * (size_t)W * (size_t)H);
        sl_pack_band(s, t->sf_buf, src8, 0, (uint32_t)H, fb);
        s->sl_fb[idx].sf_stamp = t->sf_gen;
        sl_note_fresh_pack(s, idx);           /* image RÉELLE (ardoise) : éligible au repli répétition */
      }
      s->sl_fb[idx].lines_ready = (uint32_t)H;  /* contenu EN PLACE avant d'annoncer le slot prêt */
      __sync_synchronize();
      sl_mark_pub(s, idx);
      s->sl_fb_prod = (uint16_t)((idx + 1) % s->sl_fb_cnt);
      t->index++; t->recv++;                    /* trame réellement émise (fps = nominal) */
      t->alive_ns = mono_ns();
      continue;                                 /* aucun `late` : il n'y a pas de source à rater */
    }
    if (!t->reader) {
      /* Reader absent CE TOUR (source jamais câblée, hoquet d'ouverture, ou source MORTE depuis
       * SL_FALLBACK_AFTER_NS) : un slot LIBRE a quand même été acquis plus haut — publier plutôt que
       * sauter le tour (cf. TROU DE COUVERTURE : entre la trame de tenue du callback — valide tant
       * que le worker publie à cadence correcte — et le témoin des 2 s, une source ~1 fps n'était
       * couverte par AUCUN des deux, port mesuré à 0,01 Gb/s). `morte` force le TÉMOIN qui bouge (au
       * -delà de 2 s, rejouer indéfiniment la dernière image serait indiscernable d'un gel) ; sinon
       * on RÉPÈTE quasi sans copie la dernière image réelle si elle existe (cf.
       * sl_publish_repeat_or_witness), sinon témoin aussi. Aucun `late` compté : il n'y a pas de
       * source à rater. GARDE implicite `!t->reader` : si un reader vient d'être rouvert avec succès
       * CE TOUR (producteur de retour, ou ré-attachement transitoire à l'orphelin), on saute ce bloc
       * pour tester la fraîcheur IMMÉDIATEMENT ci-dessous (sinon `last_fresh_ns` ne serait plus
       * jamais réévalué : `morte` resterait vrai à vie, un DEADLOCK de reprise). `open_reader` reste
       * tenté à chaque tour (cf. plus haut), sans recréation de session. */
      sl_publish_repeat_or_witness(s, t, idx, fb, morte, now_lf);
      continue;
    }
    /* ── FREIN TEMPOREL : attendre d'être proche de l'époque avant de saisir la source ──
     * On ne bride pas un COMPTEUR de tampons (essayé, sans effet : l'attente vaut une époque
     * qu'il y ait une ou trois trames prêtes) mais une ÉCHÉANCE D'HORLOGE. Le sommeil est
     * borné à une période : si l'horloge PTP est indisponible ou la grille incohérente, on
     * repart sans attendre plutôt que de figer l'émission. */
    if (s->publish_lead_us > 0 && s->sl_period_ns) {
      uint64_t _tai = mtl_ptp_read_time_raw(s->st);
      uint64_t _per = s->sl_period_ns;
      uint64_t _cible = ((_tai / _per) + 1) * _per;          /* prochaine frontière d'époque */
      int64_t  _reste = (int64_t)_cible - (int64_t)_tai - (int64_t)s->publish_lead_us * 1000;
      if (_reste > 0 && _reste < (int64_t)_per) {
        struct timespec _ts = { (time_t)(_reste / 1000000000LL),
                                (long)(_reste % 1000000000LL) };
        nanosleep(&_ts, NULL);
        t->alive_ns = mono_ns();   /* attente VOLONTAIRE : ce n'est pas un thread mort */
      }
    }
    /* viser le grain de tête (rattrape si on est en retard ; jamais en arrière) */
    mxlFlowRuntimeInfo rt;
    mxlStatus stx = mxlFlowReaderGetRuntimeInfo(t->reader, &rt);
    if (stx != MXL_STATUS_OK || rt.headIndex == MXL_UNDEFINED_INDEX) {
      if (stx == MXL_ERR_FLOW_NOT_FOUND || stx == MXL_ERR_FLOW_INVALID) {
        mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL;
      }
      /* RuntimeInfo indisponible ce tour (flux pas encore prêt côté producteur) : slot déjà acquis
       * → publier répétition/témoin plutôt que sauter le tour, cf. TROU DE COUVERTURE. */
      sl_publish_repeat_or_witness(s, t, idx, fb, morte, now_lf);
      continue;
    }
    if (!fi_init || next_fi < rt.headIndex) { next_fi = rt.headIndex; fi_init = 1; }
    /* 1ʳᵉ tranche du grain visé (borné ~1 période ; réveil par le commit du producteur) */
    mxlGrainInfo gi; uint8_t* pay;
    uint64_t _g0 = mono_ns();
    stx = mxlFlowReaderGetGrainSlice(t->reader, next_fi, 1, period_ns + period_ns / 4, &gi, &pay);
    uint64_t tnow = mono_ns();
    /* Attente de la SOURCE seule (cf. champs) : ce que coûte l'obtention de la 1ʳᵉ tranche du grain
     * visé. À comparer à sl_pack_ms et slot_wait_ms — la somme des trois doit expliquer la période
     * du worker. Comptée même en échec : un timeout est de l'attente, et c'est ce qu'on cherche. */
    s->sl_getgrain_ns += tnow - _g0; s->sl_getgrain_cnt++;
    /* ÂGE DU CONTENU À L'ENTRÉE DU TX, contre l'horloge PTP (TAI). L'index de grain MXL est
     * normativement dérivé du temps (index × période = instant de la trame) : cet écart dit donc,
     * en millisecondes et sans aucune comparaison d'index entre flux, l'âge de ce qu'on vient de
     * saisir. C'EST LA MESURE QUI MANQUAIT. Le retour 2110 accuse ~60 ms de plus que la source
     * lue localement ; ce compteur tranche entre les deux seules explications possibles :
     *   • proche de 0   ⇒ le worker prend du frais, les 60 ms sont EN AVAL (ordonnancement libmtl) ;
     *   • proche de 40  ⇒ le worker saisit déjà du vieux, et il faut chercher EN AMONT.
     * Sans lui on ne peut que déduire — et deux déductions se sont déjà révélées fausses ici. */
    if (period_ns && stx == MXL_STATUS_OK) {
      uint64_t _tai = mtl_ptp_read_time_raw(s->st);
      int64_t _age = (int64_t)_tai - (int64_t)(next_fi * period_ns);
      if (_age > -1000000000LL && _age < 1000000000LL) {   /* garde-fou : grille incohérente */
        s->sl_srcage_ns += _age; s->sl_srcage_cnt++;
        if (_age > s->sl_srcage_max) s->sl_srcage_max = _age;
      }
    }
    t->alive_ns = tnow;
    if (stx != MXL_STATUS_OK) {
      if (stx == MXL_ERR_FLOW_NOT_FOUND || stx == MXL_ERR_FLOW_INVALID) {
        /* Flux invalidé EN COURS de lecture (producteur détruit son flux entre l'ouverture du
         * reader et cette 1ʳᵉ tranche) : même trou que les autres échecs mxl* de ce bloc, un slot
         * a déjà été acquis → publier plutôt que sauter le tour, cf. TROU DE COUVERTURE. */
        mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL;
        sl_publish_repeat_or_witness(s, t, idx, fb, morte, now_lf);
        continue;
      }
      /* Timeout : producteur muet/figé. SÉMANTIQUE GEL D'IMAGE (durcissement 0.40.2, iso
       * whole-frame reader_latest) : REJOUER le dernier grain COMPLET de la tête au pacing de
       * sortie — le ring fb fait la contre-pression (un slot ne se libère qu'à l'émission d'une
       * trame) → cadence nominale tenue, la lib reste ALIMENTÉE en continu (l'alimentation
       * sporadique d'un slot à source figée provoquait des trames abandonnées sans frame_done →
       * churn du watchdog anneau toutes les 2 s, sortie à ~1,5 fps — banc 2026-07-11). La
       * détection stale standard (tête figée) continue de tenter GC + réouverture par nom. */
      stx = mxlFlowReaderGetGrainNonBlocking(t->reader, rt.headIndex, &gi, &pay);
      /* Le commentaire ci-dessus promettait « le dernier grain COMPLET » — le code ne le
       * vérifiait pas (2026-08-14). Sur une source tranchée la tête est le plus souvent EN COURS
       * d'écriture : on la refusait donc à tort comme complète. */
      if (stx == MXL_STATUS_OK && !grain_complet(&gi) && rt.headIndex > 0)
        stx = mxlFlowReaderGetGrainNonBlocking(t->reader, rt.headIndex - 1, &gi, &pay);
      if (stx != MXL_STATUS_OK || !grain_complet(&gi)) {   /* aucun grain complet : répli/témoin */
        tx_reopen_if_stale(t, rt.headIndex, tnow);
        sl_publish_repeat_or_witness(s, t, idx, fb, morte, now_lf);
        continue;
      }
      uint16_t total_r = gi.totalSlices ? gi.totalSlices : 1;
      uint32_t slh_r = (uint32_t)H / total_r; if (!slh_r) { slh_r = (uint32_t)H; total_r = 1; }
      size_t _gsr = (size_t)gi.grainSize;
      int src8_r = (_gsr > 0 && _gsr == (size_t)2 * (size_t)W * (size_t)H);
      /* Même règle qu'au chemin nominal : PRÊT seulement quand il y a de quoi émettre (cf. la
       * note détaillée plus bas). Publier à zéro ligne coûtait ici aussi PIT et erreurs de séquence. */
      s->sl_fb[idx].lines_ready = 0;
      s->sl_fb_prod = (uint16_t)((idx + 1) % s->sl_fb_cnt);
      for (uint16_t k = 1; k <= total_r; k++) {
        uint32_t l0 = (uint32_t)(k - 1) * slh_r;
        uint32_t nl = (k == total_r) ? (uint32_t)H - l0 : slh_r;
        sl_pack_band(s, pay, src8_r, l0, nl, fb);
        s->sl_fb[idx].lines_ready = l0 + nl;
        if (k == 1) { __sync_synchronize(); sl_mark_pub(s, idx); }
      }
      sl_note_fresh_pack(s, idx);                /* grain SOURCE réel (relu), éligible au repli répétition */
      t->recv++;                                /* trame (gelée) émise — fps reste nominal */
      t->repeats++;                             /* répétition VISIBLE : même grain que la trame précédente */
      tx_reopen_if_stale(t, rt.headIndex, tnow);
      continue;                                 /* next_fi inchangé : reprise au grain frais */
    }
    uint16_t total = gi.totalSlices ? gi.totalSlices : 1;
    uint32_t slice_h = (uint32_t)H / total;
    if (!slice_h) { slice_h = (uint32_t)H; total = 1; }
    /* profondeur SOURCE dérivée du GRAIN (flux auto-descriptif, cf. chemin whole-frame) */
    size_t _gs = (size_t)gi.grainSize;
    size_t _exp8 = (size_t)2 * (size_t)W * (size_t)H;
    int src8 = (_gs > 0 && (_gs == _exp8 || _gs == _exp8 * 2)) ? (_gs == _exp8) : 0;
    if (!t->dbg_depth_logged) {
      fprintf(stderr, "mtl_rx[video TX SLICE dbg] shm=%s grainSize=%zu totalSlices=%u slice_h=%u src8=%d\n",
              t->shm_path, _gs, (unsigned)total, (unsigned)slice_h, src8);
      t->dbg_depth_logged = 1;
    }
    /* ★ PUBLIER SEULEMENT QUAND IL Y A DE QUOI ÉMETTRE (banc 2026-07-29).
     * On posait `stat = 1` AVANT la boucle de remplissage, donc avec lines_ready = 0. La lib peut
     * se saisir du framebuffer dans cette fenêtre : elle n'a alors aucune ligne à envoyer. Mesuré
     * en production sur un EVS Neuron : PIT max 12 000 à 27 000 (le récepteur refuse au-delà de
     * 1500) et 74 erreurs de séquence RTP par 45 s, sur une sortie dont le DÉBIT était pourtant
     * nominal — la même sortie sans producteur reste à 819 et zéro erreur. La course touche ~16 %
     * des trames (mesurée à ~8/s pour 50), et coûtait aussi 8 fps de contenu neuf.
     * Elle pouvait en outre être FATALE : une trame émise vide devenait la trame de tenue, et la
     * sortie se taisait définitivement (cf. tx_sl_frame_done).
     * On publie donc après la PREMIÈRE bande : la lib a de quoi commencer, et le remplissage
     * progressif continue derrière — la latence sous-trame du mode tranche est préservée, on ne
     * perd qu'une bande (1/30ᵉ de trame à 36 lignes). */
    s->sl_fb[idx].lines_ready = 0;
    s->sl_fb_prod = (uint16_t)((idx + 1) % s->sl_fb_cnt);
    int fully_fresh = 1;   /* faux si le producteur meurt EN COURS de trame (bande noire, cf. break) */
    for (uint16_t k = 1; k <= total && !s->stop; k++) {
      if (k > 1) {
        stx = mxlFlowReaderGetGrainSlice(t->reader, next_fi, k, period_ns, &gi, &pay);
        if (stx != MXL_STATUS_OK) {             /* producteur mort en cours de trame → noir */
          uint32_t l0 = (uint32_t)(k - 1) * slice_h;
          memset(fb + (size_t)l0 * bpl_be, 0, (size_t)((uint32_t)H - l0) * bpl_be);
          s->sl_fb[idx].lines_ready = (uint32_t)H;
          fully_fresh = 0;
          break;
        }
      }
      uint32_t l0 = (uint32_t)(k - 1) * slice_h;
      uint32_t nl = (k == total) ? (uint32_t)H - l0 : slice_h;
      uint64_t _p0 = mono_ns();
      sl_pack_band(s, pay, src8, l0, nl, fb);
      s->sl_pack_ns += mono_ns() - _p0; s->sl_pack_cnt++;   /* coût RÉEL du packing (cf. champs) */
      s->sl_fb[idx].lines_ready = l0 + nl;
      /* Barrière AVANT publication, comme les chemins statiques (`contenu EN PLACE avant
       * d'annoncer le slot prêt`) : sans elle l'écriture de la bande peut être réordonnée après
       * le store de `stat`, et la lib émettrait du contenu non encore écrit. */
      if (k == 1) { __sync_synchronize(); sl_mark_pub(s, idx); }
    }
    /* n'alimenter le carrier QUE si la trame est intégralement réelle (pas de bande noire partielle
     * de secours) : sinon une répétition ailleurs dans l'anneau republierait une image mi-noire. */
    if (fully_fresh) sl_note_fresh_pack(s, idx);
    if (period_ns && t->last_feed_ns) {         /* compteur late : trame source ratée */
      /* Le gap entre deux alimentations inclut le temps passé BLOQUÉ en attente d'un slot libre
       * de l'anneau fb (contre-pression : avec epoch_shift_us la lib retient chaque trame `shift`
       * plus tard → faux `late` ~1,7/min alors que la sortie fil est parfaite — banc 2026-07-10).
       * On SOUSTRAIT cette attente pour ne mesurer que la SOURCE (seuil 1,5 période inchangé). */
      uint64_t gap = tnow - t->last_feed_ns;
      /* On soustrait AUSSI le bridage d'avance : c'est du temps qu'on s'impose, pas un retard de
       * la source. Sans ça, activer `advance` ferait grimper `late` alors que la sortie fil est
       * parfaite — exactement le faux positif déjà rencontré avec `epoch_shift_us`. */
      uint64_t w = t->slot_wait_ns + t->adv_wait_ns;
      gap = gap > w ? gap - w : 0;
      if (gap > period_ns + period_ns / 2) {
        uint64_t missed = (gap + period_ns / 2) / period_ns;
        t->late += missed > 1 ? missed - 1 : 1;
      }
    }
    t->slot_wait_ns = 0; t->adv_wait_ns = 0;
    t->last_feed_ns = tnow;
    t->index = next_fi; t->recv++;
    tx_reopen_if_stale(t, next_fi, tnow);
    next_fi++;
  }
  return NULL;
}

/* Setup TX vidéo MODE TRANCHE (raw st20, ST20_TYPE_SLICE_LEVEL). Progressive uniquement. */
static int setup_video_slice_tx(struct sess* s) {
  s->slice_on = 1;
  s->slotsize = (size_t)(s->bit_depth == 8 ? 2 : 4) * (size_t)s->width * (size_t)s->height;
  s->shm_slotsize = s->slotsize;
  pthread_mutex_init(&s->sl_mx, NULL); pthread_cond_init(&s->sl_cv, NULL);
  /* 4 framebuffers : 1 seul reste immobilisé en permanence — la TRAME DE TENUE (horloge de sortie,
   * cf. tx_sl_next_frame) — le worker en garde 3. Le TÉMOIN DE REPLI (bobi.studio) ne réserve plus
   * de slot dédié : il est publié par le worker DANS ces 3 slots normaux (cf. video_tx_slice_thread),
   * exactement comme la branche statique — donc aucun besoin d'agrandir l'anneau pour lui.
   *
   * ⚠ 2026-08-09 : ce 4 était CODÉ EN DUR et écrasait le `ring` de la config (8 demandé, 4 appliqué).
   * Le raisonnement ci-dessus ne portait que sur la SUFFISANCE fonctionnelle de la tenue, jamais sur
   * le DÉBIT. Or mesuré ce jour sur TX0 (source = mur, 50 grains neufs/s disponibles) : l'anneau est
   * plein en permanence (stat [1,1,1,2]) et le worker passe 59 % du temps bloqué en attente de slot
   * (slot_wait 1181 ms par fenêtre de stats de 2 s, 75 blocages) — la sortie se stabilise à 37 trames
   * fraîches/s, une sur quatre étant répétée. On HONORE donc le `ring` du sig — ce qui, avec la
   * config actuelle (ring=8), DOUBLE l'anneau de TX0 dès la reconstruction. Ce n'est pas un effet de
   * bord : c'est le correctif, la valeur du sig était ignorée en silence. À mesurer avant/après sur
   * `slot_wait_ms` et `srv.fresh`. Cesse de valoir si la lib change sa politique de libération. */
  {
    uint16_t _r = (uint16_t)s->ring;
    /* ★ PLANCHER ABAISSÉ À 3 (2026-08-11) — c'est la LATENCE DE SORTIE qu'il fixait.
     * ÉTABLI le même jour : l'anneau sert la trame la plus ANCIENNE (FIFO). Mesuré sur trois
     * émetteurs et douze relevés, `dist` = `depth` − 1 SANS EXCEPTION — la règle de lecture posée
     * par le commentaire de `tx_sl_next_frame` (« servi le plus ancien ⇒ dist ≈ depth »). Donc
     * la profondeur de l'anneau EST la latence : 3 slots worker = 2 images de retard à l'antenne.
     * Le plancher de 4 n'était pas un choix de latence, c'était « 3 slots worker + la tenue »,
     * hérité d'une époque où le sens de service était inconnu (cf. l'échec du drain de juillet,
     * qui jetait précisément la trame que la lib allait servir).
     * 3 = 2 slots worker + la tenue → 1 image. Le SDK n'exige que `framebuff_cnt >= 2`.
     * ⚠ CE N'EST PAS GRATUIT : moins d'avance, c'est moins d'amorti. Si le producteur hoquette,
     * la lib ne trouve rien de frais et RÉPÈTE. Les deux compteurs qui arbitrent sont déjà
     * publiés — `slot_wait_ms` (le worker s'étrangle-t-il ?) et `repeats` (l'antenne répète-t-elle ?).
     * Mesuré à ring=8 avant la bascule : slot_wait 0,0 ms, 0 répétition en régime, depth 2,9 —
     * donc de la marge. À re-mesurer sur CHAQUE émetteur après changement : un TX alimenté par
     * une source irrégulière n'a pas la même réserve qu'un TX alimenté par un mur genlocké.
     * Le défaut reste 4 (`t.get("ring") or 8` côté orchestrateur, puis ce plancher) : descendre
     * est un ACTE, jamais un effet de bord d'un redéploiement. */
    if (_r < 3) _r = 3;
    if (_r > SL_Q) _r = SL_Q;
    s->sl_fb_cnt = _r;
  }
  s->sl_fb_prod = 0; s->sl_fb_cons = 0;
  s->sl_hold_valid = 0; s->sl_hold_emitted = 0; s->sl_hold_last_ns = 0;
  s->sl_witness_switch_ns = 0; s->sl_witness_cur = 0; s->sl_witness_gen = 0;
  s->sl_carrier_valid = 0; s->sl_carrier_idx = 0; s->sl_content_gen = 0;
  s->sl_period_ns = s->fps > 0 ? (uint64_t)(1e9 / s->fps) : 0;

  struct st20_tx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_vtx_sl";
  ops.priv = s;
  ops.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.dip_addr[MTL_SESSION_PORT_P]);   /* destination */
  snprintf(ops.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.dip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.payload_type = s->payload_type;
  ops.ssrc = s->ssrc;   /* fixe (≠0) pour matcher le a=ssrc annoncé dans le SDP TX */
  ops.width = s->width; ops.height = s->height; ops.fps = to_st_fps(s->fps);
  ops.interlaced = false;
  ops.fmt = ST20_FMT_YUV_422_10BIT;
  ops.type = ST20_TYPE_SLICE_LEVEL;
  ops.framebuff_cnt = s->sl_fb_cnt;
  ops.get_next_frame = tx_sl_next_frame;
  ops.notify_frame_done = tx_sl_frame_done;
  ops.query_frame_lines_ready = tx_sl_lines_ready;
  /* CLASSE 2110-21 PAR SESSION (#26) : même résolution que le chemin whole-frame. */
  ops.pacing = resolve_profile(s->iface);
  /* EPOCH-SHIFT TX (patch bobi patch_epoch_shift) : delta NÉGATIF ⇒ libmtl décale la grille
   * d'émission de +epoch_shift_us APRÈS l'epoch nominal et le stamp RTP retombe sur l'epoch
   * NOMINAL (le récepteur mesure FPT ≈ shift = TROFF). Dans le sig ⇒ changer le shift recrée
   * la session (le pacing libmtl est figé à la création). */
  if (s->epoch_shift_us > 0) ops.rtp_timestamp_delta_us = -(int32_t)s->epoch_shift_us;

  s->sl_tx = st20_tx_create(s->st, &ops);
  if (!s->sl_tx) {
    fprintf(stderr, "mtl_rx: st20_tx_create SLICE fail (video %s:%d)\n", s->mcast, s->udp_port);
    pthread_mutex_destroy(&s->sl_mx); pthread_cond_destroy(&s->sl_cv);
    return -1;
  }
  fprintf(stderr, "mtl_rx[video TX SLICE] %dx%dp fps=%.2f pt=%d ssrc=%u → %s:%d (in shm=%s bd%d)%s\n",
          s->width, s->height, s->fps, s->payload_type, s->ssrc, s->mcast, s->udp_port,
          s->tg[0].shm_path, s->bit_depth,
          s->tg[0].has_ident ? " [IDENT ignoré en mode tranche]" : "");
  if (s->epoch_shift_us > 0)
    fprintf(stderr, "mtl_rx[video TX SLICE] epoch-shift +%d µs (émission décalée, stamp RTP sur l'epoch nominal)\n",
            s->epoch_shift_us);
  /* AMORÇAGE DE LA TRAME DE TENUE (bobi.studio) : la réserver ICI, à la création — pas d'attendre
   * la 1ère trame émise par le worker (cf. tx_sl_frame_done) — sinon rien à re-servir si le
   * producteur n'apparaît JAMAIS, ou meurt avant d'avoir livré une trame complète. On réserve le
   * DERNIER slot de l'anneau (jamais visé par sl_fb_prod, cf. la garde en tête de
   * video_tx_slice_thread) et on le remplit avec un NOIR LÉGAL 10 bits narrow (Y=64, Cb=Cr=512).
   * ⚠ Le framebuffer est RFC4175 4:2:2 10 bits BE (st20_rfc4175_422_10_pg2_be), PAS planar : un
   * memset(0) donnerait une image verte illégale. On construit donc une bande planar 16 bits
   * (Y puis Cb puis Cr, comme l'attend sl_pack_band côté src8=0 — cf. sa lecture de `pay`) dans un
   * tampon temporaire, packée UNE fois via le pack SIMD existant, puis libérée : setup, pas le
   * chemin critique (tasklet lcore). */
  {
    uint16_t hidx = (uint16_t)(s->sl_fb_cnt - 1);
    uint8_t* hfb = st20_tx_get_framebuffer(s->sl_tx, hidx);
    size_t nsamp = (size_t)2 * s->width * s->height;    /* Y (W·H) + Cb (W/2·H) + Cr (W/2·H) */
    uint16_t* black = hfb ? malloc(nsamp * sizeof(uint16_t)) : NULL;
    if (black) {
      size_t npix = (size_t)s->width * s->height;
      uint16_t* y = black;
      uint16_t* cbcr = black + npix;                    /* Cb (npix/2) puis Cr (npix/2), même valeur */
      for (size_t k = 0; k < npix; k++) y[k] = 64;
      for (size_t k = 0; k < npix; k++) cbcr[k] = 512;
      sl_pack_band(s, (uint8_t*)black, 0, 0, (uint32_t)s->height, hfb);
      free(black);
      s->sl_fb[hidx].lines_ready = (uint32_t)s->height;
      s->sl_fb[hidx].stat = 2;             /* TENUE dès la création : re-servie tant qu'aucune trame
                                             * réelle n'a encore été émise (cf. tx_sl_next_frame) */
      s->sl_hold_idx = hidx; s->sl_hold_valid = 1;
    } else {
      fprintf(stderr, "mtl_rx[video TX SLICE] %s:%d amorçage trame noire échoué (alloc/fb) — "
              "sortie MUETTE jusqu'à la 1ère trame réelle\n", s->mcast, s->udp_port);
    }
  }
  /* Plus d'amorçage de paire de repli ici : le TÉMOIN DE REPLI (bobi.studio) est désormais publié
   * par le worker DANS les slots normaux (cf. `morte` dans video_tx_slice_thread), avec
   * sl_fill_fallback_frame réutilisée pour le contenu — rien à réserver au setup pour lui. */
  if (pthread_create(&s->thread, NULL, video_tx_slice_thread, s) != 0) {
    st20_tx_free(s->sl_tx); s->sl_tx = NULL;
    pthread_mutex_destroy(&s->sl_mx); pthread_cond_destroy(&s->sl_cv);
    return -1;
  }
  s->started = 1;
  return 0;
}

static int setup_video_tx(struct sess* s) {
  /* MODE TRANCHE (SLICE_MODE=1) : bascule env-gatée (progressive uniquement — l'entrelacé garde
   * le chemin champ-natif whole-frame). Le pas de tranche vient du flux SOURCE (totalSlices). */
  if (slice_wanted() && !s->interlaced) return setup_video_slice_tx(s);
  if (s->ring < 2) s->ring = 2;                 /* (vestige) — MXL gère le ring du flux d'entrée */
  /* Champ-natif : taille d'un GRAIN = 1 CHAMP en entrelacé (½ hauteur), 1 trame en progressif.
   * 422 planar : 8b = 2·w·h_grain, 10b = 4·w·h_grain. = grainSize du flux d'entrée. */
  size_t h_grain = s->interlaced ? (size_t)s->height / 2 : (size_t)s->height;
  s->slotsize = (size_t)(s->bit_depth == 8 ? 2 : 4) * (size_t)s->width * h_grain;
  s->shm_slotsize = s->slotsize;

  struct st20p_tx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_vtx";
  ops.priv = s;
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.dip_addr[MTL_SESSION_PORT_P]);   /* destination */
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.dip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.port.payload_type = s->payload_type;
  ops.port.ssrc = s->ssrc;   /* fixe (≠0) pour matcher le a=ssrc annoncé dans le SDP TX */
  /* CADENCE ENTRELACÉ (fix racine du peigne) : libmtl TX entrelacé avec fps=cadence TRAME (P25)
   * pace get_frame à la période TRAME (40 ms) → 1 champ/40 ms = 25 champs/s = MOITIÉ de 1080i50 →
   * le désentrelaceur récepteur peigne. On passe la cadence CHAMP (×2 → P50) : libmtl pace alors à
   * 20 ms/champ = 50 champs/s, ET signale toujours exactframerate=25 dans le SDP (il /2 en entrelacé).
   * Le RX n'était pas affecté (cadencé par le réseau, pas par ops.fps). */
  ops.width = s->width; ops.height = s->height;
  ops.fps = to_st_fps(s->interlaced ? s->fps * 2.0 : s->fps);
  ops.interlaced = s->interlaced ? true : false;
  ops.input_fmt = ST_FRAME_FMT_YUV422PLANAR10LE;   /* on fournit du 10-bit (up-shift 8→10 nous-mêmes) */
  ops.transport_fmt = ST20_FMT_YUV_422_10BIT;
  ops.device = ST_PLUGIN_DEVICE_AUTO;
  /* ★ 2026-08-09 : était `3` en dur — le `ring` du sig (donc le réglage `shm_video_ring`) n'arrivait
   * pas jusqu'ici, exactement comme le `sl_fb_cnt = 4` du chemin TRANCHE corrigé le même jour. Là-bas,
   * 3 slots utilisables ont coûté 59 % de blocage du worker et UNE TRAME SUR QUATRE répétée à
   * l'antenne (mesuré sur TX0, 1080p50). Ce chemin-ci n'a PAS été mesuré — il n'est pas utilisé en
   * production aujourd'hui (les TX vidéo tournent en mode tranche) — mais il portait la même
   * constante, et rien ne justifiait qu'elle soit plus petite. Plancher 4 (au cas où le sig
   * enverrait moins), plafond ST20_FB_MAX_COUNT = 8, seule contrainte énoncée par le SDK. */
  {
    uint16_t _r = (uint16_t)s->ring;
    if (_r < 4) _r = 4;
    if (_r > 8) _r = 8;                            /* ST20_FB_MAX_COUNT */
    ops.framebuff_cnt = _r;
  }
  ops.flags = ST20P_TX_FLAG_BLOCK_GET;             /* get_frame bloque → pacing à fps */
  /* CLASSE 2110-21 PAR SESSION (#26) : cible VRX (narrow/NL/wide) selon le profil de la NIC de
   * sortie (node_interfaces.output_profile → ports[].profile). NARROW par défaut (memset l'a déjà
   * posé, resolve_profile le confirme). Sous mécanisme RL device, chaque session honore SA classe →
   * narrow sur un port, wide sur un autre, simultanément. Sous TSC, la classe oriente la cible du
   * pacer logiciel. Le leg redondant (2022-7) suit la même classe (attribut de session). */
  ops.transport_pacing = resolve_profile(s->iface);
  /* EPOCH-SHIFT TX (patch bobi patch_epoch_shift) : delta NÉGATIF ⇒ libmtl décale la grille
   * d'émission de +epoch_shift_us APRÈS l'epoch nominal et le stamp RTP retombe sur l'epoch
   * NOMINAL (le récepteur mesure FPT ≈ shift = TROFF). st20p_tx_ops PORTE bien le champ
   * (st_pipeline_api.h, relayé tel quel par st20_pipeline_tx.c → st20_tx_ops) — le chemin
   * whole-frame est donc couvert comme le chemin tranche. En entrelacé le shift s'applique
   * par CHAMP (la grille libmtl est la grille champ). Dans le sig ⇒ changer le shift recrée
   * la session (le pacing libmtl est figé à la création). */
  if (s->epoch_shift_us > 0) ops.rtp_timestamp_delta_us = -(int32_t)s->epoch_shift_us;

  s->vth = st20p_tx_create(s->st, &ops);
  if (!s->vth) { fprintf(stderr, "mtl_rx: st20p_tx_create fail (video %s:%d)\n", s->mcast, s->udp_port); return -1; }
  fprintf(stderr, "mtl_rx[video TX] %dx%d%s fps=%.2f pt=%d ssrc=%u → %s:%d (in shm=%s bd%d ring%d)\n",
          s->width, s->height, s->interlaced ? "i" : "p", s->fps, s->payload_type, s->ssrc,
          s->mcast, s->udp_port, s->tg[0].shm_path, s->bit_depth, s->ring);
  if (s->epoch_shift_us > 0)
    fprintf(stderr, "mtl_rx[video TX] epoch-shift +%d µs (émission décalée, stamp RTP sur l'epoch nominal)\n",
            s->epoch_shift_us);
  return pthread_create(&s->thread, NULL, video_tx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

static int setup_audio(struct sess* s) {
  if (s->channels <= 0) s->channels = 8;
  if (s->ring < 2) s->ring = 2;
  s->slotsize = (size_t)(48000 / 1000) * s->channels * 3;   /* 1 ms L24 = 1152 si 8ch */
  s->copy_mode = 1;

  struct st30p_rx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_a";
  ops.priv = s;
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.ip_addr[MTL_SESSION_PORT_P]);
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.ip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.port.payload_type = s->payload_type;
  ops.fmt = ST30_FMT_PCM24;
  ops.channel = (uint16_t)s->channels;
  ops.sampling = ST30_SAMPLING_48K;
  ops.ptime = to_st30_ptime(s->a_ptime);   /* AUTO depuis le SDP (a=ptime) / défaut réglage */
  ops.framebuff_size = (uint32_t)s->slotsize;   /* 1 chunk = 1 ms */
  /* bobi.studio: 4 → 32 (0.44.2). Le pool de 4 trames de 1 ms débordait dès qu'un hoquet
   * d'ordonnancement du thread audio_rx_thread (famine CPU sur cpuset étroit, cf. volet 1)
   * dépassait ~4 ms : « RX_AUDIO_SESSION back-pressure: framebuff pool empty (4/4 free) »,
   * 1,5-2,4 % de trames droppées en continu → trous silencieux dans le flux MXL, comblés en
   * VIEUX contenu d'anneau côté lecture (bouillie audio). 32 trames = 32 ms de marge d'absorption,
   * coût mémoire négligeable (32 × 1152 o/canal en 8ch). */
  ops.framebuff_cnt = 32;

  s->ah = st30p_rx_create(s->st, &ops);
  if (!s->ah) { fprintf(stderr, "mtl_rx: st30p_rx_create fail (audio %s:%d)\n", s->mcast, s->udp_port); return -1; }
  s->srate.numerator = 48000; s->srate.denominator = 1;   /* grille sample TAI */
  if (open_targets(s) != 0) return -1;
  fprintf(stderr, "mtl_rx[audio] %dch L24/48k pt=%d mc=%s:%d slot=%zu ring=%d → %d cible(s):",
          s->channels, s->payload_type, s->mcast, s->udp_port, s->slotsize, s->ring, s->ntg);
  for (int ti = 0; ti < s->ntg; ti++) fprintf(stderr, " %s", s->tg[ti].shm_path);
  fprintf(stderr, "\n");
  return pthread_create(&s->thread, NULL, audio_rx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

/* ═══ AUDIO TX (flux MXL → wire, st30p_tx) ═════════════════════════════════════ */
/* Derniers `n` samples dispo (NON bloquant) du flux audio → slc, ou -1. Recrée le reader si le
 * flux a été invalidé (producteur redéployé). */
static int reader_samples(struct target* t, size_t n, mxlWrappedMultiBufferSlice* slc) {
  mxlFlowRuntimeInfo rt;
  mxlStatus st = mxlFlowReaderGetRuntimeInfo(t->reader, &rt);
  if (st == MXL_STATUS_OK && rt.headIndex != MXL_UNDEFINED_INDEX) {
    st = mxlFlowReaderGetSamplesNonBlocking(t->reader, rt.headIndex, n, slc);
    if (st == MXL_STATUS_OK) return 0;
  }
  if (st == MXL_ERR_FLOW_NOT_FOUND || st == MXL_ERR_FLOW_INVALID) {
    mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL;
  }
  return -1;
}

/* Feeder audio TX : lit les derniers samples float32 du flux MXL d'entrée câblé, les CONVERTIT en
 * L24 BG (wire-native ST 2110-30) et les émet. reader créé paresseusement, pacing par BLOCK_GET. */
static void* audio_tx_thread(void* arg) {
  struct sess* s = arg;
  struct target* t = &s->tg[0];
  int chs = s->channels;
  while (!s->stop) {
    if (tx_take_source(t) && t->reader) {   /* bobi.studio: source audio permutée à chaud → rouvrir */
      mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL; t->tx_src_idx_init = 0;
    }
    /* SILENCE SANS PRODUCTEUR (pendant audio du repli vidéo statique) : une sortie provisionnée
     * dont l'audio n'est pas câblé doit émettre du SILENCE, pas RIEN — sinon la session (et sa
     * feuille RL) disparaît, et câbler l'audio plus tard imposerait une recréation, donc un commit
     * RL et un stop de port. Faire tourner un producteur pour du vide n'a en revanche aucun sens :
     * ~1000 réveils/s et par sortie pour des blocs de 1 ms de zéros. Sans source, on descend donc
     * directement au get_frame — le chemin « pas de samples → silence » plus bas fait déjà le
     * travail, au pacing de la session. Le silence n'est pas un signal : c'est une absence. */
    int muet = !t->shm_path[0];
    if (!muet && !t->reader) {
      t->alive_ns = mono_ns();   /* attendre un câblage n'est pas un wedge */
      if (open_reader(t) != 0) { usleep(20000); continue; }
    }
    if (muet && t->reader) {     /* audio décâblé → on lâche le reader et on passe au silence */
      mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL; t->tx_src_idx_init = 0;
    }
    struct st30_frame* frame = st30p_tx_get_frame(s->a_tx);   /* bloque (BLOCK_GET) → pacing 1ms */
    if (!frame) { usleep(500); continue; }
    t->alive_ns = mono_ns();     /* la session transmet (frames libérées par MTL) */
    uint8_t* dst = (uint8_t*)frame->addr;
    size_t n = frame->data_size / (size_t)(chs * 3);          /* samples par canal à émettre */
    mxlWrappedMultiBufferSlice slc; memset(&slc, 0, sizeof(slc));
    if (!muet && n && reader_samples(t, n, &slc) == 0) {
      size_t stride = slc.stride;
      for (size_t c = 0; c < slc.count; c++) {
        size_t pos = 0;
        for (int f = 0; f < 2; f++) {
          size_t fb = slc.base.fragments[f].size;
          if (!fb) continue;
          const float* sp = (const float*)((const uint8_t*)slc.base.fragments[f].pointer + c * stride);
          size_t cnt = fb / 4;
          for (size_t k = 0; k < cnt; k++) {
            float v = sp[k];
            if (v > 1.0f) v = 1.0f; else if (v < -1.0f) v = -1.0f;
            int32_t iv = (int32_t)lrintf(v * 8388607.0f);      /* float → 24 bits signés */
            uint8_t* p = dst + ((pos + k) * (size_t)chs + c) * 3;
            p[0] = (uint8_t)((iv >> 16) & 0xff);               /* big-endian (wire) */
            p[1] = (uint8_t)((iv >> 8) & 0xff);
            p[2] = (uint8_t)(iv & 0xff);
          }
          pos += cnt;
        }
      }
    } else {
      memset(dst, 0, frame->data_size);                        /* pas de samples → silence */
    }
    st30p_tx_put_frame(s->a_tx, frame);
    t->recv++;
  }
  return NULL;
}

static int setup_audio_tx(struct sess* s) {
  if (s->channels <= 0) s->channels = 8;
  if (s->ring < 2) s->ring = 2;
  s->slotsize = (size_t)(48000 / 1000) * s->channels * 3;   /* 1 ms L24 = 1152 si 8ch */

  struct st30p_tx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_atx";
  ops.priv = s;
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.dip_addr[MTL_SESSION_PORT_P]);   /* destination */
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.dip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.port.payload_type = s->payload_type;
  ops.port.ssrc = s->ssrc;   /* fixe (≠0) pour matcher le a=ssrc annoncé dans le SDP TX */
  ops.fmt = ST30_FMT_PCM24;
  ops.channel = (uint16_t)s->channels;
  ops.sampling = ST30_SAMPLING_48K;
  ops.ptime = to_st30_ptime(s->a_ptime);   /* AUTO depuis le SDP (a=ptime) / défaut réglage */
  ops.framebuff_size = (uint32_t)s->slotsize;
  ops.framebuff_cnt = 4;
  ops.flags = ST30P_TX_FLAG_BLOCK_GET;

  s->a_tx = st30p_tx_create(s->st, &ops);
  if (!s->a_tx) { fprintf(stderr, "mtl_rx: st30p_tx_create fail (audio %s:%d)\n", s->mcast, s->udp_port); return -1; }
  fprintf(stderr, "mtl_rx[audio TX] %dch L24/48k pt=%d ssrc=%u → %s:%d (in shm=%s)\n",
          s->channels, s->payload_type, s->ssrc, s->mcast, s->udp_port, s->tg[0].shm_path);
  return pthread_create(&s->thread, NULL, audio_tx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

/* ═══ DATA / ANC — ST 2110-40 (st40p) ═════════════════════════════════════════ */
/* Le pipeline st40p présente udw_buff_addr comme un tableau d'OCTETS (1 o = 1 UDW, low 8 bits,
 * parité déjà vérifiée à la réception) ; meta[k].udw_offset = offset OCTET dans ce tableau,
 * meta[k].udw_size = nombre de UDW. RX et TX utilisent ce MÊME format → passthrough = copie. */

/* Décode l'ATC (SMPTE ST 12-1 / RP 188, DID 0x60 SDID 0x60). 16 UDW → 8 octets de timecode :
 * octet[i] = low-nibble(UDW[2i]) | low-nibble(UDW[2i+1])<<4. Octets 0/2/4/6 = frames/sec/min/h
 * (BCD : unités b0-3, dizaines b4+). DF = bit 6 de l'octet 0. Renvoie 1 si un ATC est trouvé. */
static int decode_atc(struct st40_frame_info* f, char* out, int* df) {
  for (uint32_t m = 0; m < f->meta_num; m++) {
    struct st40_meta* md = &f->meta[m];
    if (md->did != 0x60 || md->sdid != 0x60) continue;
    if (md->udw_size < 16) continue;
    const uint8_t* w = f->udw_buff_addr + md->udw_offset;   /* 1 octet = 1 UDW (low 8 bits) */
    /* Disposition LTC : UN CHIFFRE BCD PAR QUARTET, dans le quartet HAUT de chaque UDW ; les
     * UDW impairs portent les drapeaux. Deux erreurs successives corrigées le 2026-08-07 : on
     * lisait les quartets BAS (timecode figé à 00:00:00:00, un bit qui clignotait), puis on a
     * recollé deux quartets en un octet, ce qui PERDAIT LES DIZAINES D'IMAGES (00:01:05:21 lu
     * 00:01:05:01) — invisible tant que le compteur restait sous dix. Miroir de
     * bobimxl.anc_atc_encode ; les trois implémentations changent ENSEMBLE. */
    uint8_t q[16];
    for (int i = 0; i < 16; i++) q[i] = (uint8_t)((w[i] >> 4) & 0x0f);
    int frames  = q[0]  + (q[2]  & 0x03) * 10;
    int seconds = q[4]  + (q[6]  & 0x07) * 10;
    int minutes = q[8]  + (q[10] & 0x07) * 10;
    int hours   = q[12] + (q[14] & 0x03) * 10;
    *df = (q[2] >> 2) & 0x01;
    snprintf(out, 16, "%02d:%02d:%02d%c%02d", hours, minutes, seconds, *df ? ';' : ':', frames);
    return 1;
  }
  return 0;
}

/* Feeder RX ANC : st40p_rx_get_frame → sérialise meta[] + udw dans le slot courant (fan-out),
 * + extraction du timecode ATC publié dans les stats. */
static void* data_rx_thread(void* arg) {
  struct sess* s = arg;
  rt_thread_priority("data_rx_thread");
  while (!s->stop) {
    struct st40_frame_info* frame = st40p_rx_get_frame(s->d_rx);
    if (!frame) { usleep(1000); continue; }
    char tc[16]; int df = 0; int got_tc = decode_atc(frame, tc, &df);
    uint64_t idx = mxlGetCurrentIndex(&s->mrate);   /* grille trame TAI (ANC ~1 grain/trame) */
    for (int ti = 0; ti < s->ntg; ti++) {
      struct target* t = &s->tg[ti];
      mxlGrainInfo gi; uint8_t* dst;
      if (mxlFlowWriterOpenGrain(t->writer, idx, &gi, &dst) != MXL_STATUS_OK) continue;
      /* Sérialisation NORMATIVE RFC 8331 (interopérable). Tronque proprement si le grain est
       * trop petit — jamais de débordement ; un frame vide donne un grain « 0 paquet ». */
      memset(dst, 0, ANC_HDR_BYTES);
      anc_pack_rfc8331(dst, gi.grainSize, frame);
      if (got_tc) { memcpy(t->tc, tc, sizeof(t->tc)); t->tc_df = df; t->tc_valid = 1; }
      gi.validSlices = gi.totalSlices;
      mxlFlowWriterCommitGrain(t->writer, &gi);
      t->index = idx; t->recv++;
    }
    st40p_rx_put_frame(s->d_rx, frame);
  }
  return NULL;
}

/* Feeder TX ANC : lit le slot courant du shm d'entrée → reconstruit meta[]+udw → st40p_tx_put_frame.
 * Passthrough intégral (les udw_offset restent valides : on recopie tout le buffer udw verbatim). */
static void* data_tx_thread(void* arg) {
  struct sess* s = arg;
  struct target* t = &s->tg[0];
  while (!s->stop) {
    if (tx_take_source(t) && t->reader) {   /* bobi.studio: source ANC permutée à chaud → rouvrir */
      mxlReleaseFlowReader(g_mxl, t->reader); t->reader = NULL; t->tx_src_idx_init = 0;
    }
    if (!t->reader) {
      t->alive_ns = mono_ns();   /* attendre un câblage n'est pas un wedge */
      if (open_reader(t) != 0) { usleep(20000); continue; }
    }
    struct st40_frame_info* frame = st40p_tx_get_frame(s->d_tx);   /* bloque (BLOCK_GET) → pacing fps */
    if (!frame) { usleep(1000); continue; }
    t->alive_ns = mono_ns();     /* la session transmet (frames libérées par MTL) */
    mxlGrainInfo gi; uint8_t* src;
    if (reader_latest(t, &gi, &src) == 0) {
      /* Aiguillage sur le codage ANNONCÉ par le producteur (flotte MIXTE pendant la migration).
       * Résolu UNE fois par reader (le flowDef est immuable) — pas de mxlGetFlowDef par trame. */
      if (!t->anc_fmt_init) {
        char id[37]; flow_id_str(flow_name(t->shm_path), id);
        t->anc_rfc8331 = anc_flow_is_rfc8331(id);
        t->anc_fmt_init = 1;
      }
      if (t->anc_rfc8331)
        anc_unpack_rfc8331(src, gi.grainSize, frame, s->max_udw);
      else
        anc_unpack_bobi_v1(src, gi.grainSize, frame, s->max_udw);
      t->index = gi.index;
    } else {
      frame->meta_num = 0; frame->udw_buffer_fill = 0;   /* pas de grain → ANC vide */
    }
    st40p_tx_put_frame(s->d_tx, frame);
    t->recv++;
  }
  return NULL;
}

static int setup_data(struct sess* s) {
  if (s->ring > 8) s->ring = 8; if (s->ring < 2) s->ring = 2;
  s->slotsize = ANC_SLOT; s->max_udw = ANC_MAX_UDW; s->copy_mode = 1;

  struct st40p_rx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_d";
  ops.priv = s;
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.ip_addr[MTL_SESSION_PORT_P]);
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.ip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.port.payload_type = s->payload_type;
  ops.framebuff_cnt = 4;
  ops.max_udw_buff_size = s->max_udw;
  ops.rtp_ring_size = 1024;   /* requis (>0, puissance de 2 : ring DPDK des paquets RTP ANC) */
  ops.flags = ST40P_RX_FLAG_BLOCK_GET;

  s->d_rx = st40p_rx_create(s->st, &ops);
  if (!s->d_rx) { fprintf(stderr, "mtl_rx: st40p_rx_create fail (data %s:%d)\n", s->mcast, s->udp_port); return -1; }
  s->mrate = fps_to_rational(s->fps);            /* grille TAI ANC (cadence trame) */
  if (open_targets(s) != 0) return -1;
  fprintf(stderr, "mtl_rx[data] ANC pt=%d mc=%s:%d slot=%zu ring=%d → %d cible(s):",
          s->payload_type, s->mcast, s->udp_port, s->slotsize, s->ring, s->ntg);
  for (int ti = 0; ti < s->ntg; ti++) fprintf(stderr, " %s", s->tg[ti].shm_path);
  fprintf(stderr, "\n");
  return pthread_create(&s->thread, NULL, data_rx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

static int setup_data_tx(struct sess* s) {
  if (s->ring < 2) s->ring = 2;
  s->slotsize = ANC_SLOT; s->max_udw = ANC_MAX_UDW;

  struct st40p_tx_ops ops; memset(&ops, 0, sizeof(ops));
  ops.name = "bobi_mtl_dtx";
  ops.priv = s;
  ops.port.num_port = s->num_leg;
  inet_pton(AF_INET, s->mcast, ops.port.dip_addr[MTL_SESSION_PORT_P]);   /* destination */
  snprintf(ops.port.port[MTL_SESSION_PORT_P], MTL_PORT_MAX_LEN, "%s", s->portname);
  ops.port.udp_port[MTL_SESSION_PORT_P] = s->udp_port;
  if (s->num_leg == 2) {   /* 2022-7 : 2ᵉ patte red/blue */
    inet_pton(AF_INET, s->mcast_r, ops.port.dip_addr[MTL_SESSION_PORT_R]);
    snprintf(ops.port.port[MTL_SESSION_PORT_R], MTL_PORT_MAX_LEN, "%s", s->portname_r);
    ops.port.udp_port[MTL_SESSION_PORT_R] = s->udp_port_r;
  }
  ops.port.payload_type = s->payload_type;
  ops.port.ssrc = s->ssrc;   /* fixe (≠0) pour matcher le a=ssrc annoncé dans le SDP TX */
  ops.fps = to_st_fps(s->fps);
  ops.interlaced = false;
  ops.framebuff_cnt = 4;
  ops.max_udw_buff_size = s->max_udw;
  ops.flags = ST40P_TX_FLAG_BLOCK_GET;

  s->d_tx = st40p_tx_create(s->st, &ops);
  if (!s->d_tx) { fprintf(stderr, "mtl_rx: st40p_tx_create fail (data %s:%d)\n", s->mcast, s->udp_port); return -1; }
  fprintf(stderr, "mtl_rx[data TX] ANC fps=%.2f pt=%d ssrc=%u → %s:%d (in shm=%s)\n",
          s->fps, s->payload_type, s->ssrc, s->mcast, s->udp_port, s->tg[0].shm_path);
  return pthread_create(&s->thread, NULL, data_tx_thread, s) == 0 ? (s->started = 1, 0) : -1;
}

/* ── parse config JSON ── */
static const char* jstr(struct json_object* o, const char* k, const char* def) {
  struct json_object* v;
  return (json_object_object_get_ex(o, k, &v)) ? json_object_get_string(v) : def;
}
static int jint(struct json_object* o, const char* k, int def) {
  struct json_object* v;
  return (json_object_object_get_ex(o, k, &v)) ? json_object_get_int(v) : def;
}
static double jdbl(struct json_object* o, const char* k, double def) {
  struct json_object* v;
  return (json_object_object_get_ex(o, k, &v)) ? json_object_get_double(v) : def;
}

/* Remplit une cible depuis un objet JSON {idx, shm, stats, ident_file}. Renvoie 0 si shm valide. */
static int parse_target(struct json_object* j, struct target* t) {
  t->idx = jint(j, "idx", 0);
  snprintf(t->shm_path, sizeof(t->shm_path), "%s", jstr(j, "shm", ""));
  snprintf(t->stats_path, sizeof(t->stats_path), "%s", jstr(j, "stats", ""));
  const char* idf = jstr(j, "ident_file", "");
  if (idf && *idf) { snprintf(t->ident_file, sizeof(t->ident_file), "%s", idf); t->has_ident = 1; }
  /* Trame STATIQUE : une cible SANS shm mais AVEC un fichier de trame est VALIDE — c'est le slot
   * provisionné non câblé, qui émet son repli sans aucun producteur (cf. video_tx_*_thread). */
  snprintf(t->static_frame, sizeof(t->static_frame), "%s", jstr(j, "static_frame", ""));
  return (t->shm_path[0] || t->static_frame[0]) ? 0 : -1;
}

static void usage(const char* p) {
  fprintf(stderr,
    "usage: %s --config <json>   (DAEMON : sessions réconciliées à chaud, video st20 / audio st30)\n"
    "   ou : %s [args legacy 1 session video] (--pmd af_xdp --iface .. --mcast .. --shm .. ...)\n", p, p);
}

/* Remplit une session depuis un objet JSON. 0 si valide (mcast + port + ≥1 cible). */
static int parse_session_into(struct json_object* j, struct sess* s) {
  memset(s, 0, sizeof(*s));
  const char* kind = jstr(j,"kind","video");
  s->kind = !strcmp(kind,"audio") ? K_AUDIO : !strcmp(kind,"data") ? K_DATA : K_VIDEO;
  s->role = !strcmp(jstr(j,"role","rx"), "tx") ? ROLE_TX : ROLE_RX;
  snprintf(s->mcast,sizeof(s->mcast),"%s",jstr(j,"mcast",""));
  s->udp_port=jint(j,"udp_port",0); s->payload_type=jint(j,"payload_type",96);
  s->ssrc=(uint32_t)jint(j,"ssrc",0);   /* TX : 0=aléatoire (défaut libmtl), sinon fixe (borné 31 bits côté générateur) */
  s->ring=jint(j,"ring", s->kind==K_AUDIO?100:8); s->hdr=jint(j,"hdr",64);
  /* Plafond d'avance du worker TX tranche (0 = désactivé). Réglage et non constante : cette file
   * est aussi l'amortisseur d'un hoquet de la source — on descend par paliers en surveillant
   * `repeats`, pas d'un coup. Poussé par l'orchestrateur comme `ring`, sans recréation. */
  s->advance=jint(j,"advance", 0);
  s->publish_lead_us=jint(j,"publish_lead_us", 0);
  s->serve_newest=jint(j,"serve_newest", 0);
  /* multi-NIC : `iface` (leg primaire) → portname résolu (vide = iface inconnu, détecté au create).
   * `iface` absent → port 0 (mono-NIC). 2022-7 : `iface2`/`mcast2`/`udp_port2` = 2ᵉ leg (red/blue). */
  snprintf(s->iface, sizeof(s->iface), "%s", jstr(j,"iface",""));
  resolve_port(s->iface, s->portname);
  const char* ifn2 = jstr(j,"iface2","");
  const char* mc2  = jstr(j,"mcast2","");
  int up2 = jint(j,"udp_port2",0);
  if (ifn2[0] && mc2[0] && up2) {
    snprintf(s->iface_r, sizeof(s->iface_r), "%s", ifn2);
    snprintf(s->mcast_r, sizeof(s->mcast_r), "%s", mc2);
    s->udp_port_r = up2;
    resolve_port(ifn2, s->portname_r);
    s->num_leg = 2;
  } else {
    s->num_leg = 1;
  }
  /* 2022-7 : PAS de gel/recréation au lien mort — la session reste DUALE en permanence.
   * C'est libmtl (patch bobi.studio patch_afxdp_tx_link_drop) qui jette les paquets d'un
   * port au lien mort comme s'ils étaient émis → le leg vivant ne s'arrête JAMAIS (vrai
   * hitless), reprise silencieuse au link-up. Recréer la session ici couperait ~1 s le
   * leg sain (les ports d'une session MTL sont figés à la création). */
  if (s->kind == K_VIDEO) {
    s->width=jint(j,"width",1920); s->height=jint(j,"height",1080);
    s->fps=jdbl(j,"fps",25.0); s->interlaced=jint(j,"interlaced",0);
    s->tff = strcmp(jstr(j,"field_order","tff"), "bff") != 0;   /* défaut TFF ; "bff" → 0 */
    s->bit_depth=jint(j,"bit_depth",10);
    s->epoch_shift_us=jint(j,"epoch_shift_us",0);   /* TX : émission décalée (µs, 0=off, TROFF 2110-21) */
    if (s->epoch_shift_us < 0) s->epoch_shift_us = 0;
  } else if (s->kind == K_AUDIO) {
    s->channels=jint(j,"channels",8);
    s->a_ptime=jdbl(j,"ptime",1.0);   /* ms ; doit matcher le flux (a=ptime du SDP) */
  } else {   /* K_DATA / ANC : seul fps compte (pacing TX) */
    s->fps=jdbl(j,"fps",25.0);
  }
  struct json_object* tgs;
  if (json_object_object_get_ex(j,"targets",&tgs) && json_object_is_type(tgs,json_type_array)) {
    int nt = json_object_array_length(tgs);
    for (int ti = 0; ti < nt && s->ntg < MAX_TG; ti++) {
      int r = parse_target(json_object_array_get_idx(tgs, ti), &s->tg[s->ntg]);
      /* bobi.studio: un TX peut être PRÉ-PROVISIONNÉ sans source (slot silencieux : session + feuille
       * RL créées, thread muet en attente de câblage → 0 Gb/s). parse_target rejette une source vide ;
       * pour un TX on accepte quand même (la cible EST peuplée : shm_path="", stats, ident). La source
       * arrive ensuite par swap à chaud (tx_set_source) SANS re-créer la session. RX : source de sortie
       * requise → on garde le rejet. */
      if (r == 0 || s->role == ROLE_TX) s->ntg++;
    }
  } else {
    if (parse_target(j, &s->tg[0]) == 0) s->ntg = 1;   /* compat : shm/stats/ident_file inline */
  }
  return (s->mcast[0] && s->udp_port && s->ntg > 0) ? 0 : -1;
}

/* Signature = identité réseau + format + cibles. Un sig différent ⇒ on libère l'ancienne session et
 * on en recrée une (flow RX recyclé, device/XDP intacts ⇒ pas de faute PTP). */
static void compute_sig(struct sess* s) {
  int n = snprintf(s->sig, sizeof(s->sig),
                   /* `av` = bridage d'avance. Il DOIT être dans la signature : sans lui le moteur
                    * reconnaît la session comme identique, ne la recrée pas, et `s->advance` garde
                    * la valeur lue à la création — le réglage part de l'orchestrateur, traverse le
                    * contrôleur, et n'a AUCUN effet, `adv_wait_ms` restant à 0,0 quoi qu'on demande.
                    * Constaté le 2026-08-12 après deux rebuilds passés à chercher ailleurs. */
                   "%d|%d|%s|%d|%d|%u|%dx%d|%.2f|i%d|f%d|bd%d|r%d|ch%d|ap%.3f|es%d|av%d|pl%d|sn%d|if%s|if2%s|mc2%s|p2%d|",
                   s->role, s->kind, s->mcast, s->udp_port, s->payload_type, s->ssrc,
                   s->width, s->height, s->fps, s->interlaced, s->tff, s->bit_depth, s->ring,
                   s->channels, s->a_ptime, s->epoch_shift_us, s->advance, s->publish_lead_us, s->serve_newest, s->iface, s->iface_r,
                   s->mcast_r, s->udp_port_r);
  /* bobi.studio: pour un TX la SOURCE (tg[].shm_path) n'entre PAS dans l'identité de session → la
   * changer ne recrée plus la session (swap à chaud via tx_set_source/tx_take_source, pas de commit
   * RL / dé-lock PTP). Pour un RX les flux de SORTIE font partie de l'identité → on les garde. */
  if (s->role != ROLE_TX)
    for (int ti = 0; ti < s->ntg && n > 0 && n < (int)sizeof(s->sig); ti++)
      n += snprintf(s->sig + n, sizeof(s->sig) - n, "%s>%s,",
                    s->tg[ti].shm_path, s->tg[ti].has_ident ? s->tg[ti].ident_file : "-");
}

/* Libère une session vivante : arrêt PROPRE du thread, free du handle MTL (le flow RX), munmap des
 * cibles. Le device (mtl_init/XDP) n'est PAS touché ⇒ aucune faute PTP. */
static void free_session(struct sess* s) {
  if (!s->used) return;
  if (s->started) { s->stop = 1; pthread_join(s->thread, NULL); }
  if (s->role == ROLE_TX) {
    if (s->kind == K_AUDIO)     { if (s->a_tx) st30p_tx_free(s->a_tx); }
    else if (s->kind == K_DATA) { if (s->d_tx) st40p_tx_free(s->d_tx); }
    else                        { if (s->vth)  st20p_tx_free(s->vth);
                                  if (s->sl_tx) st20_tx_free(s->sl_tx); }   /* mode tranche */
  } else if (s->kind == K_AUDIO) { if (s->ah)  st30p_rx_free(s->ah); }
  else if (s->kind == K_DATA)    { if (s->d_rx) st40p_rx_free(s->d_rx); }
  else                           { if (s->vh)  st20p_rx_free(s->vh);
                                   if (s->sl_rx) st20_rx_free(s->sl_rx); }  /* mode tranche */
  for (int ti = 0; ti < s->ntg; ti++) {
    struct target* t = &s->tg[ti];
    if (t->writer) mxlReleaseFlowWriter(g_mxl, t->writer);   /* RX/simu */
    if (t->reader) mxlReleaseFlowReader(g_mxl, t->reader);   /* TX */
    if (t->ident_patch) free(t->ident_patch);
    if (t->sf_buf) free(t->sf_buf);                          /* trame statique */
  }
  if (s->tx_scratch) free(s->tx_scratch);
  if (s->rx_scratch) free(s->rx_scratch);
  if (s->tx_frame)   free(s->tx_frame);
  if (s->sl_scratch) free(s->sl_scratch);
  if (s->slice_on) { pthread_mutex_destroy(&s->sl_mx); pthread_cond_destroy(&s->sl_cv); }
  memset(s, 0, sizeof(*s));
}

/* Écrit un statut d'ERREUR dans le stats json d'une cible (création de session ratée). Le contrôleur
 * le relaie en `rx_error` sur :8080 → l'orchestrateur affiche « abonné mais ne reçoit pas » avec la
 * cause précise (typiquement budget lcores du nœud dépassé : st20p_rx_create → no available lcore). */
static void write_stats_error(struct target* t, const char* err) {
  if (!t->stats_path[0]) return;
  FILE* sf = fopen(t->stats_path, "w");
  if (sf) { fprintf(sf, "{\"fps\": 0.0, \"frame_index\": 0, \"error\": \"%s\"}\n", err); fclose(sf); }
}

/* Réconcilie le registre des sessions vivantes avec le config (sessions désirées), À CHAUD sur le
 * mtl_handle vivant : libère les disparues, crée les nouvelles. JAMAIS de mtl_uninit. */
static void reconcile(struct sess* reg, const char* path, mtl_handle st, const char* portname) {
  struct json_object* root = json_object_from_file(path);
  if (!root) return;
  struct json_object* arr;
  if (!json_object_object_get_ex(root,"sessions",&arr) || !json_object_is_type(arr,json_type_array)) {
    json_object_put(root); return;
  }
  for (int i = 0; i < MAX_SESS; i++) reg[i].seen = 0;
  int n = json_object_array_length(arr);

  /* Pass 1 — MATCH : marquer « seen » les sessions désirées DÉJÀ présentes (sig identique).
   * On ne crée RIEN ici (une session inchangée — ex. la vidéo — n'est pas touchée → pas de faute PTP). */
  for (int k = 0; k < n; k++) {
    struct sess want;
    if (parse_session_into(json_object_array_get_idx(arr, k), &want) != 0) continue;
    compute_sig(&want);
    for (int i = 0; i < MAX_SESS; i++)
      if (reg[i].used && !reg[i].seen && !strcmp(reg[i].sig, want.sig)) {
        reg[i].seen = 1;
        /* bobi.studio: TX — sig identique même si la SOURCE diffère (source hors-sig). On PROPAGE la
         * nouvelle source à la session VIVANTE (le thread rouvre son reader) au lieu de détruire+
         * recréer → aucun st20p_tx_create, aucun commit RL, aucun dé-lock PTP de la flotte. */
        if (reg[i].role == ROLE_TX && reg[i].ntg > 0 && want.ntg > 0) {
          /* Trame STATIQUE propagée elle aussi à chaud, et AVANT la source : un slot qu'on DÉCÂBLE
           * (shm → "") doit trouver son repli déjà en place, sinon il deviendrait muet le temps
           * d'un redéploiement. La cible ne la consulte que lorsque shm_path est vide, donc après
           * que tx_take_source ait publié la nouvelle source — l'ordre d'écriture ici suffit. */
          if (strcmp(reg[i].tg[0].static_frame, want.tg[0].static_frame) != 0) {
            snprintf(reg[i].tg[0].static_frame, sizeof(reg[i].tg[0].static_frame), "%s",
                     want.tg[0].static_frame);
            reg[i].tg[0].sf_mtime = 0;      /* chemin changé → forcer un rechargement complet */
          }
          if (strcmp(reg[i].tg[0].want_shm, want.tg[0].shm_path) != 0)
            tx_set_source(&reg[i].tg[0], want.tg[0].shm_path);
        }
        break;
      }
  }

  /* Pass 2 — FREE-BEFORE-CREATE : libérer les sessions périmées AVANT toute création. Libère leur
   * file/flow AF_XDP (mt_rx_xdp_put) ; sinon recréer un flux pour une source CHANGÉE échoue
   * (« create flow fail -5 ») tant que l'ancienne session tient ses ressources → on se retrouvait
   * sans aucune session (cas changement de source audio). */
  for (int i = 0; i < MAX_SESS; i++)
    if (reg[i].used && !reg[i].seen) {
      fprintf(stderr,"mtl_rx: retrait session %s:%d\n", reg[i].mcast, reg[i].udp_port);
      free_session(&reg[i]);
    }

  /* Pass 3 — CREATE : monter les sessions désirées encore absentes (file/flow maintenant libres). */
  for (int k = 0; k < n; k++) {
    struct sess want;
    if (parse_session_into(json_object_array_get_idx(arr, k), &want) != 0) continue;
    compute_sig(&want);
    int found = -1;
    for (int i = 0; i < MAX_SESS; i++)
      if (reg[i].used && !strcmp(reg[i].sig, want.sig)) { found = i; break; }
    if (found >= 0) continue;                                /* déjà présente (gardée en Pass 1) */
    int slot = -1;
    for (int i = 0; i < MAX_SESS; i++) if (!reg[i].used) { slot = i; break; }
    if (slot < 0) { fprintf(stderr,"mtl_rx: registre plein, session ignorée\n"); continue; }
    struct sess* s = &reg[slot];
    *s = want;
    s->st = st; s->stop = 0;
    /* bobi.studio: amorcer le miroir de source (want_shm) = source initiale → reconcile compare
     * `want_shm` (possédé par reconcile) et ne déclenche un swap que sur un VRAI changement. */
    for (int ti = 0; ti < s->ntg; ti++) {
      snprintf(s->tg[ti].want_shm, sizeof(s->tg[ti].want_shm), "%s", s->tg[ti].shm_path);
      s->tg[ti].src_seq = 0; s->tg[ti].src_seq_seen = 0;
    }
    /* `iface` absent → portname déjà résolu sur le port 0 par parse. Repli mono-NIC ultime
     * (g_ports vide) : le portname global du device. */
    if (!s->portname[0] && !s->iface[0]) snprintf(s->portname, sizeof(s->portname), "%s", portname);
    /* iface DEMANDÉE mais inconnue du device → pas de session, erreur explicite sur le slot. */
    if (!s->portname[0]) {
      fprintf(stderr,"mtl_rx: session %s:%d sur iface inconnue '%s' — ignorée\n",
              s->mcast, s->udp_port, s->iface);
      if (s->role != ROLE_TX)
        for (int ti = 0; ti < s->ntg; ti++) write_stats_error(&s->tg[ti], "unknown_iface");
      memset(s,0,sizeof(*s));
      continue;
    }
    /* 2022-7 : leg redondant sur iface inconnue → dégrade en mono-leg (le primaire reste valide). */
    if (s->num_leg == 2 && !s->portname_r[0]) {
      fprintf(stderr,"mtl_rx: leg redondant %s:%d sur iface inconnue '%s' — mono-leg\n",
              s->mcast_r, s->udp_port_r, s->iface_r);
      s->num_leg = 1;
    }
    int r = (s->role == ROLE_TX)
            ? ((s->kind == K_AUDIO) ? setup_audio_tx(s) : (s->kind == K_DATA) ? setup_data_tx(s) : setup_video_tx(s))
            : ((s->kind == K_AUDIO) ? setup_audio(s)    : (s->kind == K_DATA) ? setup_data(s)    : setup_video(s));
    if (r == 0) { s->used = 1; s->seen = 1;
      /* bobi.studio: un create TX vient de committer l'arbre RL (port stoppé le temps du commit) →
       * armer la grâce backstop pour ne pas confondre le port-off avec un wedge des autres sessions. */
      if (s->role == ROLE_TX) { uint64_t g = mono_ns() + TX_ADD_GRACE_NS;
                                if (g > g_tx_add_grace_ns) g_tx_add_grace_ns = g; }
    }
    else {
      fprintf(stderr,"mtl_rx: création session %s:%d échouée\n", s->mcast, s->udp_port);
      /* RX : remonter l'échec dans le stats json du slot (sinon le contrôleur le croit « mtl »). */
      if (s->role != ROLE_TX)
        for (int ti = 0; ti < s->ntg; ti++) write_stats_error(&s->tg[ti], "rx_create_failed");
      memset(s,0,sizeof(*s));
    }
  }
  json_object_put(root);
}

/* Écrit le fichier de stats {fps, frame_index} de chaque cible vivante. `last_repeats` suit le
 * MÊME mécanisme que `last` (fenêtre glissante de `recv`) mais pour `repeats` — nécessaire pour
 * dériver fps_source = (Δrecv − Δrepeats)/dt sur la fenêtre, PAS depuis le cumul depuis le boot. */
static void write_stats(struct sess* reg, uint64_t last[][MAX_TG], uint64_t last_repeats[][MAX_TG],
                         double dt) {
  static const char* TP_VERDICT[] = {"failed", "wide", "narrow"};
  for (int i = 0; i < MAX_SESS; i++) {
    struct sess* s = &reg[i];
    if (!s->used || !s->started) continue;
    /* SONDE 2110-21 : fragment conformité (compliant/cause/cinst/vrx/fpt/latency) calculé UNE fois
     * par session (le tp_meta est une propriété du flux, commun à toutes les cibles fan-out), puis
     * concaténé à chaque stats-file. Lecture+reset de la fenêtre ici (lockless, cf. accum_tp). */
    char tpbuf[320]; tpbuf[0] = '\0';
    if (s->tp_enabled && s->tp_cnt > 0) {
      int w = s->tp_worst >= 0 && s->tp_worst < 3 ? s->tp_worst : 0;
      char cbuf[80]; cbuf[0] = '\0';
      if (w != 2 && s->tp_cause[0]) snprintf(cbuf, sizeof(cbuf), ", \"failed_cause\": \"%s\"", s->tp_cause);
      snprintf(tpbuf, sizeof(tpbuf),
               ", \"compliant\": \"%s\"%s, \"cinst_max\": %d, \"cinst_avg\": %.1f, "
               "\"vrx_max\": %d, \"vrx_min\": %d, \"vrx_avg\": %.1f, \"vrx_span\": %d, "
               "\"fpt\": %d, \"latency\": %d",
               TP_VERDICT[w], cbuf, s->tp_cinst_max, s->tp_cinst_sum / s->tp_cnt,
               s->tp_vrx_max, s->tp_vrx_min, s->tp_vrx_sum / s->tp_cnt, s->tp_vrx_span_max,
               s->tp_fpt, s->tp_latency);
    }
    /* TRANSPORT PAR SESSION : lecture + reset de la fenêtre, même modèle que le bloc tp.
     * ⚠ PUBLIÉ MÊME À ZÉRO quand des trames ont été comptées. Un compteur de pertes ABSENT et
     * un compteur de pertes À ZÉRO ne disent pas la même chose : le premier veut dire « on ne
     * mesure pas », le second « on mesure, et il n'y a rien ». C'est exactement la distinction
     * que la tuile d'ingénierie du scope affiche, et elle serait perdue si on omettait le
     * fragment quand tout va bien. */
    char pxbuf[220]; pxbuf[0] = '\0';
    if (s->px_frames > 0) {
      int64_t manque = (int64_t)s->px_pkts - (int64_t)s->px_recv_p;
      if (manque < 0) manque = 0;
      snprintf(pxbuf, sizeof(pxbuf),
               ", \"pkts\": %llu, \"pkts_perdus\": %lld, \"pkts_secours\": %llu, "
               "\"trames_incompletes\": %u, \"pire_manque\": %u, \"trames_mesurees\": %u",
               (unsigned long long)s->px_pkts, (long long)manque,
               (unsigned long long)s->px_recv_r, s->px_incomplete, s->px_pire, s->px_frames);
      s->px_pkts = s->px_recv_p = s->px_recv_r = 0;
      s->px_incomplete = s->px_frames = s->px_pire = 0;
    }
    for (int ti = 0; ti < s->ntg; ti++) {
      struct target* t = &s->tg[ti];
      if (!t->stats_path[0]) continue;
      /* ★ Δ CAPTURÉ AVANT de recaler `last` : le calcul de fps_source plus bas a besoin du MÊME
       * Δrecv que `rate`. Le lire après l'affectation donnerait 0 à tous les coups (last == recv),
       * donc fps_source = 0 en permanence — une sortie parfaitement alimentée serait affichée
       * « source à 0 image/s ». Une métrique fausse est pire que pas de métrique. */
      /* ⚠ LE REJEU N'EST PAS COMPTABILISÉ ICI — et c'est délibéré. Les trames de tenue partent bien
       * sur le fil, mais libmtl n'offre AUCUN signal fiable pour les compter : elle sollicite
       * get_next_frame sans toujours émettre (compter là ⇒ 65 fps pour un fil à 50) et ne rappelle
       * pas notify_frame_done sur un re-service (compter là ⇒ rien compté). Un bornage temporel à
       * une période majore encore (66/s, le plafond de la borne). Les trois essais sont faux.
       * `fps` publie donc ce qu'on MESURE VRAIMENT : les trames neuves produites par le worker.
       * Elle SOUS-ESTIME la cadence du fil quand la source est déficitaire — la cadence réelle est
       * garantie nominale par l'horloge de sortie et se vérifie aux compteurs du port. Publier une
       * valeur non mesurée serait retomber dans le travers qui a coûté la nuit du 27 au 28. */
      uint64_t emis = t->recv;
      uint64_t reps = t->repeats;
      uint64_t d_recv = emis - last[i][ti];
      double rate = dt > 0 ? (double)d_recv / dt : 0.0;
      last[i][ti] = emis;
      /* Latence de réception (segment A) : moyenne de la fenêtre, en ms. -1 = pas d'échantillon
       * (TX, ou flux média sans timestamp) → sérialisé `null`. Reset du cumul à chaque fenêtre. */
      double rx_lat = t->lat_cnt ? (double)t->lat_sum / (double)t->lat_cnt / 1e6 : -1.0;
      t->lat_sum = 0; t->lat_cnt = 0;
      /* bobi.studio: RÉPÉTITION DE TRAME VISIBLE (TX vidéo seulement) — fps_source = trames
       * UNIQUES/s sur la fenêtre (Δrecv − Δrepeats)/dt, `repeats` = cumul depuis le démarrage. Même
       * mécanique fenêtre glissante que `rate` ci-dessus, sur `last_repeats`. Buffer préparé même
       * hors-vidéo/TX pour rester simple ; non émis (champs absents) en dehors du cas visé. */
      int is_video_tx = (s->kind == K_VIDEO && s->role == ROLE_TX);
      double fps_source = 0.0;
      if (is_video_tx) {
        uint64_t d_rep  = reps - last_repeats[i][ti];
        d_rep = d_rep > d_recv ? d_recv : d_rep;      /* garde-fou : jamais plus de reprises que de trames */
        fps_source = dt > 0 ? (double)(d_recv - d_rep) / dt : 0.0;
        last_repeats[i][ti] = reps;
      }
      /* fps_source/repeats : UNIQUEMENT cibles TX vidéo (cf. is_video_tx ci-dessus) — toute autre
       * cible (RX, audio, ANC) n'émet PAS ces champs. */
      char repbuf[64]; repbuf[0] = '\0';
      if (is_video_tx)
        snprintf(repbuf, sizeof(repbuf), ", \"fps_source\": %.1f, \"repeats\": %llu",
                 fps_source, (unsigned long long)reps);
      /* bobi.studio: ÉTAT tenue/source (PAS un comptage — cf. tx_sl_next_frame, libmtl ne signale
       * fiablement ni le rejeu ni son absence). UNIQUEMENT les cibles TX vidéo EN MODE TRANCHE
       * (s->slice_on) : c'est la seule où la trame de tenue existe (setup_video_slice_tx / le
       * resync watchdog la préserve désormais). Un slot SANS câble (mode statique, cf. `statique`
       * dans video_tx_slice_thread) n'est PAS « holding » : il n'a jamais eu de producteur à perdre,
       * c'est son fonctionnement nominal — traité à part, jamais via last_fresh_ns (qui ne concerne
       * que le chemin avec reader). */
      /* 384 : le bloc `ring` (ci-dessous) s'ajoute à `srv` ; une troncature de snprintf produirait
       * un JSON invalide que le contrôleur rejetterait EN SILENCE — la sortie paraîtrait alors sans
       * métriques plutôt qu'en panne. Marge volontaire. */
      /* 448 octets ne suffisaient plus : l'ajout des compteurs de segment (src_age, lead,
       * wait_pub, emit) tronquait la fin du JSON — `snprintf` coupe SANS ERREUR, et le
       * fichier devenait illisible pour un lecteur strict. Un tampon trop court ne se voit
       * pas dans les logs : il se voit en aval, quand un champ manque sans raison. */
      char livebuf[1024]; livebuf[0] = '\0';
      char ringbuf[192]; ringbuf[0] = '\0';
      if (is_video_tx && s->slice_on) {
        int statique = (!t->shm_path[0] && t->static_frame[0]);
        int source_live, holding;
        if (statique) {
          source_live = 1; holding = 0;
        } else {
          uint64_t now_ns = mono_ns();
          source_live = s->sl_period_ns > 0 &&
                        (now_ns - t->last_fresh_ns) < 3 * s->sl_period_ns;
          holding = !source_live;
        }
        /* ÉTAT DE L'ANNEAU — ajouté 2026-07-29 pour instrumenter le blocage du chemin par tranches
         * (un slot CÂBLÉ meurt en ~40 s, un slot statique jamais ; mesuré au banc). Le journal du
         * watchdog dit « 4/4 slots occupés » sans dire QUI les occupe : `stat` par slot (0=libre,
         * 1=prêt/en vol, 2=tenue), `lines_ready` par slot, et les deux curseurs. Lecture seule de
         * champs volatile déjà partagés entre le worker et le tasklet — aucune synchronisation
         * ajoutée, aucune décision prise ici. */
        char slots[64] = ""; char lignes[80] = "";
        int nfb = (s->sl_fb_cnt > 0 && s->sl_fb_cnt <= SL_Q) ? s->sl_fb_cnt : SL_Q;
        for (int k = 0; k < nfb; k++) {
          char frag[20];
          snprintf(frag, sizeof(frag), "%s%d", k ? "," : "", s->sl_fb[k].stat);
          strncat(slots, frag, sizeof(slots) - strlen(slots) - 1);
          snprintf(frag, sizeof(frag), "%s%u", k ? "," : "", s->sl_fb[k].lines_ready);
          strncat(lignes, frag, sizeof(lignes) - strlen(lignes) - 1);
        }
        snprintf(ringbuf, sizeof(ringbuf),
                 ", \"ring\": {\"prod\": %u, \"cons\": %u, \"hold\": %d, \"stat\": [%s]"
                 ", \"lines\": [%s]}",
                 (unsigned)s->sl_fb_prod, (unsigned)s->sl_fb_cons,
                 s->sl_hold_valid ? (int)s->sl_hold_idx : -1, slots, lignes);

        /* + DIAGNOSTIC du service de trames : ce que le callback a rendu, par branche, et l'état
         * d'amorçage de la tenue. C'est ce qui permet de dire, quand une sortie se tait, si la lib
         * sollicite encore le callback et quelle branche il prend — au lieu de le deviner. Le TÉMOIN
         * DE REPLI n'a plus de réserve dédiée à publier ici : de ce point de vue il n'est qu'une
         * trame FRAÎCHE de plus (srv.fresh), publiée par le worker (cf. `morte`). */
        snprintf(livebuf, sizeof(livebuf),
                 ", \"source_live\": %s, \"holding\": %s, \"hold_ok\": %s"
                 ", \"srv\": {\"fresh\": %llu, \"hold\": %llu, \"busy\": %llu}%s, \"hold_empty_kept\": %llu"
                 ", \"slot_wait_ms\": %.1f, \"slot_wait_n\": %llu"
                 ", \"advance\": %d, \"adv_wait_ms\": %.1f, \"adv_wait_n\": %llu"
                 ", \"publish_lead_us\": %d, \"serve_newest\": %d, \"skipped\": %llu"
                 ", \"src_age_ms\": %.1f, \"src_age_max_ms\": %.1f"
                 ", \"lead_ms\": %.1f, \"lead_max_ms\": %.1f, \"lead_n\": %llu"
                 ", \"wait_pub_ms\": %.2f, \"wait_pub_max_ms\": %.2f, \"emit_ms\": %.2f"
                 ", \"getgrain_ms\": %.1f, \"getgrain_n\": %llu"
                 ", \"pack_ms\": %.1f, \"pack_n\": %llu, \"fb_slots\": %u"
                 ", \"drained\": %llu, \"depth_avg\": %.2f, \"depth_max\": %u, \"dist_avg\": %.2f, \"dist_max\": %u",
                 source_live ? "true" : "false", holding ? "true" : "false",
                 s->sl_hold_valid ? "true" : "false",
                 (unsigned long long)s->srv_fresh, (unsigned long long)s->srv_hold,
                 (unsigned long long)s->srv_busy, ringbuf,
                 (unsigned long long)s->sl_hold_empty_kept,
                 /* Temps passé bloqué à attendre un slot d'anneau pendant la fenêtre de stats.
                  * Rapporté à la durée de la fenêtre, ça donne directement la part du temps où le
                  * worker est étranglé par l'aval plutôt que par sa source. */
                 t->slot_wait_cum_ns / 1e6, (unsigned long long)t->slot_wait_cnt,
                 /* Bridage d'avance : le plafond en vigueur, et le temps qu'il a coûté sur la
                  * fenêtre. À lire AVEC `slot_wait_ms` : l'un dit qu'on se retient, l'autre qu'on
                  * est retenu. Les confondre ferait passer un bridage pour un étranglement. */
                 s->advance, t->adv_wait_cum_ns / 1e6, (unsigned long long)t->adv_wait_cnt,
                 s->publish_lead_us, s->serve_newest, (unsigned long long)s->sl_skipped,
                 /* Les trois postes du tour de worker, sur la MÊME fenêtre : attente d'un slot,
                  * attente du grain source, packing. Rapportés à la fenêtre (2 s), ils disent
                  * lequel étrangle la cadence — la question à laquelle aucun compteur ne répondait. */
                 /* Âge du contenu AU MOMENT où le TX le saisit. Dit si les ~60 ms du retour
                  * 2110 sont déjà là à l'entrée du TX, ou naissent en aval. */
                 s->sl_srcage_cnt ? (double)s->sl_srcage_ns / s->sl_srcage_cnt / 1e6 : 0.0,
                 s->sl_srcage_max / 1e6,
                 /* Avance d'ordonnancement de la lib : combien de temps la trame attend son
                  * époque APRÈS qu'on la lui a remise. Dernier segment non mesuré. */
                 s->sl_lead_cnt ? (double)s->sl_lead_ns / s->sl_lead_cnt / 1e6 : 0.0,
                 s->sl_lead_max / 1e6, (unsigned long long)s->sl_lead_cnt,
                 /* Le segment qui restait aveugle : attente d'une trame PRÊTE avant que la lib
                  * s'en saisisse, puis durée de son émission. */
                 s->sl_wait_pub_cnt ? (double)s->sl_wait_pub_ns / s->sl_wait_pub_cnt / 1e6 : 0.0,
                 s->sl_wait_pub_max / 1e6,
                 s->sl_emit_cnt ? (double)s->sl_emit_ns / s->sl_emit_cnt / 1e6 : 0.0,
                 s->sl_getgrain_ns / 1e6, (unsigned long long)s->sl_getgrain_cnt,
                 s->sl_pack_ns / 1e6, (unsigned long long)s->sl_pack_cnt,
                 (unsigned)s->sl_fb_cnt, (unsigned long long)s->sl_drained,
                 s->sl_depth_cnt ? (double)s->sl_depth_sum / s->sl_depth_cnt : 0.0,
                 (unsigned)s->sl_depth_max,
                 s->sl_depth_cnt ? (double)s->sl_dist_sum / s->sl_depth_cnt : 0.0,
                 (unsigned)s->sl_dist_max);
      t->slot_wait_cum_ns = 0; t->slot_wait_cnt = 0;
      t->adv_wait_cum_ns = 0; t->adv_wait_cnt = 0;
      s->sl_getgrain_ns = 0; s->sl_getgrain_cnt = 0;
      s->sl_srcage_ns = 0; s->sl_srcage_cnt = 0; s->sl_srcage_max = 0;
      s->sl_lead_ns = 0; s->sl_lead_cnt = 0; s->sl_lead_max = 0;
      s->sl_wait_pub_ns = 0; s->sl_wait_pub_cnt = 0; s->sl_wait_pub_max = 0;
      s->sl_emit_ns = 0; s->sl_emit_cnt = 0;
      s->sl_pack_ns = 0; s->sl_pack_cnt = 0;
      s->sl_depth_sum = 0; s->sl_depth_cnt = 0; s->sl_depth_max = 0;
      s->sl_dist_sum = 0; s->sl_dist_max = 0;
      }
      FILE* sf = fopen(t->stats_path, "w");
      if (sf) {
        char latbuf[32];
        if (rx_lat >= 0.0) snprintf(latbuf, sizeof(latbuf), "%.1f", rx_lat);
        else               snprintf(latbuf, sizeof(latbuf), "null");
        if (s->kind == K_DATA && t->tc_valid)
          fprintf(sf, "{\"fps\": %.1f, \"frame_index\": %llu, \"timecode\": \"%s\", \"df\": %s, \"rx_latency_ms\": %s%s%s%s%s}\n",
                  rate, (unsigned long long)t->index, t->tc, t->tc_df ? "true" : "false", latbuf, tpbuf, pxbuf, repbuf, livebuf);
        else
          fprintf(sf, "{\"fps\": %.1f, \"frame_index\": %llu, \"late\": %llu, \"rx_latency_ms\": %s%s%s%s%s}\n",
                  rate, (unsigned long long)t->index, (unsigned long long)t->late, latbuf, tpbuf, pxbuf, repbuf, livebuf);
        fclose(sf);
      }
    }
    /* Reset de la fenêtre tp après avoir servi toutes les cibles (le pire verdict repart à « aucun
     * échantillon » → chaque fenêtre reflète l'état courant, pas un pire historique collant). */
    if (s->tp_enabled) { s->tp_cnt = 0; s->tp_worst = -1; s->tp_cause[0] = '\0'; s->tp_cinst_sum = s->tp_vrx_sum = 0.0; }
  }
}

/* Stats I/O PAR PORT (NIC) — contrat /tmp/mtl_ports.json (cf. DPDK_NARROW.md « Contrats de la
 * nuit ») : remplace `ethtool -S` côté contrôleur quand un port est en PMD DPDK (l'iface kernel
 * a disparu en vfio). Source = mtl_get_port_stats() (mtl_api.h) : compteurs CUMULÉS depuis
 * mtl_init (struct mtl_port_status). Écrit pour TOUS les ports (af_xdp compris — le contrôleur
 * n'y bascule ses débits que pour pmd=dpdk). Écriture atomique (tmp + rename) : le contrôleur
 * peut lire à tout instant sans lire un JSON tronqué. Cadence = celle de write_stats (~2 s).
 *
 * `ts` = HORODATAGE DU SNAPSHOT, en **secondes flottantes de l'horloge MONOTONE** (0.59.0).
 * C'est le dénominateur du calcul de débit du contrôleur (Δoctets/Δts, cf. controller.py
 * `_nic_bps_mtl`), donc deux exigences :
 *   - **monotone** (CLOCK_MONOTONIC, PAS now_ns/CLOCK_REALTIME) : un saut NTP/PTP de l'horloge
 *     système fausserait — voire ferait reculer — le Δt d'un débit ;
 *   - **sous-seconde** : jusqu'en 0.58.0 on publiait now_ns()/1e9 en entier, donc un ts quantifié
 *     à ±1 s sur une fenêtre de ~2 s = jusqu'à ±50 % d'erreur sur le débit (d'où le lissage EMA
 *     de contournement, retiré en 0.59.0). %.6f (µs) : largement sous le bruit de la fenêtre.
 * L'unité (secondes) est inchangée — seule l'ÉPOQUE change (uptime, plus l'epoch UNIX) ; aucun
 * consommateur ne lit ce champ comme une date murale (seul `_nic_bps_mtl` en fait des deltas). */
static void write_port_stats(mtl_handle st) {
  if (g_nports <= 0) return;
  const char* tmp = "/tmp/mtl_ports.json.tmp";
  FILE* f = fopen(tmp, "w");
  if (!f) return;
  fprintf(f, "{\"ts\": %.6f, \"ports\": [",
          (double)mono_ns() / 1e9);
  for (int k = 0; k < g_nports; k++) {
    struct mtl_port_status ps; memset(&ps, 0, sizeof(ps));
    if (mtl_get_port_stats(st, (enum mtl_port)k, &ps) != 0)
      memset(&ps, 0, sizeof(ps));          /* échec → champs à 0 (contrat : absents = 0) */
    fprintf(f,
      "%s{\"port\": \"%s\", \"pmd\": \"%s\", "
      "\"rx_packets\": %llu, \"tx_packets\": %llu, "
      "\"rx_bytes\": %llu, \"tx_bytes\": %llu, "
      "\"rx_err\": %llu, \"tx_err\": %llu, "
      "\"rx_hw_dropped\": %llu, \"rx_nombuf\": %llu}",
      k ? ", " : "",
      g_ports[k].iface[0] ? g_ports[k].iface : g_ports[k].portname,
      g_ports[k].pmd[0] ? g_ports[k].pmd : "af_xdp",
      (unsigned long long)ps.rx_packets,            (unsigned long long)ps.tx_packets,
      (unsigned long long)ps.rx_bytes,              (unsigned long long)ps.tx_bytes,
      (unsigned long long)ps.rx_err_packets,        (unsigned long long)ps.tx_err_packets,
      (unsigned long long)ps.rx_hw_dropped_packets, (unsigned long long)ps.rx_nombuf_packets);
  }
  fprintf(f, "]");
  /* bobi.studio: état PTP interne libmtl (port 0) — socle DPDK (ENGINE_PTP=libmtl). Publié dans
   * mtl_ports.json pour DEUX consommateurs orchestrateur : (1) a=ts-refclk:ptp du SDP TX quand
   * ptp4l kernel est absent (gm_identity/domain, cf. app/ptp.refclk_for_node) ; (2) l'onglet
   * « Réseau 2110 - PTP » — verrouillage servo + offset (ns)/dérive + grandmaster (:8080 payload.ptp
   * relayé par controller.py). Bloc ÉMIS seulement quand le PTP interne tourne (g_engine_ptp) :
   * sinon absent → rétro-compat af_xdp (l'orchestrateur retombe sur pmc/ptp4l kernel, l'AF-XDP
   * n'est pas cassé). locked = lock SERVO RÉEL (mt_bobi_ptp_stable = max delta < 100 ns en continu),
   * conjugué à un GM connu (mt_bobi_ptp_gm : Announce reçu) — évite le piège « !active ⇒ stable=true »
   * du getter (dédié au backstop TX) et « le grandmaster que le PTP interne a VERROUILLÉ » côté SDP.
   * offset_ns = max delta de la fenêtre courante (null tant qu'aucune mesure) — trace la convergence
   * MÊME avant le lock. gm_identity au format SDP (XX-XX-…-XX), "" si Announce pas encore reçu. */
  if (g_engine_ptp) {
    unsigned char gm[8]; int dom = 0, utc = 0;
    long long raw = 0, corr = 0, pd = 0;
    int have_gm   = mt_bobi_ptp_gm(st, 0, gm, &dom, &utc) ? 1 : 0;
    int have_raw  = mt_bobi_ptp_offset(st, 0, &raw) ? 1 : 0;         /* delta brut (diag, pilote locked) */
    int have_corr = mt_bobi_ptp_correct_offset(st, 0, &corr) ? 1 : 0;/* offset corrigé (regime logiciel) */
    long long lastd = 0;
    int have_last = mt_bobi_ptp_last_delta(st, 0, &lastd) ? 1 : 0;   /* offset from master, signe */
    int have_pd   = mt_bobi_ptp_path_delay(st, 0, &pd) ? 1 : 0;      /* mean path delay */
    /* locked = lock servo STRICT de libmtl (delta brut < 100 ns en continu). ⚠ Il ne se déclenchait
     * JAMAIS tant que l'asservissement en FRÉQUENCE du PHC n'était pas compilé — on l'a longtemps
     * écrit ici comme une fatalité de l'E810, c'était un défaut : cf. patch_ptp_adjust_freq, depuis
     * lequel le verrou s'arme en permanence et le delta brut tient sous 100 ns. synced = synchro
     * RÉELLE au GM (GM connu + offset dispo) : il reste le flag qui pilote le badge, parce qu'un
     * moteur sur image ANTÉRIEURE au correctif ne verrouille toujours pas. */
    int locked = (have_gm && mt_bobi_ptp_stable(st, 0)) ? 1 : 0;
    int synced = (have_gm && have_corr) ? 1 : 0;
    fprintf(f, ", \"ptp\": {\"engine\": true, \"locked\": %s, \"synced\": %s, \"domain\": %d, \"utc_offset\": %d",
            locked ? "true" : "false", synced ? "true" : "false", dom, utc);
    /* offset_ns = « offset from master » À AFFICHER. ⚠ SA SOURCE DÉPEND DU RÉGIME, et c'est mesuré :
     * depuis que le PHC est asservi en FRÉQUENCE (patch_ptp_adjust_freq), l'offset corrigé en
     * logiciel sort des valeurs absurdes — ±7e16 ns sur 64 relevés sur 70 au banc du 2026-08-30 —
     * parce qu'il compensait précisément l'absence de discipline matérielle et que son ancre
     * `last_sync_ts` n'est plus rafraîchie dans ce régime. Le delta PHC↔GM SIGNÉ devient alors la
     * mesure juste (9 à 99 ns verrou armé). On préfère donc le delta signé dès qu'il est
     * disponible, et on garde le corrigé en repli pour une image bâtie SANS le patch de fréquence.
     * raw_delta_ns reste le MAX de la fenêtre — un diagnostic, pas un offset. */
    if      (have_last) fprintf(f, ", \"offset_ns\": %lld", lastd);
    else if (have_corr) fprintf(f, ", \"offset_ns\": %lld", corr);
    else                fprintf(f, ", \"offset_ns\": null");
    if (have_corr) fprintf(f, ", \"offset_soft_ns\": %lld", corr);
    if (have_raw)  fprintf(f, ", \"raw_delta_ns\": %lld", raw);
    else           fprintf(f, ", \"raw_delta_ns\": null");
    if (have_pd)   fprintf(f, ", \"path_delay_ns\": %lld", pd);
    else           fprintf(f, ", \"path_delay_ns\": null");
    if (have_gm)
      fprintf(f, ", \"gm_identity\": \"%02X-%02X-%02X-%02X-%02X-%02X-%02X-%02X\"}",
              gm[0], gm[1], gm[2], gm[3], gm[4], gm[5], gm[6], gm[7]);
    else
      fprintf(f, ", \"gm_identity\": \"\"}");
  }
  fprintf(f, "}\n");
  fclose(f);
  rename(tmp, "/tmp/mtl_ports.json");
}

int main(int argc, char** argv) {
  /* globaux partagés */
  char pmd[32] = "af_xdp", iface[64] = "", sip[64] = "", lcores[128] = "";
  const char* config = NULL;

  /* args legacy d'une session vidéo (compat tests manuels) */
  const char *l_mcast=NULL,*l_shm=NULL,*l_stats=NULL,*l_ident=NULL,*l_pci=NULL;
  int l_port=0,l_pt=96,l_w=1920,l_h=1080,l_ring=8,l_hdr=64,l_inter=0,l_bd=10; double l_fps=25.0;

  static struct option opts[] = {
    {"config",1,0,'c'},{"pmd",1,0,'M'},{"iface",1,0,'N'},{"sip",1,0,'s'},{"lcores",1,0,'l'},
    {"mcast",1,0,'m'},{"udp_port",1,0,'u'},{"payload_type",1,0,'t'},{"width",1,0,'W'},
    {"height",1,0,'H'},{"fps",1,0,'F'},{"interlaced",0,0,'i'},{"shm",1,0,'S'},{"ring",1,0,'R'},
    {"hdr",1,0,'D'},{"stats_file",1,0,'f'},{"bit_depth",1,0,'B'},{"ident_file",1,0,'G'},
    {"pci",1,0,'p'},{0,0,0,0}};
  int o;
  while ((o = getopt_long(argc, argv, "", opts, NULL)) != -1) {
    switch (o) {
      case 'c': config = optarg; break;
      case 'M': snprintf(pmd,sizeof(pmd),"%s",optarg); break;
      case 'N': snprintf(iface,sizeof(iface),"%s",optarg); break;
      case 's': snprintf(sip,sizeof(sip),"%s",optarg); break;
      case 'l': snprintf(lcores,sizeof(lcores),"%s",optarg); break;
      case 'm': l_mcast=optarg; break;   case 'u': l_port=atoi(optarg); break;
      case 't': l_pt=atoi(optarg); break; case 'W': l_w=atoi(optarg); break;
      case 'H': l_h=atoi(optarg); break;  case 'F': l_fps=atof(optarg); break;
      case 'i': l_inter=1; break;         case 'S': l_shm=optarg; break;
      case 'R': l_ring=atoi(optarg); break; case 'D': l_hdr=atoi(optarg); break;
      case 'f': l_stats=optarg; break;    case 'B': l_bd=atoi(optarg); break;
      case 'G': l_ident=optarg; break;    case 'p': l_pci=optarg; break;
      default: usage(argv[0]); return 1;
    }
  }
  (void)l_pci;

  /* nom de port MTL selon le PMD (rempli après lecture iface/pmd) */
  char portname[MTL_PORT_MAX_LEN];
  signal(SIGINT, on_signal); signal(SIGTERM, on_signal);
  static struct sess reg[MAX_SESS]; memset(reg, 0, sizeof(reg));
  static uint64_t last[MAX_SESS][MAX_TG]; memset(last, 0, sizeof(last));
  static uint64_t last_repeats[MAX_SESS][MAX_TG]; memset(last_repeats, 0, sizeof(last_repeats));

  /* Instance MXL (domaine tmpfs partagé avec les consommateurs). À vie, comme mtl_init. */
  const char* mxl_domain = getenv("MXL_DOMAIN");
  if (!mxl_domain || !*mxl_domain) mxl_domain = "/dev/shm/mxl";
  mkdir(mxl_domain, 0777);   /* idempotent ; /dev/shm existe déjà */
  g_mxl = mxlCreateInstance(mxl_domain, NULL);
  if (!g_mxl) { fprintf(stderr, "mtl_rx: mxlCreateInstance(%s) fail (tmpfs ?)\n", mxl_domain); return 1; }
  fprintf(stderr, "mtl_rx: domaine MXL = %s\n", mxl_domain);

  if (config) {
    /* ═══ DAEMON ═══ mtl_init UNE fois (à vie), puis réconciliation des sessions à chaud.
     * Le device reste démarré (XDP attaché) tant que le process vit → ptp4l ne faute qu'au boot. */
    struct json_object* root = json_object_from_file(config);
    if (!root) { fprintf(stderr, "mtl_rx: config illisible: %s\n", config); return 1; }
    snprintf(pmd,sizeof(pmd),"%s",jstr(root,"pmd","af_xdp"));
    snprintf(iface,sizeof(iface),"%s",jstr(root,"iface",""));
    snprintf(sip,sizeof(sip),"%s",jstr(root,"sip",""));
    snprintf(lcores,sizeof(lcores),"%s",jstr(root,"lcores",""));
    char pacing[16]; snprintf(pacing,sizeof(pacing),"%s",jstr(root,"pacing","auto"));
    char profile[16]; snprintf(profile,sizeof(profile),"%s",jstr(root,"profile",""));  /* classe scalaire (#26, repli mono-NIC) */
    int rx_q = jint(root,"rx_queues", 8);   /* plafond de sessions RX (= rx_count du moteur) */
    int tx_q = jint(root,"tx_queues", 1);   /* TX : Phase 2 (1 = file de contrôle IGMP/ARP) */
    int quota_mbs = jint(root,"quota_mbs", 5000);   /* Mb/s max par scheduler (lcore), cf. mtl_init */
    struct mtl_init_params p; memset(&p, 0, sizeof(p));
    /* ── Ports (NIC) du device ── multi-NIC : tableau "ports":[{iface,sip,rx_queues,tx_queues}].
     * Repli rétro-compat : "ports" absent → un seul port depuis les scalaires iface/sip/rx_queues/
     * tx_queues (config legacy / mono-NIC). g_ports sert ensuite à résoudre session→port par iface. */
    struct json_object* jports;
    if (json_object_object_get_ex(root,"ports",&jports) && json_object_is_type(jports,json_type_array)) {
      int np = json_object_array_length(jports);
      for (int k = 0; k < np && g_nports < MTL_PORT_MAX; k++) {
        struct json_object* pj = json_object_array_get_idx(jports, k);
        const char* pif = jstr(pj,"iface","");
        if (!pif[0]) continue;
        int idx = g_nports++;
        snprintf(g_ports[idx].iface, sizeof(g_ports[idx].iface), "%s", pif);
        /* PMD PAR PORT (chantier DPDK) : un nœud peut être MIXTE (af_xdp + dpdk). "pmd"/"bdf"
         * absents de l'entrée → pmd global (rétro-compat, comportement STRICTEMENT inchangé).
         * pmd=="dpdk" : le portname EST le BDF PCI tel quel ("0000:xx:00.y", aucun préfixe →
         * mtl_pmd_by_port_name rend MTL_PMD_DPDK_USER, cf. lib/src/mt_util.c). `iface` reste le
         * nom kernel historique : clé de résolution session→port (resolve_port) côté config. */
        const char* ppmd = jstr(pj, "pmd", "");
        const char* pbdf = jstr(pj, "bdf", "");
        if (!ppmd[0]) ppmd = pmd;                    /* repli : pmd global existant */
        if (!strcmp(ppmd, "dpdk") && pbdf[0])
          snprintf(g_ports[idx].portname, MTL_PORT_MAX_LEN, "%s", pbdf);
        else
          build_portname(ppmd, pif, g_ports[idx].portname);
        snprintf(g_ports[idx].pmd, sizeof(g_ports[idx].pmd), "%s", ppmd);
        /* CLASSE 2110-21 PAR PORT (#26) : profil d'émetteur (narrow/narrow_linear/wide) → cible VRX
         * des sessions TX de ce port. Absent → NARROW (défaut strict). */
        g_ports[idx].profile = parse_profile(jstr(pj, "profile", ""));
        snprintf(p.port[idx], MTL_PORT_MAX_LEN, "%s", g_ports[idx].portname);
        const char* psip = jstr(pj,"sip","");
        if (psip[0]) inet_pton(AF_INET, psip, p.sip_addr[idx]);
        /* MASQUE + PASSERELLE PAR PORT (2026-08-22). Un port bindé vfio-pci n'a plus de netdev
         * kernel : c'est libmtl qui porte TOUTE la couche 3, et jusqu'ici on ne lui donnait que
         * l'adresse. Sans masque elle ne sait pas ce qui est sur le lien, sans passerelle elle ne
         * peut joindre aucun unicast hors sous-réseau. Un plant ST 2110 micro-segmenté en /30 avec
         * une passerelle PAR fabric (A/B) — le cas Radio France IPMEDIA — l'exige. Optionnels :
         * absents → comportement strictement inchangé. */
        const char* pmask = jstr(pj,"netmask","");
        if (pmask[0]) inet_pton(AF_INET, pmask, p.netmask[idx]);
        const char* pgw = jstr(pj,"gateway","");
        if (pgw[0]) inet_pton(AF_INET, pgw, p.gateway[idx]);
        p.pmd[idx] = mtl_pmd_by_port_name(g_ports[idx].portname);
        int prxq = jint(pj,"rx_queues", 8), ptxq = jint(pj,"tx_queues", 1);
        p.rx_queues_cnt[idx] = prxq > 0 ? prxq : 1;
        p.tx_queues_cnt[idx] = ptxq > 0 ? ptxq : 1;
      }
    } else if (iface[0]) {   /* repli scalaire (mono-NIC) */
      int idx = g_nports++;
      snprintf(g_ports[idx].iface, sizeof(g_ports[idx].iface), "%s", iface);
      build_portname(pmd, iface, g_ports[idx].portname);
      snprintf(g_ports[idx].pmd, sizeof(g_ports[idx].pmd), "%s", pmd);
      g_ports[idx].profile = parse_profile(profile);   /* repli scalaire (#26, mono-NIC) */
      snprintf(p.port[idx], MTL_PORT_MAX_LEN, "%s", g_ports[idx].portname);
      if (sip[0]) inet_pton(AF_INET, sip, p.sip_addr[idx]);
      p.pmd[idx] = mtl_pmd_by_port_name(g_ports[idx].portname);
      p.rx_queues_cnt[idx] = rx_q > 0 ? rx_q : 1;
      p.tx_queues_cnt[idx] = tx_q > 0 ? tx_q : 1;
    }
    json_object_put(root);
    if (g_nports < 1) { fprintf(stderr,"mtl_rx: config sans port/iface\n"); return 1; }
    p.num_ports = g_nports;
    snprintf(portname, sizeof(portname), "%s", g_ports[0].portname);   /* défaut mono-NIC (reconcile) */
    /* AUTO_START_STOP : en AF_XDP le flow/XSK se crée AVEC le démarrage du device, déclenché par le
     * 1ᵉʳ st20p_rx_create (impossible de pré-démarrer un device vide : « add flow fail »). Le device
     * se start/stop au gré du nombre de sessions, mais le XDP reste attaché tant que mtl_init vit
     * (le détache n'a lieu qu'à mtl_uninit) → ptp4l ne faute qu'au boot, pas sur les changements. */
    p.flags |= MTL_FLAG_DEV_AUTO_START_STOP;
    /* Répartition des sessions sur les lcores : sans quota, libmtl empile TOUTES les sessions
     * vidéo sur sch_0 → saturation (epoch drop / build timeout → « RTP alignment failure » au
     * récepteur). quota_mbs (Mb/s) borne chaque scheduler (~2×1080p50 à 5000) → éclatement initial
     * sur les lcores fournis ; les flags MIGRATE rééquilibrent à chaud un sch détecté trop busy. */
    p.flags |= MTL_FLAG_TX_VIDEO_MIGRATE | MTL_FLAG_RX_VIDEO_MIGRATE;
    /* SONDE 2110-21 (TIMING_PARSER=1, niveau DEVICE) : active le HW timestamp du mbuf, prérequis du
     * timing parser (mesure fiable de l'arrivée paquet → Cinst/VRX/FPT/latency). Défaut OFF. */
    if (tp_wanted()) p.flags |= MTL_FLAG_ENABLE_HW_TIMESTAMP;
    /* PMD DPDK/ice sur PF média : filet optionnel pour le multicast. En AF-XDP le noyau programme
     * le filtre MAC mcast du groupe rejoint ; en DPDK user c'est libmtl (mt_mcast:
     * rte_eth_dev_mac_addr_add) qui l'ajoute. Si un déploiement observe rx_packets=0 AU NIVEAU PORT
     * malgré steering+join OK, activer MTL_FLAG_NIC_RX_PROMISCUOUS (env NIC_PROMISCUOUS=1) laisse
     * passer tout le trafic sur le port média dédié. Défaut OFF : sur ice le mac_addr_add suffit
     * dès que la SOURCE IGMP (sip du port) est correcte (cf. dl360-1 2026-07-07 : rx=0 était dû au
     * sip erroné, pas au filtre MAC). Sans objet en AF-XDP (jamais de port dpdk). */
    if (getenv("NIC_PROMISCUOUS") && atoi(getenv("NIC_PROMISCUOUS")))
      for (int k = 0; k < g_nports; k++)
        if (!strcmp(g_ports[k].pmd, "dpdk")) { p.flags |= MTL_FLAG_NIC_RX_PROMISCUOUS; break; }
    p.data_quota_mbs_per_sch = quota_mbs > 0 ? (uint32_t)quota_mbs : 0;
    /* Pacing TX ST 2110-21 (mtl_init_params.pacing, niveau DEVICE — enum st21_tx_pacing_way,
     * cf. mtl_api.h) : "rl" = rate-limit MATÉRIEL (prérequis profil narrow, PMD DPDK/ice
     * uniquement) ; "tsc" = logiciel (chemin actuel AF-XDP) ; "auto" (défaut) = libmtl choisit
     * (memset a déjà posé ST21_TX_PACING_WAY_AUTO=0 → clé absente = comportement inchangé). */
    if      (!strcmp(pacing, "rl"))         p.pacing = ST21_TX_PACING_WAY_RL;
    else if (!strcmp(pacing, "tsc_narrow")) p.pacing = ST21_TX_PACING_WAY_TSC_NARROW;
    else if (!strcmp(pacing, "tsc"))        p.pacing = ST21_TX_PACING_WAY_TSC;
    else                                    p.pacing = ST21_TX_PACING_WAY_AUTO;
    /* PTP INTERNE libmtl (chantier horloge, cf. PTP_CLOCK.md) — env-gaté, DÉFAUT OFF (inchangé :
     * libmtl lit CLOCK_REALTIME via ptp_from_real_time, discipliné par ptp4l/phc2sys kernel).
     * ENGINE_PTP=libmtl → libmtl devient esclave PTPv2 sur le(s) port(s) DPDK et discipline+lit le
     * PHC lui-même (ptp_from_eth) → indépendant du kernel (obligatoire en full-vfio où ptp4l kernel
     * ne tourne plus). +PI = servo PI (mt_ptp.c). ENGINE_PHC2SYS=1 → libmtl discipline AUSSI
     * CLOCK_REALTIME depuis le PHC (remplace phc2sys kernel). N'a de sens qu'avec ≥1 port DPDK. */
    int _has_dpdk = 0;
    for (int k = 0; k < g_nports; k++)
      if (!strcmp(g_ports[k].pmd, "dpdk")) { _has_dpdk = 1; break; }
    if (getenv("ENGINE_PTP") && !strcmp(getenv("ENGINE_PTP"), "libmtl") && _has_dpdk) {
      p.flags |= MTL_FLAG_PTP_ENABLE | MTL_FLAG_PTP_PI;
      if (getenv("ENGINE_PHC2SYS") && atoi(getenv("ENGINE_PHC2SYS")))
        p.flags |= MTL_FLAG_PHC2SYS_ENABLE;
      fprintf(stderr, "mtl_rx: PTP interne libmtl ACTIF (esclave PTPv2 + PI%s) — lit le PHC\n",
              (p.flags & MTL_FLAG_PHC2SYS_ENABLE) ? " + phc2sys REALTIME" : "");
      /* bobi.studio: le backstop « TX FIGÉ » est gaté sur l'ÉTAT de synchro PTP (cf. déclaration de
       * mt_bobi_ptp_stable) — pas de frame TX possible avant le lock, ce n'est pas un wedge. */
      g_engine_ptp = 1;
    }
    /* bobi.studio: profondeur du ring de descripteurs RX (banc G2 2026-07-11). Défaut MTL =
     * MT_DEV_RX_DESC=2048. Sous TX lourd co-localisé (2022-7 bi-port MÊME carte), la NIC droppe du RX
     * (rx_hw_dropped CORRÉLÉ sur les 2 ports) quand le DMA/PCIe est saturé par le TX : un ring plus
     * profond donne à la NIC plus de marge pour poser les paquets entrants entre deux polls. Réglable
     * via RX_NB_DESC ; défaut porté à 4096 sur socle DPDK (rte_eth_dev_adjust_nb_rx_tx_desc clampe au
     * max supporté par le device). Sans objet en AF-XDP (rx_size dérivé côté xsk). */
    { const char* _rxd = getenv("RX_NB_DESC");
      if (_rxd && atoi(_rxd) > 0)      p.nb_rx_desc = (uint16_t)atoi(_rxd);
      else if (_has_dpdk)              p.nb_rx_desc = 4096;
      if (p.nb_rx_desc)
        fprintf(stderr, "mtl_rx: ring RX = %u descripteurs (défaut MTL 2048)\n", p.nb_rx_desc); }
    /* bobi.studio: scheduler RX vidéo DÉDIÉ (levier #2, banc G2 2026-07-11). Le flag public
     * MTL_FLAG_RX_SEPARATE_VIDEO_LCORE fait demander à chaque session RX vidéo un scheduler
     * MT_SCH_TYPE_RX_VIDEO_ONLY (mt_sch.c) — isolé du CNI (qui porte le PTP/IGMP/ARP libmtl) et de
     * l'audio/TX. Complète le ring profond : le ring ABSORBE les stalls de polling induits par ces
     * co-tenants, ce flag en SUPPRIME la source (le RX ne partage plus son lcore avec eux). Coûte 1
     * lcore dédié par session RX (budget large : 16 lcores). Réglable RX_SEPARATE_LCORE ; défaut ON
     * sur socle DPDK (là où le CNI fait le PTP interne → co-tenance la plus pénalisante). */
    { const char* _rxs = getenv("RX_SEPARATE_LCORE");
      int _on = _rxs ? atoi(_rxs) : _has_dpdk;
      if (_on) { p.flags |= MTL_FLAG_RX_SEPARATE_VIDEO_LCORE;
                 fprintf(stderr, "mtl_rx: scheduler RX vidéo DÉDIÉ (isolé CNI/PTP/audio/TX)\n"); } }
    /* bobi.studio: lcore DÉDIÉ au CNI (levier #2 bis, banc 2026-07-14). Le flag ci-dessus NE SUFFIT
     * PAS : `sch_is_capable()` (mt_sch.c:399-408) PROMEUT un scheduler DEFAULT à quota NUL en
     * RX_VIDEO_ONLY — et le scheduler du CNI est exactement cela (mt_dev.c:1853-1855 crée
     * `main_sch` en MT_SCH_TYPE_DEFAULT avec quota 0 ; mt_cni.c:618 et mt_ptp.c:1379 y enregistrent
     * leurs tasklets). La PREMIÈRE session RX vidéo est donc acceptée SUR LE SCHEDULER DU CNI : le
     * levier d'isolation la rate. Mesuré : sch0 (CNI + RX vidéo #0) = 49,81-49,91 fps, 3 à 6 trames
     * incomplètes / 10 s, 0,07-0,25 % de paquets perdus non récupérés ; sch1/2/3 (RX vidéo seules) =
     * 50,000 fps, ZÉRO perte. Le CNI ne coûte pourtant que ~1 µs de boucle — ce microgramme suffit à
     * faire perdre une RX vidéo lourde (corollaire : tout tasklet de ~1 µs posé sur le scheduler
     * d'une RX vidéo lourde = perte mesurable).
     *
     * MTL_FLAG_DEDICATED_SYS_LCORE (flag PUBLIC, mtl_api.h:422) fait créer `main_sch` en
     * MT_SCH_TYPE_SYSTEM : sch_is_capable() ne promeut QUE les DEFAULT (mt_sch.c:399) et rejette
     * ensuite sur `sch->type != type` (mt_sch.c:409) → le scheduler du CNI devient INÉLIGIBLE aux
     * RX vidéo, sans patch libmtl et SANS toucher au CNI (qui garde son scheduler, ses tasklets
     * PTP/IGMP/ARP et son lcore — il ne le partage simplement plus). Coût : 1 lcore (comptabilisé
     * dans _auto_lcores côté orchestrateur). Réglable CNI_DEDICATED_LCORE ; défaut ON sur socle
     * DPDK (l'AF-XDP n'a pas le PTP interne dans le CNI et reste au comportement historique). */
    { const char* _cds = getenv("CNI_DEDICATED_LCORE");
      int _on = _cds ? atoi(_cds) : _has_dpdk;
      if (_on) { p.flags |= MTL_FLAG_DEDICATED_SYS_LCORE;
                 fprintf(stderr, "mtl_rx: lcore DÉDIÉ au CNI (scheduler système inéligible aux RX vidéo)\n"); } }
    p.log_level = mtl_log_level_env();          /* Réglages → MXL (défaut warning = logs lisibles) */
    p.dump_period_s = mtl_dump_period_env();    /* 0 = défaut lib ; grande valeur = dump neutralisé */
    p.lcores = lcores[0] ? lcores : NULL;

    mtl_handle st = mtl_init(&p);
    if (!st) { fprintf(stderr, "mtl_rx: mtl_init fail\n"); return 1; }
    fprintf(stderr, "mtl_rx: daemon up (%d port(s), rx_q[0]=%u tx_q[0]=%u) — réconciliation à chaud\n",
            g_nports, p.rx_queues_cnt[0], p.tx_queues_cnt[0]);

    /* Détection de changement en NANOSECONDES (st_mtim complet) : en rafale de commutations le
     * contrôleur réécrit le config plusieurs fois DANS LA MÊME SECONDE — comparer st_mtime seul
     * (secondes) ratait la dernière écriture → daemon figé sur un état intermédiaire jusqu'au
     * changement suivant (vu au banc de churn Horace : slot jamais monté pendant 12 s). */
    struct timespec cfg_mt = {0, 0}; struct stat cst;
    reconcile(reg, config, st, portname);                 /* état initial */
    if (stat(config, &cst) == 0) cfg_mt = cst.st_mtim;
    /* Fenêtre de stats : horloge MONOTONE nanoseconde. NE JAMAIS revenir à time_t/difftime — le
     * dt serait quantifié à la seconde ENTIÈRE (toujours 2.0) alors que la boucle tique à ~0.5s+ε
     * (overshoot usleep + stat() + iface_carrier() + write_stats). La phase du tic dérive contre la
     * grille de la seconde ; périodiquement le seuil dt>=2.0 est franchi après ~1.4s RÉELLES (3 tics
     * au lieu de 4) → fps = (1.4×50)/2.0 = 38 alors que le moteur n'a PAS ralenti. C'était le faux
     * « hoquet ~60 s » (= battement de la dérive, pas un événement) : creux toujours à 3/4 du
     * nominal, 0 frame perdue, invariant à tout (PTP, log, dump libmtl, orchestrateur arrêté). */
    uint64_t last_t = mono_ns();
    int carrier[MTL_PORT_MAX];
    for (int k = 0; k < g_nports; k++) carrier[k] = iface_carrier(g_ports[k].iface);
    while (!g_stop) {
      for (int z = 0; z < 5 && !g_stop; z++) usleep(100000);   /* ~0.5s, réactif au SIGTERM */
      if (g_stop) break;
      struct stat cs;
      if (stat(config, &cs) == 0 &&
          (cs.st_mtim.tv_sec != cfg_mt.tv_sec || cs.st_mtim.tv_nsec != cfg_mt.tv_nsec)) {
        cfg_mt = cs.st_mtim;
        reconcile(reg, config, st, portname);             /* config changé → converge à chaud */
      }
      /* Lien : journalisation seule. Les sessions ne sont PAS touchées — un port au lien
       * mort jette ses paquets côté libmtl (patch_afxdp_tx_link_drop), le leg vivant
       * continue sans interruption et le leg mort réémet seul au retour du lien. */
      for (int k = 0; k < g_nports; k++) {
        int c = iface_carrier(g_ports[k].iface);
        if (c != carrier[k]) {
          fprintf(stderr, "mtl_rx: lien %s → %s%s\n", g_ports[k].iface, c ? "UP" : "DOWN",
                  c ? "" : " (TX de ce port jeté par libmtl jusqu'au retour du lien)");
          carrier[k] = c;
        }
      }
      uint64_t now_mono = mono_ns(); double dt = (double)(now_mono - last_t) / 1e9;
      if (dt >= 2.0) { write_stats(reg, last, last_repeats, dt); last_t = now_mono;
        write_port_stats(st);            /* contrat /tmp/mtl_ports.json (stats I/O par NIC) */
        mxlGarbageCollectFlows(g_mxl);   /* récupère les flux orphelins (producteurs morts) */
        /* Backstop wedge TX (ultime filet — le lien mort est normalement absorbé par le patch
         * libmtl link_drop) : session démarrée dont le thread n'a plus AUCUN signe de vie depuis
         * > 5 s (get_frame ne rend plus rien) = queue TX XDP morte — irrécupérable in-process
         * (st20_tx_queue_fatal_error « nothing to do ») et la « récupération » interne de libmtl
         * fuit des memzones DPDK jusqu'au plafond → sortie IMMÉDIATE : le contrôleur relance le
         * daemon (purge XDP/ntuple + backoff, chemin crash déjà prévu), le process neuf repart
         * avec des memzones vierges. Pas de cleanup ici : libérer des queues mortes peut
         * bloquer, et _launch_mtl est conçu pour l'après-crash. */
        uint64_t wnow = mono_ns();
        /* bobi.studio: PTP interne pas (encore/plus) synchrone → aucune frame TX ne PEUT sortir
         * (train de pacing en attente) : suspendre le backstop, ce n'est pas un wedge de queue. */
        int ptp_gate = g_engine_ptp && !mt_bobi_ptp_stable(st, 0);
        for (int i = 0; i < MAX_SESS; i++) {
          struct sess* s2 = &reg[i];
          if (!s2->used || s2->role != ROLE_TX || !s2->started || !s2->tg[0].alive_ns) continue;
          if (ptp_gate) { s2->tg[0].alive_ns = wnow; continue; }   /* re-tare : pas de tir différé au lock */
          /* bobi.studio: ÂGE SIGNÉ obligatoire — alive_ns est posé par les threads TX (get_frame)
           * SANS synchro avec cette boucle : une mise à jour juste APRÈS le snapshot `wnow` donne
           * alive_ns > wnow, et la soustraction non signée wrappe à ~2^64 ns (« aucune frame depuis
           * 18446744073.7s ») → _exit(3) alors que la session est VIVANTE. Vu en prod 2026-07-13 :
           * 31 relances daemon en 3 h sur mtlrx141 (coupure de TOUS les flux à chaque fois). Une
           * session réellement figée donne un âge positif franc ; un delta négatif = signe de vie. */
          int64_t age_ns = (int64_t)(wnow - s2->tg[0].alive_ns);
          if (age_ns > 5ll * 1000000000ll) {
            /* bobi.studio: un ajout TX récent a stoppé le port (commit RL) → le stall n'est pas un
             * wedge, l'émission reprend seule à la fin du commit. Ne pas redémarrer pendant la grâce. */
            if (wnow < g_tx_add_grace_ns) continue;
            fprintf(stderr, "mtl_rx: TX FIGÉ %s:%d (aucune frame depuis %.1fs) — restart du daemon\n",
                    s2->mcast, s2->udp_port, age_ns / 1e9);
            fflush(stderr);
            _exit(3);
          }
        }
      }
    }
    for (int i = 0; i < MAX_SESS; i++) if (reg[i].used) free_session(&reg[i]);
    mtl_uninit(st);
    mxlDestroyInstance(g_mxl);
    return 0;
  }

  /* ═══ LEGACY one-shot ═══ (tests manuels : 1 session vidéo depuis les args, pas de réconciliation) */
  if (!l_mcast || !l_port || !l_shm || !iface[0]) { usage(argv[0]); return 1; }
  build_portname(pmd, iface, portname);

  struct mtl_init_params p; memset(&p, 0, sizeof(p));
  p.num_ports = 1;
  snprintf(p.port[MTL_PORT_P], MTL_PORT_MAX_LEN, "%s", portname);
  if (sip[0]) inet_pton(AF_INET, sip, p.sip_addr[MTL_PORT_P]);
  p.pmd[MTL_PORT_P] = mtl_pmd_by_port_name(portname);
  p.flags |= MTL_FLAG_DEV_AUTO_START_STOP;
  p.flags |= MTL_FLAG_TX_VIDEO_MIGRATE | MTL_FLAG_RX_VIDEO_MIGRATE;
  p.data_quota_mbs_per_sch = 5000;
  p.log_level = mtl_log_level_env();            /* idem site principal (Réglages → MXL) */
  p.dump_period_s = mtl_dump_period_env();
  p.lcores = lcores[0] ? lcores : NULL;
  p.rx_queues_cnt[MTL_PORT_P] = 1;
  p.tx_queues_cnt[MTL_PORT_P] = 1;

  mtl_handle st = mtl_init(&p);
  if (!st) { fprintf(stderr, "mtl_rx: mtl_init fail\n"); return 1; }

  struct sess* s = &reg[0]; s->kind = K_VIDEO; s->num_leg = 1;
  snprintf(s->mcast,sizeof(s->mcast),"%s",l_mcast);
  s->udp_port=l_port; s->payload_type=l_pt; s->ring=l_ring; s->hdr=l_hdr;
  s->width=l_w; s->height=l_h; s->fps=l_fps; s->interlaced=l_inter; s->bit_depth=l_bd;
  s->ntg = 1;
  snprintf(s->tg[0].shm_path,sizeof(s->tg[0].shm_path),"%s",l_shm);
  if (l_stats) snprintf(s->tg[0].stats_path,sizeof(s->tg[0].stats_path),"%s",l_stats);
  if (l_ident) { snprintf(s->tg[0].ident_file,sizeof(s->tg[0].ident_file),"%s",l_ident); s->tg[0].has_ident=1; }
  s->st = st; snprintf(s->portname, sizeof(s->portname), "%s", portname); s->stop = 0;
  if (setup_video(s) != 0) { fprintf(stderr,"mtl_rx: setup échoué\n"); mtl_uninit(st); mxlDestroyInstance(g_mxl); return 1; }
  s->used = 1;

  uint64_t last_t = mono_ns();       /* dt monotone ns — cf. boucle principale (jamais time_t) */
  while (!g_stop) {
    for (int z = 0; z < 20 && !g_stop; z++) usleep(100000);
    if (g_stop) break;
    uint64_t now_mono = mono_ns(); double dt = (double)(now_mono - last_t) / 1e9; last_t = now_mono;
    write_stats(reg, last, last_repeats, dt);
  }
  free_session(s);
  mtl_uninit(st);
  mxlDestroyInstance(g_mxl);
  return 0;
}

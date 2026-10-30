#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <inttypes.h>
#include <malloc.h>        // mallopt
#include <sys/socket.h>
#include <sys/resource.h>  // RLIMIT_MEMLOCK
#include <arpa/inet.h>
#include <netdb.h>
#include <infiniband/verbs.h>
#include <stdint.h> // For fixed-width integers
#include <endian.h> // For htobe64, be64toh
#include <time.h>

// Ring all-reduce over RDMA with two interchangeable protocols on one credit-based engine:
//   EAGER       the sender copies each segment into a registered bounce buffer and SENDs it into a
//               receive slot the receiver posted in advance; the receiver copies/reduces it out.
//   RENDEZVOUS  the receiver grants a destination (CTS: addr, rkey, len) and the sender RDMA-writes
//               straight from the user's buffer into it (zero copy), with an immediate as completion.
// Either way the sender needs a credit (a free receive slot at the receiver) for every message,
// so a message never finds an empty receive queue (no RNR NAKs).

// --- Defaults (each can be overridden at run time, see load_config) ---
#define DEF_SLOTS            16           // PG_SLOTS: receive slots per link = credits (K)
#define DEF_SLOT_SIZE        (64 * 1024)  // PG_SLOT_SIZE: bytes per slot = pipeline segment size
#define DEF_RNDV_THRESHOLD   (32 * 1024)  // PG_RNDV_THRESHOLD: PROTO_AUTO uses rendezvous from this chunk size
                                          // (measured crossover: 16-32 KB per chunk on 2-4 nodes)
#define DEF_RNR_RETRY        7            // PG_RNR_RETRY: 7 = retry forever, 0 = any RNR NAK is fatal
#define DEF_TIMEOUT_SEC      10           // PG_TIMEOUT: watchdog, seconds without any progress
#define REG_CACHE_SIZE       8            // rendezvous pin-down cache entries
#define POLL_BATCH           32           // completions per ibv_poll_cq
#define LEFT_MAX_INLINE      64           // control messages are sent inline on left_qp
#define RIGHT_MAX_INLINE     256          // eager-short: small segments are sent inline on right_qp
#define MEMLOCK_MARGIN       (1 << 20)    // pinned memory the provider needs for QPs and CQs

// --- QP attributes ---
// RNR NAK delay a sender waits when our receive isn't posted yet. 1 = 0.01ms. With credits it should
// never be used; PG_RNR_RETRY=0 proves that by turning any RNR NAK into an error.
#define MIN_RNR_TIMER       1
#define MAX_DEST_RD_ATOMIC  1
#define MAX_RD_ATOMIC       1
#define QP_TIMEOUT          14
#define RETRY_CNT           7
// ----------------------------

// -----------------------------------------------------------------------------
// 1. Core Structures
// -----------------------------------------------------------------------------

// Enumeration for collective operations (per your exercise instructions)
typedef enum {
    TYPE_INT,
    TYPE_FLOAT,
    TYPE_DOUBLE
} DATATYPE;

typedef enum {
    OP_SUM,
    OP_MAX,
    OP_MIN
} OPERATION;

typedef enum {
    PROTO_EAGER,
    PROTO_RNDV,
    PROTO_AUTO   // eager for small chunks, rendezvous from PG_RNDV_THRESHOLD bytes per chunk
} PROTOCOL;

// Settings both ends of every link must agree on (checked in connect_process_group).
// All uint32_t so it can be exchanged word by word in network byte order.
struct pg_config {
    uint32_t slots;           // K: receive slots per link = credits
    uint32_t slot_size;       // segment size; multiple of 8 so a segment never splits an element
    uint32_t credit_batch;    // return credits once this many slots are free again
    uint32_t grant_window;    // W: rendezvous grants a receiver may have outstanding
    uint32_t credits_on;      // 0 = ablation: eager ignores credits and relies on RNR retries
    uint32_t rnr_retry;
    uint32_t protocol;        // default protocol (PG_PROTOCOL)
    uint32_t rndv_threshold;  // PROTO_AUTO: chunk bytes from which rendezvous is used
    uint32_t reg_cache_on;    // 0 = register and deregister user buffers on every call
    uint32_t mtu;             // path MTU cap in bytes (0 = the ports' active MTU)
};
#define CFG_WORDS (sizeof(struct pg_config) / sizeof(uint32_t))

// The Out-Of-Band (TCP) data we need to exchange to connect QPs.
// This struct is exchanged field by field in network byte order.
// The fields are ordered to minimize padding.
struct rdma_dest {
    uint32_t qpn;             // Queue pair number
    uint32_t psn;             // Packet sequence number
    uint16_t lid;             // LID of the IB port
    uint16_t mtu;             // active_mtu of the port (enum ibv_mtu)
    uint32_t cfg[CFG_WORDS];  // struct pg_config, must match ours
};

// Control messages travel right -> left (receiver of data -> sender of data), sent inline.
// Both ends run the same binary, so the struct is sent as-is.
enum { CTRL_CREDIT = 1, CTRL_CTS = 2 };
struct ctrl_msg {
    uint32_t type;      // CTRL_CREDIT or CTRL_CTS
    uint32_t credits;   // slots freed since the last report (also piggybacked on every CTS)
    uint64_t seq;       // CTS: message the grant is for
    uint64_t addr;      // CTS: where to RDMA-write it
    uint32_t rkey;      // CTS
    uint32_t len;       // CTS: expected length
};
_Static_assert(sizeof(struct ctrl_msg) == 32, "ctrl_msg is sent inline, keep it 32 bytes");

// Every work request carries its kind and an index, so completions on the shared CQ are
// dispatched by wr_id instead of by counting them.
enum { WR_SLOT_RECV = 1, WR_CTRL_RECV, WR_DATA_SEND, WR_CTRL_SEND };
#define WRID(kind, idx)  (((uint64_t)(kind) << 48) | (uint64_t)(idx))
#define WRID_KIND(id)    ((int)((id) >> 48))
#define WRID_IDX(id)     ((uint32_t)((id) & 0xffffffffULL))

// A data message that has arrived (its completion was polled) but was not processed yet
struct arrival {
    uint32_t slot;
    uint32_t byte_len;
    int      opcode;
};

// A CTS grant received from the right neighbour and not used yet
struct grant {
    uint64_t seq;
    uint64_t addr;
    uint32_t rkey;
    uint32_t len;
};

// Rendezvous pin-down cache entry (page-aligned span of user memory)
struct reg_entry {
    char          *base;
    size_t         len;
    struct ibv_mr *mr;
    uint64_t       last_use;
};

struct pg_stats {
    uint64_t calls;
    uint64_t credit_stalls;   // times the next send was ready but had no credit
    uint64_t cts_waits;       // times the next rendezvous send was ready but had no CTS yet
    uint64_t ctrl_msgs;       // CREDIT + CTS messages sent
    uint64_t bytes_copied;    // eager memcpy traffic (bounce in + slot out)
    uint64_t reg_misses;      // rendezvous registrations (pin-down cache misses)
};

// The hidden implementation of your void **pg_handle
struct pg_handle_t {
    struct ibv_context *context;
    struct ibv_pd      *pd;
    struct ibv_cq      *cq;

    // Ring topology: one RC connection per neighbour. Data leaves on right_qp and arrives on
    // left_qp; credits and CTS grants travel the opposite way on the same connections.
    struct ibv_qp      *left_qp;
    struct ibv_qp      *right_qp;

    struct rdma_dest   left_remote_dest;
    struct rdma_dest   right_remote_dest;

    // OOB TCP sockets to neighbors (kept open for the closing barrier in pg_close)
    int left_sockfd;
    int right_sockfd;

    int my_rank;
    int num_nodes;

    struct pg_config cfg;
    PROTOCOL protocol;        // protocol of the next calls, see pg_set_protocol
    uint32_t ctrl_depth;      // control receives posted on right_qp
    uint32_t max_inline;      // eager-short limit: inline data supported by right_qp
    int sq_cap_left, sq_cap_right;  // send queue sizes we may fill
    int sq_out_left, sq_out_right;  // sends posted and not completed yet
    double timeout_sec;
    uint16_t path_mtu;
    size_t page_size;

    // Registered pools, fixed at connect time (independent of the message size)
    char            *slot_buf;   struct ibv_mr *slot_mr;    // K receive slots
    char            *bounce_buf; struct ibv_mr *bounce_mr;  // K eager send copies
    struct ctrl_msg *ctrl_buf;   struct ibv_mr *ctrl_mr;    // control receives
    int *bounce_free;
    int  bounce_nfree;

    // Link state. It persists across calls: a neighbour may already be in the next call.
    int64_t  tx_credits;      // free slots at the right neighbour we may fill
    uint32_t credits_owed;    // our slots re-posted but not yet reported to the left neighbour
    uint64_t tx_posted;       // data messages posted to the right (global sequence numbers)
    uint64_t tx_completed;
    uint64_t rx_polled;       // data messages from the left whose completion we polled
    uint64_t rx_processed;
    uint64_t next_grant;      // rendezvous: next message we grant to the left neighbour
    struct arrival *arrivals; // polled but not processed, indexed by seq % K
    struct grant   *grants;   // FIFO of W grants received but not used
    uint32_t grant_head, grant_count;
    uint64_t grant_next_min;  // grants arrive in increasing seq order

    // Rendezvous pin-down cache, within the memlock budget all ranks agreed on
    struct reg_entry reg[REG_CACHE_SIZE];
    uint64_t reg_clock;
    size_t   reg_pinned;
    size_t   rndv_budget;
    size_t   memlock_soft, memlock_hard;  // 0 = unlimited

    int connected;            // ring formed: pg_close may run the closing barrier
    int dead;                 // an error left the link state unusable
    double last_progress;
    struct pg_stats stats;
};

// One collective call. Message m of an all-reduce is numbered 0 .. 2(N-1)S-1:
// the first (N-1)S are reduce-scatter, the rest all-gather (S segments per chunk).
enum { RANGE_ALL, RANGE_RS, RANGE_AG };
struct ar_call {
    PROTOCOL  proto;
    DATATYPE  dt;
    OPERATION op;
    char *send, *recv;
    size_t dt_size;
    size_t chunk;             // bytes per chunk (count / N elements)
    size_t seg;               // bytes per segment = slot size
    uint64_t S;               // segments per chunk
    uint64_t m_begin, M;      // first message (all-reduce numbering) and number of messages
    uint64_t tx_base, rx_base;// global sequence number of message m_begin, per direction
    struct ibv_mr *send_mr, *recv_mr;  // rendezvous only
    int all_reduce;
    int credit_blocked, cts_blocked;
};

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

int pg_all_reduce(void *sendbuf, void *recvbuf, int count, DATATYPE datatype, OPERATION op, void *pg_handle);
int pg_close(void *pg_handle); // Forward declaration
static int collective(struct pg_handle_t *h, void *sendbuf, void *recvbuf, int count,
                      DATATYPE dt, OPERATION op, int range, int forced_proto);
static int post_slot_recv(struct pg_handle_t *h, uint32_t slot);
static int post_ctrl_recv(struct pg_handle_t *h, uint32_t idx);
static int poll_cq_batch(struct pg_handle_t *h);

#ifdef PG_SIM
// In-process simulator (sim_allreduce.c): replaces the HCA and the TCP bootstrap so the engine
// can be tested on a machine without RDMA hardware. See `make sim`.
static int  sim_open_device(struct pg_handle_t *h);
static int  sim_create_queues(struct pg_handle_t *h);
static int  sim_connect_ring(struct pg_handle_t *h);
static int  sim_barrier(struct pg_handle_t *h);
static void sim_destroy(struct pg_handle_t *h);
static struct ibv_mr *sim_reg_mr(struct ibv_pd *pd, void *addr, size_t len, int access);
static int  sim_dereg_mr(struct ibv_mr *mr);
#define pg_reg_mr   sim_reg_mr
#define pg_dereg_mr sim_dereg_mr
#else
static struct ibv_mr *pg_reg_mr(struct ibv_pd *pd, void *addr, size_t len, int access) {
    return ibv_reg_mr(pd, addr, len, access);
}
static int pg_dereg_mr(struct ibv_mr *mr) {
    return ibv_dereg_mr(mr);
}
#endif


// -----------------------------------------------------------------------------
// 2. Verbs Setup Functions
// -----------------------------------------------------------------------------

static struct ibv_qp* create_qp(struct pg_handle_t *handle, int max_send, int max_recv, int max_inline,
                                uint32_t *inline_out) {
    struct ibv_qp_init_attr attr = {
        .send_cq = handle->cq,
        .recv_cq = handle->cq,
        .cap     = {
            .max_send_wr     = max_send,
            .max_recv_wr     = max_recv,
            .max_send_sge    = 1,
            .max_recv_sge    = 1,
            .max_inline_data = max_inline,
        },
        .qp_type = IBV_QPT_RC // Reliable Connection
    };
    struct ibv_qp *qp = ibv_create_qp(handle->pd, &attr);
    if (!qp) return NULL;
    // The provider may round up, never down: make sure we got what the credit math relies on
    if ((int)attr.cap.max_send_wr < max_send || (int)attr.cap.max_recv_wr < max_recv ||
        (int)attr.cap.max_inline_data < (int)sizeof(struct ctrl_msg)) {
        fprintf(stderr, "Error: QP caps too small (send %u/%d, recv %u/%d, inline %u)\n",
                attr.cap.max_send_wr, max_send, attr.cap.max_recv_wr, max_recv, attr.cap.max_inline_data);
        ibv_destroy_qp(qp);
        return NULL;
    }
    *inline_out = attr.cap.max_inline_data;
    return qp;
}

static int modify_qp_to_init(struct ibv_qp *qp, int ib_port) {// initiation qp
    struct ibv_qp_attr attr = {
        .qp_state        = IBV_QPS_INIT,
        .pkey_index      = 0,
        .port_num        = ib_port,
        .qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
    };
    return ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
}

static int modify_qp_to_rtr(struct ibv_qp *qp, struct rdma_dest *remote, int ib_port, enum ibv_mtu mtu) { //ready to recive
    struct ibv_qp_attr attr = {
        .qp_state           = IBV_QPS_RTR,
        .path_mtu           = mtu,
        .dest_qp_num        = remote->qpn,
        .rq_psn             = remote->psn,
        .max_dest_rd_atomic = MAX_DEST_RD_ATOMIC,
        .min_rnr_timer      = MIN_RNR_TIMER,
        .ah_attr            = {
            .is_global      = 0,
            .dlid           = remote->lid,
            .sl             = 0,
            .src_path_bits  = 0,
            .port_num       = ib_port
        }
    };
    return ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
}

static int modify_qp_to_rts(struct ibv_qp *qp, int my_psn, int rnr_retry) { //ready to send
    struct ibv_qp_attr attr = {
        .qp_state       = IBV_QPS_RTS,
        .timeout        = QP_TIMEOUT,
        .retry_cnt      = RETRY_CNT,
        .rnr_retry      = rnr_retry,
        .sq_psn         = my_psn,
        .max_rd_atomic  = MAX_RD_ATOMIC
    };
    return ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
}


// -----------------------------------------------------------------------------
// 3. TCP Out-Of-Band Exchange
// -----------------------------------------------------------------------------

// read()/write() may transfer less than asked on a stream socket
static int read_full(int fd, void *buf, size_t len) {
    char *p = buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return -1;
        }
        p += n;
        len -= n;
    }
    return 0;
}

static int write_full(int fd, const void *buf, size_t len) {
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return -1;
        }
        p += n;
        len -= n;
    }
    return 0;
}

static void dest_to_net(const struct rdma_dest *in, struct rdma_dest *out) {
    out->qpn = htonl(in->qpn);
    out->psn = htonl(in->psn);
    out->lid = htons(in->lid);
    out->mtu = htons(in->mtu);
    for (size_t i = 0; i < CFG_WORDS; i++) out->cfg[i] = htonl(in->cfg[i]);
}

static void dest_from_net(const struct rdma_dest *in, struct rdma_dest *out) {
    out->qpn = ntohl(in->qpn);
    out->psn = ntohl(in->psn);
    out->lid = ntohs(in->lid);
    out->mtu = ntohs(in->mtu);
    for (size_t i = 0; i < CFG_WORDS; i++) out->cfg[i] = ntohl(in->cfg[i]);
}

// Accepts connection from the LEFT neighbor
// On success returns 0 and stores an open connected socket fd in *connfd_out (DO NOT close it)
static int exchange_with_left(struct pg_handle_t *handle, const struct rdma_dest *mine, int port, int *connfd_out) {
    int sockfd, connfd;
    struct sockaddr_in serv_addr;

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return -1;
    }

    int opt = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        close(sockfd);
        return -1;
    }

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(port);

    if (bind(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        perror("bind");
        close(sockfd);
        return -1;
    }
    if (listen(sockfd, 1) < 0) {
        perror("listen");
        close(sockfd);
        return -1;
    }

    printf("Listening for LEFT neighbor on port %d...\n", port);
    connfd = accept(sockfd, NULL, NULL);
    close(sockfd); // No longer need listening socket
    if (connfd < 0) {
        perror("accept");
        return -1;
    }

    // 1. Read Left's coordinates, 2. send mine (my left_qp, so they can send to me)
    struct rdma_dest remote_net, mine_net;
    if (read_full(connfd, &remote_net, sizeof(remote_net)) != 0) {
        fprintf(stderr, "Error: Failed to read from left neighbor.\n");
        close(connfd);
        return -1;
    }
    dest_from_net(&remote_net, &handle->left_remote_dest);

    dest_to_net(mine, &mine_net);
    if (write_full(connfd, &mine_net, sizeof(mine_net)) != 0) {
        fprintf(stderr, "Error: Failed to write to left neighbor.\n");
        close(connfd);
        return -1;
    }

    // Leave connfd open for the READY barrier and the closing barrier
    *connfd_out = connfd;
    return 0;
}

// Initiates connection to the RIGHT neighbor
// On success returns 0 and stores an open connected socket fd in *sockfd_out (DO NOT close it)
static int exchange_with_right(struct pg_handle_t *handle, const char *right_ip, const struct rdma_dest *mine, int port, int *sockfd_out) {
    int sockfd;
    struct addrinfo hints, *res;
    char port_str[16];

    // Convert port integer to string for getaddrinfo
    snprintf(port_str, sizeof(port_str), "%d", port);

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;       // IPv4
    hints.ai_socktype = SOCK_STREAM; // TCP

    // Resolve the hostname or IP address
    if (getaddrinfo(right_ip, port_str, &hints, &res) != 0) {
        fprintf(stderr, "Error: Could not resolve hostname or IP '%s'.\n", right_ip);
        return -1;
    }

    printf("Connecting to RIGHT neighbor at %s:%d...\n", right_ip, port);

    // Retry connection until the other side is ready
    while (1) {
        sockfd = socket(res->ai_family, res->ai_socktype, res->ai_protocol);
        if (sockfd < 0) {
            perror("socket");
            freeaddrinfo(res);
            return -1; // Non-recoverable
        }

        if (connect(sockfd, res->ai_addr, res->ai_addrlen) == 0) {
            break; // Success
        }
        close(sockfd);
        usleep(100000); // Retry every 100ms
    }

    freeaddrinfo(res); // Clean up the resolved address memory

    // 1. Send my coordinates (my right_qp), 2. read Right's
    struct rdma_dest remote_net, mine_net;
    dest_to_net(mine, &mine_net);
    if (write_full(sockfd, &mine_net, sizeof(mine_net)) != 0) {
        fprintf(stderr, "Error: Failed to write to right neighbor.\n");
        close(sockfd);
        return -1;
    }
    if (read_full(sockfd, &remote_net, sizeof(remote_net)) != 0) {
        fprintf(stderr, "Error: Failed to read from right neighbor.\n");
        close(sockfd);
        return -1;
    }
    dest_from_net(&remote_net, &handle->right_remote_dest);

    // Leave sockfd open for the READY barrier and the closing barrier
    *sockfd_out = sockfd;
    return 0;
}

// Pairwise barrier: write one byte to both neighbours, then read one from both
static int tcp_barrier(struct pg_handle_t *handle) {
    char token = 1;
    if (write_full(handle->left_sockfd, &token, 1) != 0 || write_full(handle->right_sockfd, &token, 1) != 0 ||
        read_full(handle->left_sockfd, &token, 1) != 0 || read_full(handle->right_sockfd, &token, 1) != 0) {
        fprintf(stderr, "Rank %d: TCP barrier with neighbours failed.\n", handle->my_rank);
        return -1;
    }
    return 0;
}


// -----------------------------------------------------------------------------
// 4. Exercise API Implementation
// -----------------------------------------------------------------------------

static const char *proto_name(PROTOCOL p) {
    return p == PROTO_EAGER ? "eager" : p == PROTO_RNDV ? "rndv" : "auto";
}

static int parse_protocol(const char *s, PROTOCOL *out) {
    if (strcmp(s, "eager") == 0) *out = PROTO_EAGER;
    else if (strcmp(s, "rndv") == 0 || strcmp(s, "rendezvous") == 0) *out = PROTO_RNDV;
    else if (strcmp(s, "auto") == 0) *out = PROTO_AUTO;
    else return -1;
    return 0;
}

static uint32_t env_u32(const char *name, uint32_t def) {
    const char *v = getenv(name);
    if (!v || !*v) return def;
    char *end;
    unsigned long x = strtoul(v, &end, 0);
    if (*end != '\0' || x > UINT32_MAX) {
        fprintf(stderr, "Warning: ignoring %s=%s (not a number)\n", name, v);
        return def;
    }
    return (uint32_t)x;
}

// Reads the PG_* environment variables. Values that break the credit math are clamped.
static int load_config(struct pg_handle_t *h) {
    struct pg_config *c = &h->cfg;

    c->slots = env_u32("PG_SLOTS", DEF_SLOTS);
    c->slot_size = env_u32("PG_SLOT_SIZE", DEF_SLOT_SIZE);
    if (c->slots < 1 || c->slots > 4096) {
        fprintf(stderr, "Error: PG_SLOTS must be in [1, 4096].\n");
        return -1;
    }
    if (c->slot_size < 64 || c->slot_size % 8 != 0 || c->slot_size > (1u << 30)) {
        fprintf(stderr, "Error: PG_SLOT_SIZE must be a multiple of 8 in [64, 1 GB].\n");
        return -1;
    }

    // Batch in [1, K]: 0 would send empty CREDITs forever, > K would never send one
    c->credit_batch = env_u32("PG_CREDIT_BATCH", c->slots / 4 ? c->slots / 4 : 1);
    if (c->credit_batch < 1 || c->credit_batch > c->slots) {
        uint32_t b = c->credit_batch < 1 ? 1 : c->slots;
        fprintf(stderr, "Warning: PG_CREDIT_BATCH=%u clamped to %u (must be in [1, PG_SLOTS])\n", c->credit_batch, b);
        c->credit_batch = b;
    }
    // A grant beyond K could never be used anyway: every rendezvous write also needs a credit
    c->grant_window = env_u32("PG_GRANT_WINDOW", c->slots);
    if (c->grant_window < 1 || c->grant_window > c->slots) {
        uint32_t w = c->grant_window < 1 ? 1 : c->slots;
        fprintf(stderr, "Warning: PG_GRANT_WINDOW=%u clamped to %u (must be in [1, PG_SLOTS])\n", c->grant_window, w);
        c->grant_window = w;
    }

    c->credits_on = env_u32("PG_CREDITS", 1) != 0;
    c->rnr_retry = env_u32("PG_RNR_RETRY", DEF_RNR_RETRY);
    if (c->rnr_retry > 7) c->rnr_retry = 7;
    c->rndv_threshold = env_u32("PG_RNDV_THRESHOLD", DEF_RNDV_THRESHOLD);
    // Off by default: a cached registration goes stale if the caller frees a buffer and gets the
    // same address back later. main() turns it on, since its buffers live for the whole run.
    c->reg_cache_on = env_u32("PG_REG_CACHE", 0) != 0;
    c->mtu = env_u32("PG_MTU", 0);
    if (c->mtu != 0 && c->mtu != 256 && c->mtu != 512 && c->mtu != 1024 && c->mtu != 2048 && c->mtu != 4096) {
        fprintf(stderr, "Error: PG_MTU must be 0 (auto), 256, 512, 1024, 2048 or 4096.\n");
        return -1;
    }

    PROTOCOL p = PROTO_AUTO;
    const char *ps = getenv("PG_PROTOCOL");
    if (ps && *ps && parse_protocol(ps, &p) != 0) {
        fprintf(stderr, "Error: PG_PROTOCOL must be eager, rndv or auto.\n");
        return -1;
    }
    c->protocol = p;
    h->protocol = p;
    h->timeout_sec = env_u32("PG_TIMEOUT", DEF_TIMEOUT_SEC);

    // Control messages in flight: at most W grants plus K/batch credit messages (each carries
    // at least batch credits and there are only K). The sender re-posts a control buffer as soon
    // as it parses it, so this many receives on right_qp can never run out.
    h->ctrl_depth = c->grant_window + c->slots / c->credit_batch + 2;
    // Send queue entries are freed by ACKs, which can lag behind the peer's parsing: keep headroom
    h->sq_cap_left = 2 * h->ctrl_depth;
    h->sq_cap_right = 2 * c->slots;
    return 0;
}

// Raises the memlock soft limit to the hard limit; registrations count against it
static void raise_memlock_limit(struct pg_handle_t *h) {
    struct rlimit rl;
    if (getrlimit(RLIMIT_MEMLOCK, &rl) != 0) return;
    if (rl.rlim_cur != rl.rlim_max) {
        struct rlimit up = { rl.rlim_max, rl.rlim_max };
        if (setrlimit(RLIMIT_MEMLOCK, &up) == 0) rl = up;
    }
    h->memlock_soft = rl.rlim_cur == RLIM_INFINITY ? 0 : (size_t)rl.rlim_cur;
    h->memlock_hard = rl.rlim_max == RLIM_INFINITY ? 0 : (size_t)rl.rlim_max;
}

static size_t pool_bytes(const struct pg_handle_t *h) {
    return 2 * (size_t)h->cfg.slots * h->cfg.slot_size + h->ctrl_depth * sizeof(struct ctrl_msg);
}

// Link state: credits, FIFOs and the bounce free-list
static int alloc_link_state(struct pg_handle_t *h) {
    h->arrivals = calloc(h->cfg.slots, sizeof(*h->arrivals));
    h->grants = calloc(h->cfg.grant_window, sizeof(*h->grants));
    h->bounce_free = calloc(h->cfg.slots, sizeof(*h->bounce_free));
    if (!h->arrivals || !h->grants || !h->bounce_free) {
        fprintf(stderr, "Error: Could not allocate link state.\n");
        return -1;
    }
    for (uint32_t i = 0; i < h->cfg.slots; i++) h->bounce_free[i] = i;
    h->bounce_nfree = h->cfg.slots;
    h->tx_credits = h->cfg.slots;   // the right neighbour starts with all K slots posted
    return 0;
}

// Slot, bounce and control pools, registered once
static int alloc_pools(struct pg_handle_t *h) {
    size_t slots_bytes = (size_t)h->cfg.slots * h->cfg.slot_size;
    size_t ctrl_bytes = h->ctrl_depth * sizeof(struct ctrl_msg);

    if (h->memlock_soft && pool_bytes(h) + MEMLOCK_MARGIN > h->memlock_soft) {
        fprintf(stderr, "Error: the slot and bounce pools need %zu bytes pinned but ulimit -l is %zu KB; "
                "lower PG_SLOTS or PG_SLOT_SIZE.\n", pool_bytes(h), h->memlock_soft / 1024);
        return -1;
    }
    if (posix_memalign((void **)&h->slot_buf, h->page_size, slots_bytes) != 0 ||
        posix_memalign((void **)&h->bounce_buf, h->page_size, slots_bytes) != 0 ||
        posix_memalign((void **)&h->ctrl_buf, h->page_size, ctrl_bytes) != 0) {
        fprintf(stderr, "Error: Failed to allocate the slot pools.\n");
        return -1;
    }
    memset(h->slot_buf, 0, slots_bytes);
    memset(h->bounce_buf, 0, slots_bytes);
    memset(h->ctrl_buf, 0, ctrl_bytes);

    // Slots are written remotely (rendezvous reduce-scatter); bounce and control are local only
    h->slot_mr = pg_reg_mr(h->pd, h->slot_buf, slots_bytes, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    h->bounce_mr = pg_reg_mr(h->pd, h->bounce_buf, slots_bytes, IBV_ACCESS_LOCAL_WRITE);
    h->ctrl_mr = pg_reg_mr(h->pd, h->ctrl_buf, ctrl_bytes, IBV_ACCESS_LOCAL_WRITE);
    if (!h->slot_mr || !h->bounce_mr || !h->ctrl_mr) {
        fprintf(stderr, "Error: Failed to register the slot pools (%s); check ulimit -l.\n", strerror(errno));
        return -1;
    }
    return 0;
}

// Opens the first device with an active port
static int open_device(struct pg_handle_t *handle, int *ib_port_out, struct ibv_port_attr *portinfo,
                       struct ibv_device_attr *device_attr) {
    struct ibv_device **dev_list = NULL;
    int num_devices = 0;
    int ib_port = -1;

    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list || num_devices == 0) {
        fprintf(stderr, "Error: Failed to get IB device list.\n");
        if (dev_list) ibv_free_device_list(dev_list);
        return -1;
    }
    // look for an infiniband device
    for (int i = 0; i < num_devices; i++) {
        handle->context = ibv_open_device(dev_list[i]);
        if (!handle->context) continue;

        if (ibv_query_device(handle->context, device_attr) != 0) {
            ibv_close_device(handle->context);
            handle->context = NULL;
            continue;
        }
        // look for an available port
        for (uint8_t p = 1; p <= device_attr->phys_port_cnt; p++) {
            if (ibv_query_port(handle->context, p, portinfo) == 0) {
                if (portinfo->state == IBV_PORT_ACTIVE) {
                    ib_port = p;
                    break;
                }
            }
        }

        if (ib_port != -1) {
            break; // Found active port
        }

        ibv_close_device(handle->context);
        handle->context = NULL;
    }
    ibv_free_device_list(dev_list);

    if (!handle->context || ib_port == -1) {
        fprintf(stderr, "Error: Failed to find an active IB device and port.\n");
        return -1;
    }
    // allocate memory to the protection domain
    handle->pd = ibv_alloc_pd(handle->context);
    if (!handle->pd) {
        fprintf(stderr, "Error: Failed to allocate PD.\n");
        return -1;
    }
    *ib_port_out = ib_port;
    return 0;
}

// CQ and the two ring QPs, sized from the credit bounds, then moved to INIT
static int create_queues(struct pg_handle_t *handle, int ib_port, const struct ibv_device_attr *device_attr) {
    int left_send = handle->sq_cap_left, left_recv = handle->cfg.slots;
    int right_send = handle->sq_cap_right, right_recv = handle->ctrl_depth;
    int cq_depth = left_send + left_recv + right_send + right_recv + 16;

    if (left_send > device_attr->max_qp_wr || right_send > device_attr->max_qp_wr ||
        left_recv > device_attr->max_qp_wr || right_recv > device_attr->max_qp_wr ||
        cq_depth > device_attr->max_cqe) {
        fprintf(stderr, "Error: queue sizes exceed the device (max_qp_wr %d, max_cqe %d); lower PG_SLOTS.\n",
                device_attr->max_qp_wr, device_attr->max_cqe);
        return -1;
    }
    // create a completion queue shared by both QPs
    handle->cq = ibv_create_cq(handle->context, cq_depth, NULL, NULL, 0);
    if (!handle->cq) {
        fprintf(stderr, "Error: Failed to create CQ.\n");
        return -1;
    }

    // left_qp sends control messages and receives data; right_qp the other way round
    uint32_t left_inline;
    handle->left_qp = create_qp(handle, left_send, left_recv, LEFT_MAX_INLINE, &left_inline);
    if (!handle->left_qp) {
        fprintf(stderr, "Error: Failed to create left QP.\n");
        return -1;
    }
    handle->right_qp = create_qp(handle, right_send, right_recv, RIGHT_MAX_INLINE, &handle->max_inline);
    if (!handle->right_qp) {
        fprintf(stderr, "Error: Failed to create right QP.\n");
        return -1;
    }
    // initialize left and right qps
    if (modify_qp_to_init(handle->left_qp, ib_port) != 0) {
        fprintf(stderr, "Error: Failed to modify left QP to INIT.\n");
        return -1;
    }
    if (modify_qp_to_init(handle->right_qp, ib_port) != 0) {
        fprintf(stderr, "Error: Failed to modify right QP to INIT.\n");
        return -1;
    }
    return 0;
}

// All receives are posted before any neighbour can send (they wait for our READY)
static int post_initial_recvs(struct pg_handle_t *h) {
    for (uint32_t i = 0; i < h->cfg.slots; i++) {
        if (post_slot_recv(h, i) != 0) {
            fprintf(stderr, "Error: Failed to post slot receive %u.\n", i);
            return -1;
        }
    }
    for (uint32_t i = 0; i < h->ctrl_depth; i++) {
        if (post_ctrl_recv(h, i) != 0) {
            fprintf(stderr, "Error: Failed to post control receive %u.\n", i);
            return -1;
        }
    }
    return 0;
}

static int check_peer_config(const struct pg_handle_t *h, const struct rdma_dest *peer, const char *side) {
    static const char *names[CFG_WORDS] = {
        "PG_SLOTS", "PG_SLOT_SIZE", "PG_CREDIT_BATCH", "PG_GRANT_WINDOW", "PG_CREDITS",
        "PG_RNR_RETRY", "PG_PROTOCOL", "PG_RNDV_THRESHOLD", "PG_REG_CACHE", "PG_MTU"
    };
    const uint32_t *mine = (const uint32_t *)&h->cfg;
    int bad = 0;
    for (size_t i = 0; i < CFG_WORDS; i++) {
        if (mine[i] != peer->cfg[i]) {
            fprintf(stderr, "Rank %d: %s is %u here but %u on the %s neighbour.\n",
                    h->my_rank, names[i], mine[i], peer->cfg[i], side);
            bad = 1;
        }
    }
    return bad ? -1 : 0;
}

// OOB exchange with both neighbours, INIT -> RTR -> RTS, READY barrier
static int connect_ring(struct pg_handle_t *handle, char *servername, int oob_port, int ib_port,
                        const struct ibv_port_attr *portinfo) {
    // PSN must be random per QP connection; rand() was never seeded here before
    srand48(getpid() ^ (long)time(NULL) ^ ((long)handle->my_rank << 16));
    uint32_t my_psn = lrand48() & 0xffffff;

    struct rdma_dest my_left = { .qpn = handle->left_qp->qp_num, .psn = my_psn,
                                 .lid = portinfo->lid, .mtu = portinfo->active_mtu };
    memcpy(my_left.cfg, &handle->cfg, sizeof(handle->cfg));
    struct rdma_dest my_right = my_left;
    my_right.qpn = handle->right_qp->qp_num;

    // Avoid Deadlocks using Rank (Even listens first, Odd connects first)
    int my_rank = handle->my_rank;
    if (my_rank % 2 == 0) {
        if (exchange_with_left(handle, &my_left, oob_port, &handle->left_sockfd) != 0 ||
            exchange_with_right(handle, servername, &my_right, oob_port, &handle->right_sockfd) != 0) {
            fprintf(stderr, "Rank %d: OOB exchange failed.\n", my_rank);
            return -1;
        }
    } else {
        if (exchange_with_right(handle, servername, &my_right, oob_port, &handle->right_sockfd) != 0 ||
            exchange_with_left(handle, &my_left, oob_port, &handle->left_sockfd) != 0) {
            fprintf(stderr, "Rank %d: OOB exchange failed.\n", my_rank);
            return -1;
        }
    }
    if (check_peer_config(handle, &handle->left_remote_dest, "left") != 0 ||
        check_peer_config(handle, &handle->right_remote_dest, "right") != 0) {
        fprintf(stderr, "Rank %d: all ranks must run with the same PG_* settings.\n", my_rank);
        return -1;
    }

    // Plug in the connections (INIT -> RTR -> RTS), each with the smaller MTU of its two ports
    enum ibv_mtu left_mtu = portinfo->active_mtu < handle->left_remote_dest.mtu ?
                            portinfo->active_mtu : (enum ibv_mtu)handle->left_remote_dest.mtu;
    enum ibv_mtu right_mtu = portinfo->active_mtu < handle->right_remote_dest.mtu ?
                             portinfo->active_mtu : (enum ibv_mtu)handle->right_remote_dest.mtu;
    if (handle->cfg.mtu) {  // PG_MTU caps it, e.g. 1024 to match the legacy binary
        enum ibv_mtu cap = handle->cfg.mtu == 256 ? IBV_MTU_256 : handle->cfg.mtu == 512 ? IBV_MTU_512 :
                           handle->cfg.mtu == 1024 ? IBV_MTU_1024 : handle->cfg.mtu == 2048 ? IBV_MTU_2048 : IBV_MTU_4096;
        if (left_mtu > cap) left_mtu = cap;
        if (right_mtu > cap) right_mtu = cap;
    }
    handle->path_mtu = left_mtu < right_mtu ? left_mtu : right_mtu;
    if (modify_qp_to_rtr(handle->left_qp, &handle->left_remote_dest, ib_port, left_mtu) != 0 ||
        modify_qp_to_rts(handle->left_qp, my_psn, handle->cfg.rnr_retry) != 0) {
        fprintf(stderr, "Error: Failed to bring left QP to RTS.\n");
        return -1;
    }
    if (modify_qp_to_rtr(handle->right_qp, &handle->right_remote_dest, ib_port, right_mtu) != 0 ||
        modify_qp_to_rts(handle->right_qp, my_psn, handle->cfg.rnr_retry) != 0) {
        fprintf(stderr, "Error: Failed to bring right QP to RTS.\n");
        return -1;
    }

    // Hardware-ready barrier: nobody sends before both its neighbours have QPs in RTS and
    // their receives posted. The sockets stay open for the closing barrier.
    if (tcp_barrier(handle) != 0) return -1;
    handle->connected = 1;
    return 0;
}

// All ranks agree on how much user memory rendezvous may pin, so every "can this size use
// rendezvous?" decision comes out the same on every rank (eager MIN all-reduce, in KB).
static int agree_rndv_budget(struct pg_handle_t *h) {
    size_t local;
    if (h->memlock_soft == 0) {
        local = (size_t)INT_MAX * 1024;
    } else {
        size_t used = pool_bytes(h) + MEMLOCK_MARGIN;
        local = h->memlock_soft > used ? h->memlock_soft - used : 0;
    }
    int n = h->num_nodes;
    int in[n], out[n];
    size_t kb = local / 1024;
    for (int i = 0; i < n; i++) in[i] = kb > INT_MAX ? INT_MAX : (int)kb;
    if (collective(h, in, out, n, TYPE_INT, OP_MIN, RANGE_ALL, PROTO_EAGER) != 0) {
        fprintf(stderr, "Rank %d: could not agree on the rendezvous memory budget.\n", h->my_rank);
        return -1;
    }
    h->rndv_budget = (size_t)out[0] * 1024;
    return 0;
}

/* * 'servername' acts as the IP address of your right-hand neighbor.
 */
int connect_process_group(char *servername, int my_rank, int num_nodes, void **pg_handle, int oob_port) {
    struct pg_handle_t *handle = calloc(1, sizeof(struct pg_handle_t));
    if (!handle) {
        fprintf(stderr, "Error: Could not allocate memory for pg_handle_t\n");
        return -1;
    }
    *pg_handle = handle;
    handle->my_rank = my_rank;
    handle->num_nodes = num_nodes;
    handle->left_sockfd = -1;
    handle->right_sockfd = -1;
    handle->page_size = sysconf(_SC_PAGESIZE);

    if (num_nodes < 2 || my_rank < 0 || my_rank >= num_nodes) {
        fprintf(stderr, "Error: need 0 <= rank < num_nodes and num_nodes >= 2.\n");
        goto error;
    }
    if (load_config(handle) != 0 || alloc_link_state(handle) != 0) goto error;
    raise_memlock_limit(handle);

#ifdef PG_SIM
    (void)servername;
    (void)oob_port;
    if (sim_open_device(handle) != 0 || alloc_pools(handle) != 0 || sim_create_queues(handle) != 0 ||
        post_initial_recvs(handle) != 0 || sim_connect_ring(handle) != 0)
        goto error;
#else
    {
        int ib_port = -1;
        struct ibv_port_attr portinfo;
        struct ibv_device_attr device_attr;
        // 1. Device, memory pools and queues; 2. receives; 3. OOB exchange, RTR/RTS, READY
        if (open_device(handle, &ib_port, &portinfo, &device_attr) != 0 || alloc_pools(handle) != 0 ||
            create_queues(handle, ib_port, &device_attr) != 0 || post_initial_recvs(handle) != 0 ||
            connect_ring(handle, servername, oob_port, ib_port, &portinfo) != 0)
            goto error;
    }
#endif
    if (agree_rndv_budget(handle) != 0) goto error;

    printf("Rank %d of %d successfully formed the RDMA Ring!\n", my_rank, num_nodes);
    return 0;

error:
    handle->connected = 0;  // no closing barrier: the neighbours may never get there
    pg_close(handle);
    *pg_handle = NULL;
    return -1;
}

// Protocol for the next calls. Collective: all ranks must set the same value between calls.
int pg_set_protocol(void *pg_handle, PROTOCOL p) {
    struct pg_handle_t *handle = (struct pg_handle_t *)pg_handle;
    if (!handle || (p != PROTO_EAGER && p != PROTO_RNDV && p != PROTO_AUTO)) return -1;
    handle->protocol = p;
    return 0;
}

static void reg_evict(struct pg_handle_t *h, struct reg_entry *e) {
    if (pg_dereg_mr(e->mr) != 0) fprintf(stderr, "Rank %d: ibv_dereg_mr failed.\n", h->my_rank);
    h->reg_pinned -= e->len;
    memset(e, 0, sizeof(*e));
}

// Drops every cached rendezvous registration. Must be called before freeing (or unmapping)
// memory that was passed to a rendezvous call: a cached MR keeps pointing at the old pages.
int pg_reg_cache_flush(void *pg_handle) {
    struct pg_handle_t *h = (struct pg_handle_t *)pg_handle;
    if (!h) return -1;
    for (int i = 0; i < REG_CACHE_SIZE; i++) {
        if (h->reg[i].mr) reg_evict(h, &h->reg[i]);
    }
    return 0;
}

int pg_close(void *pg_handle) {
    struct pg_handle_t *handle = (struct pg_handle_t *)pg_handle;
    if (!handle) {
        return 0;
    }
    if (handle->connected && !handle->dead) {
        // 1. Our last sends (credits, eager data) must complete before the neighbours tear down
        double t0 = now_sec();
        while ((handle->sq_out_left > 0 || handle->sq_out_right > 0) && now_sec() - t0 < handle->timeout_sec) {
            if (poll_cq_batch(handle) < 0) break;
        }
        // 2. Nobody destroys its QPs while a neighbour may still send to them
#ifdef PG_SIM
        sim_barrier(handle);
#else
        tcp_barrier(handle);
#endif
    }
    pg_reg_cache_flush(handle);
#ifdef PG_SIM
    sim_destroy(handle);
#else
    if (handle->left_qp) ibv_destroy_qp(handle->left_qp);
    if (handle->right_qp) ibv_destroy_qp(handle->right_qp);
    if (handle->cq) ibv_destroy_cq(handle->cq);
#endif
    if (handle->slot_mr) pg_dereg_mr(handle->slot_mr);
    if (handle->bounce_mr) pg_dereg_mr(handle->bounce_mr);
    if (handle->ctrl_mr) pg_dereg_mr(handle->ctrl_mr);
#ifndef PG_SIM
    if (handle->pd) ibv_dealloc_pd(handle->pd);
    if (handle->context) ibv_close_device(handle->context);
#endif
    if (handle->left_sockfd >= 0) close(handle->left_sockfd);
    if (handle->right_sockfd >= 0) close(handle->right_sockfd);
    free(handle->slot_buf); // free(NULL) is safe
    free(handle->bounce_buf);
    free(handle->ctrl_buf);
    free(handle->arrivals);
    free(handle->grants);
    free(handle->bounce_free);
    free(handle);
    return 0;
}

// -----------------------------------------------------------------------------
// All-Reduce Helper Functions
// -----------------------------------------------------------------------------

static size_t get_datatype_size(DATATYPE datatype) {
    switch (datatype) {
        case TYPE_INT: return sizeof(int);
        case TYPE_FLOAT: return sizeof(float);
        case TYPE_DOUBLE: return sizeof(double);
        default: return 0;
    }
}

// dst = a OP b, element-wise (dst may be a, for in-place calls)
static void reduce_into(void *dst, const void *a, const void *b, size_t n, DATATYPE datatype, OPERATION op) {
    if (datatype == TYPE_INT) {
        int *d = (int *)dst;
        const int *x = (const int *)a, *y = (const int *)b;
        if (op == OP_SUM) for (size_t i = 0; i < n; i++) d[i] = x[i] + y[i];
        else if (op == OP_MAX) for (size_t i = 0; i < n; i++) d[i] = (x[i] > y[i]) ? x[i] : y[i];
        else if (op == OP_MIN) for (size_t i = 0; i < n; i++) d[i] = (x[i] < y[i]) ? x[i] : y[i];
    } else if (datatype == TYPE_FLOAT) {
        float *d = (float *)dst;
        const float *x = (const float *)a, *y = (const float *)b;
        if (op == OP_SUM) for (size_t i = 0; i < n; i++) d[i] = x[i] + y[i];
        else if (op == OP_MAX) for (size_t i = 0; i < n; i++) d[i] = (x[i] > y[i]) ? x[i] : y[i];
        else if (op == OP_MIN) for (size_t i = 0; i < n; i++) d[i] = (x[i] < y[i]) ? x[i] : y[i];
    } else if (datatype == TYPE_DOUBLE) {
        double *d = (double *)dst;
        const double *x = (const double *)a, *y = (const double *)b;
        if (op == OP_SUM) for (size_t i = 0; i < n; i++) d[i] = x[i] + y[i];
        else if (op == OP_MAX) for (size_t i = 0; i < n; i++) d[i] = (x[i] > y[i]) ? x[i] : y[i];
        else if (op == OP_MIN) for (size_t i = 0; i < n; i++) d[i] = (x[i] < y[i]) ? x[i] : y[i];
    }
}

// Helper function to print a 2D matrix cleanly to the console
void print_data(int rank, const char* label, int *data, int rows, int cols) {
    printf("\n=== Node %d: %s ===\n", rank, label);
    for (int i = 0; i < rows; ++i) {
        for (int j = 0; j < cols; ++j) {
            printf("%4d ", data[i * cols + j]);
        }
        printf("\n");
    }
    printf("=====================\n");
}

// --- Message schedule -------------------------------------------------------
// Rank r, reduce-scatter step s: sends chunk r-1-s, receives chunk r-2-s and reduces it.
// The last step (s = N-2) delivers chunk r, which rank r then owns fully reduced.
// All-gather step s: sends chunk r-s, receives chunk r-1-s straight into recvbuf.
// Each chunk travels as S segments, so segment j of step s+1 can leave while segment j+1
// of step s is still arriving (pipelining).

static inline int ring_mod(int a, int n) {
    return ((a % n) + n) % n;
}

// Phase, step and segment of message m, plus its length
static void msg_info(const struct pg_handle_t *h, const struct ar_call *c, uint64_t m,
                     int *is_ag, int *s, uint64_t *j, size_t *len) {
    uint64_t rs_msgs = (uint64_t)(h->num_nodes - 1) * c->S;
    *is_ag = m >= rs_msgs;
    uint64_t k = *is_ag ? m - rs_msgs : m;
    *s = (int)(k / c->S);
    *j = k % c->S;
    size_t off = *j * c->seg;
    *len = c->chunk - off < c->seg ? c->chunk - off : c->seg;
}

static char *seg_addr(const struct ar_call *c, char *base, int chunk_idx, uint64_t j) {
    return base + (size_t)chunk_idx * c->chunk + j * c->seg;
}

static int send_chunk(const struct pg_handle_t *h, int is_ag, int s) {
    return is_ag ? ring_mod(h->my_rank - s, h->num_nodes) : ring_mod(h->my_rank - 1 - s, h->num_nodes);
}

static int recv_chunk(const struct pg_handle_t *h, int is_ag, int s) {
    return is_ag ? ring_mod(h->my_rank - 1 - s, h->num_nodes) : ring_mod(h->my_rank - 2 - s, h->num_nodes);
}

// Rule 1: what I send as message m is what arrived as message m-S, after I processed it
// (reduced it, or stored it). The first S messages carry my own data.
static int send_ready(const struct pg_handle_t *h, const struct ar_call *c, uint64_t m) {
    uint64_t rel = m - c->m_begin;
    return rel < c->S || h->rx_processed - c->rx_base > rel - c->S;
}

// Rendezvous may pin sendbuf and recvbuf (page-rounded) within the agreed budget
static int rndv_fits(const struct pg_handle_t *h, size_t bytes) {
    return 2 * (bytes + 2 * h->page_size) <= h->rndv_budget;
}

// --- Posting work requests -------------------------------------------------

static int post_slot_recv(struct pg_handle_t *h, uint32_t slot) {
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    sge.addr = (uintptr_t)(h->slot_buf + (size_t)slot * h->cfg.slot_size);
    sge.length = h->cfg.slot_size;
    sge.lkey = h->slot_mr->lkey;

    wr.wr_id = WRID(WR_SLOT_RECV, slot);
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    return ibv_post_recv(h->left_qp, &wr, &bad_wr);
}

static int post_ctrl_recv(struct pg_handle_t *h, uint32_t idx) {
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    sge.addr = (uintptr_t)&h->ctrl_buf[idx];
    sge.length = sizeof(struct ctrl_msg);
    sge.lkey = h->ctrl_mr->lkey;

    wr.wr_id = WRID(WR_CTRL_RECV, idx);
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    return ibv_post_recv(h->right_qp, &wr, &bad_wr);
}

// Sends a control message to the left neighbour, inline and signaled.
// Returns 0 when posted, 1 when the send queue is full (try again later), -1 on error.
static int post_ctrl(struct pg_handle_t *h, uint32_t type, uint32_t credits, uint64_t seq,
                     uint64_t addr, uint32_t rkey, uint32_t len) {
    if (h->sq_out_left >= h->sq_cap_left) return 1;

    struct ctrl_msg msg = { .type = type, .credits = credits, .seq = seq, .addr = addr, .rkey = rkey, .len = len };
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    sge.addr = (uintptr_t)&msg;   // inline: the data is copied into the WQE, msg may go away
    sge.length = sizeof(msg);
    sge.lkey = 0;

    memset(&wr, 0, sizeof(wr));
    wr.wr_id = WRID(WR_CTRL_SEND, 0);
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;

    if (ibv_post_send(h->left_qp, &wr, &bad_wr) != 0) {
        fprintf(stderr, "Rank %d: posting a control message failed.\n", h->my_rank);
        return -1;
    }
    h->sq_out_left++;
    h->stats.ctrl_msgs++;
    h->last_progress = now_sec();
    return 0;
}

// --- The engine ------------------------------------------------------------

static void report_wc_error(struct pg_handle_t *h, const struct ibv_wc *wc) {
    static const char *kinds[] = { "?", "slot receive", "control receive", "data send", "control send" };
    int kind = WRID_KIND(wc->wr_id);
    fprintf(stderr, "Rank %d: work completion failed: %s (vendor_err 0x%x) on %s #%u\n",
            h->my_rank, ibv_wc_status_str(wc->status), wc->vendor_err,
            kind >= 1 && kind <= 4 ? kinds[kind] : kinds[0], WRID_IDX(wc->wr_id));
    if (wc->status == IBV_WC_RNR_RETRY_EXC_ERR)
        fprintf(stderr, "Rank %d: a message found no posted receive at the neighbour (RNR) and "
                "PG_RNR_RETRY=%u allowed no more retries.\n", h->my_rank, h->cfg.rnr_retry);
}

// Drains up to POLL_BATCH completions. Arrivals and grants are only queued here (they may
// belong to the next call); credits count as soon as they arrive.
static int poll_cq_batch(struct pg_handle_t *h) {
    struct ibv_wc wc[POLL_BATCH];
    int n = ibv_poll_cq(h->cq, POLL_BATCH, wc);
    if (n < 0) {
        fprintf(stderr, "Rank %d: ibv_poll_cq failed.\n", h->my_rank);
        return -1;
    }
    for (int i = 0; i < n; i++) {
        uint32_t idx = WRID_IDX(wc[i].wr_id);
        if (wc[i].status != IBV_WC_SUCCESS) {
            // Only the first error matters; the flushed requests that follow are noise
            if (!h->dead) report_wc_error(h, &wc[i]);
            h->dead = 1;
            return -1;
        }
        switch (WRID_KIND(wc[i].wr_id)) {
        case WR_SLOT_RECV: {
            uint64_t seq = h->rx_polled;
            if (!(wc[i].wc_flags & IBV_WC_WITH_IMM) || ntohl(wc[i].imm_data) != (uint32_t)seq ||
                idx != seq % h->cfg.slots) {
                fprintf(stderr, "Rank %d: data message %" PRIu64 " arrived out of order (slot %u, imm %u).\n",
                        h->my_rank, seq, idx, (wc[i].wc_flags & IBV_WC_WITH_IMM) ? ntohl(wc[i].imm_data) : 0);
                return -1;
            }
            struct arrival *a = &h->arrivals[seq % h->cfg.slots];
            a->slot = idx;
            a->byte_len = wc[i].byte_len;
            a->opcode = wc[i].opcode;
            h->rx_polled++;
            break;
        }
        case WR_CTRL_RECV: {
            struct ctrl_msg msg = h->ctrl_buf[idx];
            // Re-post first: the window bounds assume a parsed control buffer is free again
            if (post_ctrl_recv(h, idx) != 0) {
                fprintf(stderr, "Rank %d: re-posting a control receive failed.\n", h->my_rank);
                return -1;
            }
            // Credits count right away, even when they ride on a CTS for a later message
            h->tx_credits += msg.credits;
            if (msg.type == CTRL_CTS) {
                if (h->grant_count == h->cfg.grant_window || msg.seq < h->grant_next_min) {
                    fprintf(stderr, "Rank %d: unexpected CTS for message %" PRIu64 ".\n", h->my_rank, msg.seq);
                    return -1;
                }
                struct grant *g = &h->grants[(h->grant_head + h->grant_count) % h->cfg.grant_window];
                g->seq = msg.seq;
                g->addr = msg.addr;
                g->rkey = msg.rkey;
                g->len = msg.len;
                h->grant_count++;
                h->grant_next_min = msg.seq + 1;
            } else if (msg.type != CTRL_CREDIT) {
                fprintf(stderr, "Rank %d: unknown control message type %u.\n", h->my_rank, msg.type);
                return -1;
            }
            break;
        }
        case WR_DATA_SEND:
            h->tx_completed++;
            h->sq_out_right--;
            if (idx) h->bounce_free[h->bounce_nfree++] = idx - 1;
            break;
        case WR_CTRL_SEND:
            h->sq_out_left--;
            break;
        default:
            fprintf(stderr, "Rank %d: completion with unknown wr_id 0x%" PRIx64 ".\n", h->my_rank, wc[i].wr_id);
            return -1;
        }
    }
    if (n > 0) h->last_progress = now_sec();
    return n;
}

// Processes arrived messages of this call in order: reduce (reduce-scatter) or store
// (all-gather), then give the slot back to the left neighbour.
static int process_arrivals(struct pg_handle_t *h, struct ar_call *c) {
    uint64_t rs_msgs = (uint64_t)(h->num_nodes - 1) * c->S;
    while (h->rx_processed < h->rx_polled && h->rx_processed - c->rx_base < c->M) {
        uint64_t seq = h->rx_processed;
        uint64_t m = c->m_begin + (seq - c->rx_base);
        struct arrival *a = &h->arrivals[seq % h->cfg.slots];
        int is_ag, s;
        uint64_t j;
        size_t len;
        msg_info(h, c, m, &is_ag, &s, &j, &len);

        // Checked here, against the call that owns the message, not when it was polled
        int want = c->proto == PROTO_RNDV ? IBV_WC_RECV_RDMA_WITH_IMM : IBV_WC_RECV;
        if (a->opcode != want || a->byte_len != len) {
            fprintf(stderr, "Rank %d: message %" PRIu64 " is a %s of %u bytes, expected a %s of %zu bytes "
                    "(all ranks must call with the same protocol and count).\n", h->my_rank, seq,
                    a->opcode == IBV_WC_RECV ? "SEND" : "WRITE_WITH_IMM", a->byte_len,
                    want == IBV_WC_RECV ? "SEND" : "WRITE_WITH_IMM", len);
            return -1;
        }

        char *slot = h->slot_buf + (size_t)a->slot * h->cfg.slot_size;
        char *dst = seg_addr(c, c->recv, recv_chunk(h, is_ag, s), j);
        if (!is_ag) {
            reduce_into(dst, seg_addr(c, c->send, recv_chunk(h, 0, s), j), slot, len / c->dt_size, c->dt, c->op);
        } else {
            // This overwrites what I sent at reduce-scatter step s. That is safe without any wait:
            // the data can only exist after my send reached the right neighbour (see report).
            if (c->all_reduce && h->tx_posted - c->tx_base <= m - rs_msgs) {
                fprintf(stderr, "Rank %d: all-gather message %" PRIu64 " arrived before my reduce-scatter "
                        "send of the same segment.\n", h->my_rank, seq);
                return -1;
            }
            if (c->proto == PROTO_EAGER) {
                memcpy(dst, slot, len);
                h->stats.bytes_copied += len;
            }  // rendezvous: the data was written straight into recvbuf
        }

        if (post_slot_recv(h, a->slot) != 0) {
            fprintf(stderr, "Rank %d: re-posting slot %u failed.\n", h->my_rank, a->slot);
            return -1;
        }
        h->credits_owed++;
        h->rx_processed++;
    }
    return 0;
}

// Rendezvous: grant the next messages of this call, in order and at most W ahead of what has
// arrived. A reduce-scatter segment lands in the slot its receive will consume; an all-gather
// segment lands at its final place in recvbuf.
static int issue_grants(struct pg_handle_t *h, struct ar_call *c) {
    while (h->next_grant < c->rx_base + c->M && h->next_grant < h->rx_polled + h->cfg.grant_window) {
        uint64_t m = c->m_begin + (h->next_grant - c->rx_base);
        int is_ag, s;
        uint64_t j;
        size_t len;
        msg_info(h, c, m, &is_ag, &s, &j, &len);

        uint64_t addr;
        uint32_t rkey;
        if (!is_ag) {
            // Free by the time the sender may write: it needs a credit for this message, and
            // slots are re-posted in sequence order
            addr = (uintptr_t)(h->slot_buf + (size_t)(h->next_grant % h->cfg.slots) * h->cfg.slot_size);
            rkey = h->slot_mr->rkey;
        } else {
            addr = (uintptr_t)seg_addr(c, c->recv, recv_chunk(h, 1, s), j);
            rkey = c->recv_mr->rkey;
        }
        int rc = post_ctrl(h, CTRL_CTS, h->credits_owed, h->next_grant, addr, rkey, (uint32_t)len);
        if (rc < 0) return -1;
        if (rc > 0) break;
        h->credits_owed = 0;
        h->next_grant++;
    }
    return 0;
}

static int return_credits(struct pg_handle_t *h) {
    if (h->credits_owed < h->cfg.credit_batch) return 0;
    int rc = post_ctrl(h, CTRL_CREDIT, h->credits_owed, 0, 0, 0, 0);
    if (rc == 0) h->credits_owed = 0;
    return rc < 0 ? -1 : 0;
}

// Posts the next messages of this call, strictly in order, while they are ready (Rule 1),
// have a credit, and have a bounce buffer (eager) or a CTS (rendezvous).
static int post_sends(struct pg_handle_t *h, struct ar_call *c) {
    while (h->tx_posted - c->tx_base < c->M) {
        uint64_t seq = h->tx_posted;
        uint64_t m = c->m_begin + (seq - c->tx_base);
        if (!send_ready(h, c, m)) return 0;

        if ((c->proto == PROTO_RNDV || h->cfg.credits_on) && h->tx_credits <= 0) {
            if (!c->credit_blocked) h->stats.credit_stalls++;
            c->credit_blocked = 1;
            return 0;
        }
        if (h->sq_out_right >= h->sq_cap_right) return 0;

        int is_ag, s;
        uint64_t j;
        size_t len;
        msg_info(h, c, m, &is_ag, &s, &j, &len);
        int from_send = !is_ag && s == 0;   // reduce-scatter step 0 sends my own input
        char *src = seg_addr(c, from_send ? c->send : c->recv, send_chunk(h, is_ag, s), j);

        struct ibv_send_wr wr, *bad_wr = NULL;
        struct ibv_sge sge;
        memset(&wr, 0, sizeof(wr));
        sge.addr = (uintptr_t)src;
        sge.length = (uint32_t)len;
        sge.lkey = 0;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.imm_data = htonl((uint32_t)seq);
        int bounce = -1;

        if (c->proto == PROTO_EAGER) {
            if (h->grant_count > 0 && h->grants[h->grant_head].seq < c->tx_base + c->M) {
                fprintf(stderr, "Rank %d: got a CTS during an eager call (protocol mismatch between ranks).\n", h->my_rank);
                return -1;
            }
            wr.opcode = IBV_WR_SEND_WITH_IMM;
            if (len <= h->max_inline) {
                // eager-short: the CPU copies the data into the work request itself
                wr.send_flags |= IBV_SEND_INLINE;
                wr.wr_id = WRID(WR_DATA_SEND, 0);
            } else {
                // eager: copy into a registered bounce buffer
                if (h->bounce_nfree == 0) return 0;
                bounce = h->bounce_free[--h->bounce_nfree];
                char *b = h->bounce_buf + (size_t)bounce * h->cfg.slot_size;
                memcpy(b, src, len);
                h->stats.bytes_copied += len;
                sge.addr = (uintptr_t)b;
                sge.lkey = h->bounce_mr->lkey;
                wr.wr_id = WRID(WR_DATA_SEND, bounce + 1);
            }
        } else {
            if (h->grant_count == 0) {
                if (!c->cts_blocked) h->stats.cts_waits++;
                c->cts_blocked = 1;
                return 0;
            }
            struct grant *g = &h->grants[h->grant_head];
            if (g->seq != seq || g->len != len) {
                fprintf(stderr, "Rank %d: CTS for message %" PRIu64 " (%u bytes) but message %" PRIu64
                        " (%zu bytes) is next.\n", h->my_rank, g->seq, g->len, seq, len);
                return -1;
            }
            // rendezvous: zero copy, straight from the user's buffer into the granted address
            wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            wr.wr.rdma.remote_addr = g->addr;
            wr.wr.rdma.rkey = g->rkey;
            sge.lkey = from_send ? c->send_mr->lkey : c->recv_mr->lkey;
            wr.wr_id = WRID(WR_DATA_SEND, 0);
        }

        if (ibv_post_send(h->right_qp, &wr, &bad_wr) != 0) {
            fprintf(stderr, "Rank %d: posting data message %" PRIu64 " failed.\n", h->my_rank, seq);
            if (bounce >= 0) h->bounce_free[h->bounce_nfree++] = bounce;
            return -1;
        }
        if (c->proto == PROTO_RNDV) {
            h->grant_head = (h->grant_head + 1) % h->cfg.grant_window;
            h->grant_count--;
        }
        c->credit_blocked = 0;
        c->cts_blocked = 0;
        h->sq_out_right++;
        h->tx_credits--;
        h->tx_posted++;
        h->last_progress = now_sec();
    }
    return 0;
}

static int call_done(const struct pg_handle_t *h, const struct ar_call *c) {
    if (h->tx_posted - c->tx_base < c->M || h->rx_processed - c->rx_base < c->M) return 0;
    // Eager sources were copied when posted. Rendezvous: the NIC reads the user's buffers until
    // our writes complete.
    return c->proto != PROTO_RNDV || h->tx_completed >= c->tx_base + c->M;
}

static void dump_state(const struct pg_handle_t *h, const struct ar_call *c) {
    fprintf(stderr, "Rank %d state: %s call, M=%" PRIu64 " S=%" PRIu64 " | tx posted %" PRIu64 " completed %" PRIu64
            " credits %" PRId64 " grants queued %u | rx polled %" PRIu64 " processed %" PRIu64 " next grant %" PRIu64
            " owed %u | base tx %" PRIu64 " rx %" PRIu64 " | sq out %d/%d\n",
            h->my_rank, proto_name(c->proto), c->M, c->S, h->tx_posted, h->tx_completed, h->tx_credits,
            h->grant_count, h->rx_polled, h->rx_processed, h->next_grant, h->credits_owed,
            c->tx_base, c->rx_base, h->sq_out_left, h->sq_out_right);
}

static int run_collective(struct pg_handle_t *h, struct ar_call *c) {
    c->tx_base = h->tx_posted;
    c->rx_base = h->rx_processed;
    if (c->proto == PROTO_RNDV) {
        // Grants are numbered per call (eager calls never advance next_grant), and no rendezvous
        // message can arrive before we granted it
        if (h->next_grant > c->rx_base || h->rx_polled != c->rx_base) {
            fprintf(stderr, "Rank %d: eager data arrived for a rendezvous call (protocol mismatch between ranks).\n",
                    h->my_rank);
            goto fail;
        }
        h->next_grant = c->rx_base;
    }

    h->last_progress = now_sec();
    unsigned spins = 0;
    while (!call_done(h, c)) {
        if (poll_cq_batch(h) < 0 || process_arrivals(h, c) < 0) goto fail;
        if (c->proto == PROTO_RNDV && issue_grants(h, c) < 0) goto fail;
        if (return_credits(h) < 0 || post_sends(h, c) < 0) goto fail;
        if (++spins % 1024 == 0 && now_sec() - h->last_progress > h->timeout_sec) {
            fprintf(stderr, "Rank %d: no progress for %.0f s (watchdog, PG_TIMEOUT), giving up.\n",
                    h->my_rank, h->timeout_sec);
            goto fail;
        }
    }
    h->stats.calls++;
    return 0;

fail:
    h->dead = 1;
    dump_state(h, c);
    return -1;
}

// --- Rendezvous pin-down cache ---------------------------------------------

// Returns an MR covering [addr, addr+len), registering the page-aligned span on a miss.
// 'keep' is an MR the current call already holds and must not be evicted.
static struct ibv_mr *reg_cache_get(struct pg_handle_t *h, void *addr, size_t len, struct ibv_mr *keep) {
    uintptr_t start = (uintptr_t)addr & ~(uintptr_t)(h->page_size - 1);
    uintptr_t end = ((uintptr_t)addr + len + h->page_size - 1) & ~(uintptr_t)(h->page_size - 1);

    for (int i = 0; i < REG_CACHE_SIZE; i++) {
        struct reg_entry *e = &h->reg[i];
        if (e->mr && (uintptr_t)e->base <= start && (uintptr_t)e->base + e->len >= end) {
            e->last_use = ++h->reg_clock;
            return e->mr;
        }
    }
    h->stats.reg_misses++;

    // The kernel pins overlapping MRs twice: drop the ones the new span overlaps, then make room
    for (int i = 0; i < REG_CACHE_SIZE; i++) {
        struct reg_entry *e = &h->reg[i];
        if (e->mr && e->mr != keep && (uintptr_t)e->base < end && (uintptr_t)e->base + e->len > start)
            reg_evict(h, e);
    }
    struct reg_entry *slot = NULL;
    for (;;) {
        slot = NULL;
        for (int i = 0; i < REG_CACHE_SIZE && !slot; i++)
            if (!h->reg[i].mr) slot = &h->reg[i];
        if (slot && h->reg_pinned + (end - start) <= h->rndv_budget) break;
        struct reg_entry *lru = NULL;
        for (int i = 0; i < REG_CACHE_SIZE; i++) {
            struct reg_entry *e = &h->reg[i];
            if (e->mr && e->mr != keep && (!lru || e->last_use < lru->last_use)) lru = e;
        }
        if (!lru) break;
        reg_evict(h, lru);
    }
    if (!slot) {
        fprintf(stderr, "Rank %d: registration cache is full.\n", h->my_rank);
        return NULL;
    }

    struct ibv_mr *mr = pg_reg_mr(h->pd, (void *)start, end - start, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    if (!mr) {
        fprintf(stderr, "Rank %d: ibv_reg_mr of %zu bytes failed (%s): %zu bytes already pinned, rendezvous "
                "budget %zu, ulimit -l %zu KB.\n", h->my_rank, (size_t)(end - start), strerror(errno),
                h->reg_pinned, h->rndv_budget, h->memlock_soft / 1024);
        return NULL;
    }
    slot->base = (char *)start;
    slot->len = end - start;
    slot->mr = mr;
    slot->last_use = ++h->reg_clock;
    h->reg_pinned += slot->len;
    return mr;
}

// Registers the user buffers a rendezvous call hands to the NIC
static int acquire_user_mrs(struct pg_handle_t *h, struct ar_call *c, size_t bytes, int range) {
    int need_send = range != RANGE_AG && c->send != c->recv;  // reduce-scatter step 0 reads sendbuf
    if (!h->cfg.reg_cache_on) {
        h->stats.reg_misses++;
        int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE;
        c->recv_mr = pg_reg_mr(h->pd, c->recv, bytes, access);
        c->send_mr = need_send ? pg_reg_mr(h->pd, c->send, bytes, access) : c->recv_mr;
    } else {
        c->recv_mr = reg_cache_get(h, c->recv, bytes, NULL);
        c->send_mr = need_send && c->recv_mr ? reg_cache_get(h, c->send, bytes, c->recv_mr) : c->recv_mr;
    }
    if (!c->recv_mr || !c->send_mr) {
        fprintf(stderr, "Rank %d: could not register the user buffers for rendezvous.\n", h->my_rank);
        return -1;
    }
    return 0;
}

static void release_user_mrs(struct pg_handle_t *h, struct ar_call *c) {
    if (h->cfg.reg_cache_on) return;
    if (c->send_mr && c->send_mr != c->recv_mr) pg_dereg_mr(c->send_mr);
    if (c->recv_mr) pg_dereg_mr(c->recv_mr);
}

// Common entry of the three collectives. forced_proto >= 0 overrides handle->protocol
// (internal reductions always use eager).
static int collective(struct pg_handle_t *h, void *sendbuf, void *recvbuf, int count,
                      DATATYPE dt, OPERATION op, int range, int forced_proto) {
    if (!h) return -1;
    if (h->dead) {
        fprintf(stderr, "Rank %d: process group unusable after an earlier error.\n", h->my_rank);
        return -1;
    }
    int num_nodes = h->num_nodes;
    size_t dt_size = get_datatype_size(dt);
    if (dt_size == 0) {
        fprintf(stderr, "Error: Invalid datatype specified.\n");
        return -1;
    }
    if (range != RANGE_AG && op != OP_SUM && op != OP_MAX && op != OP_MIN) {
        fprintf(stderr, "Error: Invalid operation specified.\n");
        return -1;
    }
    if (count < 0 || count % num_nodes != 0) {
        fprintf(stderr, "Error: Element count must be divisible by the number of nodes.\n");
        return -1;
    }
    if (count == 0) return 0;

    struct ar_call c;
    memset(&c, 0, sizeof(c));
    c.dt = dt;
    c.op = op;
    c.dt_size = dt_size;
    c.send = sendbuf;
    c.recv = recvbuf;
    c.chunk = (size_t)(count / num_nodes) * dt_size;
    c.seg = h->cfg.slot_size;  // multiple of 8: a segment never splits an element
    c.S = (c.chunk + c.seg - 1) / c.seg;
    uint64_t rs_msgs = (uint64_t)(num_nodes - 1) * c.S;
    c.m_begin = range == RANGE_AG ? rs_msgs : 0;
    c.M = range == RANGE_ALL ? 2 * rs_msgs : rs_msgs;
    c.all_reduce = range == RANGE_ALL;

    size_t bytes = (size_t)count * dt_size;
    PROTOCOL p = forced_proto >= 0 ? (PROTOCOL)forced_proto : h->protocol;
    if (p == PROTO_AUTO)
        p = c.chunk >= h->cfg.rndv_threshold && rndv_fits(h, bytes) ? PROTO_RNDV : PROTO_EAGER;
    c.proto = p;

    if (range == RANGE_AG && sendbuf != recvbuf)  // all-gather starts from my own chunk in recvbuf
        memcpy(seg_addr(&c, c.recv, h->my_rank, 0), seg_addr(&c, c.send, h->my_rank, 0), c.chunk);

    if (p == PROTO_RNDV) {
        // Same decision on every rank (agreed budget), so no rank is left waiting for grants
        if (!rndv_fits(h, bytes)) {
            fprintf(stderr, "Rank %d: %zu bytes is too large for rendezvous (needs about %zu bytes pinned, "
                    "budget %zu from ulimit -l).\n", h->my_rank, bytes, 2 * bytes, h->rndv_budget);
            return -1;
        }
        if (acquire_user_mrs(h, &c, bytes, range) != 0) {
            h->dead = 1;
            return -1;
        }
    }
    // The harness's own reductions (forced eager) are left out of the protocol statistics
    struct pg_stats saved = h->stats;
    int rc = run_collective(h, &c);
    if (forced_proto >= 0) h->stats = saved;
    if (p == PROTO_RNDV && rc == 0) release_user_mrs(h, &c);
    return rc;
}

// -----------------------------------------------------------------------------

int pg_all_reduce(void *sendbuf, void *recvbuf, int count, DATATYPE datatype, OPERATION op, void *pg_handle) {
    return collective((struct pg_handle_t *)pg_handle, sendbuf, recvbuf, count, datatype, op, RANGE_ALL, -1);
}

// Reduce-scatter: afterwards chunk my_rank of recvbuf (count / N elements) holds the reduction
// of that chunk over all ranks. The rest of recvbuf is scratch.
int pg_reduce_scatter(void *sendbuf, void *recvbuf, int count, DATATYPE datatype, OPERATION op, void *pg_handle) {
    return collective((struct pg_handle_t *)pg_handle, sendbuf, recvbuf, count, datatype, op, RANGE_RS, -1);
}

// All-gather: chunk my_rank of sendbuf is this rank's contribution; afterwards recvbuf holds
// every rank's chunk in rank order.
int pg_all_gather(void *sendbuf, void *recvbuf, int count, DATATYPE datatype, void *pg_handle) {
    return collective((struct pg_handle_t *)pg_handle, sendbuf, recvbuf, count, datatype, OP_SUM, RANGE_AG, -1);
}


// -----------------------------------------------------------------------------
// 5. Test Main
// -----------------------------------------------------------------------------

// Small LCG, so the tests don't depend on (thread-unsafe) rand()
static uint32_t lcg_next(uint32_t *state) {
    *state = *state * 1664525u + 1013904223u;
    return *state >> 8;
}

// Reductions the harness itself needs (timings, error counts) always run eager
static int agree_max(struct pg_handle_t *h, double v, double *out) {
    int n = h->num_nodes;
    double in[n], res[n];
    for (int i = 0; i < n; i++) in[i] = v;
    if (collective(h, in, res, n, TYPE_DOUBLE, OP_MAX, RANGE_ALL, PROTO_EAGER) != 0) return -1;
    *out = res[0];
    return 0;
}

static int agree_sum(struct pg_handle_t *h, int v, int *out) {
    int n = h->num_nodes;
    int in[n], res[n];
    for (int i = 0; i < n; i++) in[i] = v;
    if (collective(h, in, res, n, TYPE_INT, OP_SUM, RANGE_ALL, PROTO_EAGER) != 0) return -1;
    *out = res[0];
    return 0;
}

// Small 2x10 correctness demo with random data, printed on every node, once per protocol.
static int run_demo(struct pg_handle_t *h, char *buf_a, char *buf_b) {
    const int total_elements = 20;
    const int rows = 2;
    const int cols = 10;
    int my_rank = h->my_rank, num_nodes = h->num_nodes;

    if (total_elements % num_nodes != 0) {
        if (my_rank == 0)
            printf("Skipping demo: %d elements not divisible by %d nodes.\n", total_elements, num_nodes);
        return 0;
    }

    int *sendbuf = (int *)buf_a, *recvbuf = (int *)buf_b;
    uint32_t seed = (uint32_t)time(NULL) * 2654435761u + my_rank; // Seed uniquely per node
    for (int i = 0; i < total_elements; ++i) {
        sendbuf[i] = lcg_next(&seed) % 100; // Random integers between 0 and 99
    }
    print_data(my_rank, "BEFORE ALL-REDUCE", sendbuf, rows, cols);

    PROTOCOL protos[2] = { PROTO_EAGER, PROTO_RNDV };
    for (int p = 0; p < 2; p++) {
        char label[64];
        if (protos[p] == PROTO_RNDV && !rndv_fits(h, total_elements * sizeof(int))) continue;
        memset(recvbuf, 0, total_elements * sizeof(int));
        pg_set_protocol(h, protos[p]);
        if (pg_all_reduce(sendbuf, recvbuf, total_elements, TYPE_INT, OP_SUM, h) != 0) {
            fprintf(stderr, "Error: pg_all_reduce (%s) failed.\n", proto_name(protos[p]));
            return -1;
        }
        snprintf(label, sizeof(label), "AFTER ALL-REDUCE (%s)", proto_name(protos[p]));
        print_data(my_rank, label, recvbuf, rows, cols);
    }
    return 0;
}

// --- Correctness tests (mode "test") ---------------------------------------

static void set_elem(void *buf, DATATYPE dt, size_t i, double v) {
    if (dt == TYPE_INT) ((int *)buf)[i] = (int)v;
    else if (dt == TYPE_FLOAT) ((float *)buf)[i] = (float)v;
    else ((double *)buf)[i] = v;
}

static double get_elem(const void *buf, DATATYPE dt, size_t i) {
    if (dt == TYPE_INT) return ((const int *)buf)[i];
    if (dt == TYPE_FLOAT) return ((const float *)buf)[i];
    return ((const double *)buf)[i];
}

// Small integers, so float and double sums are exact whatever the order
static double test_value(int rank, size_t i) {
    return (double)((int)((rank * 37 + (i % 9973) * 11) % 51) - 25);
}

static double test_expected(int num_nodes, size_t i, OPERATION op) {
    double r = test_value(0, i);
    for (int k = 1; k < num_nodes; k++) {
        double v = test_value(k, i);
        if (op == OP_SUM) r += v;
        else if (op == OP_MAX) r = v > r ? v : r;
        else r = v < r ? v : r;
    }
    return r;
}

static const char *dt_name(DATATYPE dt) {
    return dt == TYPE_INT ? "int" : dt == TYPE_FLOAT ? "float" : "double";
}

static const char *op_name(OPERATION op) {
    return op == OP_SUM ? "sum" : op == OP_MAX ? "max" : "min";
}

static const char *range_name(int range) {
    return range == RANGE_ALL ? "all-reduce" : range == RANGE_RS ? "reduce-scatter" : "all-gather";
}

// One collective call, checked element by element. Returns the number of wrong elements,
// or -1 if the call itself failed.
static int test_case(struct pg_handle_t *h, char *buf_a, char *buf_b, int range, DATATYPE dt, OPERATION op,
                     PROTOCOL proto, int in_place, size_t chunk_elems, int verbose) {
    int n = h->num_nodes, r = h->my_rank;
    size_t count = chunk_elems * n, dts = get_datatype_size(dt);
    char *send = buf_a, *recv = in_place ? buf_a : buf_b;

    // Too large for rendezvous under the agreed memlock budget: skipped, identically on every rank
    if (proto == PROTO_RNDV && !rndv_fits(h, count * dts)) return 0;

    // Poison the output so a segment that is never written can't pass with stale data
    memset(recv, 0xA5, count * dts);
    for (size_t i = 0; i < count; i++) {
        if (range == RANGE_AG && i / chunk_elems != (size_t)r) continue;
        set_elem(send, dt, i, test_value(r, i));
    }

    pg_set_protocol(h, proto);
    int rc;
    if (range == RANGE_ALL) rc = pg_all_reduce(send, recv, (int)count, dt, op, h);
    else if (range == RANGE_RS) rc = pg_reduce_scatter(send, recv, (int)count, dt, op, h);
    else rc = pg_all_gather(send, recv, (int)count, dt, h);
    if (rc != 0) {
        fprintf(stderr, "Rank %d: %s %s %s %s%s count %zu failed.\n", r, proto_name(proto), range_name(range),
                dt_name(dt), op_name(op), in_place ? " in-place" : "", count);
        return -1;
    }

    int errors = 0;
    for (size_t i = 0; i < count; i++) {
        size_t owner = i / chunk_elems;
        double expect;
        if (range == RANGE_RS) {
            if (owner != (size_t)r) continue;
            expect = test_expected(n, i, op);
        } else if (range == RANGE_AG) {
            expect = test_value((int)owner, i);
        } else {
            expect = test_expected(n, i, op);
        }
        double got = get_elem(recv, dt, i);
        if (got != expect) {
            if (errors < 3 && verbose)
                fprintf(stderr, "Rank %d: %s %s %s %s%s count %zu: element %zu is %g, expected %g\n", r,
                        proto_name(proto), range_name(range), dt_name(dt), op_name(op),
                        in_place ? " in-place" : "", count, i, got, expect);
            errors++;
        }
    }
    return errors;
}

// Smoke test, matrix of all combinations, then a randomized back-to-back stress run
static int run_tests(struct pg_handle_t *h, char *buf_a, char *buf_b) {
    int n = h->num_nodes, r = h->my_rank;
    size_t seg = h->cfg.slot_size;
    int total_errors = 0, sum = 0;
    double t0 = now_sec();

    // 1. Smoke test: data and control messages in both protocols
    for (int p = 0; p < 2; p++) {
        int e = test_case(h, buf_a, buf_b, RANGE_ALL, TYPE_INT, OP_SUM, p == 0 ? PROTO_EAGER : PROTO_RNDV, 0, 1, 1);
        if (e < 0) return -1;
        if (agree_sum(h, e, &sum) != 0) return -1;
        int skipped = p == 1 && !rndv_fits(h, n * sizeof(int));
        if (r == 0) printf("selftest %-5s: %s\n", p == 0 ? "eager" : "rndv", skipped ? "skipped (memlock)" : sum == 0 ? "OK" : "FAIL");
        total_errors += sum;
    }

    // 2. Matrix: datatypes x ops x protocols x collectives x placement x sizes around a segment
    int cases = 0, errors = 0;
    for (int dt = TYPE_INT; dt <= TYPE_DOUBLE; dt++) {
        size_t per_seg = seg / get_datatype_size((DATATYPE)dt);
        // ... and one with more segments per chunk than there are slots (S > K: credits must recycle)
        size_t sizes[] = { 1, 3, per_seg - 1, per_seg, per_seg + 1, 3 * per_seg + 5,
                           ((size_t)h->cfg.slots + 2) * per_seg + 3 };
        for (int range = RANGE_ALL; range <= RANGE_AG; range++) {
            for (int op = OP_SUM; op <= OP_MIN; op++) {
                if (range == RANGE_AG && op != OP_SUM) continue;   // all-gather does not reduce
                for (int p = 0; p < 2; p++) {
                    for (int in_place = 0; in_place < 2; in_place++) {
                        for (size_t s = 0; s < sizeof(sizes) / sizeof(sizes[0]); s++) {
                            int e = test_case(h, buf_a, buf_b, range, (DATATYPE)dt, (OPERATION)op,
                                              p == 0 ? PROTO_EAGER : PROTO_RNDV, in_place, sizes[s], 1);
                            if (e < 0) return -1;
                            errors += e;
                            cases++;
                        }
                    }
                }
            }
        }
    }
    if (agree_sum(h, errors, &sum) != 0) return -1;
    if (r == 0) printf("matrix: %d calls, %d wrong elements: %s\n", cases, sum, sum == 0 ? "OK" : "FAIL");
    total_errors += sum;

    // 3. Stress: the same pseudo-random call sequence on every rank, alternating protocols and
    //    sizes back to back, with random delays on some ranks to skew them against each other
    int iters = (int)env_u32("PG_TEST_ITERS", 2000);
    uint32_t shared = 12345, local = 777u + 31u * r;
    errors = 0;
    for (int it = 0; it < iters; it++) {
        PROTOCOL proto = (PROTOCOL)(lcg_next(&shared) % 3);
        uint32_t k = lcg_next(&shared) % 10;
        int range = k < 6 ? RANGE_ALL : k < 8 ? RANGE_RS : RANGE_AG;
        DATATYPE dt = (DATATYPE)(lcg_next(&shared) % 3);
        OPERATION op = (OPERATION)(lcg_next(&shared) % 3);
        int in_place = lcg_next(&shared) % 4 == 0;
        size_t per_seg = seg / get_datatype_size(dt);
        size_t max_elems = lcg_next(&shared) % 8 == 0 ? ((size_t)h->cfg.slots + 2) * per_seg + 3 : 3 * per_seg + 5;
        size_t elems = 1 + lcg_next(&shared) % max_elems;
        if (lcg_next(&local) % 4 == 0) usleep(lcg_next(&local) % 200);
        int e = test_case(h, buf_a, buf_b, range, dt, op, proto, in_place, elems, errors < 3);
        if (e < 0) return -1;
        errors += e;
    }
    if (agree_sum(h, errors, &sum) != 0) return -1;
    if (r == 0) printf("stress: %d calls, %d wrong elements: %s\n", iters, sum, sum == 0 ? "OK" : "FAIL");
    total_errors += sum;

    if (r == 0) printf("tests %s on %d nodes (%.1f s)\n", total_errors == 0 ? "PASSED" : "FAILED", n, now_sec() - t0);
    return total_errors == 0 ? 0 : -1;
}

// --- Benchmark (mode "bench") ----------------------------------------------

static const char *human_size(size_t bytes, char *out, size_t out_len) {
    // Sizes with 3 nodes aren't powers of 2 (e.g. 2040 B), so show decimals when not exact
    if (bytes >= (1 << 20))
        snprintf(out, out_len, bytes % (1 << 20) ? "%.2f MB" : "%.0f MB", bytes / 1048576.0);
    else if (bytes >= (1 << 10))
        snprintf(out, out_len, bytes % (1 << 10) ? "%.2f KB" : "%.0f KB", bytes / 1024.0);
    else
        snprintf(out, out_len, "%zu B", bytes);
    return out;
}

static int cmp_double(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return x < y ? -1 : x > y;
}

static void stats_add(struct pg_stats *acc, const struct pg_stats *after, const struct pg_stats *before) {
    acc->calls += after->calls - before->calls;
    acc->credit_stalls += after->credit_stalls - before->credit_stalls;
    acc->cts_waits += after->cts_waits - before->cts_waits;
    acc->ctrl_msgs += after->ctrl_msgs - before->ctrl_msgs;
    acc->bytes_copied += after->bytes_copied - before->bytes_copied;
    acc->reg_misses += after->reg_misses - before->reg_misses;
}

// Time of one call, slowest rank: back-to-back loop average ("bw"), or with a barrier before
// every call and the median over calls ("lat", PG_BENCH_MODE=lat)
static int timed_loop(struct pg_handle_t *h, int *sendbuf, int *recvbuf, int count, int iters, int lat_mode,
                      double *t_out) {
    if (!lat_mode) {
        double t0 = now_sec();
        for (int it = 0; it < iters; it++) {
            if (pg_all_reduce(sendbuf, recvbuf, count, TYPE_INT, OP_SUM, h) != 0) return -1;
        }
        return agree_max(h, (now_sec() - t0) / iters, t_out);
    }
    int n = h->num_nodes, padded = (iters + n - 1) / n * n, dummy;
    double *loc = calloc(padded, sizeof(double)), *mx = calloc(padded, sizeof(double));
    int rc = -1;
    if (!loc || !mx) goto out;
    for (int it = 0; it < iters; it++) {
        if (agree_sum(h, 0, &dummy) != 0) goto out;   // barrier, untimed
        double t0 = now_sec();
        if (pg_all_reduce(sendbuf, recvbuf, count, TYPE_INT, OP_SUM, h) != 0) goto out;
        loc[it] = now_sec() - t0;
    }
    if (collective(h, loc, mx, padded, TYPE_DOUBLE, OP_MAX, RANGE_ALL, PROTO_EAGER) != 0) goto out;
    qsort(mx, iters, sizeof(double), cmp_double);
    *t_out = mx[iters / 2];
    rc = 0;
out:
    free(loc);
    free(mx);
    return rc;
}

static double memcpy_gbps(char *dst, const char *src, size_t len, int reps) {
    double t0 = now_sec();
    for (int i = 0; i < reps; i++) {
        memcpy(dst, src, len);
        dst[i % len] ^= 1;  // keep the copies from being optimized away
    }
    return (double)len * reps / (now_sec() - t0) / 1e9;
}

#define MAX_BENCH_PROTOS 3
#define MAX_BENCH_REPS   50

// Sweeps message sizes (powers of 2, rounded down to a multiple of num_nodes ints). For each size
// every protocol gets a warmup, an agreed iteration count and R repetitions, with the protocol
// order alternating per repetition (ABBA) so drift hits both alike. Time per call is the slowest
// rank's; the CSV gets min/median/max over the repetitions. Rank 0 prints a table and writes
// the CSV plus <csv>.meta.txt (configuration, memcpy bandwidth, registration cost).
static int run_benchmark(struct pg_handle_t *h, char *buf_a, char *buf_b, size_t max_bytes, const char *csv_path,
                         const PROTOCOL *protos, int nprotos) {
    int my_rank = h->my_rank, num_nodes = h->num_nodes;
    int *sendbuf = (int *)buf_a, *recvbuf = (int *)buf_b;
    int reps = (int)env_u32("PG_BENCH_REPS", 5);
    double target_sec = env_u32("PG_BENCH_TARGET_MS", 100) / 1000.0;
    const char *mode_env = getenv("PG_BENCH_MODE");
    int lat_mode = mode_env && strcmp(mode_env, "lat") == 0;
    if (reps < 1) reps = 1;
    if (reps > MAX_BENCH_REPS) reps = MAX_BENCH_REPS;

    // Largest message rendezvous may use (same on every rank: the budget is agreed)
    size_t rndv_cap = h->rndv_budget / 2 > 2 * h->page_size ? h->rndv_budget / 2 - 2 * h->page_size : 0;
    if (rndv_cap > max_bytes) rndv_cap = max_bytes;
    int any_rndv = 0;
    for (int p = 0; p < nprotos; p++) any_rndv |= protos[p] != PROTO_EAGER;  // registration-cost table

    // Deterministic input so every node can check the result locally:
    // sendbuf[i] = rank + (i % 1000)  =>  sum = N(N-1)/2 + N * (i % 1000)
    // Written before anything is measured, so no page is touched for the first time inside a timing.
    for (size_t i = 0; i < max_bytes / sizeof(int); i++) {
        sendbuf[i] = my_rank + (int)(i % 1000);
    }
    memset(recvbuf, 0, max_bytes);

    FILE *csv = NULL, *meta = NULL;
    if (my_rank == 0) {
        char meta_path[1024];
        snprintf(meta_path, sizeof(meta_path), "%s.meta.txt", csv_path);
        csv = fopen(csv_path, "w");
        meta = fopen(meta_path, "w");
        if (!csv) perror("Error: fopen CSV");
        if (csv) {
            fprintf(csv, "protocol,mode,num_nodes,bytes,elements,chunk_bytes,segments,iters,reps,"
                         "t_min_us,t_med_us,t_max_us,avg_time_us,algbw_Gbps,busbw_Gbps,errors,"
                         "slots,slot_size,grant_window,credit_batch,credits_on,rnr_retry,rndv_cap_bytes,"
                         "credit_stalls,cts_waits,ctrl_msgs,bytes_copied,reg_misses\n");
        }
        if (meta) {
            fprintf(meta, "num_nodes: %d\nslots: %u\nslot_size: %u\ncredit_batch: %u\ngrant_window: %u\n"
                          "ctrl_depth: %u\ncredits_on: %u\nrnr_retry: %u\nrndv_threshold: %u\nreg_cache: %u\n"
                          "memlock_soft_kb: %zu\nmemlock_hard_kb: %zu\nrndv_budget: %zu\nrndv_cap_bytes: %zu\n"
                          "path_mtu: %d\nmax_inline: %u\nreps: %d\nmode: %s\n",
                    num_nodes, h->cfg.slots, h->cfg.slot_size, h->cfg.credit_batch, h->cfg.grant_window,
                    h->ctrl_depth, h->cfg.credits_on, h->cfg.rnr_retry, h->cfg.rndv_threshold, h->cfg.reg_cache_on,
                    h->memlock_soft / 1024, h->memlock_hard / 1024, h->rndv_budget, rndv_cap,
                    h->path_mtu ? 128 << h->path_mtu : 0, h->max_inline, reps, lat_mode ? "lat" : "bw");
            // One copy of a segment (cache resident) and of a whole buffer (memory bound); the
            // untimed copy first maps every page of the destination
            memcpy(buf_b, buf_a, max_bytes);
            fprintf(meta, "memcpy_segment_GBps: %.2f\nmemcpy_large_GBps: %.2f\n",
                    memcpy_gbps(buf_b, buf_a, h->cfg.slot_size, 2000),
                    memcpy_gbps(buf_b, buf_a, max_bytes, 5));
            memset(buf_b, 0, max_bytes);
            // Registration cost: why rendezvous needs a pin-down cache
            if (any_rndv) {
                pg_reg_cache_flush(h);
                fprintf(meta, "reg_cost: bytes reg_us dereg_us\n");
                for (size_t b = 64 * 1024; b <= rndv_cap; b *= 2) {
                    double t0 = now_sec();
                    struct ibv_mr *mr = pg_reg_mr(h->pd, buf_a, b, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
                    double t1 = now_sec();
                    if (!mr) break;
                    pg_dereg_mr(mr);
                    fprintf(meta, "reg_cost: %zu %.1f %.1f\n", b, (t1 - t0) * 1e6, (now_sec() - t1) * 1e6);
                }
            }
        }
        printf("\n=== Ring All-Reduce benchmark: %d nodes, TYPE_INT, OP_SUM, %d reps, %s mode ===\n",
               num_nodes, reps, lat_mode ? "latency" : "bandwidth");
        printf("K=%u slots x %u B, credit batch %u, grant window %u, rendezvous up to %zu B\n",
               h->cfg.slots, h->cfg.slot_size, h->cfg.credit_batch, h->cfg.grant_window, rndv_cap);
        printf("%-6s %10s %10s %7s %11s %11s %11s %10s %10s %6s\n",
               "Proto", "Size", "Elements", "Iters", "Min (us)", "Median (us)", "Max (us)", "AlgBW Gb/s", "BusBW Gb/s", "Check");
        printf("------------------------------------------------------------------------------------------------------\n");
        fflush(stdout);
    }

    int rc = 0;
    // One-time warm-up at the largest size of each protocol: pays first-touch costs (page faults,
    // NIC address translation for the whole buffer) once, so they don't land on whichever size
    // runs first. For rendezvous it also registers the largest span, so every smaller prefix hits
    // the registration cache.
    const int global_warmup = 20;
    for (int p = 0; p < nprotos; p++) {
        size_t top = protos[p] == PROTO_RNDV ? rndv_cap : max_bytes;
        int count = (int)(top / sizeof(int)) / num_nodes * num_nodes;
        if (count == 0) continue;
        pg_set_protocol(h, protos[p]);
        for (int w = 0; w < global_warmup; w++) {
            if (pg_all_reduce(sendbuf, recvbuf, count, TYPE_INT, OP_SUM, h) != 0) return -1;
        }
    }

    size_t prev_bytes = 0;
    for (size_t target = sizeof(int); target <= max_bytes; target *= 2) {
        int count = (int)(target / sizeof(int)) / num_nodes * num_nodes;
        if (count == 0) count = num_nodes;
        size_t bytes = (size_t)count * sizeof(int);
        if (bytes == prev_bytes || bytes > max_bytes) continue;
        prev_bytes = bytes;
        size_t chunk = bytes / num_nodes;
        uint64_t segments = (chunk + h->cfg.slot_size - 1) / h->cfg.slot_size;

        int active[MAX_BENCH_PROTOS], iters[MAX_BENCH_PROTOS], errs[MAX_BENCH_PROTOS];
        double times[MAX_BENCH_PROTOS][MAX_BENCH_REPS];
        struct pg_stats acc[MAX_BENCH_PROTOS];
        memset(acc, 0, sizeof(acc));

        // Warm up, then size the timed loop to ~target_sec from the slowest rank's warm-up time.
        // The first warm-up call is left out of the estimate (it may register buffers).
        for (int p = 0; p < nprotos; p++) {
            active[p] = !(protos[p] == PROTO_RNDV && bytes > rndv_cap);
            if (!active[p]) continue;
            pg_set_protocol(h, protos[p]);
            double tw = 0;
            for (int w = 0; w < 5; w++) {
                if (w == 1) tw = now_sec();
                if (pg_all_reduce(sendbuf, recvbuf, count, TYPE_INT, OP_SUM, h) != 0) return -1;
            }
            double t_est;
            if (agree_max(h, (now_sec() - tw) / 4, &t_est) != 0) return -1;
            double it_d = target_sec / t_est;
            iters[p] = it_d > 100000 ? 100000 : (it_d < 3 ? 3 : (int)it_d);
            if (lat_mode && iters[p] > 200) iters[p] = 200;
        }

        for (int k = 0; k < reps; k++) {
            for (int q = 0; q < nprotos; q++) {
                int p = k % 2 == 0 ? q : nprotos - 1 - q;
                if (!active[p]) continue;
                pg_set_protocol(h, protos[p]);
                struct pg_stats before = h->stats;
                if (timed_loop(h, sendbuf, recvbuf, count, iters[p], lat_mode, &times[p][k]) != 0) return -1;
                stats_add(&acc[p], &h->stats, &before);
            }
        }

        // Check one more call into a poisoned buffer, so stale data from earlier calls can't pass
        for (int p = 0; p < nprotos; p++) {
            if (!active[p]) continue;
            pg_set_protocol(h, protos[p]);
            memset(recvbuf, 0xA5, bytes);
            if (pg_all_reduce(sendbuf, recvbuf, count, TYPE_INT, OP_SUM, h) != 0) return -1;
            int errors = 0, base = num_nodes * (num_nodes - 1) / 2;
            for (int i = 0; i < count; i++) {
                if (recvbuf[i] != base + num_nodes * (i % 1000)) errors++;
            }
            if (agree_sum(h, errors, &errs[p]) != 0) return -1;
            if (errs[p] != 0) rc = -1;
        }

        if (my_rank == 0) {
            for (int p = 0; p < nprotos; p++) {
                if (!active[p]) continue;
                double t[MAX_BENCH_REPS];
                memcpy(t, times[p], reps * sizeof(double));
                qsort(t, reps, sizeof(double), cmp_double);
                double tmin = t[0], tmed = t[reps / 2], tmax = t[reps - 1];
                if (reps % 2 == 0) tmed = (t[reps / 2 - 1] + t[reps / 2]) / 2;
                double algbw = bytes * 8.0 / tmed / 1e9; // Gbit/s
                double busbw = algbw * 2.0 * (num_nodes - 1) / num_nodes;
                double calls = acc[p].calls ? (double)acc[p].calls : 1;
                char hs[32];
                printf("%-6s %10s %10d %7d %11.2f %11.2f %11.2f %10.3f %10.3f %6s\n",
                       proto_name(protos[p]), human_size(bytes, hs, sizeof(hs)), count, iters[p],
                       tmin * 1e6, tmed * 1e6, tmax * 1e6, algbw, busbw, errs[p] == 0 ? "OK" : "FAIL");
                if (csv) {
                    fprintf(csv, "%s,%s,%d,%zu,%d,%zu,%" PRIu64 ",%d,%d,%.4f,%.4f,%.4f,%.4f,%.6f,%.6f,%d,"
                                 "%u,%u,%u,%u,%u,%u,%zu,%.3f,%.3f,%.3f,%.1f,%.3f\n",
                            proto_name(protos[p]), lat_mode ? "lat" : "bw", num_nodes, bytes, count, chunk, segments,
                            iters[p], reps, tmin * 1e6, tmed * 1e6, tmax * 1e6, tmed * 1e6, algbw, busbw, errs[p],
                            h->cfg.slots, h->cfg.slot_size, h->cfg.grant_window, h->cfg.credit_batch,
                            h->cfg.credits_on, h->cfg.rnr_retry, rndv_cap,
                            acc[p].credit_stalls / calls, acc[p].cts_waits / calls, acc[p].ctrl_msgs / calls,
                            acc[p].bytes_copied / calls, acc[p].reg_misses / calls);
                    fflush(csv);
                }
            }
            fflush(stdout);
        }
    }

    if (my_rank == 0) {
        printf("------------------------------------------------------------------------------------------------------\n");
        printf("Times are the slowest rank's, min/median/max over %d repetitions. AlgBW = bits / median time "
               "(Gb/s = 10^9 bits/s).\nBusBW = AlgBW * 2(N-1)/N (per-link traffic of ring all-reduce).\n", reps);
        if (csv) printf("Results written to %s (+ .meta.txt)\n", csv_path);
    }
    if (csv) fclose(csv);
    if (meta) fclose(meta);
    return rc;
}

// Parses "eager,rndv" style lists; "all" means eager, rndv and auto
static int parse_protocol_list(const char *s, PROTOCOL *out, int *n) {
    *n = 0;
    if (strcmp(s, "all") == 0) {
        out[0] = PROTO_EAGER;
        out[1] = PROTO_RNDV;
        out[2] = PROTO_AUTO;
        *n = 3;
        return 0;
    }
    char item[32];
    while (*s) {
        size_t len = strcspn(s, ",");
        if (len == 0 || len >= sizeof(item) || *n == MAX_BENCH_PROTOS) return -1;
        memcpy(item, s, len);
        item[len] = '\0';
        if (parse_protocol(item, &out[*n]) != 0) return -1;
        (*n)++;
        s += len;
        if (*s == ',') s++;
    }
    return *n > 0 ? 0 : -1;
}

int main(int argc, char *argv[]) {
    if (argc < 4 || argc > 9) {
        printf("Usage: %s <my_rank> <right_neighbor_ip> <num_nodes> [oob_port] [max_bytes] [csv_path] [protocols] [mode]\n", argv[0]);
        printf("  oob_port   default 18515\n");
        printf("  max_bytes  largest message in the size sweep, default 32MB (rendezvous rows stop at the\n");
        printf("             size that fits the agreed ulimit -l budget)\n");
        printf("  csv_path   written by rank 0, default allreduce_results.csv\n");
        printf("  protocols  eager,rndv (default) | eager | rndv | auto | all (eager, rndv and auto)\n");
        printf("  mode       demo | test | bench | all (default)\n");
        printf("Tunables (same on all ranks): PG_SLOTS PG_SLOT_SIZE PG_CREDIT_BATCH PG_GRANT_WINDOW PG_CREDITS\n");
        printf("  PG_RNR_RETRY PG_PROTOCOL PG_RNDV_THRESHOLD PG_REG_CACHE PG_MTU PG_TIMEOUT; benchmark: PG_BENCH_REPS\n");
        printf("  PG_BENCH_TARGET_MS PG_BENCH_MODE=lat; tests: PG_TEST_ITERS\n");
        return 1;
    }

    int my_rank = atoi(argv[1]);
    char *right_ip = argv[2];
    int num_nodes = atoi(argv[3]);
    int oob_port = (argc >= 5) ? atoi(argv[4]) : 18515;
    size_t max_bytes = (argc >= 6) ? strtoull(argv[5], NULL, 10) : (32UL * 1024 * 1024);
    const char *csv_path = (argc >= 7) ? argv[6] : "allreduce_results.csv";
    const char *proto_list = (argc >= 8) ? argv[7] : "eager,rndv";
    const char *mode = (argc >= 9) ? argv[8] : "all";
    PROTOCOL protos[MAX_BENCH_PROTOS];
    int nprotos;
    if (parse_protocol_list(proto_list, protos, &nprotos) != 0) {
        fprintf(stderr, "Error: protocols must be a comma list of eager, rndv, auto (or all).\n");
        return 1;
    }
    int do_demo = !strcmp(mode, "demo") || !strcmp(mode, "all");
    int do_test = !strcmp(mode, "test") || !strcmp(mode, "all");
    int do_bench = !strcmp(mode, "bench") || !strcmp(mode, "all");
    if (!do_demo && !do_test && !do_bench) {
        fprintf(stderr, "Error: mode must be demo, test, bench or all.\n");
        return 1;
    }
    if (max_bytes < (size_t)num_nodes * sizeof(int)) max_bytes = (size_t)num_nodes * sizeof(int);

    // Keep freed memory mapped, so an address can't come back with different pages behind a
    // cached rendezvous registration (the classic MPI "leave pinned" setting)
    mallopt(M_MMAP_MAX, 0);
    mallopt(M_TRIM_THRESHOLD, -1);
    // ...which makes the rendezvous registration cache safe here (off by default in the library)
    setenv("PG_REG_CACHE", "1", 0);

    void *handle = NULL;
    // Build the Ring
    if (connect_process_group(right_ip, my_rank, num_nodes, &handle, oob_port) != 0) {
        fprintf(stderr, "Failed to connect process group. Exiting.\n");
        return 1;
    }
    struct pg_handle_t *h = handle;
    if (my_rank == 0) {
        printf("Config: K=%u slots of %u B, credit batch %u, grant window %u, control depth %u, credits %s,\n"
               "        rnr_retry %u, default protocol %s (rendezvous from %u B per chunk), reg cache %s,\n"
               "        ulimit -l %zu KB (hard %zu KB, 0 = unlimited), rendezvous budget %zu KB, path MTU %d, inline %u B\n",
               h->cfg.slots, h->cfg.slot_size, h->cfg.credit_batch, h->cfg.grant_window, h->ctrl_depth,
               h->cfg.credits_on ? "on" : "OFF", h->cfg.rnr_retry, proto_name(h->protocol), h->cfg.rndv_threshold,
               h->cfg.reg_cache_on ? "on" : "off", h->memlock_soft / 1024, h->memlock_hard / 1024,
               h->rndv_budget / 1024, h->path_mtu ? 128 << h->path_mtu : 0, h->max_inline);
    }

    // One pair of buffers for everything (the registration cache relies on addresses staying valid)
    size_t test_bytes = (((size_t)h->cfg.slots + 3) * h->cfg.slot_size + 5 * sizeof(double)) * num_nodes;
    size_t buf_bytes = max_bytes > test_bytes ? max_bytes : test_bytes;
    char *buf_a = malloc(buf_bytes), *buf_b = malloc(buf_bytes);
    int rc = 0;
    if (!buf_a || !buf_b) {
        fprintf(stderr, "Error: Failed to allocate %zu-byte buffers.\n", buf_bytes);
        rc = -1;
    }
    if (rc == 0 && do_demo) rc = run_demo(h, buf_a, buf_b);
    if (rc == 0 && do_test) rc = run_tests(h, buf_a, buf_b);
    if (rc == 0 && do_bench) rc = run_benchmark(h, buf_a, buf_b, max_bytes, csv_path, protos, nprotos);

    // Teardown (deregisters the cached buffers before they are freed)
    pg_close(handle);
    free(buf_a);
    free(buf_b);
    return rc == 0 ? 0 : 1;
}

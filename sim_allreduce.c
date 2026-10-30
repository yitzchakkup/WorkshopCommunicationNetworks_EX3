// In-process simulator for rdma_allreduce_with_ring.c: runs the real engine without RDMA hardware.
//
// Every rank is a thread. The "HCA" is a model behind the same ibv_post_send / ibv_post_recv /
// ibv_poll_cq calls the engine makes (libibverbs dispatches them through context->ops). The model
// enforces what an RC connection does:
//   - messages on a QP are delivered and completed in order, after a random delay;
//   - SEND, SEND_WITH_IMM and RDMA_WRITE_WITH_IMM consume the receiver's head receive. If none is
//     posted that is an RNR NAK: retried (PG_RNR_RETRY=7) or fatal (PG_RNR_RETRY=0);
//   - a SEND larger than the receive buffer, a write outside the rkey's region, a full send queue
//     or a full CQ are errors;
//   - the NIC reads the source when it transmits, not when the work request is posted, so a
//     buffer reused too early shows up as wrong data in the tests.
//
// Build: make sim
// Run:   ./sim_allreduce [num_nodes] [mode] [protocols] [max_bytes]   (defaults: 4 test eager,rndv 1MB)
//        SIM_DELAY_US (max delivery delay, default 30), SIM_SEED, and all PG_* tunables apply.

#include <pthread.h>
#include <sched.h>

#define PG_SIM 1
#define main pg_real_main
#include "rdma_allreduce_with_ring.c"
#undef main

struct sim_mr {
    struct ibv_mr  mr;
    int            rank;
    int            access;
    struct sim_mr *next;
};

struct sim_msg {
    struct sim_msg *next;
    enum ibv_wr_opcode opcode;
    uint64_t wr_id;
    int      signaled;
    int      is_inline;
    const char *src;                      // read at delivery time, like the NIC
    char     inline_data[RIGHT_MAX_INLINE];
    uint32_t len;
    uint64_t raddr;
    uint32_t rkey;
    uint32_t imm;                         // network order, as posted
    int      rnr_tries;
    double   due;
};

struct sim_ack {                          // sender completion, delivered after an ACK delay
    struct sim_ack *next;
    struct ibv_wc   wc;
    double          due;
};

struct sim_cq {
    struct ibv_cq  cq;
    struct ibv_wc *ring;
    int cap, head, count;
};

struct sim_wqe {
    uint64_t wr_id;
    char    *addr;
    uint32_t len;
};

struct sim_qp {
    struct ibv_qp   qp;
    int             rank;
    const char     *side;
    struct sim_cq  *cq;
    struct sim_qp  *peer;
    struct sim_wqe *rq;
    int rq_cap, rq_head, rq_count;
    int sq_cap, sq_out;
    int max_inline;
    int rnr_retry;
    int error;
    struct sim_msg *head, *tail;          // in flight to the peer
    struct sim_ack *ack_head, *ack_tail;  // completions on their way back
    struct sim_qp  *next_all;
};

static pthread_mutex_t sim_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_barrier_t sim_bar;
static int sim_n;
static struct pg_handle_t **sim_handles;
static struct sim_qp *sim_qps;
static struct sim_mr *sim_mrs;
static uint32_t sim_key = 1, sim_qpn = 100, sim_rng = 4242;
static uint64_t sim_rnr_naks, sim_delivered;
static int sim_fatal;
static double sim_max_delay = 30e-6;

static uint32_t sim_rand(void) {
    sim_rng = sim_rng * 1664525u + 1013904223u;
    return sim_rng >> 8;
}

// Random delay; now and then a long one, so ranks drift apart and calls overlap
static double sim_delay(void) {
    double d = sim_max_delay * (sim_rand() % 1000) / 1000.0;
    if (sim_rand() % 64 == 0) d *= 20;
    return d;
}

static void sim_cq_push(struct sim_cq *cq, const struct ibv_wc *wc) {
    if (cq->count == cq->cap) {
        fprintf(stderr, "SIM: CQ overrun (depth %d): the CQ is too small for the outstanding requests\n", cq->cap);
        abort();
    }
    cq->ring[(cq->head + cq->count) % cq->cap] = *wc;
    cq->count++;
}

static struct sim_mr *sim_find_mr(int rank, uint32_t key, uintptr_t addr, size_t len, int need_access) {
    for (struct sim_mr *mr = sim_mrs; mr; mr = mr->next) {
        if (mr->rank != rank || mr->mr.lkey != key) continue;
        if (addr < (uintptr_t)mr->mr.addr || addr + len > (uintptr_t)mr->mr.addr + mr->mr.length) return NULL;
        if ((mr->access & need_access) != need_access) return NULL;
        return mr;
    }
    return NULL;
}

static void sim_fail(struct sim_qp *q, const char *what) {
    if (!q->error)
        fprintf(stderr, "SIM: rank %d %s QP: %s\n", q->rank, q->side, what);
    q->error = 1;
    sim_fatal = 1;
}

// Sender completion goes out after an ACK delay, in order per QP
static void sim_queue_ack(struct sim_qp *q, struct sim_msg *m, enum ibv_wc_status st, double now) {
    struct sim_ack *a = calloc(1, sizeof(*a));
    a->wc.wr_id = m->wr_id;
    a->wc.status = st;
    a->wc.opcode = (m->opcode == IBV_WR_SEND || m->opcode == IBV_WR_SEND_WITH_IMM) ? IBV_WC_SEND : IBV_WC_RDMA_WRITE;
    a->wc.qp_num = q->qp.qp_num;
    a->due = now + sim_delay();
    if (q->ack_tail && a->due < q->ack_tail->due) a->due = q->ack_tail->due;
    // Unsignaled successful sends produce no CQE but still free their send queue entry
    if (!m->signaled && st == IBV_WC_SUCCESS) a->wc.wr_id = UINT64_MAX;
    if (q->ack_tail) q->ack_tail->next = a; else q->ack_head = a;
    q->ack_tail = a;
}

static void sim_recv_cqe(struct sim_qp *peer, uint64_t wr_id, enum ibv_wc_opcode op, uint32_t len,
                         int with_imm, uint32_t imm) {
    struct ibv_wc wc;
    memset(&wc, 0, sizeof(wc));
    wc.wr_id = wr_id;
    wc.status = IBV_WC_SUCCESS;
    wc.opcode = op;
    wc.byte_len = len;
    wc.qp_num = peer->qp.qp_num;
    if (with_imm) {
        wc.wc_flags = IBV_WC_WITH_IMM;
        wc.imm_data = imm;
    }
    sim_cq_push(peer->cq, &wc);
}

// The "wire": delivers every message whose time has come, in order per QP
static void sim_pump(void) {
    double now = now_sec();
    for (struct sim_qp *q = sim_qps; q; q = q->next_all) {
        while (q->head && q->head->due <= now) {
            struct sim_msg *m = q->head;
            struct sim_qp *peer = q->peer;
            enum ibv_wc_status st = IBV_WC_SUCCESS;
            int consumes = m->opcode != IBV_WR_RDMA_WRITE;

            if (q->error) {
                st = IBV_WC_WR_FLUSH_ERR;
            } else if (!peer) {
                st = IBV_WC_RETRY_EXC_ERR;
                sim_fail(q, "the peer QP was destroyed while a message was in flight");
            } else if (consumes && peer->rq_count == 0) {
                sim_rnr_naks++;
                if (q->rnr_retry == 7 || m->rnr_tries < q->rnr_retry) {
                    m->rnr_tries++;
                    m->due = now + 10e-6;   // min_rnr_timer = 0.01 ms, then retry
                    break;
                }
                st = IBV_WC_RNR_RETRY_EXC_ERR;
                sim_fail(q, "RNR NAK with no retries left: a message found no posted receive");
            } else {
                const char *data = m->is_inline ? m->inline_data : m->src;
                if (m->opcode == IBV_WR_SEND || m->opcode == IBV_WR_SEND_WITH_IMM) {
                    struct sim_wqe w = peer->rq[peer->rq_head];
                    peer->rq_head = (peer->rq_head + 1) % peer->rq_cap;
                    peer->rq_count--;
                    if (m->len > w.len) {
                        st = IBV_WC_REM_INV_REQ_ERR;
                        sim_fail(q, "SEND larger than the receive buffer");
                    } else {
                        memcpy(w.addr, data, m->len);
                        sim_recv_cqe(peer, w.wr_id, IBV_WC_RECV, m->len, m->opcode == IBV_WR_SEND_WITH_IMM, m->imm);
                    }
                } else if (!sim_find_mr(peer->rank, m->rkey, m->raddr, m->len, IBV_ACCESS_REMOTE_WRITE)) {
                    st = IBV_WC_REM_ACCESS_ERR;
                    sim_fail(q, "RDMA write outside the region of its rkey");
                } else {
                    memcpy((void *)(uintptr_t)m->raddr, data, m->len);
                    if (m->opcode == IBV_WR_RDMA_WRITE_WITH_IMM) {
                        struct sim_wqe w = peer->rq[peer->rq_head];
                        peer->rq_head = (peer->rq_head + 1) % peer->rq_cap;
                        peer->rq_count--;
                        sim_recv_cqe(peer, w.wr_id, IBV_WC_RECV_RDMA_WITH_IMM, m->len, 1, m->imm);
                    }
                }
            }
            sim_queue_ack(q, m, st, now);
            q->head = m->next;
            if (!q->head) q->tail = NULL;
            free(m);
            sim_delivered++;
        }
        while (q->ack_head && q->ack_head->due <= now) {
            struct sim_ack *a = q->ack_head;
            q->ack_head = a->next;
            if (!q->ack_head) q->ack_tail = NULL;
            q->sq_out--;
            if (a->wc.wr_id != UINT64_MAX) sim_cq_push(q->cq, &a->wc);
            free(a);
        }
    }
}

static int sim_post_send(struct ibv_qp *ibqp, struct ibv_send_wr *wr, struct ibv_send_wr **bad_wr) {
    struct sim_qp *q = (struct sim_qp *)ibqp;
    pthread_mutex_lock(&sim_lock);
    for (; wr; wr = wr->next) {
        int err = 0;
        uintptr_t addr = wr->num_sge == 1 ? wr->sg_list[0].addr : 0;
        uint32_t len = wr->num_sge == 1 ? wr->sg_list[0].length : 0;
        int is_inline = (wr->send_flags & IBV_SEND_INLINE) != 0;
        if (q->sq_out >= q->sq_cap) err = ENOMEM;
        else if (wr->num_sge != 1) err = EINVAL;
        else if (is_inline && (int)len > q->max_inline) err = EINVAL;
        else if (!is_inline && !sim_find_mr(q->rank, wr->sg_list[0].lkey, addr, len, 0)) err = EFAULT;
        if (err) {
            fprintf(stderr, "SIM: rank %d %s QP: post_send rejected (%s)\n", q->rank, q->side, strerror(err));
            *bad_wr = wr;
            pthread_mutex_unlock(&sim_lock);
            return err;
        }
        struct sim_msg *m = calloc(1, sizeof(*m));
        m->opcode = wr->opcode;
        m->wr_id = wr->wr_id;
        m->signaled = (wr->send_flags & IBV_SEND_SIGNALED) != 0;
        m->is_inline = is_inline;
        m->len = len;
        if (is_inline) memcpy(m->inline_data, (void *)addr, len);
        else m->src = (const char *)addr;
        m->raddr = wr->wr.rdma.remote_addr;
        m->rkey = wr->wr.rdma.rkey;
        m->imm = wr->imm_data;
        m->due = now_sec() + sim_delay();
        if (q->tail && m->due < q->tail->due) m->due = q->tail->due;
        if (q->tail) q->tail->next = m; else q->head = m;
        q->tail = m;
        q->sq_out++;
    }
    pthread_mutex_unlock(&sim_lock);
    return 0;
}

static int sim_post_recv(struct ibv_qp *ibqp, struct ibv_recv_wr *wr, struct ibv_recv_wr **bad_wr) {
    struct sim_qp *q = (struct sim_qp *)ibqp;
    pthread_mutex_lock(&sim_lock);
    for (; wr; wr = wr->next) {
        int err = 0;
        if (q->rq_count == q->rq_cap) err = ENOMEM;
        else if (wr->num_sge != 1) err = EINVAL;
        else if (!sim_find_mr(q->rank, wr->sg_list[0].lkey, wr->sg_list[0].addr, wr->sg_list[0].length,
                              IBV_ACCESS_LOCAL_WRITE)) err = EFAULT;
        if (err) {
            fprintf(stderr, "SIM: rank %d %s QP: post_recv rejected (%s)\n", q->rank, q->side, strerror(err));
            *bad_wr = wr;
            pthread_mutex_unlock(&sim_lock);
            return err;
        }
        struct sim_wqe *w = &q->rq[(q->rq_head + q->rq_count) % q->rq_cap];
        w->wr_id = wr->wr_id;
        w->addr = (char *)(uintptr_t)wr->sg_list[0].addr;
        w->len = wr->sg_list[0].length;
        q->rq_count++;
    }
    pthread_mutex_unlock(&sim_lock);
    return 0;
}

static int sim_poll_cq(struct ibv_cq *ibcq, int num, struct ibv_wc *wc) {
    struct sim_cq *cq = (struct sim_cq *)ibcq;
    int n = 0;
    pthread_mutex_lock(&sim_lock);
    sim_pump();
    while (n < num && cq->count > 0) {
        wc[n++] = cq->ring[cq->head];
        cq->head = (cq->head + 1) % cq->cap;
        cq->count--;
    }
    pthread_mutex_unlock(&sim_lock);
    if (n == 0) sched_yield();   // let the other ranks run
    return n;
}

// --- Hooks called by connect_process_group / pg_close (PG_SIM) ---------------

static int sim_open_device(struct pg_handle_t *h) {
    h->context = calloc(1, sizeof(struct ibv_context));
    h->pd = calloc(1, sizeof(struct ibv_pd));
    if (!h->context || !h->pd) return -1;
    h->context->ops.poll_cq = sim_poll_cq;
    h->context->ops.post_send = sim_post_send;
    h->context->ops.post_recv = sim_post_recv;
    h->pd->context = h->context;
    h->pd->handle = h->my_rank;   // so registrations know their rank
    return 0;
}

static struct ibv_mr *sim_reg_mr(struct ibv_pd *pd, void *addr, size_t len, int access) {
    if ((access & IBV_ACCESS_REMOTE_WRITE) && !(access & IBV_ACCESS_LOCAL_WRITE)) {
        errno = EINVAL;   // as in verbs: remote write requires local write
        return NULL;
    }
    struct sim_mr *mr = calloc(1, sizeof(*mr));
    if (!mr) return NULL;
    pthread_mutex_lock(&sim_lock);
    mr->mr.context = pd->context;
    mr->mr.pd = pd;
    mr->mr.addr = addr;
    mr->mr.length = len;
    mr->mr.lkey = mr->mr.rkey = sim_key++;
    mr->rank = (int)pd->handle;
    mr->access = access;
    mr->next = sim_mrs;
    sim_mrs = mr;
    pthread_mutex_unlock(&sim_lock);
    return &mr->mr;
}

static int sim_dereg_mr(struct ibv_mr *ibmr) {
    pthread_mutex_lock(&sim_lock);
    for (struct sim_mr **p = &sim_mrs; *p; p = &(*p)->next) {
        if (&(*p)->mr == ibmr) {
            struct sim_mr *mr = *p;
            *p = mr->next;
            free(mr);
            break;
        }
    }
    pthread_mutex_unlock(&sim_lock);
    return 0;
}

static struct sim_qp *sim_new_qp(struct pg_handle_t *h, struct sim_cq *cq, int sq_cap, int rq_cap,
                                 int max_inline, const char *side) {
    struct sim_qp *q = calloc(1, sizeof(*q));
    q->rq = calloc(rq_cap, sizeof(*q->rq));
    q->qp.context = h->context;
    q->rank = h->my_rank;
    q->side = side;
    q->cq = cq;
    q->rq_cap = rq_cap;
    q->sq_cap = sq_cap;
    q->max_inline = max_inline;
    q->rnr_retry = h->cfg.rnr_retry;
    pthread_mutex_lock(&sim_lock);
    q->qp.qp_num = sim_qpn++;
    q->next_all = sim_qps;
    sim_qps = q;
    pthread_mutex_unlock(&sim_lock);
    return q;
}

static int sim_create_queues(struct pg_handle_t *h) {
    struct sim_cq *cq = calloc(1, sizeof(*cq));
    cq->cap = h->sq_cap_left + h->cfg.slots + h->sq_cap_right + h->ctrl_depth + 16;  // as create_queues
    cq->ring = calloc(cq->cap, sizeof(*cq->ring));
    cq->cq.context = h->context;
    h->cq = &cq->cq;
    h->left_qp = &sim_new_qp(h, cq, h->sq_cap_left, h->cfg.slots, LEFT_MAX_INLINE, "left")->qp;
    h->right_qp = &sim_new_qp(h, cq, h->sq_cap_right, h->ctrl_depth, RIGHT_MAX_INLINE, "right")->qp;
    h->max_inline = RIGHT_MAX_INLINE;
    return 0;
}

static int sim_connect_ring(struct pg_handle_t *h) {
    pthread_mutex_lock(&sim_lock);
    sim_handles[h->my_rank] = h;
    pthread_mutex_unlock(&sim_lock);
    pthread_barrier_wait(&sim_bar);   // every rank has its QPs and receives posted

    struct pg_handle_t *right = sim_handles[(h->my_rank + 1) % sim_n];
    struct pg_handle_t *left = sim_handles[(h->my_rank - 1 + sim_n) % sim_n];
    pthread_mutex_lock(&sim_lock);
    ((struct sim_qp *)h->right_qp)->peer = (struct sim_qp *)right->left_qp;
    ((struct sim_qp *)h->left_qp)->peer = (struct sim_qp *)left->right_qp;
    pthread_mutex_unlock(&sim_lock);
    if (memcmp(&h->cfg, &right->cfg, sizeof(h->cfg)) != 0 || memcmp(&h->cfg, &left->cfg, sizeof(h->cfg)) != 0) {
        fprintf(stderr, "SIM: rank %d: PG_* settings differ between ranks\n", h->my_rank);
        return -1;
    }
    h->path_mtu = IBV_MTU_4096;
    pthread_barrier_wait(&sim_bar);   // READY
    h->connected = 1;
    return 0;
}

static int sim_barrier(struct pg_handle_t *h) {
    (void)h;
    pthread_barrier_wait(&sim_bar);
    return 0;
}

static void sim_free_qp(struct sim_qp *q) {
    for (struct sim_qp **p = &sim_qps; *p; p = &(*p)->next_all) {
        if (*p == q) {
            *p = q->next_all;
            break;
        }
    }
    for (struct sim_qp *o = sim_qps; o; o = o->next_all)
        if (o->peer == q) o->peer = NULL;
    while (q->head) {
        struct sim_msg *m = q->head;
        q->head = m->next;
        free(m);
    }
    while (q->ack_head) {
        struct sim_ack *a = q->ack_head;
        q->ack_head = a->next;
        free(a);
    }
    free(q->rq);
    free(q);
}

static void sim_destroy(struct pg_handle_t *h) {
    pthread_mutex_lock(&sim_lock);
    if (h->left_qp) sim_free_qp((struct sim_qp *)h->left_qp);
    if (h->right_qp) sim_free_qp((struct sim_qp *)h->right_qp);
    if (h->cq) {
        free(((struct sim_cq *)h->cq)->ring);
        free(h->cq);
    }
    h->left_qp = h->right_qp = NULL;
    h->cq = NULL;
    pthread_mutex_unlock(&sim_lock);
    // The pool MRs are deregistered after this; sim_dereg_mr needs neither PD nor context
    free(h->pd);
    free(h->context);
    h->pd = NULL;
    h->context = NULL;
}

// --- Driver ---------------------------------------------------------------------

struct rank_args {
    pthread_t thread;
    int       argc;
    char     *argv[10];
    char      text[10][64];
    int       rc;
};

static void *rank_thread(void *p) {
    struct rank_args *a = p;
    a->rc = pg_real_main(a->argc, a->argv);
    return NULL;
}

int main(int argc, char *argv[]) {
    int n = argc > 1 ? atoi(argv[1]) : 4;
    const char *mode = argc > 2 ? argv[2] : "test";
    const char *protos = argc > 3 ? argv[3] : "eager,rndv";
    const char *max_bytes = argc > 4 ? argv[4] : "1048576";
    if (n < 2 || n > 64) {
        fprintf(stderr, "Usage: %s [num_nodes 2..64] [demo|test|bench|all] [protocols] [max_bytes]\n", argv[0]);
        return 1;
    }
    const char *d = getenv("SIM_DELAY_US");
    if (d) sim_max_delay = atof(d) * 1e-6;
    sim_rng = env_u32("SIM_SEED", 4242);

    sim_n = n;
    sim_handles = calloc(n, sizeof(*sim_handles));
    pthread_barrier_init(&sim_bar, NULL, n);
    struct rank_args *ranks = calloc(n, sizeof(*ranks));
    const char *args[] = { "sim", NULL, "sim", NULL, "0", max_bytes, "sim_results.csv", protos, mode };
    for (int r = 0; r < n; r++) {
        ranks[r].argc = 9;
        for (int i = 0; i < 9; i++) {
            const char *s = args[i];
            char num[16];
            if (i == 1) { snprintf(num, sizeof(num), "%d", r); s = num; }
            if (i == 3) { snprintf(num, sizeof(num), "%d", n); s = num; }
            snprintf(ranks[r].text[i], sizeof(ranks[r].text[i]), "%s", s);
            ranks[r].argv[i] = ranks[r].text[i];
        }
        pthread_create(&ranks[r].thread, NULL, rank_thread, &ranks[r]);
    }
    int failed = 0;
    for (int r = 0; r < n; r++) {
        pthread_join(ranks[r].thread, NULL);
        if (ranks[r].rc != 0) failed = 1;
    }
    printf("SIM: %d ranks, %" PRIu64 " messages delivered, %" PRIu64 " RNR NAKs, %s\n",
           n, sim_delivered, sim_rnr_naks, failed || sim_fatal ? "FAILED" : "ok");
    return failed || sim_fatal;
}

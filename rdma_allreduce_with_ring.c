#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <infiniband/verbs.h>
#include <stdint.h> // For fixed-width integers
#include <endian.h> // For htobe64, be64toh
#include <time.h>   // For time-related functions (though not directly used by print_data, it was requested)

#define MAX_BUF_SIZE (1024 * 1024) // 1MB buffer for All-Reduce

// --- Configuration Macros ---
#define CQ_DEPTH            200
#define MAX_SEND_WR         100
#define MAX_RECV_WR         100
#define MAX_SEND_SGE        1
#define MAX_RECV_SGE        1
#define MIN_RNR_TIMER       12
#define MAX_DEST_RD_ATOMIC  1
#define MAX_RD_ATOMIC       1
#define QP_TIMEOUT          14
#define RETRY_CNT           7
#define RNR_RETRY           7
// ----------------------------

// -----------------------------------------------------------------------------
// 1. Core Structures
// -----------------------------------------------------------------------------

// The Out-Of-Band (TCP) data we need to exchange to connect QPs.
// This struct is exchanged as-is, so we use fixed-size types and
// handle endianness conversion to make it portable.
// The fields are ordered to minimize padding.
struct rdma_dest {
    uint64_t vaddr;      // Buffer virtual address
    uint32_t rkey;       // Remote key
    uint32_t qpn;        // Queue pair number
    uint32_t psn;        // Packet sequence number
    uint16_t lid;        // LID of the IB port
};

// The hidden implementation of your void **pg_handle
struct pg_handle_t {
    struct ibv_context *context;
    struct ibv_pd      *pd;
    struct ibv_mr      *mr;
    struct ibv_cq      *cq;
    
    // Ring topology requires 2 QPs per node
    struct ibv_qp      *left_qp;  // Receives from the left neighbor
    struct ibv_qp      *right_qp; // Sends to the right neighbor
    
    void               *buf;      // The RDMA memory buffer
    
    struct rdma_dest   left_remote_dest;
    struct rdma_dest   right_remote_dest;

    // OOB TCP sockets to neighbors (kept open until QPs are RTS and pairwise sync completes)
    int left_sockfd;
    int right_sockfd;

    // --- New fields for All-Reduce ---
    int my_rank;
    int num_nodes;
};

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


// -----------------------------------------------------------------------------
// 2. Verbs Setup Functions
// -----------------------------------------------------------------------------

static struct ibv_qp* create_qp(struct pg_handle_t *handle) {
    struct ibv_qp_init_attr attr = {
        .send_cq = handle->cq,
        .recv_cq = handle->cq,
        .cap     = {
            .max_send_wr  = MAX_SEND_WR,
            .max_recv_wr  = MAX_RECV_WR,
            .max_send_sge = MAX_SEND_SGE,
            .max_recv_sge = MAX_RECV_SGE,
        },
        .qp_type = IBV_QPT_RC // Reliable Connection
    };
    return ibv_create_qp(handle->pd, &attr);
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

static int modify_qp_to_rtr(struct ibv_qp *qp, struct rdma_dest *remote, int ib_port) { //ready to recive
    struct ibv_qp_attr attr = {
        .qp_state           = IBV_QPS_RTR,
        .path_mtu           = IBV_MTU_1024,
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

static int modify_qp_to_rts(struct ibv_qp *qp, int my_psn) { //ready to send
    struct ibv_qp_attr attr = {
        .qp_state       = IBV_QPS_RTS,
        .timeout        = QP_TIMEOUT,
        .retry_cnt      = RETRY_CNT,
        .rnr_retry      = RNR_RETRY,
        .sq_psn         = my_psn,
        .max_rd_atomic  = MAX_RD_ATOMIC
    };
    return ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
}


// -----------------------------------------------------------------------------
// 3. TCP Out-Of-Band Exchange
// -----------------------------------------------------------------------------

// Accepts connection from the LEFT neighbor
// Accepts connection from the LEFT neighbor
// On success returns 0 and stores an open connected socket fd in *connfd_out (DO NOT close it)
static int exchange_with_left(struct pg_handle_t *handle, uint16_t my_lid, uint32_t my_psn, int port, int *connfd_out) {
    int sockfd, connfd;
    struct sockaddr_in serv_addr;
    ssize_t bytes;

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

    // 1. Read Left's coordinates
    struct rdma_dest remote_dest_net;
    bytes = read(connfd, &remote_dest_net, sizeof(remote_dest_net));
    if (bytes != sizeof(remote_dest_net)) {
        fprintf(stderr, "Error: Failed to read from left neighbor (read returned %zd).\n", bytes);
        close(connfd);
        return -1;
    }

    // Convert from network to host byte order
    handle->left_remote_dest.vaddr = be64toh(remote_dest_net.vaddr);
    handle->left_remote_dest.rkey = ntohl(remote_dest_net.rkey);
    handle->left_remote_dest.qpn = ntohl(remote_dest_net.qpn);
    handle->left_remote_dest.psn = ntohl(remote_dest_net.psn);
    handle->left_remote_dest.lid = ntohs(remote_dest_net.lid);

    // 2. Send My coordinates (specifically my left_qp so they can write to me)
    struct rdma_dest my_dest_net;
    my_dest_net.vaddr = htobe64((uint64_t)handle->buf);
    my_dest_net.rkey = htonl(handle->mr->rkey);
    my_dest_net.qpn = htonl(handle->left_qp->qp_num);
    my_dest_net.psn = htonl(my_psn);
    my_dest_net.lid = htons(my_lid);

    bytes = write(connfd, &my_dest_net, sizeof(my_dest_net));
    if (bytes != sizeof(my_dest_net)) {
        fprintf(stderr, "Error: Failed to write to left neighbor (write returned %zd).\n", bytes);
        close(connfd);
        return -1;
    }

    // Leave connfd open for the pairwise READY sync later
    *connfd_out = connfd;
    return 0;
}

// Initiates connection to the RIGHT neighbor
// On success returns 0 and stores an open connected socket fd in *sockfd_out (DO NOT close it)
static int exchange_with_right(struct pg_handle_t *handle, const char *right_ip, uint16_t my_lid, uint32_t my_psn, int port, int *sockfd_out) {
    int sockfd;
    ssize_t bytes;
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

    // 1. Send My coordinates (specifically my right_qp so I can read their acks)
    struct rdma_dest my_dest_net;
    my_dest_net.vaddr = htobe64((uint64_t)handle->buf);
    my_dest_net.rkey = htonl(handle->mr->rkey);
    my_dest_net.qpn = htonl(handle->right_qp->qp_num);
    my_dest_net.psn = htonl(my_psn);
    my_dest_net.lid = htons(my_lid);

    bytes = write(sockfd, &my_dest_net, sizeof(my_dest_net));
    if (bytes != sizeof(my_dest_net)) {
        fprintf(stderr, "Error: Failed to write to right neighbor (write returned %zd).\n", bytes);
        close(sockfd);
        return -1;
    }

    // 2. Read Right's coordinates
    struct rdma_dest remote_dest_net;
    bytes = read(sockfd, &remote_dest_net, sizeof(remote_dest_net));
    if (bytes != sizeof(remote_dest_net)) {
        fprintf(stderr, "Error: Failed to read from right neighbor (read returned %zd).\n", bytes);
        close(sockfd);
        return -1;
    }

    // Convert from network to host byte order
    handle->right_remote_dest.vaddr = be64toh(remote_dest_net.vaddr);
    handle->right_remote_dest.rkey = ntohl(remote_dest_net.rkey);
    handle->right_remote_dest.qpn = ntohl(remote_dest_net.qpn);
    handle->right_remote_dest.psn = ntohl(remote_dest_net.psn);
    handle->right_remote_dest.lid = ntohs(remote_dest_net.lid);

    // Leave sockfd open for the pairwise READY sync later
    *sockfd_out = sockfd;
    return 0;
}


// -----------------------------------------------------------------------------
// 4. Exercise API Implementation
// -----------------------------------------------------------------------------

int pg_close(void *pg_handle); // Forward declaration

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

    uint32_t my_psn = rand() & 0xffffff;
    struct ibv_device **dev_list = NULL;
    int rc = 0;
    int num_devices = 0;
    int ib_port = -1;
    struct ibv_port_attr portinfo;

    // 1. Device and Memory Setup
    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list || num_devices == 0) {
        fprintf(stderr, "Error: Failed to get IB device list.\n");
        rc = -1;
        goto error;
    }
    // look for an infiniband device
    for (int i = 0; i < num_devices; i++) {
        handle->context = ibv_open_device(dev_list[i]);
        if (!handle->context) continue;

        struct ibv_device_attr device_attr;
        if (ibv_query_device(handle->context, &device_attr) != 0) {
            ibv_close_device(handle->context);
            handle->context = NULL;
            continue;
        }
        // look for an available port
        for (uint8_t p = 1; p <= device_attr.phys_port_cnt; p++) {
            if (ibv_query_port(handle->context, p, &portinfo) == 0) {
                if (portinfo.state == IBV_PORT_ACTIVE) {
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

    if (!handle->context || ib_port == -1) {
        fprintf(stderr, "Error: Failed to find an active IB device and port.\n");
        rc = -1;
        goto error;
    }
    // allocate memory to the protection domain
    handle->pd = ibv_alloc_pd(handle->context);
    if (!handle->pd) {
        fprintf(stderr, "Error: Failed to allocate PD.\n");
        rc = -1;
        goto error;
    }
    // allocate memory to the process' buffer
    handle->buf = malloc(MAX_BUF_SIZE);
    if (!handle->buf) {
        fprintf(stderr, "Error: Failed to allocate memory for buffer.\n");
        rc = -1;
        goto error;
    }
    memset(handle->buf, 0, MAX_BUF_SIZE);
    // allow for RDMA write and read operations to the buffer by local and remote precesses
    handle->mr = ibv_reg_mr(handle->pd, handle->buf, MAX_BUF_SIZE, 
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!handle->mr) {
        fprintf(stderr, "Error: Failed to register MR.\n");
        rc = -1;
        goto error;
    }
    // create a copletion queue
    handle->cq = ibv_create_cq(handle->context, CQ_DEPTH, NULL, NULL, 0);
    if (!handle->cq) {
        fprintf(stderr, "Error: Failed to create CQ.\n");
        rc = -1;
        goto error;
    }

    // 2. Create the Two Ring QPs, left and right
    handle->left_qp = create_qp(handle);
    if (!handle->left_qp) {
        fprintf(stderr, "Error: Failed to create left QP.\n");
        rc = -1;
        goto error;
    }

    handle->right_qp = create_qp(handle);
    if (!handle->right_qp) {
        fprintf(stderr, "Error: Failed to create right QP.\n");
        rc = -1;
        goto error;
    }
    // initialize left and right qps
    if (modify_qp_to_init(handle->left_qp, ib_port) != 0) {
        fprintf(stderr, "Error: Failed to modify left QP to INIT.\n");
        rc = -1;
        goto error;
    }

    if (modify_qp_to_init(handle->right_qp, ib_port) != 0) {
        fprintf(stderr, "Error: Failed to modify right QP to INIT.\n");
        rc = -1;
        goto error;
    }

    uint16_t my_lid = portinfo.lid;

    // 3. Avoid Deadlocks using Rank (Even listens first, Odd connects first)
    if (my_rank % 2 == 0) {
        if (exchange_with_left(handle, my_lid, my_psn, oob_port, &handle->left_sockfd) != 0) {
            fprintf(stderr, "Rank %d: OOB exchange with left neighbor failed.\n", my_rank);
            rc = -1;
            goto error;
        }
        if (exchange_with_right(handle, servername, my_lid, my_psn, oob_port, &handle->right_sockfd) != 0) {
            fprintf(stderr, "Rank %d: OOB exchange with right neighbor failed.\n", my_rank);
            rc = -1;
            goto error;
        }
    } else {
        if (exchange_with_right(handle, servername, my_lid, my_psn, oob_port, &handle->right_sockfd) != 0) {
            fprintf(stderr, "Rank %d: OOB exchange with right neighbor failed.\n", my_rank);
            rc = -1;
            goto error;
        }
        if (exchange_with_left(handle, my_lid, my_psn, oob_port, &handle->left_sockfd) != 0) {
            fprintf(stderr, "Rank %d: OOB exchange with left neighbor failed.\n", my_rank);
            rc = -1;
            goto error;
        }
    }

    // 4. Plug in the connections (INIT -> RTR -> RTS)
    if (modify_qp_to_rtr(handle->left_qp, &handle->left_remote_dest, ib_port) != 0) {
        fprintf(stderr, "Error: Failed to modify left QP to RTR.\n");
        rc = -1;
        goto error;
    }
    if (modify_qp_to_rts(handle->left_qp, my_psn) != 0) {
        fprintf(stderr, "Error: Failed to modify left QP to RTS.\n");
        rc = -1;
        goto error;
    }
    if (modify_qp_to_rtr(handle->right_qp, &handle->right_remote_dest, ib_port) != 0) {
        fprintf(stderr, "Error: Failed to modify right QP to RTR.\n");
        rc = -1;
        goto error;
    }
    if (modify_qp_to_rts(handle->right_qp, my_psn) != 0) {
        fprintf(stderr, "Error: Failed to modify right QP to RTS.\n");
        rc = -1;
        goto error;
    }

    // --- PAIRWISE TCP SYNC (Hardware-ready barrier) ---
    // Each node writes a 1-byte READY to both neighbors and then reads a 1-byte READY from both.
    {
        char ready = 1;
        char peer_ready = 0;
        ssize_t n;

        // Send READY to left and right neighbors (non-blocking from protocol perspective).
        n = write(handle->left_sockfd, &ready, 1);
        if (n != 1) {
            fprintf(stderr, "Rank %d: Failed to send READY to LEFT neighbor (wrote %zd).\n", my_rank, n);
            rc = -1;
            goto error;
        }
        n = write(handle->right_sockfd, &ready, 1);
        if (n != 1) {
            fprintf(stderr, "Rank %d: Failed to send READY to RIGHT neighbor (wrote %zd).\n", my_rank, n);
            rc = -1;
            goto error;
        }

        // Read READY from left neighbor
        n = read(handle->left_sockfd, &peer_ready, 1);
        if (n != 1) {
            fprintf(stderr, "Rank %d: Failed to read READY from LEFT neighbor (read %zd).\n", my_rank, n);
            rc = -1;
            goto error;
        }
        // Read READY from right neighbor
        n = read(handle->right_sockfd, &peer_ready, 1);
        if (n != 1) {
            fprintf(stderr, "Rank %d: Failed to read READY from RIGHT neighbor (read %zd).\n", my_rank, n);
            rc = -1;
            goto error;
        }

        // Close the OOB sockets after successful handshake
        close(handle->left_sockfd);
        close(handle->right_sockfd);
        handle->left_sockfd = -1;
        handle->right_sockfd = -1;
    }

    printf("Rank %d of %d successfully formed the RDMA Ring!\n", my_rank, num_nodes);
    ibv_free_device_list(dev_list);
    return 0;

error:
    if (dev_list) {
        ibv_free_device_list(dev_list);
    }
    pg_close(handle); 
    *pg_handle = NULL;
    return rc;
}

int pg_close(void *pg_handle) {
    struct pg_handle_t *handle = (struct pg_handle_t *)pg_handle;
    if (!handle) {
        return 0;
    }
    if (handle->left_sockfd >= 0) close(handle->left_sockfd);
    if (handle->right_sockfd >= 0) close(handle->right_sockfd);
    if (handle->left_qp) ibv_destroy_qp(handle->left_qp);
    if (handle->right_qp) ibv_destroy_qp(handle->right_qp);
    if (handle->cq) ibv_destroy_cq(handle->cq);
    if (handle->mr) ibv_dereg_mr(handle->mr);
    if (handle->pd) ibv_dealloc_pd(handle->pd);
    if (handle->context) ibv_close_device(handle->context);
    free(handle->buf); // free(NULL) is safe
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

static int poll_completion(struct ibv_cq *cq) {
    struct ibv_wc wc;
    int num_comp;
    do {
        num_comp = ibv_poll_cq(cq, 1, &wc);
    } while (num_comp == 0);

    if (num_comp < 0) {
        fprintf(stderr, "Error: ibv_poll_cq failed.\n");
        return -1;
    }
    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "Error: Work completion failed with status %s.\n", ibv_wc_status_str(wc.status));
        return -1;
    }
    return 0;
}

static int post_recv(struct pg_handle_t *handle, uint64_t buf_addr, uint32_t size) {
    struct ibv_recv_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    sge.addr = buf_addr;
    sge.length = size;
    sge.lkey = handle->mr->lkey;

    wr.wr_id = (uint64_t)handle; // Use handle as WR ID for identification
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    return ibv_post_recv(handle->left_qp, &wr, &bad_wr);
}

static int post_send(struct pg_handle_t *handle, uint64_t buf_addr, uint32_t size) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    sge.addr = buf_addr;
    sge.length = size;
    sge.lkey = handle->mr->lkey;

    wr.wr_id = (uint64_t)handle;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    return ibv_post_send(handle->right_qp, &wr, &bad_wr);
}

static int post_rdma_write(struct pg_handle_t *handle, uint64_t local_addr, uint64_t remote_addr, uint32_t size) {
    struct ibv_send_wr wr, *bad_wr = NULL;
    struct ibv_sge sge;

    // Local buffer details
    sge.addr = local_addr;
    sge.length = size;
    sge.lkey = handle->mr->lkey;

    // Describe the send request
    wr.wr_id = (uint64_t)handle;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED; // Request a completion event

    // Remote buffer details
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = handle->right_remote_dest.rkey;

    return ibv_post_send(handle->right_qp, &wr, &bad_wr);
}

static void reduce_chunk(void *local_chunk, void *recv_chunk, int chunk_elems, DATATYPE datatype, OPERATION op) {
    if (datatype == TYPE_INT) {
        int *l = (int *)local_chunk;
        int *r = (int *)recv_chunk;
        if (op == OP_SUM) for (int i = 0; i < chunk_elems; i++) l[i] += r[i];
        else if (op == OP_MAX) for (int i = 0; i < chunk_elems; i++) l[i] = (l[i] > r[i]) ? l[i] : r[i];
        else if (op == OP_MIN) for (int i = 0; i < chunk_elems; i++) l[i] = (l[i] < r[i]) ? l[i] : r[i];
    } else if (datatype == TYPE_FLOAT) {
        float *l = (float *)local_chunk;
        float *r = (float *)recv_chunk;
        if (op == OP_SUM) for (int i = 0; i < chunk_elems; i++) l[i] += r[i];
        else if (op == OP_MAX) for (int i = 0; i < chunk_elems; i++) l[i] = (l[i] > r[i]) ? l[i] : r[i];
        else if (op == OP_MIN) for (int i = 0; i < chunk_elems; i++) l[i] = (l[i] < r[i]) ? l[i] : r[i];
    } else if (datatype == TYPE_DOUBLE) {
        double *l = (double *)local_chunk;
        double *r = (double *)recv_chunk;
        if (op == OP_SUM) for (int i = 0; i < chunk_elems; i++) l[i] += r[i];
        else if (op == OP_MAX) for (int i = 0; i < chunk_elems; i++) l[i] = (l[i] > r[i]) ? l[i] : r[i];
        else if (op == OP_MIN) for (int i = 0; i < chunk_elems; i++) l[i] = (l[i] < r[i]) ? l[i] : r[i];
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

// -----------------------------------------------------------------------------

int pg_all_reduce(void *sendbuf, void *recvbuf, int count, DATATYPE datatype, OPERATION op, void *pg_handle) {
    struct pg_handle_t *handle = (struct pg_handle_t *)pg_handle;
    int my_rank = handle->my_rank;
    int num_nodes = handle->num_nodes;

    size_t dt_size = get_datatype_size(datatype);
    if (dt_size == 0) {
        fprintf(stderr, "Error: Invalid datatype specified.\n");
        return -1;
    }

    if (count % num_nodes != 0) {
        fprintf(stderr, "Error: Element count must be divisible by the number of nodes.\n");
        return -1;
    }

    int chunk_elems = count / num_nodes;
    int chunk_bytes = chunk_elems * dt_size;
    size_t total_bytes = count * dt_size;

    // We need space for the main data, TWO receive scratch buffers, and a 1-byte sync buffer
    if (total_bytes + (2 * chunk_bytes) + 1 > MAX_BUF_SIZE) {
        fprintf(stderr, "Error: Not enough buffer space for double-buffered operation.\n");
        return -1;
    }

    // 1. Copy user data into our registered buffer
    memcpy(handle->buf, sendbuf, total_bytes);

    // --- PHASE 1: REDUCE-SCATTER (Pipelined Double-Buffering) ---
    void *reduce_scratch_buf[2];
    reduce_scratch_buf[0] = (char*)handle->buf + total_bytes;
    reduce_scratch_buf[1] = (char*)handle->buf + total_bytes + chunk_bytes;

    // --- Prime the Pipeline (Step 0) ---
    int send_chunk_idx = my_rank;
    int recv_chunk_idx = (my_rank - 1 + num_nodes) % num_nodes;

    // Post first receive into scratch buffer 0
    if (post_recv(handle, (uint64_t)reduce_scratch_buf[0], chunk_bytes) != 0) {
        fprintf(stderr, "Error: Failed to post initial receive.\n");
        return -1;
    }
    // Post first send
    uint64_t send_buf_addr = (uint64_t)handle->buf + (send_chunk_idx * chunk_bytes);
    if (post_send(handle, send_buf_addr, chunk_bytes) != 0) {
        fprintf(stderr, "Error: Failed to post initial send.\n");
        return -1;
    }

    // --- The Pipeline Loop (Steps 1 to num_nodes - 1) ---
    for (int step = 1; step < num_nodes; step++) {
        // A. Wait for the PREVIOUS step's send and receive to complete
        if (poll_completion(handle->cq) != 0) return -1; // Previous Recv
        if (poll_completion(handle->cq) != 0) return -1; // Previous Send

        // B. Post the NEXT receive early so hardware works in the background
        if (step < num_nodes - 1) {
            if (post_recv(handle, (uint64_t)reduce_scratch_buf[step % 2], chunk_bytes) != 0) {
                fprintf(stderr, "Error: Failed to post receive in step %d.\n", step);
                return -1;
            }
        }

        // C. Compute reduction on the data that just arrived from the PREVIOUS step
        recv_chunk_idx = (my_rank - step + num_nodes) % num_nodes;
        void *local_chunk_ptr = (char*)handle->buf + (recv_chunk_idx * chunk_bytes);
        reduce_chunk(local_chunk_ptr, reduce_scratch_buf[(step - 1) % 2], chunk_elems, datatype, op);

        // D. NOW post the send for the chunk we *just finished reducing*
        if (step < num_nodes - 1) {
            send_chunk_idx = (my_rank - step + num_nodes) % num_nodes;
            send_buf_addr = (uint64_t)handle->buf + (send_chunk_idx * chunk_bytes);
            if (post_send(handle, send_buf_addr, chunk_bytes) != 0) {
                fprintf(stderr, "Error: Failed to post send in step %d.\n", step);
                return -1;
            }
        }
    }

    // --- PHASE 2: ALL-GATHER (using RDMA WRITE) ---
    // The sync buffer must sit safely AFTER the two scratch buffers
    void *sync_buf = (char*)reduce_scratch_buf[1] + chunk_bytes;

    for (int i = 0; i < num_nodes - 1; i++) {
        int send_chunk_idx_ag = (my_rank - i + 1 + num_nodes) % num_nodes;

        // Post a receive for the 1-byte synchronization signal
        if (post_recv(handle, (uint64_t)sync_buf, 1) != 0) {
            fprintf(stderr, "Error: Failed to post sync receive for All-Gather.\n");
            return -1;
        }

        // RDMA Write the final data chunk directly
        uint64_t local_vaddr = (uint64_t)handle->buf + (send_chunk_idx_ag * chunk_bytes);
        uint64_t remote_vaddr = handle->right_remote_dest.vaddr + (send_chunk_idx_ag * chunk_bytes);
        if (post_rdma_write(handle, local_vaddr, remote_vaddr, chunk_bytes) != 0) {
            fprintf(stderr, "Error: Failed to post RDMA Write for All-Gather.\n");
            return -1;
        }

        // Send a 1-byte synchronization signal
        if (post_send(handle, (uint64_t)sync_buf, 1) != 0) {
            fprintf(stderr, "Error: Failed to post sync send for All-Gather.\n");
            return -1;
        }

        // Wait for all three operations to complete
        if (poll_completion(handle->cq) != 0) return -1; // RDMA Write completion
        if (poll_completion(handle->cq) != 0) return -1; // Sync receive completion
        if (poll_completion(handle->cq) != 0) return -1; // Sync send completion
    }

    // Copy the final, complete buffer to the user's output buffer
    memcpy(recvbuf, handle->buf, total_bytes);
    
    return 0;
}


// -----------------------------------------------------------------------------
// 5. Test Main
// -----------------------------------------------------------------------------
int main(int argc, char *argv[]) {
    if (argc < 4 || argc > 5) {
        printf("Usage: %s <my_rank> <right_neighbor_ip> <num_nodes> [oob_port]\n", argv[0]);
        return 1;
    }

    int my_rank = atoi(argv[1]);
    char *right_ip = argv[2];
    int num_nodes = atoi(argv[3]);
    int oob_port = (argc == 5) ? atoi(argv[4]) : 18515;
    void *handle = NULL;

    // Build the Ring
    if (connect_process_group(right_ip, my_rank, num_nodes, &handle, oob_port) != 0) {
        fprintf(stderr, "Failed to connect process group. Exiting.\n");
        return 1;
    }

    // --- All-Reduce Test Setup ---
    srand(time(NULL) + my_rank); // Seed random number generator uniquely per node

    const int total_elements = 20;
    const int rows = 2;
    const int cols = 10;

    if (total_elements % num_nodes != 0) {
        fprintf(stderr, "Error: Total elements (%d) must be divisible by num_nodes (%d) for this test.\n", total_elements, num_nodes);
        pg_close(handle);
        return 1;
    }

    int *sendbuf = (int *)malloc(total_elements * sizeof(int));
    int *recvbuf = (int *)malloc(total_elements * sizeof(int));

    if (!sendbuf || !recvbuf) {
        fprintf(stderr, "Error: Failed to allocate sendbuf or recvbuf.\n");
        free(sendbuf);
        free(recvbuf);
        pg_close(handle);
        return 1;
    }

    // Fill sendbuf with random integers
    for (int i = 0; i < total_elements; ++i) {
        sendbuf[i] = rand() % 100; // Random integers between 0 and 99
    }

    // Initialize recvbuf to all zeros
    memset(recvbuf, 0, total_elements * sizeof(int));

    // Print data before the operation
    print_data(my_rank, "BEFORE ALL-REDUCE", sendbuf, rows, cols);

    // Call pg_all_reduce
    if (pg_all_reduce(sendbuf, recvbuf, total_elements, TYPE_INT, OP_SUM, handle) != 0) {
        fprintf(stderr, "Error: pg_all_reduce failed.\n");
        free(sendbuf);
        free(recvbuf);
        pg_close(handle);
        return 1;
    }

    // Print data after the operation
    print_data(my_rank, "AFTER ALL-REDUCE", recvbuf, rows, cols);

    // Free allocated arrays
    free(sendbuf);
    free(recvbuf);
    // --- End All-Reduce Test Setup ---

    // Teardown
    pg_close(handle);
    return 0;
}
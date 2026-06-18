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

#define OOB_PORT 18515
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
static int exchange_with_left(struct pg_handle_t *handle, uint16_t my_lid, uint32_t my_psn, int port) {
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

    close(connfd);
    return 0;
}

// Initiates connection to the RIGHT neighbor
static int exchange_with_right(struct pg_handle_t *handle, const char *right_ip, uint16_t my_lid, uint32_t my_psn, int port) {
    int sockfd;
    struct sockaddr_in serv_addr;
    ssize_t bytes;

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    if (inet_pton(AF_INET, right_ip, &serv_addr.sin_addr) <= 0) {
        fprintf(stderr, "Error: Invalid right neighbor IP address '%s'.\n", right_ip);
        return -1;
    }

    printf("Connecting to RIGHT neighbor at %s:%d...\n", right_ip, port);
    
    // Retry connection until the other side is ready
    while (1) {
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            perror("socket");
            return -1; // Non-recoverable
        }
        if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) == 0) {
            break; // Success
        }
        close(sockfd);
        usleep(100000); // Retry every 100ms
    }

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

    close(sockfd);
    return 0;
}


// -----------------------------------------------------------------------------
// 4. Exercise API Implementation
// -----------------------------------------------------------------------------

int pg_close(void *pg_handle); // Forward declaration

/* * 'servername' acts as the IP address of your right-hand neighbor.
 */
int connect_process_group(char *servername, int my_rank, int num_nodes, void **pg_handle) {
    struct pg_handle_t *handle = calloc(1, sizeof(struct pg_handle_t));
    if (!handle) {
        fprintf(stderr, "Error: Could not allocate memory for pg_handle_t\n");
        return -1;
    }
    *pg_handle = handle;
    handle->my_rank = my_rank;
    handle->num_nodes = num_nodes;

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

    for (int i = 0; i < num_devices; i++) {
        handle->context = ibv_open_device(dev_list[i]);
        if (!handle->context) continue;

        struct ibv_device_attr device_attr;
        if (ibv_query_device(handle->context, &device_attr) != 0) {
            ibv_close_device(handle->context);
            handle->context = NULL;
            continue;
        }

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

    handle->pd = ibv_alloc_pd(handle->context);
    if (!handle->pd) {
        fprintf(stderr, "Error: Failed to allocate PD.\n");
        rc = -1;
        goto error;
    }
    
    handle->buf = malloc(MAX_BUF_SIZE);
    if (!handle->buf) {
        fprintf(stderr, "Error: Failed to allocate memory for buffer.\n");
        rc = -1;
        goto error;
    }
    memset(handle->buf, 0, MAX_BUF_SIZE);

    handle->mr = ibv_reg_mr(handle->pd, handle->buf, MAX_BUF_SIZE, 
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!handle->mr) {
        fprintf(stderr, "Error: Failed to register MR.\n");
        rc = -1;
        goto error;
    }
    
    handle->cq = ibv_create_cq(handle->context, CQ_DEPTH, NULL, NULL, 0);
    if (!handle->cq) {
        fprintf(stderr, "Error: Failed to create CQ.\n");
        rc = -1;
        goto error;
    }

    // 2. Create the Two Ring QPs
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
        if (exchange_with_left(handle, my_lid, my_psn, OOB_PORT) != 0) {
            fprintf(stderr, "Rank %d: OOB exchange with left neighbor failed.\n", my_rank);
            rc = -1;
            goto error;
        }
        if (exchange_with_right(handle, servername, my_lid, my_psn, OOB_PORT) != 0) {
            fprintf(stderr, "Rank %d: OOB exchange with right neighbor failed.\n", my_rank);
            rc = -1;
            goto error;
        }
    } else {
        if (exchange_with_right(handle, servername, my_lid, my_psn, OOB_PORT) != 0) {
            fprintf(stderr, "Rank %d: OOB exchange with right neighbor failed.\n", my_rank);
            rc = -1;
            goto error;
        }
        if (exchange_with_left(handle, my_lid, my_psn, OOB_PORT) != 0) {
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

// Helper function to get the size of the datatype
static size_t get_datatype_size(DATATYPE datatype) {
    switch (datatype) {
        case TYPE_INT: return sizeof(int);
        case TYPE_FLOAT: return sizeof(float);
        case TYPE_DOUBLE: return sizeof(double);
        default: return 0;
    }
}

// Helper to poll for a completion
static int poll_completion(struct ibv_cq *cq) {
    struct ibv_wc wc;
    int num_comp;

    do {
        num_comp = ibv_poll_cq(cq, 1, &wc);
    } while (num_comp == 0);

    if (num_comp < 0) {
        fprintf(stderr, "poll_cq failed\n");
        return -1;
    }

    if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                ibv_wc_status_str(wc.status), wc.status, (int)wc.wr_id);
        return -1;
    }

    return 0;
}


int pg_all_reduce(void *sendbuf, void *recvbuf, int count, DATATYPE datatype, OPERATION op, void *pg_handle) {
    struct pg_handle_t *handle = (struct pg_handle_t *)pg_handle;
    
    // TODO: Implement Ring All-Reduce Logic here!
    // Phase 1: Reduce-Scatter (Send to right_qp, receive from left_qp, compute OP)
    // Phase 2: All-Gather (Send to right_qp, receive from left_qp, using RDMA WRITE zero-copy)
    
    return 0;
}


// -----------------------------------------------------------------------------
// 5. Test Main
// -----------------------------------------------------------------------------
int main(int argc, char *argv[]) {
    if (argc != 4) {
        printf("Usage: %s <my_rank> <right_neighbor_ip> <num_nodes>\n", argv[0]);
        return 1;
    }

    int my_rank = atoi(argv[1]);
    char *right_ip = argv[2];
    int num_nodes = atoi(argv[3]);
    void *handle = NULL;

    // Build the Ring
    if (connect_process_group(right_ip, my_rank, num_nodes, &handle) != 0) {
        fprintf(stderr, "Failed to connect process group. Exiting.\n");
        return 1;
    }

    // Call your future function
    // pg_all_reduce(NULL, NULL, 1024, TYPE_INT, OP_SUM, handle);

    // Teardown
    pg_close(handle);
    return 0;
}

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <infiniband/verbs.h>

#define OOB_PORT 18515
#define MAX_BUF_SIZE (1024 * 1024) // 1MB buffer for All-Reduce

// -----------------------------------------------------------------------------
// 1. Core Structures
// -----------------------------------------------------------------------------

// The Out-Of-Band (TCP) data we need to exchange to connect QPs
struct rdma_dest {
    int lid;
    int qpn;
    int psn;
    uint64_t vaddr;
    uint32_t rkey;
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
            .max_send_wr  = 100,
            .max_recv_wr  = 100,
            .max_send_sge = 1,
            .max_recv_sge = 1,
        },
        .qp_type = IBV_QPT_RC // Reliable Connection
    };
    return ibv_create_qp(handle->pd, &attr);
}

static int modify_qp_to_init(struct ibv_qp *qp, int ib_port) {
    struct ibv_qp_attr attr = {
        .qp_state        = IBV_QPS_INIT,
        .pkey_index      = 0,
        .port_num        = ib_port,
        .qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE
    };
    return ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
}

static int modify_qp_to_rtr(struct ibv_qp *qp, struct rdma_dest *remote, int ib_port) {
    struct ibv_qp_attr attr = {
        .qp_state           = IBV_QPS_RTR,
        .path_mtu           = IBV_MTU_1024,
        .dest_qp_num        = remote->qpn,
        .rq_psn             = remote->psn,
        .max_dest_rd_atomic = 1,
        .min_rnr_timer      = 12,
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

static int modify_qp_to_rts(struct ibv_qp *qp, int my_psn) {
    struct ibv_qp_attr attr = {
        .qp_state       = IBV_QPS_RTS,
        .timeout        = 14,
        .retry_cnt      = 7,
        .rnr_retry      = 7,
        .sq_psn         = my_psn,
        .max_rd_atomic  = 1
    };
    return ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC);
}


// -----------------------------------------------------------------------------
// 3. TCP Out-Of-Band Exchange
// -----------------------------------------------------------------------------

// Accepts connection from the LEFT neighbor
static int exchange_with_left(struct pg_handle_t *handle, int my_lid, int my_psn, int port) {
    int sockfd, connfd;
    struct sockaddr_in serv_addr;
    char msg[128];

    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(port);

    bind(sockfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    listen(sockfd, 1);
    
    printf("Listening for LEFT neighbor on port %d...\n", port);
    connfd = accept(sockfd, NULL, NULL);
    close(sockfd);

    // 1. Read Left's coordinates
    read(connfd, msg, sizeof(msg));
    sscanf(msg, "%x:%x:%x:%llx:%x", &handle->left_remote_dest.lid, &handle->left_remote_dest.qpn, 
           &handle->left_remote_dest.psn, (unsigned long long*)&handle->left_remote_dest.vaddr, 
           &handle->left_remote_dest.rkey);

    // 2. Send My coordinates (specifically my left_qp so they can write to me)
    sprintf(msg, "%04x:%06x:%06x:%016llx:%08x", my_lid, handle->left_qp->qp_num, my_psn, 
            (unsigned long long)handle->buf, handle->mr->rkey);
    write(connfd, msg, sizeof(msg));

    close(connfd);
    return 0;
}

// Initiates connection to the RIGHT neighbor
static int exchange_with_right(struct pg_handle_t *handle, const char *right_ip, int my_lid, int my_psn, int port) {
    int sockfd = -1;
    struct sockaddr_in serv_addr;
    char msg[128];

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    inet_pton(AF_INET, right_ip, &serv_addr.sin_addr);

    printf("Connecting to RIGHT neighbor at %s:%d...\n", right_ip, port);
    while (sockfd < 0) {
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
            close(sockfd);
            sockfd = -1;
            usleep(100000); // Retry every 100ms
        }
    }

    // 1. Send My coordinates (specifically my right_qp so I can read their acks)
    sprintf(msg, "%04x:%06x:%06x:%016llx:%08x", my_lid, handle->right_qp->qp_num, my_psn, 
            (unsigned long long)handle->buf, handle->mr->rkey);
    write(sockfd, msg, sizeof(msg));

    // 2. Read Right's coordinates
    read(sockfd, msg, sizeof(msg));
    sscanf(msg, "%x:%x:%x:%llx:%x", &handle->right_remote_dest.lid, &handle->right_remote_dest.qpn, 
           &handle->right_remote_dest.psn, (unsigned long long*)&handle->right_remote_dest.vaddr, 
           &handle->right_remote_dest.rkey);

    close(sockfd);
    return 0;
}


// -----------------------------------------------------------------------------
// 4. Exercise API Implementation
// -----------------------------------------------------------------------------

/* * I have added 'my_rank' to your requested signature to prevent TCP deadlocks. 
 * 'servername' acts as the IP address of your right-hand neighbor.
 */
int connect_process_group(char *servername, int my_rank, void **pg_handle) {
    struct pg_handle_t *handle = calloc(1, sizeof(struct pg_handle_t));
    int ib_port = 1;
    int my_psn = rand() & 0xffffff;

    // 1. Device and Memory Setup
    struct ibv_device **dev_list = ibv_get_device_list(NULL);
    handle->context = ibv_open_device(dev_list[0]);
    handle->pd = ibv_alloc_pd(handle->context);
    
    handle->buf = malloc(MAX_BUF_SIZE);
    memset(handle->buf, 0, MAX_BUF_SIZE);
    handle->mr = ibv_reg_mr(handle->pd, handle->buf, MAX_BUF_SIZE, 
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    
    handle->cq = ibv_create_cq(handle->context, 200, NULL, NULL, 0);

    // 2. Create the Two Ring QPs
    handle->left_qp = create_qp(handle);
    handle->right_qp = create_qp(handle);
    modify_qp_to_init(handle->left_qp, ib_port);
    modify_qp_to_init(handle->right_qp, ib_port);

    struct ibv_port_attr portinfo;
    ibv_query_port(handle->context, ib_port, &portinfo);
    int my_lid = portinfo.lid;

    // 3. Avoid Deadlocks using Rank (Even listens first, Odd connects first)
    if (my_rank % 2 == 0) {
        exchange_with_left(handle, my_lid, my_psn, OOB_PORT);
        exchange_with_right(handle, servername, my_lid, my_psn, OOB_PORT);
    } else {
        exchange_with_right(handle, servername, my_lid, my_psn, OOB_PORT);
        exchange_with_left(handle, my_lid, my_psn, OOB_PORT);
    }

    // 4. Plug in the connections (INIT -> RTR -> RTS)
    // Left QP
    modify_qp_to_rtr(handle->left_qp, &handle->left_remote_dest, ib_port);
    modify_qp_to_rts(handle->left_qp, my_psn);
    // Right QP
    modify_qp_to_rtr(handle->right_qp, &handle->right_remote_dest, ib_port);
    modify_qp_to_rts(handle->right_qp, my_psn);

    printf("Rank %d successfully formed the RDMA Ring!\n", my_rank);
    *pg_handle = handle;
    ibv_free_device_list(dev_list);
    return 0;
}

int pg_close(void *pg_handle) {
    struct pg_handle_t *handle = (struct pg_handle_t *)pg_handle;
    ibv_destroy_qp(handle->left_qp);
    ibv_destroy_qp(handle->right_qp);
    ibv_destroy_cq(handle->cq);
    ibv_dereg_mr(handle->mr);
    ibv_dealloc_pd(handle->pd);
    ibv_close_device(handle->context);
    free(handle->buf);
    free(handle);
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
    if (argc != 3) {
        printf("Usage: %s <my_rank> <right_neighbor_ip>\n", argv[0]);
        return 1;
    }

    int my_rank = atoi(argv[1]);
    char *right_ip = argv[2];
    void *handle = NULL;

    // Build the Ring
    connect_process_group(right_ip, my_rank, &handle);

    // Call your future function
    // pg_all_reduce(NULL, NULL, 1024, TYPE_INT, OP_SUM, handle);

    // Teardown
    pg_close(handle);
    return 0;
}
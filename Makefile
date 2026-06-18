CC = gcc
CFLAGS = -Wall -O2 -D_GNU_SOURCE
LDFLAGS = -libverbs

# Default target
all: rdma_allreduce

# Rule to build the executable
rdma_allreduce: rdma_allreduce_with_ring.c
	$(CC) $(CFLAGS) -o rdma_allreduce rdma_allreduce.c $(LDFLAGS)

# Rule to clean up the workspace
clean:
	rm -f rdma_allreduce

.PHONY: all clean
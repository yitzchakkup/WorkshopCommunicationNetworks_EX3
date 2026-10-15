CC = gcc
CFLAGS = -Wall -O2 -D_GNU_SOURCE
LDFLAGS = -libverbs

# Optional: override the registered buffer size, e.g. make MAX_BUF_SIZE=16777216
ifdef MAX_BUF_SIZE
CFLAGS += -DMAX_BUF_SIZE=$(MAX_BUF_SIZE)
endif

# Default target
all: rdma_allreduce

# Rule to build the executable
rdma_allreduce: rdma_allreduce_with_ring.c
	$(CC) $(CFLAGS) -o rdma_allreduce rdma_allreduce_with_ring.c $(LDFLAGS)

# Plot the benchmark CSV written by rank 0
plot: allreduce_results.csv
	python3 plot_results.py allreduce_results.csv allreduce_results.png

# Rule to clean up the workspace
clean:
	rm -f rdma_allreduce

.PHONY: all clean plot

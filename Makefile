CC = gcc
CFLAGS = -Wall -Wextra -O2 -D_GNU_SOURCE
LDFLAGS = -libverbs

# Default target: the new engine plus the legacy baseline (same CLI, run side by side)
all: rdma_allreduce rdma_allreduce_legacy

# Rule to build the executable
rdma_allreduce: rdma_allreduce_with_ring.c
	$(CC) $(CFLAGS) -o rdma_allreduce rdma_allreduce_with_ring.c $(LDFLAGS)

# The original eager implementation, kept for before/after comparisons.
# Optional: make legacy LEGACY_FLAGS=-DRNR_RETRY=0 (any RNR NAK becomes fatal)
legacy: rdma_allreduce_legacy
rdma_allreduce_legacy: rdma_allreduce_legacy.c
	$(CC) -Wall -O2 -D_GNU_SOURCE $(LEGACY_FLAGS) -o rdma_allreduce_legacy rdma_allreduce_legacy.c $(LDFLAGS)

# In-process simulator: runs the engine with every rank as a thread, no RDMA hardware needed
# Example: make sim && ./sim_allreduce 4 test
sim: sim_allreduce
sim_allreduce: sim_allreduce.c rdma_allreduce_with_ring.c
	$(CC) $(CFLAGS) -Wno-unused-function -pthread -o sim_allreduce sim_allreduce.c $(LDFLAGS)

# Plot the benchmark CSV written by rank 0
plot: allreduce_results.csv
	python3 plot_results.py allreduce_results.csv allreduce_results.png

# Rule to clean up the workspace
clean:
	rm -f rdma_allreduce rdma_allreduce_legacy sim_allreduce

.PHONY: all legacy sim clean plot

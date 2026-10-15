#!/usr/bin/env bash
# Run the ring all-reduce benchmark on the mlx-stud nodes and plot the results.
#
# Usage: ./run_benchmark.sh [num_nodes|all] [max_bytes] [oob_port]
#   num_nodes  2, 3 or 4 (default 4); uses mlx-stud-01 .. mlx-stud-0N as ranks 0 .. N-1
#              "all" runs 2, 3 and 4 one after another and adds a comparison graph
#   max_bytes  largest message in the sweep (default 32MB, capped by MAX_BUF_SIZE in the binary)
#   oob_port   TCP port for the QP exchange (default 18515; change it if a stale run holds the port)
#
# You are asked for your password once per node; all later commands reuse that connection.
set -u

USER_NAME="shimon.fishman"
ALL_HOSTS=(mlx-stud-01.cs.huji.ac.il mlx-stud-02.cs.huji.ac.il mlx-stud-03.cs.huji.ac.il mlx-stud-04.cs.huji.ac.il)
ALL_IPS=(10.128.128.211 10.128.128.212 10.128.128.213 10.128.128.214)   # RDMA-network IP of each host
MAX_NODES=${#ALL_HOSTS[@]}

MODE="${1:-4}"
MAX_BYTES="${2:-33554432}"
OOB_PORT="${3:-18515}"

if [ "$MODE" = "all" ]; then
    NODE_COUNTS=($(seq 2 "$MAX_NODES"))
elif [[ "$MODE" =~ ^[0-9]+$ ]] && [ "$MODE" -ge 2 ] && [ "$MODE" -le "$MAX_NODES" ]; then
    NODE_COUNTS=("$MODE")
else
    echo "Usage: $0 [num_nodes 2..$MAX_NODES | all] [max_bytes] [oob_port]" >&2
    exit 1
fi
# Every host any of the requested rings will use
HOSTS=("${ALL_HOSTS[@]:0:${NODE_COUNTS[-1]}}")

RUN_TIMEOUT=900                         # seconds before a hung node is killed

LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"
SHARED_DIR="$LOCAL_DIR"                 # used if the nodes mount the same home
COPY_DIR="ex3_allreduce_run"            # relative to remote $HOME otherwise
SRC_FILES=(rdma_allreduce_with_ring.c Makefile)

STAMP="$(date +%Y%m%d_%H%M%S)"
if [ "$MODE" = "all" ]; then
    OUT_ROOT="$LOCAL_DIR/results/all_$STAMP"
else
    OUT_ROOT="$LOCAL_DIR/results/${MODE}nodes_$STAMP"
fi
mkdir -p "$OUT_ROOT"

CTL_DIR="$(mktemp -d)"
SSH_OPTS=(-o ControlPath="$CTL_DIR/%h" -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)

rsh() { local host=$1; shift; ssh "${SSH_OPTS[@]}" "$USER_NAME@$host" "$@"; }

kill_stale() {
    for h in "$@"; do rsh "$h" "pkill -u $USER_NAME -x rdma_allreduce" 2>/dev/null; done
}

cleanup() {
    for h in "${HOSTS[@]}"; do
        ssh "${SSH_OPTS[@]}" -O exit "$USER_NAME@$h" 2>/dev/null
    done
    rm -rf "$CTL_DIR"
}
trap cleanup EXIT

# --- 1. Open one persistent connection per node (password prompt happens here) ---
for h in "${HOSTS[@]}"; do
    echo ">> Connecting to $h"
    if ! ssh "${SSH_OPTS[@]}" -o ControlMaster=yes -o ControlPersist=yes -fN "$USER_NAME@$h"; then
        echo "!! Could not connect to $h" >&2
        exit 1
    fi
done

# --- 2. Decide where the code lives on the nodes ---
if rsh "${HOSTS[0]}" "test -f '$SHARED_DIR/rdma_allreduce_with_ring.c'"; then
    REMOTE_DIR="$SHARED_DIR"
    SHARED=1
    echo ">> Nodes share $REMOTE_DIR"
else
    REMOTE_DIR="$COPY_DIR"
    SHARED=0
    echo ">> No shared home; copying sources to ~/$COPY_DIR on each node"
fi

# --- 3. Copy (if needed), kill stale runs, build ---
kill_stale "${HOSTS[@]}"
if [ "$SHARED" -eq 0 ]; then
    for h in "${HOSTS[@]}"; do
        rsh "$h" "mkdir -p '$REMOTE_DIR'" &&
        scp -q "${SSH_OPTS[@]}" "${SRC_FILES[@]/#/$LOCAL_DIR/}" "$USER_NAME@$h:$REMOTE_DIR/" || {
            echo "!! Copy to $h failed" >&2; exit 1; }
    done
fi

BUILD_HOSTS=("${HOSTS[@]}")
[ "$SHARED" -eq 1 ] && BUILD_HOSTS=("${HOSTS[0]}")   # one build is enough on a shared dir
for h in "${BUILD_HOSTS[@]}"; do
    echo ">> Building on $h"
    if ! rsh "$h" "cd '$REMOTE_DIR' && make clean >/dev/null && make" > "$OUT_ROOT/build_${h%%.*}.log" 2>&1; then
        echo "!! Build failed on $h:" >&2
        cat "$OUT_ROOT/build_${h%%.*}.log" >&2
        exit 1
    fi
done

# --- 4. Run one ring of N nodes: launch, wait, collect CSV, plot ---
# Usage: run_ring <num_nodes> <out_dir>; returns non-zero on failure
run_ring() {
    local n=$1 out=$2
    local hosts=("${ALL_HOSTS[@]:0:$n}")
    local remote_csv="allreduce_results_${n}nodes_$STAMP.csv"
    local pids=() failed=0 rank h right

    mkdir -p "$out"
    echo
    echo ">> Launching $n ranks (max_bytes=$MAX_BYTES, port=$OOB_PORT), logs in $out"
    for rank in $(seq 0 $((n - 1))); do
        h="${hosts[$rank]}"
        right="${ALL_IPS[$(( (rank + 1) % n ))]}"   # ring: rank r -> rank (r+1) % n
        echo "   ${h%%.*}: ./rdma_allreduce $rank $right $n"
        # stdbuf -oL: line-buffered logs, so a hang shows exactly where each rank stopped
        rsh "$h" "cd '$REMOTE_DIR' && timeout $RUN_TIMEOUT stdbuf -oL ./rdma_allreduce $rank $right $n $OOB_PORT $MAX_BYTES '$remote_csv'" \
            > "$out/rank${rank}_${h%%.*}.log" 2>&1 &
        pids+=($!)
    done

    # Stream rank 0's table live while the run is in progress
    echo
    tail -n +1 -f --pid="${pids[0]}" "$out/rank0_${hosts[0]%%.*}.log" 2>/dev/null \
        | sed -un '/=== Ring All-Reduce benchmark/,$p' &
    local tail_pid=$!

    for rank in $(seq 0 $((n - 1))); do
        if ! wait "${pids[$rank]}"; then
            echo "!! Rank $rank (${hosts[$rank]}) failed, see $out/rank${rank}_${hosts[$rank]%%.*}.log" >&2
            failed=1
        fi
    done
    # Don't leave the other ranks waiting on a dead neighbor
    [ "$failed" -ne 0 ] && kill_stale "${hosts[@]}"
    wait "$tail_pid" 2>/dev/null

    local csv="$out/allreduce_results.csv"
    if [ "$SHARED" -eq 1 ]; then
        mv "$REMOTE_DIR/$remote_csv" "$csv" 2>/dev/null
    else
        scp -q "${SSH_OPTS[@]}" "$USER_NAME@${hosts[0]}:$REMOTE_DIR/$remote_csv" "$csv" 2>/dev/null
    fi

    if [ -s "$csv" ]; then
        python3 "$LOCAL_DIR/plot_results.py" "$csv" "$out/allreduce_results.png"
    else
        echo "!! No CSV produced by rank 0 for $n nodes" >&2
        failed=1
    fi
    return $failed
}

FAILED=0
CSVS=()
for n in "${NODE_COUNTS[@]}"; do
    if [ "$MODE" = "all" ]; then out="$OUT_ROOT/${n}nodes"; else out="$OUT_ROOT"; fi
    run_ring "$n" "$out" || FAILED=1
    [ -s "$out/allreduce_results.csv" ] && CSVS+=("$out/allreduce_results.csv")
done

# --- 5. Comparison graph across ring sizes ---
if [ "$MODE" = "all" ] && [ "${#CSVS[@]}" -ge 2 ]; then
    echo
    python3 "$LOCAL_DIR/plot_results.py" --compare "$OUT_ROOT/comparison.png" "${CSVS[@]}"
fi

echo
echo ">> Results in $OUT_ROOT"
exit $FAILED

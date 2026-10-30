#!/usr/bin/env bash
# Run the ring all-reduce (eager vs rendezvous) on the mlx-stud nodes and plot the results.
#
# Usage: ./run_benchmark.sh [num_nodes|all] [max_bytes] [oob_port] [protocols] [mode]
#   num_nodes  2, 3 or 4 (default 4); uses mlx-stud-01 .. mlx-stud-0N as ranks 0 .. N-1
#              "all" runs 2, 3 and 4 one after another and adds a comparison graph
#   max_bytes  largest message in the sweep (default 32MB; rendezvous rows stop at the size
#              that fits the memlock budget the ranks agree on, see the rank 0 log)
#   oob_port   TCP port for the QP exchange (default 18515; change it if a stale run holds the port)
#   protocols  eager,rndv (default) | eager | rndv | auto | all; add ",legacy" to also run the
#              original implementation (rdma_allreduce_legacy) on the same nodes and sizes
#   mode       demo | test | bench | all (default)
#
# Tunables are passed with PG_* environment variables and forwarded to every rank, e.g.
#   PG_SLOTS=4 ./run_benchmark.sh 4                    # credit window ablation
#   PG_RNR_RETRY=0 ./run_benchmark.sh all 0 18515 eager,rndv test   # strict mode: any RNR NAK fails
# See ./rdma_allreduce without arguments for the full list.
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
PROTOS_ARG="${4:-eager,rndv}"
RUN_MODE="${5:-all}"

if [ "$MODE" = "all" ]; then
    NODE_COUNTS=($(seq 2 "$MAX_NODES"))
elif [[ "$MODE" =~ ^[0-9]+$ ]] && [ "$MODE" -ge 2 ] && [ "$MODE" -le "$MAX_NODES" ]; then
    NODE_COUNTS=("$MODE")
else
    echo "Usage: $0 [num_nodes 2..$MAX_NODES | all] [max_bytes] [oob_port] [protocols] [mode]" >&2
    exit 1
fi
case "$RUN_MODE" in demo|test|bench|all) ;; *) echo "!! mode must be demo, test, bench or all" >&2; exit 1 ;; esac

# Split "legacy" off the protocol list: it is a separate binary
RUN_LEGACY=0
PROTOS=""
IFS=',' read -ra _items <<< "$PROTOS_ARG"
for p in "${_items[@]}"; do
    case "$p" in
        legacy) RUN_LEGACY=1 ;;
        eager|rndv|auto|all) PROTOS="${PROTOS:+$PROTOS,}$p" ;;
        *) echo "!! unknown protocol '$p'" >&2; exit 1 ;;
    esac
done
[ -z "$PROTOS" ] && PROTOS="eager,rndv"

# PG_* settings travel in the remote command line (ssh does not forward them). Only plain values.
PG_ENV=""
while IFS= read -r kv; do
    [ -z "$kv" ] && continue
    if [[ ! "$kv" =~ ^PG_[A-Z_]+=[A-Za-z0-9_.,]+$ ]]; then
        echo "!! refusing to forward '$kv' (only PG_NAME=plain-value)" >&2
        exit 1
    fi
    PG_ENV="$PG_ENV $kv"
done < <(env | grep '^PG_' | sort)

# Every host any of the requested rings will use
HOSTS=("${ALL_HOSTS[@]:0:${NODE_COUNTS[-1]}}")

RUN_TIMEOUT=900                         # seconds before a hung node is killed

LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"
SHARED_DIR="$LOCAL_DIR"                 # used if the nodes mount the same home
COPY_DIR="ex3_allreduce_run"            # relative to remote $HOME otherwise
SRC_FILES=(rdma_allreduce_with_ring.c rdma_allreduce_legacy.c Makefile)

STAMP="$(date +%Y%m%d_%H%M%S)"
if [ "$MODE" = "all" ]; then
    OUT_ROOT="$LOCAL_DIR/results/all_$STAMP"
else
    OUT_ROOT="$LOCAL_DIR/results/${MODE}nodes_$STAMP"
fi
mkdir -p "$OUT_ROOT"
{
    echo "date: $(date)"
    echo "args: $*"
    echo "protocols: $PROTOS legacy=$RUN_LEGACY mode=$RUN_MODE max_bytes=$MAX_BYTES"
    echo "env:$PG_ENV"
    echo "source md5: $(md5sum "$LOCAL_DIR/rdma_allreduce_with_ring.c" | cut -d' ' -f1)"
} > "$OUT_ROOT/run_meta.txt"

CTL_DIR="$(mktemp -d)"
SSH_OPTS=(-o ControlPath="$CTL_DIR/%h" -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)

rsh() { local host=$1; shift; ssh "${SSH_OPTS[@]}" "$USER_NAME@$host" "$@"; }

kill_stale() {
    for h in "$@"; do rsh "$h" "pkill -u $USER_NAME -x rdma_allreduce; pkill -u $USER_NAME -x rdma_allreduce_legacy" 2>/dev/null; done
}

# Memlock limits, load, and the HCA error counters (RNR NAKs sent = out_of_buffer) of one node
NODE_SNAPSHOT='echo "ulimit_soft_kb $(ulimit -S -l)"; echo "ulimit_hard_kb $(ulimit -H -l)"; echo "uptime $(uptime)";
for f in /sys/class/infiniband/*/ports/*/hw_counters/{out_of_buffer,rnr_nak_retry_err,local_ack_timeout_err,out_of_sequence,packet_seq_err} /sys/class/infiniband/*/ports/*/counters/port_rcv_errors; do
    [ -r "$f" ] && echo "$f $(cat "$f")"; done'

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

# --- 4. Run one ring of N nodes: launch, wait (fail fast), collect CSV ---
# Usage: launch_ring <num_nodes> <out_dir> <binary> <csv_name> <log_prefix> [extra args]
# Returns non-zero on failure
launch_ring() {
    local n=$1 out=$2 bin=$3 csv_name=$4 prefix=$5; shift 5
    local extra="$*"
    local hosts=("${ALL_HOSTS[@]:0:$n}")
    local remote_csv="${prefix}_${n}nodes_$STAMP.csv"
    local pids=() failed=0 rank h right

    echo
    echo ">> Launching $n ranks of $bin (max_bytes=$MAX_BYTES, port=$OOB_PORT${extra:+, $extra}), logs in $out"
    [ -n "$PG_ENV" ] && echo "   env:$PG_ENV"
    for rank in $(seq 0 $((n - 1))); do
        h="${hosts[$rank]}"
        right="${ALL_IPS[$(( (rank + 1) % n ))]}"   # ring: rank r -> rank (r+1) % n
        echo "   ${h%%.*}: ./$bin $rank $right $n $extra"
        # stdbuf -oL: line-buffered logs, so a hang shows exactly where each rank stopped
        rsh "$h" "cd '$REMOTE_DIR' && env $PG_ENV timeout $RUN_TIMEOUT stdbuf -oL ./$bin $rank $right $n $OOB_PORT $MAX_BYTES '$remote_csv' $extra" \
            > "$out/${prefix}_rank${rank}_${h%%.*}.log" 2>&1 &
        pids+=($!)
    done

    # Stream rank 0's output (tests and table) live while the run is in progress
    echo
    tail -n +1 -f --pid="${pids[0]}" "$out/${prefix}_rank0_${hosts[0]%%.*}.log" 2>/dev/null \
        | sed -un '/^selftest\|^matrix\|^stress\|^tests\|=== Ring All-Reduce benchmark/,$p' &
    local tail_pid=$!

    # Fail fast: the first rank that dies takes the others down instead of leaving them to time out
    local left=$n pid
    while [ "$left" -gt 0 ]; do
        wait -n -p pid "${pids[@]}"
        local status=$?
        left=$((left - 1))
        if [ "$status" -ne 0 ]; then
            for rank in "${!pids[@]}"; do
                [ "${pids[$rank]}" = "$pid" ] && \
                    echo "!! Rank $rank (${hosts[$rank]}) failed (exit $status), see $out/${prefix}_rank${rank}_${hosts[$rank]%%.*}.log" >&2
            done
            if [ "$failed" -eq 0 ]; then
                failed=1
                kill_stale "${hosts[@]}"
            fi
        fi
    done
    wait "$tail_pid" 2>/dev/null

    if [ "$RUN_MODE" = "bench" ] || [ "$RUN_MODE" = "all" ]; then
        local csv="$out/$csv_name"
        if [ "$SHARED" -eq 1 ]; then
            mv "$REMOTE_DIR/$remote_csv" "$csv" 2>/dev/null
            mv "$REMOTE_DIR/$remote_csv.meta.txt" "$csv.meta.txt" 2>/dev/null
        else
            scp -q "${SSH_OPTS[@]}" "$USER_NAME@${hosts[0]}:$REMOTE_DIR/$remote_csv" "$csv" 2>/dev/null
            scp -q "${SSH_OPTS[@]}" "$USER_NAME@${hosts[0]}:$REMOTE_DIR/$remote_csv.meta.txt" "$csv.meta.txt" 2>/dev/null
        fi
        if [ ! -s "$csv" ]; then
            echo "!! No CSV produced by rank 0 of $bin for $n nodes" >&2
            failed=1
        fi
    fi
    return $failed
}

# Usage: run_ring <num_nodes> <out_dir>; runs the new binary (and legacy if asked), snapshots, plots
run_ring() {
    local n=$1 out=$2 failed=0 h
    local hosts=("${ALL_HOSTS[@]:0:$n}")
    mkdir -p "$out"

    for h in "${hosts[@]}"; do rsh "$h" "$NODE_SNAPSHOT" > "$out/node_${h%%.*}_before.txt" 2>&1; done
    launch_ring "$n" "$out" rdma_allreduce allreduce_results.csv rank "$PROTOS" "$RUN_MODE" || failed=1
    if [ "$RUN_LEGACY" -eq 1 ] && { [ "$RUN_MODE" = "bench" ] || [ "$RUN_MODE" = "all" ]; }; then
        launch_ring "$n" "$out" rdma_allreduce_legacy legacy_results.csv legacy || failed=1
    fi
    for h in "${hosts[@]}"; do rsh "$h" "$NODE_SNAPSHOT" > "$out/node_${h%%.*}_after.txt" 2>&1; done

    # HCA counter deltas over the whole ring run (out_of_buffer = RNR NAKs this node had to send)
    for h in "${hosts[@]}"; do
        local b="$out/node_${h%%.*}_before.txt" a="$out/node_${h%%.*}_after.txt"
        awk 'NR==FNR { if ($1 ~ /^\//) before[$1] = $2; next }
             $1 ~ /^\// && ($1 in before) && $2 != before[$1] { n = split($1, p, "/"); printf "   %s %s +%d\n", host, p[n], $2 - before[$1] }' \
            host="${h%%.*}" "$b" "$a"
    done > "$out/hw_counter_deltas.txt"
    if [ -s "$out/hw_counter_deltas.txt" ]; then
        echo ">> HCA counters that changed during the run:"
        cat "$out/hw_counter_deltas.txt"
    fi

    # Strict mode verdict: with PG_RNR_RETRY=0 any receiver-not-ready is a fatal error
    if [[ "$PG_ENV" == *"PG_RNR_RETRY=0"* ]]; then
        if grep -q "RNR retry counter exceeded\|found no posted receive" "$out"/rank*_*.log 2>/dev/null; then
            echo "!! strict mode (PG_RNR_RETRY=0): FAIL, see the rank logs" >&2
            failed=1
        else
            echo ">> strict mode (PG_RNR_RETRY=0): PASS, no RNR NAK reached a sender"
        fi
    fi

    local csv="$out/allreduce_results.csv"
    if [ -s "$csv" ]; then
        local base=()
        [ -s "$out/legacy_results.csv" ] && base=(--baseline "$out/legacy_results.csv")
        python3 "$LOCAL_DIR/plot_results.py" "$csv" "$out/allreduce_results.png" "${base[@]}"
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

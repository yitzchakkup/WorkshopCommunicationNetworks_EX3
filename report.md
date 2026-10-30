# Exercise 3 – Ring All-Reduce over RDMA: Eager vs. Rendezvous

## 1. Overview

We implement Reduce-Scatter, All-Gather and All-Reduce on a logical ring of N processes (tested with N = 2, 3, 4) using the Verbs API, single-threaded. The goal of the exercise is to compare a **rendezvous** protocol with a trivial **eager** one. Both protocols run on **one credit-based, segmented pipeline engine**. They differ only in how a single segment travels from a rank to its right neighbour:

| | Eager | Rendezvous |
|---|---|---|
| Sender | copies the segment into a registered bounce buffer (or inlines it into the work request when it is ≤ 256 B), `SEND_WITH_IMM` | waits for a **CTS** grant from the receiver, then `RDMA_WRITE_WITH_IMM` straight from the user's buffer (**zero copy**) |
| Where the data lands | a receive slot the receiver posted in advance | the address in the grant: a slot (reduce-scatter) or its **final place in `recvbuf`** (all-gather) |
| Receiver | reduces from the slot (RS) or copies slot → `recvbuf` (AG) | reduces from the slot (RS); nothing to do in AG |
| Extra cost | 2 CPU copies per all-gather segment, 1 per RS segment | one control message per segment, plus pinning the user buffers (pin-down cache) |

The API keeps the exercise's `pg_all_reduce(sendbuf, recvbuf, count, datatype, op, pg_handle)` signature, and adds:
- `pg_reduce_scatter` and `pg_all_gather`, which use the same engine;
- `pg_set_protocol(handle, PROTO_EAGER | PROTO_RNDV | PROTO_AUTO)`, a collective call that all ranks make with the same value;
- `pg_reg_cache_flush`.

`connect_process_group` takes `(servername, rank, num_nodes, pg_handle, oob_port)` instead of the PDF's two arguments. The ring needs every rank to know its position, and a fixed TCP port.

---

## 2. Ring topology and connection setup

Each rank r has two RC queue pairs:
- `right_qp`, connected to rank r+1's `left_qp`;
- `left_qp`, connected to rank r−1's `right_qp`.

Data always flows left → right. **Control messages (credits, CTS grants) flow right → left over the same RC connections.** RC QPs are bidirectional, so no extra QPs are needed.

| QP | Send queue | Receive queue |
|---|---|---|
| `left_qp` | CREDIT / CTS to the left neighbour (inline, signalled) | K data slots |
| `right_qp` | data to the right neighbour | control buffers |

Both QPs share one CQ. Every work request carries its kind and an index in `wr_id`, and completions are dispatched on that. They are never matched by counting.

Bootstrap:
- Out-of-band TCP with rank-parity ordering (even ranks listen first, odd ranks connect first). It exchanges QPN, PSN, LID, the port's active MTU, and every `PG_*` setting. Any mismatch aborts.
- Each connection uses the smaller of its two active MTUs.
- **All receives are posted before the TCP READY barrier**, so no first message can find an empty receive queue.
- The TCP sockets stay open for a closing barrier in `pg_close`. Each rank first waits for its own sends to complete, then does the barrier, and only then destroys its QPs. A neighbour can therefore never send into a destroyed QP.

---

## 3. Memory

Everything the NIC touches internally is registered once, at connect time, with a size that does not depend on the message size:

| Pool | Size (default) | Access |
|---|---|---|
| slots | K × SLOT = 16 × 64 KB | local + remote write (rendezvous RS writes into them) |
| bounce buffers | K × SLOT | local (eager send copies) |
| control buffers | CTRL_DEPTH × 32 B | local |

The old design copied each message into a single 4 MB registered buffer, so messages were limited to ~2 MB. That limit is gone:
- **Eager** never registers user memory at all.
- **Rendezvous** registers `sendbuf`/`recvbuf` through a **pin-down cache**: 8 entries, LRU, a hit means the cached region contains the requested range, page-aligned spans.

Pinned memory counts against `ulimit -l`:
- At start-up we raise the soft limit to the hard limit.
- All ranks agree (MIN all-reduce) on how much user memory rendezvous may pin. Every "can this size use rendezvous?" decision is therefore identical on all ranks.
- On a cache miss, entries overlapping the new span are dropped first, because the kernel would count overlapping registrations twice.

**Pin-down cache contract.** The cache is off by default in the library (`PG_REG_CACHE=1` turns it on), and the test program turns it on because its buffers live for the whole run. A cached registration keeps pointing at the physical pages that were mapped when it was created. If a buffer is freed and its address later reused, the cache would hand out a stale MR. Callers must call `pg_reg_cache_flush` before freeing memory that was used with rendezvous. The test program also keeps freed memory mapped (`mallopt(M_MMAP_MAX, 0)`, `M_TRIM_THRESHOLD = -1`, as MPI does) and allocates its buffers only once. *Registration cost per size: see `*.meta.txt` (`reg_cost`) in each results folder.*

---

## 4. The credit system

**Credit = one posted receive slot at the receiver.**
- The sender keeps `tx_credits`, starting at K.
- Every data message, eager or rendezvous, consumes exactly one receive at the receiver (SEND and RDMA-Write-with-Immediate both do), so it is posted only when a credit is available.
- Invariant: **a message never arrives at an empty receive queue, so RNR NAKs cannot happen.**

The old implementation had no flow control: it posted receives just in time and relied on RNR-NAK retries.

**Returning credits.**
- When the receiver has finished with a slot (reduced it, copied it, or consumed the rendezvous notification), it re-posts the slot and owes the sender one credit.
- Owed credits go back in a `CREDIT{n}` message once n ≥ `CREDIT_BATCH` (default K/4, kept within [1, K]), and are piggy-backed on every CTS.
- The sender adds them as soon as it parses the control message, not when it uses the grant. Otherwise a sender waiting for credits could deadlock while those credits sit inside a queued CTS for a later message.

**Grants (rendezvous).**
- The receiver grants message g with a CTS {seq, addr, rkey, len}.
- Grants are issued in order, at most W ahead of what has arrived (`g < rx_polled + W`, W ≤ K).
- A reduce-scatter segment is granted the slot its receive will consume (slot = seq mod K). That slot is free by the time the sender may write, because the write also needs a credit, and slots are re-posted in sequence order.
- An all-gather segment is granted its final address in `recvbuf`.

**The control channel needs flow control too.** Each control message consumes a control receive at the data sender. At most W grants and ⌊K/batch⌋ credit messages can be unparsed at a time, and the sender re-posts a control buffer as soon as it parses it. So `CTRL_DEPTH = W + ⌊K/batch⌋ + 2` control receives are enough (22 with the defaults).

**Proof by experiment.** With `PG_RNR_RETRY=0` the HCA turns any RNR NAK into a fatal `RNR retry counter exceeded` completion. A clean run therefore shows the invariant holds, and `run_benchmark.sh` prints a PASS/FAIL line. The HCA counter `out_of_buffer` (RNR NAKs sent) is snapshotted before and after each run.

**Liveness.**
- If the sender has no credits and nothing is in flight, the receiver owes it K ≥ batch credits and must send them.
- Reduce-scatter arrivals can always be processed immediately, and all-gather arrivals never wait for anything.
- A neighbour is at most one call ahead, because a rank cannot finish call c+1 before every rank has contributed to it. Its next-call messages occupy only slots that the current call has already given back.

---

## 5. Message schedule and pipelining

Each chunk (count/N elements) is cut into S segments of at most SLOT bytes. A call is a sequence of M = 2(N−1)·S messages per link, numbered identically on both ends:
- the first (N−1)·S are reduce-scatter;
- the rest are all-gather.

Rank r:
- **RS step s:** sends chunk r−1−s (step 0 from `sendbuf`, later from `recvbuf`), receives chunk r−2−s and computes `recvbuf = sendbuf OP slot`. The last step delivers chunk r, which rank r then owns fully reduced. This is the standard reduce-scatter output.
- **AG step s:** sends chunk r−s and receives chunk r−1−s into `recvbuf`.

**Rule 1, the only enforced dependency:** message m may be sent once arrival m−S has been processed. The first S messages carry the rank's own data.

**Pipelining:** segment j of step s+1 leaves as soon as segment j of step s has been reduced, while segment j+1 is still arriving. Communication and the reduction overlap at segment granularity. In the old code everyone reduced a whole chunk, then everyone sent it.

**No write-after-read gate is needed.** An all-gather arrival for chunk c overwrites the `recvbuf` region this rank sent from at reduce-scatter step s. That all-gather data can only exist once chunk c is fully reduced. Full reduction requires this rank's reduce-scatter contribution to have *reached the right neighbour*, so:
- for eager, that send was already posted, and its data already copied;
- for rendezvous, the NIC has finished reading the source.

This also holds in-place (`sendbuf == recvbuf`). A cheap assertion checks it at run time. An explicit gate would only add a pointless completion wait to rendezvous.

**Sequence checks.** Every data message carries its global sequence number as the immediate. The receiver checks it, plus the opcode and length expected for the call that owns the message. Messages of the next call, possibly with the other protocol, may already arrive during the current call; they simply stay queued. Any mismatch between ranks (protocol, count) becomes an immediate error instead of a wrong sum. A watchdog (`PG_TIMEOUT`, default 10 s without progress) turns a hang into an error and dumps the link state.

**Local completion asymmetry.** An eager call may return once its sends are *posted*, because the data was already copied. A rendezvous call must wait for its writes to *complete*, because the NIC reads the user's buffers.

---

## 6. Receiver-initiated rendezvous vs. the textbook handshake

In MPI point-to-point, the sender opens with an RTS because the receiver doesn't know the message size. In a collective the receiver already knows exactly which segment comes next and how big it is. So we use **receiver-initiated** rendezvous: the receiver sends the CTS as soon as it has room, up to W segments ahead. Each call still pays at least one exposed handshake, since the first grant can only be sent after the receiver has entered the call. After that, grants overlap with the data. The ablation `PG_GRANT_WINDOW=1` makes every segment wait for its own CTS, which approximates the classic per-message handshake cost.

---

## 7. Testing

- **Simulator (`make sim`, `./sim_allreduce N test`)** runs the real engine with every rank as a thread and a model of the HCA behind `ibv_post_send/recv/poll_cq`, without RDMA hardware. The model enforces:
  - per-QP FIFO delivery and completions, with random delays;
  - every SEND and Write-with-Immediate consumes the head receive, and finding none is an RNR NAK (fatal when `PG_RNR_RETRY=0`);
  - receive length, rkey and region checks, plus send-queue and CQ limits;
  - the source is read at delivery time, so reusing a buffer too early shows up as wrong data.

  Passed: N = 2…8; K = 1, 2, 16; W = 1; batch 1 and 16; slot sizes from 64 B to 256 KB; credits off; registration cache off; delays from 0 to 400 µs. The credit system is visible directly: at the default K = 16, the test workload with credits **off** causes 13 155 RNR NAKs, and with credits **on** 0.

  Known limits of the model: it moves data by virtual address, so it cannot show stale pinned pages (see the pin-down cache contract). It also places a multi-packet write all at once rather than packet by packet, which only matters without credits; rendezvous always uses credits.
- **Test mode (`mode test`)**:
  - a self-test;
  - a 588-call matrix: {int, float, double} × {sum, max, min} × {eager, rendezvous} × {all-reduce, reduce-scatter, all-gather} × {in-place, out-of-place} × sizes around a segment boundary, plus one size with more segments per chunk than credit slots;
  - 2000 randomized back-to-back calls that alternate protocols, with random delays to skew the ranks.

  Outputs are poisoned before every call. Inputs are small integers, so float sums are exact. Rendezvous cases that don't fit the agreed memlock budget are skipped on every rank.
- **Benchmark (`mode bench`)**:
  - one-time warm-up at the largest size of each protocol;
  - power-of-2 sweep with 5 repetitions per size and protocol, alternating the protocol order (ABBA);
  - time per call is the slowest rank's; we report min/median/max;
  - correctness is checked on one extra call into a poisoned buffer.

---

## 8. Results

Run: `./run_benchmark.sh all` on 30 Oct (`results/all_20261030_133500/`). Default settings: K = 16 slots × 64 KB, credit batch 4, grant window 16, path MTU 4096, 5 repetitions per size, alternating protocol order. `ulimit -l` is unlimited on the nodes, so rendezvous ran up to 32 MB. All tests passed on 2, 3 and 4 nodes (504-call matrix plus 2000-call stress), and every benchmark row verified with 0 wrong elements.

Figures: `<N>nodes/allreduce_results.png` (latency, bandwidth, rendezvous/eager ratio), `comparison.png`, `comparison_ratio.png`, `latency.png`.

### 8.1 Setup
- memcpy bandwidth (rank 0): 18.5 GB/s for a 64 KB segment (cache resident), 5.6 GB/s for a large buffer (memory bound).
- Registration cost (`reg_cost` in `*.meta.txt`): about 60–460 µs per `ibv_reg_mr` of 64 KB–4 MB, comparable to a whole 1 MB all-reduce. This is why rendezvous needs the pin-down cache.
- *To add: HCA model and link rate (`ibv_devinfo -v`, `ib_write_bw` per link).*

### 8.2 Before/after the credit system

Small-message latency (median, slowest rank):

| | legacy (Appendix A) | credit-based eager | rendezvous |
|---|---|---|---|
| 2 nodes | 8–14 µs | **4.4 µs** | 8.0 µs |
| 3 nodes | 0.3–2.4 ms | **8.5 µs** | 14.2 µs |
| 4 nodes | 0.5–3.6 ms | **12.7 µs** | 19.6 µs |

The millisecond-scale stalls of the legacy code at 3–4 nodes are gone. The time now grows linearly with the ring, about 2.1 µs per hop over the 2(N−1) sequential hops, as expected for a latency-bound ring. *To add, as a same-session comparison: `./run_benchmark.sh all 33554432 18515 eager,rndv,legacy bench` (use `PG_MTU=1024`, or build legacy with `LEGACY_FLAGS=-DLEGACY_MTU=IBV_MTU_4096`, so both use the same MTU), plus `PG_CREDITS=0` for eager without credits.*

### 8.3 Eager vs. rendezvous

| Size | 2 nodes eager / rndv (µs) | 3 nodes eager / rndv (µs) | 4 nodes eager / rndv (µs) |
|---:|---:|---:|---:|
| 16 B | 4.4 / 8.0 | 8.5 / 14.2 | 12.7 / 19.6 |
| 4 KB | 9.0 / 11.3 | 15.9 / 17.6 | 22.1 / 23.8 |
| 64 KB | 43.9 / **41.2** | 63.5 / **60.7** | 75.9 / **75.3** |
| 1 MB | 411 / **329** | 557 / **449** | 612 / **576** |
| 32 MB | 20 193 / **12 631** | 26 207 / **17 740** | 29 893 / **19 703** |

Peak bus bandwidth: eager about 20.5 Gb/s (512 KB–1 MB), falling to about 13.4 Gb/s at 32 MB; rendezvous about 25.5 Gb/s, still about 20–21 Gb/s at 32 MB.

Observations:
1. **Small messages: eager wins by 1.5–1.8×.** Rendezvous pays one exposed handshake per call (`cts_waits` ≈ 1.0 per call) plus posting its grants: 2 / 5 / 7 control messages per call on 2 / 3 / 4 nodes, against 0.5–1.5 credit messages for eager. That is why the gap grows with N (+3.6 µs at 2 nodes, +6.9 µs at 4 nodes). Eager also has a visible step from 5.4 to 6.6 µs (2 nodes) when the chunk passes 256 B: above that it stops sending inline and starts copying into bounce buffers.
2. **Crossover at 64 KB total on every ring size, i.e. 16–32 KB per chunk.** From there rendezvous is faster with non-overlapping min–max ranges. The AUTO default (`PG_RNDV_THRESHOLD`) is set to 32 KB per chunk accordingly.
3. **Large messages: rendezvous wins by 1.5–1.6×.** Eager copies 1.5× the message per call (`bytes_copied`). While the data fits in cache, memcpy (18.5 GB/s) is faster than the link and the pipeline hides it. Once it no longer fits, memcpy drops to 5.6 GB/s and becomes the bottleneck, so eager falls from 20.5 to 13.4 Gb/s. Rendezvous moves no bytes with the CPU except the reduction itself.
4. **Rendezvous also dips at 8 MB and above** (25 → 21 Gb/s), with many credit stalls per call. With 16 × 64 KB in flight the window is 1 MB; the K and segment-size ablations (8.4) show whether a larger window helps.
5. **The HCA counter snapshots are empty on these nodes** (no `hw_counters` directory), so the no-RNR claim rests on the strict run: `PG_RNR_RETRY=0 ./run_benchmark.sh all 1048576 18515 eager,rndv test`.

### 8.4 Ablations (`plot_results.py --ablation <column> …`)
*To run (one invocation per value):*
- credit window K ∈ {1, 2, 4, 16} (`PG_SLOTS`);
- segment size at constant window bytes: 64 × 16 KB, 16 × 64 KB, 4 × 256 KB (`PG_SLOTS`, `PG_SLOT_SIZE`);
- grant window W ∈ {1, K} (W = 1 approximates the classic per-message handshake), credit batch ∈ {1, K/4};
- registration cache off (`PG_REG_CACHE=0`).

### 8.5 Cost model
Per segment of size s on a link with latency α and bandwidth β, and memcpy bandwidth γ, a chunk of S segments over 2(N−1) steps costs roughly:
- eager: 2(N−1)·(α + s/β) + copy terms (s/γ per RS segment, 2s/γ per AG segment), partly hidden by pipelining;
- rendezvous: the same transfer terms, no copies, plus one exposed CTS latency α per call (more when W is small).

With γ (cache) ≫ β the copies hide behind the transfer, and eager wins on its smaller fixed cost. With γ (DRAM) ≈ 5.6 GB/s < β the copies dominate, and rendezvous wins. This matches 8.3.

## 9. Summary of design decisions

| Decision | Choice | Rationale |
|---|---|---|
| Flow control | Credits = posted receive slots, returned in batches and on CTS | No RNR NAKs; bounded memory; the same mechanism for both protocols |
| Eager | Copy into bounce buffers + `SEND_WITH_IMM`; inline ≤ 256 B | The classic eager protocol: no handshake, no user registration |
| Rendezvous | Receiver-initiated CTS + `RDMA_WRITE_WITH_IMM` | Zero copy; the immediate signals completion; a collective needs no RTS |
| Pipelining | Fixed-size segments, Rule 1 | Overlaps reduction and transfer; message size no longer limited by a buffer |
| User memory | Pin-down cache within an agreed memlock budget | Registration is expensive; agreement keeps the ranks in lockstep |
| Control channel | Its own bound (`CTRL_DEPTH`) and in-order grant window | Control messages also consume receives |
| Robustness | Sequence numbers in immediates, opcode/length checks, watchdog | Bugs and rank mismatches fail loudly instead of corrupting sums |
| Completion detection | Busy-poll, dispatched by `wr_id` | Lowest latency; correct with mixed completions on a shared CQ |

---

## Appendix A – Performance of the original (legacy) implementation

*This is the evaluation of the original eager implementation (now `rdma_allreduce_legacy.c`), run on 15 Oct, before the credit system and rendezvous. It is kept as the "before" picture for Section 8.2.*


### A.1 Setup and Methodology

The benchmark is built into `rdma_allreduce` and driven by `run_benchmark.sh all`, which runs rings of 2, 3 and 4 nodes on `mlx-stud-01` … `mlx-stud-04` (rank *i* on `mlx-stud-0(i+1)`). The numbers below come from the run in `results/all_20261015_144736/`.

- **Operation**: `TYPE_INT`, `OP_SUM`.
- **Message sizes**: powers of two, rounded down to a multiple of `N` integers so that every node gets an equal chunk (for example, the "16 B" row of the 3-node ring is really 12 B). The sweep stops at the largest message that fits in the 4 MB registered buffer together with the two scratch chunks and the sync byte: 1 MB for 2 nodes and 2 MB for 3 and 4 nodes.
- **Timing**: 5 warm-up calls, then a timed loop sized to take about 0.5 s (between 5 and 1000 iterations). Each rank measures its own average time per call, and the reported time is the **slowest rank's** average, gathered with an `OP_MAX` all-reduce.
- **Bandwidth**: algorithm bandwidth `AlgBW = bytes × 8 / time`; bus bandwidth `BusBW = AlgBW × 2(N−1)/N`, the data each link actually carries in a ring all-reduce, which makes rings of different sizes comparable.
- **Correctness**: every node fills `sendbuf[i] = rank + (i mod 1000)` and checks after every size that `recvbuf[i] = N(N−1)/2 + N·(i mod 1000)`. All sizes on all ring sizes passed with 0 wrong elements.

### A.2 Bandwidth

![Algorithm and bus bandwidth vs. message size for 2, 3 and 4 nodes](results/all_20261015_144736/comparison.png)

*Figure 1 – Algorithm bandwidth (left) and bus bandwidth (right) against message size. For N = 2 the factor 2(N−1)/N is 1, so the two are equal.*

### A.3 Latency

![Average time per all-reduce vs. message size, log scale](results/all_20261015_144736/latency.png)

*Figure 2 – Average time per all-reduce call (slowest rank), log scale.*

### A.4 Results Table

| Size | 2 nodes: time (µs) | 2 nodes: BusBW (Gb/s) | 3 nodes: time (µs) | 3 nodes: BusBW (Gb/s) | 4 nodes: time (µs) | 4 nodes: BusBW (Gb/s) |
|---:|---:|---:|---:|---:|---:|---:|
| 8 B | 13.9 | 0.005 | — | — | — | — |
| 16 B | 9.3 | 0.014 | 1,516.6 | 0.000 | 1,236.3 | 0.000 |
| 32 B | 8.2 | 0.031 | 812.0 | 0.000 | 3,535.5 | 0.000 |
| 64 B | 9.5 | 0.054 | 2,375.6 | 0.000 | 3,600.4 | 0.000 |
| 128 B | 12.1 | 0.084 | 1,358.2 | 0.001 | 2,647.6 | 0.001 |
| 256 B | 10.3 | 0.200 | 1,155.1 | 0.002 | 2,221.7 | 0.001 |
| 512 B | 10.2 | 0.403 | 2,412.6 | 0.002 | 3,577.7 | 0.002 |
| 1 KB | 9.4 | 0.867 | 274.6 | 0.040 | 3,420.6 | 0.004 |
| 2 KB | 10.1 | 1.617 | 1,703.6 | 0.013 | 3,030.3 | 0.008 |
| 4 KB | 16.7 | 1.966 | 658.1 | 0.066 | 523.1 | 0.094 |
| 8 KB | 17.0 | 3.860 | 927.3 | 0.094 | 2,781.3 | 0.035 |
| 16 KB | 22.9 | 5.713 | 1,254.9 | 0.139 | 294.6 | 0.667 |
| 32 KB | 35.1 | 7.468 | 1,755.0 | 0.199 | 2,124.3 | 0.185 |
| 64 KB | 83.8 | 6.257 | 1,968.6 | 0.355 | 3,236.6 | 0.243 |
| 128 KB | 129.1 | 8.119 | 2,227.3 | 0.628 | 3,078.8 | 0.511 |
| 256 KB | 286.8 | 7.312 | 1,311.8 | 2.132 | 2,900.5 | 1.085 |
| 512 KB | 517.6 | 8.104 | 2,284.1 | 2.448 | 3,190.7 | 1.972 |
| 1 MB | 1,579.6 | 5.311 | 2,760.9 | 4.051 | 3,852.3 | 3.266 |
| 2 MB | — | — | 3,215.7 | 6.956 | 4,794.4 | 5.249 |

*Table 1 – Average time per call and bus bandwidth. "—" means that size was not part of that ring's sweep. Full per-run data, including iteration counts and algorithm bandwidth, is in `results/all_20261015_144736/<N>nodes/allreduce_results.csv`.*

Per-ring charts are also available: [2 nodes](results/all_20261015_144736/2nodes/allreduce_results.png), [3 nodes](results/all_20261015_144736/3nodes/allreduce_results.png), [4 nodes](results/all_20261015_144736/4nodes/allreduce_results.png).

### A.5 Observations

1. **Two nodes behave as expected.** Up to 2 KB the time stays flat at about 8–14 µs, so small messages are bound by per-operation latency. From 4 KB on, bandwidth rises quickly and levels off at about **8.1 Gb/s** (128 KB and 512 KB), where the link is the bottleneck. The drop to 5.3 Gb/s at 1 MB comes from a single measurement and would need repeated runs before drawing conclusions.

2. **Three and four nodes have a millisecond-scale floor.** Messages of 2 KB and below take 0.3–3.6 ms on 3 and 4 nodes (mostly above 1 ms), roughly 30–380× the 2-node time for the same size, and the times jump erratically between neighboring sizes (on 4 nodes: 16 KB takes 295 µs while 32 KB takes 2,124 µs). This is not a bandwidth limit: the time barely depends on message size until the largest messages. Instead, individual steps are stalling. The likely difference from the 2-node case: there, each node's left and right neighbors are the same node, so the two ranks stay in lockstep. With three or more nodes, a rank can issue its two-sided SEND before its right neighbor has posted the matching RECV, which makes the sender back off and retry (RNR). Lowering `MIN_RNR_TIMER` from 12 (0.64 ms) to 1 (0.01 ms) already helped, but the remaining stalls are much longer than one RNR delay, so the cause is not fully pinned down yet. The lab machines are shared, and each size was timed only 96–506 times on these rings, so some of the noise may also come from outside the benchmark.

3. **Large messages amortize the stall.** As messages grow, the fixed overhead matters less: on 3 nodes bus bandwidth climbs to **6.96 Gb/s** at 2 MB, and on 4 nodes to **5.25 Gb/s**. Both curves are still rising at the end of the sweep, so the 4 MB registered buffer, not the network, sets the limit here. Building with a larger buffer (`make MAX_BUF_SIZE=67108864`) would show where they level off.

4. **Correctness held everywhere.** Every size on every ring returned the exact expected sum on all ranks, including the 3-node sizes, which are not powers of two.

#!/usr/bin/env python3
"""Plot ring all-reduce benchmark results (CSV written by rank 0 of rdma_allreduce).

Usage: python3 plot_results.py [allreduce_results.csv] [allreduce_results.png]
                               [--baseline legacy_results.csv] [--x chunk]
       python3 plot_results.py --compare comparison.png run_2nodes.csv run_3nodes.csv ... [--x chunk]
       python3 plot_results.py --ablation slots ablation.png run_K1.csv run_K4.csv ...
       python3 plot_results.py --latency latency.png run_2nodes.csv run_3nodes.csv ...

A run figure has three panels: latency (median, min-max band), bus bandwidth, and the
rendezvous/eager time ratio with the crossover marked. --compare draws one row per ring size
plus <png>_ratio.png; --ablation plots bus bandwidth per value of a CSV config column (e.g.
slots, slot_size, grant_window, credit_batch, credits_on). CSVs without a protocol column
(older runs, rdma_allreduce_legacy) are labelled "legacy".
"""
import argparse
import csv
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator

SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"
SERIES_1 = "#2a78d6"  # blue
SERIES_2 = "#eb6834"  # orange
SERIES_3 = "#1baf7a"  # aqua
SERIES_4 = "#eda100"  # yellow (ablations with a 4th value only)
# Fixed color per protocol and per ring size, whatever subset is plotted. Legacy is a reference
# line in muted ink (dashed), not a categorical slot.
PROTO_COLORS = {"eager": SERIES_1, "rndv": SERIES_2, "auto": SERIES_3, "legacy": TEXT_SECONDARY}
PROTO_LABELS = {"eager": "eager", "rndv": "rendezvous", "auto": "auto", "legacy": "legacy (before credits)"}
PROTO_ORDER = ["eager", "rndv", "auto", "legacy"]
NODE_COLORS = {2: SERIES_1, 3: SERIES_2, 4: SERIES_3}
ABLATION_COLORS = [SERIES_1, SERIES_2, SERIES_3, SERIES_4]
LINE = dict(linewidth=2, marker="o", markersize=5, markeredgecolor=SURFACE, markeredgewidth=1.5)


def human_size(b, _pos=None):
    b = int(b)
    if b >= 1 << 20:
        return f"{b >> 20}MB"
    if b >= 1 << 10:
        return f"{b >> 10}KB"
    return f"{b}B"


def bw(row, name):
    """Bandwidth in Gb/s; CSVs from older runs stored GB/s instead."""
    if f"{name}_Gbps" in row:
        return float(row[f"{name}_Gbps"])
    return float(row[f"{name}_GBps"]) * 8


def load(path):
    """{num_nodes, series: {(protocol, mode): {...lists}}, cfg: first row}"""
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        sys.exit(f"No rows in {path}")
    num_nodes = int(rows[0]["num_nodes"])
    groups = {}
    for r in rows:
        key = (r.get("protocol") or "legacy", r.get("mode") or "bw")
        groups.setdefault(key, []).append(r)
    series = {}
    for key, rs in groups.items():
        rs.sort(key=lambda r: int(r["bytes"]))
        med = [float(r.get("t_med_us") or r["avg_time_us"]) for r in rs]
        series[key] = {
            "bytes": [int(r["bytes"]) for r in rs],
            "chunk": [int(r["chunk_bytes"]) if r.get("chunk_bytes") else int(r["bytes"]) // num_nodes for r in rs],
            "med": med,
            "min": [float(r["t_min_us"]) if r.get("t_min_us") else m for r, m in zip(rs, med)],
            "max": [float(r["t_max_us"]) if r.get("t_max_us") else m for r, m in zip(rs, med)],
            "algbw": [bw(r, "algbw") for r in rs],
            "busbw": [bw(r, "busbw") for r in rs],
            "errors": sum(int(r["errors"]) for r in rs),
        }
    return {"num_nodes": num_nodes, "series": series, "cfg": rows[0], "path": path}


def by_protocol(d):
    """protocol -> series, preferring the back-to-back ("bw") mode when a CSV has both"""
    out = {}
    for (proto, mode), s in sorted(d["series"].items(), key=lambda kv: kv[0][1] != "bw"):
        out.setdefault(proto, s)
    return out


def style_axes(ax, title, ylabel, xlabel="Message size"):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=TEXT_PRIMARY, fontsize=12, fontweight="bold")
    ax.set_xlabel(xlabel, color=TEXT_SECONDARY)
    ax.set_ylabel(ylabel, color=TEXT_SECONDARY)
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(FuncFormatter(human_size))
    ax.grid(True, which="major", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=TEXT_SECONDARY, labelsize=9)
    for label in ax.get_xticklabels():
        label.set_rotation(45)
        label.set_ha("right")


def log_time_axis(ax):
    """Log y axis for times, labelled 1, 2, 5, 10, ... µs instead of bare powers of ten"""
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1, 2, 5)))
    ax.yaxis.set_minor_locator(LogLocator(base=10, subs=(3, 4, 6, 7, 8, 9)))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _p: f"{v:g}"))
    ax.yaxis.set_minor_formatter(FuncFormatter(lambda v, _p: ""))


def legend(ax, loc="upper left"):
    leg = ax.legend(loc=loc, frameon=False, fontsize=9)
    for t in leg.get_texts():
        t.set_color(TEXT_PRIMARY)


def end_labels(ax, items):
    """Direct labels at the last point of each line, nudged apart so they don't collide.
    items: [(x, y, text)]. Text stays in primary ink; the line end carries the color."""
    items = sorted(items, key=lambda it: it[1])
    placed = []
    for x, y, text in items:
        dy = 0
        for _, py in placed:
            if abs(ax.transData.transform((x, y))[1] - py) < 12:
                dy += 12
        ty = ax.transData.transform((x, y))[1] + dy
        placed.append((x, ty))
        ax.annotate(text, (x, y), xytext=(6, dy), textcoords="offset points", va="center",
                    color=TEXT_PRIMARY, fontsize=9, annotation_clip=False)


def ratio_series(eager, rndv, xkey):
    """rndv/eager median time ratio on the sizes both measured, with a min-max range"""
    e = {b: i for i, b in enumerate(eager["bytes"])}
    xs, r, lo, hi, clearly = [], [], [], [], []
    for j, b in enumerate(rndv["bytes"]):
        if b not in e:
            continue
        i = e[b]
        xs.append(rndv[xkey][j])
        r.append(rndv["med"][j] / eager["med"][i])
        lo.append(rndv["min"][j] / eager["max"][i])
        hi.append(rndv["max"][j] / eager["min"][i])
        clearly.append(rndv["max"][j] < eager["min"][i])   # ranges don't overlap
    return xs, r, lo, hi, clearly


def crossover(xs, r, clearly):
    """Smallest size from which rendezvous is clearly faster there and faster (median) at every
    larger size; None if there is no such size."""
    for i in range(len(xs)):
        if clearly[i] and all(v < 1 for v in r[i:]):
            return xs[i]
    return None


def draw_ratio(ax, xs, r, lo, hi, clearly, color, label):
    ax.fill_between(xs, lo, hi, color=color, alpha=0.15, linewidth=0)
    ax.plot(xs, r, color=color, label=label, **LINE)
    return crossover(xs, r, clearly)


def plot_run(csv_path, png_path, baseline=None, xkey="bytes"):
    d = load(csv_path)
    series = by_protocol(d)
    base = by_protocol(load(baseline)) if baseline else {}
    xlabel = "Chunk size (bytes per rank)" if xkey == "chunk" else "Message size"
    n = d["num_nodes"]

    fig, (ax_lat, ax_bw, ax_ratio) = plt.subplots(1, 3, figsize=(17, 5.4), facecolor=SURFACE)
    labels = []
    for proto in PROTO_ORDER:
        s = series.get(proto) or (base.get(proto) if proto == "legacy" else None)
        if not s:
            continue
        x = s[xkey]
        style = dict(LINE, linestyle="--") if proto == "legacy" else LINE
        ax_lat.fill_between(x, s["min"], s["max"], color=PROTO_COLORS[proto], alpha=0.15, linewidth=0)
        ax_lat.plot(x, s["med"], color=PROTO_COLORS[proto], label=PROTO_LABELS[proto], **style)
        ax_bw.plot(x, s["busbw"], color=PROTO_COLORS[proto], label=PROTO_LABELS[proto], **style)
        labels.append((x[-1], s["busbw"][-1], PROTO_LABELS[proto].split(" (")[0]))

    style_axes(ax_lat, "Latency (median, min–max band)", "µs per all-reduce", xlabel)
    log_time_axis(ax_lat)
    legend(ax_lat)
    style_axes(ax_bw, "Bus bandwidth (× 2(N−1)/N, per link)", "Gb/s", xlabel)
    ax_bw.set_ylim(bottom=0)
    end_labels(ax_bw, labels)

    cap = int(d["cfg"].get("rndv_cap_bytes") or 0)
    if cap and xkey == "bytes" and "eager" in series and cap < max(series["eager"]["bytes"]):
        for ax in (ax_lat, ax_bw):
            ax.axvline(cap, color=TEXT_SECONDARY, linestyle=":", linewidth=1)
        ax_bw.annotate("rendezvous memlock cap", (cap, 0), xytext=(-4, 6),
                       textcoords="offset points", ha="right", color=TEXT_SECONDARY, fontsize=8)

    style_axes(ax_ratio, "Rendezvous / eager time", "ratio (< 1: rendezvous faster)", xlabel)
    ax_ratio.axhline(1, color=TEXT_SECONDARY, linewidth=1)
    if "eager" in series and "rndv" in series:
        xs, r, lo, hi, clearly = ratio_series(series["eager"], series["rndv"], xkey)
        if xs:
            x_cross = draw_ratio(ax_ratio, xs, r, lo, hi, clearly, SERIES_2, "rendezvous / eager")
            if x_cross:
                ax_ratio.axvline(x_cross, color=TEXT_PRIMARY, linestyle=":", linewidth=1)
                ax_ratio.annotate(f"crossover {human_size(x_cross)}", (x_cross, 1), xytext=(6, 10),
                                  textcoords="offset points", color=TEXT_PRIMARY, fontsize=9)
            else:
                ax_ratio.annotate("no clear crossover", (0.98, 0.95), xycoords="axes fraction",
                                  ha="right", va="top", color=TEXT_SECONDARY, fontsize=9)

    errors = sum(s["errors"] for s in series.values())
    status = "all results verified" if errors == 0 else f"{errors} WRONG ELEMENTS"
    cfg = d["cfg"]
    conf = f" · K={cfg['slots']} × {human_size(cfg['slot_size'])}" if cfg.get("slots") else ""
    fig.suptitle(f"RDMA ring all-reduce · {n} nodes · int32 SUM{conf} · {status}",
                 x=0.01, ha="left", color=TEXT_PRIMARY, fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(png_path, dpi=150, facecolor=SURFACE)
    print(f"Saved {png_path}")


def plot_compare(png_path, csv_paths, xkey="bytes"):
    runs = sorted((load(p) for p in csv_paths), key=lambda d: d["num_nodes"])
    xlabel = "Chunk size (bytes per rank)" if xkey == "chunk" else "Message size"

    # One row per ring size: latency and bus bandwidth, protocols in their fixed colors
    fig, axes = plt.subplots(len(runs), 2, figsize=(13, 4.2 * len(runs)), facecolor=SURFACE, squeeze=False)
    for row, d in zip(axes, runs):
        series = by_protocol(d)
        for proto in PROTO_ORDER:
            s = series.get(proto)
            if not s:
                continue
            style = dict(LINE, linestyle="--") if proto == "legacy" else LINE
            row[0].fill_between(s[xkey], s["min"], s["max"], color=PROTO_COLORS[proto], alpha=0.15, linewidth=0)
            row[0].plot(s[xkey], s["med"], color=PROTO_COLORS[proto], label=PROTO_LABELS[proto], **style)
            row[1].plot(s[xkey], s["busbw"], color=PROTO_COLORS[proto], label=PROTO_LABELS[proto], **style)
        style_axes(row[0], f"{d['num_nodes']} nodes · latency", "µs per all-reduce", xlabel)
        log_time_axis(row[0])
        style_axes(row[1], f"{d['num_nodes']} nodes · bus bandwidth", "Gb/s", xlabel)
        row[1].set_ylim(bottom=0)
        legend(row[0])
    errors = sum(s["errors"] for d in runs for s in d["series"].values())
    status = "all results verified" if errors == 0 else f"{errors} WRONG ELEMENTS"
    fig.suptitle(f"RDMA ring all-reduce · eager vs rendezvous by ring size · int32 SUM · {status}",
                 x=0.01, ha="left", color=TEXT_PRIMARY, fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(png_path, dpi=150, facecolor=SURFACE)
    print(f"Saved {png_path}")

    # Ratio per ring size on one panel; here color identifies the ring size
    fig, ax = plt.subplots(figsize=(8, 5.2), facecolor=SURFACE)
    notes = []
    for d in runs:
        series = by_protocol(d)
        if "eager" not in series or "rndv" not in series:
            continue
        xs, r, lo, hi, clearly = ratio_series(series["eager"], series["rndv"], xkey)
        if not xs:
            continue
        n = d["num_nodes"]
        x_cross = draw_ratio(ax, xs, r, lo, hi, clearly, NODE_COLORS.get(n, TEXT_SECONDARY), f"{n} nodes")
        notes.append(f"{n} nodes: {human_size(x_cross) if x_cross else 'none'}")
    ax.axhline(1, color=TEXT_SECONDARY, linewidth=1)
    style_axes(ax, "Rendezvous / eager time", "ratio (< 1: rendezvous faster)", xlabel)
    legend(ax, loc="upper right")
    if notes:
        ax.annotate("crossover  " + " · ".join(notes), (0.01, 0.02), xycoords="axes fraction",
                    color=TEXT_SECONDARY, fontsize=9)
    fig.tight_layout()
    root, ext = os.path.splitext(png_path)
    ratio_png = f"{root}_ratio{ext or '.png'}"
    fig.savefig(ratio_png, dpi=150, facecolor=SURFACE)
    print(f"Saved {ratio_png}")


def plot_ablation(column, png_path, csv_paths, xkey="bytes"):
    runs = [load(p) for p in csv_paths]
    for d in runs:
        if column not in d["cfg"]:
            sys.exit(f"{d['path']} has no column '{column}'")
    runs.sort(key=lambda d: float(d["cfg"][column]))
    if len(runs) > len(ABLATION_COLORS):
        sys.exit(f"At most {len(ABLATION_COLORS)} values per ablation plot (got {len(runs)}); split them.")
    xlabel = "Chunk size (bytes per rank)" if xkey == "chunk" else "Message size"
    protos = [p for p in ("eager", "rndv") if any(p in by_protocol(d) for d in runs)]

    fig, axes = plt.subplots(1, len(protos), figsize=(6.6 * len(protos), 5.2), facecolor=SURFACE, squeeze=False)
    for ax, proto in zip(axes[0], protos):
        labels = []
        for color, d in zip(ABLATION_COLORS, runs):
            s = by_protocol(d).get(proto)
            if not s:
                continue
            value = d["cfg"][column]
            ax.plot(s[xkey], s["busbw"], color=color, label=f"{column} = {value}", **LINE)
            labels.append((s[xkey][-1], s["busbw"][-1], value))
        style_axes(ax, f"{PROTO_LABELS[proto]} · bus bandwidth", "Gb/s", xlabel)
        ax.set_ylim(bottom=0)
        legend(ax)
        end_labels(ax, labels)
    n = runs[0]["num_nodes"]
    fig.suptitle(f"RDMA ring all-reduce · {n} nodes · ablation of {column}",
                 x=0.01, ha="left", color=TEXT_PRIMARY, fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(png_path, dpi=150, facecolor=SURFACE)
    print(f"Saved {png_path}")


def plot_latency(png_path, csv_paths, xkey="bytes"):
    """Median time per call vs size, one color per ring size; eager/legacy solid, rendezvous dashed"""
    runs = sorted((load(p) for p in csv_paths), key=lambda d: d["num_nodes"])
    xlabel = "Chunk size (bytes per rank)" if xkey == "chunk" else "Message size"
    fig, ax = plt.subplots(figsize=(8, 5.2), facecolor=SURFACE)
    for d in runs:
        n = d["num_nodes"]
        for proto, s in by_protocol(d).items():
            if proto == "auto":
                continue
            style = dict(LINE, linestyle="--") if proto == "rndv" else LINE
            ax.plot(s[xkey], s["med"], color=NODE_COLORS.get(n, TEXT_SECONDARY),
                    label=f"{n} nodes · {PROTO_LABELS[proto]}", **style)
    style_axes(ax, "Time per all-reduce (slowest rank, median)", "µs (log scale)", xlabel)
    log_time_axis(ax)
    legend(ax)
    fig.suptitle("RDMA ring all-reduce · latency vs message size · int32 SUM",
                 x=0.01, ha="left", color=TEXT_PRIMARY, fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(png_path, dpi=150, facecolor=SURFACE)
    print(f"Saved {png_path}")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("files", nargs="*", help="run: [csv] [png]; --compare/--ablation: the CSVs")
    ap.add_argument("--compare", metavar="PNG", help="one row per ring size, plus PNG_ratio")
    ap.add_argument("--ablation", nargs=2, metavar=("COLUMN", "PNG"), help="bus bandwidth per config value")
    ap.add_argument("--latency", metavar="PNG", help="time vs size, one color per ring size")
    ap.add_argument("--baseline", metavar="CSV", help="overlay a legacy run (dashed)")
    ap.add_argument("--x", choices=("bytes", "chunk"), default="bytes", help="x axis: message or chunk size")
    args = ap.parse_args()

    if args.latency:
        if len(args.files) < 1:
            sys.exit(__doc__)
        plot_latency(args.latency, args.files, args.x)
    elif args.compare:
        if len(args.files) < 1:
            sys.exit(__doc__)
        plot_compare(args.compare, args.files, args.x)
    elif args.ablation:
        if len(args.files) < 2:
            sys.exit(__doc__)
        plot_ablation(args.ablation[0], args.ablation[1], args.files, args.x)
    else:
        csv_path = args.files[0] if len(args.files) > 0 else "allreduce_results.csv"
        png_path = args.files[1] if len(args.files) > 1 else "allreduce_results.png"
        plot_run(csv_path, png_path, args.baseline, args.x)


if __name__ == "__main__":
    main()

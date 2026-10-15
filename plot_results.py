#!/usr/bin/env python3
"""Plot ring all-reduce benchmark results (CSV written by rank 0 of rdma_allreduce).

Usage: python3 plot_results.py [allreduce_results.csv] [allreduce_results.png]
       python3 plot_results.py --compare comparison.png run_2nodes.csv run_3nodes.csv ...
"""
import csv
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

SURFACE = "#fcfcfb"
TEXT_PRIMARY = "#0b0b0b"
TEXT_SECONDARY = "#52514e"
GRID = "#e4e3df"
SERIES_1 = "#2a78d6"  # blue
SERIES_2 = "#eb6834"  # orange
SERIES_3 = "#1baf7a"  # aqua
# Fixed color per ring size, so N=4 is the same color whatever subset is plotted
NODE_COLORS = {2: SERIES_1, 3: SERIES_2, 4: SERIES_3}


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
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        sys.exit(f"No rows in {path}")
    return {
        "num_nodes": int(rows[0]["num_nodes"]),
        "bytes": [int(r["bytes"]) for r in rows],
        "time_us": [float(r["avg_time_us"]) for r in rows],
        "algbw": [bw(r, "algbw") for r in rows],
        "busbw": [bw(r, "busbw") for r in rows],
        "errors": sum(int(r["errors"]) for r in rows),
    }


def style_axes(ax, title, ylabel):
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=TEXT_PRIMARY, fontsize=12, fontweight="bold")
    ax.set_xlabel("Message size", color=TEXT_SECONDARY)
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


def plot_compare(png_path, csv_paths):
    runs = sorted((load(p) for p in csv_paths), key=lambda d: d["num_nodes"])
    line = dict(linewidth=2, marker="o", markersize=5,
                markeredgecolor=SURFACE, markeredgewidth=1.5)

    fig, axes = plt.subplots(1, 2, figsize=(13, 5.4), facecolor=SURFACE)
    panels = [
        ("algbw", "Algorithm bandwidth (bits / time)", "Gb/s"),
        ("busbw", "Bus bandwidth (× 2(N−1)/N, per-link)", "Gb/s"),
    ]
    # No end-of-line value labels here: the curves often converge and the labels collide.
    # The legend identifies each ring size; exact values are in each run's table/CSV.
    for ax, (key, title, ylabel) in zip(axes, panels):
        for d in runs:
            n = d["num_nodes"]
            ax.plot(d["bytes"], d[key], color=NODE_COLORS.get(n, TEXT_SECONDARY),
                    label=f"{n} nodes", **line)
        style_axes(ax, title, ylabel)
        ax.set_ylim(bottom=0)
        leg = ax.legend(loc="upper left", frameon=False, fontsize=9)
        for t in leg.get_texts():
            t.set_color(TEXT_PRIMARY)

    errors = sum(d["errors"] for d in runs)
    status = "all results verified" if errors == 0 else f"{errors} WRONG ELEMENTS"
    fig.suptitle(f"RDMA ring all-reduce · ring size comparison · int32 SUM · {status}",
                 x=0.01, ha="left", color=TEXT_PRIMARY, fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(png_path, dpi=150, facecolor=SURFACE)
    print(f"Saved {png_path}")


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--compare":
        if len(sys.argv) < 4:
            sys.exit(__doc__)
        plot_compare(sys.argv[2], sys.argv[3:])
        return
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "allreduce_results.csv"
    png_path = sys.argv[2] if len(sys.argv) > 2 else "allreduce_results.png"
    d = load(csv_path)
    x = d["bytes"]
    line = dict(linewidth=2, marker="o", markersize=5,
                markeredgecolor=SURFACE, markeredgewidth=1.5)

    fig, ax_bw = plt.subplots(figsize=(8, 5.2), facecolor=SURFACE)

    # Bandwidth: two series, legend + direct labels at the last point
    ax_bw.plot(x, d["algbw"], color=SERIES_1, label="Algorithm BW (bits / time)", **line)
    ax_bw.plot(x, d["busbw"], color=SERIES_2, label="Bus BW (× 2(N−1)/N)", **line)
    style_axes(ax_bw, "Bandwidth", "Gb/s")
    ax_bw.set_ylim(bottom=0)
    for ys in (d["algbw"], d["busbw"]):
        ax_bw.annotate(f"{ys[-1]:.2f}", (x[-1], ys[-1]), xytext=(-8, 6),
                       textcoords="offset points", ha="right",
                       color=TEXT_PRIMARY, fontsize=9)
    leg = ax_bw.legend(loc="upper left", frameon=False, fontsize=9)
    for t in leg.get_texts():
        t.set_color(TEXT_PRIMARY)

    status = "all results verified" if d["errors"] == 0 else f"{d['errors']} WRONG ELEMENTS"
    fig.suptitle(f"RDMA ring all-reduce · {d['num_nodes']} nodes · int32 SUM · {status}",
                 x=0.01, ha="left", color=TEXT_PRIMARY, fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    fig.savefig(png_path, dpi=150, facecolor=SURFACE)
    print(f"Saved {png_path}")


if __name__ == "__main__":
    main()

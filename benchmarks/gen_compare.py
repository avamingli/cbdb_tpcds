#!/usr/bin/env python3
"""Generate a side-by-side bar chart comparing two benchmark result folders.

Each folder may contain either:
  - summary.csv  (query_id, status, duration_ms, rows_returned)
  - Individual .txt files with EXPLAIN ANALYZE output (Execution Time parsed)

Usage:
    python3 benchmarks/gen_compare.py <folder_A> <folder_B> [options]

Options:
    -o PATH          Output file (default: benchmarks/comparison.png)
    --label-a NAME   Label for folder A (default: auto-detect)
    --label-b NAME   Label for folder B (default: auto-detect)

Examples:
    # Compare two full benchmark runs (each has summary.csv):
    python3 benchmarks/gen_compare.py benchmarks/2026-03-01_sf100 benchmarks/2026-03-01_sf100_2

    # Compare EXPLAIN plans in separate folders:
    python3 benchmarks/gen_compare.py plans/cte_off plans/cte_on \\
        --label-a "cte_pushdown=off" --label-b "cte_pushdown=on"
"""

import argparse
import csv
import datetime
import os
import re
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np

from chart_utils import fix_text_overlaps


# ── Data loading ─────────────────────────────────────────────────────────────

def load_from_csv(path):
    """Parse a summary.csv file into a list of row dicts."""
    rows = []
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            rows.append({
                "query_id": int(r["query_id"]),
                "status":   r["status"],
                "duration_ms": float(r["duration_ms"]),
                "rows_returned": int(r["rows_returned"]) if r.get("rows_returned") else 0,
            })
    return rows


def load_from_plan_files(dirpath):
    """Scan *.txt files in a directory, parse Execution Time from each."""
    rows = []
    for fname in sorted(os.listdir(dirpath)):
        if not fname.endswith(".txt") or "_result" in fname:
            continue
        m = re.search(r"q(?:uery)?[_-]?(\d+)", fname)
        if not m:
            continue
        qid = int(m.group(1))
        fpath = os.path.join(dirpath, fname)
        with open(fpath) as f:
            text = f.read()
        times = re.findall(r"Execution Time:\s*([\d.]+)\s*ms", text)
        if times:
            rows.append({
                "query_id": qid,
                "status": "OK",
                "duration_ms": float(times[-1]),
                "rows_returned": 0,
            })
        else:
            rows.append({
                "query_id": qid,
                "status": "ERROR",
                "duration_ms": 0,
                "rows_returned": 0,
            })
    return rows


def load_folder(path):
    """Load benchmark data from a folder (summary.csv or .txt plan files)."""
    csv_path = os.path.join(path, "summary.csv")
    if os.path.isfile(csv_path):
        return load_from_csv(csv_path)
    return load_from_plan_files(path)


def auto_label(path):
    """Derive a display label from a folder."""
    return os.path.basename(os.path.normpath(path))


# ── Formatting ───────────────────────────────────────────────────────────────

def fmt_time(secs):
    if secs >= 3600:
        return f"{secs/3600:.1f}h"
    if secs >= 60:
        return f"{secs/60:.1f}min"
    return f"{secs:.0f}s"


# ── Chart ────────────────────────────────────────────────────────────────────

def create_chart(out_path, a_rows, b_rows, a_label, b_label):
    a_map = {r["query_id"]: r for r in a_rows}
    b_map = {r["query_id"]: r for r in b_rows}
    all_qids = sorted(set(a_map) | set(b_map))
    n = len(all_qids)
    detailed = n <= 20  # rich annotations for small sets

    # Build parallel arrays
    a_secs, b_secs = [], []
    a_st, b_st = [], []
    for qid in all_qids:
        if qid in a_map:
            a_secs.append(a_map[qid]["duration_ms"] / 1000.0)
            a_st.append(a_map[qid]["status"])
        else:
            a_secs.append(0)
            a_st.append("SKIP")
        if qid in b_map:
            b_secs.append(b_map[qid]["duration_ms"] / 1000.0)
            b_st.append(b_map[qid]["status"])
        else:
            b_secs.append(0)
            b_st.append("SKIP")

    # ── Y-axis cap ──
    ok_vals = [v for v, s in zip(a_secs + b_secs, a_st + b_st)
               if s == "OK" and v > 0]
    if ok_vals:
        y_cap = sorted(ok_vals)[int(len(ok_vals) * 0.97)] * 1.35
        y_cap = max(y_cap, max(ok_vals) if detailed else 30)
    else:
        y_cap = 300

    # ── Colours ──
    C_A_OK   = "#5B8FF9"
    C_B_OK   = "#5AD8A6"
    C_TIMEOUT = "#F6685E"
    C_SKIP_BG = "#FFF8E1"
    C_FASTER  = "#2E7D32"
    C_SLOWER  = "#C62828"

    x = np.arange(n)
    w = 0.35

    fig_w = max(14, n * 0.32) if not detailed else max(12, n * 2.2)
    fig, ax = plt.subplots(figsize=(fig_w, 7.5))
    ax.set_facecolor("#FAFAFA")
    fig.set_facecolor("white")

    # ── Bar colours ──
    a_colors = []
    b_colors = []
    for s in a_st:
        if s == "OK":     a_colors.append(C_A_OK)
        elif s == "SKIP": a_colors.append("none")
        else:             a_colors.append(C_TIMEOUT)
    for i, s in enumerate(b_st):
        if s == "SKIP":
            b_colors.append("none")
        elif s != "OK":
            b_colors.append(C_TIMEOUT)
        elif detailed and a_st[i] == "OK" and b_secs[i] > a_secs[i] * 1.05:
            b_colors.append(C_TIMEOUT)  # slower → coral in detailed mode
        else:
            b_colors.append(C_B_OK)

    # Clip display heights for large charts
    if detailed:
        a_disp = list(a_secs)
        b_disp = list(b_secs)
        y_top = max(a_secs + b_secs) * 1.25
        ax.set_ylim(-y_top * 0.02, y_top)
    else:
        a_disp = [min(v, y_cap * 0.95) if s != "SKIP" else 0
                  for v, s in zip(a_secs, a_st)]
        b_disp = [min(v, y_cap * 0.95) if s != "SKIP" else 0
                  for v, s in zip(b_secs, b_st)]
        y_top = y_cap * 1.12
        ax.set_ylim(0, y_top)

    # Draw bars
    ax.bar(x - w/2, a_disp, w, color=a_colors, edgecolor="white",
           linewidth=0.4, zorder=2)
    ax.bar(x + w/2, b_disp, w, color=b_colors, edgecolor="white",
           linewidth=0.4, zorder=2)

    # ── SKIP columns (large chart only) ──
    if not detailed:
        for i in range(n):
            if a_st[i] == "SKIP" and b_st[i] == "SKIP":
                ax.axvspan(i - 0.45, i + 0.45, color=C_SKIP_BG, zorder=0)
                ax.text(i, y_cap * 0.5, "SKIP", ha="center", va="center",
                        fontsize=7, color="#E65100", fontweight="bold",
                        bbox=dict(boxstyle="round,pad=0.25", facecolor="#FFF3E0",
                                  edgecolor="#FFB74D", linewidth=0.8),
                        zorder=5)

    # ── Annotations ──
    for i in range(n):
        av, bv = a_secs[i], b_secs[i]

        if detailed:
            # Time labels above each bar
            if a_st[i] == "OK":
                ax.text(i - w/2, av + y_top * 0.01, f"{av:.0f}s",
                        ha="center", va="bottom", fontsize=8,
                        color="#3366AA", zorder=5)
            if b_st[i] == "OK":
                ax.text(i + w/2, bv + y_top * 0.01, f"{bv:.0f}s",
                        ha="center", va="bottom", fontsize=8,
                        color="#227744" if bv <= av else "#AA3333", zorder=5)
            # Ratio text above the taller bar
            if a_st[i] == "OK" and b_st[i] == "OK" and av > 0:
                ratio = av / bv if bv > 0 else 1
                bar_top = max(av, bv)
                if ratio > 1.05:
                    txt = f"{ratio:.1f}x faster"
                    color = C_FASTER
                elif ratio < 0.95:
                    txt = f"{1/ratio:.1f}x slower"
                    color = C_SLOWER
                else:
                    txt = "~same"
                    color = "#666666"
                ax.text(i, bar_top + y_top * 0.06, txt, ha="center",
                        va="bottom", fontsize=10, fontweight="bold",
                        color=color, zorder=5)
        else:
            # Clipped-bar annotations
            a_clipped = a_st[i] != "SKIP" and av > y_cap * 0.95
            b_clipped = b_st[i] != "SKIP" and bv > y_cap * 0.95
            if a_clipped and b_clipped:
                al, bl = fmt_time(av), fmt_time(bv)
                if al == bl:
                    ax.text(i, y_cap * 1.01, al, ha="center", va="bottom",
                            fontsize=5.5, fontweight="bold", color="#B71C1C",
                            zorder=5)
                else:
                    ax.text(i - w/2, y_cap * 0.97, al, ha="center",
                            va="bottom", fontsize=5.5, fontweight="bold",
                            color="#B71C1C" if "TIMEOUT" in a_st[i] else "#1565C0",
                            zorder=5)
                    ax.text(i + w/2, y_cap * 1.04, bl, ha="center",
                            va="bottom", fontsize=5.5, fontweight="bold",
                            color="#B71C1C" if "TIMEOUT" in b_st[i] else "#388E3C",
                            zorder=5)
            else:
                if a_clipped:
                    ax.text(i - w/2, y_cap * 0.97, fmt_time(av), ha="center",
                            va="bottom", fontsize=5.5, fontweight="bold",
                            color="#B71C1C" if "TIMEOUT" in a_st[i] else "#1565C0",
                            zorder=5)
                if b_clipped:
                    ax.text(i + w/2, y_cap * 0.97, fmt_time(bv), ha="center",
                            va="bottom", fontsize=5.5, fontweight="bold",
                            color="#B71C1C" if "TIMEOUT" in b_st[i] else "#388E3C",
                            zorder=5)

            # Speedup / regression markers (large chart)
            if a_st[i] == "OK" and b_st[i] == "OK" and av > 2 and bv > 0.5:
                ratio = av / bv
                bar_top = min(max(av, bv), y_cap * 0.95)
                if ratio > 1.5:
                    txt = f"{ratio:.0f}x" if ratio >= 2.5 else f"{ratio:.1f}x"
                    ax.text(i, bar_top + y_cap * 0.02, txt, ha="center",
                            va="bottom", fontsize=6.5, color=C_FASTER,
                            fontweight="bold", zorder=5)
                elif ratio < 0.67:
                    inv = 1.0 / ratio
                    txt = f"{inv:.0f}x" if inv >= 2.5 else f"{inv:.1f}x"
                    ax.text(i, bar_top + y_cap * 0.02, txt, ha="center",
                            va="bottom", fontsize=6.5, color=C_SLOWER,
                            fontweight="bold", zorder=5)

    # ── Axes ──
    tick_fs = 10 if detailed else 7.5
    ax.set_xticks(x)
    ax.set_xticklabels([f"Q{q}" if detailed else str(q) for q in all_qids],
                       fontsize=tick_fs, fontweight="bold" if detailed else "normal")
    ax.set_xlabel("Query" if detailed else "Query ID", fontsize=11, labelpad=8)
    ax.set_ylabel("Execution Time (seconds)" if detailed else "Duration (seconds)",
                  fontsize=11)
    ax.set_xlim(-0.6, n - 0.4)
    ax.yaxis.grid(True, linestyle="--", alpha=0.2, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#CCCCCC")
    ax.spines["bottom"].set_color("#CCCCCC")
    ax.tick_params(colors="#666666")

    # ── Title ──
    a_ok = sum(1 for s in a_st if s == "OK")
    b_ok = sum(1 for s in b_st if s == "OK")
    a_tot = sum(a_secs)
    b_tot = sum(b_secs)
    if detailed:
        title = f"{a_label} vs {b_label}"
    else:
        title = (f"{a_label}: {a_ok}/{n} OK, {fmt_time(a_tot)}  vs  "
                 f"{b_label}: {b_ok}/{n} OK, {fmt_time(b_tot)}")
    ax.set_title(title, fontsize=13 if not detailed else 14,
                 fontweight="bold", pad=14)

    # ── Legend ──
    has_timeout = any("TIMEOUT" in s for s in a_st + b_st)
    has_skip = any(s == "SKIP" for s in a_st + b_st)
    handles = [
        Patch(facecolor=C_A_OK, edgecolor="white", label=a_label),
        Patch(facecolor=C_B_OK, edgecolor="white",
              label=f"{b_label} (faster)" if detailed else b_label),
    ]
    if detailed:
        handles.append(Patch(facecolor=C_TIMEOUT, edgecolor="white",
                             label=f"{b_label} (slower)"))
    elif has_timeout:
        handles.append(Patch(facecolor=C_TIMEOUT, edgecolor="white",
                             label="Timeout"))
    if has_skip:
        handles.append(Patch(facecolor="#FFF3E0", edgecolor="#FFB74D",
                             label="Skip"))
    ax.legend(handles=handles,
              loc="upper right" if detailed else "upper left",
              fontsize=9 if not detailed else 10,
              framealpha=0.95, edgecolor="#DDDDDD", fancybox=True)

    # ── Date ──
    date_str = datetime.date.today().isoformat()
    ax.annotate(date_str, xy=(1, 0), xycoords="axes fraction",
                xytext=(-8, 6), textcoords="offset points",
                ha="right", va="bottom", fontsize=8, color="#AAAAAA")

    fig.subplots_adjust(left=0.06, right=0.97, top=0.90,
                        bottom=0.10 if detailed else 0.08)

    n_adj = fix_text_overlaps(fig, ax)
    if n_adj:
        print(f"  auto-fixed {n_adj} text overlap(s)")
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"  wrote {out_path}")

    # ── Summary ──
    improved = regressed = 0
    for i in range(n):
        if a_st[i] == "OK" and b_st[i] == "OK" and a_secs[i] > 0 and b_secs[i] > 0:
            ratio = a_secs[i] / b_secs[i]
            if ratio > 1.5:
                improved += 1
            elif ratio < 0.67:
                regressed += 1
    print(f"  Improved (>1.5x faster): {improved} queries")
    print(f"  Regressed (>1.5x slower): {regressed} queries")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Compare two benchmark result folders side-by-side.",
        epilog="Each folder may contain summary.csv or individual .txt plan files.",
    )
    parser.add_argument("folder_a", help="First result folder (shown in blue)")
    parser.add_argument("folder_b", help="Second result folder (shown in green)")
    parser.add_argument("-o", "--output", default=None,
                        help="Output PNG path (default: benchmarks/comparison.png)")
    parser.add_argument("--label-a", default=None, help="Label for folder A")
    parser.add_argument("--label-b", default=None, help="Label for folder B")
    args = parser.parse_args()

    for d in (args.folder_a, args.folder_b):
        if not os.path.isdir(d):
            print(f"ERROR: {d} is not a directory", file=sys.stderr)
            sys.exit(1)

    a_rows = load_folder(args.folder_a)
    b_rows = load_folder(args.folder_b)
    if not a_rows:
        print(f"ERROR: No data found in {args.folder_a}", file=sys.stderr)
        sys.exit(1)
    if not b_rows:
        print(f"ERROR: No data found in {args.folder_b}", file=sys.stderr)
        sys.exit(1)

    a_label = args.label_a or auto_label(args.folder_a)
    b_label = args.label_b or auto_label(args.folder_b)

    out_path = args.output
    if out_path is None:
        out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "comparison.png")

    print(f"Comparing: {a_label} vs {b_label}")
    print(f"  folder A: {args.folder_a} ({len(a_rows)} queries)")
    print(f"  folder B: {args.folder_b} ({len(b_rows)} queries)")
    create_chart(out_path, a_rows, b_rows, a_label, b_label)


if __name__ == "__main__":
    main()

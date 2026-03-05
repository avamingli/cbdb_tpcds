#!/usr/bin/env python3
"""Generate TPC-DS benchmark chart and report from bench_summary.

Connects to PostgreSQL using standard environment variables ($PGHOST,
$PGPORT, $PGDATABASE, $PGUSER, $PGPASSWORD) and reads tpcds.bench_summary.

Creates a dated subdirectory under benchmarks/ with:
  - queries.png   bar chart of per-query timing
  - summary.csv   query_id, status, duration_ms, rows_returned
  - meta.json     PG version, system info, summary statistics

Also regenerates benchmarks/README.md to index all historical runs.

Usage:
    python3 benchmarks/gen_chart.py
"""

import csv
import datetime
import glob
import json
import os
import platform
import socket
import statistics
import sys

import psycopg2

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

from chart_utils import fix_text_overlaps

SCHEMA = "tpcds"
BENCH_NAME = "TPC-DS"


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def check_schema(conn):
    """Verify the tpcds extension schema and bench_summary table exist."""
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = 'bench_summary'",
        (SCHEMA,),
    )
    if cur.fetchone() is None:
        print(
            f"ERROR: {SCHEMA}.bench_summary not found.\n"
            f"  Run the following in psql first:\n"
            f"    CREATE EXTENSION {SCHEMA};\n"
            f"    SELECT {SCHEMA}.bench();",
            file=sys.stderr,
        )
        sys.exit(1)


def fetch_bench_data(conn):
    """Fetch latest benchmark results from bench_summary."""
    cur = conn.cursor()
    cur.execute(
        f"SELECT query_id, status, duration_ms, rows_returned, run_ts "
        f"FROM {SCHEMA}.bench_summary ORDER BY query_id"
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def fetch_metadata(conn):
    """Collect PG version, GUCs, scale factor, system info."""
    cur = conn.cursor()
    meta = {}

    cur.execute("SELECT version()")
    meta["pg_version"] = cur.fetchone()[0]

    cur.execute("SHOW server_version_num")
    meta["pg_version_num"] = int(cur.fetchone()[0])

    for guc in ("shared_buffers", "work_mem", "effective_cache_size",
                "max_parallel_workers_per_gather", "max_connections"):
        cur.execute(f"SHOW {guc}")
        meta[f"pg_{guc}"] = cur.fetchone()[0]

    # Scale factor from extension config table
    cur.execute(f"SELECT value FROM {SCHEMA}.config WHERE key = 'scale_factor'")
    row = cur.fetchone()
    meta["scale_factor"] = int(row[0]) if row and row[0] else None

    # System info
    meta["hostname"]  = socket.gethostname()
    meta["os"]        = platform.platform()
    meta["arch"]      = platform.machine()
    meta["cpu_count"] = os.cpu_count()

    return meta


# ---------------------------------------------------------------------------
# Output generation
# ---------------------------------------------------------------------------

def make_run_dir(base_dir, date_str, sf):
    dirname = f"{date_str}_sf{sf}"
    run_dir = os.path.join(base_dir, dirname)
    if os.path.exists(run_dir):
        i = 2
        while os.path.exists(f"{run_dir}_{i}"):
            i += 1
        run_dir = f"{run_dir}_{i}"
    os.makedirs(run_dir, exist_ok=True)
    return run_dir


def write_summary_csv(run_dir, rows):
    path = os.path.join(run_dir, "summary.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["query_id", "status", "duration_ms", "rows_returned"])
        w.writeheader()
        for r in rows:
            w.writerow({
                "query_id":      r["query_id"],
                "status":        r["status"],
                "duration_ms":   float(r["duration_ms"]),
                "rows_returned": r["rows_returned"],
            })


def create_chart(run_dir, rows, sf, date_str):
    """Generate queries.png — vertical bar chart of per-query duration."""
    query_ids = [r["query_id"] for r in rows]
    durations = [float(r["duration_ms"]) for r in rows]
    statuses  = [r["status"] for r in rows]

    colors = ["#4CAF50" if s == "OK" else "#F44336" for s in statuses]

    fig_w = max(14, len(query_ids) * 0.18)
    fig, ax = plt.subplots(figsize=(fig_w, 6))

    # Detect outliers: if max > 3x the 95th percentile, clip Y axis
    sorted_d = sorted(durations)
    p95 = sorted_d[int(len(sorted_d) * 0.95)] if len(sorted_d) >= 5 else max(sorted_d)
    y_cap = None
    if max(durations) > 3 * p95 and p95 > 0:
        y_cap = p95 * 2.0

    bars = ax.bar(range(len(query_ids)), durations, color=colors, edgecolor="none", width=0.8)

    # Annotate clipped bars with their actual value
    if y_cap:
        ax.set_ylim(0, y_cap * 1.12)
        for i, (d, bar) in enumerate(zip(durations, bars)):
            if d > y_cap:
                bar.set_height(y_cap * 1.05)
                label = f"{d/1000:.0f}s" if d >= 1000 else f"{d:.0f}"
                ax.text(i, y_cap * 1.07, label, ha="center", va="bottom",
                        fontsize=6, fontweight="bold", color="#B71C1C")

    # X axis
    ax.set_xticks(range(len(query_ids)))
    ax.set_xticklabels([str(q) for q in query_ids], fontsize=7, rotation=90)
    ax.set_xlabel("Query ID")

    # Y axis
    ax.set_ylabel("Duration (ms)")

    # Title
    total_ms = sum(durations)
    total_str = f"{total_ms/1000:.1f}s" if total_ms >= 60000 else f"{total_ms:.0f}ms"
    ok_count  = sum(1 for s in statuses if s == "OK")
    err_count = len(statuses) - ok_count

    title = f"{BENCH_NAME} SF={sf} — {ok_count}/{len(query_ids)} OK"
    if err_count:
        title += f", {err_count} errors"
    title += f"  |  Total: {total_str}"
    ax.set_title(title, fontsize=12, fontweight="bold")

    # Legend
    handles = [Patch(facecolor="#4CAF50", label="OK")]
    if err_count:
        handles.append(Patch(facecolor="#F44336", label="ERROR"))
    ax.legend(handles=handles, loc="upper right")

    # Grid
    ax.yaxis.grid(True, linestyle="--", alpha=0.3)
    ax.set_axisbelow(True)

    # Date annotation
    ax.annotate(date_str, xy=(1, 0), xycoords="axes fraction",
                xytext=(-5, 5), textcoords="offset points",
                ha="right", va="bottom", fontsize=8, color="gray")

    plt.tight_layout()
    fix_text_overlaps(fig, ax)
    fig.savefig(os.path.join(run_dir, "queries.png"), dpi=150)
    plt.close(fig)


def write_meta_json(run_dir, rows, metadata, sf, date_str):
    durations = [float(r["duration_ms"]) for r in rows]
    ok_count  = sum(1 for r in rows if r["status"] == "OK")
    err_count = len(rows) - ok_count
    total_ms  = sum(durations)

    ok_durations = [float(r["duration_ms"]) for r in rows if r["status"] == "OK"]
    fastest_q = min(rows, key=lambda r: float(r["duration_ms"])) if ok_durations else None
    slowest_q = max(rows, key=lambda r: float(r["duration_ms"])) if ok_durations else None

    meta = {
        "benchmark":    BENCH_NAME,
        "schema":       SCHEMA,
        "date":         date_str,
        "scale_factor": sf,
        "run_ts":       rows[0]["run_ts"].isoformat() if rows[0].get("run_ts") else None,
        "postgresql": {
            "version":     metadata.get("pg_version", ""),
            "version_num": metadata.get("pg_version_num", 0),
            **{k.replace("pg_", ""): v for k, v in metadata.items()
               if k.startswith("pg_") and k not in ("pg_version", "pg_version_num")},
        },
        "system": {
            "hostname":  metadata.get("hostname", ""),
            "os":        metadata.get("os", ""),
            "arch":      metadata.get("arch", ""),
            "cpu_count": metadata.get("cpu_count", 0),
        },
        "results": {
            "total_queries":     len(rows),
            "ok_count":          ok_count,
            "error_count":       err_count,
            "total_duration_ms": round(total_ms, 2),
            "total_duration_s":  round(total_ms / 1000, 1),
            "min_duration_ms":   round(min(durations), 2) if durations else 0,
            "max_duration_ms":   round(max(durations), 2) if durations else 0,
            "median_duration_ms": round(statistics.median(durations), 2) if durations else 0,
            "fastest_query":     fastest_q["query_id"] if fastest_q else None,
            "slowest_query":     slowest_q["query_id"] if slowest_q else None,
        },
    }

    path = os.path.join(run_dir, "meta.json")
    with open(path, "w") as f:
        json.dump(meta, f, indent=2, default=str)


def update_readme(base_dir):
    """Regenerate README.md from all run directories."""
    # Scan run directories
    pattern = os.path.join(base_dir, "????-??-??_sf*")
    run_dirs = sorted(glob.glob(pattern), reverse=True)

    runs = []
    for d in run_dirs:
        meta_path = os.path.join(d, "meta.json")
        if not os.path.exists(meta_path):
            continue
        with open(meta_path) as f:
            runs.append((os.path.basename(d), json.load(f)))

    if not runs:
        return

    latest_dir, latest = runs[0]
    res = latest["results"]

    total_s = res["total_duration_s"]
    total_str = f"{total_s:.1f}s"

    lines = []
    lines.append(f"# {BENCH_NAME} Benchmark Results\n")

    # Latest run
    lines.append("## Latest Run\n")
    lines.append(f"**Date:** {latest['date']} | "
                 f"**Scale Factor:** {latest['scale_factor']} | "
                 f"**Total:** {total_str} | "
                 f"**Status:** {res['ok_count']}/{res['total_queries']} OK\n")
    lines.append(f"![Latest benchmark results](./{latest_dir}/queries.png)\n")

    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    pg_ver = latest["postgresql"]["version"]
    pg_short = pg_ver.split(",")[0] if "," in pg_ver else pg_ver
    lines.append(f"| PostgreSQL | {pg_short} |")
    lines.append(f"| Scale Factor | {latest['scale_factor']} |")
    lines.append(f"| Total Time | {total_str} |")
    lines.append(f"| Queries OK | {res['ok_count']}/{res['total_queries']} |")
    if res.get("fastest_query") is not None:
        lines.append(f"| Fastest | Q{res['fastest_query']} ({res['min_duration_ms']:.0f}ms) |")
        lines.append(f"| Slowest | Q{res['slowest_query']} ({res['max_duration_ms']:.0f}ms) |")
        lines.append(f"| Median | {res['median_duration_ms']:.0f}ms |")
    lines.append("")

    # All runs table
    if len(runs) > 1:
        lines.append("## All Runs\n")
        lines.append("| Date | SF | Total | OK | Errors | Details |")
        lines.append("|------|----|-------|----|--------|---------|")
        for dirname, meta in runs:
            r = meta["results"]
            t = f"{r['total_duration_s']:.1f}s"
            lines.append(
                f"| [{meta['date']}](./{dirname}/) | {meta['scale_factor']} | "
                f"{t} | {r['ok_count']} | {r['error_count']} | "
                f"[csv](./{dirname}/summary.csv) [chart](./{dirname}/queries.png) |"
            )
        lines.append("")

    lines.append("---")
    lines.append("*Generated by `gen_chart.py`*")
    lines.append("")

    readme_path = os.path.join(base_dir, "README.md")
    with open(readme_path, "w") as f:
        f.write("\n".join(lines))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # Connect using standard PG environment variables
    try:
        conn = psycopg2.connect()
    except Exception as e:
        print(f"ERROR: Cannot connect to PostgreSQL: {e}", file=sys.stderr)
        sys.exit(1)

    # Verify schema exists
    check_schema(conn)

    # Fetch data
    rows = fetch_bench_data(conn)
    if not rows:
        print(
            f"ERROR: {SCHEMA}.bench_summary is empty.\n"
            f"  Run SELECT {SCHEMA}.bench() first.",
            file=sys.stderr,
        )
        conn.close()
        sys.exit(1)

    metadata = fetch_metadata(conn)
    conn.close()

    sf = metadata.get("scale_factor") or "unknown"
    date_str = datetime.date.today().isoformat()

    # Output to tpcds_results dir next to the extension
    import subprocess
    try:
        sharedir = subprocess.check_output(["pg_config", "--sharedir"], text=True).strip()
        base_dir = os.path.join(sharedir, "extension", "tpcds_results")
    except Exception:
        base_dir = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(base_dir, exist_ok=True)

    # Create run directory
    run_dir = make_run_dir(base_dir, date_str, sf)
    run_name = os.path.basename(run_dir)

    print(f"{BENCH_NAME} benchmark report")
    print(f"  SF: {sf}  Date: {date_str}")
    print(f"  Queries: {len(rows)}")

    # Generate outputs
    write_summary_csv(run_dir, rows)
    print(f"  wrote {run_name}/summary.csv")

    create_chart(run_dir, rows, sf, date_str)
    print(f"  wrote {run_name}/queries.png")

    write_meta_json(run_dir, rows, metadata, sf, date_str)
    print(f"  wrote {run_name}/meta.json")

    update_readme(base_dir)
    print(f"  updated README.md")

    # Summary
    durations = [float(r["duration_ms"]) for r in rows]
    total_ms = sum(durations)
    total_str = f"{total_ms/1000:.1f}s" if total_ms >= 1000 else f"{total_ms:.0f}ms"
    ok = sum(1 for r in rows if r["status"] == "OK")
    print(f"\nTotal: {total_str}  ({ok}/{len(rows)} OK)")


if __name__ == "__main__":
    main()

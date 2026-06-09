"""
Parameter sensitivity experiment for PostGIS-based spatial DBSCAN pipeline.

Tests combinations of congestion filter thresholds and clustering parameters.
Outputs a CSV and Markdown summary to outputs/experiments/.

Prerequisites:
  docker-compose up -d
  python download_data.py
  python ingest_data.py
  python create_views.py   (not strictly required; uses ibb_traffic_density directly)

Usage:
  python experiments/parameter_sensitivity.py
  python experiments/parameter_sensitivity.py --quick   # reduced grid for fast testing
  python experiments/parameter_sensitivity.py --top5    # include top-5 cluster detail
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from itertools import product
from pathlib import Path

# Allow running from project root or experiments/ directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DB_CONFIG, ensure_cli_logging  # noqa: E402

import logging
import psycopg2

logger = logging.getLogger(__name__)

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "experiments"
CSV_OUTPUT = OUTPUT_DIR / "parameter_sensitivity.csv"
MD_OUTPUT = OUTPUT_DIR / "parameter_sensitivity_summary.md"

# ── Parameter grid ──────────────────────────────────────────────────────────
SPEED_THRESHOLDS = [10, 15, 20, 25]        # km/h  (avg_speed < threshold)
VEHICLE_THRESHOLDS = [300, 500, 700, 1000] # vehicle_count > threshold
EPS_VALUES = [250, 500, 750, 1000]         # metres (EPSG:32636)
MINPOINTS_VALUES = [3, 5, 10]

QUICK_SPEED_THRESHOLDS = [15, 20]
QUICK_VEHICLE_THRESHOLDS = [500, 700]
QUICK_EPS_VALUES = [250, 500]
QUICK_MINPOINTS_VALUES = [3, 5]

CSV_COLUMNS = [
    "speed_threshold_kmh",
    "vehicle_threshold",
    "eps_meters",
    "minpoints",
    "filtered_count",
    "cluster_count",
    "noise_count",
    "noise_pct",
    "avg_cluster_size",
    "top_ais_score",
    "runtime_s",
]

# ── SQL ──────────────────────────────────────────────────────────────────────
PREFLIGHT_TABLE_SQL = """
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'ibb_traffic_density';
"""

PREFLIGHT_ROW_SQL = "SELECT COUNT(*)::int FROM ibb_traffic_density;"

# One query: filtered count + clustered aggregates.
# Geometry column is EPSG:32636, so eps is in metres.
SENSITIVITY_SQL = """
WITH filtered AS (
    SELECT geom, vehicle_count, avg_speed, record_time
    FROM ibb_traffic_density
    WHERE avg_speed < %(speed)s
      AND vehicle_count > %(vehicle)s
),
total_filtered AS (
    SELECT COUNT(*)::int AS n FROM filtered
),
clustered AS (
    SELECT
        vehicle_count,
        avg_speed,
        record_time,
        COALESCE(
            ST_ClusterDBSCAN(geom, eps := %(eps)s, minpoints := %(minpoints)s) OVER (),
            -1
        ) AS cluster_id
    FROM filtered
),
cluster_agg AS (
    SELECT
        COUNT(*)::int AS total_rows,
        COUNT(DISTINCT CASE WHEN cluster_id >= 0 THEN cluster_id END)::int AS cluster_count,
        COUNT(CASE WHEN cluster_id = -1 THEN 1 END)::int AS noise_count,
        AVG(CASE WHEN cluster_id >= 0 THEN vehicle_count ELSE NULL END)::float AS avg_volume,
        AVG(CASE WHEN cluster_id >= 0 THEN avg_speed ELSE NULL END)::float AS avg_speed_in_clusters
    FROM clustered
),
per_cluster AS (
    SELECT
        cluster_id,
        AVG(vehicle_count)::float AS avg_vol,
        AVG(avg_speed)::float AS avg_spd,
        EXTRACT(EPOCH FROM (MAX(record_time) - MIN(record_time))) / 3600.0 AS dur_h,
        COUNT(DISTINCT record_time::date)::int AS rec_days
    FROM clustered
    WHERE cluster_id >= 0
    GROUP BY cluster_id
)
SELECT
    tf.n AS filtered_count,
    ca.cluster_count,
    ca.noise_count,
    ca.total_rows,
    pc_stats.max_vol,
    pc_stats.max_spd_drop,
    pc_stats.max_dur,
    pc_stats.max_rec
FROM total_filtered tf
CROSS JOIN cluster_agg ca
CROSS JOIN (
    SELECT
        MAX(avg_vol)   AS max_vol,
        MAX(1.0 - avg_spd / 35.0) AS max_spd_drop,
        MAX(dur_h)     AS max_dur,
        MAX(rec_days)  AS max_rec
    FROM per_cluster
) pc_stats;
"""

TOP5_SQL = """
WITH filtered AS (
    SELECT geom, vehicle_count, avg_speed, record_time, geohash
    FROM ibb_traffic_density
    WHERE avg_speed < %(speed)s
      AND vehicle_count > %(vehicle)s
),
clustered AS (
    SELECT
        vehicle_count, avg_speed, record_time, geohash,
        COALESCE(
            ST_ClusterDBSCAN(geom, eps := %(eps)s, minpoints := %(minpoints)s) OVER (),
            -1
        ) AS cluster_id
    FROM filtered
)
SELECT
    cluster_id,
    COUNT(*)::int AS point_count,
    AVG(vehicle_count)::float AS avg_volume,
    AVG(avg_speed)::float AS avg_speed,
    EXTRACT(EPOCH FROM (MAX(record_time) - MIN(record_time))) / 3600.0 AS duration_h,
    COUNT(DISTINCT record_time::date)::int AS recurrence_days
FROM clustered
WHERE cluster_id >= 0
GROUP BY cluster_id
ORDER BY AVG(vehicle_count) DESC
LIMIT 5;
"""


def _preflight(conn) -> bool:
    """Return True if DB is ready with data; print guidance and return False otherwise."""
    with conn.cursor() as cur:
        cur.execute(PREFLIGHT_TABLE_SQL)
        exists = cur.fetchone()[0]
        if not exists:
            logger.error(
                "\nibb_traffic_density table not found.\n"
                "Run the following to prepare the database:\n\n"
                "  docker-compose up -d\n"
                "  python download_data.py\n"
                "  python ingest_data.py\n"
                "  python create_views.py\n"
            )
            return False
        cur.execute(PREFLIGHT_ROW_SQL)
        n_rows = cur.fetchone()[0]
        if n_rows == 0:
            logger.error(
                "\nibb_traffic_density exists but is empty.\n"
                "Run:\n\n"
                "  python download_data.py\n"
                "  python ingest_data.py\n"
            )
            return False
    logger.info(f"  ibb_traffic_density: {n_rows:,} rows — OK")
    return True


def _compute_ais(max_vol, max_spd_drop, max_dur, max_rec) -> float | None:
    """Rough top-AIS estimate from the per-cluster maxima. Not identical to full AIS."""
    if max_vol is None:
        return None
    # Each normalised component is 1.0 at the maximum — this is the upper-bound AIS
    # for the best cluster, assuming all four maxima belong to the same cluster.
    return round(0.30 * 1.0 + 0.30 * min(1.0, float(max_spd_drop or 0))
                 + 0.25 * 1.0 + 0.15 * 1.0, 4)


def run_experiment(
    conn,
    speed: int,
    vehicle: int,
    eps: float,
    minpoints: int,
    include_top5: bool = False,
) -> dict:
    params = {"speed": speed, "vehicle": vehicle, "eps": eps, "minpoints": minpoints}
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(SENSITIVITY_SQL, params)
        row = cur.fetchone()
    elapsed = round(time.perf_counter() - t0, 2)

    filtered_count, cluster_count, noise_count, total_rows, max_vol, max_spd_drop, max_dur, max_rec = row

    noise_pct = round(100.0 * noise_count / total_rows, 2) if total_rows else 0.0
    avg_cluster_size = (
        round((total_rows - noise_count) / cluster_count, 1)
        if cluster_count else 0.0
    )
    top_ais = _compute_ais(max_vol, max_spd_drop, max_dur, max_rec)

    result = {
        "speed_threshold_kmh": speed,
        "vehicle_threshold": vehicle,
        "eps_meters": eps,
        "minpoints": minpoints,
        "filtered_count": filtered_count,
        "cluster_count": cluster_count,
        "noise_count": noise_count,
        "noise_pct": noise_pct,
        "avg_cluster_size": avg_cluster_size,
        "top_ais_score": top_ais if top_ais is not None else "",
        "runtime_s": elapsed,
    }

    if include_top5 and cluster_count > 0:
        with conn.cursor() as cur:
            cur.execute(TOP5_SQL, params)
            result["top5"] = cur.fetchall()

    return result


def write_csv(rows: list[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(CSV_OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"\nCSV output: {CSV_OUTPUT}")


def write_markdown(rows: list[dict], grid_desc: str, total_time: float) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Parameter Sensitivity Analysis",
        "",
        "**Methodology:** PostGIS-based Spatial DBSCAN with Temporal Recurrence Analysis",
        "**Note:** top_ais_score is an upper-bound estimate. Full AIS requires per-cluster normalisation across all clusters in the run.",
        "",
        f"**Grid:** {grid_desc}",
        f"**Total combinations:** {len(rows)}",
        f"**Total runtime:** {total_time:.1f}s",
        "",
        "| speed_kmh | vehicle | eps_m | minpts | filtered | clusters | noise | noise_pct | avg_size | top_ais | runtime_s |",
        "|-----------|---------|-------|--------|----------|----------|-------|-----------|----------|---------|-----------|",
    ]
    for r in rows:
        lines.append(
            f"| {r['speed_threshold_kmh']} | {r['vehicle_threshold']} | "
            f"{r['eps_meters']} | {r['minpoints']} | {r['filtered_count']:,} | "
            f"{r['cluster_count']} | {r['noise_count']:,} | {r['noise_pct']}% | "
            f"{r['avg_cluster_size']} | {r['top_ais_score']} | {r['runtime_s']}s |"
        )
    lines += [
        "",
        "## Limitations",
        "",
        "- `top_ais_score` is an upper-bound approximation, not a full AIS computation.",
        "- Area-based dynamic density filtering is not applied here; see `dynamic_density.py`.",
        "- Results depend on the data currently loaded in ibb_traffic_density.",
    ]
    with open(MD_OUTPUT, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    logger.info(f"Markdown output: {MD_OUTPUT}")


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(
        description="Parameter sensitivity experiment for PostGIS spatial DBSCAN pipeline"
    )
    parser.add_argument("--quick", action="store_true", help="Reduced grid for fast testing")
    parser.add_argument("--top5", action="store_true", help="Include top-5 cluster detail per run")
    args = parser.parse_args()

    if args.quick:
        speeds = QUICK_SPEED_THRESHOLDS
        vehicles = QUICK_VEHICLE_THRESHOLDS
        eps_vals = QUICK_EPS_VALUES
        minpts_vals = QUICK_MINPOINTS_VALUES
        grid_label = "quick"
    else:
        speeds = SPEED_THRESHOLDS
        vehicles = VEHICLE_THRESHOLDS
        eps_vals = EPS_VALUES
        minpts_vals = MINPOINTS_VALUES
        grid_label = "full"

    combos = list(product(speeds, vehicles, eps_vals, minpts_vals))
    logger.info("=" * 60)
    logger.info("Parameter Sensitivity Experiment")
    logger.info("=" * 60)
    logger.info(f"  Grid        : {grid_label}")
    logger.info(f"  Combinations: {len(combos)}")
    logger.info(f"  Speeds      : {speeds}")
    logger.info(f"  Vehicles    : {vehicles}")
    logger.info(f"  Eps (m)     : {eps_vals}")
    logger.info(f"  Minpoints   : {minpts_vals}")
    logger.info("")

    try:
        logger.info("Connecting to DB...")
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        logger.error(
            f"\nCannot connect to the database: {e}\n\n"
            "Start it with:\n\n"
            "  docker-compose up -d\n"
        )
        sys.exit(1)

    logger.info("Running preflight checks...")
    if not _preflight(conn):
        conn.close()
        sys.exit(1)

    results = []
    t_total_start = time.perf_counter()
    for idx, (speed, vehicle, eps, minpts) in enumerate(combos, 1):
        logger.info(
            f"[{idx:3d}/{len(combos)}] speed<{speed} vehicle>{vehicle} "
            f"eps={eps}m minpts={minpts} ..."
        )
        try:
            row = run_experiment(conn, speed, vehicle, eps, minpts, include_top5=args.top5)
            logger.info(
                f"          filtered={row['filtered_count']:,}  "
                f"clusters={row['cluster_count']}  "
                f"noise={row['noise_pct']}%  "
                f"time={row['runtime_s']}s"
            )
            results.append(row)
        except Exception as exc:
            logger.error(f"          ERROR: {exc}")
            results.append({
                "speed_threshold_kmh": speed, "vehicle_threshold": vehicle,
                "eps_meters": eps, "minpoints": minpts,
                "filtered_count": "ERROR", "cluster_count": "ERROR",
                "noise_count": "ERROR", "noise_pct": "ERROR",
                "avg_cluster_size": "ERROR", "top_ais_score": "ERROR",
                "runtime_s": "ERROR",
            })

    total_time = time.perf_counter() - t_total_start
    conn.close()

    grid_desc = (
        f"speed∈{speeds}, vehicle∈{vehicles}, "
        f"eps∈{eps_vals}m, minpoints∈{minpts_vals}"
    )
    write_csv(results)
    write_markdown(results, grid_desc, total_time)

    logger.info(f"\nExperiment complete. {len(results)} combinations in {total_time:.1f}s")
    logger.info(f"Outputs:\n  {CSV_OUTPUT}\n  {MD_OUTPUT}")


if __name__ == "__main__":
    main()

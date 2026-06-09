"""
Road-Network-Aware Density Filtering — comparison experiment.

Compares three congestion candidate selection methods:
  1. Static baseline:     avg_speed < 20 AND vehicle_count > 500
  2. Geohash area:        vehicles / geohash_cell_area_km2  (area approximation)
  3. Road-network density: vehicles / road_length_km_in_cell (PostGIS road network)

Prerequisites:
  docker-compose up -d
  python download_data.py && python ingest_data.py && python create_views.py
  python scripts/create_road_schema.py
  python scripts/import_road_network.py --input data/road_network/Istanbul.osm.pbf

Outputs:
  outputs/experiments/road_density_comparison.csv
  outputs/experiments/road_density_comparison.md
  outputs/experiments/road_density_filter_comparison.png  (if matplotlib available)

Usage:
  python experiments/road_density.py
  python experiments/road_density.py --percentile 80 --speed-threshold 20
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    DB_CONFIG,
    HIGH_CONGESTION_MAX_AVG_SPEED,
    HIGH_CONGESTION_MIN_VEHICLE_COUNT,
    ROAD_DENSITY_PERCENTILE_THRESHOLD,
    ensure_cli_logging,
)

import psycopg2  # noqa: E402

logger = logging.getLogger(__name__)

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "experiments"
CSV_OUTPUT = OUTPUT_DIR / "road_density_comparison.csv"
MD_OUTPUT = OUTPUT_DIR / "road_density_comparison.md"
PNG_OUTPUT = OUTPUT_DIR / "road_density_filter_comparison.png"

# ── SQL ──────────────────────────────────────────────────────────────────────

_STATIC_GEOHASHES_SQL = """
SELECT DISTINCT geohash
FROM ibb_traffic_density
WHERE avg_speed < %(speed)s
  AND vehicle_count > %(vehicle)s
  AND geohash IS NOT NULL;
"""

_STATIC_COUNT_SQL = """
SELECT COUNT(*)::int FROM ibb_traffic_density
WHERE avg_speed < %(speed)s AND vehicle_count > %(vehicle)s;
"""

_ROAD_DENSITY_AVAILABLE_SQL = """
SELECT
    (SELECT COUNT(*) FROM road_segments)::int            AS road_segments,
    (SELECT COUNT(*) FROM geohash_road_lengths)::int     AS road_length_cells,
    (SELECT COUNT(NULLIF(road_length_km, 0))
     FROM geohash_road_lengths)::int                     AS cells_with_roads;
"""

_GEOHASH_AGG_SQL = """
SELECT
    geohash,
    AVG(vehicle_count)::float       AS avg_vehicle_count,
    AVG(avg_speed)::float           AS avg_avg_speed,
    COUNT(*)::int                   AS record_count
FROM ibb_traffic_density
WHERE geohash IS NOT NULL
GROUP BY geohash;
"""

_ROAD_DENSITY_PER_GEOHASH_SQL = """
SELECT
    t.geohash,
    grl.road_length_km,
    grl.segment_count,
    AVG(t.vehicle_count)::float                                         AS avg_vehicle_count,
    AVG(t.avg_speed)::float                                             AS avg_avg_speed,
    AVG(t.vehicle_count)::float / NULLIF(grl.road_length_km, 0)        AS vehicles_per_road_km,
    COUNT(t.*)::int                                                     AS record_count
FROM ibb_traffic_density t
JOIN geohash_road_lengths grl ON t.geohash = grl.geohash
WHERE grl.road_length_km > 0
GROUP BY t.geohash, grl.road_length_km, grl.segment_count
ORDER BY vehicles_per_road_km DESC NULLS LAST;
"""

_ROAD_LENGTH_STATS_SQL = """
SELECT
    COUNT(*)::int                                   AS total_cells,
    COUNT(NULLIF(road_length_km, 0))::int           AS cells_with_roads,
    ROUND(MIN(road_length_km)::numeric, 3)          AS min_road_km,
    ROUND(percentile_cont(0.25) WITHIN GROUP (ORDER BY road_length_km)::numeric, 3)
                                                    AS p25_road_km,
    ROUND(percentile_cont(0.50) WITHIN GROUP (ORDER BY road_length_km)::numeric, 3)
                                                    AS median_road_km,
    ROUND(percentile_cont(0.75) WITHIN GROUP (ORDER BY road_length_km)::numeric, 3)
                                                    AS p75_road_km,
    ROUND(MAX(road_length_km)::numeric, 3)          AS max_road_km,
    ROUND(AVG(road_length_km)::numeric, 3)          AS avg_road_km,
    ROUND(SUM(road_length_km)::numeric, 1)          AS total_road_km
FROM geohash_road_lengths
WHERE road_length_km > 0;
"""


def _check_preflight(conn) -> dict:
    """Return info about available data tables."""
    cur = conn.cursor()
    status = {}

    cur.execute("SELECT COUNT(*)::int FROM ibb_traffic_density")
    status["traffic_rows"] = cur.fetchone()[0]

    # road_segments and geohash_cells are regular tables — use information_schema
    for table in ("road_segments", "geohash_cells"):
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema='public' AND table_name=%s
        """, (table,))
        exists = cur.fetchone()[0] > 0
        if exists:
            cur.execute(f"SELECT COUNT(*)::int FROM {table}")
            status[table] = cur.fetchone()[0]
        else:
            status[table] = 0

    # geohash_road_lengths is a MATERIALIZED VIEW — use pg_matviews
    cur.execute(
        "SELECT ispopulated FROM pg_matviews WHERE matviewname = 'geohash_road_lengths'"
    )
    mv_row = cur.fetchone()
    if mv_row and mv_row[0]:
        cur.execute("SELECT COUNT(*)::int FROM geohash_road_lengths")
        status["road_length_cells"] = cur.fetchone()[0]
    else:
        status["road_length_cells"] = 0

    return status


def _compute_road_density_candidates(
    road_rows: list[dict], percentile: float
) -> tuple[set[str], float]:
    """Select geohashes above the percentile threshold for vehicles_per_road_km."""
    values = sorted(r["vehicles_per_road_km"] for r in road_rows if r["vehicles_per_road_km"])
    if not values:
        return set(), 0.0
    idx = max(0, int(len(values) * percentile / 100) - 1)
    threshold = values[idx]
    selected = {r["geohash"] for r in road_rows if (r["vehicles_per_road_km"] or 0) >= threshold}
    return selected, threshold


def _write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_md(
    output_path: Path,
    params: dict,
    summary: dict,
    road_stats: dict,
    road_rows: list[dict],
    static_geohashes: set[str],
    road_geohashes: set[str],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    overlap = static_geohashes & road_geohashes
    static_only = static_geohashes - road_geohashes
    road_only = road_geohashes - static_geohashes

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# Road-Network-Aware Density Filtering — Comparison Report\n\n")
        f.write(
            "> **Method**: Vehicles per road kilometre (vehicles_per_road_km)\n"
            "> computed by intersecting geohash cell polygons with OSM road segment\n"
            "> geometries in EPSG:32636 using PostGIS ST_Intersection + ST_Length.\n"
            "> This is a genuine road-length-aware metric, not an area approximation.\n\n"
            "> **Limitation**: OSM dual carriageways may cause minor double-counting\n"
            "> of road length. Pedestrian/non-vehicle highway classes are excluded.\n\n"
        )

        f.write("## Parameters\n\n")
        f.write("| Parameter | Value |\n|-----------|-------|\n")
        for k, v in params.items():
            f.write(f"| {k} | {v} |\n")

        f.write("\n## Road Length Distribution (per geohash cell)\n\n")
        f.write("| Metric | Value |\n|--------|-------|\n")
        for k, v in road_stats.items():
            f.write(f"| {k} | {v} |\n")

        f.write("\n## Filter Comparison\n\n")
        f.write("| Metric | Count |\n|--------|-------|\n")
        for k, v in summary.items():
            f.write(f"| {k} | {v} |\n")

        f.write(f"\n| Overlap (static ∩ road-density) | {len(overlap):,} |\n")
        f.write(f"| Static-only (missed by road-density) | {len(static_only):,} |\n")
        f.write(f"| Road-density-only (missed by static) | {len(road_only):,} |\n")

        # Top 10 by vehicles_per_road_km
        if road_rows:
            f.write("\n## Top 10 Geohashes by vehicles_per_road_km\n\n")
            f.write("| geohash | road_km | veh/road-km | avg_speed | avg_vehicles |\n")
            f.write("|---------|---------|-------------|-----------|-------------|\n")
            for r in road_rows[:10]:
                f.write(
                    f"| {r['geohash']} | {r['road_length_km']:.3f} | "
                    f"{r['vehicles_per_road_km']:.1f} | {r['avg_avg_speed']:.1f} | "
                    f"{r['avg_vehicle_count']:.0f} |\n"
                )

        f.write("\n## Limitations\n\n")
        f.write(
            "- `vehicles_per_road_km` uses road length within the geohash cell bounding box.\n"
            "- OSM dual carriageways (motorways, trunk roads) may be double-counted.\n"
            "- Road length is computed using EPSG:32636 (UTM Zone 36N); minor distortion\n"
            "  exists for western Istanbul (lon ~28–29°E) which falls in UTM Zone 35N.\n"
            "- Geohash cell area does not align exactly with road network coverage.\n"
        )


def _plot_comparison(
    static_geohashes: set[str],
    road_geohashes: set[str],
    geohash_area_geohashes: set[str],
    output_path: Path,
) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("matplotlib not installed — skipping chart generation")
        return

    methods = ["Static\n(speed+count)", "Geohash\narea", "Road-network\ndensity"]
    counts = [len(static_geohashes), len(geohash_area_geohashes), len(road_geohashes)]
    colors = ["#3498db", "#e67e22", "#2ecc71"]

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(methods, counts, color=colors, edgecolor="white", linewidth=1.5)
    for bar, count in zip(bars, counts):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + max(counts) * 0.01,
            f"{count:,}",
            ha="center", va="bottom", fontsize=11, fontweight="bold",
        )
    ax.set_title("Congestion Candidate Geohashes — Method Comparison", fontsize=13)
    ax.set_ylabel("Number of selected geohash cells")
    ax.set_ylim(0, max(counts) * 1.15)
    ax.grid(axis="y", alpha=0.3)
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
    logger.info(f"Chart saved: {output_path}")


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(
        description="Compare static, geohash-area, and road-network density filtering"
    )
    parser.add_argument("--percentile", type=float, default=ROAD_DENSITY_PERCENTILE_THRESHOLD)
    parser.add_argument("--speed-threshold", type=int, default=HIGH_CONGESTION_MAX_AVG_SPEED)
    parser.add_argument("--vehicle-threshold", type=int, default=HIGH_CONGESTION_MIN_VEHICLE_COUNT)
    args = parser.parse_args()

    logger.info("=" * 65)
    logger.info("Road-Network-Aware Density Filtering — Comparison")
    logger.info("=" * 65)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as exc:
        logger.error(f"Cannot connect to database: {exc}\nStart: docker-compose up -d")
        sys.exit(1)

    from experiments._dataset_check import print_dataset_coverage
    print_dataset_coverage(conn)

    status = _check_preflight(conn)
    logger.info(f"Traffic rows: {status['traffic_rows']:,}")
    logger.info(f"Road segments: {status.get('road_segments', 0):,}")
    logger.info(f"Geohash road-length cells: {status.get('road_length_cells', 0):,}")

    if status.get("road_segments", 0) == 0:
        logger.error(
            "\nroad_segments is empty — cannot run road-density comparison.\n\n"
            "Download Istanbul OSM extract (~75 MB):\n"
            "  https://download.bbbike.org/osm/bbbike/Istanbul/Istanbul.osm.pbf\n\n"
            "Save to: data/road_network/Istanbul.osm.pbf\n\n"
            "Then run:\n"
            "  python scripts/create_road_schema.py\n"
            "  python scripts/import_road_network.py\n"
            "  python experiments/road_density.py"
        )
        sys.exit(1)

    if status.get("road_length_cells", 0) == 0:
        logger.error(
            "\ngeohash_road_lengths is empty — refresh it:\n"
            "  python scripts/create_road_schema.py --refresh"
        )
        sys.exit(1)

    cur = conn.cursor()

    # ── Static candidates ─────────────────────────────────────────────────
    cur.execute(_STATIC_GEOHASHES_SQL, {"speed": args.speed_threshold, "vehicle": args.vehicle_threshold})
    static_geohashes = {r[0] for r in cur.fetchall()}
    cur.execute(_STATIC_COUNT_SQL, {"speed": args.speed_threshold, "vehicle": args.vehicle_threshold})
    static_record_count = cur.fetchone()[0]

    # ── Road density candidates ───────────────────────────────────────────
    cur.execute(_ROAD_DENSITY_PER_GEOHASH_SQL)
    road_rows = [
        {
            "geohash": r[0],
            "road_length_km": float(r[1]),
            "segment_count": int(r[2]),
            "avg_vehicle_count": float(r[3]),
            "avg_avg_speed": float(r[4]),
            "vehicles_per_road_km": float(r[5]) if r[5] else None,
            "record_count": int(r[6]),
        }
        for r in cur.fetchall()
    ]
    road_geohashes, road_threshold = _compute_road_density_candidates(road_rows, args.percentile)

    # ── Geohash area candidates (from dynamic_density logic if CSV available) ─
    area_csv = OUTPUT_DIR / "density_comparison.csv"
    geohash_area_geohashes: set[str] = set()
    if area_csv.exists():
        import csv as _csv
        with open(area_csv, "r", encoding="utf-8") as f:
            for row in _csv.DictReader(f):
                if row.get("in_density_filter", "").lower() == "true":
                    geohash_area_geohashes.add(row["geohash"])

    # ── Road length distribution stats ───────────────────────────────────
    cur.execute(_ROAD_LENGTH_STATS_SQL)
    r = cur.fetchone()
    if r:
        road_stats = {
            "Cells with road coverage": f"{r[1]:,} / {r[0]:,} ({100 * r[1] / max(r[0], 1):.1f}%)",
            "Min road length": f"{r[2]} km",
            "25th percentile": f"{r[3]} km",
            "Median": f"{r[4]} km",
            "75th percentile": f"{r[5]} km",
            "Max road length": f"{r[6]} km",
            "Mean road length": f"{r[7]} km",
            "Total road length": f"{r[8]} km",
        }
    else:
        road_stats = {}

    # ── Report ────────────────────────────────────────────────────────────
    overlap = static_geohashes & road_geohashes
    static_only = static_geohashes - road_geohashes
    road_only = road_geohashes - static_geohashes

    logger.info(f"\nStatic filter:       {len(static_geohashes):>5,} geohashes, {static_record_count:,} records")
    logger.info(f"Road-density filter: {len(road_geohashes):>5,} geohashes "
                f"(threshold: {road_threshold:.2f} veh/road-km, p{args.percentile:.0f})")
    if geohash_area_geohashes:
        logger.info(f"Geohash-area filter: {len(geohash_area_geohashes):>5,} geohashes (from prior experiment)")
    logger.info(f"\nOverlap:             {len(overlap):,}")
    logger.info(f"Static-only:         {len(static_only):,}")
    logger.info(f"Road-density-only:   {len(road_only):,}")
    if road_stats:
        logger.info(f"\nRoad coverage: {road_stats.get('Cells with road coverage', 'N/A')}")
        logger.info(f"Total network: {road_stats.get('Total road length', 'N/A')}")

    # ── CSV output ────────────────────────────────────────────────────────
    csv_rows = []
    all_geohashes = static_geohashes | road_geohashes | geohash_area_geohashes
    road_by_gh = {r["geohash"]: r for r in road_rows}
    for gh in sorted(all_geohashes):
        rd = road_by_gh.get(gh, {})
        csv_rows.append({
            "geohash": gh,
            "in_static_filter": gh in static_geohashes,
            "in_road_density_filter": gh in road_geohashes,
            "in_geohash_area_filter": gh in geohash_area_geohashes,
            "road_length_km": rd.get("road_length_km", ""),
            "segment_count": rd.get("segment_count", ""),
            "avg_vehicle_count": round(rd["avg_vehicle_count"], 2) if rd.get("avg_vehicle_count") else "",
            "vehicles_per_road_km": round(rd["vehicles_per_road_km"], 2) if rd.get("vehicles_per_road_km") else "",
            "avg_avg_speed": round(rd["avg_avg_speed"], 2) if rd.get("avg_avg_speed") else "",
        })
    _write_csv(csv_rows, CSV_OUTPUT)
    logger.info(f"\nCSV: {CSV_OUTPUT}")

    params = {
        "Speed threshold": f"< {args.speed_threshold} km/h",
        "Vehicle threshold (static)": f"> {args.vehicle_threshold}",
        "Road-density percentile": f"p{args.percentile:.0f}",
        "Road-density threshold": f"{road_threshold:.2f} vehicles/road-km",
    }
    summary = {
        "Total unique geohash cells": f"{len(all_geohashes):,}",
        "Static filter candidates": f"{len(static_geohashes):,}",
        "Road-density candidates": f"{len(road_geohashes):,}",
    }
    if geohash_area_geohashes:
        summary["Geohash-area candidates"] = f"{len(geohash_area_geohashes):,}"

    _write_md(MD_OUTPUT, params, summary, road_stats, road_rows,
              static_geohashes, road_geohashes)
    logger.info(f"MD:  {MD_OUTPUT}")

    _plot_comparison(static_geohashes, road_geohashes, geohash_area_geohashes, PNG_OUTPUT)

    conn.close()


if __name__ == "__main__":
    main()

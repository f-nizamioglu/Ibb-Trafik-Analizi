"""
Geohash Cell Density Prototype — area-based congestion candidate filter.

IMPORTANT: This is an area-based approximation, not true road-length-aware
traffic density. True road-length-aware filtering requires a road network
layer (e.g. OSM road segments) and density per road kilometre using PostGIS
functions such as ST_Length, ST_Intersection, and ST_Buffer.

Since no road network dataset exists in this repository, this prototype
estimates spatial density using geohash cell area:

    vehicles_per_km2 = avg_vehicle_count / cell_area_km2

A percentile-based threshold is applied to select high-density cells.
The result is compared against the static baseline filter (avg_speed < 20
AND vehicle_count > 500).

Outputs:
  outputs/experiments/density_comparison.csv
  outputs/experiments/density_comparison.md

Usage:
  python experiments/dynamic_density.py
  python experiments/dynamic_density.py --percentile 80
  python experiments/dynamic_density.py --percentile 75 --min-vehicles 100
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DB_CONFIG, DENSITY_PERCENTILE_THRESHOLD, ensure_cli_logging  # noqa: E402

import logging
import psycopg2

logger = logging.getLogger(__name__)

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "experiments"
CSV_OUTPUT = OUTPUT_DIR / "density_comparison.csv"
MD_OUTPUT = OUTPUT_DIR / "density_comparison.md"

# ── Geohash decoder (no external dependency) ─────────────────────────────────
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def _decode_geohash_bounds(gh: str) -> tuple[float, float, float, float]:
    """Return (min_lat, min_lon, max_lat, max_lon) for a geohash cell."""
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    is_lon = True
    for char in gh:
        try:
            bits = _BASE32.index(char)
        except ValueError:
            raise ValueError(f"Invalid geohash character: '{char}'")
        for i in range(4, -1, -1):
            bit = (bits >> i) & 1
            if is_lon:
                mid = (lon_range[0] + lon_range[1]) / 2
                if bit:
                    lon_range[0] = mid
                else:
                    lon_range[1] = mid
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                if bit:
                    lat_range[0] = mid
                else:
                    lat_range[1] = mid
            is_lon = not is_lon
    return lat_range[0], lon_range[0], lat_range[1], lon_range[1]


def cell_area_km2(gh: str) -> float:
    """Approximate cell area in km² using the geohash bounding box."""
    min_lat, min_lon, max_lat, max_lon = _decode_geohash_bounds(gh)
    lat_km = (max_lat - min_lat) * 111.1
    mid_lat_rad = math.radians((min_lat + max_lat) / 2)
    lon_km = (max_lon - min_lon) * 111.1 * math.cos(mid_lat_rad)
    return lat_km * lon_km


# ── SQL ──────────────────────────────────────────────────────────────────────
PREFLIGHT_SQL = """
SELECT COUNT(*) FROM information_schema.tables
WHERE table_schema = 'public' AND table_name = 'ibb_traffic_density';
"""

PREFLIGHT_ROW_SQL = "SELECT COUNT(*)::int FROM ibb_traffic_density;"

# Per-geohash aggregates (no speed/vehicle pre-filter — full dataset)
GEOHASH_AGG_SQL = """
SELECT
    geohash,
    COUNT(*)::int                   AS record_count,
    AVG(vehicle_count)::float       AS avg_vehicle_count,
    AVG(avg_speed)::float           AS avg_speed
FROM ibb_traffic_density
WHERE geohash IS NOT NULL AND geohash != ''
GROUP BY geohash
ORDER BY geohash;
"""

# Static baseline candidates (used for comparison)
STATIC_CANDIDATES_SQL = """
SELECT DISTINCT geohash
FROM ibb_traffic_density
WHERE avg_speed < %(speed)s
  AND vehicle_count > %(vehicle)s
  AND geohash IS NOT NULL AND geohash != '';
"""

STATIC_RECORD_COUNT_SQL = """
SELECT COUNT(*)::int FROM ibb_traffic_density
WHERE avg_speed < %(speed)s AND vehicle_count > %(vehicle)s;
"""


def compute_density_candidates(
    geohash_aggs: list[dict],
    percentile: float,
    min_vehicles: int,
) -> tuple[set[str], float]:
    """
    Apply percentile-based density threshold to select high-density geohash cells.

    Returns (set of selected geohashes, density threshold used).
    """
    # Only consider cells with enough vehicle activity
    active = [r for r in geohash_aggs if r["avg_vehicle_count"] >= min_vehicles]
    if not active:
        return set(), 0.0

    densities = sorted(r["vehicles_per_km2"] for r in active)
    idx = max(0, int(len(densities) * percentile / 100) - 1)
    threshold = densities[idx]

    selected = {r["geohash"] for r in active if r["vehicles_per_km2"] >= threshold}
    return selected, threshold


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(
        description="Geohash Cell Density Prototype — area-based congestion candidate filter"
    )
    parser.add_argument(
        "--percentile",
        type=float,
        default=DENSITY_PERCENTILE_THRESHOLD,
        help=f"Density percentile threshold (default: {DENSITY_PERCENTILE_THRESHOLD})",
    )
    parser.add_argument(
        "--min-vehicles",
        type=int,
        default=50,
        help="Minimum avg_vehicle_count to include a cell in density ranking (default: 50)",
    )
    parser.add_argument(
        "--speed-threshold",
        type=int,
        default=20,
        help="Static baseline speed threshold km/h (default: 20)",
    )
    parser.add_argument(
        "--vehicle-threshold",
        type=int,
        default=500,
        help="Static baseline vehicle_count threshold (default: 500)",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Geohash Cell Density Prototype")
    logger.info("=" * 60)
    logger.info(
        "NOTE: Area-based approximation only. Not true road-length-aware density."
    )
    logger.info(
        "  True road-length-aware density requires OSM road segment data\n"
        "  and PostGIS functions (ST_Length, ST_Intersection, ST_Buffer).\n"
        "  Since no road network dataset exists in this repository,\n"
        "  cell area is used as a proxy for road length coverage."
    )
    logger.info("")

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except psycopg2.OperationalError as e:
        logger.error(
            f"\nCannot connect to the database: {e}\n\n"
            "Start it with:\n\n  docker-compose up -d\n"
        )
        sys.exit(1)

    with conn.cursor() as cur:
        cur.execute(PREFLIGHT_SQL)
        if not cur.fetchone()[0]:
            logger.error(
                "\nibb_traffic_density table not found.\nRun:\n\n"
                "  docker-compose up -d\n"
                "  python download_data.py\n"
                "  python ingest_data.py\n"
            )
            conn.close()
            sys.exit(1)
        cur.execute(PREFLIGHT_ROW_SQL)
        n_rows = cur.fetchone()[0]
        if n_rows == 0:
            logger.error("\nibb_traffic_density is empty.\nRun python ingest_data.py first.\n")
            conn.close()
            sys.exit(1)
    logger.info(f"  ibb_traffic_density: {n_rows:,} rows — OK\n")

    # ── Load per-geohash aggregates ───────────────────────────────────────
    logger.info("Loading per-geohash aggregates ...")
    with conn.cursor() as cur:
        cur.execute(GEOHASH_AGG_SQL)
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description]

    geohash_aggs = [dict(zip(col_names, r)) for r in rows]
    n_unique_geohashes = len(geohash_aggs)
    logger.info(f"  Unique geohash cells: {n_unique_geohashes:,}")

    # ── Compute cell areas ────────────────────────────────────────────────
    logger.info("Computing geohash cell areas ...")
    error_cells = 0
    for rec in geohash_aggs:
        try:
            area = cell_area_km2(rec["geohash"])
            rec["cell_area_km2"] = round(area, 6)
            rec["vehicles_per_km2"] = (
                round(rec["avg_vehicle_count"] / area, 2) if area > 0 else 0.0
            )
        except (ValueError, ZeroDivisionError):
            rec["cell_area_km2"] = None
            rec["vehicles_per_km2"] = 0.0
            error_cells += 1

    if error_cells:
        logger.warning(f"  {error_cells} geohash cells could not be decoded and were skipped.")

    # ── Static baseline candidates ────────────────────────────────────────
    logger.info("Loading static baseline candidates ...")
    params = {"speed": args.speed_threshold, "vehicle": args.vehicle_threshold}
    with conn.cursor() as cur:
        cur.execute(STATIC_CANDIDATES_SQL, params)
        static_geohashes = {r[0] for r in cur.fetchall()}
        cur.execute(STATIC_RECORD_COUNT_SQL, params)
        static_record_count = cur.fetchone()[0]

    logger.info(
        f"  Static filter (avg_speed<{args.speed_threshold} AND vehicle_count>{args.vehicle_threshold}): "
        f"{len(static_geohashes):,} unique geohashes, {static_record_count:,} records"
    )

    # ── Density prototype candidates ──────────────────────────────────────
    density_geohashes, density_threshold = compute_density_candidates(
        geohash_aggs,
        percentile=args.percentile,
        min_vehicles=args.min_vehicles,
    )
    logger.info(
        f"  Density prototype ({args.percentile}th percentile, "
        f"min_vehicles>={args.min_vehicles}): {len(density_geohashes):,} unique geohashes"
    )
    logger.info(f"  Density threshold applied: >= {density_threshold:.2f} vehicles/km²")

    # ── Comparison ────────────────────────────────────────────────────────
    overlap = static_geohashes & density_geohashes
    static_only = static_geohashes - density_geohashes
    density_only = density_geohashes - static_geohashes

    logger.info("\n--- Comparison Summary ---")
    logger.info(f"  Overlap (both methods)    : {len(overlap):,}")
    logger.info(f"  Static-only candidates    : {len(static_only):,}")
    logger.info(f"  Density-only candidates   : {len(density_only):,}")

    # ── Write outputs ─────────────────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # CSV: per-geohash with both filter flags
    csv_rows = []
    geohash_lookup = {r["geohash"]: r for r in geohash_aggs}
    all_geohashes = static_geohashes | density_geohashes
    for gh in sorted(all_geohashes):
        rec = geohash_lookup.get(gh, {})
        csv_rows.append({
            "geohash": gh,
            "record_count": rec.get("record_count", ""),
            "avg_vehicle_count": round(rec.get("avg_vehicle_count") or 0, 2),
            "avg_speed_kmh": round(rec.get("avg_speed") or 0, 2),
            "cell_area_km2": rec.get("cell_area_km2", ""),
            "vehicles_per_km2": rec.get("vehicles_per_km2", ""),
            "in_static_filter": gh in static_geohashes,
            "in_density_filter": gh in density_geohashes,
            "filter_method": (
                "both" if gh in overlap
                else ("static_only" if gh in static_only else "density_only")
            ),
        })

    csv_columns = [
        "geohash", "record_count", "avg_vehicle_count", "avg_speed_kmh",
        "cell_area_km2", "vehicles_per_km2",
        "in_static_filter", "in_density_filter", "filter_method",
    ]
    with open(CSV_OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_columns)
        writer.writeheader()
        writer.writerows(csv_rows)
    logger.info(f"\nCSV output: {CSV_OUTPUT}")

    # Markdown summary
    md_lines = [
        "# Geohash Cell Density Prototype — Comparison Report",
        "",
        "> **Important:** This is an area-based approximation, not true road-length-aware",
        "> traffic density. True road-length-aware filtering requires importing a road",
        "> network layer (e.g. OSM road segments) and calculating vehicle density per road",
        "> kilometre using PostGIS functions such as `ST_Length`, `ST_Intersection`, and",
        "> `ST_Buffer`. No road network data exists in this repository.",
        "",
        "## Parameters",
        "",
        f"| Parameter | Value |",
        f"|-----------|-------|",
        f"| Static speed threshold | < {args.speed_threshold} km/h |",
        f"| Static vehicle threshold | > {args.vehicle_threshold} vehicles |",
        f"| Density percentile | {args.percentile}th |",
        f"| Min vehicles (density ranking) | >= {args.min_vehicles} |",
        f"| Density threshold applied | >= {density_threshold:.2f} vehicles/km² |",
        "",
        "## Results",
        "",
        f"| Metric | Count |",
        f"|--------|-------|",
        f"| Total unique geohash cells | {n_unique_geohashes:,} |",
        f"| Static filter candidates | {len(static_geohashes):,} geohashes |",
        f"| Static filter records | {static_record_count:,} |",
        f"| Density prototype candidates | {len(density_geohashes):,} geohashes |",
        f"| Overlap (both methods) | {len(overlap):,} geohashes |",
        f"| Static-only | {len(static_only):,} geohashes |",
        f"| Density-only | {len(density_only):,} geohashes |",
        "",
        "## Limitations",
        "",
        "- `vehicles_per_km2` is computed using geohash cell bounding-box area as a proxy.",
        "- Cell areas are approximate and ignore actual road coverage within the cell.",
        "- This is a prototype-level approximation labelled as `filter_method=density_only`.",
        "- Do not claim road-length-aware density filtering based on these results.",
    ]
    with open(MD_OUTPUT, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines) + "\n")
    logger.info(f"Markdown output: {MD_OUTPUT}")

    conn.close()
    logger.info("\nDone.")


if __name__ == "__main__":
    main()

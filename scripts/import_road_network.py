"""
Import Istanbul road network from an OSM PBF file into road_segments table.

Requirements:
  pip install geopandas  (installs pyogrio + shapely + pyproj)

Supported input formats:
  - OSM PBF  (.osm.pbf) — via geopandas + pyogrio GDAL OSM driver
  - GeoPackage (.gpkg)  — via geopandas + pyogrio

Data sources (manual download required):
  BBBike Istanbul extract (RECOMMENDED — ~75 MB):
    https://download.bbbike.org/osm/bbbike/Istanbul/Istanbul.osm.pbf

  Geofabrik Turkey (alternative — ~400 MB, covers whole Turkey):
    https://download.geofabrik.de/europe/turkey-latest.osm.pbf
    After download: clip to Istanbul bounding box in QGIS or osmconvert.

Download and place the file at:
  data/road_network/Istanbul.osm.pbf

Then run:
  python scripts/create_road_schema.py
  python scripts/import_road_network.py --input data/road_network/Istanbul.osm.pbf

Notes:
  - Filters to vehicle-relevant highway classes only.
  - Strips Z coordinates (OSM sometimes includes elevation).
  - Transforms to EPSG:32636 for metric length calculations.
  - Idempotent: uses ON CONFLICT DO NOTHING on osm_id+highway.
  - After import, automatically refreshes geohash_road_lengths matview.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DB_CONFIG, ensure_cli_logging  # noqa: E402

import psycopg2  # noqa: E402
import psycopg2.extras  # noqa: E402

logger = logging.getLogger(__name__)

# ── Vehicle-relevant highway classes (excludes footways, cycleways, etc.) ────
VEHICLE_HIGHWAY_TYPES = {
    "motorway", "trunk", "primary", "secondary", "tertiary",
    "residential", "unclassified", "service",
    "motorway_link", "trunk_link", "primary_link",
    "secondary_link", "tertiary_link",
}

BATCH_SIZE = 500


def _check_geopandas() -> bool:
    try:
        import geopandas  # noqa: F401
        import shapely  # noqa: F401
        return True
    except ImportError:
        logger.error(
            "geopandas is required for road network import.\n"
            "Install with: pip install geopandas\n"
            "  (installs geopandas, pyogrio, shapely, pyproj)"
        )
        return False


def _check_road_schema(conn) -> bool:
    """Verify road_segments table exists."""
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'road_segments'
    """)
    if cur.fetchone()[0] == 0:
        logger.error(
            "road_segments table not found.\n"
            "Create schema first: python scripts/create_road_schema.py"
        )
        return False
    return True


def _check_matview(conn) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_matviews WHERE matviewname = 'geohash_road_lengths'")
    return cur.fetchone() is not None


def read_road_network(input_path: Path) -> "geopandas.GeoDataFrame":
    """Read vehicle-relevant road segments from OSM PBF or GeoPackage."""
    import geopandas as gpd

    suffix = input_path.suffix.lower()
    logger.info(f"Reading {input_path.name} …")

    if suffix in (".pbf", ".osm"):
        # GDAL OSM driver via pyogrio
        gdf = gpd.read_file(str(input_path), layer="lines", engine="pyogrio")
        # Filter to vehicle highway types
        if "highway" not in gdf.columns:
            raise ValueError(
                "OSM file does not expose 'highway' column. "
                "Check GDAL OSM driver configuration."
            )
        gdf = gdf[gdf["highway"].isin(VEHICLE_HIGHWAY_TYPES)].copy()
        logger.info(
            f"  Read {len(gdf):,} road segments "
            f"(filtered from full OSM lines layer)"
        )
    elif suffix in (".gpkg", ".shp"):
        gdf = gpd.read_file(str(input_path), engine="pyogrio")
        if "highway" in gdf.columns:
            gdf = gdf[gdf["highway"].isin(VEHICLE_HIGHWAY_TYPES)].copy()
        logger.info(f"  Read {len(gdf):,} features from {suffix}")
    else:
        raise ValueError(
            f"Unsupported file format: {suffix!r}. "
            "Expected .osm.pbf, .gpkg, or .shp"
        )

    if gdf.empty:
        raise ValueError(
            "No vehicle-relevant road segments found after filtering. "
            "Check that the file covers Istanbul and includes highway tags."
        )

    # Drop rows with null or empty geometry
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty].copy()

    # Explode multipart geometries to LineStrings
    gdf = gdf.explode(index_parts=False).reset_index(drop=True)

    # Strip Z coordinates — PostGIS 2D geometry columns
    from shapely import force_2d
    gdf["geometry"] = gdf["geometry"].apply(force_2d)

    # Ensure CRS is EPSG:4326
    if gdf.crs is None:
        gdf = gdf.set_crs(epsg=4326)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    logger.info(
        f"  After cleaning: {len(gdf):,} line segments, CRS={gdf.crs.to_epsg()}"
    )
    return gdf


def insert_road_segments(conn, gdf: "geopandas.GeoDataFrame") -> int:
    """Batch-insert road segments into road_segments table."""
    from shapely import wkb as shapely_wkb

    cur = conn.cursor()

    # Check existing osm_ids to allow re-runs without duplicates
    cur.execute("SELECT COUNT(*) FROM road_segments")
    existing = cur.fetchone()[0]
    if existing > 0:
        logger.info(f"  road_segments already has {existing:,} rows — using ON CONFLICT DO NOTHING")

    total_inserted = 0
    total_rows = len(gdf)
    start = time.monotonic()

    for batch_start in range(0, total_rows, BATCH_SIZE):
        batch = gdf.iloc[batch_start: batch_start + BATCH_SIZE]
        rows = []
        for _, row in batch.iterrows():
            geom = row.geometry
            if geom is None or geom.is_empty:
                continue
            try:
                wkb_hex = shapely_wkb.dumps(geom, hex=True)
                osm_id = int(row["osm_id"]) if "osm_id" in row and row["osm_id"] else None
                highway = str(row.get("highway", "")) or None
                name = str(row.get("name", "")) or None
                rows.append((osm_id, highway, name, wkb_hex))
            except Exception as exc:
                logger.debug(f"Skipping row: {exc}")
                continue

        if not rows:
            continue

        # Insert geom_4326, then UPDATE geom_32636 + length_m in PostgreSQL
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO road_segments (osm_id, highway, name, geom_4326)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
            template="(%s, %s, %s, ST_Force2D(ST_SetSRID(ST_GeomFromWKB(decode(%s, 'hex')), 4326)))",
        )
        total_inserted += cur.rowcount
        conn.commit()

        if (batch_start // BATCH_SIZE) % 50 == 0 and batch_start > 0:
            pct = 100 * batch_start / total_rows
            elapsed = time.monotonic() - start
            logger.info(f"  {batch_start:,}/{total_rows:,} ({pct:.0f}%) — {elapsed:.1f}s elapsed")

    # Compute geom_32636 and length_m in PostGIS (one bulk UPDATE)
    logger.info("  Computing EPSG:32636 geometries and lengths (PostGIS UPDATE)…")
    cur.execute("""
        UPDATE road_segments
        SET
            geom_32636 = ST_Transform(geom_4326, 32636),
            length_m   = ST_Length(ST_Transform(geom_4326, 32636))
        WHERE geom_32636 IS NULL
    """)
    updated = cur.rowcount
    conn.commit()
    logger.info(f"  Updated {updated:,} rows with geom_32636 and length_m")

    elapsed = time.monotonic() - start
    logger.info(
        f"Import complete: {total_inserted:,} new rows inserted in {elapsed:.1f}s"
    )
    return total_inserted


def _matview_is_populated(conn) -> bool:
    """Return True if geohash_road_lengths has been populated at least once.

    Uses pg_matviews.ispopulated — safe to call even on a WITH NO DATA view.
    Querying the view directly raises ObjectNotInPrerequisiteState when unpopulated.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT ispopulated FROM pg_matviews WHERE matviewname = 'geohash_road_lengths'"
    )
    row = cur.fetchone()
    return bool(row and row[0])


def refresh_matview(conn) -> None:
    """Refresh geohash_road_lengths with safe first-refresh detection.

    PostgreSQL requires at least one non-concurrent REFRESH before
    CONCURRENTLY can be used.  We check pg_matviews.ispopulated:
      - False → first refresh → non-concurrent
      - True  → subsequent refresh → concurrent (with non-concurrent fallback)
    """
    start = time.monotonic()
    cur = conn.cursor()

    populated = _matview_is_populated(conn)

    if populated:
        logger.info(
            "Existing populated materialized view detected; using concurrent refresh"
        )
        try:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY geohash_road_lengths")
            conn.commit()
        except psycopg2.errors.FeatureNotSupported:
            logger.warning(
                "Concurrent refresh not supported in this state; "
                "falling back to non-concurrent refresh"
            )
            conn.rollback()
            cur = conn.cursor()
            cur.execute("REFRESH MATERIALIZED VIEW geohash_road_lengths")
            conn.commit()
    else:
        logger.info(
            "First materialized view refresh detected; using non-concurrent refresh"
        )
        cur.execute("REFRESH MATERIALIZED VIEW geohash_road_lengths")
        conn.commit()

    elapsed = time.monotonic() - start
    cur = conn.cursor()
    cur.execute("""
        SELECT
            COUNT(*)::int                               AS total_cells,
            COUNT(NULLIF(road_length_km, 0))::int       AS cells_with_roads,
            ROUND(AVG(road_length_km)::numeric, 3)      AS avg_road_km,
            ROUND(MAX(road_length_km)::numeric, 3)      AS max_road_km
        FROM geohash_road_lengths
    """)
    row = cur.fetchone()
    total, with_roads, avg_km, max_km = row
    coverage = 100 * with_roads / max(total, 1)
    logger.info(
        f"geohash_road_lengths refreshed in {elapsed:.1f}s:\n"
        f"  {total:,} geohash cells total\n"
        f"  {with_roads:,} cells with road coverage ({coverage:.1f}%)\n"
        f"  avg road length per cell: {avg_km} km\n"
        f"  max road length in one cell: {max_km} km"
    )


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(description="Import road network from OSM PBF into road_segments")
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=PROJECT_ROOT / "data" / "road_network" / "Istanbul.osm.pbf",
        help="Path to .osm.pbf or .gpkg file (default: data/road_network/Istanbul.osm.pbf)",
    )
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        help="Skip refreshing geohash_road_lengths after import",
    )
    args = parser.parse_args()

    if not _check_geopandas():
        sys.exit(1)

    input_path: Path = args.input
    if not input_path.exists():
        logger.error(
            f"Road network file not found: {input_path}\n\n"
            "Download the Istanbul OSM extract (BBBike, ~75 MB):\n"
            "  https://download.bbbike.org/osm/bbbike/Istanbul/Istanbul.osm.pbf\n\n"
            "Save it to:\n"
            f"  {input_path}\n\n"
            "Then re-run:\n"
            "  python scripts/import_road_network.py"
        )
        sys.exit(1)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as exc:
        logger.error(f"Cannot connect to database: {exc}\nStart: docker-compose up -d")
        sys.exit(1)

    if not _check_road_schema(conn):
        sys.exit(1)

    gdf = read_road_network(input_path)
    insert_road_segments(conn, gdf)

    if not args.no_refresh:
        if _check_matview(conn):
            refresh_matview(conn)
        else:
            logger.warning(
                "geohash_road_lengths not found — skipping refresh.\n"
                "Create schema: python scripts/create_road_schema.py"
            )

    logger.info("\nRoad network import complete.")
    conn.close()


if __name__ == "__main__":
    main()

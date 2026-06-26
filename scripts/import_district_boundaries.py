"""
Import Istanbul district boundary polygons into istanbul_district_boundaries.

This script reads a locally supplied GeoJSON FeatureCollection of district
polygons and loads them into PostGIS as EPSG:32636 MultiPolygons so that the
hourly `/api/clusters` endpoint can filter traffic measurements by a real
district polygon before running PostGIS spatial DBSCAN.

Data policy
-----------
- The district boundary GeoJSON is NOT downloaded automatically and is NOT
  committed to the repository unless its source/license is explicitly approved.
- Place the file locally at:
      data/boundaries/istanbul_districts.geojson
- Only `data/boundaries/.gitkeep` is tracked by Git.

Expected input
--------------
A GeoJSON FeatureCollection where each feature is a Polygon or MultiPolygon and
carries a district name in one of these common property keys:
    name, NAME, district, DISTRICT, ilce, ILCE, Ilce, İLÇE, ADI, ad, AD

Geometry handling
-----------------
GeoJSON coordinates are WGS84 (EPSG:4326). Geometries are passed straight to
PostGIS via ST_GeomFromGeoJSON (no extra Python geometry dependency), repaired
with ST_MakeValid, coerced to MultiPolygon, and transformed to EPSG:32636 to
match ibb_traffic_density.geom.

Usage
-----
    python scripts/import_district_boundaries.py
    python scripts/import_district_boundaries.py --input data/boundaries/istanbul_districts.geojson
    python scripts/import_district_boundaries.py --source-name "IBB" --source-note "Sehir Haritasi acik veri"
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DB_CONFIG, ensure_cli_logging  # noqa: E402

# Shared with the API router so district keys stay identical on both sides.
from backend.app.services.district_utils import normalize_district_key  # noqa: E402,F401

import psycopg2  # noqa: E402

logger = logging.getLogger(__name__)

DEFAULT_INPUT = PROJECT_ROOT / "data" / "boundaries" / "istanbul_districts.geojson"

# Property keys that may hold the district name, in priority order.
NAME_KEYS = [
    "name", "NAME", "district", "DISTRICT",
    "ilce", "ILCE", "Ilce", "İLÇE",
    "ADI", "ad", "AD",
]

# Table DDL — created if missing so this script is self-contained and does not
# require the full traffic database to be present.
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS istanbul_district_boundaries (
    id            SERIAL PRIMARY KEY,
    district_name TEXT NOT NULL,
    district_key  TEXT NOT NULL UNIQUE,
    geom          geometry(MultiPolygon, 32636) NOT NULL,
    source_name   TEXT,
    source_note   TEXT
);
"""

_CREATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_istanbul_district_boundaries_geom
ON istanbul_district_boundaries USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_istanbul_district_boundaries_key
ON istanbul_district_boundaries (district_key);
"""

# Parameterized insert. The GeoJSON geometry string is bound as a value; PostGIS
# repairs it (ST_MakeValid), keeps only polygonal parts (ST_CollectionExtract),
# coerces to MultiPolygon, sets SRID 4326, then transforms to 32636. The CTE lets
# us skip NULL/empty geometries so we never insert a zero-area district row.
_INSERT_SQL = """
WITH g AS (
    SELECT ST_Transform(
        ST_SetSRID(
            ST_Multi(
                ST_CollectionExtract(ST_MakeValid(ST_GeomFromGeoJSON(%s)), 3)
            ),
            4326
        ),
        32636
    ) AS geom
)
INSERT INTO istanbul_district_boundaries
    (district_name, district_key, geom, source_name, source_note)
SELECT %s, %s, g.geom, %s, %s
FROM g
WHERE g.geom IS NOT NULL AND NOT ST_IsEmpty(g.geom)
ON CONFLICT (district_key) DO NOTHING;
"""


def extract_district_name(properties: dict) -> str | None:
    """Return the first non-empty district name from known property keys."""
    if not isinstance(properties, dict):
        return None
    for key in NAME_KEYS:
        if key in properties and properties[key] not in (None, ""):
            return str(properties[key]).strip()
    return None


def load_features(input_path: Path) -> list[dict]:
    """Load and validate the GeoJSON FeatureCollection."""
    try:
        with input_path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid GeoJSON (JSON parse error): {exc}") from exc

    if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
        raise ValueError(
            "Expected a GeoJSON FeatureCollection at the top level "
            f"(got type={data.get('type') if isinstance(data, dict) else type(data).__name__!r})."
        )

    features = data.get("features")
    if not isinstance(features, list) or not features:
        raise ValueError("GeoJSON FeatureCollection has no features.")
    return features


def import_boundaries(
    conn,
    features: list[dict],
    source_name: str | None,
    source_note: str | None,
) -> list[str]:
    """Create the table if needed, reload it, and return imported district names.

    Table/index DDL is committed first (idempotent, non-destructive). The
    TRUNCATE and all row inserts then run in a single transaction that is
    committed only after every feature has been processed — so if any insert
    raises, the caller's rollback leaves the previously imported rows intact.
    """
    cur = conn.cursor()
    # DDL first — safe to commit on its own (CREATE ... IF NOT EXISTS is a no-op
    # when the table already exists and never drops data).
    cur.execute(_CREATE_TABLE_SQL)
    cur.execute(_CREATE_INDEXES_SQL)
    conn.commit()

    # Single transaction: TRUNCATE + inserts, committed only at the very end.
    cur.execute("TRUNCATE TABLE istanbul_district_boundaries RESTART IDENTITY;")

    imported: list[str] = []
    seen_keys: set[str] = set()
    skipped = 0
    skipped_empty: list[str] = []

    for idx, feature in enumerate(features):
        if not isinstance(feature, dict):
            skipped += 1
            continue
        geometry = feature.get("geometry")
        if not isinstance(geometry, dict) or geometry.get("type") not in (
            "Polygon",
            "MultiPolygon",
        ):
            logger.warning("Feature %d skipped: geometry is not a (Multi)Polygon.", idx)
            skipped += 1
            continue

        name = extract_district_name(feature.get("properties", {}))
        if not name:
            logger.warning("Feature %d skipped: no district name property found.", idx)
            skipped += 1
            continue

        key = normalize_district_key(name)
        if not key:
            logger.warning("Feature %d skipped: name %r normalized to an empty key.", idx, name)
            skipped += 1
            continue
        if key in seen_keys:
            logger.warning("Duplicate district key %r (%s) — keeping the first.", key, name)
            skipped += 1
            continue

        geojson_str = json.dumps(geometry)
        # Param order follows _INSERT_SQL: geojson (for the CTE), then
        # district_name, district_key, source_name, source_note.
        cur.execute(_INSERT_SQL, (geojson_str, name, key, source_name, source_note))
        if cur.rowcount == 1:
            imported.append(f"{name} ({key})")
            seen_keys.add(key)
        else:
            # No row inserted: the WHERE filtered an empty/NULL geometry (the
            # table was just truncated, so a conflict is not possible here).
            logger.warning("Feature %d skipped: %r produced an empty geometry.", idx, name)
            skipped_empty.append(f"{name} ({key})")
            skipped += 1

    conn.commit()

    if skipped:
        logger.info(
            "Skipped %d feature(s) (non-polygon / unnamed / duplicate / empty).",
            skipped,
        )
    if skipped_empty:
        logger.warning(
            "Empty geometries skipped (%d): %s",
            len(skipped_empty),
            ", ".join(skipped_empty),
        )
    return imported


# Rough WGS84 bounding box of Istanbul province — used only to warn if the
# imported polygons clearly do not look like Istanbul (wrong CRS / swapped axes).
# Western districts (Silivri/Çatalca) reach ~27.97 lon, so the lower bound is
# kept generous to avoid false positives on correct data.
_ISTANBUL_LON_MIN, _ISTANBUL_LON_MAX = 27.8, 30.5
_ISTANBUL_LAT_MIN, _ISTANBUL_LAT_MAX = 40.5, 41.7


def _post_import_report(conn) -> None:
    """Print read-only sanity checks on the imported boundary table."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COUNT(*)::int,
            (COUNT(*) FILTER (WHERE ST_SRID(geom) <> 32636))::int,
            (COUNT(*) FILTER (WHERE NOT ST_IsValid(geom)))::int,
            (COUNT(*) FILTER (WHERE ST_IsEmpty(geom)))::int
        FROM istanbul_district_boundaries
        """
    )
    total, wrong_srid, invalid, empty = cur.fetchone()

    logger.info("Sanity checks:")
    logger.info("  rows in table:        %d", total)
    logger.info("  SRID != 32636:        %d", wrong_srid)
    logger.info("  invalid geometries:   %d", invalid)
    logger.info("  empty geometries:     %d", empty)

    cur.execute(
        """
        SELECT ST_XMin(e), ST_YMin(e), ST_XMax(e), ST_YMax(e)
        FROM (
            SELECT ST_Extent(ST_Transform(geom, 4326)) AS e
            FROM istanbul_district_boundaries
        ) s
        """
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        logger.warning("  WGS84 envelope:       (unavailable)")
        return

    min_lon, min_lat, max_lon, max_lat = row
    logger.info(
        "  WGS84 envelope:       lon %.4f..%.4f, lat %.4f..%.4f",
        min_lon, max_lon, min_lat, max_lat,
    )
    if (
        min_lon < _ISTANBUL_LON_MIN or max_lon > _ISTANBUL_LON_MAX
        or min_lat < _ISTANBUL_LAT_MIN or max_lat > _ISTANBUL_LAT_MAX
    ):
        logger.warning(
            "  WARNING: envelope is outside the expected Istanbul range "
            "(lon %.1f..%.1f, lat %.1f..%.1f). Check the source CRS and "
            "coordinate order (lon/lat).",
            _ISTANBUL_LON_MIN, _ISTANBUL_LON_MAX,
            _ISTANBUL_LAT_MIN, _ISTANBUL_LAT_MAX,
        )


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(
        description="Import Istanbul district polygons into istanbul_district_boundaries."
    )
    parser.add_argument(
        "--input", "-i",
        type=Path,
        default=DEFAULT_INPUT,
        help="Path to district GeoJSON FeatureCollection "
             "(default: data/boundaries/istanbul_districts.geojson)",
    )
    parser.add_argument(
        "--source-name",
        default=None,
        help="Optional provenance label stored in source_name (e.g. data provider).",
    )
    parser.add_argument(
        "--source-note",
        default=None,
        help="Optional provenance note stored in source_note (e.g. license/URL).",
    )
    args = parser.parse_args()

    input_path: Path = args.input
    if not input_path.exists():
        logger.error(
            "District boundary file not found: data/boundaries/istanbul_districts.geojson\n\n"
            f"Expected at: {input_path}\n\n"
            "Supply a GeoJSON FeatureCollection of Istanbul district polygons locally\n"
            "(this file is not downloaded automatically and is not tracked by Git),\n"
            "then re-run:\n"
            "  python scripts/import_district_boundaries.py"
        )
        sys.exit(1)

    try:
        features = load_features(input_path)
    except ValueError as exc:
        logger.error("Failed to read %s: %s", input_path, exc)
        sys.exit(1)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as exc:
        logger.error(
            "Cannot connect to database: %s\n"
            "Start it with: docker-compose up -d",
            exc,
        )
        sys.exit(1)

    try:
        imported = import_boundaries(conn, features, args.source_name, args.source_note)
    except Exception as exc:
        conn.rollback()
        logger.error("Import failed; previous table contents left intact: %s", exc)
        conn.close()
        sys.exit(1)

    if not imported:
        conn.close()
        logger.error(
            "No district polygons were imported. Check that features carry a name "
            "property (one of: %s) and (Multi)Polygon geometry.",
            ", ".join(NAME_KEYS),
        )
        sys.exit(1)

    logger.info("Imported %d district polygon(s):", len(imported))
    for entry in sorted(imported):
        logger.info("  - %s", entry)

    _post_import_report(conn)
    conn.close()


if __name__ == "__main__":
    main()

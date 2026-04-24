"""
Temporary check: compare high-congestion rows against the geohash shown in QGIS (e.g. Bostancı).

Set TARGET_GEOHASH to the exact value from your screenshot, or enable USE_PREFIX_MATCH if QGIS
shows a shorter parent cell and you want all matching sub-cells.
"""

import logging

import psycopg2

from config import DB_CONFIG, ensure_cli_logging

logger = logging.getLogger(__name__)

# Typical Bostancı-area cell in this dataset (~40.965°N, 29.086°E). Replace with your QGIS value.
TARGET_GEOHASH = "sxk9jc"
# If True: WHERE geohash LIKE 'sxk9j%' (use when the screenshot shows a shorter prefix).
USE_PREFIX_MATCH = False


def main() -> None:
    ensure_cli_logging()
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        if USE_PREFIX_MATCH:
            pattern = f"{TARGET_GEOHASH}%"
            where_sql = "geohash LIKE %s"
            param = (pattern,)
            logger.info(
                f"Filtering high_congestion_zones where geohash LIKE '{pattern}' "
                f"(Bostancı / QGIS area check).\n"
            )
        else:
            where_sql = "geohash = %s"
            param = (TARGET_GEOHASH,)
            logger.info(
                f"Filtering high_congestion_zones where geohash = '{TARGET_GEOHASH}' "
                f"(Bostancı / QGIS area check).\n"
            )

        query = f"""
            SELECT
                geohash,
                AVG(vehicle_count)::double precision AS avg_vehicle_count,
                AVG(avg_speed)::double precision AS avg_avg_speed
            FROM high_congestion_zones
            WHERE {where_sql}
            GROUP BY geohash
            ORDER BY geohash;
        """
        cur.execute(query, param)
        rows = cur.fetchall()

        if not rows:
            logger.info(
                "No rows in high_congestion_zones for this geohash filter.\n"
                "Either congestion is not reported under that cell in this view "
                "(avg_speed < 20 AND vehicle_count > 500), or the hash does not match — "
                "try USE_PREFIX_MATCH or update TARGET_GEOHASH from QGIS."
            )
            return

        logger.info("Unique geohashes — avg vehicle_count & avg_speed:\n")
        for gh, avg_vc, avg_sp in rows:
            logger.info(
                f"  {gh}:  avg_vehicle_count={avg_vc:.2f}  avg_speed={avg_sp:.2f}"
            )
        logger.info(
            "\nIf a single geohash shows averages in line with the rest of the city, "
            "congestion is plausibly real for that cell; if this view is empty or values "
            "look extreme vs nearby cells, treat as a possible outlier for ST-DBSCAN."
        )
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()

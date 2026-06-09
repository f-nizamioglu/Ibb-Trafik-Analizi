"""
Audit the filtering pipeline impact on ibb_traffic_density.

Reports how many rows survive each stage:
  Raw table -> static candidate filter -> road-density filter -> cluster-ready

Usage:
    python scripts/audit_filtering_impact.py
    python scripts/audit_filtering_impact.py --output outputs/data_audit

Output:
    outputs/data_audit/filtering_impact.md   (overwritten with live DB figures)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DB_CONFIG, ensure_cli_logging  # noqa: E402

import psycopg2  # noqa: E402

logger = logging.getLogger(__name__)


# ── helpers ────────────────────────────────────────────────────────────────────

def _table_exists(cur, name: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema='public' AND table_name=%s", (name,)
    )
    return cur.fetchone()[0] > 0


def _view_exists(cur, name: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.views "
        "WHERE table_schema='public' AND table_name=%s", (name,)
    )
    return cur.fetchone()[0] > 0


def _matview_populated(cur, name: str) -> bool:
    cur.execute(
        "SELECT ispopulated FROM pg_matviews WHERE matviewname=%s", (name,)
    )
    row = cur.fetchone()
    return bool(row and row[0])


# ── main audit ─────────────────────────────────────────────────────────────────

def run_audit(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        conn = psycopg2.connect(**DB_CONFIG, connect_timeout=5)
    except Exception as exc:
        print(f"Cannot connect to database: {exc}")
        print("Start with: docker-compose up -d")
        sys.exit(1)

    cur = conn.cursor()

    if not _table_exists(cur, "ibb_traffic_density"):
        print("ibb_traffic_density not found. Run: python ingest_data.py")
        conn.close()
        sys.exit(1)

    results: dict = {}

    # ── A. Raw table ──────────────────────────────────────────────────────────
    cur.execute(
        "SELECT COUNT(*)::bigint, MIN(record_time)::text, MAX(record_time)::text, "
        "COUNT(DISTINCT geohash)::int, "
        "COUNT(DISTINCT DATE_TRUNC('month', record_time))::int "
        "FROM ibb_traffic_density"
    )
    raw_rows, raw_min, raw_max, raw_geohashes, raw_months = cur.fetchone()
    results["raw"] = {
        "rows": raw_rows,
        "geohashes": raw_geohashes,
        "months": raw_months,
        "min_date": raw_min,
        "max_date": raw_max,
    }
    print(f"A. Raw: {raw_rows:,} rows | {raw_geohashes:,} geohashes | "
          f"{raw_months} months | {raw_min} to {raw_max}")

    # ── B. Static candidate filter ─────────────────────────────────────────────
    cur.execute(
        "SELECT COUNT(*)::bigint, COUNT(DISTINCT geohash)::int "
        "FROM ibb_traffic_density WHERE avg_speed < 20"
    )
    speed_rows, _ = cur.fetchone()

    cur.execute(
        "SELECT COUNT(*)::bigint, COUNT(DISTINCT geohash)::int "
        "FROM ibb_traffic_density WHERE vehicle_count > 500"
    )
    veh_rows, _ = cur.fetchone()

    cur.execute(
        "SELECT COUNT(*)::bigint, COUNT(DISTINCT geohash)::int "
        "FROM ibb_traffic_density WHERE avg_speed < 20 AND vehicle_count > 500"
    )
    static_rows, static_geohashes = cur.fetchone()

    results["static"] = {
        "speed_lt20_rows": speed_rows,
        "veh_gt500_rows": veh_rows,
        "both_rows": static_rows,
        "both_geohashes": static_geohashes,
    }
    pct_static = 100 * static_rows / raw_rows if raw_rows else 0
    print(f"B. Static filter (speed<20 AND vehicles>500): "
          f"{static_rows:,} rows ({pct_static:.4f}%) | {static_geohashes} geohashes")

    # Check if high_congestion_zones view matches
    if _view_exists(cur, "high_congestion_zones"):
        cur.execute("SELECT COUNT(*)::bigint FROM high_congestion_zones")
        view_count = cur.fetchone()[0]
        results["static"]["view_count"] = view_count
        match = "OK" if view_count == static_rows else f"MISMATCH (view={view_count})"
        print(f"   high_congestion_zones view: {view_count:,} rows [{match}]")

    # ── C. Road-density filter ─────────────────────────────────────────────────
    road_density_exists = _view_exists(cur, "road_density_congestion_candidates") or \
                          _table_exists(cur, "road_density_congestion_candidates")
    if road_density_exists:
        cur.execute(
            "SELECT COUNT(*)::bigint, COUNT(DISTINCT geohash)::int "
            "FROM road_density_congestion_candidates"
        )
        rd_rows, rd_geohashes = cur.fetchone()

        # Geohash overlap: how many static-filter geohashes are also in road density
        cur.execute(
            "SELECT COUNT(DISTINCT t.geohash)::int "
            "FROM ibb_traffic_density t "
            "WHERE t.avg_speed < 20 AND t.vehicle_count > 500 "
            "  AND t.geohash IN (SELECT DISTINCT geohash FROM road_density_congestion_candidates)"
        )
        overlap_geohashes = cur.fetchone()[0]

        # Rows with no road coverage
        if _view_exists(cur, "road_density_base") or _table_exists(cur, "road_density_base"):
            cur.execute(
                "SELECT COUNT(*)::bigint FROM road_density_base "
                "WHERE road_length_km IS NULL OR road_length_km = 0"
            )
            no_road_rows = cur.fetchone()[0]
        else:
            no_road_rows = None

        pct_rd = 100 * rd_rows / raw_rows if raw_rows else 0
        results["road_density"] = {
            "rows": rd_rows,
            "geohashes": rd_geohashes,
            "overlap_with_static_geohashes": overlap_geohashes,
            "no_road_coverage_rows": no_road_rows,
        }
        print(f"C. Road-density filter (vehicles/road-km > 75th pct): "
              f"{rd_rows:,} rows ({pct_rd:.4f}%) | {rd_geohashes} geohashes")
        if no_road_rows is not None:
            pct_no_road = 100 * no_road_rows / raw_rows if raw_rows else 0
            print(f"   No OSM road coverage: {no_road_rows:,} rows ({pct_no_road:.1f}%)")
    else:
        results["road_density"] = {"available": False}
        print("C. Road-density filter: view not available")

    # ── D. Geohash area density prototype ──────────────────────────────────────
    geohash_density_exists = _view_exists(cur, "geohash_density_candidates") or \
                             _table_exists(cur, "geohash_density_candidates")
    if geohash_density_exists:
        cur.execute("SELECT COUNT(*)::bigint, COUNT(DISTINCT geohash)::int "
                    "FROM geohash_density_candidates")
        gd_rows, gd_geohashes = cur.fetchone()
        pct_gd = 100 * gd_rows / raw_rows if raw_rows else 0
        results["geohash_density"] = {"rows": gd_rows, "geohashes": gd_geohashes}
        print(f"D. Geohash area density prototype: "
              f"{gd_rows:,} rows ({pct_gd:.4f}%) | {gd_geohashes} geohashes")
    else:
        results["geohash_density"] = {"available": False}
        print("D. Geohash area density prototype: view not available")

    # ── E. Clustering output ───────────────────────────────────────────────────
    if _table_exists(cur, "traffic_clusters"):
        cur.execute(
            "SELECT COUNT(*)::bigint, COUNT(DISTINCT cluster_id)::int, "
            "SUM(CASE WHEN cluster_id = -1 THEN 1 ELSE 0 END)::int "
            "FROM traffic_clusters"
        )
        tc_rows, tc_cluster_ids, noise = cur.fetchone()
        noise = noise or 0
        non_noise_clusters = tc_cluster_ids - (1 if noise > 0 else 0)
        pct_noise = 100 * noise / tc_rows if tc_rows else 0
        results["clusters"] = {
            "rows": tc_rows,
            "distinct_cluster_ids": tc_cluster_ids,
            "non_noise_clusters": non_noise_clusters,
            "noise_points": noise,
            "noise_pct": round(pct_noise, 1),
        }
        print(f"E. traffic_clusters: {tc_rows:,} rows | "
              f"{non_noise_clusters} clusters | {noise} noise ({pct_noise:.1f}%)")

        # AIS from cluster_scores.csv if available
        import csv as csv_mod
        cs_path = PROJECT_ROOT / "cluster_scores.csv"
        if cs_path.exists():
            with open(cs_path, encoding="utf-8") as f:
                cluster_scores = list(csv_mod.DictReader(f))
            if cluster_scores:
                ais_vals = [float(r["AIS"]) for r in cluster_scores if r.get("AIS")]
                severities: dict[str, int] = {}
                for r in cluster_scores:
                    s = r.get("severity", "?")
                    severities[s] = severities.get(s, 0) + 1
                results["clusters"]["ais_min"] = round(min(ais_vals), 3) if ais_vals else None
                results["clusters"]["ais_max"] = round(max(ais_vals), 3) if ais_vals else None
                results["clusters"]["ais_mean"] = round(sum(ais_vals)/len(ais_vals), 3) if ais_vals else None
                results["clusters"]["severity"] = severities
                print(f"   AIS: {min(ais_vals):.3f}–{max(ais_vals):.3f} | {severities}")
    else:
        results["clusters"] = {"available": False}
        print("E. traffic_clusters: table not found (run pipeline first)")

    conn.close()

    # ── Write markdown ─────────────────────────────────────────────────────────
    md_path = output_dir / "filtering_impact.md"
    _write_md(md_path, results)
    print(f"\nMD: {md_path}")


def _write_md(path: Path, r: dict) -> None:
    lines: list[str] = []
    a = lines.append
    raw = r.get("raw", {})
    static = r.get("static", {})
    rd = r.get("road_density", {})
    cl = r.get("clusters", {})

    raw_rows = raw.get("rows", 0)

    def pct(n):
        if not raw_rows or n is None:
            return "?"
        return f"{100 * n / raw_rows:.4f}%"

    a("# Filtering Pipeline Impact Audit")
    a("")
    a(f"Dataset: {raw.get('months','?')} months  "
      f"({raw.get('min_date','?')} to {raw.get('max_date','?')})")
    a("")
    a("## Stage Summary")
    a("")
    a("| Stage | Rows | % of Raw | Geohashes |")
    a("|-------|------|----------|-----------|")
    a(f"| A. Raw (ibb_traffic_density) | {raw_rows:,} | 100% | "
      f"{raw.get('geohashes','?'):,} |")
    a(f"| B. Static filter (speed<20 AND vehicles>500) | "
      f"{static.get('both_rows',0):,} | {pct(static.get('both_rows'))} | "
      f"{static.get('both_geohashes','?')} |")
    if rd.get("available", True):
        a(f"| C. Road-density filter (75th pct vehicles/road-km) | "
          f"{rd.get('rows',0):,} | {pct(rd.get('rows'))} | "
          f"{rd.get('geohashes','?')} |")
    else:
        a("| C. Road-density filter | not available | — | — |")
    if cl.get("available", True):
        a(f"| D. After ST_ClusterDBSCAN (total input) | "
          f"{cl.get('rows',0):,} | {pct(cl.get('rows'))} | — |")
        a(f"| E. Non-noise cluster points | "
          f"{(cl.get('rows',0) - cl.get('noise_points',0)):,} | "
          f"{pct(cl.get('rows',0) - cl.get('noise_points',0))} | — |")
    else:
        a("| D–E. Clustering output | table not yet populated | — | — |")

    a("")
    a("## A. Raw Table")
    a("")
    a(f"| Metric | Value |")
    a(f"|--------|-------|")
    a(f"| Total rows | {raw_rows:,} |")
    a(f"| Distinct months | {raw.get('months','?')} |")
    a(f"| Date range | {raw.get('min_date','?')} to {raw.get('max_date','?')} |")
    a(f"| Distinct geohashes | {raw.get('geohashes','?'):,} |")

    a("")
    a("## B. Static Candidate Filter (`high_congestion_zones`)")
    a("")
    a("Condition: `avg_speed < 20 km/h AND vehicle_count > 500`")
    a("")
    a(f"| Condition | Rows | % of Raw |")
    a(f"|-----------|------|----------|")
    a(f"| speed < 20 km/h only | {static.get('speed_lt20_rows',0):,} | "
      f"{pct(static.get('speed_lt20_rows'))} |")
    a(f"| vehicle_count > 500 only | {static.get('veh_gt500_rows',0):,} | "
      f"{pct(static.get('veh_gt500_rows'))} |")
    a(f"| **Both conditions (active filter)** | **{static.get('both_rows',0):,}** | "
      f"**{pct(static.get('both_rows'))}** |")
    a(f"| Distinct geohashes after filter | {static.get('both_geohashes','?')} | — |")
    if "view_count" in static:
        match = "matches filter count" if static["view_count"] == static.get("both_rows") \
            else f"MISMATCH (view={static['view_count']})"
        a(f"| `high_congestion_zones` view rows | {static['view_count']:,} | {match} |")

    if rd.get("available", True):
        a("")
        a("## C. Road-Length-Aware Density Filter")
        a("")
        a("Method: `vehicles_per_road_km` above the 75th percentile threshold.")
        a("")
        a(f"| Metric | Value |")
        a(f"|--------|-------|")
        a(f"| Candidate rows | {rd.get('rows',0):,} |")
        a(f"| % of raw | {pct(rd.get('rows'))} |")
        a(f"| Distinct geohashes | {rd.get('geohashes','?')} |")
        a(f"| Static-filter geohashes also in road-density | "
          f"{rd.get('overlap_with_static_geohashes','?')} |")
        if rd.get("no_road_coverage_rows") is not None:
            a(f"| Rows with no OSM road coverage | "
              f"{rd['no_road_coverage_rows']:,} ({pct(rd['no_road_coverage_rows'])}) |")

    if cl.get("available", True):
        a("")
        a("## D–E. Clustering Output (`traffic_clusters`)")
        a("")
        a(f"Input: {static.get('both_rows',0):,} static-filter records  "
          f"-> `ST_ClusterDBSCAN(eps=500m, minpoints=3)`")
        a("")
        a(f"| Metric | Value |")
        a(f"|--------|-------|")
        a(f"| Total clustered input records | {cl.get('rows',0):,} |")
        a(f"| Non-noise clusters | {cl.get('non_noise_clusters','?')} |")
        a(f"| Noise points (cluster_id = -1) | {cl.get('noise_points',0)} |")
        a(f"| Noise percentage | {cl.get('noise_pct','?')}% |")
        if cl.get("ais_min") is not None:
            a(f"| AIS range | {cl['ais_min']} – {cl['ais_max']} (mean {cl['ais_mean']}) |")
        if cl.get("severity"):
            sev = cl["severity"]
            a(f"| Severity distribution | "
              f"HIGH={sev.get('HIGH',0)} MEDIUM={sev.get('MEDIUM',0)} "
              f"LOW={sev.get('LOW',0)} |")

    a("")
    a("## Notes")
    a("")
    a("- The IBB CSV files are **raw hourly aggregated traffic-density data**, not pre-filtered.")
    a("- The pipeline applies filtering after ingestion; `ibb_traffic_density` stores raw rows.")
    a("- Stage B (static filter) and Stage C (road-density) are alternative candidate "
      "selection methods, not sequential stages.")
    a("- The active pipeline uses Stage B (static filter) as input to ST_ClusterDBSCAN.")
    a("- Stage C (road-density) requires OSM road network data (`road_segments` table).")
    a("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(description="Audit filtering pipeline impact")
    parser.add_argument(
        "--output",
        default="outputs/data_audit",
        metavar="DIR",
        help="Output directory (default: outputs/data_audit)",
    )
    args = parser.parse_args()
    run_audit(PROJECT_ROOT / args.output)


if __name__ == "__main__":
    main()

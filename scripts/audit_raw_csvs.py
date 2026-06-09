"""
Audit all downloaded IBB traffic density CSV files.

For each downloaded file, reports: row count, date range, column names,
null counts, speed/vehicle distributions, geohash cardinality, and whether
the data appears raw or pre-filtered.

Usage:
    python scripts/audit_raw_csvs.py
    python scripts/audit_raw_csvs.py --output outputs/data_audit

Output:
    outputs/data_audit/raw_csv_audit.csv
    outputs/data_audit/raw_csv_audit.md
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import MANIFEST_PATH, RAW_DATA_DIR  # noqa: E402

logger = logging.getLogger(__name__)

KEY_COLS = [
    "DATE_TIME",
    "LATITUDE",
    "LONGITUDE",
    "GEOHASH",
    "AVERAGE_SPEED",
    "NUMBER_OF_VEHICLES",
]

def _load_manifest() -> list[dict]:
    if not MANIFEST_PATH.exists():
        return []
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        return json.load(f).get("resources", [])


def _find_local_path(resource: dict) -> Path | None:
    """Return the local path for a manifest resource from the canonical raw directory."""
    period = resource.get("period", "")
    if period:
        yyyymm = period.replace("-", "")
        candidate = RAW_DATA_DIR / f"traffic_density_{yyyymm}.csv"
        if candidate.exists():
            return candidate
    return None


def audit_csv(path: Path, period: str) -> dict:
    """Audit a single CSV file and return a stats dict."""
    null_counts: dict[str, int] = {c: 0 for c in KEY_COLS}
    speeds: list[float] = []
    vcounts: list[int] = []
    geohashes: set[str] = set()
    dates: list[str] = []
    total_rows = 0
    columns: list[str] = []

    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        columns = list(reader.fieldnames or [])
        for row in reader:
            total_rows += 1
            for col in KEY_COLS:
                v = row.get(col, "")
                if v is None or str(v).strip() == "":
                    null_counts[col] += 1

            try:
                speeds.append(float(row.get("AVERAGE_SPEED") or 0))
            except (ValueError, TypeError):
                pass

            try:
                vcounts.append(int(row.get("NUMBER_OF_VEHICLES") or 0))
            except (ValueError, TypeError):
                pass

            gh = (row.get("GEOHASH") or "").strip()
            if gh:
                geohashes.add(gh)

            dt = (row.get("DATE_TIME") or "").strip()
            if dt:
                dates.append(dt)

    min_speed = min(speeds) if speeds else None
    max_speed = max(speeds) if speeds else None
    mean_speed = sum(speeds) / len(speeds) if speeds else None
    min_veh = min(vcounts) if vcounts else None
    max_veh = max(vcounts) if vcounts else None
    mean_veh = sum(vcounts) / len(vcounts) if vcounts else None

    # Raw-vs-filtered heuristics
    raw_evidence: list[str] = []
    filtered_evidence: list[str] = []

    if max_speed and max_speed > 60:
        raw_evidence.append(f"max speed {max_speed:.0f} km/h (high values present)")
    if min_speed and min_speed < 5:
        raw_evidence.append(f"min speed {min_speed:.0f} km/h (near-zero values present)")
    if min_veh and min_veh < 10:
        raw_evidence.append(f"min vehicles {min_veh} (low-traffic records present)")
    if speeds:
        low_speed_pct = 100 * sum(1 for s in speeds if s < 20) / len(speeds)
        if low_speed_pct < 20:
            raw_evidence.append(
                f"only {low_speed_pct:.1f}% of records have speed < 20 km/h"
            )
        else:
            filtered_evidence.append(
                f"{low_speed_pct:.1f}% of records have speed < 20 km/h (unusually high)"
            )
    if vcounts:
        high_veh_pct = 100 * sum(1 for v in vcounts if v > 500) / len(vcounts)
        if high_veh_pct < 10:
            raw_evidence.append(
                f"only {high_veh_pct:.1f}% of records have vehicles > 500"
            )
        else:
            filtered_evidence.append(
                f"{high_veh_pct:.1f}% of records have vehicles > 500 (unusually high)"
            )

    if raw_evidence and not filtered_evidence:
        raw_verdict = "RAW"
    elif filtered_evidence and not raw_evidence:
        raw_verdict = "PRE-FILTERED"
    else:
        raw_verdict = "LIKELY RAW"

    return {
        "period": period,
        "filename": path.name,
        "file_size_mb": round(path.stat().st_size / 1e6, 2),
        "total_rows": total_rows,
        "columns": "|".join(columns),
        "min_date": min(dates) if dates else "",
        "max_date": max(dates) if dates else "",
        "distinct_geohashes": len(geohashes),
        "min_speed": min_speed,
        "max_speed": max_speed,
        "mean_speed": round(mean_speed, 1) if mean_speed else None,
        "min_vehicles": min_veh,
        "max_vehicles": max_veh,
        "mean_vehicles": round(mean_veh, 1) if mean_veh else None,
        "pct_speed_lt20": (
            round(100 * sum(1 for s in speeds if s < 20) / len(speeds), 2)
            if speeds
            else None
        ),
        "pct_vehicles_gt500": (
            round(100 * sum(1 for v in vcounts if v > 500) / len(vcounts), 2)
            if vcounts
            else None
        ),
        "null_DATE_TIME": null_counts["DATE_TIME"],
        "null_GEOHASH": null_counts["GEOHASH"],
        "null_LATITUDE": null_counts["LATITUDE"],
        "null_LONGITUDE": null_counts["LONGITUDE"],
        "null_AVERAGE_SPEED": null_counts["AVERAGE_SPEED"],
        "null_NUMBER_OF_VEHICLES": null_counts["NUMBER_OF_VEHICLES"],
        "verdict": raw_verdict,
        "raw_evidence": "; ".join(raw_evidence),
        "filtered_evidence": "; ".join(filtered_evidence),
    }


def run_audit(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    resources = _load_manifest()

    if not resources:
        print("No manifest found. Run: python download_data.py --discover")
        sys.exit(1)

    downloaded = [r for r in resources if r.get("downloaded")]
    print(f"Manifest total: {len(resources)} resources, {len(downloaded)} downloaded")
    print()

    results: list[dict] = []

    for resource in sorted(resources, key=lambda x: x.get("period", "")):
        period = resource.get("period", "?")
        local_path = _find_local_path(resource)

        if not local_path:
            print(f"  {period}  SKIPPED (not downloaded)")
            continue

        print(f"  {period}  Auditing {local_path.name} ...")
        try:
            stats = audit_csv(local_path, period)
            results.append(stats)
            print(
                f"    rows={stats['total_rows']:,}  "
                f"geohashes={stats['distinct_geohashes']:,}  "
                f"speed={stats['min_speed']:.0f}-{stats['max_speed']:.0f} km/h  "
                f"verdict={stats['verdict']}"
            )
        except Exception as exc:
            logger.error(f"Failed to audit {local_path}: {exc}")
            results.append({"period": period, "filename": local_path.name, "error": str(exc)})

    if not results:
        print("No files audited.")
        return

    # Write CSV
    csv_path = output_dir / "raw_csv_audit.csv"
    all_keys = [
        "period", "filename", "file_size_mb", "total_rows",
        "min_date", "max_date", "distinct_geohashes",
        "min_speed", "max_speed", "mean_speed",
        "min_vehicles", "max_vehicles", "mean_vehicles",
        "pct_speed_lt20", "pct_vehicles_gt500",
        "null_DATE_TIME", "null_GEOHASH", "null_LATITUDE", "null_LONGITUDE",
        "null_AVERAGE_SPEED", "null_NUMBER_OF_VEHICLES",
        "verdict", "raw_evidence", "filtered_evidence", "columns",
    ]
    with open(csv_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)
    print(f"\nCSV: {csv_path}")

    # Write Markdown
    md_path = output_dir / "raw_csv_audit.md"
    _write_md(md_path, results, resources)
    print(f"MD:  {md_path}")


def _write_md(path: Path, results: list[dict], resources: list[dict]) -> None:
    lines: list[str] = []
    a = lines.append

    audited = [r for r in results if "error" not in r]
    total_rows = sum(r.get("total_rows", 0) for r in audited)
    total_size = sum(r.get("file_size_mb", 0.0) for r in audited)

    a("# IBB Raw CSV Audit")
    a("")
    a(f"Manifest resources: **{len(resources)}** (Jan 2020 - Jan 2025)")
    a(f"Downloaded and audited: **{len(audited)}**")
    a(f"Total rows across audited files: **{total_rows:,}**")
    a(f"Total file size: **{total_size:.1f} MB**")
    a("")
    a("## Audited Files")
    a("")
    a("| Period | Filename | Rows | Geohashes | Speed Range | Speed <20% | Veh >500% | Verdict |")
    a("|--------|----------|------|-----------|-------------|------------|-----------|---------|")
    for r in audited:
        speed_range = (
            f"{r['min_speed']:.0f}-{r['max_speed']:.0f}"
            if r.get("min_speed") is not None
            else "?"
        )
        a(
            f"| {r['period']} | {r['filename']} | {r['total_rows']:,} | "
            f"{r['distinct_geohashes']:,} | {speed_range} km/h | "
            f"{r.get('pct_speed_lt20', '?')}% | {r.get('pct_vehicles_gt500', '?')}% | "
            f"**{r['verdict']}** |"
        )

    a("")
    a("## Raw vs. Pre-filtered Conclusion")
    a("")
    verdicts = [r["verdict"] for r in audited]
    if all(v in ("RAW", "LIKELY RAW") for v in verdicts):
        a(
            "The downloaded IBB files appear to be **raw hourly aggregated traffic-density data**, "
            "not a pre-filtered anomaly dataset. Evidence:"
        )
        a("")
        for r in audited:
            if r.get("raw_evidence"):
                a(f"- **{r['period']}**: {r['raw_evidence']}")
    else:
        a("Mixed or unclear verdict. See per-file evidence below.")
        for r in audited:
            a(f"- **{r['period']}**: verdict={r['verdict']}, evidence={r.get('raw_evidence','')} | {r.get('filtered_evidence','')}")

    a("")
    a("## Not-Yet-Downloaded Files")
    a("")
    downloaded_periods = {r["period"] for r in audited}
    missing = sorted(
        r["period"] for r in resources if r.get("period") not in downloaded_periods
    )
    a(f"{len(missing)} months not yet downloaded:")
    a("")
    # Show in rows of 6
    for i in range(0, len(missing), 6):
        a("  " + "  ".join(missing[i : i + 6]))
    a("")
    a("Run `python download_data.py --download` to download all remaining files.")
    a("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = argparse.ArgumentParser(description="Audit downloaded IBB CSV files")
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

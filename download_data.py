"""
IBB Hourly Traffic Density Dataset — Download Manager

Discovers, downloads, and tracks monthly CSV files from the Istanbul
Metropolitan Municipality (IBB) Open Data Portal.

Discovery uses the CKAN API (confirmed working). HTML scraping is available
as a fallback if the API becomes unavailable.

Usage:
    python download_data.py --discover [--expected-months N]
    python download_data.py --download [--start YYYY-MM] [--end YYYY-MM] [--limit N]
    python download_data.py --dry-run [--expected-months N]
    python download_data.py --status
    python download_data.py --force            # re-download even if file exists
    python download_data.py --download --start 2022-01 --end 2022-12

Raw files are stored under:
    data/raw/ibb_hourly_traffic_density/

January 2025 is stored at data/raw/ibb_hourly_traffic_density/traffic_density_202501.csv
and is automatically registered in the manifest. It will never be re-downloaded unless
--force is explicitly passed.

Manifest is stored at:
    data/download_manifest.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    IBB_CKAN_BASE_URL,
    IBB_DATASET_SLUG,
    MANIFEST_PATH,
    RAW_DATA_DIR,
    ensure_cli_logging,
)

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

STREAM_CHUNK_BYTES = 65_536
REQUEST_TIMEOUT_S = 60
RETRY_BACKOFF_S = (2, 5, 10)

LEGACY_JAN2025_PATH = RAW_DATA_DIR / "traffic_density_202501.csv"
LEGACY_JAN2025_SHA256 = "9bb1e4600ea6bc979ab5084446eacf974ffddf719fc25120451cf87d67ebb4e1"
LEGACY_JAN2025_PERIOD = "2025-01"


# ── Manifest I/O ──────────────────────────────────────────────────────────────

def load_manifest() -> dict:
    """Load the download manifest from disk; return empty manifest if not present."""
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH, encoding="utf-8") as f:
            return json.load(f)
    return {"version": 1, "resources": []}


def save_manifest(manifest: dict) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


def _manifest_index(manifest: dict) -> dict[str, dict]:
    """Return a mapping period → resource entry."""
    return {r["period"]: r for r in manifest["resources"] if r.get("period")}


def _upsert_resource(manifest: dict, entry: dict) -> None:
    """Insert or update a resource entry by period."""
    for i, r in enumerate(manifest["resources"]):
        if r.get("period") == entry["period"]:
            manifest["resources"][i] = entry
            return
    manifest["resources"].append(entry)


# ── Month / period helpers ─────────────────────────────────────────────────────

def parse_period_from_filename(filename: str) -> str | None:
    """Extract YYYY-MM period from a filename like traffic_density_202501.csv.

    Uses finditer so that if an early match fails validation (e.g. a UUID
    fragment like '357821'), subsequent matches in the string are still tried.
    """
    for m in re.finditer(r"(\d{4})(\d{2})", filename):
        year, month = int(m.group(1)), int(m.group(2))
        if 2000 <= year <= 2100 and 1 <= month <= 12:
            return f"{year:04d}-{month:02d}"
    return None


def period_in_range(period: str, start: str | None, end: str | None) -> bool:
    return (start is None or period >= start) and (end is None or period <= end)


# ── SHA256 ─────────────────────────────────────────────────────────────────────

def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


# ── Discovery via CKAN API ────────────────────────────────────────────────────

def discover_via_ckan() -> list[dict]:
    """
    Query the IBB CKAN API to list all resources for the hourly traffic density dataset.
    Returns a list of dicts with keys: period, title, url, resource_id.
    """
    url = f"{IBB_CKAN_BASE_URL}/api/3/action/package_show?id={IBB_DATASET_SLUG}"
    logger.info(f"Querying CKAN API: {url}")

    for attempt, backoff in enumerate((*RETRY_BACKOFF_S, None), start=1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT_S)
            resp.raise_for_status()
            data = json.loads(resp.text)
            break
        except Exception as exc:
            if backoff is None:
                logger.error(f"CKAN API failed after {attempt} attempts: {exc}")
                raise
            logger.warning(f"CKAN attempt {attempt} failed ({exc}), retrying in {backoff}s…")
            time.sleep(backoff)

    if not data.get("success"):
        raise RuntimeError(f"CKAN API returned error: {data.get('error')}")

    resources = []
    for res in data["result"].get("resources", []):
        url_str = res.get("url", "")
        if not url_str:
            continue
        # Extract only the filename from the URL to avoid matching UUID hex digits
        url_filename = url_str.rsplit("/", 1)[-1].split("?")[0]
        period = parse_period_from_filename(url_filename)
        if period is None:
            period = parse_period_from_filename(res.get("name", ""))
        if period is None:
            logger.debug(f"Could not extract period from resource: {res.get('name')!r}")
            continue
        resources.append({
            "period": period,
            "title": res.get("name", ""),
            "url": url_str,
            "resource_id": res.get("id", ""),
        })

    logger.info(f"CKAN API returned {len(resources)} CSV resources")
    return resources


def discover_via_html() -> list[dict]:
    """
    Fallback: scrape the IBB dataset HTML page for CSV download links.
    Used only when CKAN API is unavailable.
    """
    from bs4 import BeautifulSoup

    page_url = f"{IBB_CKAN_BASE_URL}/dataset/{IBB_DATASET_SLUG}"
    logger.info(f"Scraping HTML fallback: {page_url}")
    resp = requests.get(page_url, headers=HEADERS, timeout=REQUEST_TIMEOUT_S)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    resources = []
    seen_urls: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/download/" not in href and not href.lower().endswith(".csv"):
            continue
        full_url = urljoin(page_url, href)
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)
        filename = full_url.split("/")[-1].split("?")[0]
        period = parse_period_from_filename(filename)
        if period:
            resources.append({
                "period": period,
                "title": a.get_text(strip=True) or filename,
                "url": full_url,
                "resource_id": "",
            })

    logger.info(f"HTML scraping found {len(resources)} CSV links")
    return resources


def discover_resources() -> list[dict]:
    """Try CKAN API first; fall back to HTML scraping."""
    try:
        return discover_via_ckan()
    except Exception as ckan_exc:
        logger.warning(f"CKAN API unavailable ({ckan_exc}); falling back to HTML scraping")
        try:
            return discover_via_html()
        except Exception as html_exc:
            logger.error(
                f"Both CKAN API and HTML scraping failed.\n"
                f"  CKAN: {ckan_exc}\n"
                f"  HTML: {html_exc}\n\n"
                f"Fallback: Manually add resource URLs to {MANIFEST_PATH} and re-run."
            )
            return []


# ── Register legacy January 2025 file ────────────────────────────────────────

def _make_entry(
    period: str,
    title: str,
    url: str,
    local_path: Path,
    sha256: str | None = None,
    size_bytes: int | None = None,
    downloaded: bool = False,
    ingested: bool = False,
    row_count: int | None = None,
    min_date: str | None = None,
    max_date: str | None = None,
    status: str = "ok",
    error: str | None = None,
) -> dict:
    return {
        "period": period,
        "title": title,
        "source_url": url,
        "local_path": str(local_path) if local_path else None,
        "filename": local_path.name if local_path else None,
        "sha256": sha256,
        "size_bytes": size_bytes,
        "downloaded": downloaded,
        "ingested": ingested,
        "row_count": row_count,
        "min_date": min_date,
        "max_date": max_date,
        "status": status,
        "error": error,
    }


def register_legacy_jan2025(manifest: dict, ckan_resources: list[dict]) -> bool:
    """
    Register the existing January 2025 CSV in the manifest.
    Returns True if registered (new or updated), False if already correct.
    """
    index = _manifest_index(manifest)
    if LEGACY_JAN2025_PERIOD in index:
        entry = index[LEGACY_JAN2025_PERIOD]
        if entry.get("downloaded") and entry.get("sha256") == LEGACY_JAN2025_SHA256:
            logger.info(f"January 2025 already registered in manifest — skipping")
            return False

    if not LEGACY_JAN2025_PATH.exists():
        logger.warning(
            f"Legacy January 2025 file not found at {LEGACY_JAN2025_PATH}. "
            "Cannot register — will be discovered and downloaded normally."
        )
        return False

    # Verify SHA256 of the existing file
    actual_sha = sha256_of_file(LEGACY_JAN2025_PATH)
    if actual_sha != LEGACY_JAN2025_SHA256:
        logger.warning(
            f"SHA256 mismatch on legacy Jan 2025 file.\n"
            f"  Expected: {LEGACY_JAN2025_SHA256}\n"
            f"  Actual:   {actual_sha}\n"
            "File may have been modified. Will be re-registered with actual SHA256."
        )

    # Find matching CKAN entry for the source URL
    source_url = ""
    title = "January 2025 Hourly Traffic Density (legacy local file)"
    for r in ckan_resources:
        if r["period"] == LEGACY_JAN2025_PERIOD:
            source_url = r["url"]
            title = r["title"]
            break

    size_bytes = LEGACY_JAN2025_PATH.stat().st_size
    entry = _make_entry(
        period=LEGACY_JAN2025_PERIOD,
        title=title,
        url=source_url,
        local_path=LEGACY_JAN2025_PATH,
        sha256=actual_sha,
        size_bytes=size_bytes,
        downloaded=True,
        ingested=False,
        min_date="2025-01-01",
        max_date="2025-01-31",
        status="ok",
    )
    _upsert_resource(manifest, entry)
    logger.info(
        f"Registered legacy January 2025 file:\n"
        f"  Path:     {LEGACY_JAN2025_PATH}\n"
        f"  SHA256:   {actual_sha}\n"
        f"  Size:     {size_bytes / 1e6:.1f} MB"
    )
    return True


# ── Downloader ────────────────────────────────────────────────────────────────

def download_file(url: str, dest: Path, force: bool = False) -> tuple[bool, str]:
    """
    Stream-download url to dest, computing SHA256 during download.
    Returns (success, sha256_hex).
    Skips if file already exists and is non-zero, unless force=True.
    """
    if dest.exists() and dest.stat().st_size > 0 and not force:
        sha = sha256_of_file(dest)
        logger.info(f"  Already exists ({dest.stat().st_size / 1e6:.1f} MB), skipping: {dest.name}")
        return True, sha

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(".tmp")

    for attempt, backoff in enumerate((*RETRY_BACKOFF_S, None), start=1):
        try:
            resp = requests.get(url, headers=HEADERS, stream=True, timeout=REQUEST_TIMEOUT_S)
            resp.raise_for_status()

            content_length = resp.headers.get("Content-Length")
            total_mb = int(content_length) / 1e6 if content_length else None

            h = hashlib.sha256()
            downloaded = 0
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=STREAM_CHUNK_BYTES):
                    if chunk:
                        f.write(chunk)
                        h.update(chunk)
                        downloaded += len(chunk)
                        if total_mb and downloaded % (10 * 1024 * 1024) < STREAM_CHUNK_BYTES:
                            pct = 100 * downloaded / (total_mb * 1e6)
                            logger.info(f"    {downloaded / 1e6:.0f}/{total_mb:.0f} MB ({pct:.0f}%)")

            tmp.replace(dest)
            sha = h.hexdigest()
            logger.info(f"  Downloaded {dest.name} ({downloaded / 1e6:.1f} MB), SHA256={sha[:12]}…")
            return True, sha

        except Exception as exc:
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            if backoff is None:
                logger.error(f"  Download failed after {attempt} attempts: {exc}")
                return False, ""
            logger.warning(f"  Attempt {attempt} failed ({exc}), retrying in {backoff}s…")
            time.sleep(backoff)

    return False, ""


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_discover(expected_months: int) -> None:
    """Discover all resources via CKAN API and update manifest."""
    manifest = load_manifest()
    ckan_resources = discover_resources()

    if not ckan_resources:
        logger.error("No resources discovered. Manifest unchanged.")
        return

    # Register/update discovered resources in manifest
    new_count = 0
    for res in sorted(ckan_resources, key=lambda r: r["period"]):
        index = _manifest_index(manifest)
        if res["period"] in index:
            # Update source_url in case it changed; keep download status
            index[res["period"]]["source_url"] = res["url"]
            index[res["period"]]["title"] = res["title"]
            _upsert_resource(manifest, index[res["period"]])
        else:
            entry = _make_entry(
                period=res["period"],
                title=res["title"],
                url=res["url"],
                local_path=RAW_DATA_DIR / f"traffic_density_{res['period'].replace('-', '')}.csv",
                downloaded=False,
                ingested=False,
            )
            _upsert_resource(manifest, entry)
            new_count += 1

    # Register the legacy January 2025 file
    register_legacy_jan2025(manifest, ckan_resources)

    # Sort by period
    manifest["resources"].sort(key=lambda r: r.get("period", ""))

    save_manifest(manifest)

    total = len(manifest["resources"])
    downloaded = sum(1 for r in manifest["resources"] if r.get("downloaded"))
    logger.info(
        f"\nDiscovery complete:\n"
        f"  {len(ckan_resources)} resources found via CKAN API\n"
        f"  {new_count} new entries added to manifest\n"
        f"  {total} total in manifest ({downloaded} already downloaded)\n"
        f"  Manifest: {MANIFEST_PATH}"
    )
    if total != expected_months:
        logger.warning(
            f"\n  NOTE: Found {total} months but expected {expected_months}.\n"
            f"  The IBB dataset currently covers Jan 2020 – Jan 2025 ({total} months).\n"
            f"  If your target is {expected_months} months, additional data may not yet be published."
        )


def cmd_status(expected_months: int) -> None:
    """Print manifest status summary."""
    manifest = load_manifest()
    resources = manifest.get("resources", [])
    if not resources:
        logger.info("Manifest is empty. Run: python download_data.py --discover")
        return

    downloaded = [r for r in resources if r.get("downloaded")]
    ingested = [r for r in resources if r.get("ingested")]
    pending = [r for r in resources if not r.get("downloaded")]
    failed = [r for r in resources if r.get("status") == "error"]

    periods = sorted(r["period"] for r in resources if r.get("period"))
    logger.info(f"\nManifest summary:")
    logger.info(f"  Total resources:  {len(resources)}")
    logger.info(f"  Downloaded:       {len(downloaded)}")
    logger.info(f"  Ingested:         {len(ingested)}")
    logger.info(f"  Pending download: {len(pending)}")
    logger.info(f"  Failed:           {len(failed)}")
    if periods:
        logger.info(f"  Period range:     {periods[0]} to {periods[-1]}")
    if len(resources) != expected_months:
        logger.warning(
            f"  Expected {expected_months} months but manifest has {len(resources)}."
        )
    if pending:
        logger.info(f"\n  Pending months: {[r['period'] for r in pending][:10]}"
                    f"{'...' if len(pending) > 10 else ''}")
    if failed:
        logger.warning(f"\n  Failed: {[r['period'] for r in failed]}")


def cmd_download(
    start: str | None,
    end: str | None,
    limit: int | None,
    force: bool,
    dry_run: bool,
    expected_months: int,
) -> None:
    """Download pending resources in the manifest."""
    manifest = load_manifest()
    if not manifest["resources"]:
        logger.error("Manifest is empty. Run --discover first.")
        return

    # Ensure legacy Jan 2025 is registered
    ckan_resources = []
    register_legacy_jan2025(manifest, ckan_resources)

    candidates = [
        r for r in sorted(manifest["resources"], key=lambda x: x.get("period", ""))
        if period_in_range(r.get("period", ""), start, end)
        and (force or not r.get("downloaded"))
        and r.get("source_url")
    ]

    if limit:
        candidates = candidates[:limit]

    if dry_run:
        logger.info(f"\nDry run — would download {len(candidates)} files:")
        for r in candidates:
            logger.info(f"  {r['period']}  {r['source_url']}")
        total = len(manifest["resources"])
        if total != expected_months:
            logger.warning(f"  Expected {expected_months} months, manifest has {total}.")
        return

    if not candidates:
        logger.info("Nothing to download. All matching resources already downloaded.")
        return

    logger.info(f"Downloading {len(candidates)} files…\n")
    ok = 0
    for i, res in enumerate(candidates, 1):
        period = res.get("period", "?")
        local_path = Path(res.get("local_path") or RAW_DATA_DIR / f"traffic_density_{period.replace('-', '')}.csv")
        url = res["source_url"]
        logger.info(f"[{i}/{len(candidates)}] {period}  ->  {local_path.name}")

        success, sha = download_file(url, local_path, force=force)
        if success:
            res["downloaded"] = True
            res["sha256"] = sha
            res["size_bytes"] = local_path.stat().st_size
            res["local_path"] = str(local_path)
            res["filename"] = local_path.name
            res["status"] = "ok"
            res["error"] = None
            ok += 1
        else:
            res["status"] = "error"
            res["error"] = "download failed"

        _upsert_resource(manifest, res)
        save_manifest(manifest)

    logger.info(f"\nDownload complete: {ok}/{len(candidates)} succeeded.")
    if len(manifest["resources"]) != expected_months:
        logger.warning(
            f"Manifest has {len(manifest['resources'])} months; expected {expected_months}."
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(
        description="IBB hourly traffic density dataset download manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--discover", action="store_true",
                       help="Discover available resources via CKAN API and update manifest")
    group.add_argument("--download", action="store_true",
                       help="Download pending resources from manifest")
    group.add_argument("--dry-run", action="store_true",
                       help="Show what would be downloaded without downloading")
    group.add_argument("--status", action="store_true",
                       help="Show manifest status summary")

    parser.add_argument("--start", metavar="YYYY-MM", help="Download from this period onward")
    parser.add_argument("--end", metavar="YYYY-MM", help="Download up to and including this period")
    parser.add_argument("--limit", type=int, metavar="N", help="Limit to N downloads")
    parser.add_argument("--force", action="store_true",
                        help="Re-download even if file already exists")
    parser.add_argument("--expected-months", type=int, default=63, metavar="N",
                        help="Expected number of monthly files (default: 63, warn if different)")

    args = parser.parse_args()

    if args.discover:
        cmd_discover(args.expected_months)
    elif args.status:
        cmd_status(args.expected_months)
    elif args.dry_run:
        cmd_download(
            start=args.start, end=args.end, limit=args.limit,
            force=args.force, dry_run=True, expected_months=args.expected_months,
        )
    elif args.download:
        cmd_download(
            start=args.start, end=args.end, limit=args.limit,
            force=args.force, dry_run=False, expected_months=args.expected_months,
        )


if __name__ == "__main__":
    main()

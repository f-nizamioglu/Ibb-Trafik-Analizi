"""
Tests for the download manager and ingestion pipeline.

Unit tests run without network access or database.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ── parse_period_from_filename ────────────────────────────────────────────────

def test_parse_period_standard_filename():
    from download_data import parse_period_from_filename
    assert parse_period_from_filename("traffic_density_202501.csv") == "2025-01"


def test_parse_period_various_formats():
    from download_data import parse_period_from_filename
    assert parse_period_from_filename("traffic_density_202001.csv") == "2020-01"
    assert parse_period_from_filename("traffic_density_202312.csv") == "2023-12"
    assert parse_period_from_filename("202406_data.csv") == "2024-06"


def test_parse_period_jan2025():
    from download_data import parse_period_from_filename
    assert parse_period_from_filename("traffic_density_202501.csv") == "2025-01"


def test_parse_period_no_match():
    from download_data import parse_period_from_filename
    assert parse_period_from_filename("readme.txt") is None
    assert parse_period_from_filename("data.csv") is None


def test_parse_period_rejects_invalid_month():
    from download_data import parse_period_from_filename
    assert parse_period_from_filename("data_202513.csv") is None


# ── parse_period in ingest_data matches download_data ─────────────────────────

def test_ingest_parse_period_consistent():
    from download_data import parse_period_from_filename as dl_parse
    from ingest_data import parse_period_from_filename as ing_parse
    for name in [
        "traffic_density_202501.csv",
        "traffic_density_202001.csv",
        "traffic_density_202312.csv",
    ]:
        assert dl_parse(name) == ing_parse(name), f"Mismatch for {name}"


# ── period_in_range ───────────────────────────────────────────────────────────

def test_period_in_range_both_bounds():
    from download_data import period_in_range
    assert period_in_range("2022-06", "2022-01", "2022-12")
    assert not period_in_range("2021-12", "2022-01", "2022-12")
    assert not period_in_range("2023-01", "2022-01", "2022-12")


def test_period_in_range_no_bounds():
    from download_data import period_in_range
    assert period_in_range("2020-01", None, None)
    assert period_in_range("2025-01", None, None)


def test_period_in_range_start_only():
    from download_data import period_in_range
    assert period_in_range("2023-06", "2023-01", None)
    assert not period_in_range("2022-12", "2023-01", None)


# ── Manifest I/O ──────────────────────────────────────────────────────────────

def test_manifest_roundtrip(tmp_path, monkeypatch):
    from download_data import load_manifest, save_manifest
    monkeypatch.setattr("download_data.MANIFEST_PATH", tmp_path / "manifest.json")

    manifest = load_manifest()
    assert manifest["resources"] == []

    manifest["resources"].append({"period": "2025-01", "downloaded": True})
    save_manifest(manifest)

    loaded = load_manifest()
    assert loaded["resources"][0]["period"] == "2025-01"
    assert loaded["resources"][0]["downloaded"] is True


def test_manifest_empty_if_missing(tmp_path, monkeypatch):
    monkeypatch.setattr("download_data.MANIFEST_PATH", tmp_path / "nonexistent.json")
    from download_data import load_manifest
    m = load_manifest()
    assert m == {"version": 1, "resources": []}


# ── _manifest_index ───────────────────────────────────────────────────────────

def test_manifest_index_keyed_by_period():
    from download_data import _manifest_index
    manifest = {
        "resources": [
            {"period": "2025-01", "downloaded": True},
            {"period": "2024-12", "downloaded": False},
        ]
    }
    index = _manifest_index(manifest)
    assert "2025-01" in index
    assert "2024-12" in index
    assert index["2025-01"]["downloaded"] is True


# ── _upsert_resource ──────────────────────────────────────────────────────────

def test_upsert_resource_insert_new():
    from download_data import _upsert_resource
    manifest = {"resources": []}
    _upsert_resource(manifest, {"period": "2025-01", "downloaded": True})
    assert len(manifest["resources"]) == 1
    assert manifest["resources"][0]["period"] == "2025-01"


def test_upsert_resource_updates_existing():
    from download_data import _upsert_resource
    manifest = {"resources": [{"period": "2025-01", "downloaded": False}]}
    _upsert_resource(manifest, {"period": "2025-01", "downloaded": True, "sha256": "abc"})
    assert len(manifest["resources"]) == 1
    assert manifest["resources"][0]["downloaded"] is True
    assert manifest["resources"][0]["sha256"] == "abc"


def test_upsert_jan2025_exactly_once():
    from download_data import _upsert_resource
    manifest = {"resources": []}
    _upsert_resource(manifest, {"period": "2025-01", "downloaded": True})
    _upsert_resource(manifest, {"period": "2025-01", "downloaded": True, "sha256": "xyz"})
    jan_entries = [r for r in manifest["resources"] if r["period"] == "2025-01"]
    assert len(jan_entries) == 1, "January 2025 must appear exactly once"


# ── Duplicate month detection ─────────────────────────────────────────────────

def test_no_duplicate_periods_after_multiple_upserts():
    from download_data import _upsert_resource
    manifest = {"resources": []}
    for _ in range(5):
        _upsert_resource(manifest, {"period": "2022-06", "downloaded": True})
    periods = [r["period"] for r in manifest["resources"]]
    assert periods.count("2022-06") == 1


# ── Skip existing valid local file ────────────────────────────────────────────

def test_download_skips_existing_nonzero_file(tmp_path, monkeypatch):
    """download_file must return True+sha without downloading if file exists and is non-empty."""
    from download_data import download_file

    target = tmp_path / "traffic_density_202501.csv"
    target.write_bytes(b"fake,csv,content\n1,2,3\n")

    # If it tries to actually download, requests.get will fail — which would cause test failure.
    # By NOT patching requests, we ensure the function returns early without network access.
    success, sha = download_file("http://should-not-be-called.example/", target, force=False)
    assert success is True
    assert len(sha) == 64  # valid SHA256 hex


def test_download_force_overwrites_existing(tmp_path, monkeypatch):
    """With force=True, download_file should attempt the network request."""
    import requests as _requests
    from download_data import download_file

    target = tmp_path / "test.csv"
    target.write_bytes(b"old content")

    # Patch requests.get to simulate a failed download (so we can detect the attempt)
    def fake_get(url, **kwargs):
        raise ConnectionError("simulated network failure")

    monkeypatch.setattr(_requests, "get", fake_get)
    success, sha = download_file("http://example.com/test.csv", target, force=True)
    assert success is False  # simulated failure, but the download WAS attempted


# ── SHA256 computation ────────────────────────────────────────────────────────

def test_sha256_of_file(tmp_path):
    from download_data import sha256_of_file
    f = tmp_path / "test.bin"
    content = b"hello world"
    f.write_bytes(content)
    expected = hashlib.sha256(content).hexdigest()
    assert sha256_of_file(f) == expected


# ── ingest parse_row ──────────────────────────────────────────────────────────

def test_parse_row_valid():
    from ingest_data import parse_row
    cols = ["2025-01-15 08:00:00", "41.0123", "28.9456", "sxk9jc", "10", "60", "35", "250"]
    result = parse_row(cols)
    assert result is not None
    assert result[0] == "2025-01-15 08:00:00"
    assert result[1] == pytest.approx(41.0123)
    assert result[7] == 250


def test_parse_row_wrong_length():
    from ingest_data import parse_row
    assert parse_row(["a", "b", "c"]) is None


def test_parse_row_invalid_float():
    from ingest_data import parse_row
    cols = ["2025-01-01", "not_a_float", "28.9", "abc", "5", "50", "30", "100"]
    assert parse_row(cols) is None


def test_parse_row_with_bom():
    from ingest_data import parse_row
    # Simulate BOM on first column (as seen in IBB CSV files)
    cols = ['﻿"2025-01-01 00:00:00"', "41.0", "29.0", "sxk9", "10", "50", "30", "100"]
    result = parse_row(cols)
    assert result is not None


# ── Coverage validation helpers ───────────────────────────────────────────────

def test_all_months_in_range():
    from scripts.validate_data_coverage import _all_months_in_range
    months = _all_months_in_range("2024-11", "2025-01")
    assert months == ["2024-11", "2024-12", "2025-01"]


def test_all_months_single():
    from scripts.validate_data_coverage import _all_months_in_range
    assert _all_months_in_range("2025-01", "2025-01") == ["2025-01"]


def test_all_months_full_year():
    from scripts.validate_data_coverage import _all_months_in_range
    months = _all_months_in_range("2023-01", "2023-12")
    assert len(months) == 12
    assert months[0] == "2023-01"
    assert months[-1] == "2023-12"

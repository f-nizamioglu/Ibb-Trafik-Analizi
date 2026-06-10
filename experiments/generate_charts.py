"""
Generate charts from parameter sensitivity and density experiment outputs.

Reads CSV files produced by parameter_sensitivity.py and dynamic_density.py
and writes PNG charts to outputs/experiments/.

Prerequisites:
  pip install matplotlib
  python experiments/parameter_sensitivity.py   (produces parameter_sensitivity.csv)
  python experiments/dynamic_density.py         (produces density_comparison.csv)

Usage:
  python experiments/generate_charts.py
  python experiments/generate_charts.py --skip-density   # skip density chart
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import ensure_cli_logging  # noqa: E402

import logging

logger = logging.getLogger(__name__)

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "experiments"
SENSITIVITY_CSV = OUTPUT_DIR / "parameter_sensitivity.csv"
DENSITY_CSV = OUTPUT_DIR / "density_comparison.csv"


def _load_csv(path: Path) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _check_matplotlib() -> bool:
    try:
        import matplotlib  # noqa: F401
        return True
    except ImportError:
        logger.error(
            "\nmatplotlib is not installed.\n"
            "Install it with:\n\n  pip install matplotlib\n"
        )
        return False


def chart_threshold_impact(rows: list[dict]) -> Path:
    """Cluster count vs speed threshold, grouped by vehicle threshold."""
    import matplotlib.pyplot as plt

    vehicle_thresholds = sorted({int(r["vehicle_threshold"]) for r in rows})
    eps_vals = sorted({float(r["eps_meters"]) for r in rows})
    minpts_vals = sorted({int(r["minpoints"]) for r in rows})

    # Use the most common eps and minpoints as the fixed axis
    fixed_eps = 500.0 if 500.0 in eps_vals else eps_vals[len(eps_vals) // 2]
    fixed_minpts = 3 if 3 in minpts_vals else minpts_vals[0]

    fig, ax = plt.subplots(figsize=(9, 5))
    for vt in vehicle_thresholds:
        subset = [
            r for r in rows
            if int(r["vehicle_threshold"]) == vt
            and float(r["eps_meters"]) == fixed_eps
            and int(r["minpoints"]) == fixed_minpts
            and r["cluster_count"] not in ("", "ERROR")
        ]
        if not subset:
            continue
        subset.sort(key=lambda r: int(r["speed_threshold_kmh"]))
        x = [int(r["speed_threshold_kmh"]) for r in subset]
        y = [int(r["cluster_count"]) for r in subset]
        ax.plot(x, y, marker="o", label=f"vehicle>{vt}")

    ax.set_xlabel("Speed threshold (km/h)")
    ax.set_ylabel("Cluster count")
    ax.set_title(
        f"Cluster Count vs Speed Threshold\n"
        f"(eps={fixed_eps}m, minpoints={fixed_minpts})"
    )
    ax.legend(title="vehicle threshold")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    out = OUTPUT_DIR / "threshold_impact.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info(f"  threshold_impact.png -> {out}")
    return out


def chart_eps_minpoints_impact(rows: list[dict]) -> Path:
    """Cluster count vs eps, grouped by minpoints. Fixed speed=20, vehicle=500."""
    import matplotlib.pyplot as plt

    # Use the most common/default thresholds
    speed_vals = sorted({int(r["speed_threshold_kmh"]) for r in rows})
    vehicle_vals = sorted({int(r["vehicle_threshold"]) for r in rows})
    fixed_speed = 20 if 20 in speed_vals else speed_vals[len(speed_vals) // 2]
    fixed_vehicle = 500 if 500 in vehicle_vals else vehicle_vals[len(vehicle_vals) // 2]

    minpts_vals = sorted({int(r["minpoints"]) for r in rows})

    fig, ax = plt.subplots(figsize=(9, 5))
    for mp in minpts_vals:
        subset = [
            r for r in rows
            if int(r["minpoints"]) == mp
            and int(r["speed_threshold_kmh"]) == fixed_speed
            and int(r["vehicle_threshold"]) == fixed_vehicle
            and r["cluster_count"] not in ("", "ERROR")
        ]
        if not subset:
            continue
        subset.sort(key=lambda r: float(r["eps_meters"]))
        x = [float(r["eps_meters"]) for r in subset]
        y = [int(r["cluster_count"]) for r in subset]
        ax.plot(x, y, marker="s", label=f"minpoints={mp}")

    ax.set_xlabel("eps (metres)")
    ax.set_ylabel("Cluster count")
    ax.set_title(
        f"Cluster Count vs Spatial Epsilon\n"
        f"(speed<{fixed_speed} km/h, vehicle>{fixed_vehicle})"
    )
    ax.legend(title="minpoints")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()

    out = OUTPUT_DIR / "eps_minpoints_impact.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info(f"  eps_minpoints_impact.png -> {out}")
    return out


def chart_runtime(rows: list[dict]) -> Path:
    """Runtime distribution as a histogram."""
    import matplotlib.pyplot as plt

    runtimes = [float(r["runtime_s"]) for r in rows if r.get("runtime_s") not in ("", "ERROR")]
    if not runtimes:
        logger.warning("  No valid runtime data — skipping runtime chart.")
        return None

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.hist(runtimes, bins=20, edgecolor="white", color="steelblue")
    ax.set_xlabel("Runtime per combination (s)")
    ax.set_ylabel("Frequency")
    ax.set_title("Runtime Distribution across Parameter Combinations")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = OUTPUT_DIR / "runtime_comparison.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info(f"  runtime_comparison.png -> {out}")
    return out


def chart_density_comparison(rows: list[dict]) -> Path:
    """Bar chart: static-only, overlap, density-only geohash cell counts."""
    import matplotlib.pyplot as plt

    counts = {"static_only": 0, "both": 0, "density_only": 0}
    for r in rows:
        method = r.get("filter_method", "")
        if method in counts:
            counts[method] += 1

    fig, ax = plt.subplots(figsize=(6, 4))
    labels = ["Static only", "Both", "Density only"]
    values = [counts["static_only"], counts["both"], counts["density_only"]]
    colors = ["#4472C4", "#70AD47", "#ED7D31"]
    bars = ax.bar(labels, values, color=colors, edgecolor="white")
    ax.bar_label(bars, padding=3)
    ax.set_ylabel("Unique geohash cells")
    ax.set_title("Static Baseline vs Geohash Density Prototype\n(area-based approximation)")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()

    out = OUTPUT_DIR / "density_filter_comparison.png"
    fig.savefig(out, dpi=150)
    plt.close(fig)
    logger.info(f"  density_filter_comparison.png -> {out}")
    return out


def main() -> None:
    ensure_cli_logging()
    parser = argparse.ArgumentParser(description="Generate charts from experiment CSV outputs")
    parser.add_argument("--skip-density", action="store_true", help="Skip density comparison chart")
    args = parser.parse_args()

    if not _check_matplotlib():
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    generated = []

    # ── Parameter sensitivity charts ──────────────────────────────────────
    if not SENSITIVITY_CSV.exists():
        logger.warning(
            f"\n{SENSITIVITY_CSV} not found.\n"
            "Run first:\n\n  python experiments/parameter_sensitivity.py\n"
        )
    else:
        logger.info(f"\nReading {SENSITIVITY_CSV} ...")
        sensitivity_rows = _load_csv(SENSITIVITY_CSV)
        valid_rows = [r for r in sensitivity_rows if r.get("cluster_count") not in ("", "ERROR")]
        logger.info(f"  {len(valid_rows)} valid rows (of {len(sensitivity_rows)} total)")

        if valid_rows:
            logger.info("\nGenerating sensitivity charts ...")
            generated.append(chart_threshold_impact(valid_rows))
            generated.append(chart_eps_minpoints_impact(valid_rows))
            generated.append(chart_runtime(sensitivity_rows))

    # ── Density comparison chart ──────────────────────────────────────────
    if not args.skip_density:
        if not DENSITY_CSV.exists():
            logger.warning(
                f"\n{DENSITY_CSV} not found.\n"
                "Run first:\n\n  python experiments/dynamic_density.py\n"
            )
        else:
            logger.info(f"\nReading {DENSITY_CSV} ...")
            density_rows = _load_csv(DENSITY_CSV)
            logger.info(f"  {len(density_rows)} rows")
            if density_rows:
                logger.info("\nGenerating density comparison chart ...")
                generated.append(chart_density_comparison(density_rows))

    valid_generated = [p for p in generated if p is not None]
    if valid_generated:
        logger.info(f"\nDone. {len(valid_generated)} chart(s) written to {OUTPUT_DIR}")
    else:
        logger.warning("\nNo charts generated. Check that experiment CSVs exist and contain data.")


if __name__ == "__main__":
    main()

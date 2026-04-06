"""
ETL Pipeline Scheduler

Compatibility runner for the Spark ETL pipeline.

Airflow is the primary orchestrator. This script remains as a local fallback
for manual runs and quick testing.

Usage:
    python scripts/etl_scheduler.py

For testing:
    python scripts/etl_scheduler.py --interval 30 --auto-copy

For production:
    python scripts/etl_scheduler.py --mode s3 --interval 600
"""

from __future__ import annotations

import argparse
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DEFAULT_INTERVAL_SECONDS = 30

sys.path.insert(0, str(Path(__file__).resolve().parent))
from etl_pipeline import (  # noqa: E402
    ETL_LOCAL_BASE_DIR,
    build_runtime_config,
    create_spark_session,
    resolve_output_paths,
    run_etl_pipeline,
)


def _resolve_local_landing(local_base: Path) -> Path:
    return local_base / "landing"


def check_landing_zone(landing_dir: Path) -> bool:
    """Check if there are new files in the landing zone."""
    if not landing_dir.exists():
        landing_dir.mkdir(parents=True, exist_ok=True)
        return False

    files = list(landing_dir.glob("*.json")) + list(landing_dir.glob("*.jsonl"))
    return len(files) > 0


def copy_mock_data(local_base: Path) -> int:
    """Copy sample data from the local mock staging area to the landing zone."""
    s3_mock_path = local_base / "s3_mock" / "raw"
    landing_dir = _resolve_local_landing(local_base)

    if not s3_mock_path.exists():
        print(f"  No mock data found at {s3_mock_path}")
        return 0

    vitals_path = s3_mock_path / "vitals"
    movement_path = s3_mock_path / "movement"

    files_copied = 0

    if vitals_path.exists():
        for jsonl_file in vitals_path.rglob("*.jsonl"):
            dest = landing_dir / f"vitals_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            shutil.copy2(jsonl_file, dest)
            files_copied += 1
            print(f"  Copied: {jsonl_file.name} -> landing/")
            break

    if movement_path.exists():
        for jsonl_file in movement_path.rglob("*.jsonl"):
            dest = landing_dir / f"movement_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            shutil.copy2(jsonl_file, dest)
            files_copied += 1
            print(f"  Copied: {jsonl_file.name} -> landing/")
            break

    return files_copied


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="ETL Pipeline Scheduler")
    parser.add_argument(
        "--interval",
        type=int,
        default=DEFAULT_INTERVAL_SECONDS,
        help=f"Run interval in seconds (default: {DEFAULT_INTERVAL_SECONDS})",
    )
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--auto-copy", action="store_true", help="Auto-copy mock data for local testing")
    parser.add_argument(
        "--mode",
        choices=("auto", "local", "s3"),
        default="auto",
        help="Execution mode passed to the shared ETL pipeline.",
    )
    parser.add_argument(
        "--local-base",
        type=Path,
        default=ETL_LOCAL_BASE_DIR,
        help="Base directory for local ETL input/output.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    """Main scheduler loop."""
    args = parse_args(argv)
    config = build_runtime_config(mode=args.mode, local_base=args.local_base)
    output_paths = resolve_output_paths(config)
    landing_dir = Path(output_paths["landing"]) if config.use_local else None

    print("\n" + "=" * 60)
    print("ETL PIPELINE SCHEDULER")
    print("=" * 60)
    print(f"Interval: {args.interval} seconds")
    print(f"Mode: {config.mode_label}")
    print(f"Local Base: {config.local_base}")
    if landing_dir is not None:
        print(f"Landing Zone: {landing_dir}")
    print(f"Auto-copy: {args.auto_copy}")
    print("=" * 60)

    run_count = 0
    success_count = 0

    spark = None

    try:
        spark = create_spark_session()

        while True:
            run_count += 1
            start_time = datetime.now()

            print(f"\n[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] === ETL Run #{run_count} ===")

            has_data = True
            if config.use_local:
                has_data = check_landing_zone(landing_dir)

                if not has_data:
                    print("  No data in landing zone...")

                    if args.auto_copy:
                        print("  Auto-copying mock data...")
                        files_copied = copy_mock_data(config.local_base)
                        if files_copied > 0:
                            has_data = True
                        else:
                            print("  No mock data available to copy")

            if has_data:
                print("  Processing data through ETL pipeline...")
                try:
                    run_etl_pipeline(spark, use_local=config.use_local, output_paths=output_paths)
                    success_count += 1
                    print("  ✓ ETL completed successfully")

                    if config.use_local and landing_dir is not None:
                        for f in landing_dir.glob("*.json*"):
                            f.unlink()
                        print("  Cleaned up processed files from landing zone")
                except Exception as exc:
                    print(f"  ✗ ETL failed - check logs: {exc}")
            else:
                print("  Skipping this run - no data available")

            elapsed = (datetime.now() - start_time).total_seconds()
            sleep_time = max(0, args.interval - elapsed)

            if args.once:
                break

            print(f"\n  Next run in {sleep_time:.0f} seconds...")
            print("  (Press Ctrl+C to stop)")
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n\n" + "=" * 60)
        print("Scheduler stopped by user")
        print("=" * 60)

    finally:
        if spark is not None:
            spark.stop()

        print(f"\nFinal Statistics:")
        print(f"  Total runs: {run_count}")
        print(f"  Successful: {success_count}")
        print(f"  Failed: {run_count - success_count}")
        print("=" * 60)


if __name__ == "__main__":
    main()

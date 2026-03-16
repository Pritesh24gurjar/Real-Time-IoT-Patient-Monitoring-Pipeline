"""
ETL Pipeline Scheduler

Runs the ETL pipeline every 30 seconds (testing) or 10 minutes (production).
Monitors the landing zone for new data and processes it through Bronze/Silver/Gold layers.

Usage:
    python scripts/etl_scheduler.py
    
For testing:
    python scripts/etl_scheduler.py --interval 30
    
For production (10 minutes):
    python scripts/etl_scheduler.py --interval 600
"""

import os
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
DEFAULT_INTERVAL_SECONDS = 30  # Testing: 30 seconds, Production: 600 (10 minutes)
LOCAL_LANDING = Path(r"C:\Users\prite\Documents\CWRU courses\data eng\DE project\data\etl_output\landing")


def check_landing_zone():
    """Check if there are new files in the landing zone."""
    if not LOCAL_LANDING.exists():
        LOCAL_LANDING.mkdir(parents=True, exist_ok=True)
        return False
    
    # Check for JSON/JSONL files
    files = list(LOCAL_LANDING.glob("*.json")) + list(LOCAL_LANDING.glob("*.jsonl"))
    return len(files) > 0


def run_etl_with_spark():
    """Run the ETL pipeline using Spark."""
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, to_timestamp, sqrt, when, window, avg, max as spark_max, sum as spark_sum
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("HealthData-ETL-Scheduler") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/etl-scheduler") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        # Import and run ETL
        sys.path.insert(0, str(Path(__file__).parent))
        from etl_pipeline import run_etl_pipeline
        run_etl_pipeline(spark, use_local=True)
        return True
        
    except Exception as e:
        print(f"✗ ETL failed: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        spark.stop()


def copy_mock_data():
    """Copy sample data from S3 mock to landing zone for testing."""
    import shutil
    
    s3_mock_path = Path(r"C:\Users\prite\Documents\CWRU courses\data eng\DE project\data\s3_mock\raw")
    
    if not s3_mock_path.exists():
        print(f"  No mock data found at {s3_mock_path}")
        return 0
    
    # Find latest hour directory for vitals
    vitals_path = s3_mock_path / "vitals"
    movement_path = s3_mock_path / "movement"
    
    files_copied = 0
    
    # Copy vitals data
    if vitals_path.exists():
        for jsonl_file in vitals_path.rglob("*.jsonl"):
            dest = LOCAL_LANDING / f"vitals_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            shutil.copy2(jsonl_file, dest)
            files_copied += 1
            print(f"  Copied: {jsonl_file.name} -> landing/")
            break  # Just copy one file for testing
    
    # Copy movement data
    if movement_path.exists():
        for jsonl_file in movement_path.rglob("*.jsonl"):
            dest = LOCAL_LANDING / f"movement_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
            shutil.copy2(jsonl_file, dest)
            files_copied += 1
            print(f"  Copied: {jsonl_file.name} -> landing/")
            break  # Just copy one file for testing
    
    return files_copied


def main():
    """Main scheduler loop."""
    parser = argparse.ArgumentParser(description="ETL Pipeline Scheduler")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SECONDS,
                       help=f"Run interval in seconds (default: {DEFAULT_INTERVAL_SECONDS})")
    parser.add_argument("--once", action="store_true",
                       help="Run once and exit")
    parser.add_argument("--auto-copy", action="store_true",
                       help="Auto-copy mock data from s3_mock for testing")
    
    args = parser.parse_args()
    
    print("\n" + "="*60)
    print("ETL PIPELINE SCHEDULER")
    print("="*60)
    print(f"Interval: {args.interval} seconds")
    print(f"Landing Zone: {LOCAL_LANDING}")
    print(f"Auto-copy: {args.auto_copy}")
    print("="*60)
    
    run_count = 0
    success_count = 0
    
    try:
        while True:
            run_count += 1
            start_time = datetime.now()
            
            print(f"\n[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] "
                  f"=== ETL Run #{run_count} ===")
            
            # Check landing zone
            has_data = check_landing_zone()
            
            if not has_data:
                print("  No data in landing zone...")
                
                if args.auto_copy:
                    print("  Auto-copying mock data...")
                    files_copied = copy_mock_data()
                    if files_copied > 0:
                        has_data = True
                    else:
                        print("  No mock data available to copy")
            
            if has_data:
                print("  Processing data through ETL pipeline...")
                success = run_etl_with_spark()
                
                if success:
                    success_count += 1
                    print("  ✓ ETL completed successfully")
                    
                    # Clean up processed files
                    for f in LOCAL_LANDING.glob("*.json*"):
                        f.unlink()
                    print("  Cleaned up processed files from landing zone")
                else:
                    print("  ✗ ETL failed - check logs")
            else:
                print("  Skipping this run - no data available")
            
            # Calculate next run time
            elapsed = (datetime.now() - start_time).total_seconds()
            sleep_time = max(0, args.interval - elapsed)
            
            if args.once:
                break
            
            print(f"\n  Next run in {sleep_time:.0f} seconds...")
            print(f"  (Press Ctrl+C to stop)")
            
            time.sleep(sleep_time)
    
    except KeyboardInterrupt:
        print("\n\n" + "="*60)
        print("Scheduler stopped by user")
        print("="*60)
    
    finally:
        print(f"\nFinal Statistics:")
        print(f"  Total runs: {run_count}")
        print(f"  Successful: {success_count}")
        print(f"  Failed: {run_count - success_count}")
        print("="*60)


if __name__ == "__main__":
    main()

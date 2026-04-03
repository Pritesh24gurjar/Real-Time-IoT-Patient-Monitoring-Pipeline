"""
ETL Pipeline - Bronze/Silver/Gold Layers

Processes raw JSON data from S3 landing zone through three ETL layers:
- Bronze: Raw ingestion with lineage metadata
- Silver: Data cleaning and feature engineering  
- Gold: Clinical aggregation for dashboards

Runs every 30 seconds (testing) or 10 minutes (production).

Usage:
    python scripts/etl_pipeline.py
"""

import os
import sys
import argparse
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
AWS_REGION = os.getenv('AWS_REGION', 'us-west-2')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
ETL_LOCAL_BASE_DIR = Path(
    os.getenv(
        "ETL_LOCAL_BASE_DIR",
        Path(__file__).resolve().parents[1] / "data" / "etl_output",
    )
)

# Fall detection threshold
FALL_SVM_THRESHOLD = 25.0


@dataclass(frozen=True)
class ETLRuntimeConfig:
    """Resolved runtime configuration for the ETL pipeline."""

    use_local: bool
    local_base: Path
    s3_bucket_name: str | None

    @property
    def mode_label(self) -> str:
        return "Local Testing" if self.use_local else "Production (S3)"

    @property
    def landing_path(self) -> Path | str:
        return self.local_base / "landing" if self.use_local else f"s3://{self.s3_bucket_name}/landing/"

    @property
    def bronze_vitals_path(self) -> Path | str:
        return self.local_base / "bronze" / "vitals" if self.use_local else f"s3://{self.s3_bucket_name}/bronze/vitals/"

    @property
    def bronze_movement_path(self) -> Path | str:
        return self.local_base / "bronze" / "movement" if self.use_local else f"s3://{self.s3_bucket_name}/bronze/movement/"

    @property
    def silver_vitals_path(self) -> Path | str:
        return self.local_base / "silver" / "vitals" if self.use_local else f"s3://{self.s3_bucket_name}/silver/vitals/"

    @property
    def silver_movement_path(self) -> Path | str:
        return self.local_base / "silver" / "movement" if self.use_local else f"s3://{self.s3_bucket_name}/silver/movement/"

    @property
    def gold_vitals_path(self) -> Path | str:
        return self.local_base / "gold" / "vitals_summary" if self.use_local else f"s3://{self.s3_bucket_name}/gold/vitals_summary/"

    @property
    def gold_movement_path(self) -> Path | str:
        return self.local_base / "gold" / "movement_summary" if self.use_local else f"s3://{self.s3_bucket_name}/gold/movement_summary/"


def build_runtime_config(mode: str = "auto", local_base: Path | None = None) -> ETLRuntimeConfig:
    """
    Resolve runtime configuration without asking for interactive input.

    The selection order is:
    - explicit CLI mode when provided
    - S3 only when credentials and bucket are present
    - local fallback otherwise
    """
    normalized_mode = (mode or "auto").lower()
    resolved_local_base = local_base or ETL_LOCAL_BASE_DIR

    if normalized_mode not in {"auto", "local", "s3"}:
        raise ValueError(f"Unsupported ETL mode: {mode}")

    if normalized_mode == "local":
        return ETLRuntimeConfig(use_local=True, local_base=resolved_local_base, s3_bucket_name=None)

    if normalized_mode == "s3":
        if not S3_BUCKET_NAME:
            raise ValueError("S3 mode requested but S3_BUCKET_NAME is not configured.")
        return ETLRuntimeConfig(use_local=False, local_base=resolved_local_base, s3_bucket_name=S3_BUCKET_NAME)

    has_s3_config = all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, S3_BUCKET_NAME])
    if has_s3_config:
        return ETLRuntimeConfig(use_local=False, local_base=resolved_local_base, s3_bucket_name=S3_BUCKET_NAME)

    return ETLRuntimeConfig(use_local=True, local_base=resolved_local_base, s3_bucket_name=None)


def create_spark_session():
    """Create Spark session with AWS S3 support."""
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("HealthData-ETL-Pipeline") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/etl") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def process_bronze(spark, input_path, output_paths, use_local=False):
    """
    Bronze Layer - Raw ingestion with lineage metadata.
    
    Reads raw JSON, adds ETL metadata (timestamp, source file),
    and routes data to vitals/movement bronze tables.
    """
    from pyspark.sql.functions import col, current_timestamp, lit
    import json
    
    print(f"\n{'='*60}")
    print("BRONZE LAYER - Raw Ingestion")
    print(f"{'='*60}")
    print(f"Reading from: {input_path}")
    
    # Find all JSON/JSONL files
    if use_local:
        json_files = list(input_path.glob("*.json")) + list(input_path.glob("*.jsonl"))
        if not json_files:
            print("✗ No JSON/JSONL files found in landing zone")
            return
        print(f"Found {len(json_files)} file(s)")
        
        # Read files manually and create DataFrame
        all_records = []
        for json_file in json_files:
            with open(json_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            record = json.loads(line)
                            record['etl_processed_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            record['source_file'] = str(json_file)
                            record['etl_batch_date'] = datetime.now().strftime("%Y-%m-%d")
                            all_records.append(record)
                        except json.JSONDecodeError as e:
                            print(f"  Warning: Skipping invalid JSON line: {e}")
        
        if not all_records:
            print("✗ No valid records found in files")
            return
        
        # Create DataFrame from records
        raw_with_meta = spark.createDataFrame(all_records)
        print(f"✓ Loaded {len(all_records)} records")
    else:
        # S3 mode - use Spark read
        try:
            raw_df = spark.read.json(input_path)
            print(f"✓ Loaded {raw_df.count()} records")
            
            raw_with_meta = raw_df \
                .withColumn("etl_processed_time", current_timestamp()) \
                .withColumn("source_file", lit("s3_source")) \
                .withColumn("etl_batch_date", lit(datetime.now().strftime("%Y-%m-%d")))
        except Exception as e:
            print(f"✗ No data found or read error: {e}")
            return
    
    # Router pattern - split by device_type
    vitals_bronze = raw_with_meta.filter(col("device_type") == "vital_monitor")
    movement_bronze = raw_with_meta.filter(col("device_type") == "imu_sensor")
    
    print(f"  - Vitals records: {vitals_bronze.count()}")
    print(f"  - Movement records: {movement_bronze.count()}")
    
    # Write to bronze layer
    if vitals_bronze.count() > 0:
        output_path = output_paths["bronze_vitals"]
        if use_local:
            output_path.mkdir(parents=True, exist_ok=True)
        vitals_bronze.write.mode("append").parquet(str(output_path) if use_local else output_path)
        print(f"✓ Vitals bronze saved to: {output_path}")
    
    if movement_bronze.count() > 0:
        output_path = output_paths["bronze_movement"]
        if use_local:
            output_path.mkdir(parents=True, exist_ok=True)
        movement_bronze.write.mode("append").parquet(str(output_path) if use_local else output_path)
        print(f"✓ Movement bronze saved to: {output_path}")


def process_silver(spark, output_paths, use_local=False):
    """
    Silver Layer - Data cleaning and feature engineering.
    
    Cleans data: drops nulls, filters impossible values,
    converts timestamps, calculates SVM for movement data.
    """
    from pyspark.sql.functions import to_timestamp, sqrt, col
    
    print(f"\n{'='*60}")
    print("SILVER LAYER - Data Cleaning & Features")
    print(f"{'='*60}")
    
    # --- VITALS CLEANING ---
    print("\nCleaning Vitals Data...")
    vitals_input = output_paths["bronze_vitals"]
    
    if use_local and not vitals_input.exists():
        print(f"  ✗ Bronze vitals path does not exist: {vitals_input}")
        return
    
    try:
        vitals_bronze = spark.read.parquet(str(vitals_input) if use_local else vitals_input)
        count = vitals_bronze.count()
        print(f"  Loaded {count} bronze records")
        
        if count == 0:
            print("  No data to process")
            return
        
        vitals_silver = vitals_bronze \
            .filter(col("heart_rate").isNotNull()) \
            .filter((col("heart_rate") >= 30) & (col("heart_rate") <= 220)) \
            .filter(col("spo2").isNotNull()) \
            .filter((col("spo2") >= 90) & (col("spo2") <= 100)) \
            .withColumn("event_timestamp", to_timestamp(col("timestamp")))
        
        print(f"  After cleaning: {vitals_silver.count()} records")
        
        if vitals_silver.count() > 0:
            output_path = output_paths["silver_vitals"]
            if use_local:
                output_path.mkdir(parents=True, exist_ok=True)
            vitals_silver.write.mode("append").parquet(str(output_path) if use_local else output_path)
            print(f"✓ Vitals silver saved to: {output_path}")
            
    except Exception as e:
        print(f"  ✗ Vitals processing error: {e}")
    
    # --- MOVEMENT CLEANING & MATH ---
    print("\nCleaning Movement Data...")
    movement_input = output_paths["bronze_movement"]
    
    if use_local and not movement_input.exists():
        print(f"  ✗ Bronze movement path does not exist: {movement_input}")
        return
    
    try:
        movement_bronze = spark.read.parquet(str(movement_input) if use_local else movement_input)
        count = movement_bronze.count()
        print(f"  Loaded {count} bronze records")
        
        if count == 0:
            print("  No data to process")
            return
        
        movement_silver = movement_bronze \
            .filter(col("x").isNotNull() & col("y").isNotNull() & col("z").isNotNull()) \
            .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("svm", sqrt((col("x")**2) + (col("y")**2) + (col("z")**2)))
        
        print(f"  After cleaning: {movement_silver.count()} records")
        print(f"  ✓ Added SVM (Signal Vector Magnitude) feature")
        
        if movement_silver.count() > 0:
            output_path = output_paths["silver_movement"]
            if use_local:
                output_path.mkdir(parents=True, exist_ok=True)
            movement_silver.write.mode("append").parquet(str(output_path) if use_local else output_path)
            print(f"✓ Movement silver saved to: {output_path}")
            
    except Exception as e:
        print(f"  ✗ Movement processing error: {e}")


def process_gold(spark, output_paths, use_local=False):
    """
    Gold Layer - Clinical aggregation for dashboards.
    
    Aggregates data into time windows, calculates MEWS scores
    for vitals and counts fall events for movement.
    """
    from pyspark.sql.functions import window, avg, max as spark_max, sum as spark_sum, when, col
    
    print(f"\n{'='*60}")
    print("GOLD LAYER - Clinical Aggregation")
    print(f"{'='*60}")
    
    # --- VITALS GOLD (MEWS Score) ---
    print("\nAggregating Vitals (MEWS Score)...")
    vitals_input = output_paths["silver_vitals"]
    
    if use_local and not vitals_input.exists():
        print(f"  ✗ Silver vitals path does not exist: {vitals_input}")
        return
    
    try:
        vitals_silver = spark.read.parquet(str(vitals_input) if use_local else vitals_input)
        count = vitals_silver.count()
        print(f"  Loaded {count} silver records")
        
        if count == 0:
            print("  No data to process")
            return
        
        vitals_gold = vitals_silver \
            .groupBy("patient_id", window("event_timestamp", "10 minutes")) \
            .agg(
                avg("heart_rate").alias("avg_hr"),
                spark_max("heart_rate").alias("peak_hr"),
                avg("spo2").alias("avg_spo2"),
                spark_max("spo2").alias("peak_spo2")
            ) \
            .withColumn("mews_score",
                when(col("avg_hr") > 130, 3)      # Critical Tachycardia
                .when(col("avg_hr") >= 111, 2)    # High HR
                .when(col("avg_hr") <= 40, 2)     # Critical Bradycardia
                .when(col("avg_spo2") < 90, 3)    # Hypoxia
                .otherwise(0)                      # Normal
            )
        
        print(f"  Generated {vitals_gold.count()} aggregated records")
        print(f"  ✓ Added MEWS score (Modified Early Warning Score)")
        
        if vitals_gold.count() > 0:
            output_path = output_paths["gold_vitals"]
            if use_local:
                output_path.mkdir(parents=True, exist_ok=True)
            vitals_gold.write.mode("append").parquet(str(output_path) if use_local else output_path)
            print(f"✓ Vitals gold saved to: {output_path}")
            
            # Show sample
            print("\n  Sample Gold Records:")
            vitals_gold.select("patient_id", "avg_hr", "peak_hr", "avg_spo2", "mews_score").show(5)
            
    except Exception as e:
        print(f"  ✗ Vitals gold processing error: {e}")
    
    # --- MOVEMENT GOLD (Fall Detection) ---
    print("\nAggregating Movement (Fall Detection)...")
    movement_input = output_paths["silver_movement"]
    
    if use_local and not movement_input.exists():
        print(f"  ✗ Silver movement path does not exist: {movement_input}")
        return
    
    try:
        movement_silver = spark.read.parquet(str(movement_input) if use_local else movement_input)
        count = movement_silver.count()
        print(f"  Loaded {count} silver records")
        
        if count == 0:
            print("  No data to process")
            return
        
        movement_gold = movement_silver \
            .withColumn("is_fall", when(col("svm") > FALL_SVM_THRESHOLD, 1).otherwise(0)) \
            .groupBy("patient_id", window("event_timestamp", "10 minutes")) \
            .agg(
                avg("svm").alias("avg_activity"),
                spark_max("svm").alias("peak_impact"),
                spark_sum("is_fall").alias("fall_events_detected")
            )
        
        print(f"  Generated {movement_gold.count()} aggregated records")
        print(f"  ✓ Fall threshold: SVM > {FALL_SVM_THRESHOLD}")
        
        if movement_gold.count() > 0:
            output_path = output_paths["gold_movement"]
            if use_local:
                output_path.mkdir(parents=True, exist_ok=True)
            movement_gold.write.mode("append").parquet(str(output_path) if use_local else output_path)
            print(f"✓ Movement gold saved to: {output_path}")
            
            # Show sample
            print("\n  Sample Gold Records:")
            movement_gold.select("patient_id", "avg_activity", "peak_impact", "fall_events_detected").show(5)
            
    except Exception as e:
        print(f"  ✗ Movement gold processing error: {e}")


def resolve_output_paths(config: ETLRuntimeConfig):
    """Build runtime-specific landing and output paths."""
    if config.use_local:
        return {
            "landing": config.landing_path,
            "bronze_vitals": config.bronze_vitals_path,
            "bronze_movement": config.bronze_movement_path,
            "silver_vitals": config.silver_vitals_path,
            "silver_movement": config.silver_movement_path,
            "gold_vitals": config.gold_vitals_path,
            "gold_movement": config.gold_movement_path,
        }

    return {
        "landing": config.landing_path,
        "bronze_vitals": config.bronze_vitals_path,
        "bronze_movement": config.bronze_movement_path,
        "silver_vitals": config.silver_vitals_path,
        "silver_movement": config.silver_movement_path,
        "gold_vitals": config.gold_vitals_path,
        "gold_movement": config.gold_movement_path,
    }


def run_etl_pipeline(spark, use_local=True, output_paths=None):
    """Run complete ETL pipeline: Bronze → Silver → Gold."""
    
    print("\n" + "="*60)
    print("ETL PIPELINE - Starting")
    print("="*60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'Local Testing' if use_local else 'Production (S3)'}")
    
    # Determine input path
    output_paths = output_paths or resolve_output_paths(
        ETLRuntimeConfig(
            use_local=use_local,
            local_base=ETL_LOCAL_BASE_DIR,
            s3_bucket_name=S3_BUCKET_NAME,
        )
    )

    if use_local:
        input_path = output_paths["landing"]
        print(f"Input: {input_path}")
    else:
        input_path = output_paths["landing"]
        print(f"Input: {input_path}")
    
    # Check if input has data
    if use_local and not input_path.exists():
        print(f"✗ Input directory does not exist: {input_path}")
        print("  Creating directory...")
        input_path.mkdir(parents=True, exist_ok=True)
        return
    
    # Run ETL layers
    process_bronze(spark, input_path, output_paths, use_local)
    process_silver(spark, output_paths, use_local)
    process_gold(spark, output_paths, use_local)
    
    print("\n" + "="*60)
    print("ETL PIPELINE - Complete")
    print("="*60)


def parse_args(argv=None):
    """Parse CLI arguments for non-interactive execution."""
    parser = argparse.ArgumentParser(description="Health Data ETL Pipeline")
    parser.add_argument(
        "--mode",
        choices=("auto", "local", "s3"),
        default="auto",
        help="Execution mode. auto uses S3 when credentials are present, otherwise local.",
    )
    parser.add_argument(
        "--local-base",
        type=Path,
        default=ETL_LOCAL_BASE_DIR,
        help="Base directory for local ETL outputs.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    """Main entry point."""
    args = parse_args(argv)
    config = build_runtime_config(mode=args.mode, local_base=args.local_base)

    print("\n" + "="*60)
    print("HEALTH DATA ETL PIPELINE")
    print("="*60)

    if config.use_local:
        print("\nRunning in LOCAL TEST MODE")
        print(f"Data location: {config.local_base}")
        print("To use S3, configure .env file with AWS credentials or pass --mode s3")
    else:
        print("\nRunning in PRODUCTION MODE")
        print(f"S3 Bucket: s3://{config.s3_bucket_name}")
    
    # Create Spark session
    print("\nInitializing Spark...")
    spark = create_spark_session()
    
    try:
        # Run ETL pipeline
        run_etl_pipeline(spark, config.use_local, resolve_output_paths(config))
        
    except Exception as e:
        print(f"\n✗ ETL Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()

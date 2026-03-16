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

# S3 Paths
S3_LANDING = f"s3://{S3_BUCKET_NAME}/landing/"
S3_BRONZE_VITALS = f"s3://{S3_BUCKET_NAME}/bronze/vitals/"
S3_BRONZE_MOVEMENT = f"s3://{S3_BUCKET_NAME}/bronze/movement/"
S3_SILVER_VITALS = f"s3://{S3_BUCKET_NAME}/silver/vitals/"
S3_SILVER_MOVEMENT = f"s3://{S3_BUCKET_NAME}/silver/movement/"
S3_GOLD_VITALS = f"s3://{S3_BUCKET_NAME}/gold/vitals_summary/"
S3_GOLD_MOVEMENT = f"s3://{S3_BUCKET_NAME}/gold/movement_summary/"

# Local testing paths (when S3 not configured)
LOCAL_BASE = Path(r"C:\Users\prite\Documents\CWRU courses\data eng\DE project\data\etl_output")
LOCAL_LANDING = LOCAL_BASE / "landing"
LOCAL_BRONZE_VITALS = LOCAL_BASE / "bronze" / "vitals"
LOCAL_BRONZE_MOVEMENT = LOCAL_BASE / "bronze" / "movement"
LOCAL_SILVER_VITALS = LOCAL_BASE / "silver" / "vitals"
LOCAL_SILVER_MOVEMENT = LOCAL_BASE / "silver" / "movement"
LOCAL_GOLD_VITALS = LOCAL_BASE / "gold" / "vitals_summary"
LOCAL_GOLD_MOVEMENT = LOCAL_BASE / "gold" / "movement_summary"

# Fall detection threshold
FALL_SVM_THRESHOLD = 25.0


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


def process_bronze(spark, input_path, use_local=False):
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
        output_path = LOCAL_BRONZE_VITALS if use_local else S3_BRONZE_VITALS
        if use_local:
            output_path.mkdir(parents=True, exist_ok=True)
        vitals_bronze.write.mode("append").parquet(str(output_path) if use_local else output_path)
        print(f"✓ Vitals bronze saved to: {output_path}")
    
    if movement_bronze.count() > 0:
        output_path = LOCAL_BRONZE_MOVEMENT if use_local else S3_BRONZE_MOVEMENT
        if use_local:
            output_path.mkdir(parents=True, exist_ok=True)
        movement_bronze.write.mode("append").parquet(str(output_path) if use_local else output_path)
        print(f"✓ Movement bronze saved to: {output_path}")


def process_silver(spark, use_local=False):
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
    vitals_input = LOCAL_BRONZE_VITALS if use_local else S3_BRONZE_VITALS
    
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
            output_path = LOCAL_SILVER_VITALS if use_local else S3_SILVER_VITALS
            if use_local:
                output_path.mkdir(parents=True, exist_ok=True)
            vitals_silver.write.mode("append").parquet(str(output_path) if use_local else output_path)
            print(f"✓ Vitals silver saved to: {output_path}")
            
    except Exception as e:
        print(f"  ✗ Vitals processing error: {e}")
    
    # --- MOVEMENT CLEANING & MATH ---
    print("\nCleaning Movement Data...")
    movement_input = LOCAL_BRONZE_MOVEMENT if use_local else S3_BRONZE_MOVEMENT
    
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
            output_path = LOCAL_SILVER_MOVEMENT if use_local else S3_SILVER_MOVEMENT
            if use_local:
                output_path.mkdir(parents=True, exist_ok=True)
            movement_silver.write.mode("append").parquet(str(output_path) if use_local else output_path)
            print(f"✓ Movement silver saved to: {output_path}")
            
    except Exception as e:
        print(f"  ✗ Movement processing error: {e}")


def process_gold(spark, use_local=False):
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
    vitals_input = LOCAL_SILVER_VITALS if use_local else S3_SILVER_VITALS
    
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
            output_path = LOCAL_GOLD_VITALS if use_local else S3_GOLD_VITALS
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
    movement_input = LOCAL_SILVER_MOVEMENT if use_local else S3_SILVER_MOVEMENT
    
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
            output_path = LOCAL_GOLD_MOVEMENT if use_local else S3_GOLD_MOVEMENT
            if use_local:
                output_path.mkdir(parents=True, exist_ok=True)
            movement_gold.write.mode("append").parquet(str(output_path) if use_local else output_path)
            print(f"✓ Movement gold saved to: {output_path}")
            
            # Show sample
            print("\n  Sample Gold Records:")
            movement_gold.select("patient_id", "avg_activity", "peak_impact", "fall_events_detected").show(5)
            
    except Exception as e:
        print(f"  ✗ Movement gold processing error: {e}")


def run_etl_pipeline(spark, use_local=True):
    """Run complete ETL pipeline: Bronze → Silver → Gold."""
    
    print("\n" + "="*60)
    print("ETL PIPELINE - Starting")
    print("="*60)
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Mode: {'Local Testing' if use_local else 'Production (S3)'}")
    
    # Determine input path
    if use_local:
        input_path = LOCAL_LANDING
        print(f"Input: {input_path}")
    else:
        input_path = S3_LANDING
        print(f"Input: {input_path}")
    
    # Check if input has data
    if use_local and not input_path.exists():
        print(f"✗ Input directory does not exist: {input_path}")
        print("  Creating directory...")
        input_path.mkdir(parents=True, exist_ok=True)
        return
    
    # Run ETL layers
    process_bronze(spark, input_path, use_local)
    process_silver(spark, use_local)
    process_gold(spark, use_local)
    
    print("\n" + "="*60)
    print("ETL PIPELINE - Complete")
    print("="*60)


def main():
    """Main entry point."""
    print("\n" + "="*60)
    print("HEALTH DATA ETL PIPELINE")
    print("="*60)
    
    # Check if running in local test mode or production
    use_local = True
    if all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, S3_BUCKET_NAME]):
        response = input("\nAWS credentials found. Use S3? (y/n): ").lower()
        if response == 'y':
            use_local = False
    
    if use_local:
        print("\nRunning in LOCAL TEST MODE")
        print(f"Data location: {LOCAL_BASE}")
        print("To use S3, configure .env file with AWS credentials")
    else:
        print("\nRunning in PRODUCTION MODE")
        print(f"S3 Bucket: s3://{S3_BUCKET_NAME}")
    
    # Create Spark session
    print("\nInitializing Spark...")
    spark = create_spark_session()
    
    try:
        # Run ETL pipeline
        run_etl_pipeline(spark, use_local)
        
    except Exception as e:
        print(f"\n✗ ETL Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    main()

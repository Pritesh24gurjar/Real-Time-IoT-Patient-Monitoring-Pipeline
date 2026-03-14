"""
Real-Time Alert Engine - Spark Structured Streaming

Consumes raw_vitals and raw_movement streams from Kafka,
evaluates rules in real-time, and publishes critical alerts.

Alert Rules:
- FALL_DETECTED: SVM > 25.0 (sudden impact)
- TACHYCARDIA: Heart rate > 130 BPM
- BRADYCARDIA: Heart rate < 40 BPM
- HYPOXIA: SpO2 < 90%
- HIGH_STRESS: Stress level >= 8

Usage:
    pyspark scripts/realtime_alert_engine.py
    OR
    spark-submit scripts/realtime_alert_engine.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, to_json, struct, col, lit, sqrt, 
    when, window, count, avg, max as spark_max
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    LongType, IntegerType, FloatType, TimestampType
)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
CHECKPOINT_LOCATION = "/tmp/checkpoints"

# Alert thresholds
FALL_SVM_THRESHOLD = 25.0
TACHYCARDIA_HR_THRESHOLD = 130
BRADYCARDIA_HR_THRESHOLD = 40
HYPOXIA_SPO2_THRESHOLD = 90
HIGH_STRESS_THRESHOLD = 8


def create_spark_session():
    """Create Spark session with Kafka and logging configurations."""
    return SparkSession.builder \
        .appName("HotPathAlerts") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.kafka.maxOffsetsPerTrigger", "10000") \
        .getOrCreate()


def define_schemas():
    """Define schemas for incoming Kafka messages."""
    
    # Schema for vitals data (from iot_vitals_producer.py)
    vitals_schema = StructType([
        StructField("patient_id", StringType()),
        StructField("device_type", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("heart_rate", DoubleType()),
        StructField("spo2", DoubleType()),
        StructField("step_count", DoubleType()),
        StructField("sleep_duration", DoubleType()),
        StructField("activity_level", StringType()),
        StructField("stress_level", DoubleType())
    ])
    
    # Schema for movement data (from iot_movement_producer.py)
    movement_schema = StructType([
        StructField("patient_id", StringType()),
        StructField("device_type", StringType()),
        StructField("sensor_type", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("activity_code", StringType()),
        StructField("activity_name", StringType()),
        StructField("x", DoubleType()),
        StructField("y", DoubleType()),
        StructField("z", DoubleType()),
        StructField("svm", DoubleType())
    ])
    
    return vitals_schema, movement_schema


def read_kafka_stream(spark, topic, schema):
    """Read and parse a Kafka stream."""
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON value
    parsed = raw_stream.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("key").cast("string").alias("patient_id"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "patient_id", "kafka_timestamp")
    
    return parsed


def process_vitals_stream(vitals_df):
    """
    Process vitals stream and detect critical conditions.
    
    Alert conditions:
    - TACHYCARDIA: HR > 130
    - BRADYCARDIA: HR < 40
    - HYPOXIA: SpO2 < 90
    - HIGH_STRESS: Stress >= 8
    """
    
    # Detect vital sign anomalies
    alerts = vitals_df.select(
        col("patient_id"),
        col("timestamp"),
        col("heart_rate"),
        col("spo2"),
        col("stress_level"),
        when(col("heart_rate") > TACHYCARDIA_HR_THRESHOLD, lit("TACHYCARDIA"))
            .when(col("heart_rate") < BRADYCARDIA_HR_THRESHOLD, lit("BRADYCARDIA"))
            .otherwise(None).alias("hr_alert"),
        when(col("spo2") < HYPOXIA_SPO2_THRESHOLD, lit("HYPOXIA")).otherwise(None).alias("spo2_alert"),
        when(col("stress_level") >= HIGH_STRESS_THRESHOLD, lit("HIGH_STRESS")).otherwise(None).alias("stress_alert")
    )
    
    # Create alert records for each condition
    hr_alerts = alerts.filter(col("hr_alert").isNotNull()).select(
        col("patient_id").alias("key"),
        to_json(struct(
            col("patient_id"),
            col("timestamp"),
            lit("TACHYCARDIA").alias("alert_type")
        )).alias("value")
    )
    
    brady_alerts = alerts.filter(col("hr_alert") == "BRADYCARDIA").select(
        col("patient_id").alias("key"),
        to_json(struct(
            col("patient_id"),
            col("timestamp"),
            lit("BRADYCARDIA").alias("alert_type"),
            col("heart_rate").alias("heart_rate")
        )).alias("value")
    )
    
    hypoxia_alerts = alerts.filter(col("spo2_alert").isNotNull()).select(
        col("patient_id").alias("key"),
        to_json(struct(
            col("patient_id"),
            col("timestamp"),
            lit("HYPOXIA").alias("alert_type"),
            col("spo2").alias("spo2_level")
        )).alias("value")
    )
    
    # stress_alerts = alerts.filter(col("stress_alert").isNotNull()).select(
    #     col("patient_id").alias("key"),
    #     to_json(struct(
    #         col("patient_id"),
    #         col("timestamp"),
    #         lit("HIGH_STRESS").alias("alert_type"),
    #         col("stress_level").alias("stress_level")
    #     )).alias("value")
    # )
    
    # Union all alert types
    all_vitals_alerts = hr_alerts.union(brady_alerts).union(hypoxia_alerts)
    # .union(stress_alerts)
    
    return all_vitals_alerts


def process_movement_stream(movement_df):
    """
    Process movement stream and detect fall events.
    
    Fall detection: SVM > 25.0 indicates sudden impact
    """
    
    # Filter for fall events (high SVM impact)
    fall_alerts = movement_df.filter(col("svm") > FALL_SVM_THRESHOLD).select(
        col("patient_id").alias("key"),
        to_json(struct(
            col("patient_id"),
            col("timestamp"),
            lit("FALL_DETECTED").alias("alert_type"),
            col("svm").alias("impact_force"),
            col("activity_name").alias("activity_at_time")
        )).alias("value")
    )
    
    return fall_alerts


def write_to_kafka(df, topic, checkpoint_name):
    """Write stream to Kafka with checkpointing."""
    checkpoint_path = f"{CHECKPOINT_LOCATION}/{checkpoint_name}"
    
    query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", topic) \
        .option("checkpointLocation", checkpoint_path) \
        .option("failOnDataLoss", "false") \
        .start()
    
    return query


def console_output(df, alert_name):
    """Write stream to console for debugging."""
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .queryName(alert_name) \
        .start()
    
    return query


def main():
    """Main entry point for the alert engine."""
    print("=" * 60)
    print("REAL-TIME ALERT ENGINE - Starting...")
    print("=" * 60)
    
    # Initialize
    spark = create_spark_session()
    vitals_schema, movement_schema = define_schemas()
    
    # Read streams from Kafka
    print("Connecting to Kafka topics: raw_vitals, raw_movement")
    vitals_df = read_kafka_stream(spark, "raw_vitals", vitals_schema)
    movement_df = read_kafka_stream(spark, "raw_movement", movement_schema)
    
    # Process streams
    print("Applying alert rules...")
    vitals_alerts = process_vitals_stream(vitals_df)
    movement_alerts = process_movement_stream(movement_df)
    
    # Combine all alerts
    all_alerts = vitals_alerts.union(movement_alerts)
    
    # Write alerts to Kafka
    print("Writing alerts to critical_alerts topic...")
    alerts_query = write_to_kafka(all_alerts, "critical_alerts", "alerts_checkpoint")
    
    # Also output to console for monitoring
    console_output(all_alerts, "AlertConsole")
    
    print("=" * 60)
    print("Alert engine is running. Press Ctrl+C to stop.")
    print("=" * 60)
    
    # Wait for termination
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nShutting down alert engine...")
        spark.stop()


if __name__ == "__main__":
    import sys
    import os
    
    # Check if running with spark-submit
    if "SPARK_HOME" not in os.environ:
        print("=" * 60)
        print("ERROR: This script must be run with spark-submit")
        print("=" * 60)
        print("\nUsage:")
        print("  spark-submit --packages org.apache.spark:spark-sql-kafka-0-1_2.12:3.4.0 scripts/realtime_alert_engine.py")
        print("\nOr use the simpler Python-based alert engine:")
        print("  python scripts/simple_alert_engine.py")
        print("=" * 60)
        sys.exit(1)
    
    main()

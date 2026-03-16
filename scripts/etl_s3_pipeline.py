"""
ETL Pipeline - S3 Production Version (pandas + PyArrow, no Spark/JVM)

Processes data from the S3 landing zone through Bronze / Silver / Gold layers.
Replaces PySpark with pandas + PyArrow - identical logic, no JVM, no winutils,
no HADOOP_HOME, runs on any OS straight from the venv.

Dependencies:
    pip install pandas pyarrow boto3 python-dotenv

Landing zone layout (written by kafka_to_s3_stream.py):
    s3://<bucket>/landing/vitals/year=YYYY/.../  *.jsonl
    s3://<bucket>/landing/movement/year=YYYY/... *.jsonl

After Bronze ingestion, processed files are moved to:
    s3://<bucket>/landing/_processed/vitals/...
    s3://<bucket>/landing/_processed/movement/...

Usage:
    python scripts/etl_s3_pipeline.py                        # run once
    python scripts/etl_s3_pipeline.py --once
    python scripts/etl_s3_pipeline.py --schedule [--interval 600]
    python scripts/etl_s3_pipeline.py --test
"""

import argparse
import io
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

load_dotenv()

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN     = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION            = os.getenv("AWS_REGION", "us-west-2")
S3_BUCKET_NAME        = os.getenv("S3_BUCKET_NAME")

# S3 path constants - shared root must match kafka_to_s3_stream.py LANDING_PREFIX
LANDING_PREFIX    = "landing"
LANDING_VITALS    = f"{LANDING_PREFIX}/vitals/"
LANDING_MOVEMENT  = f"{LANDING_PREFIX}/movement/"
LANDING_PROCESSED = f"{LANDING_PREFIX}/_processed/"

BRONZE_VITALS   = "bronze/vitals/"
BRONZE_MOVEMENT = "bronze/movement/"
SILVER_VITALS   = "silver/vitals/"
SILVER_MOVEMENT = "silver/movement/"
GOLD_VITALS     = "gold/vitals_summary/"
GOLD_MOVEMENT   = "gold/movement_summary/"

# Pipeline knobs (overridable via env)
DEFAULT_INTERVAL_SEC = int(os.getenv("ETL_INTERVAL_SECONDS", "600"))
FALL_SVM_THRESHOLD   = float(os.getenv("FALL_SVM_THRESHOLD", "25.0"))
WINDOW_MINUTES       = int(os.getenv("WINDOW_MINUTES", "10"))
MIN_RECORD_THRESHOLD = int(os.getenv("MIN_RECORD_THRESHOLD", "1"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_env(*names):
    missing = [n for n in names if not os.getenv(n)]
    if missing:
        raise ValueError(
            "Missing required environment variables:\n"
            + "\n".join(f"  - {n}" for n in missing)
        )


def _make_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        region_name=AWS_REGION,
    )


# ---------------------------------------------------------------------------
# S3 I/O utilities
# ---------------------------------------------------------------------------

def list_landing_files(s3, prefix):
    """Return all .json/.jsonl S3 keys under prefix, following pagination."""
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith((".json", ".jsonl")):
                keys.append(key)
    return keys


def read_jsonl_from_s3(s3, key):
    """Download one JSONL file from S3 and parse into a DataFrame."""
    resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    return pd.read_json(io.BytesIO(resp["Body"].read()), lines=True)


def write_parquet_to_s3(s3, df, prefix, batch_date, run_id, label):
    """
    Serialize df to Parquet in memory and upload to:
        s3://<bucket>/<prefix>etl_batch_date=<batch_date>/<label>_<run_id>.parquet
    """
    if df is None or df.empty:
        return

    key = f"{prefix}etl_batch_date={batch_date}/{label}_{run_id}.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)

    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=buf.read(),
        ContentType="application/octet-stream",
        Metadata={
            "record_count": str(len(df)),
            "run_id":       run_id,
            "batch_date":   batch_date,
        },
    )
    log.info("  Written %d rows -> s3://%s/%s", len(df), S3_BUCKET_NAME, key)


def read_parquet_from_s3(s3, prefix, batch_date):
    """
    Read all Parquet files under:
        s3://<bucket>/<prefix>etl_batch_date=<batch_date>/
    and return them as a single concatenated DataFrame.
    Returns an empty DataFrame if no files exist.
    """
    partition_prefix = f"{prefix}etl_batch_date={batch_date}/"
    paginator = s3.get_paginator("list_objects_v2")
    frames = []

    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=partition_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                try:
                    resp = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
                    frames.append(
                        pd.read_parquet(io.BytesIO(resp["Body"].read()), engine="pyarrow")
                    )
                except Exception as exc:
                    log.error("  Failed to read parquet s3://%s/%s: %s",
                              S3_BUCKET_NAME, key, exc)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def archive_landing_files(s3, keys):
    """
    Move each processed landing key to landing/_processed/...
    Deletes the source only after a successful copy.
    Pattern: landing/vitals/... -> landing/_processed/vitals/...
    """
    for src_key in keys:
        rel      = src_key[len(LANDING_PREFIX) + 1:]   # strip "landing/"
        dest_key = f"{LANDING_PROCESSED}{rel}"
        try:
            s3.copy_object(
                Bucket=S3_BUCKET_NAME,
                CopySource={"Bucket": S3_BUCKET_NAME, "Key": src_key},
                Key=dest_key,
            )
            s3.delete_object(Bucket=S3_BUCKET_NAME, Key=src_key)
        except (BotoCoreError, ClientError) as exc:
            log.error("  Failed to archive %s: %s", src_key, exc)


# ---------------------------------------------------------------------------
# Bronze layer
# ---------------------------------------------------------------------------

def process_bronze(s3, run_id, batch_date):
    """
    Read JSONL from landing/, split by device_type, write Parquet to bronze/.
    Returns (success: bool, ingested_keys: list[str]).
    """
    log.info("=" * 60)
    log.info("BRONZE LAYER - landing -> bronze  [run_id=%s]", run_id)
    log.info("=" * 60)

    vitals_keys   = list_landing_files(s3, LANDING_VITALS)
    movement_keys = list_landing_files(s3, LANDING_MOVEMENT)
    all_keys      = vitals_keys + movement_keys

    if not all_keys:
        log.info("No new files in landing zone - skipping.")
        return False, []

    log.info("Found %d file(s) in landing zone.", len(all_keys))

    # Read all JSONL files
    frames = []
    for key in all_keys:
        try:
            df = read_jsonl_from_s3(s3, key)
            df["_source_key"] = key
            frames.append(df)
            log.info("  Read %d records from %s", len(df), key)
        except Exception as exc:
            log.error("  Failed to read s3://%s/%s: %s", S3_BUCKET_NAME, key, exc)

    if not frames:
        log.error("All landing files failed to parse - aborting Bronze.")
        return False, []

    raw = pd.concat(frames, ignore_index=True)
    log.info("Loaded %d total records.", len(raw))
    log.info("  Columns found: %s", list(raw.columns))

    if len(raw) < MIN_RECORD_THRESHOLD:
        log.warning("Record count %d below minimum threshold %d - aborting Bronze.",
                    len(raw), MIN_RECORD_THRESHOLD)
        return False, []

    # Unnest Kafka envelope: the uploader wraps each message as
    # {"offset":..., "partition":..., "timestamp":..., "key":..., "value":{...sensor data...}}
    # so device_type and all sensor fields live inside "value".
    if "value" in raw.columns and isinstance(raw["value"].dropna().iloc[0] if not raw["value"].dropna().empty else None, dict):
        log.info("  Detected Kafka envelope - unnesting 'value' column.")
        value_df = pd.json_normalize(raw["value"])

        # Keep only envelope cols that do NOT clash with payload cols.
        # The envelope has its own 'timestamp' which duplicates the sensor
        # payload's 'timestamp' — PyArrow will refuse to write duplicate names.
        payload_cols  = set(value_df.columns)
        envelope_keep = [
            c for c in raw.columns
            if c != "value" and c not in payload_cols
        ]
        raw = pd.concat(
            [raw[envelope_keep].reset_index(drop=True),
             value_df.reset_index(drop=True)],
            axis=1
        )
        log.info("  After unnesting, columns: %s", list(raw.columns))

    # Add ETL metadata columns
    raw["etl_run_id"]         = run_id
    raw["etl_processed_time"] = datetime.now(timezone.utc).isoformat()
    raw["etl_batch_date"]     = batch_date

    # Route by device type — log unique values to help debug mismatches
    if "device_type" in raw.columns:
        log.info("  device_type values found: %s", raw["device_type"].unique().tolist())
        vitals_df   = raw[raw["device_type"] == "vital_monitor"].copy()
        movement_df = raw[raw["device_type"] == "imu_sensor"].copy()
    else:
        log.error("  'device_type' column not found after unnesting - cannot route records.")
        vitals_df   = pd.DataFrame()
        movement_df = pd.DataFrame()

    log.info("Routed  vitals=%d  movement=%d", len(vitals_df), len(movement_df))

    wrote_any = False
    if not vitals_df.empty:
        write_parquet_to_s3(s3, vitals_df,   BRONZE_VITALS,   batch_date, run_id, "vitals")
        wrote_any = True
    if not movement_df.empty:
        write_parquet_to_s3(s3, movement_df, BRONZE_MOVEMENT, batch_date, run_id, "movement")
        wrote_any = True

    if not wrote_any:
        log.error(
            "Records were read (%d) but none matched known device_type values. "
            "Check the device_type values logged above against 'vital_monitor' / 'imu_sensor'. "
            "Landing files will NOT be archived so you can inspect them.",
            len(raw)
        )
        return False, []

    return True, all_keys


# ---------------------------------------------------------------------------
# Silver layer
# ---------------------------------------------------------------------------

def process_silver(s3, run_id, batch_date):
    """Clean Bronze data and add derived features -> Silver."""
    log.info("=" * 60)
    log.info("SILVER LAYER - bronze -> silver  batch_date=%s", batch_date)
    log.info("=" * 60)

    # -- Vitals ---------------------------------------------------------------
    log.info("Processing vitals...")
    try:
        bronze = read_parquet_from_s3(s3, BRONZE_VITALS, batch_date)
        log.info("  Loaded %d records from bronze.", len(bronze))

        if bronze.empty:
            log.warning("  No vitals bronze data for batch_date=%s", batch_date)
        else:
            silver = bronze.copy()

            # Coerce numeric fields
            for col in ("heart_rate", "spo2"):
                if col not in silver.columns:
                    log.error("  Column '%s' missing from bronze vitals - check payload schema.", col)
                    silver[col] = pd.NA
                silver[col] = pd.to_numeric(silver[col], errors="coerce")

            log.info("  heart_rate range: min=%.1f max=%.1f  spo2 range: min=%.1f max=%.1f",
                     silver["heart_rate"].min(), silver["heart_rate"].max(),
                     silver["spo2"].min(),       silver["spo2"].max())

            # Apply quality filters
            silver = silver[
                silver["heart_rate"].notna() &
                silver["heart_rate"].between(30, 220) &
                silver["spo2"].notna() &
                silver["spo2"].between(90, 100)
            ].copy()

            ts_col = "timestamp" if "timestamp" in silver.columns else None
            silver["event_timestamp"] = pd.to_datetime(
                silver[ts_col] if ts_col else pd.NaT, errors="coerce", utc=True
            )

            log.info("  Cleaned %d / %d records.", len(silver), len(bronze))

            if len(silver) >= MIN_RECORD_THRESHOLD:
                write_parquet_to_s3(s3, silver, SILVER_VITALS, batch_date, run_id, "vitals")
            else:
                log.warning("  Vitals below minimum threshold - skipping Silver write.")

    except Exception as exc:
        log.error("Vitals silver failed: %s", exc)

    # -- Movement -------------------------------------------------------------
    log.info("Processing movement...")
    try:
        bronze = read_parquet_from_s3(s3, BRONZE_MOVEMENT, batch_date)
        log.info("  Loaded %d records from bronze.", len(bronze))

        if bronze.empty:
            log.warning("  No movement bronze data for batch_date=%s", batch_date)
        else:
            silver = bronze.copy()

            for axis in ("x", "y", "z"):
                if axis not in silver.columns:
                    log.error("  Column '%s' missing from bronze movement - check payload schema.", axis)
                    silver[axis] = pd.NA
                silver[axis] = pd.to_numeric(silver[axis], errors="coerce")

            silver = silver[
                silver["x"].notna() &
                silver["y"].notna() &
                silver["z"].notna()
            ].copy()

            ts_col = "timestamp" if "timestamp" in silver.columns else None
            silver["event_timestamp"] = pd.to_datetime(
                silver[ts_col] if ts_col else pd.NaT, errors="coerce", utc=True
            )
            # Signal Vector Magnitude - fall detection feature
            silver["svm"] = (
                silver["x"] ** 2 + silver["y"] ** 2 + silver["z"] ** 2
            ) ** 0.5

            log.info("  Cleaned %d / %d records. Added SVM feature.", len(silver), len(bronze))

            if len(silver) >= MIN_RECORD_THRESHOLD:
                write_parquet_to_s3(s3, silver, SILVER_MOVEMENT, batch_date, run_id, "movement")
            else:
                log.warning("  Movement below minimum threshold - skipping Silver write.")

    except Exception as exc:
        log.error("Movement silver failed: %s", exc)


# ---------------------------------------------------------------------------
# Gold layer - MEWS scoring helpers
# ---------------------------------------------------------------------------

def _mews_hr_score(avg_hr):
    """
    Modified Early Warning Score - heart rate component only.
    Full MEWS also needs: respiratory rate, systolic BP, temperature, AVPU.
    Add those fields to the pipeline when sensor data becomes available.
    """
    if avg_hr > 130:  return 3
    if avg_hr >= 111: return 2
    if avg_hr >= 101: return 1
    if avg_hr >= 51:  return 0
    if avg_hr >= 41:  return 1
    return 2  # <= 40


def _mews_spo2_score(avg_spo2):
    if avg_spo2 < 90: return 3
    if avg_spo2 < 93: return 2
    if avg_spo2 < 95: return 1
    return 0


def process_gold(s3, run_id, batch_date):
    """Aggregate Silver -> Gold using WINDOW_MINUTES-minute windows per patient."""
    log.info("=" * 60)
    log.info("GOLD LAYER - silver -> gold  batch_date=%s", batch_date)
    log.info("=" * 60)

    freq = f"{WINDOW_MINUTES}min"

    # -- Vitals / MEWS --------------------------------------------------------
    log.info("Aggregating vitals (MEWS)...")
    try:
        silver = read_parquet_from_s3(s3, SILVER_VITALS, batch_date)
        log.info("  Loaded %d silver records.", len(silver))

        if silver.empty:
            log.warning("  No vitals silver data - skipping Gold.")
        else:
            silver["event_timestamp"] = pd.to_datetime(
                silver["event_timestamp"], utc=True, errors="coerce"
            )
            silver = silver.dropna(subset=["event_timestamp", "patient_id"])
            silver["window_start"] = silver["event_timestamp"].dt.floor(freq)

            gold = (
                silver
                .groupby(["patient_id", "window_start"], as_index=False)
                .agg(
                    avg_hr   =("heart_rate", "mean"),
                    peak_hr  =("heart_rate", "max"),
                    avg_spo2 =("spo2",       "mean"),
                    peak_spo2=("spo2",       "max"),
                )
            )

            gold["mews_hr_score"]   = gold["avg_hr"].map(_mews_hr_score)
            gold["mews_spo2_score"] = gold["avg_spo2"].map(_mews_spo2_score)
            gold["mews_score"]      = gold["mews_hr_score"] + gold["mews_spo2_score"]
            gold["etl_batch_date"]  = batch_date

            log.info("  Generated %d aggregated records.", len(gold))
            log.info("\n%s",
                gold[["patient_id", "avg_hr", "peak_hr",
                       "mews_hr_score", "mews_spo2_score", "mews_score"]]
                .head(5).to_string(index=False)
            )

            write_parquet_to_s3(s3, gold, GOLD_VITALS, batch_date, run_id, "vitals")

    except Exception as exc:
        log.error("Vitals gold failed: %s", exc)

    # -- Movement / Fall detection --------------------------------------------
    log.info("Aggregating movement (fall detection)...")
    try:
        silver = read_parquet_from_s3(s3, SILVER_MOVEMENT, batch_date)
        log.info("  Loaded %d silver records.", len(silver))

        if silver.empty:
            log.warning("  No movement silver data - skipping Gold.")
        else:
            silver["event_timestamp"] = pd.to_datetime(
                silver["event_timestamp"], utc=True, errors="coerce"
            )
            silver = silver.dropna(subset=["event_timestamp", "patient_id"])
            silver["window_start"] = silver["event_timestamp"].dt.floor(freq)
            silver["is_fall"]      = (silver["svm"] > FALL_SVM_THRESHOLD).astype(int)

            gold = (
                silver
                .groupby(["patient_id", "window_start"], as_index=False)
                .agg(
                    avg_activity        =("svm",     "mean"),
                    peak_impact         =("svm",     "max"),
                    fall_events_detected=("is_fall", "sum"),
                )
            )
            gold["etl_batch_date"] = batch_date

            log.info("  Generated %d aggregated records.", len(gold))
            log.info("\n%s",
                gold[["patient_id", "avg_activity", "peak_impact", "fall_events_detected"]]
                .head(5).to_string(index=False)
            )

            write_parquet_to_s3(s3, gold, GOLD_MOVEMENT, batch_date, run_id, "movement")

    except Exception as exc:
        log.error("Movement gold failed: %s", exc)


# ---------------------------------------------------------------------------
# Pipeline orchestrator
# ---------------------------------------------------------------------------

def run_etl_pipeline(s3):
    run_id     = str(uuid.uuid4())[:8]
    batch_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    log.info("=" * 60)
    log.info("ETL PIPELINE START  [run_id=%s  batch_date=%s]", run_id, batch_date)
    log.info("Bucket: s3://%s", S3_BUCKET_NAME)
    log.info("=" * 60)

    t0 = datetime.now()

    success, ingested_keys = process_bronze(s3, run_id, batch_date)

    if not success:
        log.warning("No Bronze data - skipping Silver and Gold.")
        return

    log.info("Archiving %d landing file(s)...", len(ingested_keys))
    archive_landing_files(s3, ingested_keys)

    process_silver(s3, run_id, batch_date)
    process_gold(s3, run_id, batch_date)

    elapsed = (datetime.now() - t0).total_seconds()
    log.info("=" * 60)
    log.info("ETL PIPELINE COMPLETE  elapsed=%.1fs  [run_id=%s]", elapsed, run_id)
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# S3 connection test
# ---------------------------------------------------------------------------

def test_s3_connection():
    log.info("Testing S3 connection...")
    try:
        s3      = _make_s3_client()
        buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        log.info("Connected. %d bucket(s) visible.", len(buckets))

        if S3_BUCKET_NAME not in buckets:
            log.error("Bucket not found: %s", S3_BUCKET_NAME)
            return False

        log.info("Bucket confirmed: s3://%s", S3_BUCKET_NAME)

        probe_key = f"_probe/{uuid.uuid4()}.txt"
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=probe_key, Body=b"ok")
        s3.delete_object(Bucket=S3_BUCKET_NAME, Key=probe_key)
        log.info("Write permission confirmed.")
        return True

    except (BotoCoreError, ClientError) as exc:
        log.error("S3 connection failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ETL Pipeline - S3 Production (pandas + PyArrow)"
    )
    parser.add_argument("--once",     action="store_true",
                        help="Run ETL once and exit (default behaviour)")
    parser.add_argument("--schedule", action="store_true",
                        help="Run ETL every --interval seconds continuously")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_SEC,
                        help=f"Schedule interval in seconds (default {DEFAULT_INTERVAL_SEC})")
    parser.add_argument("--test",     action="store_true",
                        help="Test S3 credentials and exit")
    args = parser.parse_args()

    _require_env(
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "S3_BUCKET_NAME",
    )

    if args.test:
        sys.exit(0 if test_s3_connection() else 1)

    if not test_s3_connection():
        log.error("S3 check failed - aborting.")
        sys.exit(1)

    s3 = _make_s3_client()

    try:
        if args.schedule:
            log.info(
                "Scheduled mode: every %d seconds. Press Ctrl+C to stop.",
                args.interval,
            )
            run_count = 0
            while True:
                run_count += 1
                t0 = datetime.now()
                log.info("--- Scheduled run #%d ---", run_count)
                try:
                    run_etl_pipeline(s3)
                except Exception as exc:
                    log.error("Pipeline run #%d failed: %s", run_count, exc)

                sleep_for = max(0, args.interval - (datetime.now() - t0).total_seconds())
                log.info("Next run in %.1f minutes.", sleep_for / 60)
                time.sleep(sleep_for)
        else:
            run_etl_pipeline(s3)

    except KeyboardInterrupt:
        log.info("Stopped by user.")
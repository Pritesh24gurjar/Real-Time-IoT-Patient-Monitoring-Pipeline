"""
Kafka to S3 Stream Uploader — Production Version

Consumes data from Kafka topics and uploads batches to the S3 landing zone
every 10 minutes (or when the batch size limit is reached). This is the
ingestion layer only; the ETL pipeline picks up from landing/.

Landing zone layout written by this script:
    s3://<bucket>/landing/vitals/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/<file>.jsonl
    s3://<bucket>/landing/movement/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/<file>.jsonl

Usage:
    python scripts/kafka_to_s3_stream.py               # production (S3)
    python scripts/kafka_to_s3_stream.py --local       # local filesystem mock
    python scripts/kafka_to_s3_stream.py --test        # verify S3 credentials and exit
"""

import json
import logging
import os
import sys
import time
import threading
import uuid
from datetime import datetime
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

load_dotenv()

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN     = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION            = os.getenv("AWS_REGION", "us-west-2")
S3_BUCKET_NAME        = os.getenv("S3_BUCKET_NAME")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094").split(",")
VITALS_TOPIC            = os.getenv("VITALS_TOPIC",   "raw_vitals")
MOVEMENT_TOPIC          = os.getenv("MOVEMENT_TOPIC", "raw_movement")

UPLOAD_INTERVAL_SECONDS = int(os.getenv("UPLOAD_INTERVAL_SECONDS", "30"))   # 10 minutes
BATCH_SIZE              = int(os.getenv("BATCH_SIZE", "5000"))

# S3 landing prefix — must match the ETL pipeline's LANDING_PREFIX
LANDING_PREFIX = "landing"

# Local mock root (used only with --local)
LOCAL_STORAGE_PATH = Path(os.getenv(
    "LOCAL_STORAGE_PATH",
    r"C:\Users\prite\Documents\CWRU courses\data eng\DE project\data\s3_mock"
))

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

def _require_env(*names: str) -> None:
    """Raise ValueError if any required env var is missing."""
    missing = [n for n in names if not os.getenv(n)]
    if missing:
        raise ValueError(
            "Missing required environment variables:\n"
            + "\n".join(f"  - {n}" for n in missing)
        )


def _time_partition(ts: datetime) -> str:
    """Return Hive-style partition path for a given timestamp."""
    return (
        f"year={ts.year:04d}/"
        f"month={ts.month:02d}/"
        f"day={ts.day:02d}/"
        f"hour={ts.hour:02d}/"
        f"minute={ts.minute:02d}"
    )


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------

class KafkaToS3Stream:
    """Consumes Kafka topics and periodically uploads batches to S3."""

    def __init__(self, use_local: bool = False) -> None:
        self.use_local  = use_local
        self.run_id     = str(uuid.uuid4())[:8]
        self.running    = True

        log.info("=" * 60)
        log.info("Kafka to S3 Stream Uploader  [run_id=%s]", self.run_id)
        log.info("=" * 60)

        # ------------------------------------------------------------------ #
        # Storage backend
        # ------------------------------------------------------------------ #
        if use_local:
            LOCAL_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
            log.info("Mode: LOCAL  path=%s", LOCAL_STORAGE_PATH)
            self.s3_client = None
        else:
            _require_env(
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "S3_BUCKET_NAME",
            )
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                aws_session_token=AWS_SESSION_TOKEN,
                region_name=AWS_REGION,
            )
            log.info("Mode: PRODUCTION  bucket=s3://%s", S3_BUCKET_NAME)

        # ------------------------------------------------------------------ #
        # Kafka consumer
        # ------------------------------------------------------------------ #
        log.info("Connecting to Kafka  servers=%s", KAFKA_BOOTSTRAP_SERVERS)
        self.consumer = KafkaConsumer(
            VITALS_TOPIC,
            MOVEMENT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="kafka-to-s3-stream-group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            key_deserializer=lambda x: x.decode("utf-8") if x else None,
            consumer_timeout_ms=1000,
        )

        # ------------------------------------------------------------------ #
        # State
        # ------------------------------------------------------------------ #
        self._vitals_batch:   list = []
        self._movement_batch: list = []
        self._last_upload_at: datetime = datetime.now()
        self._lock = threading.Lock()

        self._stats = {
            "vitals_consumed":   0,
            "movement_consumed": 0,
            "vitals_uploaded":   0,
            "movement_uploaded": 0,
            "upload_failures":   0,
            "uploads_performed": 0,
        }

        log.info(
            "Config  interval=%ss  batch_size=%s  vitals_topic=%s  movement_topic=%s",
            UPLOAD_INTERVAL_SECONDS, BATCH_SIZE, VITALS_TOPIC, MOVEMENT_TOPIC,
        )
        log.info("=" * 60)

    # ---------------------------------------------------------------------- #
    # Path helpers
    # ---------------------------------------------------------------------- #

    def _s3_key(self, data_type: str, ts: datetime) -> str:
        """
        Build the S3 key for a batch file.

        Pattern:
            landing/<data_type>/year=YYYY/month=MM/day=DD/hour=HH/minute=MM/
                data_<HHMMSSffffff>_<run_id>.jsonl
        """
        filename = f"data_{ts.strftime('%H%M%S_%f')}_{self.run_id}.jsonl"
        return f"{LANDING_PREFIX}/{data_type}/{_time_partition(ts)}/{filename}"

    def _local_path(self, data_type: str, ts: datetime) -> Path:
        """Mirror of _s3_key but rooted at LOCAL_STORAGE_PATH."""
        filename = f"data_{ts.strftime('%H%M%S_%f')}_{self.run_id}.jsonl"
        return (
            LOCAL_STORAGE_PATH
            / LANDING_PREFIX
            / data_type
            / _time_partition(ts)
            / filename
        )

    # ---------------------------------------------------------------------- #
    # Upload logic
    # ---------------------------------------------------------------------- #

    def _upload_batch(self, batch: list, data_type: str) -> None:
        """
        Serialise *batch* to JSON Lines and write it to S3 (or local disk).
        Mutates self._stats in place.
        """
        if not batch:
            return

        ts           = datetime.now()
        jsonl_bytes  = ("\n".join(json.dumps(r) for r in batch)).encode("utf-8")
        record_count = len(batch)

        try:
            if self.use_local:
                dest = self._local_path(data_type, ts)
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(jsonl_bytes)
                log.info(
                    "Saved  type=%s  records=%d  path=%s",
                    data_type, record_count, dest,
                )
            else:
                key = self._s3_key(data_type, ts)
                self.s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=key,
                    Body=jsonl_bytes,
                    ContentType="application/x-jsonlines",
                    Metadata={
                        "record_count": str(record_count),
                        "data_type":    data_type,
                        "uploaded_at":  ts.isoformat(),
                        "run_id":       self.run_id,
                    },
                )
                log.info(
                    "Uploaded  type=%s  records=%d  key=s3://%s/%s",
                    data_type, record_count, S3_BUCKET_NAME, key,
                )

            self._stats[f"{data_type}_uploaded"] += record_count
            self._stats["uploads_performed"]     += 1

        except (BotoCoreError, ClientError, OSError) as exc:
            self._stats["upload_failures"] += 1
            log.error("Upload failed  type=%s  records=%d  error=%s", data_type, record_count, exc)
            # Re-raise so the caller can decide whether to retain the batch
            raise

    def _should_upload(self) -> bool:
        elapsed = (datetime.now() - self._last_upload_at).total_seconds()
        return (
            elapsed >= UPLOAD_INTERVAL_SECONDS
            or len(self._vitals_batch)   >= BATCH_SIZE
            or len(self._movement_batch) >= BATCH_SIZE
        )

    def _flush(self) -> None:
        """Upload both batches and reset them. Must be called under self._lock."""
        for data_type, batch_attr in (
            ("vitals",   "_vitals_batch"),
            ("movement", "_movement_batch"),
        ):
            batch = getattr(self, batch_attr)
            if not batch:
                continue
            try:
                self._upload_batch(batch, data_type)
                setattr(self, batch_attr, [])          # clear only on success
            except Exception:
                log.warning(
                    "Retaining %d %s records in memory after upload failure.",
                    len(batch), data_type,
                )

        self._last_upload_at = datetime.now()

    # ---------------------------------------------------------------------- #
    # Main loop
    # ---------------------------------------------------------------------- #

    def consume_and_upload(self) -> None:
        """Poll Kafka indefinitely, flushing to S3 on schedule."""
        log.info("Consumer started. Press Ctrl+C to stop.")

        try:
            while self.running:
                messages = self.consumer.poll(timeout_ms=1000)

                with self._lock:
                    for tp, records in messages.items():
                        for msg in records:
                            envelope = {
                                "offset":    msg.offset,
                                "partition": msg.partition,
                                "timestamp": msg.timestamp,
                                "key":       msg.key,
                                "value":     msg.value,
                            }
                            if tp.topic == VITALS_TOPIC:
                                self._vitals_batch.append(envelope)
                                self._stats["vitals_consumed"] += 1
                            elif tp.topic == MOVEMENT_TOPIC:
                                self._movement_batch.append(envelope)
                                self._stats["movement_consumed"] += 1

                    if self._should_upload():
                        self._flush()

                total = self._stats["vitals_consumed"] + self._stats["movement_consumed"]
                if total and total % 500 == 0:
                    log.info("Consumed %d messages so far.", total)

        except KeyboardInterrupt:
            log.info("Interrupted — uploading remaining data...")
            self.running = False
        finally:
            self._final_upload()

    def _final_upload(self) -> None:
        log.info("=" * 60)
        log.info("FINAL UPLOAD")
        log.info("=" * 60)
        with self._lock:
            self._flush()
        self._print_statistics()
        self.consumer.close()
        log.info("Uploader stopped.")

    def _print_statistics(self) -> None:
        s = self._stats
        log.info("=" * 60)
        log.info("STATISTICS  [run_id=%s]", self.run_id)
        log.info("  Vitals   consumed=%-6d  uploaded=%d", s["vitals_consumed"],   s["vitals_uploaded"])
        log.info("  Movement consumed=%-6d  uploaded=%d", s["movement_consumed"], s["movement_uploaded"])
        log.info("  Uploads performed=%d  failures=%d",   s["uploads_performed"], s["upload_failures"])
        log.info("=" * 60)


# ---------------------------------------------------------------------------
# S3 connectivity check
# ---------------------------------------------------------------------------

def test_s3_connection() -> bool:
    """Verify AWS credentials and bucket access; return True on success."""
    log.info("Testing S3 connection...")
    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
            region_name=AWS_REGION,
        )
        buckets = [b["Name"] for b in client.list_buckets().get("Buckets", [])]
        log.info("S3 connection successful. Found %d bucket(s).", len(buckets))

        if S3_BUCKET_NAME in buckets:
            log.info("Target bucket confirmed: s3://%s", S3_BUCKET_NAME)
        else:
            log.warning("Target bucket NOT found: %s", S3_BUCKET_NAME)
            return False

        # Smoke-test write permission
        probe_key = f"_probe/{uuid.uuid4()}.txt"
        client.put_object(Bucket=S3_BUCKET_NAME, Key=probe_key, Body=b"ok")
        client.delete_object(Bucket=S3_BUCKET_NAME, Key=probe_key)
        log.info("Write permission confirmed.")
        return True

    except (BotoCoreError, ClientError) as exc:
        log.error("S3 connection failed: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Kafka to S3 Stream Uploader")
    parser.add_argument("--local", action="store_true",
                        help="Save to local filesystem instead of S3")
    parser.add_argument("--test",  action="store_true",
                        help="Test S3 credentials and exit")
    args = parser.parse_args()

    if args.test:
        sys.exit(0 if test_s3_connection() else 1)

    try:
        uploader = KafkaToS3Stream(use_local=args.local)
        uploader.consume_and_upload()
    except ValueError as exc:
        log.error("Configuration error: %s", exc)
        sys.exit(1)
    except KafkaError as exc:
        log.error("Kafka error: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.exception("Unexpected error: %s", exc)
        sys.exit(1)
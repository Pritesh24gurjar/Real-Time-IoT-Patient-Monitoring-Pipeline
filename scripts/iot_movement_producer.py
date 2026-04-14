"""
IoT Movement Simulator - Kafka Producer

Reads the WISDM accelerometer dataset and streams it to Kafka
to simulate real-time 20Hz IMU movement data from wearable devices.

Usage:
    python scripts/iot_movement_producer.py
"""

import json
import time
import os
import random
from pathlib import Path
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094']
KAFKA_TOPIC = 'raw_movement'
PROJECT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.getenv("MOVEMENT_DATA_DIR", str(PROJECT_DIR / "data")))
CSV_FILE = Path(
    os.getenv(
        "MOVEMENT_CSV_FILE",
        str(DATA_DIR / "wisdm-dataset-organized" / "wisdm_all_raw.csv"),
    )
)
SYNTHETIC_ROWS = int(os.getenv("MOVEMENT_SYNTHETIC_ROWS", "10000"))


def create_producer():
    """Initialize Kafka Producer with optimal settings for high-throughput movement data."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8'),
        acks='all',  # Wait for all replicas to acknowledge
        retries=3,
        batch_size=16384,  # Larger batches for high-frequency data
        linger_ms=10,  # Wait up to 10ms to batch messages
    )


def generate_synthetic_movement(row_count: int = SYNTHETIC_ROWS):
    """Generate realistic accelerometer data when the WISDM CSV is unavailable."""
    import pandas as pd

    activity_rows = [
        ("A", "walking", (0.4, 0.6, 0.7)),
        ("B", "jogging", (1.2, 1.4, 1.5)),
        ("C", "stairs", (0.9, 1.0, 1.2)),
        ("D", "sitting", (0.05, 0.05, 0.02)),
        ("E", "standing", (0.08, 0.04, 0.03)),
        ("F", "typing", (0.2, 0.15, 0.1)),
        ("P", "dribbling", (1.5, 1.8, 1.3)),
    ]

    rows = []
    base_timestamp = int(time.time() * 1e9)

    for i in range(row_count):
        activity_code, activity_name, scale = random.choice(activity_rows)
        x = round(random.gauss(scale[0], 0.12), 4)
        y = round(random.gauss(scale[1], 0.12), 4)
        z = round(random.gauss(scale[2], 0.12), 4)
        svm = (x**2 + y**2 + z**2) ** 0.5

        rows.append({
            "subject_id": random.randint(1600, 1650),
            "activity_code": activity_code,
            "activity_name": activity_name,
            "timestamp": base_timestamp + i * 50_000_000,
            "x": x,
            "y": y,
            "z": z,
            "sensor_type": "accelerometer",
            "device_type": "imu_sensor",
            "svm": round(svm, 4),
        })

    return pd.DataFrame(rows)


def stream_movement(csv_filepath: Path, loop: bool = False, delay: float = 0.05, max_records: int = None):
    """
    Simulates 20Hz IMU Movement streaming (accelerometer data).
    
    Args:
        csv_filepath: Path to the WISDM raw CSV
        loop: If True, continuously loop through the data
        delay: Delay between readings in seconds (default 0.05 for 20Hz)
        max_records: Maximum number of records to send (None for all)
    """
    import pandas as pd

    if not csv_filepath.exists():
        raise FileNotFoundError(
            f"Movement CSV not found: {csv_filepath}\n"
            f"Set MOVEMENT_CSV_FILE or place wisdm_all_raw.csv under {DATA_DIR / 'wisdm-dataset-organized'}"
        )

    print(f"Loading movement data from {csv_filepath}...")
    print("This may take a moment for large files...")
    
    # Read in chunks if file is very large
    chunksize = 100000
    chunks = []
    for chunk in pd.read_csv(csv_filepath, chunksize=chunksize):
        chunks.append(chunk)
        if max_records and len(chunks) * chunksize >= max_records:
            break
    
    df = pd.concat(chunks, ignore_index=True)
    
    if max_records:
        df = df.head(max_records)
    
    print(f"Loaded {len(df)} accelerometer records")
    print(f"Columns: {list(df.columns)}")
    
    producer = create_producer()
    iteration = 0
    total_sent = 0
    
    try:
        while True:
            for _, row in df.iterrows():
                # Calculate Signal Vector Magnitude (SVM) for fall detection
                # Clean data - remove trailing semicolons from numeric values
                x = float(str(row['x']).rstrip(';'))
                y = float(str(row['y']).rstrip(';'))
                z = float(str(row['z']).rstrip(';'))
                svm = (x**2 + y**2 + z**2) ** 0.5
                
                payload = {
                    "patient_id": str(int(row['subject_id'])),
                    "device_type": "imu_sensor",
                    "sensor_type": "accelerometer",
                    "timestamp": float(row['timestamp']) / 1e9,  # Convert nanoseconds to seconds
                    "activity_code": row['activity_code'],
                    "activity_name": row['activity_name'],
                    "x": x,
                    "y": y,
                    "z": z,
                    "svm": round(svm, 4)  # Pre-calculate SVM for consumer efficiency
                }
                
                # Send to Kafka with patient_id as key (ensures partition affinity)
                future = producer.send(KAFKA_TOPIC, key=payload['patient_id'], value=payload)
                total_sent += 1
                
                # Log progress every 1000 messages
                if total_sent % 1000 == 0:
                    print(f"Sent {total_sent} records to {KAFKA_TOPIC} | SVM avg: {payload['svm']:.2f}")
                
                time.sleep(delay)
            
            iteration += 1
            print(f"\n=== Completed iteration {iteration} ({total_sent} total records) ===")
            
            if not loop:
                break
    
    except KeyboardInterrupt:
        print(f"\nInterrupted after sending {total_sent} records")
    finally:
        producer.flush()
        producer.close()
        print("Movement streaming complete")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="IoT Movement Simulator")
    parser.add_argument(
        "--loop", action="store_true",
        help="Continuously loop through the data"
    )
    parser.add_argument(
        "--delay", type=float, default=0.05,
        help="Delay between readings in seconds (default: 0.05 for 20Hz)"
    )
    parser.add_argument(
        "--max-records", type=int, default=None,
        help="Maximum number of records to send (default: all)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print sample messages without sending to Kafka"
    )
    
    args = parser.parse_args()
    
    if args.dry_run:
        import pandas as pd
        if CSV_FILE.exists():
            df = pd.read_csv(CSV_FILE, nrows=5)
        else:
            print(f"Movement CSV not found at {CSV_FILE}; generating synthetic data instead.")
            df = generate_synthetic_movement(row_count=5)
        print("DRY RUN - Sample payloads:")
        for _, row in df.iterrows():
            x, y, z = float(row['x']), float(row['y']), float(row['z'])
            svm = (x**2 + y**2 + z**2) ** 0.5
            payload = {
                "patient_id": str(int(row['subject_id'])),
                "device_type": "imu_sensor",
                "sensor_type": "accelerometer",
                "timestamp": float(row['timestamp']) / 1e9,
                "activity_code": row['activity_code'],
                "activity_name": row['activity_name'],
                "x": x,
                "y": y,
                "z": z,
                "svm": round(svm, 4)
            }
            print(json.dumps(payload, indent=2))
    else:
        try:
            if CSV_FILE.exists():
                stream_movement(
                    CSV_FILE,
                    loop=args.loop,
                    delay=args.delay,
                    max_records=args.max_records
                )
            else:
                print(f"Movement CSV not found at {CSV_FILE}; generating synthetic data instead.")
                df = generate_synthetic_movement()
                if args.max_records:
                    df = df.head(args.max_records)
                print(f"Loaded {len(df)} synthetic accelerometer records")

                producer = create_producer()
                iteration = 0
                total_sent = 0

                try:
                    while True:
                        for _, row in df.iterrows():
                            payload = {
                                "patient_id": str(int(row["subject_id"])),
                                "device_type": "imu_sensor",
                                "sensor_type": "accelerometer",
                                "timestamp": float(row["timestamp"]) / 1e9,
                                "activity_code": row["activity_code"],
                                "activity_name": row["activity_name"],
                                "x": float(row["x"]),
                                "y": float(row["y"]),
                                "z": float(row["z"]),
                                "svm": float(row["svm"]),
                            }

                            producer.send(KAFKA_TOPIC, key=payload["patient_id"], value=payload)
                            total_sent += 1

                            if total_sent % 1000 == 0:
                                print(f"Sent {total_sent} records to {KAFKA_TOPIC} | SVM avg: {payload['svm']:.2f}")

                            time.sleep(args.delay)

                        iteration += 1
                        print(f"\n=== Completed synthetic iteration {iteration} ({total_sent} total records) ===")
                        if not args.loop:
                            break
                finally:
                    producer.flush()
                    producer.close()
                    print("Movement streaming complete")
        except Exception as e:
            print(f"Error: {e}")
            print("Make sure Kafka is running. Use: docker-compose up -d")

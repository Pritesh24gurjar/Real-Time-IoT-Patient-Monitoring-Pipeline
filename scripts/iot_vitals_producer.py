"""
IoT Vitals Simulator - Kafka Producer

Reads the clean smartwatch health data CSV and streams it to Kafka
to simulate real-time 1Hz vital sign monitoring (heart rate, SpO2).

Usage:
    python scripts/iot_vitals_producer.py
"""

import json
import time
import random
from pathlib import Path
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094']
KAFKA_TOPIC = 'raw_vitals'
DATA_DIR = Path(r"C:\Users\prite\Documents\CWRU courses\data eng\DE project\data")
CSV_FILE = DATA_DIR / "clean_smartwatch_health_data.csv"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: str(k).encode('utf-8'),
    acks='all',  # Wait for all replicas to acknowledge
    retries=3,
)


def stream_vitals(csv_filepath: Path, loop: bool = False, delay: float = 1.0):
    """
    Simulates 1Hz Smartwatch Vitals streaming.
    
    Args:
        csv_filepath: Path to the clean smartwatch health CSV
        loop: If True, continuously loop through the data
        delay: Delay between readings in seconds (default 1.0 for 1Hz)
    """
    import pandas as pd
    
    print(f"Loading vitals data from {csv_filepath}...")
    df = pd.read_csv(csv_filepath)
    print(f"Loaded {len(df)} records")
    
    iteration = 0
    while True:
        for _, row in df.iterrows():
            # Map column names from cleaned CSV to payload format
            payload = {
                "patient_id": str(int(row['User ID'])),
                "device_type": "vital_monitor",
                "timestamp": time.time(),
                "heart_rate": int(row['Heart Rate (BPM)']) if pd.notnull(row['Heart Rate (BPM)']) else None,
                "spo2": int(row['Blood Oxygen Level (%)']) if pd.notnull(row['Blood Oxygen Level (%)']) else None,
                "step_count": int(row['Step Count']) if pd.notnull(row['Step Count']) else None,
                "sleep_duration": float(row['Sleep Duration (hours)']) if pd.notnull(row['Sleep Duration (hours)']) else None,
                "activity_level": row['Activity Level'],
                "stress_level": int(row['Stress Level']) if pd.notnull(row['Stress Level']) else None
            }
            
            # Send to Kafka with patient_id as key (ensures partition affinity)
            future = producer.send(KAFKA_TOPIC, key=payload['patient_id'], value=payload)
            
            # Log every 100 messages
            if _ % 100 == 0:
                print(f"Sent {iteration * len(df) + _} records to {KAFKA_TOPIC}")
            
            time.sleep(delay)
        
        iteration += 1
        print(f"\n=== Completed iteration {iteration} ===")
        
        if not loop:
            break
    
    producer.flush()
    print("Vitals streaming complete")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="IoT Vitals Simulator")
    parser.add_argument(
        "--loop", action="store_true",
        help="Continuously loop through the data"
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Delay between readings in seconds (default: 1.0)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print messages without sending to Kafka"
    )
    
    args = parser.parse_args()
    
    if args.dry_run:
        import pandas as pd
        df = pd.read_csv(CSV_FILE)
        print("DRY RUN - Sample payloads:")
        for i, row in df.head(5).iterrows():
            payload = {
                "patient_id": str(int(row['User ID'])),
                "device_type": "vital_monitor",
                "timestamp": time.time(),
                "heart_rate": int(row['Heart Rate (BPM)']),
                "spo2": int(row['Blood Oxygen Level (%)']),
                "step_count": int(row['Step Count']),
                "sleep_duration": float(row['Sleep Duration (hours)']),
                "activity_level": row['Activity Level'],
                "stress_level": int(row['Stress Level'])
            }
            print(json.dumps(payload, indent=2))
    else:
        try:
            stream_vitals(CSV_FILE, loop=args.loop, delay=args.delay)
        except KeyboardInterrupt:
            print("\nShutting down producer...")
        except Exception as e:
            print(f"Error: {e}")
            print("Make sure Kafka is running. Use: docker-compose up -d")

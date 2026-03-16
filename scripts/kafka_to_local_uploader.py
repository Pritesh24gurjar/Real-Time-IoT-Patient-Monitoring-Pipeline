"""
Kafka to Local Storage Uploader - Testing Mode

Simulates S3 uploads by saving data to local filesystem with the same
directory structure as S3. Use this for testing without AWS credentials.

Usage:
    python scripts/kafka_to_local_uploader.py
"""

import os
import json
import time
from datetime import datetime
from pathlib import Path
from kafka import KafkaConsumer
from collections import defaultdict
import threading

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094']
VITALS_TOPIC = 'raw_vitals'
MOVEMENT_TOPIC = 'raw_movement'

# Upload Configuration
UPLOAD_INTERVAL_SECONDS = 600  # 10 minutes
BATCH_SIZE = 1000  # Upload if batch reaches this size before interval

# Local storage path (simulates S3)
LOCAL_STORAGE_PATH = Path(r"C:\Users\prite\Documents\CWRU courses\data eng\DE project\data\s3_mock")


class KafkaToLocalUploader:
    """Consumes from Kafka and saves batched data to local storage."""
    
    def __init__(self):
        # Create local storage directory
        LOCAL_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            VITALS_TOPIC, MOVEMENT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='kafka-to-local-etl-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=1000,
        )
        
        # Batch storage
        self.vitals_batch = []
        self.movement_batch = []
        
        # Timing
        self.last_upload_time = datetime.now()
        self.running = True
        
        # Statistics
        self.stats = {
            'vitals_consumed': 0,
            'movement_consumed': 0,
            'vitals_uploaded': 0,
            'movement_uploaded': 0,
            'uploads_performed': 0
        }
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        print("=" * 60)
        print("Kafka to Local Storage Uploader - ETL Pipeline (Testing)")
        print("=" * 60)
        print(f"\nConfiguration:")
        print(f"  Kafka Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"  Vitals Topic: {VITALS_TOPIC}")
        print(f"  Movement Topic: {MOVEMENT_TOPIC}")
        print(f"  Local Storage: {LOCAL_STORAGE_PATH}")
        print(f"  Upload Interval: {UPLOAD_INTERVAL_SECONDS} seconds")
        print(f"  Batch Size: {BATCH_SIZE}")
        print("=" * 60)
    
    def generate_local_path(self, data_type: str, timestamp: datetime) -> Path:
        """
        Generate local path with time-based partitioning (mimics S3 structure).
        
        Format: {base}/raw/{data_type}/year=YYYY/month=MM/day=DD/hour=HH/data_HHMMSS.jsonl
        """
        partition = (
            f"year={timestamp.year:04d}/"
            f"month={timestamp.month:02d}/"
            f"day={timestamp.day:02d}/"
            f"hour={timestamp.hour:02d}"
        )
        
        filename = f"data_{timestamp.strftime('%H%M%S_%f')}.jsonl"
        
        directory = LOCAL_STORAGE_PATH / "raw" / data_type / partition
        directory.mkdir(parents=True, exist_ok=True)
        
        return directory / filename
    
    def upload_batch(self, batch: list, data_type: str):
        """Save a batch of messages to local storage."""
        if not batch:
            return None
        
        timestamp = datetime.now()
        file_path = self.generate_local_path(data_type, timestamp)
        
        # Convert batch to JSON Lines format (one JSON object per line)
        jsonl_content = '\n'.join(json.dumps(record) for record in batch)
        
        try:
            # Write to file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(jsonl_content)
            
            # Update statistics
            if data_type == 'vitals':
                self.stats['vitals_uploaded'] += len(batch)
            else:
                self.stats['movement_uploaded'] += len(batch)
            self.stats['uploads_performed'] += 1
            
            print(f"\n[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] "
                  f"✓ Saved {len(batch)} {data_type} records to {file_path}")
            
        except Exception as e:
            print(f"\n[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] "
                  f"✗ Save failed: {e}")
        
        # Clear the batch after upload
        return []
    
    def should_upload(self):
        """Check if it's time to upload based on interval or batch size."""
        time_elapsed = (datetime.now() - self.last_upload_time).total_seconds()
        
        # Upload if interval passed OR batch is large enough
        return (
            time_elapsed >= UPLOAD_INTERVAL_SECONDS or
            len(self.vitals_batch) >= BATCH_SIZE or
            len(self.movement_batch) >= BATCH_SIZE
        )
    
    def consume_and_upload(self):
        """Main loop: consume from Kafka and save to local storage."""
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
              f"Starting Kafka consumer...")
        print(f"Press Ctrl+C to stop and upload remaining data\n")
        
        # For testing: upload every 30 seconds instead of 10 minutes
        original_interval = UPLOAD_INTERVAL_SECONDS
        test_interval = 30  # seconds for testing
        
        try:
            while self.running:
                # Poll for messages
                messages = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, records in messages.items():
                    for message in records:
                        topic = topic_partition.topic
                        
                        with self.lock:
                            if topic == VITALS_TOPIC:
                                self.vitals_batch.append({
                                    'offset': message.offset,
                                    'partition': message.partition,
                                    'timestamp': message.timestamp,
                                    'key': message.key.decode('utf-8') if message.key else None,
                                    'value': message.value
                                })
                                self.stats['vitals_consumed'] += 1
                                
                            elif topic == MOVEMENT_TOPIC:
                                self.movement_batch.append({
                                    'offset': message.offset,
                                    'partition': message.partition,
                                    'timestamp': message.timestamp,
                                    'key': message.key.decode('utf-8') if message.key else None,
                                    'value': message.value
                                })
                                self.stats['movement_consumed'] += 1
                
                # Use shorter interval for testing
                time_elapsed = (datetime.now() - self.last_upload_time).total_seconds()
                if time_elapsed >= test_interval or len(self.vitals_batch) >= 10 or len(self.movement_batch) >= 10:
                    with self.lock:
                        # Upload vitals batch
                        if self.vitals_batch:
                            self.vitals_batch = self.upload_batch(
                                self.vitals_batch, 'vitals'
                            )
                        
                        # Upload movement batch
                        if self.movement_batch:
                            self.movement_batch = self.upload_batch(
                                self.movement_batch, 'movement'
                            )
                        
                        # Reset timer
                        self.last_upload_time = datetime.now()
                
                # Print progress every 50 messages
                total_consumed = self.stats['vitals_consumed'] + self.stats['movement_consumed']
                if total_consumed > 0 and total_consumed % 50 == 0:
                    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                          f"Consumed: {total_consumed} messages "
                          f"(Vitals: {self.stats['vitals_consumed']}, "
                          f"Movement: {self.stats['movement_consumed']})")
        
        except KeyboardInterrupt:
            print("\n\nStopping consumer...")
            self.running = False
        finally:
            # Final upload of remaining data
            self.final_upload()
    
    def final_upload(self):
        """Upload any remaining data before shutdown."""
        print("\n" + "=" * 60)
        print("Performing final upload of remaining data...")
        print("=" * 60)
        
        with self.lock:
            # Upload remaining vitals
            if self.vitals_batch:
                print(f"\nUploading {len(self.vitals_batch)} remaining vitals records...")
                self.vitals_batch = self.upload_batch(self.vitals_batch, 'vitals')
            
            # Upload remaining movement
            if self.movement_batch:
                print(f"\nUploading {len(self.movement_batch)} remaining movement records...")
                self.movement_batch = self.upload_batch(self.movement_batch, 'movement')
        
        # Print final statistics
        self.print_statistics()
        
        # Close consumer
        self.consumer.close()
        print("\nUploader stopped gracefully.")
    
    def print_statistics(self):
        """Print consumption and upload statistics."""
        print("\n" + "=" * 60)
        print("FINAL STATISTICS")
        print("=" * 60)
        print(f"  Vitals Records:")
        print(f"    Consumed:  {self.stats['vitals_consumed']:,}")
        print(f"    Saved:     {self.stats['vitals_uploaded']:,}")
        print(f"  Movement Records:")
        print(f"    Consumed:  {self.stats['movement_consumed']:,}")
        print(f"    Saved:     {self.stats['movement_uploaded']:,}")
        print(f"  Total Uploads: {self.stats['uploads_performed']}")
        print(f"\n  Data Location: {LOCAL_STORAGE_PATH}")
        print("=" * 60)


if __name__ == "__main__":
    print(f"\nLocal storage directory: {LOCAL_STORAGE_PATH}")
    print("This will simulate S3 uploads by saving to local files.\n")
    
    # Start the uploader
    uploader = KafkaToLocalUploader()
    uploader.consume_and_upload()

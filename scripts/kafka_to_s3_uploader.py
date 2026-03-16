"""
Kafka to S3 Stream Uploader - ETL Pipeline

Consumes data from Kafka topics (raw_vitals, raw_movement) and uploads
batched data to S3 every 10 minutes with timestamp-based partitioning.

Features:
- Batches messages by topic (vitals vs movement)
- Uploads every 10 minutes automatically
- Partitions data by year/month/day/hour in S3
- Saves as JSON Lines format for efficient processing
- Tracks offsets to avoid duplicate uploads

Usage:
    python scripts/kafka_to_s3_uploader.py
"""

import os
import json
import time
import boto3
from datetime import datetime, timedelta
from pathlib import Path
from kafka import KafkaConsumer
from dotenv import load_dotenv
from collections import defaultdict
import threading

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
AWS_REGION = os.getenv('AWS_REGION', 'us-west-2')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9094']
VITALS_TOPIC = 'raw_vitals'
MOVEMENT_TOPIC = 'raw_movement'

# Upload Configuration
UPLOAD_INTERVAL_SECONDS = 600  # 10 minutes
BATCH_SIZE = 1000  # Upload if batch reaches this size before interval

# Validate credentials
if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN]):
    raise ValueError(
        "Missing AWS credentials! Please set in .env file:\n"
        "  - AWS_ACCESS_KEY_ID\n"
        "  - AWS_SECRET_ACCESS_KEY\n"
        "  - AWS_SESSION_TOKEN"
    )

if not S3_BUCKET_NAME:
    raise ValueError("S3_BUCKET_NAME not configured in .env file")


class KafkaToS3Uploader:
    """Consumes from Kafka and uploads batched data to S3."""
    
    def __init__(self):
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
            region_name=AWS_REGION
        )
        
        # Initialize Kafka consumer
        self.consumer = KafkaConsumer(
            VITALS_TOPIC, MOVEMENT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='kafka-to-s3-etl-group',
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
        print("Kafka to S3 Uploader - ETL Pipeline")
        print("=" * 60)
        print(f"\nConfiguration:")
        print(f"  Kafka Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"  Vitals Topic: {VITALS_TOPIC}")
        print(f"  Movement Topic: {MOVEMENT_TOPIC}")
        print(f"  S3 Bucket: s3://{S3_BUCKET_NAME}")
        print(f"  Upload Interval: {UPLOAD_INTERVAL_SECONDS} seconds")
        print(f"  Batch Size: {BATCH_SIZE}")
        print("=" * 60)
    
    def generate_s3_key(self, data_type: str, timestamp: datetime) -> str:
        """
        Generate S3 key with time-based partitioning.
        
        Format: raw/{data_type}/year=YYYY/month=MM/day=DD/hour=HH/data_HHMMSS.jsonl
        """
        partition = (
            f"year={timestamp.year:04d}/"
            f"month={timestamp.month:02d}/"
            f"day={timestamp.day:02d}/"
            f"hour={timestamp.hour:02d}"
        )
        
        filename = f"data_{timestamp.strftime('%H%M%S_%f')}.jsonl"
        
        return f"raw/{data_type}/{partition}/{filename}"
    
    def upload_batch(self, batch: list, data_type: str):
        """Upload a batch of messages to S3."""
        if not batch:
            return
        
        timestamp = datetime.now()
        s3_key = self.generate_s3_key(data_type, timestamp)
        
        # Convert batch to JSON Lines format (one JSON object per line)
        jsonl_content = '\n'.join(json.dumps(record) for record in batch)
        
        try:
            # Upload to S3
            self.s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=jsonl_content.encode('utf-8'),
                ContentType='application/x-jsonlines',
                Metadata={
                    'record_count': str(len(batch)),
                    'data_type': data_type,
                    'uploaded_at': timestamp.isoformat()
                }
            )
            
            # Update statistics
            if data_type == 'vitals':
                self.stats['vitals_uploaded'] += len(batch)
            else:
                self.stats['movement_uploaded'] += len(batch)
            self.stats['uploads_performed'] += 1
            
            print(f"\n[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] "
                  f"✓ Uploaded {len(batch)} {data_type} records to s3://{S3_BUCKET_NAME}/{s3_key}")
            
        except Exception as e:
            print(f"\n[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] "
                  f"✗ Upload failed: {e}")
        
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
        """Main loop: consume from Kafka and upload to S3."""
        print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
              f"Starting Kafka consumer...")
        print(f"Press Ctrl+C to stop and upload remaining data\n")
        
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
                
                # Check if we should upload
                if self.should_upload():
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
                
                # Print progress every 100 messages
                total_consumed = self.stats['vitals_consumed'] + self.stats['movement_consumed']
                if total_consumed > 0 and total_consumed % 100 == 0:
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
        print(f"    Uploaded:  {self.stats['vitals_uploaded']:,}")
        print(f"  Movement Records:")
        print(f"    Consumed:  {self.stats['movement_consumed']:,}")
        print(f"    Uploaded:  {self.stats['movement_uploaded']:,}")
        print(f"  Total Uploads: {self.stats['uploads_performed']}")
        print("=" * 60)


def test_s3_connection():
    """Test S3 connection before starting."""
    print("\nTesting S3 connection...")
    
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
            region_name=AWS_REGION
        )
        
        # Test by listing buckets
        response = s3_client.list_buckets()
        print(f"✓ S3 connection successful!")
        print(f"\nAvailable buckets:")
        for bucket in response['Buckets'][:10]:  # Show first 10
            print(f"  - {bucket['Name']}")
        
        # Check if target bucket exists
        bucket_names = [b['Name'] for b in response['Buckets']]
        if S3_BUCKET_NAME in bucket_names:
            print(f"\n✓ Target bucket 's3://{S3_BUCKET_NAME}' exists!")
        else:
            print(f"\n⚠ Target bucket 's3://{S3_BUCKET_NAME}' not found.")
            print("  Please create the bucket or update S3_BUCKET_NAME in .env")
        
        return True
        
    except Exception as e:
        print(f"✗ S3 connection failed: {e}")
        return False


if __name__ == "__main__":
    # Test connection first
    if not test_s3_connection():
        print("\nExiting due to S3 connection failure.")
        print("Please check your AWS credentials in .env file.")
        exit(1)
    
    # Start the uploader
    uploader = KafkaToS3Uploader()
    uploader.consume_and_upload()

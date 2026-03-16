"""
S3 Uploader for Kafka Stream Data - ETL Pipeline

Uploads processed data from Kafka streams to S3 for batch processing.
Credentials are loaded from environment variables or .env file.

Usage:
    python scripts/s3_uploader.py
"""

import os
import json
import boto3
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
AWS_REGION = os.getenv('AWS_REGION', 'us-west-2')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# Validate credentials
if not all([AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN]):
    raise ValueError(
        "Missing AWS credentials! Please set:\n"
        "  - AWS_ACCESS_KEY_ID\n"
        "  - AWS_SECRET_ACCESS_KEY\n"
        "  - AWS_SESSION_TOKEN\n"
        "in your .env file or environment variables."
    )

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=AWS_REGION
)


def upload_to_s3(file_path: str, s3_key: str, bucket: str = None):
    """
    Upload a file to S3.
    
    Args:
        file_path: Local path to the file
        s3_key: S3 object key (path within bucket)
        bucket: S3 bucket name (defaults to S3_BUCKET_NAME from env)
    """
    bucket = bucket or S3_BUCKET_NAME
    
    if not bucket:
        raise ValueError("S3_BUCKET_NAME not configured")
    
    try:
        s3_client.upload_file(file_path, bucket, s3_key)
        print(f"✓ Uploaded {file_path} to s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        print(f"✗ Upload failed: {e}")
        return False


def upload_json_data(data: dict, s3_key: str, bucket: str = None):
    """
    Upload JSON data directly to S3 without creating a local file.
    
    Args:
        data: Python dictionary to upload as JSON
        s3_key: S3 object key
        bucket: S3 bucket name
    """
    bucket = bucket or S3_BUCKET_NAME
    
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )
        print(f"✓ Uploaded JSON to s3://{bucket}/{s3_key}")
        return True
    except Exception as e:
        print(f"✗ Upload failed: {e}")
        return False


def generate_s3_key(prefix: str, file_type: str, timestamp: datetime = None):
    """
    Generate an S3 key with partitioning by date.
    
    Args:
        prefix: Base prefix (e.g., 'raw/vitals', 'processed/alerts')
        file_type: File extension (e.g., 'json', 'csv', 'parquet')
        timestamp: Timestamp for partitioning (default: now)
    
    Returns:
        S3 key string like: raw/vitals/year=2026/month=03/day=14/data_123456.json
    """
    timestamp = timestamp or datetime.now()
    
    # Create partitioned path
    partition = f"year={timestamp.year:04d}/month={timestamp.month:02d}/day={timestamp.day:02d}"
    filename = f"data_{timestamp.strftime('%H%M%S_%f')}.{file_type}"
    
    return f"{prefix}/{partition}/{filename}"


def test_connection():
    """Test S3 connection and list buckets."""
    try:
        response = s3_client.list_buckets()
        print("✓ S3 connection successful!")
        print("\nAvailable buckets:")
        for bucket in response['Buckets']:
            print(f"  - {bucket['Name']} (created: {bucket['CreationDate']})")
        return True
    except Exception as e:
        print(f"✗ S3 connection failed: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("S3 Uploader - ETL Pipeline")
    print("=" * 60)
    
    # Test connection
    print("\nTesting S3 connection...")
    if test_connection():
        print("\n✓ Ready to upload data!")
        
        # Example: Upload sample data
        sample_data = {
            "test": "Hello from Kafka ETL pipeline!",
            "timestamp": datetime.now().isoformat(),
            "source": "smartwatch_health_data"
        }
        
        s3_key = generate_s3_key("test", "json")
        upload_json_data(sample_data, s3_key)
    else:
        print("\n✗ Please check your AWS credentials in .env file")

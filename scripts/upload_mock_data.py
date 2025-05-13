import boto3
import json
import uuid
from datetime import datetime, timezone
import os

# Config
BUCKET_NAME = "call-records-data-dev-hp"  # Your static bucket name
RECORD_COUNT = 10                         # How many mock call records to generate

# Initialize S3 client
s3 = boto3.client("s3", region_name="us-east-1")

def generate_mock_call():
    now = datetime.utcnow()
    return {
        "call_id": str(uuid.uuid4()),
        "businessUnit": "Support",
        "domain_id": "dom-001",
        "domain_name": "example.com",
        "agent": {
            "id": "agent-001",
            "name": "Alice Smith",
            "user_name": "asmith"
        },
        "customer": {
            "id": "cust-001",
            "name": "John Doe",
            "phone": "+1234567890"
        },
        "call_metadata": {
            "start_timestamp": now.isoformat(),
            "end_timestamp": now.isoformat(),
            "hold_time": "15",
            "length": "00:05:30",
            "duration": 330,
            "language": "en",
            "region": "NA"
        },
        "summary": "Customer reported a missed pickup.",
        "keywords": [],
        "sentiment": {
            "overall": "neutral",
            "confidence_score": 0.0
        },
        "timeStamp": now.isoformat()
    }

def upload_to_s3():
    now = datetime.now(timezone.utc)
    key_prefix = now.strftime("%Y/%m/%d")
    key = f"{key_prefix}/call_data.json"

    call_data = [generate_mock_call() for _ in range(RECORD_COUNT)]

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(call_data),
        ContentType='application/json'
    )
    print(f"✅ Uploaded {RECORD_COUNT} call records to s3://{BUCKET_NAME}/{key}")

if __name__ == "__main__":
    upload_to_s3()

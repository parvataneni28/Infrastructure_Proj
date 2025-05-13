import boto3
import json
import uuid
import random
from datetime import datetime, timedelta, timezone
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# --- Configuration ---
BUCKET_NAME = "call-records-data-dev-hp"
TOTAL_RECORDS = 100
DAYS_SPREAD = 5
RECORDS_PER_DAY = TOTAL_RECORDS // DAYS_SPREAD
REGION = "us-east-1"

# --- AWS Client ---
s3 = boto3.client("s3", region_name=REGION)

# --- Sample Values ---
business_units = ["Recycling", "Garbage", "Compost", "Customer Support", "Commercial Waste"]
domains = ["wasteco.com", "cleanworld.org", "enviroplus.net", "greenway.ca", "gfl.com"]
summaries = [
    "The customer called to report that their recycling bin wasn't emptied this morning and asked if a truck could return today.",
    "Caller complained that the garbage truck skipped their entire street again despite following the usual schedule.",
    "The customer wanted to confirm the next compost pickup date, as their bin is already full.",
    "Reported that their green bin is cracked and leaking, and requested a replacement to be delivered soon.",
    "Customer was concerned about a spike in their monthly bill and wanted clarification on the charges.",
    "They found broken glass scattered across the driveway after pickup and were worried it came from the collection process.",
    "Requested a special bulk pickup for two old sofas and a mattress, and asked about extra charges involved.",
    "Asked whether garbage collection is affected due to the upcoming holiday and requested a copy of the revised schedule.",
    "Business account called urgently to report that the commercial bin wasn’t picked up this morning, causing overflow issues.",
    "Customer reported that the truck blocked their driveway again and requested the driver be notified.",
    "They said the new collection time was not communicated and that bins were missed as a result.",
    "Complained about a persistent odor coming from the compost bin and asked for cleaning or replacement options.",
    "Wanted to know how to properly dispose of a broken microwave and a set of old batteries.",
    "Requested an additional recycling bin for their office building due to increased usage.",
    "Asked about the maximum weight limit allowed for green bins before they are considered overloaded.",
    "Reported that several neighbors on their street also didn’t get their trash picked up today.",
    "Said the bin was knocked over by the collection truck and waste was left all over the curb.",
    "Customer filed a noise complaint about collections happening very late at night.",
    "Called in to set up paperless billing and update their email address on file.",
    "Needed help verifying their service address to request a new bin and schedule changes."
]

def generate_call(timestamp):
    return {
        "call_id": str(uuid.uuid4()),
        "businessUnit": random.choice(business_units),
        "domain_id": str(uuid.uuid4()),
        "domain_name": random.choice(domains),
        "agent": {
            "id": str(uuid.uuid4()),
            "name": f"Agent {random.choice(['Alice', 'Bob', 'Carlos', 'Dana'])}",
            "user_name": f"agent_{random.randint(1000, 9999)}"
        },
        "customer": {
            "id": str(uuid.uuid4()),
            "name": f"Customer {random.randint(1000, 9999)}",
            "phone": f"+1{random.randint(1000000000, 9999999999)}"
        },
        "call_metadata": {
            "start_timestamp": timestamp.isoformat(),
            "end_timestamp": (timestamp + timedelta(minutes=5)).isoformat(),
            "hold_time": str(random.randint(10, 60)),
            "length": "00:05:00",
            "duration": random.randint(100, 600),
            "language": "en",
            "region": "NA"
        },
        "summary": random.choice(summaries),
        "keywords": [],
        "sentiment": {
            "overall": "neutral",
            "confidence_score": 0.0
        },
        "timeStamp": timestamp.isoformat()
    }

def upload_parquet(day_offset):
    now = datetime.now(timezone.utc) - timedelta(days=day_offset)
    prefix = now.strftime("%Y/%m/%d")
    key = f"{prefix}/call_data.parquet"

    records = []
    for _ in range(RECORDS_PER_DAY):
        call_time = now.replace(
            hour=random.randint(8, 18),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
        records.append(generate_call(call_time))

    # Normalize nested JSON fields for compatibility
    df = pd.json_normalize(records, sep='_')

    table = pa.Table.from_pandas(df)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression='snappy')

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream'
    )

    print(f"Uploaded Parquet to s3://{BUCKET_NAME}/{key}")

if __name__ == "__main__":
    for day in range(DAYS_SPREAD):
        upload_parquet(day)
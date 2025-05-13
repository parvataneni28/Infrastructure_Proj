import boto3
import json
import pandas as pd
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone

# --- Configuration ---
BUCKET_NAME = "call-records-data-dev-hp"
DATE_PATH = datetime.now(timezone.utc).strftime("%Y/%m/%d")  # matches the upload path
INPUT_KEY = f"{DATE_PATH}/call_data.json"
OUTPUT_KEY = f"{DATE_PATH}/enriched.parquet"

# --- AWS Clients ---
s3 = boto3.client("s3", region_name="us-east-1")
comprehend = boto3.client("comprehend", region_name="us-east-1")

# --- Load Data ---
response = s3.get_object(Bucket=BUCKET_NAME, Key=INPUT_KEY)
call_data = json.loads(response["Body"].read())

# --- Enrich Records ---
for record in call_data:
    summary = record.get("summary", "")
    if summary.strip():
        sentiment_result = comprehend.detect_sentiment(Text=summary, LanguageCode="en")
        keyphrases_result = comprehend.detect_key_phrases(Text=summary, LanguageCode="en")

        record["sentiment"] = {
            "overall": sentiment_result["Sentiment"].lower(),
            "confidence_score": sentiment_result["SentimentScore"][sentiment_result["Sentiment"].capitalize()]
        }

        record["keywords"] = [phrase["Text"] for phrase in keyphrases_result["KeyPhrases"]]

# --- Save as Parquet ---
df = pd.DataFrame(call_data)
table = pa.Table.from_pandas(df)
pq_buffer = BytesIO()
pq.write_table(table, pq_buffer)

s3.put_object(
    Bucket=BUCKET_NAME,
    Key=OUTPUT_KEY,
    Body=pq_buffer.getvalue(),
    ContentType='application/octet-stream'
)

print(f"✅ Enriched data written to s3://{BUCKET_NAME}/{OUTPUT_KEY}")

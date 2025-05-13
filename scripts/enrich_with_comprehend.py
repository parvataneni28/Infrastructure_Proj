import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
from datetime import datetime, timedelta, timezone

# --- Config ---
BUCKET_NAME = "call-records-data-dev-hp"
REGION = "us-east-1"
DAYS_SPREAD = 5  # how many past days to process

# --- AWS Clients ---
s3 = boto3.client("s3", region_name=REGION)
comprehend = boto3.client("comprehend", region_name=REGION)

def enrich_record(record):
    text = record.get("summary", "")
    if not text.strip():
        return record

    try:
        sentiment = comprehend.detect_sentiment(Text=text, LanguageCode="en")
        key_phrases = comprehend.detect_key_phrases(Text=text, LanguageCode="en")

        record["sentiment_overall"] = sentiment["Sentiment"].lower()
        record["sentiment_confidence"] = sentiment["SentimentScore"][sentiment["Sentiment"].capitalize()]
        record["keywords"] = [phrase["Text"] for phrase in key_phrases["KeyPhrases"]]
    except Exception as e:
        print(f" Error processing summary: {e}")

    return record

def process_day(offset):
    now = datetime.now(timezone.utc) - timedelta(days=offset)
    prefix = now.strftime("%Y/%m/%d")
    input_key = f"{prefix}/call_data.parquet"
    output_key = f"{prefix}/enriched.parquet"
    local_csv_path = f"enriched_{prefix.replace('/', '-')}.csv"

    print(f"Processing {input_key}...")

    # Download parquet file from S3
    response = s3.get_object(Bucket=BUCKET_NAME, Key=input_key)
    df = pd.read_parquet(BytesIO(response["Body"].read()))

    # Enrich each record
    records = df.to_dict(orient="records")
    enriched = [enrich_record(r) for r in records]
    enriched_df = pd.DataFrame(enriched)

    # Save to local CSV for verification
    enriched_df.to_csv(local_csv_path, index=False)
    print(f" Saved local CSV: {local_csv_path}")

    # Convert back to Parquet
    table = pa.Table.from_pandas(enriched_df)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression="snappy")

    # Upload enriched parquet to S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=output_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )

    print(f" Uploaded enriched file to s3://{BUCKET_NAME}/{output_key}")

if __name__ == "__main__":
    for day in range(DAYS_SPREAD):
        process_day(day)

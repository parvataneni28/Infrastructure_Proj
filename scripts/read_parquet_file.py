import boto3
import pandas as pd

# Setup
bucket = "call-records-data-dev-hp"
key = "2025/05/13/enriched.parquet"
local_file = "enriched_downloaded.parquet"

# Download from S3
s3 = boto3.client("s3", region_name="us-east-1")
with open(local_file, "wb") as f:
    s3.download_fileobj(bucket, key, f)

# Read locally
df = pd.read_parquet(local_file)
print(df.head())
df.to_csv("enriched_preview.csv", index=False)
print("✅ Exported to enriched_preview.csv")
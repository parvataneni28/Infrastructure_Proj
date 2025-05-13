
resource "aws_s3_bucket" "kafka_data" {
  bucket = "kafka-go-data-bucket-${random_id.bucket_id.hex}"  # globally unique
  force_destroy = true

  tags = {
    Name = "Kafka Go Data Bucket"
    Environment = "dev"
  }
}

resource "random_id" "bucket_id" {
  byte_length = 4
}

# S3 Bucket for Call Records

resource "aws_s3_bucket" "call_records" {
  bucket        = "call-records-data-dev-hp"  # fixed, must be globally unique
  force_destroy = true

  tags = {
    Name        = "Call Records Data Bucket"
    Environment = "dev"
    Project     = "AWSComprehendPOC"
  }
}

resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.call_records.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "sse" {
  bucket = aws_s3_bucket.call_records.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
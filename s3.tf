
resource "aws_s3_bucket" "kafka_data" {
  bucket = "kafka-go-data-bucket-${random_id.bucket_id.hex}"  # globally unique
  force_destroy = true

  tags = {
    Name        = "Kafka Go Data Bucket"
    Environment = "dev"
  }
}

resource "random_id" "bucket_id" {
  byte_length = 4
}

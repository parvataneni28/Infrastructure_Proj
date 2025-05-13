output "s3_bucket_name" {
  value = aws_s3_bucket.kafka_data.bucket
}


output "call_records_s3_bucket_name" {
  value = aws_s3_bucket.call_records.bucket
}
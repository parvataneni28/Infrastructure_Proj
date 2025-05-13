output "s3_bucket_name" {
  value = aws_s3_bucket.kafka_data.bucket
}


output "call_records_s3_bucket_name" {
  value = aws_s3_bucket.call_records.bucket
}

output "glue_database_name" {
  value = aws_glue_catalog_database.call_records.name
}

output "glue_crawler_name" {
  value = aws_glue_crawler.call_records_enriched.name
}

output "athena_workgroup_name" {
  value = aws_athena_workgroup.call_records_wg.name
}
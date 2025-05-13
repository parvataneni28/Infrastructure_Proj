resource "aws_glue_catalog_database" "call_records" {
  name = "call_records_db"
}

resource "aws_glue_crawler" "call_records_enriched" {
  name          = "call-records-enriched-crawler"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.call_records.name

  s3_target {
    path = "s3://call-records-data-dev-hp/"
  }

  table_prefix = "enriched_"

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  recrawl_policy {
    recrawl_behavior = "CRAWL_EVERYTHING"
  }

  schedule = null  # On-demand
}

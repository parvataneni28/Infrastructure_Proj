resource "aws_athena_workgroup" "call_records_wg" {
  name = "call-records-workgroup"

  configuration {
    enforce_workgroup_configuration = true
    result_configuration {
      output_location = "s3://call-records-data-dev-hp/athena-results/"
    }
  }
}

resource "aws_s3_bucket_object" "athena_results_placeholder" {
  bucket = "call-records-data-dev-hp"
  key    = "athena-results/"
  content = ""
}
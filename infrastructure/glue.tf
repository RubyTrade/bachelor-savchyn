data "archive_file" "glue_src_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../spark_jobs/src"
  output_path = "${path.module}/glue_src.zip"
}

resource "aws_s3_object" "glue_main_script" {
  bucket = aws_s3_bucket.kafka_sink.id
  key    = "glue/scripts/main.py"
  source = "${path.module}/../spark_jobs/src/main.py"
  etag   = filemd5("${path.module}/../spark_jobs/src/main.py")
}

resource "aws_s3_object" "glue_src_bundle" {
  bucket = aws_s3_bucket.kafka_sink.id
  key    = "glue/dependencies/glue_src.zip"
  source = data.archive_file.glue_src_zip.output_path
  etag   = data.archive_file.glue_src_zip.output_md5
}

resource "aws_s3_object" "glue_config" {
  bucket = aws_s3_bucket.kafka_sink.id
  key    = "glue/config/configs.yaml"
  source = "${path.module}/../spark_jobs/configs/configs.yaml"
  etag   = filemd5("${path.module}/../spark_jobs/configs/configs.yaml")
}

resource "aws_iam_role" "glue_job_role" {
  name = "${local.appname}-glue-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_policy" "glue_s3_policy" {
  name = "${local.appname}-glue-s3-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.kafka_sink.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.kafka_sink.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_s3_policy_attach" {
  role       = aws_iam_role.glue_job_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

resource "aws_glue_job" "lakehouse_pipeline" {
  name     = var.glue_job_name
  role_arn = aws_iam_role.glue_job_role.arn

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.kafka_sink.bucket}/${aws_s3_object.glue_main_script.key}"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  default_arguments = {
    "--job-language"                       = "python"
    "--enable-metrics"                     = "true"
    "--enable-continuous-cloudwatch-log"   = "true"
    "--extra-py-files"                     = "s3://${aws_s3_bucket.kafka_sink.bucket}/${aws_s3_object.glue_src_bundle.key}"
    "--config-path"                        = "s3://${aws_s3_bucket.kafka_sink.bucket}/${aws_s3_object.glue_config.key}"
    "--pipeline-root"                      = "s3://${aws_s3_bucket.kafka_sink.bucket}/lakehouse"
    "--TempDir"                            = "s3://${aws_s3_bucket.kafka_sink.bucket}/glue/temp/"
  }

  depends_on = [
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy_attachment.glue_s3_policy_attach,
    aws_s3_object.glue_main_script,
    aws_s3_object.glue_src_bundle,
    aws_s3_object.glue_config
  ]
}

# resource "aws_glue_trigger" "lakehouse_manual" {
#   name = "${var.glue_job_name}-manual"
#   type = "ON_DEMAND"

#   actions {
#     job_name = aws_glue_job.lakehouse_pipeline.name
#   }
# }

# resource "aws_glue_trigger" "lakehouse_daily" {
#   name              = "${var.glue_job_name}-daily"
#   type              = "SCHEDULED"
#   schedule          = var.glue_daily_schedule
#   start_on_creation = var.glue_enable_daily_trigger

#   actions {
#     job_name = aws_glue_job.lakehouse_pipeline.name
#   }
# }

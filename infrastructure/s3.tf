resource "random_id" "bucket_suffix" {
  byte_length = 4
}

resource "aws_s3_bucket" "kafka_sink" {
  bucket = "kafka-sink-${random_id.bucket_suffix.hex}"

  force_destroy = true # later change to false TO DO
}

resource "aws_s3_bucket_versioning" "kafka_sink" {
    bucket = aws_s3_bucket.kafka_sink.id

    versioning_configuration {
      status = "Enabled"
    }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "kafka_sink" {
    bucket = aws_s3_bucket.kafka_sink.id

    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
}

resource "aws_s3_bucket_public_access_block" "kafka_sink" {
    bucket = aws_s3_bucket.kafka_sink.id

    block_public_acls = true
    block_public_policy = true
    ignore_public_acls = true
    restrict_public_buckets = true
}

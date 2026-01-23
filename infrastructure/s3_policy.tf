resource "aws_s3_bucket_lifecycle_configuration" "kafka_sink" {
  bucket = aws_s3_bucket.kafka_sink.id

  rule {
    id = "cleanup-old-data"
    status = "Enabled"

    expiration {
      days = 30
    }
  }
}

resource "aws_iam_policy" "kafka_connect_s3_policy" {
  name = "kafka-connect-s3-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:AbortMultipartUpload",
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = [
          aws_s3_bucket.kafka_sink.arn,
          "${aws_s3_bucket.kafka_sink.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "kafka_connect_attach" {
  role       = aws_iam_role.kafka_connect_role.name // name or id
  policy_arn = aws_iam_policy.kafka_connect_s3_policy.arn
}

resource "aws_iam_instance_profile" "kafka_connect_profile" {
  name = "kafka-connect-profile"
  role = aws_iam_role.kafka_connect_role.name
}

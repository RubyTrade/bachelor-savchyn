resource "aws_iam_role" "grafana_role" {
  name = "${local.appname}-grafana-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "grafana_policy" {
  name = "${local.appname}-grafana-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [

      {
        Effect = "Allow"
        Action = [
          "athena:StartQueryExecution",
          "athena:GetQueryExecution",
          "athena:GetQueryResults",
          "athena:StopQueryExecution",
          "athena:GetWorkGroup",
          "athena:ListWorkGroups",
          "athena:ListDataCatalogs",
          "athena:GetDataCatalog",
          "athena:ListDatabases",
          "athena:ListTableMetadata"

        ]
        Resource = "*"
      },

      {
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartitions"
        ]
        Resource = "*"
      },

      {
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation",
          "s3:ListBucket",
          "s3:PutObject",
          "s3:GetObject",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.kafka_sink.arn,
          "${aws_s3_bucket.kafka_sink.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "grafana_attach" {
  role       = aws_iam_role.grafana_role.name
  policy_arn = aws_iam_policy.grafana_policy.arn
}

resource "aws_iam_instance_profile" "grafana_profile" {
  name = "${local.appname}-grafana-profile"
  role = aws_iam_role.grafana_role.name
}
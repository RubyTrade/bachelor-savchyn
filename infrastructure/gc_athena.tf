resource "aws_glue_catalog_database" "gold" {
  name = "gold"
}

resource "aws_athena_workgroup" "grafana" {
  name = "grafana-workgroup"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.kafka_sink.bucket}/athena/results/"
    }
  }
}

resource "aws_glue_catalog_table" "trades_table" {
  name          = "trades"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.kafka_sink.bucket}/lakehouse/gold/trades/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "order_id"
      type = "bigint"
    }

    columns {
      name = "symbol"
      type = "string"
    }

    columns {
      name = "side"
      type = "string"
    }

    columns {
      name = "original_quantity"
      type = "decimal(18,8)"
    }

    columns {
      name = "price"
      type = "decimal(18,8)"
    }

    columns {
      name = "realized_pnl"
      type = "decimal(18,8)"
    }

    columns {
      name = "trade_time"
      type = "timestamp"
    }

    columns {
      name = "order_status"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "daily_metrics_table" {
  name          = "daily_metrics"
  database_name = aws_glue_catalog_database.gold.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.kafka_sink.bucket}/lakehouse/gold/daily_metrics/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "day"
      type = "date"
    }

    columns {
      name = "total_pnl"
      type = "decimal(28,8)"
    }

    columns {
      name = "trades_count"
      type = "bigint"
    }

    columns {
      name = "win_rate"
      type = "double"
    }

    columns {
      name = "avg_win"
      type = "decimal(22,12)"
    }

    columns {
      name = "avg_loss"
      type = "decimal(22,12)"
    }
  }
}

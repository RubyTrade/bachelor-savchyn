from config_loader import get_path
from processing.bronze_layer import BronzeProcessor


def test_create_raw_table_extracts_event_fields(spark, tiny_pipeline_env):
    cfg = tiny_pipeline_env.config
    raw_path = get_path(cfg, "sample_data")
    bronze = BronzeProcessor(spark, cfg)
    raw = bronze.read_raw_data(raw_path)
    enriched = bronze.create_raw_table(raw)

    assert "raw_json" in enriched.columns
    assert "event_type" in enriched.columns
    assert "event_time" in enriched.columns
    assert "event_date" in enriched.columns
    assert enriched.filter(enriched.event_type == "ORDER_TRADE_UPDATE").count() >= 1


def test_save_to_parquet_writes_partitions(spark, tiny_pipeline_env, tmp_path):
    cfg = tiny_pipeline_env.config
    raw_path = get_path(cfg, "sample_data")
    out = str(tmp_path / "bronze_out")
    bronze = BronzeProcessor(spark, cfg)
    raw = bronze.read_raw_data(raw_path)
    enriched = bronze.create_raw_table(raw)
    bronze.save_to_parquet(enriched, out)

    df = spark.read.parquet(out)
    assert df.count() == enriched.count()

from config_loader import get_path
from processing.bronze_layer import BronzeProcessor
from processing.gold_layer import GoldProcessor
from processing.silver_layer import SilverProcessor


def test_bronze_silver_gold_smoke(spark, tiny_pipeline_env):
    cfg = tiny_pipeline_env.config
    root = tiny_pipeline_env.root

    bronze_path = str(root / "bronze_out")
    silver_orders = str(root / "silver_orders")
    silver_account = str(root / "silver_account")
    gold_metrics = str(root / "gold_metrics")
    gold_trades = str(root / "gold_trades")

    raw_path = get_path(cfg, "sample_data")

    bronze_p = BronzeProcessor(spark, cfg)
    raw = bronze_p.read_raw_data(raw_path)
    bronze_p.save_to_parquet(bronze_p.create_raw_table(raw), bronze_path)

    silver_p = SilverProcessor(spark, cfg)
    bronze_df = silver_p.read_bronze(bronze_path)
    orders_parsed = silver_p.parsing_orders_trades(bronze_df)
    account_parsed = silver_p.parsing_account_trades(bronze_df)
    orders_silver = silver_p.create_orders_silver(orders_parsed)
    account_silver = silver_p.create_account_trades_silver(account_parsed)

    import shutil

    for p in (silver_orders, silver_account):
        shutil.rmtree(p, ignore_errors=True)

    silver_p.save_quarantine(
        orders_silver,
        ["symbol", "side", "order_type", "event_time"],
        get_path(cfg, "quarantine", "base"),
        silver_orders,
    )
    silver_p.save_quarantine(
        account_silver,
        ["symbol", "position_side", "entry_price", "event_time"],
        get_path(cfg, "quarantine", "base"),
        silver_account,
    )

    gold_p = GoldProcessor(spark, cfg)
    o_df, a_df = gold_p.read_silver(silver_orders, silver_account)
    gold_orders = gold_p.create_orders_table(o_df)
    metrics = gold_p.create_daily_metrcis(gold_orders)

    assert gold_orders.count() == 2
    assert metrics.count() >= 1

    gold_p.save_to_gold_layer(metrics, gold_metrics)
    gold_p.save_to_gold_layer(gold_orders, gold_trades)
    assert spark.read.parquet(gold_trades).count() == 2

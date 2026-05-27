from pyspark.sql import SparkSession
from config_loader import load_config, get_path
from processing.bronze_layer import BronzeProcessor
from processing.silver_layer import SilverProcessor
from processing.gold_layer import GoldProcessor
from pathlib import Path
import sys
import time

CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "configs.yaml"


class LakehousePipeline:
    def __init__(self, config_path=None, pipeline_root=None):
        self.config = load_config(config_path or CONFIG_PATH)
        if pipeline_root:
            self.config["paths"]["root"] = pipeline_root
        self.session = self.create_session()
        self.bronze_layer = BronzeProcessor(self.session, self.config)
        self.silver_layer = SilverProcessor(self.session, self.config)
        self.gold_layer = GoldProcessor(self.session, self.config)
        self.silver_order_path = get_path(self.config, "silver", "orders")
        self.silver_account_path = get_path(self.config, "silver", "account")

    def create_session(self):
        print('Creating the spark session')

        spark_builder = SparkSession.builder\
            .appName(self.config["spark"]["app_name"])

        for key, value in self.config["spark"]["configs"].items():
            spark_builder = spark_builder.config(key, value)

        spark = spark_builder.getOrCreate()

        print("Spark Session was successfully created! ")

        return spark

    def run_bronze_layer(self):

        try:
            raw = self.bronze_layer.read_raw_data(
                get_path(self.config, "sample_data"))

            raw_table = self.bronze_layer.create_raw_table(raw)

            self.bronze_layer.save_to_parquet(
                raw_table, get_path(self.config, "bronze", "base"))
        except Exception as e:
            print(f"Failed run bronze layer due this error: {e}")
            raise e
        
    def run_silver_layer(self):
        try:
            bronze = self.silver_layer.read_bronze(get_path(self.config, "bronze", "base"))

            order_parsed_df = self.silver_layer.parsing_orders_trades(bronze)

            account_parsed_df = self.silver_layer.parsing_account_trades(bronze)

            order_silver = self.silver_layer.create_orders_silver(order_parsed_df)

            account_silver = self.silver_layer.create_account_trades_silver(account_parsed_df)

            self.silver_layer.save_quarantine(order_silver, 
                                                ['symbol', 'side', 'order_type', 'event_time'], 
                                                get_path(self.config, "quarantine", "base" ),
                                                self.silver_order_path)

            self.silver_layer.save_quarantine(account_silver,
                                                ['symbol', 'position_side', 'entry_price', 'event_time'],
                                                get_path(self.config, "quarantine", "base" ),
                                                self.silver_account_path)

        except Exception as e:
            print(f"Failed run silver layer due to this error: {e}")
            raise e

    def run_gold_layer(self):
        order_df, account_df = self.gold_layer.read_silver(self.silver_order_path, self.silver_account_path)

        order_table = self.gold_layer.create_orders_table(order_df)

        daily_metrics_table = self.gold_layer.create_daily_metrcis(order_table)

        self.gold_layer.save_to_gold_layer(daily_metrics_table, get_path(self.config, "gold", "daily_metrics"))
        self.gold_layer.save_to_gold_layer(order_table, get_path(self.config, "gold", "trades"))


            

    def run_pipeline(self):
        try:
            start = time.time()
            self.run_bronze_layer()
            self.run_silver_layer()
            self.run_gold_layer()
            end = time.time()
            print(f"Execution time:  {end - start:.2f}s")
        except Exception as e:
            print(f"failed to run layer error: {e}")
            raise
        finally:
            self.stop()

    def stop(self):
        self.session.stop()
        print("Spark session was stopped")


def main():
    runtime_args = _parse_runtime_args(sys.argv[1:])
    create_pipeline = LakehousePipeline(
        config_path=runtime_args.get("config-path"),
        pipeline_root=runtime_args.get("pipeline-root"),
    )
    create_pipeline.run_pipeline()


def _parse_runtime_args(arguments):
    parsed = {}
    index = 0

    while index < len(arguments):
        token = arguments[index]
        if not token.startswith("--"):
            index += 1
            continue

        key = token[2:]
        if "=" in key:
            key, value = key.split("=", 1)
            parsed[key] = value
            index += 1
            continue

        if index + 1 < len(arguments) and not arguments[index + 1].startswith("--"):
            parsed[key] = arguments[index + 1]
            index += 2
        else:
            parsed[key] = "true"
            index += 1

    return parsed


if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
from config_loader import load_config, get_path
from processing.bronze_layer import BronzeProcessor
from processing.silver_layer import SilverProcessor
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "configs.yaml"


class LakehousePipeline:
    def __init__(self):
        self.config = load_config(CONFIG_PATH)
        self.session = self.create_session()
        self.bronze_layer = BronzeProcessor(self.session, self.config)
        self.silver_layer = SilverProcessor(self.session, self.config)

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
                                                get_path(self.config, "silver", "orders"))
        
            self.silver_layer.save_quarantine(account_silver,
                                                ['symbol', 'position_side', 'entry_price', 'event_time'],
                                                get_path(self.config, "quarantine", "base" ),
                                                get_path(self.config, "silver", "accounts"))

        except Exception as e:
            print(f"Failed run silver layer due to this error: {e}")
            raise e

    def run_gold_layer(self):
        pass

            

    def run_pipeline(self):
        try:
            self.run_bronze_layer()
            self.run_silver_layer()
            # df = self.session.read.parquet(get_path(self.config, "silver", "orders"))
            # df.printSchema()
            # df.show()
        except Exception as e:
            print(f"failed to run layer error: {e}")
            raise
        finally:
            self.stop()

    def stop(self):
        self.session.stop()
        print("Spark session was stopped")


def main():
    create_pipeline = LakehousePipeline()
    create_pipeline.run_pipeline()


if __name__ == "__main__":
    main()

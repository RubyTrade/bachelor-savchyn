from pyspark.sql import SparkSession
from config_loader import load_config, get_path
from processing.bronze_layer import BronzeProcessor
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parents[1] / "configs" / "configs.yaml"

class LakehousePipeline:
    def __init__(self):
        self.config = load_config(CONFIG_PATH)
        self.session = self.create_session()
        self.bronze_layer = BronzeProcessor(self.session, self.config)
    
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
            raw = self.bronze_layer.read_raw_data(get_path(self.config, "sample_data"))

            raw_table = self.bronze_layer.create_raw_table(raw)

            self.bronze_layer.save_to_parquet(raw_table, get_path(self.config, "bronze", "base"))
        except Exception as e:
            print(f"Failed run bronze layer due this error: {e}")
            raise

    def run_pipeline(self):
        try:
            self.run_bronze_layer()
        except Exception as e:
            print(f"failed to run bronze layer error: {e}")
            raise
        finally:
            self.stop()


    def stop(self):
        self.session.stop()
        print("Spark session was stopped")


def main():
    create_pipline = LakehousePipeline()
    create_pipline.run_pipeline()


if __name__ == "__main__":
    main()
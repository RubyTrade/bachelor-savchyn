from typing import Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, get_json_object, from_json, to_timestamp


class BronzeProccesor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    def read_raw_data(self, path: str) -> DataFrame:
        """Function that read data from Json file 
        and return Dataframe"""
        try:
            df = (self.spark.read.text(path))
            return df
        except Exception as e:
            print(f'Failed to read data from {path}')
            raise 

    def create_raw_table(self, raw_df: DataFrame) -> DataFrame:
        bronze_df = (
            raw_df \
            .withColumnRenamed("value", "raw_json") \
            .withColumn("ingest_time", current_timestamp()) \
            .withColumn('event_type' , get_json_object("raw_json", "$.e"))
        )
        return bronze_df

    def routing_by_events(self, bronze_df: DataFrame) -> Dict[str, Any]:
        """
        Added new column that specifies a type of the 
        trade and separates them to diffrent dataframes
        """
        event_types = {}
        event_types["orders_raw"] = bronze_df.filter(col('event_type') == "ORDER_TRADE_UPDATE")
        event_types["accounts_raw"] = bronze_df.filter(col('event_type') == "ACCOUNT_UPDATE")

        return event_types
    
    def save_to_parquet(self, bronze_df: DataFrame, path: str):
        try:
            bronze_df.write \
                .mode('append') \
                .partitionBy("event_type") \
                .parquet(path)
        except Exception as e:
            print(f"Failed to save bronze table. Error: {e}")
            raise





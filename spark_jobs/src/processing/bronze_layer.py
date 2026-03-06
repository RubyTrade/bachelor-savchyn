from pyspark.sql import  DataFrame
from pyspark.sql.functions import col, current_timestamp, get_json_object, to_timestamp, to_date


class BronzeProcessor:
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
            print(f'Failed to read data from {path} error: {e}')
            raise 

    def create_raw_table(self, raw_df: DataFrame) -> DataFrame:
        bronze_df = (
            raw_df \
            .withColumnRenamed("value", "raw_json") \
            .withColumn("ingest_time", current_timestamp()) \
            .withColumn("event_type" , get_json_object("raw_json", "$.e")) \
            .withColumn("event_time", to_timestamp(get_json_object("raw_json", "$.T") / 1000).cast('timestamp')) \
            .withColumn("event_date", to_date(col("event_time")))
        )

        return bronze_df

    def save_to_parquet(self, bronze_df: DataFrame, path: str):
        try:
            bronze_df.write \
                .mode('overwrite') \
                .partitionBy("event_type", "event_date") \
                .parquet(path)
        except Exception as e:
            print(f"Failed to save bronze table. Error: {e}")
            raise

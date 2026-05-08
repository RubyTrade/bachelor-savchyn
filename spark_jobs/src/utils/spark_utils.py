from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, to_date, to_timestamp
from functools import reduce


class Sparktransformations:
    @staticmethod
    def filter_not_null(df: DataFrame, columns: list) -> DataFrame:
        condition = reduce(
            lambda acc, c: acc & col(c).isNotNull(),
            columns[1:],
            col(columns[0]).isNotNull()
            )
        return df.filter(condition)
    
    @staticmethod
    def filter_null(df: DataFrame, columns: list) -> DataFrame:
        condition = reduce(
            lambda acc, c: acc | col(c).isNull(),
            columns[1:],
            col(columns[0]).isNull()
            )
        return df.filter(condition)

    @staticmethod
    def cast_to_timestamp(df: DataFrame, columns: list) -> DataFrame:
        for column in columns:
            if column in df.columns:
                df = df.withColumn( column, to_timestamp(col(column) / 1000))
            else:
                print(f"Warning: Column '{column}' not found in DataFrame for timestamp conversion")
        return df
    
    @staticmethod
    def add_partition_columns(df: DataFrame, columns: list) -> DataFrame:
        for column in columns:
            if column in df.columns:
                df = df.withColumn(column, to_date(col(column)))
            else:
                print(f"Warning: Column '{column}' not found in DataFrame for date conversion")
        return df
    
    @staticmethod
    def add_metadata_columns(df: DataFrame, schema_version: str) -> DataFrame:
        df = df\
            .withColumn(
            "processing_time",
            current_timestamp()
    ) \
    .withColumn('schema_version', lit(schema_version)) 
        return df
    
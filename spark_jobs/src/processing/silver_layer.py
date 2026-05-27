from utils.spark_utils import Sparktransformations
from schemas.binance_schemas import BinanceSchemas
from pyspark.sql.functions import col, from_json, explode_outer
from pyspark.sql import DataFrame

class SilverProcessor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.schema_version = self.config["spark"]["schema_version"]

    def read_bronze(self, path: str) -> DataFrame:
        print('Satarting Silver layer processing')

        df = self.spark.read.parquet(path)

        return df

    def parsing_orders_trades(self, df: DataFrame) -> DataFrame:
        print('Parsing order trades')
        
        schema = BinanceSchemas.order_schema_structure_v1()

        orders_df = df.filter(col('event_type') == 'ORDER_TRADE_UPDATE')

        ordera_parsed = orders_df.withColumn(
            'data',
            from_json(col('raw_json'), schema)
        )

        return ordera_parsed
    
    def parsing_account_trades(self, df: DataFrame) -> DataFrame:
        print('Parsing account trades')
        
        schema = BinanceSchemas.account_schema_structure_v1()

        account_df = df.filter(col('event_type') == 'ACCOUNT_UPDATE')

        account_parsed = account_df.withColumn(
            'data',
            from_json(col('raw_json'), schema)
        )
        return account_parsed
    

    def create_orders_silver(self, orders_parsed: DataFrame) -> DataFrame:
        print('Creating orders silver table')

        d = col("data")
        o = d["o"]

        silver_df = orders_parsed.select(
            # event metadata
            d["e"].alias("event_type"),
            d["T"].alias("event_time"),
            d["E"].alias("event_received_time"),

            # order core
            o["s"].alias("symbol"),
            o["c"].alias("client_order_id"),
            o["S"].alias("side"),
            o["o"].alias("order_type"),
            o["f"].alias("time_in_force"),

            # prices & quantities
            o["q"].alias("original_quantity"),
            o["p"].alias("price"),
            o["ap"].alias("avg_price"),
            o["sp"].alias("stop_price"),

            # execution state
            o["x"].alias("execution_type"),
            o["X"].alias("order_status"),

            # identifiers
            o["i"].alias("order_id"),

            # fills
            o["l"].alias("last_fill_qty"),
            o["z"].alias("cum_fill_qty"),
            o["L"].alias("last_fill_price"),

            # commission
            o["n"].alias("commission"),
            o["N"].alias("commission_asset"),

            # trade info
            o["T"].alias("trade_time"),
            o["t"].alias("trade_id"),

            # notionals
            o["b"].alias("bid_notional"),
            o["a"].alias("ask_notional"),

            # flags
            o["m"].alias("is_maker"),
            o["R"].alias("is_reduce_only"),

            # order config
            o["wt"].alias("working_type"),
            o["ot"].alias("original_order_type"),
            o["ps"].alias("position_side"),

            # pnl / risk
            o["cp"].alias("close_position"),
            o["rp"].alias("realized_pnl"),
            o["pP"].alias("price_protect"),

            # misc
            o["V"].alias("stp_mode"),
            o["pm"].alias("price_match_mode"),
            o["gtd"].alias("good_till_date"),
            o["er"].alias("event_reason"),

            # ingestion metadata
            col("ingest_time"),
            col('raw_json')
        )

        silver_df = Sparktransformations.cast_to_timestamp(silver_df, ['event_time', 'trade_time', 'event_received_time'])
        silver_df = Sparktransformations.add_partition_columns(silver_df, ['event_time'])
        silver_df = Sparktransformations.add_metadata_columns(silver_df, self.schema_version)
        return silver_df
    
    def create_account_trades_silver(self, account_trades_parsed: DataFrame) -> DataFrame:
        print('Creating account trades silver table')
        accounts_flat = (
            account_trades_parsed
            .withColumn("balance", explode_outer(col("data.a.B")))
            .withColumn("position", explode_outer(col("data.a.P")))
        )

        
        silver_df = accounts_flat.select(
            # event metadata
            col("data.e").alias("event_type"),
            col("data.T").alias("event_time"),
            col("data.E").alias("event_received_time"),

            # reason
            col("data.a.m").alias("event_reason"),

            # balance
            col("balance.a").alias("asset"),
            col("balance.wb").alias("wallet_balance"),
            col("balance.cw").alias("cross_wallet_balance"),
            col("balance.bc").alias("balance_change"),

            # position
            col("position.s").alias("symbol"),
            col("position.pa").alias("position_amount"),
            col("position.ep").alias("entry_price"),
            col("position.cr").alias("realized_pnl"),
            col("position.up").alias("unrealized_pnl"),
            col("position.ps").alias("position_side"),

            col("ingest_time"),
            col("raw_json")
        )

        silver_df = Sparktransformations.cast_to_timestamp(silver_df, ['event_time', 'event_received_time'])
        silver_df = Sparktransformations.add_partition_columns(silver_df, ['event_time'])
        silver_df = Sparktransformations.add_metadata_columns(silver_df, self.schema_version)
        return silver_df
    
    def save_quarantine(self, silver_df: DataFrame, columns: list, quarantine_path: str, silver_path: str):
        clean_df = Sparktransformations.filter_not_null(silver_df, columns)
        quarantine_df = Sparktransformations.filter_null(silver_df, columns)

        quarantine_count = quarantine_df.limit(1).count()

        # Write the clean data
        clean_df.write \
                .mode('append') \
                .partitionBy("event_time") \
                .parquet(silver_path)

        if quarantine_count > 0:
            print(f'Quarantine {quarantine_count} records with null')
            try:
                quarantine_df.write \
                            .mode('append') \
                            .parquet(quarantine_path)
            except Exception as e:
                print(f'Error in saving Dataframe to parquet: {e}')
                raise

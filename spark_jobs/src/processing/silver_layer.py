from utils.spark_utils import Sparktransformations
from schemas.binance_schemas import BinanceSchemas
from pyspark.sql.functions import col, from_json, explode_outer
from pyspark.sql import DataFrame
from config_loader import load_config, get_path
from functools import reduce
from operator import or_, and_

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
        
        silver_df = orders_parsed.select(
            # event metadata
            col("data.e").alias("event_type"),
            col("data.T").alias("event_time"),
            col("data.E").alias("event_received_time"),

            # order core
            col("data.o.s").alias("symbol"),
            col("data.o.c").alias("client_order_id"),
            col("data.o.S").alias("side"),
            col("data.o.o").alias("order_type"),
            col("data.o.f").alias("time_in_force"),

            # prices & quantities
            col("data.o.q").alias("original_quantity"),
            col("data.o.p").alias("price"),
            col("data.o.ap").alias("avg_price"),
            col("data.o.sp").alias("stop_price"),

            # execution state
            col("data.o.x").alias("execution_type"),
            col("data.o.X").alias("order_status"),

            # identifiers
            col("data.o.i").alias("order_id"),

            # fills
            col("data.o.l").alias("last_fill_qty"),
            col("data.o.z").alias("cum_fill_qty"),
            col("data.o.L").alias("last_fill_price"),

            # commission
            col("data.o.n").alias("commission"),
            col("data.o.N").alias("commission_asset"),

            # trade info
            col("data.o.T").alias("trade_time"),
            col("data.o.t").alias("trade_id"),

            # notionals
            col("data.o.b").alias("bid_notional"),
            col("data.o.a").alias("ask_notional"),

            # flags
            col("data.o.m").alias("is_maker"),
            col("data.o.R").alias("is_reduce_only"),

            # order config
            col("data.o.wt").alias("working_type"),
            col("data.o.ot").alias("original_order_type"),
            col("data.o.ps").alias("position_side"),

            # pnl / risk
            col("data.o.cp").alias("close_position"),
            col("data.o.rp").alias("realized_pnl"),
            col("data.o.pP").alias("price_protect"),

            # misc
            col("data.o.V").alias("stp_mode"),
            col("data.o.pm").alias("price_match_mode"),
            col("data.o.gtd").alias("good_till_date"),
            col("data.o.er").alias("event_reason"),

            # ingestion metadata
            col("ingest_time"),
            col('raw_json')
        )

        # silver_df = Sparktransformations.filter_not_null(silver_df, ['symbol', 'side', 'order_type', 'event_time'])
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
            col("position.up").alias("unrealized_pnl"),
            col("position.ps").alias("position_side"),

            col("ingest_time"),
            col("raw_json")
        )

        # silver_df = Sparktransformations.filter_not_null(silver_df, ['symbol', 'position_side', 'quantity', 'price', 'event_time'])
        silver_df = Sparktransformations.cast_to_timestamp(silver_df, ['event_time', 'event_received_time'])
        silver_df = Sparktransformations.add_partition_columns(silver_df, ['event_time'])
        silver_df = Sparktransformations.add_metadata_columns(silver_df, self.schema_version)
        return silver_df
    
    def save_quarantine(self, silver_df: DataFrame, columns: list, quarantine_path: str, silver_path: str):
        null_condition = reduce(or_, [col(c).isNull() for c in columns])
        not_null_condition = reduce(and_, [col(c).isNotNull() for c in columns])

        clean_df = silver_df.filter(not_null_condition)
        quarantine_df = silver_df.filter(null_condition)

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

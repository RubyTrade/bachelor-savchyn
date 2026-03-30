from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as func
from functools import reduce
from pyspark.sql.functions import col, current_timestamp, get_json_object, from_json, to_timestamp
from pyspark.sql.types import *
from spark_jobs.src.schemas.binance_schemas import BinanceSchemas

SCHEMA_VERSION = 1
config = {"spark.sql.caseSensitive", "true"}
spark = SparkSession.builder.appName('Broze to Silver') \
                        .config(map=config) \
                        .getOrCreate()


raw = (
    spark.read.text("file:///SparkCourse/data/sample_data.jsonl")
)


bronze = (
    raw  \
    .withColumnRenamed("value", "raw_json") \
    .withColumn("ingest_time", current_timestamp()) \
    .withColumn('event_type' , get_json_object("raw_json", "$.e"))
)


## Routing
# Added new column that specifies a type of the trade or account data
orders_raw = bronze.filter(col("event_type") == "ORDER_TRADE_UPDATE")
accounts_raw = bronze.filter(col('event_type') == "ACCOUNT_UPDATE")


order_schema = BinanceSchemas.order_schema_structure_v1()
account_schema = BinanceSchemas.account_schema_structure_v1()

# Silver
## Parsing
# Craeting a separate dataframe for diffrent type of trade data
orders_parsed = orders_raw.withColumn(
    'data', 
    from_json(col('raw_json'), order_schema)
    )
accounts_parsed = accounts_raw.withColumn(
    'data',
    from_json(col('raw_json'), account_schema)
)
# +--------------------+--------------------+------------------+--------------------+
# |            raw_json|         ingest_time|        event_type|                data|
# +--------------------+--------------------+------------------+--------------------+
# |{"e":"ORDER_TRADE...|2026-02-25 12:51:...|ORDER_TRADE_UPDATE|{ORDER_TRADE_UPDA...|
# |{"e":"ORDER_TRADE...|2026-02-25 12:51:...|ORDER_TRADE_UPDATE|{ORDER_TRADE_UPDA...|


## SILVER: ORDER TRADE TABLE


orders_silver = orders_parsed.select(

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
    col("data.o.si").alias("si"),
    col("data.o.ss").alias("ss"),
    col("data.o.V").alias("v"),
    col("data.o.pm").alias("price_match_mode"),
    col("data.o.gtd").alias("good_till_date"),
    col("data.o.er").alias("event_reason"),

    # ingestion metadata
    col("ingest_time"),
    col('raw_json')
)

accounts_flat = (
    accounts_parsed
    .withColumn("balance", func.explode_outer(col("data.a.B")))
    .withColumn("position", func.explode_outer(col("data.a.P")))
)

account_silver = accounts_flat.select(

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
    

## Filter the data to not have a null
def filter_not_null(df: DataFrame, columns: list) -> DataFrame:
    condition = reduce(
        lambda acc, c: acc & col(c).isNotNull(),
        columns[1:],
        col(columns[0]).isNotNull()
        )
    return df.filter(condition)

orders_silver = filter_not_null(orders_silver, ["event_time", "symbol", "order_id", "side"])
account_silver = filter_not_null(account_silver, ["event_time", "asset"])


def cast_to_timestamp(df: DataFrame, columns: list) -> DataFrame:
    for column in columns:
        df = df.withColumn( column, to_timestamp(col(column) / 1000))
    return df


def paritition_columns(df: DataFrame, columns: list) -> DataFrame:
    for column in columns:
        df = df.withColumn( column, func.to_date(col(column)))
    return df


orders_silver = cast_to_timestamp(orders_silver, ['event_time', 'event_received_time', 'trade_time'])
account_silver = cast_to_timestamp( account_silver, ['event_time', 'event_received_time',])

orders_silver = paritition_columns(orders_silver, ['event_time'])
account_silver = paritition_columns(account_silver, ['event_time'])


# Adding this table to have Error qusrantine for bad records
oredrs_spoiled = orders_silver.filter(col('symbol').isNull())
accounts_spoiled = account_silver.filter(col('symbol').isNull())


# adding column "processing_time" to know
# when the data created in silver
orders_silver = orders_silver\
    .withColumn(
        "processing_time",
        current_timestamp()
    ) \
    .withColumn('schema_version', func.lit(SCHEMA_VERSION)) 


account_silver = account_silver \
    .withColumn(
        "processing_time",
        current_timestamp()
    ) \
    .withColumn('schema_version', func.lit(SCHEMA_VERSION))


orders_silver.select('event_type', 'event_time', 'schema_version' ).show(10)
# accounts_parsed.show(10)

spark.stop()

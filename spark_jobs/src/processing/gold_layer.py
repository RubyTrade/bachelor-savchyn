from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, count, when, avg, to_date

class GoldProcessor:
    def __init__(self, spark, config):
        self.config = config
        self.spark = spark

    def read_silver(self, path_orders: str, path_account: str) -> tuple:
        print('Starting Golden layer processing')

        df_orders = self.spark.read.parquet(path_orders)

        df_account = self.spark.read.parquet(path_account)

        return df_orders, df_account
    
    def create_orders_table(self, df_orders: DataFrame) -> DataFrame:
        gold_orders_df = df_orders \
        .filter((df_orders.side == 'SELL') & (df_orders.order_status == 'FILLED')) \
        .select(
                'order_id', 'symbol', 'side', 'original_quantity',
                col('avg_price').alias('price'),
                'realized_pnl', 'trade_time', 'order_status'
                )
        return gold_orders_df
    
    def create_daily_metrcis(self, gold_orders_df: DataFrame):
        df = gold_orders_df.withColumn('day', to_date('trade_time'))

        gold_daily_metrics = (
            df \
            .groupBy('day') \
            
            .agg(
                sum('realized_pnl').alias('total_pnl'),
                count('*').alias("trades_count"),

                # win rate
                ( 
                    sum(when(col('realized_pnl') > 0, 1).otherwise(0))
                    / count('*')
                ).alias('win_rate'),

                # average win
                avg(
                    when(col('realized_pnl') > 0, col('realized_pnl'))
                ).alias('avg_win'),

                # average loss
                avg(
                    when(col('realized_pnl') < 0, col('realized_pnl'))
                ).alias('avg_loss')

                # TODO: max_drawdown and sharpe
            )  
        )
        return gold_daily_metrics
      
        # day   |  total_pnl | win_rate %  |   trades-count| avg_win | avg_loss | max_drawdown | sharpe |
        # 2026-4-05 |  1$   |  (кількість виграшних / скльи всього ставок )| 3 | avg(pnl where pnl > 0) | avg(pnl where pnl < 0) | wallet_balance + unrealized_pnl | 

    def save_to_gold_layer(self, gold_df: DataFrame, path: str):
        try:
            gold_df.write \
                .mode('overwrite') \
                .parquet(path)
            print(f"Saved data to {path} in Gold layer")
        except Exception as e:
            print(f"Failed to save gold table. Error: {e}")
            raise

"""
Binance Data Schema Modules
Containes all the schema definition for diffrent event types
"""

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, 
    DecimalType, BooleanType, IntegerType, ArrayType
)


class BinanceSchemas:
    """Define Schema for Binance"""

    @staticmethod
    def order_schema_structure_v1() -> StructType:
        order_schema = StructType([
        StructField("e", StringType(), True),          # event type
        StructField("T", LongType(), True),            # event time
        StructField("E", LongType(), True),            # event receive time
        
        StructField("o", StructType([
            StructField("s", StringType(), True),      # symbol
            StructField("c", StringType(), True),      # client order id
            StructField("S", StringType(), True),      # side (BUY, SELL)
            StructField("o", StringType(), True),      # order type
            StructField("f", StringType(), True),      # time in force
            
            StructField("q", DecimalType(18, 8), True),  # original quantity
            StructField("p", DecimalType(18, 8), True),  # price
            StructField("ap", DecimalType(18, 8), True), # average price
            StructField("sp", DecimalType(18, 8), True), # stop price
            
            StructField("x", StringType(), True),      # execution type
            StructField("X", StringType(), True),      # order status
            
            StructField("i", LongType(), True),        # order id
            
            StructField("l", DecimalType(18, 8), True), # last filled qty
            StructField("z", DecimalType(18, 8), True), # accumulated qty
            StructField("L", DecimalType(18, 8), True), # last filled price
            
            StructField("n", DecimalType(18, 8), True), # commission
            StructField("N", StringType(), True),      # commission asset
            
            StructField("T", LongType(), True),        # trade time
            StructField("t", LongType(), True),        # trade id
            
            StructField("b", DecimalType(18, 8), True), # bids notional
            StructField("a", DecimalType(18, 8), True), # asks notional
            
            StructField("m", BooleanType(), True),     # maker flag
            StructField("R", BooleanType(), True),     # reduce only
            
            StructField("wt", StringType(), True),     # working type
            StructField("ot", StringType(), True),     # original order type
            StructField("ps", StringType(), True),     # position side
            
            StructField("cp", BooleanType(), True),    # close position
            StructField("rp", DecimalType(18, 8), True), # realized pnl
            StructField("pP", BooleanType(), True),    # price protect
            
            StructField("si", IntegerType(), True),
            StructField("ss", IntegerType(), True),
            
            StructField("V", StringType(), True),
            StructField("pm", StringType(), True),
            StructField("gtd", LongType(), True),
            StructField("er", StringType(), True)
        ]), True)
    ])
        return order_schema
    
    @staticmethod
    def account_schema_structure_v1() -> StructType:
        account_schema = StructType([
            StructField("e", StringType()),
            StructField("T", LongType()),
            StructField("E", LongType()),
            StructField("a", StructType([
                StructField("m", StringType()),
                StructField("B", ArrayType(StructType([
                    StructField("a", StringType()),
                    StructField("wb", DecimalType(18, 8)),
                    StructField("cw", DecimalType(18, 8)),
                    StructField("bc", DecimalType(18, 8))
                ]))),
                StructField("P", ArrayType(StructType([
                    StructField("s", StringType()),
                    StructField("pa", DecimalType(18, 8)),
                    StructField("ep", DecimalType(18, 8)),
                    StructField("up", DecimalType(18, 8)),
                    StructField("ps", StringType())
                ])))
            ]))
        ])
        return account_schema

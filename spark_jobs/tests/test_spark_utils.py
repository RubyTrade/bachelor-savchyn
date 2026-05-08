from pyspark.sql import Row

from utils.spark_utils import Sparktransformations


def test_filter_not_null_and_null(spark):
    df = spark.createDataFrame(
        [
            Row(a=1, b=2),
            Row(a=None, b=2),
            Row(a=1, b=None),
        ]
    )
    not_null = Sparktransformations.filter_not_null(df, ["a", "b"])
    null_only = Sparktransformations.filter_null(df, ["a", "b"])
    assert not_null.count() == 1
    assert null_only.count() == 2

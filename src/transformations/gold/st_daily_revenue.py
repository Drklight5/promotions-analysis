from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(name="st_daily_revenue")
def st_daily_revenue():
    return (
        spark.readStream.table("st_orders_cleaned")
        .groupBy(F.to_date("order_date").alias("order_date"))
        .agg(F.sum("amount").alias("revenue"))
    )

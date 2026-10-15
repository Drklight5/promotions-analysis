from pyspark import pipelines as dp


# All your silver streaming tables
@dp.table(name="st_orders_cleaned")
def st_orders_cleaned():
    return spark.readStream.table("bronze_orders")

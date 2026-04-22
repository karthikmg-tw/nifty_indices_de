import dlt
import json
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA silver")

@dlt.table(
    name="stg_orders_agg",
    comment = "Table with total order value instead of the individual order values"
)
@dlt.expect_or_fail(
    "valid_order", 
    "order_id is not null"
)
def stg_orders_agg():
    df_orders_items = dlt.read("data_sentinals.bronze.raw_order_items")

    df_orders_items = df_orders_items.withColumn("order_item_cost", col("price") + col("freight_value"))

    df_order_items = df_orders_items.groupBy("order_id").agg(F.sum("order_item_cost").alias("total_price"))

    return df_order_items
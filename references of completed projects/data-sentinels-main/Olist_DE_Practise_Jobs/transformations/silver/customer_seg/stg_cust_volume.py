import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA silver")

@dlt.table(
    name="stg_cust_vol_agg",
    comment = "Contains the order volume at unique customer id"
)
@dlt.expect_or_fail(
    "valid_customer", 
    "customer_unique_id is not null"
)
def stg_cust_vol_agg():
    df_custs = dlt.read("data_sentinals.silver.stg_last_12months_orders")
    df_cust_vol = df_custs.groupBy("customer_unique_id").agg(count("order_id").alias("order_volume"))
    df_segmented = df_cust_vol.withColumn(
    "volume_segment",
    when(col("order_volume") >= 7, "High")
    .when((col("order_volume") >= 2) & (col("order_volume") <= 6), "Medium")
    .otherwise("Low")
    )

    return df_segmented
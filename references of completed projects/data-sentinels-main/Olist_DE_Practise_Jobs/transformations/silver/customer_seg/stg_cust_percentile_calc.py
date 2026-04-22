import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA silver")
@dlt.table(
    name = "stg_cust_percentile",
    comment = "Contains percentile value of the total purchase power of the customers"
)
def stg_cust_percentile():
    df_cust_data = dlt.read("data_sentinals.silver.stg_last_12months_orders")
    # grouping the total_price at customer_unique_id level
    df_total_price = df_cust_data.groupBy("customer_unique_id").agg(F.sum("total_price").alias("total_price_unique_cust"))

    window_spec = Window.orderBy(col("total_price_unique_cust"))
    df_ranked = df_total_price.withColumn(
        "spend_percentile", 
        percent_rank().over(window_spec)
    )
    df_ranked = df_ranked.withColumn("spend_percentile_100",F.col("spend_percentile")*100)

    df_segmented = df_ranked.withColumn("value_segment",
        when(col("spend_percentile") >= 0.85, "High")
        .when((col("spend_percentile") >= 0.40) & (col("spend_percentile") < 0.85 ), "Medium")
        .otherwise("Low"))

    return df_segmented
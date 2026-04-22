import dlt
from pyspark.sql.functions import *
from pyspark.sql import functions as F

spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA gold")

@dlt.table(
    name = "customer_segmentation",
    comment = "Final view of customer segmentation based on pre-defined rules"
)
@dlt.expect_or_fail(
    "valid_persona", 
    "customer_persona IN ('VIP', 'Core Customer', 'Casual Buyer')"
)
def customer_segmenation():
    df_stg_vol = dlt.read("data_sentinals.silver.stg_cust_vol_agg")
    df_stg_price = dlt.read("data_sentinals.silver.stg_cust_percentile")

    df_combined = df_stg_price.join(df_stg_vol, on = "customer_unique_id", how = "inner")

    df_final_segmentation = df_combined.withColumn(
    "customer_persona",
    
    # 1. THE HIGH TIER (VIPs & Big Spenders)
    when(
        lower(col("value_segment")) == "high", 
        "VIP"
    )
    
    # 2. THE MEDIUM TIER (Core Customers)
    .when(
        (lower(col("volume_segment")) == "medium") & (lower(col("value_segment")) == "medium") |
        ((lower(col("volume_segment")) == "high") & (lower(col("value_segment")) != "high")), 
        "Core Customer"
    )
    
    # 3. THE LOW TIER (Casual Buyers)
    .otherwise("Casual Buyer")
    )
    


    return df_final_segmentation 


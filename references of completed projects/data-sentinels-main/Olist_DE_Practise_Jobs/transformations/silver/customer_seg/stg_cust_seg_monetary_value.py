"""
Purpose: This file is to create a staging table to get customer records for the last 12 months.
Steps
1. Calculate the max order date from the available order information. Since we donot have latest data taking this approach
2. For the customer for last 12 months calculate the total purchase value.
3. Calcualate the percentile value based on the purchase value.
4. Create a staging table with the final column containing the high. medium, low (based on the percentile values)

Developer : Jagadeesh D

Tables involved
raw_customers
raw_orders
raw_order_items
raw_order_payments

"""

import dlt
import json
from pyspark.sql.functions import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA silver")



# -------------------------------------------------------------------
# table 1: Filtering out last 12 months from anchordate
# --------------------------------------------------------------------
@dlt.table(
    name = "stg_last_12months_orders",
    comment = "Calculating latest order date from order_purchase_timestamp"
)
@dlt.expect_or_fail(
    "valid_order_date", 
    "order_purchase_timestamp is not null"
)
def last_12months_orders():
    # Read both upstream views
    df_orders = dlt.read("data_sentinals.bronze.raw_orders")
    df_orders = df_orders.filter(lower(col("order_status")) == "delivered") 

    # calculating the anchor date
    df_anchor = df_orders.agg(max("order_purchase_timestamp").alias("anchor_date"))
    
    # Cross join and filter
    df_joined = df_orders.crossJoin(df_anchor)


    # filtering last 12 months order
    df_last_12_month_orders = df_joined.filter(
        col("order_purchase_timestamp") >= F.add_months(F.col("anchor_date"), -12)
    )

    # joining with stg_orders_agg (inner join)
    
    df_orders_agg = dlt.read("data_sentinals.silver.stg_orders_agg")
    df_orders_agg = df_orders_agg.select("order_id", "total_price")
    df_last_12_month_orders = df_last_12_month_orders.join(df_orders_agg, on="order_id", how="left")

    # getting the customer_unique_id
    df_customers = dlt.read("data_sentinals.bronze.raw_customers")
    df_customers = df_customers.select("customer_id", "customer_unique_id")
    
    df_cust_unique = df_last_12_month_orders.join(df_customers, on="customer_id", how = "left")

    
    return df_cust_unique

    
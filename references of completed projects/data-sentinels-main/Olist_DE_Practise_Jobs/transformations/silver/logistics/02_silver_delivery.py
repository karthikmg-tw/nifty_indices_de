import dlt
from pyspark.sql import functions as F


spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA silver")


# -------------------------------------------------------------------
# CONFIG
# Update if your catalog/schema names differ.
# -------------------------------------------------------------------
BRONZE_ORDERS_TABLE = "data_sentinals.bronze.raw_orders"
BRONZE_ORDER_ITEMS_TABLE = "data_sentinals.bronze.raw_order_items"

# These are created by 01_silver_dimensions.py in the same pipeline target schema
CUSTOMER_GEO_TABLE = "customer_geo_silver"
SELLER_GEO_TABLE = "seller_geo_silver"


# Build order-level delivery fact table
@dlt.table(
    name="delivery_order_silver",
    comment="Order-level delivery performance base for logistics and SLA analytics"
)
@dlt.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect("customer_id_not_null", "customer_id IS NOT NULL")
@dlt.expect("valid_order_status", """
  order_status IN (
    'created','approved','invoiced','processing',
    'shipped','delivered','unavailable','canceled'
  )
""")
def delivery_order_silver():

    # Read Bronze orders and Silver customer geography
    orders = spark.read.table(BRONZE_ORDERS_TABLE)
    customers = dlt.read(CUSTOMER_GEO_TABLE)

    # Standardize order timestamps and status values
    orders_std = (
        orders.select(
            F.col("order_id").cast("string").alias("order_id"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.lower(F.trim(F.col("order_status"))).alias("order_status"),
            F.to_timestamp("order_purchase_timestamp").alias("order_purchase_ts"),
            F.to_timestamp("order_approved_at").alias("order_approved_ts"),
            F.to_timestamp("order_delivered_carrier_date").alias("order_delivered_carrier_ts"),
            F.to_timestamp("order_delivered_customer_date").alias("order_delivered_customer_ts"),
            F.to_timestamp("order_estimated_delivery_date").alias("order_estimated_delivery_ts")
        )
    )

    # Join customer region details
    joined = (
        orders_std.alias("o")
        .join(
            customers.alias("c"),
            F.col("o.customer_id") == F.col("c.customer_id"),
            "left"
        )
        .select(
            F.col("o.order_id"),
            F.col("o.customer_id"),
            F.col("o.order_status"),
            F.col("o.order_purchase_ts"),
            F.col("o.order_approved_ts"),
            F.col("o.order_delivered_carrier_ts"),
            F.col("o.order_delivered_customer_ts"),
            F.col("o.order_estimated_delivery_ts"),
            F.col("c.customer_unique_id"),
            F.col("c.customer_zip_code_prefix"),
            F.col("c.customer_city_std"),
            F.col("c.customer_state"),
            F.col("c.customer_lat"),
            F.col("c.customer_lng")
        )
    )

    # Compute delivery KPIs and business flags
    return (
        joined
        .withColumn(
            "is_delivered",
            F.when(F.col("order_status") == "delivered", F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "has_actual_delivery_ts",
            F.when(F.col("order_delivered_customer_ts").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "has_estimated_delivery_ts",
            F.when(F.col("order_estimated_delivery_ts").isNotNull(), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn(
            "delivery_delta_days",
            F.when(
                (F.col("order_status") == "delivered") &
                F.col("order_delivered_customer_ts").isNotNull() &
                F.col("order_estimated_delivery_ts").isNotNull(),
                F.datediff(
                    F.to_date("order_delivered_customer_ts"),
                    F.to_date("order_estimated_delivery_ts")
                )
            )
        )
        .withColumn(
            "purchase_to_delivery_days",
            F.when(
                F.col("order_delivered_customer_ts").isNotNull() &
                F.col("order_purchase_ts").isNotNull(),
                F.datediff(
                    F.to_date("order_delivered_customer_ts"),
                    F.to_date("order_purchase_ts")
                )
            )
        )
        .withColumn(
            "carrier_to_customer_days",
            F.when(
                F.col("order_delivered_customer_ts").isNotNull() &
                F.col("order_delivered_carrier_ts").isNotNull(),
                F.datediff(
                    F.to_date("order_delivered_customer_ts"),
                    F.to_date("order_delivered_carrier_ts")
                )
            )
        )
        .withColumn(
            "estimated_lead_time_days",
            F.when(
                F.col("order_estimated_delivery_ts").isNotNull() &
                F.col("order_purchase_ts").isNotNull(),
                F.datediff(
                    F.to_date("order_estimated_delivery_ts"),
                    F.to_date("order_purchase_ts")
                )
            )
        )
        .withColumn(
            "is_late",
            F.when(F.col("delivery_delta_days") > 0, F.lit(1))
             .when(F.col("delivery_delta_days").isNotNull(), F.lit(0))
        )
        .withColumn(
            "delivery_status_bucket",
            F.when(F.col("order_status") != "delivered", F.lit("not_delivered"))
             .when(F.col("order_delivered_customer_ts").isNull(), F.lit("delivery_timestamp_missing"))
             .when(F.col("order_estimated_delivery_ts").isNull(), F.lit("estimated_timestamp_missing"))
             .when(F.col("delivery_delta_days") < 0, F.lit("early"))
             .when(F.col("delivery_delta_days") == 0, F.lit("on_time"))
             .when((F.col("delivery_delta_days") >= 1) & (F.col("delivery_delta_days") <= 3), F.lit("late_1_3_days"))
             .when((F.col("delivery_delta_days") >= 4) & (F.col("delivery_delta_days") <= 7), F.lit("late_4_7_days"))
             .when(F.col("delivery_delta_days") > 7, F.lit("late_8_plus_days"))
        )
        .withColumn("order_purchase_date", F.to_date("order_purchase_ts"))
        .withColumn("order_purchase_month", F.date_trunc("month", F.col("order_purchase_ts")))
    )


# Build order + seller rollup table from item-level Bronze data
@dlt.table(
    name="order_seller_rollup_silver",
    comment="Order-to-seller rollup with item and revenue metrics for seller attribution"
)
@dlt.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("seller_id_not_null", "seller_id IS NOT NULL")
def order_seller_rollup_silver():

    # Read Bronze order items and Silver seller geography
    items = spark.read.table(BRONZE_ORDER_ITEMS_TABLE)
    sellers = dlt.read(SELLER_GEO_TABLE)

    # Standardize item-level order data
    items_std = (
        items.select(
            F.col("order_id").cast("string").alias("order_id"),
            F.col("order_item_id").cast("int").alias("order_item_id"),
            F.col("product_id").cast("string").alias("product_id"),
            F.col("seller_id").cast("string").alias("seller_id"),
            F.to_timestamp("shipping_limit_date").alias("shipping_limit_ts"),
            F.col("price").cast("double").alias("price"),
            F.col("freight_value").cast("double").alias("freight_value")
        )
    )

    # Aggregate item rows to one row per order + seller
    seller_rollup = (
        items_std.groupBy("order_id", "seller_id")
        .agg(
            F.count("*").alias("item_count"),
            F.countDistinct("product_id").alias("distinct_product_count"),
            F.sum("price").alias("order_seller_item_value"),
            F.sum("freight_value").alias("order_seller_freight_value"),
            F.max("shipping_limit_ts").alias("max_shipping_limit_ts")
        )
    )

    # Enrich seller rollup with seller region information
    return (
        seller_rollup.alias("r")
        .join(
            sellers.alias("s"),
            F.col("r.seller_id") == F.col("s.seller_id"),
            "left"
        )
        .select(
            F.col("r.order_id"),
            F.col("r.seller_id"),
            F.col("r.item_count"),
            F.col("r.distinct_product_count"),
            F.col("r.order_seller_item_value"),
            F.col("r.order_seller_freight_value"),
            F.col("r.max_shipping_limit_ts"),
            F.col("s.seller_zip_code_prefix"),
            F.col("s.seller_city_std"),
            F.col("s.seller_state"),
            F.col("s.seller_lat"),
            F.col("s.seller_lng")
        )
    )


# Build final order + seller delivery analysis table
@dlt.table(
    name="delivery_order_item_silver",
    comment="Order-seller delivery base for corridor and seller-region logistics analysis"
)
@dlt.expect_or_drop("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect("seller_id_not_null", "seller_id IS NOT NULL")
def delivery_order_item_silver():

    # Read prior Silver tables from same pipeline
    delivery_orders = dlt.read("delivery_order_silver")
    seller_rollup = dlt.read("order_seller_rollup_silver")

    # Join order-level delivery results with seller-level attribution
    return (
        seller_rollup.alias("s")
        .join(
            delivery_orders.alias("d"),
            F.col("s.order_id") == F.col("d.order_id"),
            "inner"
        )
        .select(
            F.col("d.order_id"),
            F.col("d.customer_id"),
            F.col("s.seller_id"),
            F.col("d.order_status"),
            F.col("d.order_purchase_ts"),
            F.col("d.order_purchase_date"),
            F.col("d.order_purchase_month"),
            F.col("d.order_approved_ts"),
            F.col("d.order_delivered_carrier_ts"),
            F.col("d.order_delivered_customer_ts"),
            F.col("d.order_estimated_delivery_ts"),
            F.col("d.is_delivered"),
            F.col("d.has_actual_delivery_ts"),
            F.col("d.has_estimated_delivery_ts"),
            F.col("d.delivery_delta_days"),
            F.col("d.purchase_to_delivery_days"),
            F.col("d.carrier_to_customer_days"),
            F.col("d.estimated_lead_time_days"),
            F.col("d.is_late"),
            F.col("d.delivery_status_bucket"),
            F.col("d.customer_zip_code_prefix"),
            F.col("d.customer_city_std"),
            F.col("d.customer_state"),
            F.col("d.customer_lat"),
            F.col("d.customer_lng"),
            F.col("s.seller_zip_code_prefix"),
            F.col("s.seller_city_std"),
            F.col("s.seller_state"),
            F.col("s.seller_lat"),
            F.col("s.seller_lng"),
            F.col("s.item_count"),
            F.col("s.distinct_product_count"),
            F.col("s.order_seller_item_value"),
            F.col("s.order_seller_freight_value"),
            F.col("s.max_shipping_limit_ts")
        )
    )
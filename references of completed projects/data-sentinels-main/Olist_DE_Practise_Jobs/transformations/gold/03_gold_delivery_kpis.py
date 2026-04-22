import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA gold")

# ================================================================
# GOLD 1: CUSTOMER STATE MONTHLY DELIVERY PERFORMANCE
# ================================================================
# Purpose:
# - Analyze delivery performance from the customer (destination) side
# - Identify which states are facing delays over time
# - Provide SLA metrics aggregated monthly


@dlt.table(
    name="gold_delivery_state_month",
    comment="Monthly delivery KPI summary by customer state"
)
def gold_delivery_state_month():

    # Read Silver order-level delivery dataset
    # (Created in 02_silver_delivery.py)
    delivery = dlt.read("delivery_order_silver")

    # Filter only valid rows:
    # - delivery_delta_days must exist (only delivered orders with timestamps)
    # - customer_state must exist
    base = delivery.filter(
        F.col("delivery_delta_days").isNotNull() &
        F.col("customer_state").isNotNull()
    )

    # Aggregate KPIs at:
    # month x customer_state level
    agg_df = (
        base.groupBy("order_purchase_month", "customer_state")
        .agg(

            # Total number of delivered orders
            F.countDistinct("order_id").alias("total_delivered_orders"),

            # Average delay (actual - estimated)
            F.avg("delivery_delta_days").alias("avg_delivery_delta_days"),

            # Median delay (more robust than average)
            F.expr("percentile_approx(delivery_delta_days, 0.5)")
                .alias("median_delivery_delta_days"),

            # Average total delivery time
            F.avg("purchase_to_delivery_days")
                .alias("avg_purchase_to_delivery_days"),

            # Average last-mile delivery time
            F.avg("carrier_to_customer_days")
                .alias("avg_carrier_to_customer_days"),

            # Average promised delivery duration
            F.avg("estimated_lead_time_days")
                .alias("avg_estimated_lead_time_days"),

            # % of late deliveries
            # (1 = late, 0 = not late → average gives %)
            F.avg(F.when(F.col("is_late") == 1, 1.0).otherwise(0.0))
                .alias("late_delivery_rate"),

            # % delivered on time or early
            F.avg(F.when(F.col("delivery_delta_days") <= 0, 1.0).otherwise(0.0))
                .alias("on_time_or_early_rate"),

            # % delivered early
            F.avg(F.when(F.col("delivery_delta_days") < 0, 1.0).otherwise(0.0))
                .alias("early_delivery_rate"),

            # % of severe delays (>7 days)
            F.avg(F.when(F.col("delivery_delta_days") > 7, 1.0).otherwise(0.0))
                .alias("severe_delay_rate")
        )
    )

    # Rank states within each month by worst delay
    return agg_df.withColumn(
        "state_delay_rank_in_month",
        F.dense_rank().over(
            Window.partitionBy("order_purchase_month")
            .orderBy(F.desc("avg_delivery_delta_days"))
        )
    )


# ================================================================
# GOLD 2: SELLER STATE MONTHLY DELIVERY PERFORMANCE
# ================================================================
# Purpose:
# - Analyze delivery performance from seller (origin) side
# - Identify which seller regions are causing delays


@dlt.table(
    name="gold_delivery_seller_state_month",
    comment="Monthly delivery KPI summary by seller state"
)
def gold_delivery_seller_state_month():

    # Read Silver order + seller enriched dataset
    delivery = dlt.read("delivery_order_item_silver")

    # Filter valid rows
    base = delivery.filter(
        F.col("delivery_delta_days").isNotNull() &
        F.col("seller_state").isNotNull()
    )

    # Aggregate KPIs at:
    # month x seller_state level
    agg_df = (
        base.groupBy("order_purchase_month", "seller_state")
        .agg(

            # Total number of orders
            F.countDistinct("order_id").alias("total_orders"),

            # Number of sellers active in that state
            F.countDistinct("seller_id").alias("active_sellers"),

            # Average delay
            F.avg("delivery_delta_days").alias("avg_delivery_delta_days"),

            # Median delay
            F.expr("percentile_approx(delivery_delta_days, 0.5)")
                .alias("median_delivery_delta_days"),

            # Average item value handled by sellers
            F.avg("order_seller_item_value")
                .alias("avg_order_seller_item_value"),

            # Average freight cost
            F.avg("order_seller_freight_value")
                .alias("avg_order_seller_freight_value"),

            # Late delivery rate
            F.avg(F.when(F.col("is_late") == 1, 1.0).otherwise(0.0))
                .alias("late_delivery_rate"),

            # Severe delay rate (>7 days)
            F.avg(F.when(F.col("delivery_delta_days") > 7, 1.0).otherwise(0.0))
                .alias("severe_delay_rate")
        )
    )

    # Rank seller states by worst performance per month
    return agg_df.withColumn(
        "seller_state_delay_rank_in_month",
        F.dense_rank().over(
            Window.partitionBy("order_purchase_month")
            .orderBy(F.desc("avg_delivery_delta_days"))
        )
    )


# ================================================================
# GOLD 3: CORRIDOR ANALYSIS (SELLER STATE → CUSTOMER STATE)
# ================================================================
# Purpose:
# - Identify bottlenecks at route level
# - Core table for logistics optimization insights


@dlt.table(
    name="gold_delivery_corridor_month",
    comment="Monthly delivery KPI summary by seller-state to customer-state corridor"
)
def gold_delivery_corridor_month():

    # Read Silver enriched dataset
    delivery = dlt.read("delivery_order_item_silver")

    # Filter valid rows
    base = delivery.filter(
        F.col("delivery_delta_days").isNotNull() &
        F.col("seller_state").isNotNull() &
        F.col("customer_state").isNotNull()
    )

    # Aggregate KPIs at:
    # month x seller_state x customer_state
    agg_df = (
        base.groupBy("order_purchase_month", "seller_state", "customer_state")
        .agg(

            # Total orders in that route
            F.countDistinct("order_id").alias("total_orders"),

            # Active sellers contributing to this route
            F.countDistinct("seller_id").alias("active_sellers"),

            # Average delay
            F.avg("delivery_delta_days").alias("avg_delivery_delta_days"),

            # Median delay
            F.expr("percentile_approx(delivery_delta_days, 0.5)")
                .alias("median_delivery_delta_days"),

            # Average total delivery time
            F.avg("purchase_to_delivery_days")
                .alias("avg_purchase_to_delivery_days"),

            # Average freight cost
            F.avg("order_seller_freight_value")
                .alias("avg_freight_value"),

            # Late rate
            F.avg(F.when(F.col("is_late") == 1, 1.0).otherwise(0.0))
                .alias("late_delivery_rate"),

            # Severe delay rate
            F.avg(F.when(F.col("delivery_delta_days") > 7, 1.0).otherwise(0.0))
                .alias("severe_delay_rate")
        )
    )

    # Rank worst corridors per month
    return agg_df.withColumn(
        "corridor_delay_rank_in_month",
        F.dense_rank().over(
            Window.partitionBy("order_purchase_month")
            .orderBy(F.desc("avg_delivery_delta_days"))
        )
    )


# ================================================================
# GOLD 4: DELIVERY HOTSPOTS (OVERALL STATE VIEW)
# ================================================================
# Purpose:
# - Identify worst-performing states overall (not time-based)
# - Useful for dashboards and summary insights


@dlt.table(
    name="gold_delivery_hotspots",
    comment="Delay hotspot summary by customer state"
)
def gold_delivery_hotspots():

    delivery = dlt.read("delivery_order_silver")

    base = delivery.filter(
        F.col("delivery_delta_days").isNotNull() &
        F.col("customer_state").isNotNull()
    )

    agg_df = (
        base.groupBy("customer_state")
        .agg(
            F.countDistinct("order_id").alias("total_delivered_orders"),
            F.avg("delivery_delta_days").alias("avg_delivery_delta_days"),
            F.expr("percentile_approx(delivery_delta_days, 0.5)")
                .alias("median_delivery_delta_days"),
            F.avg(F.when(F.col("is_late") == 1, 1.0).otherwise(0.0))
                .alias("late_delivery_rate"),
            F.avg(F.when(F.col("delivery_delta_days") > 7, 1.0).otherwise(0.0))
                .alias("severe_delay_rate")
        )
    )

    # Rank worst states globally
    return agg_df.withColumn(
        "hotspot_rank",
        F.dense_rank().over(
            Window.orderBy(F.desc("avg_delivery_delta_days"))
        )
    )


# ================================================================
# GOLD 5: EXECUTIVE SUMMARY (ONE ROW KPI TABLE)
# ================================================================
# Purpose:
# - Provide high-level KPIs for dashboards / leadership view


@dlt.table(
    name="gold_delivery_exec_summary",
    comment="Executive summary of delivery performance KPIs"
)
def gold_delivery_exec_summary():

    delivery = dlt.read("delivery_order_silver")

    base = delivery.filter(F.col("delivery_delta_days").isNotNull())

    # Aggregate all data into a single KPI row
    return (
        base.agg(
            F.countDistinct("order_id").alias("total_delivered_orders"),
            F.avg("delivery_delta_days").alias("avg_delivery_delta_days"),
            F.expr("percentile_approx(delivery_delta_days, 0.5)")
                .alias("median_delivery_delta_days"),
            F.avg("purchase_to_delivery_days")
                .alias("avg_purchase_to_delivery_days"),
            F.avg("carrier_to_customer_days")
                .alias("avg_carrier_to_customer_days"),
            F.avg("estimated_lead_time_days")
                .alias("avg_estimated_lead_time_days"),
            F.avg(F.when(F.col("is_late") == 1, 1.0).otherwise(0.0))
                .alias("late_delivery_rate"),
            F.avg(F.when(F.col("delivery_delta_days") <= 0, 1.0).otherwise(0.0))
                .alias("on_time_or_early_rate"),
            F.avg(F.when(F.col("delivery_delta_days") < 0, 1.0).otherwise(0.0))
                .alias("early_delivery_rate"),
            F.avg(F.when(F.col("delivery_delta_days") > 7, 1.0).otherwise(0.0))
                .alias("severe_delay_rate")
        )
        # Add timestamp for tracking refresh time
        .withColumn("kpi_generated_ts", F.current_timestamp())
    )
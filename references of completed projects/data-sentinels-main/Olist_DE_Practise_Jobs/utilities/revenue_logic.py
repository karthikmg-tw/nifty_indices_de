"""
/// Summary : Pure PySpark transformation functions for Use Case 1
///           (Revenue & Growth Analytics). DataFrame-in / DataFrame-out
///           helpers, intentionally decoupled from Delta Live Tables so
///           they can be unit-tested with a local SparkSession.
/// Data    : raw_orders, raw_order_items, raw_order_payments, raw_products,
///           raw_product_names_translation, raw_customers
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


REVENUE_RECOGNISED_STATUSES = [
    "delivered",
    "shipped",
    "invoiced",
    "processing",
    "approved",
    "created",
]

VALID_PAYMENT_TYPES = [
    "credit_card",
    "boleto",
    "voucher",
    "debit_card",
    "not_defined",
]


def clean_payments(raw_order_payments: DataFrame) -> DataFrame:
    """Normalise payment rows. Negative payments are filtered out here
    so downstream aggregations cannot be skewed by refunds/corrections."""
    return (
        raw_order_payments.select(
            F.col("order_id").cast("string").alias("order_id"),
            F.col("payment_sequential").cast("int").alias("payment_sequential"),
            F.lower(F.trim(F.col("payment_type"))).alias("payment_type"),
            F.col("payment_installments").cast("int").alias("payment_installments"),
            F.col("payment_value").cast("double").alias("payment_value"),
        )
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("payment_value") >= 0)
    )


def aggregate_payments_per_order(payments_clean: DataFrame) -> DataFrame:
    """Collapse the 1-to-many `raw_order_payments` rows into one row per
    `order_id`. Derives `primary_payment_type` as the payment type
    contributing the largest single value for the order."""
    window_by_value = (
        Window.partitionBy("order_id").orderBy(F.desc("payment_value"))
    )
    with_primary = payments_clean.withColumn(
        "_rank_by_value", F.row_number().over(window_by_value)
    )
    primary = (
        with_primary.filter(F.col("_rank_by_value") == 1)
        .select(
            F.col("order_id"),
            F.col("payment_type").alias("primary_payment_type"),
        )
    )

    rollup = (
        payments_clean.groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.count("*").alias("n_payments"),
            F.max("payment_installments").alias("installments_max"),
            F.concat_ws(
                ",",
                F.array_sort(F.collect_set("payment_type")),
            ).alias("payment_type_mix"),
        )
    )

    return rollup.join(primary, on="order_id", how="left")


def build_product_catalog(
    raw_products: DataFrame,
    raw_product_names_translation: DataFrame,
) -> DataFrame:
    """Translate category names to English; coalesce missing translations
    to the original category or the sentinel `unknown`."""
    translation = raw_product_names_translation.select(
        F.col("product_category_name").alias("_cat_pt"),
        F.col("product_category_name_english").alias("_cat_en"),
    )

    products = raw_products.select(
        F.col("product_id").cast("string").alias("product_id"),
        F.col("product_category_name").alias("product_category_name"),
        F.col("product_weight_g").cast("int").alias("product_weight_g"),
        F.col("product_length_cm").cast("int").alias("product_length_cm"),
        F.col("product_height_cm").cast("int").alias("product_height_cm"),
        F.col("product_width_cm").cast("int").alias("product_width_cm"),
    ).filter(F.col("product_id").isNotNull())

    return (
        products.join(
            translation,
            products["product_category_name"] == translation["_cat_pt"],
            "left",
        )
        .withColumn(
            "product_category_en",
            F.coalesce(F.col("_cat_en"), F.col("product_category_name"), F.lit("unknown")),
        )
        .drop("_cat_pt", "_cat_en")
    )


def build_order_items_revenue(
    raw_order_items: DataFrame,
    product_catalog: DataFrame,
) -> DataFrame:
    """Enrich each line item with its English category and compute the
    item-level revenue (price + freight)."""
    items = raw_order_items.select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("order_item_id").cast("int").alias("order_item_id"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("seller_id").cast("string").alias("seller_id"),
        F.col("price").cast("double").alias("price"),
        F.col("freight_value").cast("double").alias("freight_value"),
    ).filter(
        (F.col("price") >= 0) & (F.col("freight_value") >= 0)
    )

    return (
        items.join(
            product_catalog.select("product_id", "product_category_en"),
            on="product_id",
            how="left",
        )
        .withColumn(
            "product_category_en",
            F.coalesce(F.col("product_category_en"), F.lit("unknown")),
        )
        .withColumn(
            "item_total_revenue",
            F.col("price") + F.col("freight_value"),
        )
    )


def aggregate_items_per_order(order_items_revenue: DataFrame) -> DataFrame:
    """Collapse line items into an order-grain revenue row."""
    return (
        order_items_revenue.groupBy("order_id")
        .agg(
            F.count("*").alias("n_items"),
            F.sum("price").alias("gross_revenue"),
            F.sum("freight_value").alias("freight_total"),
            F.sum("item_total_revenue").alias("revenue_total"),
        )
    )


def build_orders_revenue(
    raw_orders: DataFrame,
    items_agg: DataFrame,
    payments_agg: DataFrame,
    customer_geo: DataFrame,
) -> DataFrame:
    """Build the order-grain revenue fact."""
    orders = raw_orders.select(
        F.col("order_id").cast("string").alias("order_id"),
        F.col("customer_id").cast("string").alias("customer_id"),
        F.lower(F.trim(F.col("order_status"))).alias("order_status"),
        F.to_timestamp("order_purchase_timestamp").alias("order_purchase_ts"),
    ).filter(F.col("order_id").isNotNull())

    base = (
        orders.join(items_agg, on="order_id", how="inner")
        .join(payments_agg, on="order_id", how="left")
    )

    if customer_geo is not None:
        base = base.join(
            customer_geo.select(
                "customer_id",
                "customer_unique_id",
                "customer_state",
                "customer_city_std",
            ),
            on="customer_id",
            how="left",
        )
    else:
        base = (
            base.withColumn("customer_unique_id", F.lit(None).cast("string"))
            .withColumn("customer_state", F.lit(None).cast("string"))
            .withColumn("customer_city_std", F.lit(None).cast("string"))
        )

    return (
        base.withColumn("order_purchase_date", F.to_date("order_purchase_ts"))
        .withColumn(
            "order_purchase_month",
            F.trunc(F.col("order_purchase_date"), "MM"),
        )
        .withColumn("order_purchase_year", F.year("order_purchase_ts"))
        .withColumn(
            "payment_reconciliation_delta",
            F.col("revenue_total") - F.coalesce(F.col("total_payment_value"), F.lit(0.0)),
        )
        .withColumn(
            "is_revenue_recognised",
            F.when(
                F.col("order_status").isin("canceled", "unavailable"),
                F.lit(0),
            ).otherwise(F.lit(1)),
        )
    )


def build_gold_revenue_monthly(orders_revenue: DataFrame) -> DataFrame:
    """Monthly GMV with MoM / YoY and rolling windows (3m, 6m)."""
    base = (
        orders_revenue.filter(F.col("is_revenue_recognised") == 1)
        .filter(F.col("order_purchase_month").isNotNull())
        .groupBy("order_purchase_month")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("revenue_total").alias("total_revenue"),
        )
        .withColumn(
            "aov",
            F.when(F.col("total_orders") > 0, F.col("total_revenue") / F.col("total_orders")),
        )
    )

    prev_month = base.select(
        F.add_months(F.col("order_purchase_month"), 1).cast("date").alias("order_purchase_month"),
        F.col("total_revenue").alias("prev_month_revenue"),
    )
    prev_year = base.select(
        F.add_months(F.col("order_purchase_month"), 12).cast("date").alias("order_purchase_month"),
        F.col("total_revenue").alias("prev_year_revenue"),
    )

    w_all = Window.orderBy("order_purchase_month")
    w_roll_3 = w_all.rowsBetween(-2, 0)
    w_roll_6 = w_all.rowsBetween(-5, 0)

    return (
        base.join(prev_month, on="order_purchase_month", how="left")
        .join(prev_year, on="order_purchase_month", how="left")
        .withColumn(
            "revenue_mom_pct",
            F.when(
                F.col("prev_month_revenue") > 0,
                (F.col("total_revenue") - F.col("prev_month_revenue"))
                / F.col("prev_month_revenue") * 100.0,
            ),
        )
        .withColumn(
            "revenue_yoy_pct",
            F.when(
                F.col("prev_year_revenue") > 0,
                (F.col("total_revenue") - F.col("prev_year_revenue"))
                / F.col("prev_year_revenue") * 100.0,
            ),
        )
        .withColumn("revenue_rolling_3m", F.sum("total_revenue").over(w_roll_3))
        .withColumn("revenue_rolling_6m", F.sum("total_revenue").over(w_roll_6))
        .drop("prev_month_revenue", "prev_year_revenue")
    )


def build_gold_revenue_by_category_month(
    orders_revenue: DataFrame,
    order_items_revenue: DataFrame,
) -> DataFrame:
    """Monthly revenue by product category."""
    order_month = (
        orders_revenue.filter(F.col("is_revenue_recognised") == 1)
        .select(F.col("order_id"), F.col("order_purchase_month"))
    )

    base = (
        order_items_revenue.join(order_month, on="order_id", how="inner")
        .groupBy("order_purchase_month", "product_category_en")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("item_total_revenue").alias("total_revenue"),
        )
    )

    w_month_total = Window.partitionBy("order_purchase_month")
    w_month_rank = Window.partitionBy("order_purchase_month").orderBy(
        F.desc("total_revenue")
    )

    return (
        base.withColumn("month_total_revenue", F.sum("total_revenue").over(w_month_total))
        .withColumn(
            "revenue_share_of_month",
            F.when(
                F.col("month_total_revenue") > 0,
                F.col("total_revenue") / F.col("month_total_revenue"),
            ),
        )
        .withColumn("category_rank_in_month", F.dense_rank().over(w_month_rank))
        .drop("month_total_revenue")
    )


def build_gold_revenue_by_state_month(orders_revenue: DataFrame) -> DataFrame:
    """Monthly revenue by customer state."""
    base = (
        orders_revenue.filter(F.col("is_revenue_recognised") == 1)
        .filter(F.col("customer_state").isNotNull())
        .groupBy("order_purchase_month", "customer_state")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("customer_unique_id").alias("total_customers"),
            F.sum("revenue_total").alias("total_revenue"),
        )
        .withColumn(
            "aov",
            F.when(F.col("total_orders") > 0, F.col("total_revenue") / F.col("total_orders")),
        )
    )

    w_month_total = Window.partitionBy("order_purchase_month")
    w_month_rank = Window.partitionBy("order_purchase_month").orderBy(
        F.desc("total_revenue")
    )

    return (
        base.withColumn("month_total_revenue", F.sum("total_revenue").over(w_month_total))
        .withColumn(
            "revenue_share_of_month",
            F.when(
                F.col("month_total_revenue") > 0,
                F.col("total_revenue") / F.col("month_total_revenue"),
            ),
        )
        .withColumn("state_rank_in_month", F.dense_rank().over(w_month_rank))
        .drop("month_total_revenue")
    )


def build_gold_revenue_by_payment_type_month(orders_revenue: DataFrame) -> DataFrame:
    """Monthly revenue split by the primary payment type for the order."""
    base = (
        orders_revenue.filter(F.col("is_revenue_recognised") == 1)
        .filter(F.col("primary_payment_type").isNotNull())
        .groupBy("order_purchase_month", "primary_payment_type")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("revenue_total").alias("total_revenue"),
            F.avg("installments_max").alias("avg_installments"),
        )
    )

    w_month_total = Window.partitionBy("order_purchase_month")

    return (
        base.withColumn("month_total_revenue", F.sum("total_revenue").over(w_month_total))
        .withColumn(
            "share_of_month_revenue",
            F.when(
                F.col("month_total_revenue") > 0,
                F.col("total_revenue") / F.col("month_total_revenue"),
            ),
        )
        .drop("month_total_revenue")
    )


def build_gold_revenue_exec_summary(
    orders_revenue: DataFrame,
    revenue_monthly: DataFrame,
    revenue_by_category_month: DataFrame,
    revenue_by_state_month: DataFrame,
    revenue_by_payment_type_month: DataFrame,
) -> DataFrame:
    """Single-row snapshot for the executive dashboard.
    
    Refactored to be pure PySpark (no .collect() or .limit()) so it can
    be safely evaluated in Delta Live Tables during Graph Initialization.
    """
    recognised = orders_revenue.filter(F.col("is_revenue_recognised") == 1)

    # 1. Grab the max date into a 1-row DataFrame and broadcast it. 
    # Using a dummy key (_dummy) allows us to safely append this max date to every row.
    anchor_df = recognised.agg(
        F.max("order_purchase_date").alias("anchor_date")
    ).withColumn("_dummy", F.lit(1))

    df_with_anchor = recognised.withColumn("_dummy", F.lit(1)).join(
        F.broadcast(anchor_df), on="_dummy", how="inner"
    )

    # 2. Tag rows that belong to the L12M and Prior 12M windows
    df_summary_stats = df_with_anchor.withColumn(
        "is_l12m", F.col("order_purchase_date") > F.date_sub(F.col("anchor_date"), 365)
    ).withColumn(
        "is_prior12m", (F.col("order_purchase_date") <= F.date_sub(F.col("anchor_date"), 365)) &
                       (F.col("order_purchase_date") > F.date_sub(F.col("anchor_date"), 730))
    )

    # 3. Aggregate globally (creates our single-row base output)
    totals = df_summary_stats.agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("revenue_total").alias("total_revenue"),
        F.coalesce(F.sum(F.when(F.col("is_l12m"), F.col("revenue_total"))), F.lit(0.0)).alias("last_12m_revenue"),
        F.coalesce(F.sum(F.when(F.col("is_prior12m"), F.col("revenue_total"))), F.lit(0.0)).alias("prior_12m_revenue")
    ).withColumn(
        "aov",
        F.when(F.col("total_orders") > 0, F.col("total_revenue") / F.col("total_orders"))
    ).withColumn(
        "yoy_growth_pct",
        F.when(
            F.col("prior_12m_revenue") > 0,
            (F.col("last_12m_revenue") - F.col("prior_12m_revenue")) / F.col("prior_12m_revenue") * 100.0
        )
    ).withColumn("_dummy", F.lit(1))

    # 4. Helper function to find the top item dynamically without .collect()
    def get_top_item(df: DataFrame, group_col: str, value_col: str, alias_name: str) -> DataFrame:
        return (
            df.groupBy(group_col)
            .agg(F.sum(value_col).alias("_rev"))
            .withColumn("_rank", F.row_number().over(Window.orderBy(F.desc("_rev"))))
            .filter(F.col("_rank") == 1)
            .select(F.col(group_col).alias(alias_name))
            .withColumn("_dummy", F.lit(1)) # Add dummy key so we can left-join it back to the summary row
        )

    top_category = get_top_item(
        revenue_by_category_month, "product_category_en", "total_revenue", "top_category"
    )
    top_state = get_top_item(
        revenue_by_state_month, "customer_state", "total_revenue", "top_customer_state"
    )
    top_payment = get_top_item(
        revenue_by_payment_type_month, "primary_payment_type", "total_revenue", "top_payment_type"
    )

    # 5. Stitch everything together using left joins to ensure empty source tables don't crash the pipeline
    return (
        totals
        .join(top_category, on="_dummy", how="left")
        .join(top_state, on="_dummy", how="left")
        .join(top_payment, on="_dummy", how="left")
        .drop("_dummy")
        .withColumn("kpi_generated_ts", F.current_timestamp())
    )
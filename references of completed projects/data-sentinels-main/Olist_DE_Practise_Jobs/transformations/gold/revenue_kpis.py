"""
/// Summary : Gold marts for Use Case 1 (Revenue & Growth Analytics).
///           Publishes five business-ready tables: monthly revenue
///           trend with MoM/YoY and rolling windows, revenue by
///           product category, revenue by customer state, revenue
///           by primary payment type, and a single-row executive
///           summary. All marts filter out canceled/unavailable
///           orders via `is_revenue_recognised = 1`.
/// Data    : silver.orders_revenue_silver, silver.order_items_revenue_silver
"""

import dlt
import os
import sys

# Shared pure-logic module lives in `utilities/` (used by silver and gold).
# We use os.getcwd() rather than __file__ because the latter is not
# guaranteed to be defined in every DLT execution context - the same
# pattern used by transformations/bronze/data_ingestion.py.
_cwd = os.getcwd()
if "Olist_DE_Practise_Jobs" in _cwd:
    _utilities_dir = os.path.join(
        _cwd.split("Olist_DE_Practise_Jobs")[0],
        "Olist_DE_Practise_Jobs",
        "utilities",
    )
else:
    _utilities_dir = os.path.join(_cwd, "utilities")
if _utilities_dir not in sys.path:
    sys.path.insert(0, _utilities_dir)

from revenue_logic import (  # noqa: E402
    build_gold_revenue_monthly,
    build_gold_revenue_by_category_month,
    build_gold_revenue_by_state_month,
    build_gold_revenue_by_payment_type_month,
    build_gold_revenue_exec_summary,
)


spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA gold")


SILVER_ORDERS_REVENUE_TABLE = "data_sentinals.silver.orders_revenue_silver"
SILVER_ORDER_ITEMS_REVENUE_TABLE = "data_sentinals.silver.order_items_revenue_silver"


# -------------------------------------------------------------------
# GOLD 1: MONTHLY REVENUE TREND
# Grain: one row per month
# Signature metrics: total_revenue, MoM %, YoY %, rolling 3m & 6m
# -------------------------------------------------------------------
@dlt.table(
    name="gold_revenue_monthly",
    comment="Monthly revenue trend with MoM, YoY and rolling 3m/6m windows",
)
@dlt.expect("total_revenue_non_negative", "total_revenue >= 0")
def gold_revenue_monthly():
    orders_revenue = dlt.read(SILVER_ORDERS_REVENUE_TABLE)
    return build_gold_revenue_monthly(orders_revenue)


# -------------------------------------------------------------------
# GOLD 2: REVENUE BY CATEGORY & MONTH
# Grain: month x product_category_en
# -------------------------------------------------------------------
@dlt.table(
    name="gold_revenue_by_category_month",
    comment="Monthly revenue by product category with share-of-month and rank",
)
@dlt.expect("category_not_null", "product_category_en IS NOT NULL")
def gold_revenue_by_category_month():
    orders_revenue = dlt.read(SILVER_ORDERS_REVENUE_TABLE)
    order_items_revenue = dlt.read(SILVER_ORDER_ITEMS_REVENUE_TABLE)
    return build_gold_revenue_by_category_month(orders_revenue, order_items_revenue)


# -------------------------------------------------------------------
# GOLD 3: REVENUE BY CUSTOMER STATE & MONTH
# Grain: month x customer_state
# -------------------------------------------------------------------
@dlt.table(
    name="gold_revenue_by_state_month",
    comment="Monthly revenue by customer state with share-of-month, AOV and rank",
)
@dlt.expect("customer_state_present", "customer_state IS NOT NULL")
def gold_revenue_by_state_month():
    orders_revenue = dlt.read(SILVER_ORDERS_REVENUE_TABLE)
    return build_gold_revenue_by_state_month(orders_revenue)


# -------------------------------------------------------------------
# GOLD 4: REVENUE BY PAYMENT TYPE & MONTH
# Grain: month x primary_payment_type
# Showcases the 1-to-many payments handling done in silver.
# -------------------------------------------------------------------
@dlt.table(
    name="gold_revenue_by_payment_type_month",
    comment="Monthly revenue split by primary payment type with share-of-month",
)
@dlt.expect("payment_type_present", "primary_payment_type IS NOT NULL")
def gold_revenue_by_payment_type_month():
    orders_revenue = dlt.read(SILVER_ORDERS_REVENUE_TABLE)
    return build_gold_revenue_by_payment_type_month(orders_revenue)


# -------------------------------------------------------------------
# GOLD 5: EXECUTIVE SUMMARY
# Grain: one row; refreshed KPI snapshot for dashboards and slides
# -------------------------------------------------------------------
@dlt.table(
    name="gold_revenue_exec_summary",
    comment="Single-row executive summary of revenue KPIs (GMV, AOV, YoY, top dims)",
)
def gold_revenue_exec_summary():
    orders_revenue = dlt.read(SILVER_ORDERS_REVENUE_TABLE)
    order_items_revenue = dlt.read(SILVER_ORDER_ITEMS_REVENUE_TABLE)

    revenue_monthly = build_gold_revenue_monthly(orders_revenue)
    revenue_by_category_month = build_gold_revenue_by_category_month(
        orders_revenue, order_items_revenue
    )
    revenue_by_state_month = build_gold_revenue_by_state_month(orders_revenue)
    revenue_by_payment_type_month = build_gold_revenue_by_payment_type_month(
        orders_revenue
    )

    return build_gold_revenue_exec_summary(
        orders_revenue,
        revenue_monthly,
        revenue_by_category_month,
        revenue_by_state_month,
        revenue_by_payment_type_month,
    )

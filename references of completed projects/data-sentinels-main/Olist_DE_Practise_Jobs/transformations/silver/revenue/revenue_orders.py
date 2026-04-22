"""
/// Summary : Silver orders-revenue fact for Use Case 1. Order-grain
///           table joining orders, item rollup, payment rollup and
///           customer geography. The inner join with items guarantees
///           that "ghost" payments (pointing to non-existent orders)
///           cannot reach the Gold layer - a spec requirement.
/// Data    : raw_orders, order_items_revenue_silver,
///           payments_order_agg_silver, customer_geo_silver
"""

import dlt
import os
import sys

# Shared pure-logic module lives in `utilities/` (used by silver and gold).
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
    aggregate_items_per_order,
    build_orders_revenue,
)


spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA silver")


BRONZE_ORDERS_TABLE = "data_sentinals.bronze.raw_orders"
# Produced by 01_silver_dimensions.py in the same pipeline target schema.
# Read via dlt.read so DLT tracks the dependency in the pipeline DAG
# (matches the pattern used by 02_silver_delivery.py).
CUSTOMER_GEO_TABLE = "customer_geo_silver"


@dlt.table(
    name="orders_revenue_silver",
    comment="Order-grain revenue fact: items + payments + customer geo joined onto raw_orders",
)
@dlt.expect_or_fail("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("revenue_total_non_negative", "revenue_total >= 0")
@dlt.expect(
    "payment_reconciles_within_5_pct",
    "abs(payment_reconciliation_delta) / greatest(revenue_total, 1) < 0.05",
)
@dlt.expect(
    "valid_order_status",
    """
    order_status IN (
        'created','approved','invoiced','processing',
        'shipped','delivered','unavailable','canceled'
    )
    """,
)
def orders_revenue_silver():
    orders = dlt.read(BRONZE_ORDERS_TABLE)
    items_revenue = dlt.read("order_items_revenue_silver")
    payments_agg = dlt.read("payments_order_agg_silver")
    customer_geo = dlt.read(CUSTOMER_GEO_TABLE)

    items_agg = aggregate_items_per_order(items_revenue)
    return build_orders_revenue(orders, items_agg, payments_agg, customer_geo)

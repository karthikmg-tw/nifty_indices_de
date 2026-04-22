"""
/// Summary : Silver line-item revenue for Use Case 1. One row per
///           order line item, enriched with English category and the
///           derived `item_total_revenue = price + freight_value`.
/// Data    : raw_order_items, product_catalog_silver
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

from revenue_logic import build_order_items_revenue  # noqa: E402


spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA silver")


BRONZE_ORDER_ITEMS_TABLE = "data_sentinals.bronze.raw_order_items"


@dlt.table(
    name="order_items_revenue_silver",
    comment="Line-item revenue with English category and item_total_revenue = price + freight",
)
@dlt.expect_or_fail("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("price_non_negative", "price >= 0")
@dlt.expect_or_drop("freight_non_negative", "freight_value >= 0")
@dlt.expect("category_populated", "product_category_en IS NOT NULL")
def order_items_revenue_silver():
    items = dlt.read(BRONZE_ORDER_ITEMS_TABLE)
    catalog = dlt.read("product_catalog_silver")
    return build_order_items_revenue(items, catalog)

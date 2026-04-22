"""
/// Summary : Silver tables for Use Case 1 (Revenue & Growth Analytics).
///           Cleanses `raw_order_payments`, enforces the spec's
///           "no negative payments" rule, and collapses the 1-to-many
///           payment relationship into a single row per order.
/// Data    : raw_order_payments
"""

import dlt
import os
import sys

# Make the shared pure-logic module importable in the DLT runtime.
# `revenue_logic.py` lives in `utilities/` because it is consumed by
# both silver and gold files. We use `os.getcwd()` rather than
# `__file__` because the latter is not guaranteed to be defined in
# every DLT execution context - same pattern as `data_ingestion.py`.
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
    clean_payments,
    aggregate_payments_per_order,
)


spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA silver")


BRONZE_ORDER_PAYMENTS_TABLE = "data_sentinals.bronze.raw_order_payments"


# -------------------------------------------------------------------
# 1) PAYMENTS CLEAN SILVER
# Grain: one row per (order_id, payment_sequential)
# Purpose:
# - Type-cast and normalise payment_type
# - Enforce no-negative-payments rule via @dlt.expect_or_drop
# -------------------------------------------------------------------
@dlt.table(
    name="payments_clean_silver",
    comment="Cleansed order_payments with no negative values and normalised payment_type",
)
@dlt.expect_or_fail("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_drop("payment_value_non_negative", "payment_value >= 0")
@dlt.expect(
    "valid_payment_type",
    "payment_type IN ('credit_card','boleto','voucher','debit_card','not_defined')",
)
@dlt.expect("installments_non_negative", "payment_installments >= 0")
def payments_clean_silver():
    raw = dlt.read(BRONZE_ORDER_PAYMENTS_TABLE)
    return clean_payments(raw)


# -------------------------------------------------------------------
# 2) PAYMENTS ORDER AGGREGATE SILVER
# Grain: one row per order_id
# Purpose:
# - Collapse the 1-to-many payment rows into a single order row
# - Derive primary_payment_type (payment type with max value for the order)
# - Provide total_payment_value for reconciliation against item revenue
# -------------------------------------------------------------------
@dlt.table(
    name="payments_order_agg_silver",
    comment="Order-grain payment rollup: total value, payment count, primary payment type",
)
@dlt.expect_or_fail("order_id_not_null", "order_id IS NOT NULL")
@dlt.expect_or_fail("total_payment_non_negative", "total_payment_value >= 0")
def payments_order_agg_silver():
    payments_clean = dlt.read("payments_clean_silver")
    return aggregate_payments_per_order(payments_clean)

"""
/// Summary : Unit tests for Use Case 1 (Revenue & Growth Analytics).
///           Exercises the pure PySpark functions in
///           `utilities/revenue_logic.py` against
///           small synthetic DataFrames so behaviour is verified
///           without any DLT or Databricks dependency.
"""

from datetime import datetime, date

import pytest
from pyspark.sql import functions as F

from revenue_logic import (
    aggregate_items_per_order,
    aggregate_payments_per_order,
    build_gold_revenue_by_category_month,
    build_gold_revenue_by_payment_type_month,
    build_gold_revenue_by_state_month,
    build_gold_revenue_exec_summary,
    build_gold_revenue_monthly,
    build_order_items_revenue,
    build_orders_revenue,
    build_product_catalog,
    clean_payments,
)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _rows_to_map(df, key_cols):
    """Collect a DataFrame and index rows by a tuple of key columns."""
    result = {}
    for row in df.collect():
        if isinstance(key_cols, str):
            key = row[key_cols]
        else:
            key = tuple(row[c] for c in key_cols)
        result[key] = row.asDict()
    return result


def _ts(y, m, d):
    return datetime(y, m, d, 12, 0, 0)


# ---------------------------------------------------------------------------
# clean_payments
# ---------------------------------------------------------------------------


def test_clean_payments_drops_negative_values(spark):
    df = spark.createDataFrame(
        [
            ("o1", 1, "credit_card", 3, 100.0),
            ("o1", 2, "voucher", 1, -20.0),
            ("o2", 1, "boleto", 1, 50.0),
        ],
        "order_id string, payment_sequential int, payment_type string, "
        "payment_installments int, payment_value double",
    )

    out = clean_payments(df).collect()

    assert len(out) == 2
    assert all(row["payment_value"] >= 0 for row in out)


def test_clean_payments_lowercases_payment_type(spark):
    df = spark.createDataFrame(
        [("o1", 1, "Credit_Card", 1, 10.0)],
        "order_id string, payment_sequential int, payment_type string, "
        "payment_installments int, payment_value double",
    )

    row = clean_payments(df).collect()[0]

    assert row["payment_type"] == "credit_card"


# ---------------------------------------------------------------------------
# aggregate_payments_per_order
# ---------------------------------------------------------------------------


def test_payments_order_agg_collapses_multi_payment_orders(spark):
    """Two payment rows for the same order become a single row with
    summed value, correct primary_payment_type, and sorted type mix."""
    df = spark.createDataFrame(
        [
            ("o1", 1, "credit_card", 3, 80.0),
            ("o1", 2, "voucher", 1, 20.0),
            ("o2", 1, "boleto", 1, 50.0),
        ],
        "order_id string, payment_sequential int, payment_type string, "
        "payment_installments int, payment_value double",
    )

    result = _rows_to_map(aggregate_payments_per_order(df), "order_id")

    assert result["o1"]["total_payment_value"] == pytest.approx(100.0)
    assert result["o1"]["n_payments"] == 2
    assert result["o1"]["primary_payment_type"] == "credit_card"
    assert result["o1"]["payment_type_mix"] == "credit_card,voucher"
    assert result["o1"]["installments_max"] == 3

    assert result["o2"]["total_payment_value"] == pytest.approx(50.0)
    assert result["o2"]["n_payments"] == 1
    assert result["o2"]["primary_payment_type"] == "boleto"


def test_payments_order_agg_picks_highest_value_as_primary(spark):
    df = spark.createDataFrame(
        [
            ("o1", 1, "voucher", 1, 30.0),
            ("o1", 2, "credit_card", 5, 70.0),
            ("o1", 3, "boleto", 1, 10.0),
        ],
        "order_id string, payment_sequential int, payment_type string, "
        "payment_installments int, payment_value double",
    )

    row = aggregate_payments_per_order(df).collect()[0]

    assert row["primary_payment_type"] == "credit_card"


# ---------------------------------------------------------------------------
# build_product_catalog
# ---------------------------------------------------------------------------


def test_product_catalog_translates_category(spark):
    products = spark.createDataFrame(
        [
            ("p1", "beleza_saude", 100, 10, 10, 10),
            ("p2", "unknown_category", 100, 10, 10, 10),
            ("p3", None, 100, 10, 10, 10),
        ],
        "product_id string, product_category_name string, product_weight_g int, "
        "product_length_cm int, product_height_cm int, product_width_cm int",
    )
    translation = spark.createDataFrame(
        [("beleza_saude", "health_beauty")],
        "product_category_name string, product_category_name_english string",
    )

    result = _rows_to_map(build_product_catalog(products, translation), "product_id")

    assert result["p1"]["product_category_en"] == "health_beauty"
    assert result["p2"]["product_category_en"] == "unknown_category"
    assert result["p3"]["product_category_en"] == "unknown"


# ---------------------------------------------------------------------------
# build_order_items_revenue + aggregate_items_per_order
# ---------------------------------------------------------------------------


def test_order_items_revenue_computes_item_total(spark):
    items = spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", 80.0, 20.0),
            ("o1", 2, "p2", "s1", 40.0, 10.0),
        ],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "price double, freight_value double",
    )
    catalog = spark.createDataFrame(
        [("p1", "health_beauty"), ("p2", "electronics")],
        "product_id string, product_category_en string",
    )

    enriched = build_order_items_revenue(items, catalog).collect()

    totals = {r["order_item_id"]: r["item_total_revenue"] for r in enriched}
    categories = {r["order_item_id"]: r["product_category_en"] for r in enriched}

    assert totals == {1: 100.0, 2: 50.0}
    assert categories == {1: "health_beauty", 2: "electronics"}


def test_aggregate_items_per_order_sums_all_components(spark):
    items_revenue = spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", "health_beauty", 80.0, 20.0, 100.0),
            ("o1", 2, "p2", "s1", "electronics", 40.0, 10.0, 50.0),
        ],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "product_category_en string, price double, freight_value double, "
        "item_total_revenue double",
    )

    row = aggregate_items_per_order(items_revenue).collect()[0]

    assert row["n_items"] == 2
    assert row["gross_revenue"] == pytest.approx(120.0)
    assert row["freight_total"] == pytest.approx(30.0)
    assert row["revenue_total"] == pytest.approx(150.0)


# ---------------------------------------------------------------------------
# build_orders_revenue (ghost-order defence)
# ---------------------------------------------------------------------------


def _orders_df(spark, rows):
    return spark.createDataFrame(
        rows,
        "order_id string, customer_id string, order_status string, "
        "order_purchase_timestamp timestamp",
    )


def _items_agg_df(spark, rows):
    return spark.createDataFrame(
        rows,
        "order_id string, n_items bigint, gross_revenue double, "
        "freight_total double, revenue_total double",
    )


def _payments_agg_df(spark, rows):
    return spark.createDataFrame(
        rows,
        "order_id string, total_payment_value double, n_payments bigint, "
        "installments_max int, payment_type_mix string, primary_payment_type string",
    )


def test_orders_revenue_drops_ghost_payments(spark):
    """A payment referencing an order that does not exist must not appear
    in `orders_revenue` (ghost-order defence from the spec)."""
    orders = _orders_df(
        spark,
        [
            ("o1", "c1", "delivered", _ts(2023, 1, 15)),
        ],
    )
    items_agg = _items_agg_df(
        spark,
        [
            ("o1", 1, 100.0, 10.0, 110.0),
        ],
    )
    payments_agg = _payments_agg_df(
        spark,
        [
            ("o1", 110.0, 1, 1, "credit_card", "credit_card"),
            ("o_ghost", 50.0, 1, 1, "voucher", "voucher"),
        ],
    )

    result = build_orders_revenue(orders, items_agg, payments_agg, customer_geo=None)

    order_ids = [r["order_id"] for r in result.collect()]
    assert order_ids == ["o1"]


def test_orders_revenue_marks_canceled_orders_not_recognised(spark):
    orders = _orders_df(
        spark,
        [
            ("o1", "c1", "delivered", _ts(2023, 1, 15)),
            ("o2", "c2", "canceled", _ts(2023, 1, 16)),
            ("o3", "c3", "unavailable", _ts(2023, 1, 17)),
        ],
    )
    items_agg = _items_agg_df(
        spark,
        [
            ("o1", 1, 100.0, 10.0, 110.0),
            ("o2", 1, 50.0, 5.0, 55.0),
            ("o3", 1, 20.0, 2.0, 22.0),
        ],
    )
    payments_agg = _payments_agg_df(spark, [])

    result = _rows_to_map(
        build_orders_revenue(orders, items_agg, payments_agg, customer_geo=None),
        "order_id",
    )

    assert result["o1"]["is_revenue_recognised"] == 1
    assert result["o2"]["is_revenue_recognised"] == 0
    assert result["o3"]["is_revenue_recognised"] == 0


def test_orders_revenue_computes_reconciliation_delta(spark):
    orders = _orders_df(
        spark, [("o1", "c1", "delivered", _ts(2023, 1, 15))]
    )
    items_agg = _items_agg_df(spark, [("o1", 1, 100.0, 10.0, 110.0)])
    payments_agg = _payments_agg_df(
        spark,
        [("o1", 105.0, 1, 1, "credit_card", "credit_card")],
    )

    row = build_orders_revenue(
        orders, items_agg, payments_agg, customer_geo=None
    ).collect()[0]

    assert row["payment_reconciliation_delta"] == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# gold_revenue_monthly (MoM / YoY / rolling)
# ---------------------------------------------------------------------------


def _orders_revenue_for_monthly(spark, monthly_map):
    """Given {(year, month): revenue} produce a minimal orders_revenue DF."""
    rows = []
    i = 0
    for (y, m), revenue in monthly_map.items():
        i += 1
        rows.append(
            (
                f"o{i}",
                f"c{i}",
                "delivered",
                _ts(y, m, 15),
                date(y, m, 15),
                date(y, m, 1),
                y,
                1,
                revenue,
                0.0,
                revenue,
                None,
                None,
                None,
                None,
                None,
                None,
                0.0,
                1,
            )
        )
    return spark.createDataFrame(
        rows,
        "order_id string, customer_id string, order_status string, "
        "order_purchase_ts timestamp, order_purchase_date date, "
        "order_purchase_month date, order_purchase_year int, "
        "n_items bigint, gross_revenue double, freight_total double, "
        "revenue_total double, total_payment_value double, n_payments bigint, "
        "installments_max int, payment_type_mix string, "
        "primary_payment_type string, customer_unique_id string, "
        "payment_reconciliation_delta double, is_revenue_recognised int",
    )


def test_monthly_revenue_mom_and_yoy(spark):
    orders_revenue = _orders_revenue_for_monthly(
        spark,
        {
            (2022, 1): 100.0,
            (2022, 2): 200.0,
            (2023, 1): 150.0,
            (2023, 2): 400.0,
        },
    )

    result = _rows_to_map(
        build_gold_revenue_monthly(orders_revenue), "order_purchase_month"
    )

    jan22 = result[date(2022, 1, 1)]
    feb22 = result[date(2022, 2, 1)]
    jan23 = result[date(2023, 1, 1)]
    feb23 = result[date(2023, 2, 1)]

    assert jan22["revenue_mom_pct"] is None
    assert feb22["revenue_mom_pct"] == pytest.approx(100.0)
    assert jan23["revenue_yoy_pct"] == pytest.approx(50.0)
    assert feb23["revenue_yoy_pct"] == pytest.approx(100.0)


def test_monthly_revenue_rolling_3m(spark):
    orders_revenue = _orders_revenue_for_monthly(
        spark,
        {
            (2023, 1): 100.0,
            (2023, 2): 200.0,
            (2023, 3): 300.0,
            (2023, 4): 400.0,
        },
    )

    result = _rows_to_map(
        build_gold_revenue_monthly(orders_revenue), "order_purchase_month"
    )

    assert result[date(2023, 1, 1)]["revenue_rolling_3m"] == pytest.approx(100.0)
    assert result[date(2023, 2, 1)]["revenue_rolling_3m"] == pytest.approx(300.0)
    assert result[date(2023, 3, 1)]["revenue_rolling_3m"] == pytest.approx(600.0)
    assert result[date(2023, 4, 1)]["revenue_rolling_3m"] == pytest.approx(900.0)


# ---------------------------------------------------------------------------
# gold_revenue_by_category_month
# ---------------------------------------------------------------------------


def test_category_revenue_share_sums_to_one_per_month(spark):
    orders_revenue = spark.createDataFrame(
        [
            ("o1", date(2023, 1, 1), 1),
            ("o2", date(2023, 1, 1), 1),
            ("o3", date(2023, 2, 1), 1),
        ],
        "order_id string, order_purchase_month date, is_revenue_recognised int",
    )
    order_items_revenue = spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", "electronics", 100.0, 10.0, 110.0),
            ("o2", 1, "p2", "s1", "beauty", 40.0, 10.0, 50.0),
            ("o3", 1, "p3", "s2", "electronics", 30.0, 20.0, 50.0),
        ],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "product_category_en string, price double, freight_value double, "
        "item_total_revenue double",
    )

    result = build_gold_revenue_by_category_month(orders_revenue, order_items_revenue)

    shares = (
        result.groupBy("order_purchase_month")
        .agg(F.sum("revenue_share_of_month").alias("total_share"))
        .collect()
    )
    for row in shares:
        assert row["total_share"] == pytest.approx(1.0)


def test_category_revenue_rank_by_revenue(spark):
    orders_revenue = spark.createDataFrame(
        [("o1", date(2023, 1, 1), 1), ("o2", date(2023, 1, 1), 1)],
        "order_id string, order_purchase_month date, is_revenue_recognised int",
    )
    order_items_revenue = spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", "electronics", 100.0, 10.0, 110.0),
            ("o2", 1, "p2", "s1", "beauty", 20.0, 5.0, 25.0),
        ],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "product_category_en string, price double, freight_value double, "
        "item_total_revenue double",
    )

    result = _rows_to_map(
        build_gold_revenue_by_category_month(orders_revenue, order_items_revenue),
        ("order_purchase_month", "product_category_en"),
    )

    assert result[(date(2023, 1, 1), "electronics")]["category_rank_in_month"] == 1
    assert result[(date(2023, 1, 1), "beauty")]["category_rank_in_month"] == 2


# ---------------------------------------------------------------------------
# gold_revenue_by_state_month + payment_type
# ---------------------------------------------------------------------------


def _minimal_orders_revenue(spark, rows):
    """rows: (order_id, month_date_or_ts, state, payment_type, installments, revenue, cust_unique_id)"""
    data = [
        (
            r[0],
            "c" + r[0],
            "delivered",
            _ts(r[1].year, r[1].month, 10),
            date(r[1].year, r[1].month, 10),
            date(r[1].year, r[1].month, 1),
            r[1].year,
            1,
            r[5],
            0.0,
            r[5],
            r[5],
            1,
            r[4],
            None,
            r[3],
            r[6],
            r[2],
            None,
            0.0,
            1,
        )
        for r in rows
    ]
    return spark.createDataFrame(
        data,
        "order_id string, customer_id string, order_status string, "
        "order_purchase_ts timestamp, order_purchase_date date, "
        "order_purchase_month date, order_purchase_year int, "
        "n_items bigint, gross_revenue double, freight_total double, "
        "revenue_total double, total_payment_value double, n_payments bigint, "
        "installments_max int, payment_type_mix string, "
        "primary_payment_type string, customer_unique_id string, "
        "customer_state string, customer_city_std string, "
        "payment_reconciliation_delta double, is_revenue_recognised int",
    )


def test_by_state_month_ranks_and_shares(spark):
    orders_revenue = _minimal_orders_revenue(
        spark,
        [
            ("o1", datetime(2023, 1, 1), "SP", "credit_card", 1, 600.0, "u1"),
            ("o2", datetime(2023, 1, 1), "RJ", "credit_card", 1, 400.0, "u2"),
        ],
    )

    result = _rows_to_map(
        build_gold_revenue_by_state_month(orders_revenue),
        ("order_purchase_month", "customer_state"),
    )

    sp = result[(date(2023, 1, 1), "SP")]
    rj = result[(date(2023, 1, 1), "RJ")]
    assert sp["state_rank_in_month"] == 1
    assert rj["state_rank_in_month"] == 2
    assert sp["revenue_share_of_month"] == pytest.approx(0.6)
    assert rj["revenue_share_of_month"] == pytest.approx(0.4)


def test_by_payment_type_month_share_sums_to_one(spark):
    orders_revenue = _minimal_orders_revenue(
        spark,
        [
            ("o1", datetime(2023, 1, 1), "SP", "credit_card", 3, 700.0, "u1"),
            ("o2", datetime(2023, 1, 1), "RJ", "boleto", 1, 300.0, "u2"),
        ],
    )

    result = build_gold_revenue_by_payment_type_month(orders_revenue)

    shares = (
        result.groupBy("order_purchase_month")
        .agg(F.sum("share_of_month_revenue").alias("total_share"))
        .collect()
    )
    for row in shares:
        assert row["total_share"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# exec summary
# ---------------------------------------------------------------------------


def test_exec_summary_single_row_and_populated(spark):
    orders_revenue = _minimal_orders_revenue(
        spark,
        [
            ("o1", datetime(2023, 1, 1), "SP", "credit_card", 3, 700.0, "u1"),
            ("o2", datetime(2023, 2, 1), "RJ", "boleto", 1, 300.0, "u2"),
        ],
    )
    order_items_revenue = spark.createDataFrame(
        [
            ("o1", 1, "p1", "s1", "electronics", 650.0, 50.0, 700.0),
            ("o2", 1, "p2", "s1", "beauty", 280.0, 20.0, 300.0),
        ],
        "order_id string, order_item_id int, product_id string, seller_id string, "
        "product_category_en string, price double, freight_value double, "
        "item_total_revenue double",
    )

    revenue_monthly = build_gold_revenue_monthly(orders_revenue)
    revenue_by_cat = build_gold_revenue_by_category_month(
        orders_revenue, order_items_revenue
    )
    revenue_by_state = build_gold_revenue_by_state_month(orders_revenue)
    revenue_by_pay = build_gold_revenue_by_payment_type_month(orders_revenue)

    summary = build_gold_revenue_exec_summary(
        orders_revenue,
        revenue_monthly,
        revenue_by_cat,
        revenue_by_state,
        revenue_by_pay,
    ).collect()

    assert len(summary) == 1
    row = summary[0]
    assert row["total_orders"] == 2
    assert row["total_revenue"] == pytest.approx(1000.0)
    assert row["top_category"] == "electronics"
    assert row["top_customer_state"] == "SP"
    assert row["top_payment_type"] == "credit_card"
    assert row["kpi_generated_ts"] is not None

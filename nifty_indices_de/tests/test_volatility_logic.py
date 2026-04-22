"""
Unit tests for utilities/volatility_logic.py.
"""
from datetime import date

import pytest
from pyspark.sql import functions as F

from volatility_logic import (
    compute_high_low_spread,
    compute_true_range,
    build_rolling_volatility,
    build_drawdown,
    classify_vix_regime,
    build_volatility_exec_summary,
    build_volatility_monthly_summary,
)


_BASE_SCHEMA = (
    "trade_date DATE, index_name STRING, index_category STRING, "
    "close DOUBLE, high DOUBLE, low DOUBLE, open DOUBLE, "
    "prev_close DOUBLE, daily_return_pct DOUBLE"
)


def _make_df(spark, rows):
    return spark.createDataFrame(rows, _BASE_SCHEMA)


# ---------------------------------------------------------------------------
# compute_high_low_spread
# ---------------------------------------------------------------------------


def test_high_low_spread_formula(spark):
    df = _make_df(spark, [
        (date(2023, 1, 4), "NIFTY 50", "broad", 103.0, 110.0, 90.0, 100.0, 100.0, 3.0),
    ])
    row = compute_high_low_spread(df).collect()[0]
    # spread = (110 - 90) / 100 * 100 = 20.0%
    assert row["high_low_spread_pct"] == pytest.approx(20.0)


def test_high_low_spread_null_when_prev_close_null(spark):
    df = _make_df(spark, [
        (date(2023, 1, 3), "NIFTY 50", "broad", 100.0, 105.0, 95.0, 98.0, None, None),
    ])
    row = compute_high_low_spread(df).collect()[0]
    assert row["high_low_spread_pct"] is None


# ---------------------------------------------------------------------------
# compute_true_range
# ---------------------------------------------------------------------------


def test_true_range_uses_prior_close_for_gap(spark):
    # Overnight gap-up: previous close 100, today opens gap-up at 110
    # high=115, low=108, prev_close=100
    # TR = max(115-108=7, |115-100|=15, |108-100|=8) = 15
    df = _make_df(spark, [
        (date(2023, 1, 4), "NIFTY 50", "broad", 112.0, 115.0, 108.0, 110.0, 100.0, 5.0),
    ])
    row = compute_true_range(df).collect()[0]
    assert row["true_range"] == pytest.approx(15.0)


def test_true_range_null_when_no_prev_close(spark):
    df = _make_df(spark, [
        (date(2023, 1, 3), "NIFTY 50", "broad", 100.0, 105.0, 95.0, 100.0, None, None),
    ])
    row = compute_true_range(df).collect()[0]
    assert row["true_range"] is None


# ---------------------------------------------------------------------------
# build_rolling_volatility
# ---------------------------------------------------------------------------


def test_rolling_vol_zero_for_constant_returns(spark):
    schema = (
        "trade_date DATE, index_name STRING, index_category STRING, "
        "close DOUBLE, high DOUBLE, low DOUBLE, open DOUBLE, "
        "prev_close DOUBLE, daily_return_pct DOUBLE"
    )
    rows = [
        (date(2023, 1, d), "NIFTY 50", "broad", 100.0 + d, 101.0 + d, 99.0 + d, 100.0 + d, 100.0 + d - 1, 1.0)
        for d in range(3, 3 + 25)
    ]
    df = spark.createDataFrame(rows, schema)
    out = build_rolling_volatility(df, windows=[21])
    last = out.orderBy("trade_date").collect()[-1]
    assert last["rolling_vol_21d"] == pytest.approx(0.0, abs=1e-9)


def test_rolling_vol_independent_per_index(spark):
    rows_50 = [
        (date(2023, 1, d), "NIFTY 50", "broad", 100.0, 101.0, 99.0, 100.0, 100.0, float(d % 3))
        for d in range(3, 28)
    ]
    rows_bank = [
        (date(2023, 1, d), "NIFTY BANK", "sector", 500.0, 510.0, 490.0, 500.0, 500.0, float((d % 5) * 2))
        for d in range(3, 28)
    ]
    df = spark.createDataFrame(rows_50 + rows_bank, _BASE_SCHEMA)
    out = build_rolling_volatility(df, windows=[21]).filter("index_name = 'NIFTY 50'")
    # If partitioning works, vol for NIFTY 50 should differ from NIFTY BANK
    # (different return series); just assert it is not null
    last = out.orderBy("trade_date").collect()[-1]
    assert last["rolling_vol_21d"] is not None


# ---------------------------------------------------------------------------
# build_drawdown
# ---------------------------------------------------------------------------


def test_drawdown_correct(spark):
    rows = [
        (date(2023, 1, d), "NIFTY 50", "broad", float(100 - abs(d - 15)), 101.0, 99.0, 100.0, 100.0, 0.5)
        for d in range(3, 28)
    ]
    df = spark.createDataFrame(rows, _BASE_SCHEMA)
    out = build_drawdown(df).filter("index_name = 'NIFTY 50'")
    rows_out = out.orderBy("trade_date").collect()
    for row in rows_out:
        assert row["drawdown_pct"] is None or row["drawdown_pct"] <= 0.0


def test_drawdown_zero_at_new_high(spark):
    # Monotonically increasing prices: drawdown should always be 0
    rows = [
        (date(2023, 1, d), "NIFTY 50", "broad", float(100 + d), float(101 + d), float(99 + d), float(100 + d), float(99 + d), 1.0)
        for d in range(3, 28)
    ]
    df = spark.createDataFrame(rows, _BASE_SCHEMA)
    out = build_drawdown(df)
    last = out.orderBy("trade_date").collect()[-1]
    assert last["drawdown_pct"] == pytest.approx(0.0, abs=1e-9)


# ---------------------------------------------------------------------------
# classify_vix_regime
# ---------------------------------------------------------------------------


def test_classify_vix_regime_boundaries(spark):
    schema = _BASE_SCHEMA + ", vix_close DOUBLE"
    rows = [
        (date(2023, 1, 3), "NIFTY 50", "broad", 100.0, 105.0, 95.0, 100.0, 99.0, 1.0, 14.9),
        (date(2023, 1, 4), "NIFTY 50", "broad", 101.0, 106.0, 96.0, 100.0, 100.0, 1.0, 15.0),
        (date(2023, 1, 5), "NIFTY 50", "broad", 102.0, 107.0, 97.0, 101.0, 101.0, 1.0, 25.0),
        (date(2023, 1, 6), "NIFTY 50", "broad", 103.0, 108.0, 98.0, 102.0, 102.0, 1.0, 40.1),
    ]
    df = spark.createDataFrame(rows, schema)
    rows_out = {r["trade_date"]: r for r in classify_vix_regime(df).collect()}
    assert rows_out[date(2023, 1, 3)]["vix_regime"] == "low"
    assert rows_out[date(2023, 1, 4)]["vix_regime"] == "moderate"
    assert rows_out[date(2023, 1, 5)]["vix_regime"] == "elevated"
    assert rows_out[date(2023, 1, 6)]["vix_regime"] == "extreme"


def test_classify_vix_regime_null_vix(spark):
    schema = _BASE_SCHEMA + ", vix_close DOUBLE"
    df = spark.createDataFrame([
        (date(2023, 1, 3), "NIFTY 50", "broad", 100.0, 105.0, 95.0, 100.0, 99.0, 1.0, None),
    ], schema)
    row = classify_vix_regime(df).collect()[0]
    assert row["vix_regime"] is None


# ---------------------------------------------------------------------------
# build_volatility_exec_summary
# ---------------------------------------------------------------------------


def test_volatility_exec_summary_single_row(spark):
    schema = (
        "trade_date DATE, index_name STRING, index_category STRING, "
        "close DOUBLE, high DOUBLE, low DOUBLE, open DOUBLE, "
        "prev_close DOUBLE, daily_return_pct DOUBLE, "
        "high_low_spread_pct DOUBLE, true_range DOUBLE, "
        "rolling_vol_21d DOUBLE, rolling_vol_63d DOUBLE, "
        "drawdown_pct DOUBLE, rolling_52wk_high DOUBLE, "
        "vix_close DOUBLE, vix_regime STRING"
    )
    rows = [
        (date(2023, 1, 3), "NIFTY 50", "broad", 100.0, 105.0, 95.0, 100.0, 99.0, 1.0,
         10.0, 10.0, 1.2, 1.1, -2.0, 102.0, 18.0, "moderate"),
        (date(2023, 1, 3), "NIFTY BANK", "sector", 500.0, 510.0, 490.0, 500.0, 499.0, 0.5,
         8.0, 9.0, 1.5, 1.4, -3.0, 515.0, 18.0, "moderate"),
    ]
    df = spark.createDataFrame(rows, schema)
    monthly = build_volatility_monthly_summary(df)
    summary = build_volatility_exec_summary(df, monthly)
    assert summary.count() == 1
    assert summary.collect()[0]["kpi_generated_ts"] is not None

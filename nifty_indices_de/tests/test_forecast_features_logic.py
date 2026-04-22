"""
Unit tests for utilities/forecast_features_logic.py.
"""
import math
from datetime import date

import pytest
from pyspark.sql import functions as F

from forecast_features_logic import (
    build_lag_features,
    build_rolling_mean_features,
    build_seasonality_features,
    build_return_distribution_stats,
)


_DAILY_SCHEMA = (
    "trade_date DATE, index_name STRING, index_category STRING, "
    "close DOUBLE, daily_return_pct DOUBLE, "
    "trade_year INT, trade_month INT, trade_quarter INT, "
    "trade_week INT, day_of_week INT, "
    "is_month_end BOOLEAN, is_quarter_end BOOLEAN"
)


def _make_daily(spark, rows):
    return spark.createDataFrame(rows, _DAILY_SCHEMA)


# ---------------------------------------------------------------------------
# build_lag_features
# ---------------------------------------------------------------------------


def test_lag_features_correct_offset(spark):
    rows = [
        (date(2023, 1, 3), "NIFTY 50", "broad", 100.0, 1.0, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 4), "NIFTY 50", "broad", 101.0, 2.0, 2023, 1, 1, 1, 4, False, False),
        (date(2023, 1, 5), "NIFTY 50", "broad", 103.0, 3.0, 2023, 1, 1, 1, 5, False, False),
        (date(2023, 1, 6), "NIFTY 50", "broad", 105.0, 4.0, 2023, 1, 1, 1, 6, False, False),
    ]
    df = _make_daily(spark, rows)
    out = {r["trade_date"]: r for r in build_lag_features(df, lags=[1]).collect()}

    assert out[date(2023, 1, 3)]["lag_return_1d"] is None
    assert out[date(2023, 1, 4)]["lag_return_1d"] == pytest.approx(1.0)
    assert out[date(2023, 1, 5)]["lag_return_1d"] == pytest.approx(2.0)
    assert out[date(2023, 1, 6)]["lag_return_1d"] == pytest.approx(3.0)


def test_lag_features_no_cross_index_bleed(spark):
    rows = [
        (date(2023, 1, 3), "NIFTY 50",   "broad",  100.0, 5.0, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 4), "NIFTY 50",   "broad",  105.0, 3.0, 2023, 1, 1, 1, 4, False, False),
        (date(2023, 1, 3), "NIFTY BANK", "sector", 500.0, 1.0, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 4), "NIFTY BANK", "sector", 502.0, 2.0, 2023, 1, 1, 1, 4, False, False),
    ]
    df = _make_daily(spark, rows)
    out = {(r["index_name"], r["trade_date"]): r for r in build_lag_features(df, lags=[1]).collect()}

    # First row for each index must have null lag
    assert out[("NIFTY 50",   date(2023, 1, 3))]["lag_return_1d"] is None
    assert out[("NIFTY BANK", date(2023, 1, 3))]["lag_return_1d"] is None
    # Second row must reference its own index's prior return
    assert out[("NIFTY 50",   date(2023, 1, 4))]["lag_return_1d"] == pytest.approx(5.0)
    assert out[("NIFTY BANK", date(2023, 1, 4))]["lag_return_1d"] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# build_rolling_mean_features
# ---------------------------------------------------------------------------


def test_rolling_mean_5d_correct(spark):
    closes = [100.0, 102.0, 104.0, 106.0, 110.0]
    rows = [
        (date(2023, 1, d + 3), "NIFTY 50", "broad", c, 1.0, 2023, 1, 1, 1, 3, False, False)
        for d, c in enumerate(closes)
    ]
    df = _make_daily(spark, rows)
    out = build_rolling_mean_features(df, windows=[5]).orderBy("trade_date").collect()
    last = out[-1]
    expected_mean = sum(closes) / 5
    assert last["rolling_mean_5d"] == pytest.approx(expected_mean)


def test_rolling_mean_independent_per_index(spark):
    rows_50 = [
        (date(2023, 1, d), "NIFTY 50",   "broad",  float(100 + d), 1.0, 2023, 1, 1, 1, 3, False, False)
        for d in range(3, 10)
    ]
    rows_bank = [
        (date(2023, 1, d), "NIFTY BANK", "sector", float(500 + d * 2), 1.0, 2023, 1, 1, 1, 3, False, False)
        for d in range(3, 10)
    ]
    df = _make_daily(spark, rows_50 + rows_bank)
    out = {(r["index_name"], r["trade_date"]): r for r in build_rolling_mean_features(df, windows=[5]).collect()}
    # NIFTY 50 and NIFTY BANK should have different rolling means
    d = date(2023, 1, 7)
    assert out[("NIFTY 50", d)]["rolling_mean_5d"] != pytest.approx(out[("NIFTY BANK", d)]["rolling_mean_5d"])


# ---------------------------------------------------------------------------
# build_seasonality_features
# ---------------------------------------------------------------------------


def test_seasonality_sin_cos_range(spark):
    rows = [
        (date(2023, m, 15), "NIFTY 50", "broad", 100.0, 1.0, 2023, m, (m - 1) // 3 + 1, 1, 3, False, False)
        for m in range(1, 13)
    ]
    df = _make_daily(spark, rows)
    for row in build_seasonality_features(df).collect():
        assert -1.0 <= row["month_sin"] <= 1.0
        assert -1.0 <= row["month_cos"] <= 1.0
        assert -1.0 <= row["dow_sin"] <= 1.0
        assert -1.0 <= row["dow_cos"] <= 1.0


def test_seasonality_december_close_to_january(spark):
    """Cyclical encoding: Dec (month=12) and Jan (month=1) should be close."""
    rows = [
        (date(2023, 1,  15), "NIFTY 50", "broad", 100.0, 1.0, 2023, 1,  1, 1, 3, False, False),
        (date(2023, 12, 15), "NIFTY 50", "broad", 110.0, 1.0, 2023, 12, 4, 1, 5, False, False),
    ]
    df = _make_daily(spark, rows)
    out = {r["trade_month"]: r for r in build_seasonality_features(df).collect()}

    jan_sin = out[1]["month_sin"]
    dec_sin = out[12]["month_sin"]
    # sin(2π*1/12) and sin(2π*12/12)=sin(2π)≈0 vs sin(π/6)≈0.5 — but both should be
    # close to each other compared to sin for month=6 (sin(π)≈0)
    # Simpler test: cos values for month 1 and 12 should be close
    jan_cos = out[1]["month_cos"]
    dec_cos = out[12]["month_cos"]
    # cos(2π*1/12) ≈ 0.866, cos(2π*12/12) = cos(2π) = 1.0 — reasonably close
    # vs cos(2π*6/12) = cos(π) = -1.0 which is very different
    mid_year_cos = math.cos(2 * math.pi * 6 / 12)
    assert abs(jan_cos - dec_cos) < abs(jan_cos - mid_year_cos)


# ---------------------------------------------------------------------------
# build_return_distribution_stats
# ---------------------------------------------------------------------------


def test_return_distribution_not_null(spark):
    rows = [
        (date(2023, 1, d), "NIFTY 50", "broad", float(100 + d), float(d % 3 - 1), 2023, 1, 1, 1, 3, False, False)
        for d in range(3, 28)
    ]
    df = _make_daily(spark, rows)
    out = build_return_distribution_stats(df)
    row = out.filter("index_name = 'NIFTY 50'").collect()[0]
    assert row["mean_daily_return"] is not None
    assert row["stddev_daily_return"] is not None
    assert row["skewness_daily_return"] is not None


def test_sharpe_proxy_formula(spark):
    """With a constant return of 0.1% and stddev approaching 0, proxy -> infinity.
    Use a known series: 20 rows with returns 1.0 and 20 rows with returns -1.0 (mean=0, stddev>0)."""
    rows = (
        [(date(2023, 1, d), "NIFTY 50", "broad", 100.0, 1.0, 2023, 1, 1, 1, 3, False, False) for d in range(3, 23)]
        + [(date(2023, 1, d), "NIFTY 50", "broad", 100.0, -1.0, 2023, 1, 1, 1, 3, False, False) for d in range(23, 43)]
    )
    df = _make_daily(spark, rows)
    row = build_return_distribution_stats(df).collect()[0]
    assert row["sharpe_proxy"] is not None
    # mean ≈ 0, so sharpe_proxy ≈ 0
    assert abs(row["sharpe_proxy"]) < 0.1


def test_return_distribution_percentiles_ordered(spark):
    rows = [
        (date(2023, 1, d), "NIFTY 50", "broad", 100.0, float(d - 15), 2023, 1, 1, 1, 3, False, False)
        for d in range(3, 33)
    ]
    df = _make_daily(spark, rows)
    row = build_return_distribution_stats(df).collect()[0]
    assert row["percentile_5th"] <= row["percentile_25th"]
    assert row["percentile_25th"] <= row["percentile_75th"]
    assert row["percentile_75th"] <= row["percentile_95th"]

"""
Unit tests for utilities/sector_rotation_logic.py.
"""
from datetime import date

import pytest
from pyspark.sql import functions as F

from sector_rotation_logic import (
    build_sector_monthly_returns,
    join_benchmark_returns,
    rank_sectors_by_month,
    compute_rolling_relative_strength,
    classify_momentum_signal,
    build_leaders_laggards,
    build_sector_exec_summary,
)


_DAILY_SCHEMA = (
    "trade_date DATE, index_name STRING, index_category STRING, "
    "close DOUBLE, trade_year INT, trade_month INT"
)


def _make_daily(spark, rows):
    return spark.createDataFrame(rows, _DAILY_SCHEMA)


def _row_map(df, key_cols):
    result = {}
    for row in df.collect():
        if isinstance(key_cols, str):
            key = row[key_cols]
        else:
            key = tuple(row[c] for c in key_cols)
        result[key] = row.asDict()
    return result


# ---------------------------------------------------------------------------
# build_sector_monthly_returns
# ---------------------------------------------------------------------------


def test_monthly_returns_open_close(spark):
    rows = [
        (date(2023, 1, 3),  "NIFTY BANK", "sector", 500.0, 2023, 1),
        (date(2023, 1, 16), "NIFTY BANK", "sector", 520.0, 2023, 1),
        (date(2023, 1, 31), "NIFTY BANK", "sector", 550.0, 2023, 1),
    ]
    df = _make_daily(spark, rows)
    out = _row_map(build_sector_monthly_returns(df), ("index_name", "trade_year", "trade_month"))
    key = ("NIFTY BANK", 2023, 1)
    assert out[key]["month_open"] == pytest.approx(500.0)
    assert out[key]["month_close"] == pytest.approx(550.0)
    assert out[key]["monthly_return_pct"] == pytest.approx(10.0)


def test_monthly_returns_single_day_month(spark):
    rows = [
        (date(2023, 1, 31), "NIFTY BANK", "sector", 500.0, 2023, 1),
    ]
    df = _make_daily(spark, rows)
    out = _row_map(build_sector_monthly_returns(df), ("index_name", "trade_year", "trade_month"))
    key = ("NIFTY BANK", 2023, 1)
    assert out[key]["monthly_return_pct"] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# join_benchmark_returns
# ---------------------------------------------------------------------------


def test_join_benchmark_relative_strength(spark):
    rows = [
        (date(2023, 1, 3),  "NIFTY 50",   "broad",  100.0, 2023, 1),
        (date(2023, 1, 31), "NIFTY 50",   "broad",  103.0, 2023, 1),
        (date(2023, 1, 3),  "NIFTY BANK", "sector", 500.0, 2023, 1),
        (date(2023, 1, 31), "NIFTY BANK", "sector", 525.0, 2023, 1),
    ]
    df = _make_daily(spark, rows)
    monthly = build_sector_monthly_returns(df)
    with_rs = join_benchmark_returns(monthly)
    out = _row_map(with_rs, ("index_name", "trade_year", "trade_month"))

    nifty50_return = out[("NIFTY 50", 2023, 1)]["monthly_return_pct"]
    bank_return = out[("NIFTY BANK", 2023, 1)]["monthly_return_pct"]
    bank_rs = out[("NIFTY BANK", 2023, 1)]["relative_strength"]

    assert bank_rs == pytest.approx(bank_return - nifty50_return)


# ---------------------------------------------------------------------------
# rank_sectors_by_month
# ---------------------------------------------------------------------------


def test_rank_sectors_by_month_ordering(spark):
    rows = [
        (date(2023, 1, 3),  "NIFTY AUTO",  "sector", 100.0, 2023, 1),
        (date(2023, 1, 31), "NIFTY AUTO",  "sector", 112.0, 2023, 1),  # +12%
        (date(2023, 1, 3),  "NIFTY BANK",  "sector", 500.0, 2023, 1),
        (date(2023, 1, 31), "NIFTY BANK",  "sector", 505.0, 2023, 1),  # +1%
        (date(2023, 1, 3),  "NIFTY FMCG",  "sector", 200.0, 2023, 1),
        (date(2023, 1, 31), "NIFTY FMCG",  "sector", 196.0, 2023, 1),  # -2%
        (date(2023, 1, 3),  "NIFTY 50",    "broad",  1000.0, 2023, 1),
        (date(2023, 1, 31), "NIFTY 50",    "broad",  1050.0, 2023, 1),
    ]
    df = _make_daily(spark, rows)
    monthly = build_sector_monthly_returns(df)
    with_rs = join_benchmark_returns(monthly)
    ranked = rank_sectors_by_month(with_rs)
    out = _row_map(ranked, ("index_name", "trade_year", "trade_month"))

    assert out[("NIFTY AUTO", 2023, 1)]["sector_rank_in_month"] < out[("NIFTY BANK", 2023, 1)]["sector_rank_in_month"]
    assert out[("NIFTY BANK", 2023, 1)]["sector_rank_in_month"] < out[("NIFTY FMCG", 2023, 1)]["sector_rank_in_month"]


# ---------------------------------------------------------------------------
# classify_momentum_signal
# ---------------------------------------------------------------------------


_MONTHLY_SCHEMA = (
    "index_name STRING, index_category STRING, trade_year INT, trade_month INT, "
    "trade_month_date DATE, monthly_return_pct DOUBLE, relative_strength DOUBLE, "
    "sector_rank_in_month INT, rolling_rs_3m DOUBLE"
)


def test_classify_momentum_all_quadrants(spark):
    rows = [
        ("NIFTY AUTO",  "sector", 2023, 1, date(2023, 1, 1), 5.0,  2.0,  1, 1.5),   # leading
        ("NIFTY BANK",  "sector", 2023, 1, date(2023, 1, 1), -1.0, 0.5,  3, 0.3),   # deteriorating
        ("NIFTY FMCG",  "sector", 2023, 1, date(2023, 1, 1), 3.0,  -1.5, 2, -0.8),  # recovering
        ("NIFTY METAL", "sector", 2023, 1, date(2023, 1, 1), -2.0, -0.8, 4, -1.2),  # lagging
    ]
    df = spark.createDataFrame(rows, _MONTHLY_SCHEMA)
    out = _row_map(classify_momentum_signal(df), "index_name")

    assert out["NIFTY AUTO"]["momentum_signal"] == "leading"
    assert out["NIFTY BANK"]["momentum_signal"] == "deteriorating"
    assert out["NIFTY FMCG"]["momentum_signal"] == "recovering"
    assert out["NIFTY METAL"]["momentum_signal"] == "lagging"


# ---------------------------------------------------------------------------
# build_leaders_laggards
# ---------------------------------------------------------------------------


def test_build_leaders_laggards_correct_count(spark):
    rows = [
        ("NIFTY AUTO",   "sector", 2023, 1, date(2023, 1, 1), 12.0, 7.0,  1, 5.0),
        ("NIFTY BANK",   "sector", 2023, 1, date(2023, 1, 1),  8.0, 3.0,  2, 2.0),
        ("NIFTY FMCG",   "sector", 2023, 1, date(2023, 1, 1),  5.0, 0.5,  3, 0.3),
        ("NIFTY IT",     "sector", 2023, 1, date(2023, 1, 1),  2.0, -1.0, 4, -0.5),
        ("NIFTY METAL",  "sector", 2023, 1, date(2023, 1, 1), -1.0, -4.0, 5, -2.0),
        ("NIFTY PHARMA", "sector", 2023, 1, date(2023, 1, 1), -5.0, -9.0, 6, -6.0),
    ]
    df = spark.createDataFrame(rows, _MONTHLY_SCHEMA)
    result = build_leaders_laggards(df, n=3)

    positions = [r["position_label"] for r in result.collect()]
    assert "leader_1" in positions
    assert "leader_3" in positions
    assert "laggard_1" in positions
    assert "laggard_3" in positions
    assert result.count() == 6


def test_build_leaders_laggards_leader_has_best_rank(spark):
    rows = [
        ("NIFTY AUTO",   "sector", 2023, 1, date(2023, 1, 1), 12.0, 7.0, 1, 5.0),
        ("NIFTY BANK",   "sector", 2023, 1, date(2023, 1, 1),  5.0, 2.0, 2, 2.0),
        ("NIFTY PHARMA", "sector", 2023, 1, date(2023, 1, 1), -3.0, -2.0, 3, -1.0),
    ]
    df = spark.createDataFrame(rows, _MONTHLY_SCHEMA)
    result = build_leaders_laggards(df, n=1)
    out = {r["position_label"]: r for r in result.collect()}
    assert out["leader_1"]["index_name"] == "NIFTY AUTO"
    assert out["laggard_1"]["index_name"] == "NIFTY PHARMA"


# ---------------------------------------------------------------------------
# build_sector_exec_summary
# ---------------------------------------------------------------------------


def test_sector_exec_summary_single_row(spark):
    rows = [
        ("NIFTY AUTO",   "sector", 2023, 1, date(2023, 1, 1), 12.0, 7.0, 1, 5.0),
        ("NIFTY BANK",   "sector", 2023, 1, date(2023, 1, 1),  5.0, 2.0, 2, 2.0),
        ("NIFTY PHARMA", "sector", 2023, 1, date(2023, 1, 1), -3.0, -2.0, 3, -1.0),
    ]
    df = spark.createDataFrame(rows, _MONTHLY_SCHEMA)
    leaders_laggards = build_leaders_laggards(df, n=1)
    summary = build_sector_exec_summary(df, leaders_laggards)

    assert summary.count() == 1
    assert summary.collect()[0]["kpi_generated_ts"] is not None

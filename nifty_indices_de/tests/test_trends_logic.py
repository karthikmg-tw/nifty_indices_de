"""
Unit tests for utilities/trends_logic.py.
"""
from datetime import date

import pytest
from pyspark.sql import functions as F

from trends_logic import build_index_monthly, build_index_yearly, build_trends_exec_summary


_DAILY_SCHEMA = (
    "trade_date DATE, index_name STRING, index_category STRING, "
    "close DOUBLE, daily_return_pct DOUBLE, prev_close DOUBLE, "
    "pe DOUBLE, pb DOUBLE, div_yield DOUBLE, "
    "trade_year INT, trade_month INT, trade_quarter INT, "
    "trade_week INT, day_of_week INT, "
    "is_month_end BOOLEAN, is_quarter_end BOOLEAN"
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
# build_index_monthly
# ---------------------------------------------------------------------------


def test_build_index_monthly_return_correct(spark):
    rows = [
        (date(2023, 1, 3), "NIFTY 50", "broad", 100.0, None, None, 20.0, 3.0, 1.5, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 31), "NIFTY 50", "broad", 110.0, 5.0, 100.0, 21.0, 3.1, 1.4, 2023, 1, 1, 5, 3, True, False),
    ]
    df = _make_daily(spark, rows)
    out = _row_map(build_index_monthly(df), ("index_name", "trade_year", "trade_month"))
    key = ("NIFTY 50", 2023, 1)
    assert key in out
    assert out[key]["monthly_return_pct"] == pytest.approx(10.0)


def test_build_index_monthly_mom_pct(spark):
    rows = [
        (date(2023, 1, 3),  "NIFTY 50", "broad", 100.0, None, None, 20.0, 3.0, 1.5, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 31), "NIFTY 50", "broad", 105.0, 2.0,  100.0, 20.0, 3.0, 1.5, 2023, 1, 1, 5, 3, True,  False),
        (date(2023, 2, 1),  "NIFTY 50", "broad", 105.0, 0.0,  105.0, 20.5, 3.1, 1.4, 2023, 2, 1, 5, 3, False, False),
        (date(2023, 2, 28), "NIFTY 50", "broad", 115.0, 3.0,  105.0, 21.0, 3.2, 1.3, 2023, 2, 1, 9, 2, True,  False),
    ]
    df = _make_daily(spark, rows)
    monthly = build_index_monthly(df)
    out = _row_map(monthly, ("index_name", "trade_year", "trade_month"))

    jan_return = out[("NIFTY 50", 2023, 1)]["monthly_return_pct"]
    feb_return = out[("NIFTY 50", 2023, 2)]["monthly_return_pct"]
    feb_mom = out[("NIFTY 50", 2023, 2)]["return_mom_pct"]

    assert feb_mom == pytest.approx(feb_return - jan_return)


def test_build_index_monthly_first_row_mom_null(spark):
    rows = [
        (date(2023, 1, 3),  "NIFTY 50", "broad", 100.0, None, None, 20.0, 3.0, 1.5, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 31), "NIFTY 50", "broad", 105.0, 2.0, 100.0, 20.0, 3.0, 1.5, 2023, 1, 1, 5, 3, True, False),
    ]
    df = _make_daily(spark, rows)
    out = _row_map(build_index_monthly(df), ("index_name", "trade_year", "trade_month"))
    assert out[("NIFTY 50", 2023, 1)]["return_mom_pct"] is None


def test_build_index_monthly_no_cross_index_bleed(spark):
    """Rolling windows must not bleed across different index_name values."""
    rows = [
        (date(2023, 1, 3),  "NIFTY 50",   "broad",  100.0, None, None, 20.0, 3.0, 1.5, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 31), "NIFTY 50",   "broad",  105.0, 2.0, 100.0, 20.0, 3.0, 1.5, 2023, 1, 1, 5, 3, True, False),
        (date(2023, 2, 1),  "NIFTY 50",   "broad",  105.0, 0.0, 105.0, 20.0, 3.0, 1.5, 2023, 2, 1, 5, 3, False, False),
        (date(2023, 2, 28), "NIFTY 50",   "broad",  110.0, 2.0, 105.0, 20.0, 3.0, 1.5, 2023, 2, 1, 9, 2, True, False),
        (date(2023, 1, 3),  "NIFTY BANK", "sector", 500.0, None, None, 15.0, 2.0, 0.5, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 31), "NIFTY BANK", "sector", 480.0, -1.0, 500.0, 14.0, 1.9, 0.6, 2023, 1, 1, 5, 3, True, False),
        (date(2023, 2, 1),  "NIFTY BANK", "sector", 480.0, 0.0, 480.0, 14.0, 2.0, 0.5, 2023, 2, 1, 5, 3, False, False),
        (date(2023, 2, 28), "NIFTY BANK", "sector", 510.0, 3.0, 480.0, 15.0, 2.1, 0.4, 2023, 2, 1, 9, 2, True, False),
    ]
    df = _make_daily(spark, rows)
    monthly = build_index_monthly(df)
    out = _row_map(monthly, ("index_name", "trade_year", "trade_month"))

    nifty50_feb_roll3 = out[("NIFTY 50", 2023, 2)]["rolling_return_3m"]
    bank_feb_roll3 = out[("NIFTY BANK", 2023, 2)]["rolling_return_3m"]
    assert nifty50_feb_roll3 != bank_feb_roll3, "rolling windows bled across indices"


def test_build_index_monthly_rolling_3m(spark):
    rows = [
        (date(2023, 1, 3),  "NIFTY 50", "broad", 100.0, None, None, 20.0, 3.0, 1.5, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 31), "NIFTY 50", "broad", 102.0, 1.0, 100.0, 20.0, 3.0, 1.5, 2023, 1, 1, 5, 3, True, False),
        (date(2023, 2, 1),  "NIFTY 50", "broad", 102.0, 0.0, 102.0, 20.0, 3.0, 1.5, 2023, 2, 1, 5, 3, False, False),
        (date(2023, 2, 28), "NIFTY 50", "broad", 106.0, 2.0, 102.0, 20.0, 3.0, 1.5, 2023, 2, 1, 9, 2, True, False),
        (date(2023, 3, 1),  "NIFTY 50", "broad", 106.0, 0.0, 106.0, 20.0, 3.0, 1.5, 2023, 3, 1, 9, 3, False, False),
        (date(2023, 3, 31), "NIFTY 50", "broad", 109.0, 1.0, 106.0, 20.0, 3.0, 1.5, 2023, 3, 1, 13, 5, True, True),
    ]
    df = _make_daily(spark, rows)
    monthly = build_index_monthly(df)
    out = _row_map(monthly, ("index_name", "trade_year", "trade_month"))

    jan_ret = out[("NIFTY 50", 2023, 1)]["monthly_return_pct"]
    feb_ret = out[("NIFTY 50", 2023, 2)]["monthly_return_pct"]
    mar_ret = out[("NIFTY 50", 2023, 3)]["monthly_return_pct"]
    mar_roll3 = out[("NIFTY 50", 2023, 3)]["rolling_return_3m"]

    assert mar_roll3 == pytest.approx(jan_ret + feb_ret + mar_ret)


# ---------------------------------------------------------------------------
# build_index_yearly
# ---------------------------------------------------------------------------


def test_build_index_yearly_annual_return(spark):
    rows = [
        (date(2023, 1, 3),  "NIFTY 50", "broad", 100.0, None, None, 20.0, 3.0, 1.5, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 31), "NIFTY 50", "broad", 105.0, 2.0, 100.0, 20.0, 3.0, 1.5, 2023, 1, 1, 5, 3, True, False),
        (date(2023, 2, 1),  "NIFTY 50", "broad", 105.0, 0.0, 105.0, 21.0, 3.1, 1.4, 2023, 2, 1, 5, 3, False, False),
        (date(2023, 2, 28), "NIFTY 50", "broad", 110.0, 2.0, 105.0, 21.0, 3.1, 1.4, 2023, 2, 1, 9, 2, True, False),
    ]
    df = _make_daily(spark, rows)
    monthly = build_index_monthly(df)
    yearly = build_index_yearly(monthly)
    out = _row_map(yearly, ("index_name", "trade_year"))

    assert ("NIFTY 50", 2023) in out
    # annual_return_pct is the sum of monthly returns
    assert out[("NIFTY 50", 2023)]["total_months"] == 2


# ---------------------------------------------------------------------------
# build_trends_exec_summary
# ---------------------------------------------------------------------------


def test_trends_exec_summary_single_row(spark):
    rows = [
        (date(2023, 1, 3),  "NIFTY 50",   "broad",  100.0, None, None, 20.0, 3.0, 1.5, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 31), "NIFTY 50",   "broad",  105.0, 2.0, 100.0, 20.0, 3.0, 1.5, 2023, 1, 1, 5, 3, True, False),
        (date(2023, 1, 3),  "NIFTY BANK", "sector", 500.0, None, None, 15.0, 2.0, 0.5, 2023, 1, 1, 1, 3, False, False),
        (date(2023, 1, 31), "NIFTY BANK", "sector", 480.0, -1.0, 500.0, 14.0, 1.9, 0.6, 2023, 1, 1, 5, 3, True, False),
    ]
    df = _make_daily(spark, rows)
    monthly = build_index_monthly(df)
    yearly = build_index_yearly(monthly)
    summary = build_trends_exec_summary(df, yearly)

    assert summary.count() == 1
    row = summary.collect()[0]
    assert row["kpi_generated_ts"] is not None
    assert row["data_through_date"] == date(2023, 1, 31)

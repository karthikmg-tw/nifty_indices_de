"""
Unit tests for utilities/index_common.py.
All tests use synthetic DataFrames — no files, no DLT, no Databricks.
"""
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DoubleType, StringType, StructField, StructType, TimestampType, BooleanType,
)

from index_common import (
    normalize_index_frame,
    dedupe_by_date,
    compute_daily_returns,
    enrich_calendar_parts,
)

_INGESTION_TS = datetime(2024, 1, 1, 0, 0, 0)


def _make_group_c_df(spark, rows):
    """Helper: create a Group-C style bronze DataFrame (with Volume/Turnover)."""
    schema = (
        "Date STRING, Open DOUBLE, High DOUBLE, Low DOUBLE, Close DOUBLE, "
        "Volume DOUBLE, Turnover DOUBLE, PE DOUBLE, PB DOUBLE, DivYield DOUBLE, "
        "ingestion_ts TIMESTAMP"
    )
    return spark.createDataFrame(rows, schema)


def _make_group_b_df(spark, rows):
    """Helper: create a Group-B style bronze DataFrame (no Volume/Turnover)."""
    schema = (
        "Date STRING, Open DOUBLE, High DOUBLE, Low DOUBLE, Close DOUBLE, "
        "PE DOUBLE, PB DOUBLE, DivYield DOUBLE, ingestion_ts TIMESTAMP"
    )
    return spark.createDataFrame(rows, schema)


# ---------------------------------------------------------------------------
# normalize_index_frame
# ---------------------------------------------------------------------------


def test_normalize_adds_discriminator_columns(spark):
    df = _make_group_c_df(spark, [
        ("2023-01-03", 100.0, 105.0, 99.0, 103.0, 1000.0, 5000.0, 20.0, 3.0, 1.5, _INGESTION_TS),
    ])
    out = normalize_index_frame(df, "NIFTY 50", "broad").collect()[0]
    assert out["index_name"] == "NIFTY 50"
    assert out["index_category"] == "broad"


def test_normalize_casts_date(spark):
    df = _make_group_c_df(spark, [
        ("2023-06-15", 100.0, 105.0, 99.0, 103.0, 1000.0, 5000.0, 20.0, 3.0, 1.5, _INGESTION_TS),
    ])
    out = normalize_index_frame(df, "NIFTY BANK", "sector").collect()[0]
    assert out["trade_date"] == date(2023, 6, 15)


def test_normalize_group_b_adds_null_volume(spark):
    df = _make_group_b_df(spark, [
        ("2023-01-03", 100.0, 105.0, 99.0, 103.0, 20.0, 3.0, 1.5, _INGESTION_TS),
    ])
    out = normalize_index_frame(df, "NIFTY 50", "broad").collect()[0]
    assert out["volume"] is None
    assert out["turnover"] is None


def test_normalize_group_c_preserves_volume(spark):
    df = _make_group_c_df(spark, [
        ("2023-01-03", 100.0, 105.0, 99.0, 103.0, 9999.0, 12345.0, 20.0, 3.0, 1.5, _INGESTION_TS),
    ])
    out = normalize_index_frame(df, "NIFTY BANK", "sector").collect()[0]
    assert out["volume"] == pytest.approx(9999.0)
    assert out["turnover"] == pytest.approx(12345.0)


def test_normalize_zero_ohlc_flagged(spark):
    df = _make_group_c_df(spark, [
        ("2000-01-01", None, None, None, 1000.0, None, None, None, None, None, _INGESTION_TS),
        ("2000-01-03", 1050.0, 1060.0, 1040.0, 1055.0, 100.0, 500.0, 11.0, 1.2, 2.0, _INGESTION_TS),
    ])
    rows = {r["trade_date"]: r for r in normalize_index_frame(df, "NIFTY BANK", "sector").collect()}
    assert rows[date(2000, 1, 1)]["is_zero_ohlc"] is True
    assert rows[date(2000, 1, 3)]["is_zero_ohlc"] is False


# ---------------------------------------------------------------------------
# dedupe_by_date
# ---------------------------------------------------------------------------


def test_dedupe_keeps_higher_close(spark):
    df = _make_group_c_df(spark, [
        ("2005-04-01", None, None, None, 1000.0, None, None, None, None, None, _INGESTION_TS),
        ("2005-04-01", None, None, None, 1000.0, None, None, None, None, None, _INGESTION_TS),
        ("2005-04-04", None, None, None, 1007.5, None, None, None, None, None, _INGESTION_TS),
    ])
    normalised = normalize_index_frame(df, "NIFTY SMALLCAP 250", "size")
    deduped = dedupe_by_date(normalised)
    dates = [r["trade_date"] for r in deduped.collect()]
    assert len(dates) == len(set(dates)), "dedupe left duplicate dates"
    assert date(2005, 4, 1) in dates


def test_dedupe_no_loss_when_no_duplicates(spark):
    df = _make_group_c_df(spark, [
        ("2023-01-03", 100.0, 105.0, 99.0, 103.0, 1000.0, 5000.0, 20.0, 3.0, 1.5, _INGESTION_TS),
        ("2023-01-04", 103.0, 108.0, 101.0, 106.0, 1100.0, 5500.0, 20.5, 3.1, 1.4, _INGESTION_TS),
    ])
    normalised = normalize_index_frame(df, "NIFTY 50", "broad")
    assert dedupe_by_date(normalised).count() == 2


# ---------------------------------------------------------------------------
# compute_daily_returns
# ---------------------------------------------------------------------------


def test_daily_returns_correct_value(spark):
    schema = "trade_date DATE, index_name STRING, index_category STRING, close DOUBLE"
    df = spark.createDataFrame([
        (date(2023, 1, 3), "NIFTY 50", "broad", 100.0),
        (date(2023, 1, 4), "NIFTY 50", "broad", 105.0),
        (date(2023, 1, 5), "NIFTY 50", "broad", 110.25),
    ], schema)
    rows = {r["trade_date"]: r for r in compute_daily_returns(df).collect()}
    assert rows[date(2023, 1, 3)]["daily_return_pct"] is None
    assert rows[date(2023, 1, 4)]["daily_return_pct"] == pytest.approx(5.0)
    assert rows[date(2023, 1, 5)]["daily_return_pct"] == pytest.approx(5.0)


def test_daily_returns_no_cross_index_bleed(spark):
    """Returns must never bleed across index boundaries."""
    schema = "trade_date DATE, index_name STRING, index_category STRING, close DOUBLE"
    df = spark.createDataFrame([
        (date(2023, 1, 3), "NIFTY 50",   "broad",  100.0),
        (date(2023, 1, 3), "NIFTY BANK", "sector", 500.0),
        (date(2023, 1, 4), "NIFTY 50",   "broad",  110.0),
        (date(2023, 1, 4), "NIFTY BANK", "sector", 490.0),
    ], schema)
    rows = {(r["index_name"], r["trade_date"]): r for r in compute_daily_returns(df).collect()}
    # First row per index must have null return
    assert rows[("NIFTY 50",   date(2023, 1, 3))]["daily_return_pct"] is None
    assert rows[("NIFTY BANK", date(2023, 1, 3))]["daily_return_pct"] is None
    # Second row returns must use their own index's prior close
    assert rows[("NIFTY 50",   date(2023, 1, 4))]["daily_return_pct"] == pytest.approx(10.0)
    assert rows[("NIFTY BANK", date(2023, 1, 4))]["daily_return_pct"] == pytest.approx(-2.0)


# ---------------------------------------------------------------------------
# enrich_calendar_parts
# ---------------------------------------------------------------------------


def test_calendar_parts_quarter_end(spark):
    schema = "trade_date DATE, index_name STRING"
    df = spark.createDataFrame([
        (date(2023, 3, 31), "NIFTY 50"),
    ], schema)
    row = enrich_calendar_parts(df).collect()[0]
    assert row["trade_year"] == 2023
    assert row["trade_month"] == 3
    assert row["trade_quarter"] == 1
    assert row["is_month_end"] is True
    assert row["is_quarter_end"] is True


def test_calendar_parts_non_quarter_end(spark):
    schema = "trade_date DATE, index_name STRING"
    df = spark.createDataFrame([
        (date(2023, 1, 31), "NIFTY 50"),
    ], schema)
    row = enrich_calendar_parts(df).collect()[0]
    assert row["is_month_end"] is True
    assert row["is_quarter_end"] is False

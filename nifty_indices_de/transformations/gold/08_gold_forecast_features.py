"""
Q4 (Forecast Feature Engineering) gold tables.
Delivers ML-ready daily feature matrix and annual return distribution stats.
No model training — DE deliverable only.
"""
import dlt
import os
import sys

_cwd = os.getcwd()
if "nifty_indices_de" in _cwd:
    _idx = _cwd.rfind("nifty_indices_de")
    _utilities_dir = os.path.join(_cwd[:_idx + len("nifty_indices_de")], "utilities")
else:
    _utilities_dir = os.path.join(_cwd, "utilities")
if _utilities_dir not in sys.path:
    sys.path.insert(0, _utilities_dir)

from index_common import compute_daily_returns, enrich_calendar_parts  # noqa: E402
from volatility_logic import build_rolling_volatility, compute_high_low_spread  # noqa: E402
from forecast_features_logic import (  # noqa: E402
    build_lag_features,
    build_rolling_mean_features,
    build_seasonality_features,
    build_return_distribution_stats,
)

from pyspark.sql import functions as F

spark.sql("USE CATALOG nifty_de")
spark.sql("USE SCHEMA gold")

CATALOG = "nifty_de"
SILVER_INDEX_DAILY = f"{CATALOG}.silver.index_daily_silver"
SILVER_VIX = f"{CATALOG}.silver.vix_daily_silver"


@dlt.table(
    name="gold_forecast_features_daily",
    comment="Q4 Gold: complete ML feature matrix per (index_name, trade_date)",
    table_properties={"quality": "gold"},
)
def gold_forecast_features_daily():
    base = (
        dlt.read(SILVER_INDEX_DAILY)
        .filter("is_zero_ohlc = false")
    )

    vix = (
        dlt.read(SILVER_VIX)
        .select(F.col("trade_date"), F.col("close").alias("vix_close"))
    )

    with_returns = compute_daily_returns(base)
    with_calendar = enrich_calendar_parts(with_returns)
    with_spread = compute_high_low_spread(with_calendar)
    with_rolling_vol = build_rolling_volatility(with_spread, windows=[21, 63])
    with_lags = build_lag_features(with_rolling_vol, lags=[1, 5, 21])
    with_means = build_rolling_mean_features(with_lags, windows=[5, 21])
    with_seasonality = build_seasonality_features(with_means)

    return (
        with_seasonality
        .join(vix, on="trade_date", how="left")
        .select(
            "trade_date",
            "index_name",
            "index_category",
            "close",
            "daily_return_pct",
            "lag_return_1d",
            "lag_return_5d",
            "lag_return_21d",
            "rolling_mean_5d",
            "rolling_mean_21d",
            "rolling_vol_21d",
            "rolling_vol_63d",
            "high_low_spread_pct",
            "trade_year",
            "trade_month",
            "trade_quarter",
            "day_of_week",
            "is_month_end",
            "is_quarter_end",
            "month_sin",
            "month_cos",
            "dow_sin",
            "dow_cos",
            "vix_close",
            "pe",
            "pb",
            "div_yield",
        )
    )


@dlt.table(
    name="gold_return_distribution_stats",
    comment="Q4 Gold: annual return distribution stats per index (skew, kurtosis, Sharpe proxy)",
    table_properties={"quality": "gold"},
)
def gold_return_distribution_stats():
    base = (
        dlt.read(SILVER_INDEX_DAILY)
        .filter("is_zero_ohlc = false")
    )
    with_returns = compute_daily_returns(base)
    with_calendar = enrich_calendar_parts(with_returns)
    return build_return_distribution_stats(with_calendar)

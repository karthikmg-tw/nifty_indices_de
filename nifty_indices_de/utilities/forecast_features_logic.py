"""
Pure PySpark transformation functions for Q4 (Forecast Feature Engineering).
Delivers ML-ready feature tables — no actual model training.
DataFrame-in / DataFrame-out — no DLT or Databricks dependencies.
"""

import math

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def build_lag_features(df: DataFrame, lags: list = None) -> DataFrame:
    """
    Add lag_return_Nd columns for each lag N in `lags`.
    Each is the lagged value of daily_return_pct N trading days prior.
    CRITICAL: partitioned by index_name so lags never cross index boundaries.
    """
    if lags is None:
        lags = [1, 5, 21]

    result = df
    w = Window.partitionBy("index_name").orderBy("trade_date")
    for n in lags:
        result = result.withColumn(
            f"lag_return_{n}d",
            F.lag("daily_return_pct", n).over(w),
        )
    return result


def build_rolling_mean_features(df: DataFrame, windows: list = None) -> DataFrame:
    """
    Add rolling_mean_Nd columns for close price over N-day windows.
    CRITICAL: partitioned by index_name.
    """
    if windows is None:
        windows = [5, 21]

    result = df
    for n in windows:
        w = (
            Window
            .partitionBy("index_name")
            .orderBy("trade_date")
            .rowsBetween(-(n - 1), 0)
        )
        result = result.withColumn(
            f"rolling_mean_{n}d",
            F.avg("close").over(w),
        )
    return result


def build_seasonality_features(df: DataFrame) -> DataFrame:
    """
    Add cyclical calendar encodings so December and January are close in
    feature space (standard approach for time-series ML):

      month_sin = sin(2π * trade_month / 12)
      month_cos = cos(2π * trade_month / 12)
      dow_sin   = sin(2π * day_of_week / 5)   (Mon=1, Fri=5 for NSE)
      dow_cos   = cos(2π * day_of_week / 5)

    Also adds is_month_end and is_quarter_end boolean flags.
    Requires trade_month and day_of_week columns (from enrich_calendar_parts).
    """
    two_pi = 2.0 * math.pi
    return (
        df
        .withColumn(
            "month_sin",
            F.sin(F.lit(two_pi) * F.col("trade_month") / F.lit(12.0)),
        )
        .withColumn(
            "month_cos",
            F.cos(F.lit(two_pi) * F.col("trade_month") / F.lit(12.0)),
        )
        .withColumn(
            "dow_sin",
            F.sin(F.lit(two_pi) * F.col("day_of_week") / F.lit(5.0)),
        )
        .withColumn(
            "dow_cos",
            F.cos(F.lit(two_pi) * F.col("day_of_week") / F.lit(5.0)),
        )
    )


def build_return_distribution_stats(daily_df: DataFrame) -> DataFrame:
    """
    Aggregate to (index_name, trade_year) grain.
    Provides distributional statistics that describe return characteristics
    for each index per year — useful for seasonality and tail-risk analysis.

    sharpe_proxy = mean_daily_return / stddev_daily_return * sqrt(252)
    """
    return (
        daily_df
        .filter(F.col("daily_return_pct").isNotNull())
        .groupBy("index_name", "trade_year")
        .agg(
            F.count("*").alias("n_trading_days"),
            F.avg("daily_return_pct").alias("mean_daily_return"),
            F.stddev("daily_return_pct").alias("stddev_daily_return"),
            F.skewness("daily_return_pct").alias("skewness_daily_return"),
            F.kurtosis("daily_return_pct").alias("kurtosis_daily_return"),
            F.avg(
                F.when(F.col("daily_return_pct") > 0, F.lit(1.0)).otherwise(F.lit(0.0))
            ).alias("pct_positive_days"),
            F.max("daily_return_pct").alias("max_daily_return"),
            F.min("daily_return_pct").alias("min_daily_return"),
            F.expr("percentile_approx(daily_return_pct, 0.05)").alias("percentile_5th"),
            F.expr("percentile_approx(daily_return_pct, 0.25)").alias("percentile_25th"),
            F.expr("percentile_approx(daily_return_pct, 0.75)").alias("percentile_75th"),
            F.expr("percentile_approx(daily_return_pct, 0.95)").alias("percentile_95th"),
        )
        .withColumn(
            "sharpe_proxy",
            F.when(
                F.col("stddev_daily_return").isNotNull() & (F.col("stddev_daily_return") > 0),
                F.col("mean_daily_return") / F.col("stddev_daily_return") * F.lit(math.sqrt(252)),
            ),
        )
    )

"""
Pure PySpark transformation functions for UC2 (Volatility Pattern Analysis).
DataFrame-in / DataFrame-out — no DLT or Databricks dependencies.
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


VIX_REGIME_THRESHOLDS = {
    "low": 15.0,
    "moderate": 25.0,
    "elevated": 40.0,
}


def compute_high_low_spread(df: DataFrame) -> DataFrame:
    """
    Add high_low_spread_pct = (high - low) / prev_close * 100.
    Requires prev_close to already be present (output of compute_daily_returns).
    """
    return df.withColumn(
        "high_low_spread_pct",
        F.when(
            F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
            (F.col("high") - F.col("low")) / F.col("prev_close") * 100.0,
        ),
    )


def compute_true_range(df: DataFrame) -> DataFrame:
    """
    Add true_range = max(high-low, |high-prev_close|, |low-prev_close|).
    This is the standard ATR building block; accounts for overnight gaps.
    Requires prev_close to already be present.
    """
    return df.withColumn(
        "true_range",
        F.when(
            F.col("prev_close").isNotNull(),
            F.greatest(
                F.col("high") - F.col("low"),
                F.abs(F.col("high") - F.col("prev_close")),
                F.abs(F.col("low") - F.col("prev_close")),
            ),
        ),
    )


def build_rolling_volatility(df: DataFrame, windows: list = None) -> DataFrame:
    """
    Add rolling_vol_Nd columns for each window N in `windows`.
    Each column is the rolling standard deviation of daily_return_pct.

    CRITICAL: window is partitioned by index_name — never global.
    """
    if windows is None:
        windows = [21, 63]

    result = df
    for n in windows:
        w = (
            Window
            .partitionBy("index_name")
            .orderBy("trade_date")
            .rowsBetween(-(n - 1), 0)
        )
        result = result.withColumn(
            f"rolling_vol_{n}d",
            F.stddev("daily_return_pct").over(w),
        )
    return result


def build_drawdown(df: DataFrame) -> DataFrame:
    """
    Add rolling_52wk_high and drawdown_pct.

    rolling_52wk_high is the max close over the trailing 252 trading days
    (approximate — uses calendar-day rows, not strictly 252 trading days).
    drawdown_pct = (close - rolling_52wk_high) / rolling_52wk_high * 100
    A value of -10 means the index is 10% below its 52-week high.
    """
    w_52wk = (
        Window
        .partitionBy("index_name")
        .orderBy("trade_date")
        .rowsBetween(-251, 0)
    )
    return (
        df
        .withColumn("rolling_52wk_high", F.max("close").over(w_52wk))
        .withColumn(
            "drawdown_pct",
            F.when(
                F.col("rolling_52wk_high") > 0,
                (F.col("close") - F.col("rolling_52wk_high")) / F.col("rolling_52wk_high") * 100.0,
            ),
        )
    )


def classify_vix_regime(df: DataFrame) -> DataFrame:
    """
    Add vix_regime STRING based on vix_close value:
      < 15   -> "low"
      15-25  -> "moderate"
      25-40  -> "elevated"
      > 40   -> "extreme"
    Null vix_close (post-2021 dates where VIX data ends) produces null regime.
    """
    return df.withColumn(
        "vix_regime",
        F.when(F.col("vix_close").isNull(), F.lit(None).cast("string"))
        .when(F.col("vix_close") < VIX_REGIME_THRESHOLDS["low"], F.lit("low"))
        .when(F.col("vix_close") < VIX_REGIME_THRESHOLDS["moderate"], F.lit("moderate"))
        .when(F.col("vix_close") < VIX_REGIME_THRESHOLDS["elevated"], F.lit("elevated"))
        .otherwise(F.lit("extreme")),
    )


def build_volatility_monthly_summary(daily_vol_df: DataFrame) -> DataFrame:
    """
    Aggregate index_volatility_silver to monthly grain per index.

    instability_flag is True when the month's return standard deviation
    exceeds the overall 90th-percentile threshold for that index.
    Uses approx_percentile to avoid collecting data.
    """
    agg = (
        daily_vol_df
        .groupBy("index_name", "index_category", F.trunc("trade_date", "MM").cast("date").alias("trade_month_date"))
        .agg(
            F.avg("high_low_spread_pct").alias("avg_high_low_spread_pct"),
            F.avg("true_range").alias("avg_true_range"),
            F.stddev("daily_return_pct").alias("monthly_vol_stdev"),
            F.sum(
                F.when(
                    F.col("daily_return_pct").isNotNull(),
                    F.col("daily_return_pct"),
                ).otherwise(F.lit(0.0))
            ).alias("monthly_return_pct"),
            F.min("drawdown_pct").alias("max_drawdown_in_month"),
            F.avg("vix_close").alias("avg_vix_in_month"),
        )
    )

    # Dominant VIX regime: most frequent regime within the month
    regime_counts = (
        daily_vol_df
        .filter(F.col("vix_regime").isNotNull())
        .groupBy(
            "index_name",
            F.trunc("trade_date", "MM").cast("date").alias("trade_month_date"),
            "vix_regime",
        )
        .agg(F.count("*").alias("_cnt"))
    )
    w_regime = Window.partitionBy("index_name", "trade_month_date").orderBy(F.desc("_cnt"))
    dominant_regime = (
        regime_counts
        .withColumn("_rn", F.row_number().over(w_regime))
        .filter(F.col("_rn") == 1)
        .select("index_name", "trade_month_date", F.col("vix_regime").alias("dominant_vix_regime"))
    )

    # Historical 90th-percentile of monthly_vol_stdev per index for instability_flag
    pct90 = (
        agg
        .groupBy("index_name")
        .agg(
            F.expr("percentile_approx(monthly_vol_stdev, 0.9)").alias("_vol_pct90")
        )
    )

    return (
        agg
        .join(dominant_regime, on=["index_name", "trade_month_date"], how="left")
        .join(pct90, on="index_name", how="left")
        .withColumn(
            "instability_flag",
            F.col("monthly_vol_stdev") > F.col("_vol_pct90"),
        )
        .drop("_vol_pct90")
    )


def build_volatility_exec_summary(
    daily_vol_df: DataFrame,
    monthly_vol_df: DataFrame,
) -> DataFrame:
    """
    Single-row executive snapshot using dummy-key broadcast pattern.
    """
    anchor_df = (
        daily_vol_df
        .agg(F.max("trade_date").alias("data_through_date"))
        .withColumn("_dummy", F.lit(1))
    )

    latest_date_df = (
        daily_vol_df
        .withColumn("_dummy", F.lit(1))
        .join(F.broadcast(anchor_df), on="_dummy", how="inner")
        .filter(F.col("trade_date") == F.col("data_through_date"))
    )

    def get_extreme_index(df: DataFrame, order_dir, col: str, alias_name: str) -> DataFrame:
        w = Window.orderBy(order_dir(col))
        return (
            df.filter(F.col(col).isNotNull())
            .withColumn("_rank", F.row_number().over(w))
            .filter(F.col("_rank") == 1)
            .select(F.col("index_name").alias(alias_name))
            .withColumn("_dummy", F.lit(1))
        )

    most_volatile = get_extreme_index(
        latest_date_df, F.desc, "rolling_vol_21d", "most_volatile_index_21d"
    )
    least_volatile = get_extreme_index(
        latest_date_df, F.asc, "rolling_vol_21d", "least_volatile_index_21d"
    )

    nifty50_vol = (
        latest_date_df.filter(F.col("index_name") == "NIFTY 50")
        .select(
            F.col("rolling_vol_21d").alias("nifty50_current_vol_21d"),
            F.col("vix_close").alias("current_vix_close"),
            F.col("vix_regime").alias("current_vix_regime"),
        )
        .withColumn("_dummy", F.lit(1))
    )

    vix_extremes = (
        daily_vol_df.filter(F.col("vix_close").isNotNull())
        .agg(F.max("vix_close").alias("highest_ever_vix"))
        .withColumn("_dummy", F.lit(1))
    )

    # Date of highest VIX
    highest_vix_date = (
        daily_vol_df.filter(F.col("vix_close").isNotNull())
        .withColumn("_dummy", F.lit(1))
        .join(F.broadcast(vix_extremes), on="_dummy", how="inner")
        .filter(F.col("vix_close") == F.col("highest_ever_vix"))
        .agg(F.min("trade_date").alias("highest_ever_vix_date"))
        .withColumn("_dummy", F.lit(1))
    )

    nifty50_drawdown = (
        daily_vol_df.filter(F.col("index_name") == "NIFTY 50")
        .agg(F.min("drawdown_pct").alias("max_nifty50_drawdown_ever"))
        .withColumn("_dummy", F.lit(1))
    )

    return (
        anchor_df
        .join(most_volatile, on="_dummy", how="left")
        .join(least_volatile, on="_dummy", how="left")
        .join(nifty50_vol, on="_dummy", how="left")
        .join(vix_extremes, on="_dummy", how="left")
        .join(highest_vix_date, on="_dummy", how="left")
        .join(nifty50_drawdown, on="_dummy", how="left")
        .drop("_dummy")
        .withColumn("kpi_generated_ts", F.current_timestamp())
    )

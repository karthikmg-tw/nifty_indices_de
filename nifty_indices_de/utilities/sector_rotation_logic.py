"""
Pure PySpark transformation functions for UC3 (Sector Rotation Analysis).
DataFrame-in / DataFrame-out — no DLT or Databricks dependencies.
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

BENCHMARK_INDEX = "NIFTY 50"


def build_sector_monthly_returns(daily_df: DataFrame) -> DataFrame:
    """
    Aggregate index_daily_silver (sector + broad indices) to monthly grain.

    month_open  = close of the first trading day of the month
    month_close = close of the last trading day of the month
    monthly_return_pct = (month_close - month_open) / month_open * 100
    """
    w_asc = Window.partitionBy("index_name", "trade_year", "trade_month").orderBy("trade_date")
    w_desc = Window.partitionBy("index_name", "trade_year", "trade_month").orderBy(F.desc("trade_date"))

    ranked = (
        daily_df
        .withColumn("_rn_asc", F.row_number().over(w_asc))
        .withColumn("_rn_desc", F.row_number().over(w_desc))
    )

    month_open = (
        ranked.filter(F.col("_rn_asc") == 1)
        .select("index_name", "trade_year", "trade_month", F.col("close").alias("month_open"))
    )
    month_close = (
        ranked.filter(F.col("_rn_desc") == 1)
        .select("index_name", "trade_year", "trade_month", F.col("close").alias("month_close"))
    )

    agg = (
        daily_df
        .groupBy("index_name", "index_category", "trade_year", "trade_month")
        .agg(
            F.trunc(F.min("trade_date"), "MM").cast("date").alias("trade_month_date"),
            F.max("close").alias("monthly_high"),
            F.min("close").alias("monthly_low"),
        )
    )

    base = (
        agg
        .join(month_open, on=["index_name", "trade_year", "trade_month"], how="left")
        .join(month_close, on=["index_name", "trade_year", "trade_month"], how="left")
        .withColumn(
            "monthly_return_pct",
            F.when(
                F.col("month_open").isNotNull() & (F.col("month_open") > 0),
                (F.col("month_close") - F.col("month_open")) / F.col("month_open") * 100.0,
            ),
        )
        .withColumn(
            "monthly_range_pct",
            F.when(
                F.col("month_open").isNotNull() & (F.col("month_open") > 0),
                (F.col("monthly_high") - F.col("monthly_low")) / F.col("month_open") * 100.0,
            ),
        )
    )

    return base


def join_benchmark_returns(
    monthly_df: DataFrame,
    benchmark_name: str = BENCHMARK_INDEX,
) -> DataFrame:
    """
    Self-join to attach the benchmark's monthly_return_pct to each sector row.
    relative_strength = sector monthly_return_pct - benchmark monthly_return_pct.
    """
    benchmark = (
        monthly_df
        .filter(F.col("index_name") == benchmark_name)
        .select(
            "trade_month_date",
            F.col("monthly_return_pct").alias("nifty50_monthly_return"),
        )
    )
    return (
        monthly_df
        .join(benchmark, on="trade_month_date", how="left")
        .withColumn(
            "relative_strength",
            F.when(
                F.col("nifty50_monthly_return").isNotNull(),
                F.col("monthly_return_pct") - F.col("nifty50_monthly_return"),
            ),
        )
    )


def rank_sectors_by_month(monthly_df: DataFrame) -> DataFrame:
    """
    Add sector_rank_in_month: rank 1 = best monthly_return_pct in that month.
    Uses dense_rank so tied indices share a rank.
    Only sector-category indices participate in the ranking.
    """
    w_rank = Window.partitionBy("trade_month_date").orderBy(F.desc("monthly_return_pct"))
    return monthly_df.withColumn("sector_rank_in_month", F.dense_rank().over(w_rank))


def compute_rolling_relative_strength(
    monthly_df: DataFrame,
    windows: list = None,
) -> DataFrame:
    """
    Add rolling_rs_Nm columns for each window N in `windows`.
    Each is the rolling average of relative_strength over N months.
    CRITICAL: partitioned by index_name, never global.
    """
    if windows is None:
        windows = [3, 6]

    result = monthly_df
    for n in windows:
        w = (
            Window
            .partitionBy("index_name")
            .orderBy("trade_month_date")
            .rowsBetween(-(n - 1), 0)
        )
        result = result.withColumn(
            f"rolling_rs_{n}m",
            F.avg("relative_strength").over(w),
        )
    return result


def classify_momentum_signal(df: DataFrame) -> DataFrame:
    """
    Add momentum_signal based on current monthly_return_pct and rolling_rs_3m:

      rs_3m > 0 AND monthly_return > 0  -> "leading"
      rs_3m > 0 AND monthly_return <= 0 -> "deteriorating"
      rs_3m <= 0 AND monthly_return > 0 -> "recovering"
      rs_3m <= 0 AND monthly_return <= 0 -> "lagging"
    """
    return df.withColumn(
        "momentum_signal",
        F.when(
            F.col("rolling_rs_3m").isNull() | F.col("monthly_return_pct").isNull(),
            F.lit(None).cast("string"),
        )
        .when(
            (F.col("rolling_rs_3m") > 0) & (F.col("monthly_return_pct") > 0),
            F.lit("leading"),
        )
        .when(
            (F.col("rolling_rs_3m") > 0) & (F.col("monthly_return_pct") <= 0),
            F.lit("deteriorating"),
        )
        .when(
            (F.col("rolling_rs_3m") <= 0) & (F.col("monthly_return_pct") > 0),
            F.lit("recovering"),
        )
        .otherwise(F.lit("lagging")),
    )


def build_leaders_laggards(monthly_df: DataFrame, n: int = 3) -> DataFrame:
    """
    For each month extract the top-N leaders and bottom-N laggards by sector_rank.
    Returns a long-format DataFrame with position_label column.
    Only sector-category indices are included.
    """
    sectors = monthly_df.filter(F.col("index_category") == "sector")

    w_asc = Window.partitionBy("trade_month_date").orderBy("sector_rank_in_month")
    w_desc = Window.partitionBy("trade_month_date").orderBy(F.desc("sector_rank_in_month"))

    ranked = (
        sectors
        .withColumn("_rn_leader", F.row_number().over(w_asc))
        .withColumn("_rn_laggard", F.row_number().over(w_desc))
    )

    leaders = (
        ranked.filter(F.col("_rn_leader") <= n)
        .withColumn("position_label", F.concat_ws("_", F.lit("leader"), F.col("_rn_leader").cast("string")))
        .select("trade_month_date", "position_label", "index_name", "monthly_return_pct", "relative_strength", "sector_rank_in_month")
    )

    laggards = (
        ranked.filter(F.col("_rn_laggard") <= n)
        .withColumn("position_label", F.concat_ws("_", F.lit("laggard"), F.col("_rn_laggard").cast("string")))
        .select("trade_month_date", "position_label", "index_name", "monthly_return_pct", "relative_strength", "sector_rank_in_month")
    )

    return leaders.union(laggards)


def build_sector_exec_summary(
    monthly_df: DataFrame,
    leaders_laggards_df: DataFrame,
) -> DataFrame:
    """
    Single-row executive snapshot using dummy-key broadcast pattern.
    """
    anchor_df = (
        monthly_df
        .agg(F.max("trade_month_date").alias("data_through_date"))
        .withColumn("_dummy", F.lit(1))
    )

    latest_month_df = (
        monthly_df
        .withColumn("_dummy", F.lit(1))
        .join(F.broadcast(anchor_df), on="_dummy", how="inner")
        .filter(F.col("trade_month_date") == F.col("data_through_date"))
        .filter(F.col("index_category") == "sector")
    )

    def get_extreme_sector(df: DataFrame, order_dir, col: str, alias_name: str) -> DataFrame:
        w = Window.orderBy(order_dir(col))
        return (
            df.filter(F.col(col).isNotNull())
            .withColumn("_rank", F.row_number().over(w))
            .filter(F.col("_rank") == 1)
            .select(F.col("index_name").alias(alias_name))
            .withColumn("_dummy", F.lit(1))
        )

    leading = get_extreme_sector(latest_month_df, F.desc, "relative_strength", "current_leading_sector")
    lagging = get_extreme_sector(latest_month_df, F.asc, "relative_strength", "current_lagging_sector")

    # Current year
    current_year_df = (
        monthly_df
        .withColumn("_dummy", F.lit(1))
        .join(F.broadcast(anchor_df), on="_dummy", how="inner")
        .filter(F.year("trade_month_date") == F.year("data_through_date"))
        .filter(F.col("index_category") == "sector")
    )

    ytd_by_sector = (
        current_year_df
        .groupBy("index_name")
        .agg(F.sum("monthly_return_pct").alias("ytd_return_pct"))
    )

    best_ytd = get_extreme_sector(ytd_by_sector, F.desc, "ytd_return_pct", "best_sector_ytd")
    worst_ytd = get_extreme_sector(ytd_by_sector, F.asc, "ytd_return_pct", "worst_sector_ytd")

    best_ytd_return = (
        ytd_by_sector
        .withColumn("_rank", F.row_number().over(Window.orderBy(F.desc("ytd_return_pct"))))
        .filter(F.col("_rank") == 1)
        .select(F.col("ytd_return_pct").alias("best_sector_ytd_return_pct"))
        .withColumn("_dummy", F.lit(1))
    )
    worst_ytd_return = (
        ytd_by_sector
        .withColumn("_rank", F.row_number().over(Window.orderBy(F.asc("ytd_return_pct"))))
        .filter(F.col("_rank") == 1)
        .select(F.col("ytd_return_pct").alias("worst_sector_ytd_return_pct"))
        .withColumn("_dummy", F.lit(1))
    )

    return (
        anchor_df
        .join(leading, on="_dummy", how="left")
        .join(lagging, on="_dummy", how="left")
        .join(best_ytd, on="_dummy", how="left")
        .join(worst_ytd, on="_dummy", how="left")
        .join(best_ytd_return, on="_dummy", how="left")
        .join(worst_ytd_return, on="_dummy", how="left")
        .drop("_dummy")
        .withColumn("kpi_generated_ts", F.current_timestamp())
    )

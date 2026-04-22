"""
Pure PySpark transformation functions for UC1 (Market Trend Analysis).
DataFrame-in / DataFrame-out — no DLT or Databricks dependencies.
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def build_index_monthly(daily_df: DataFrame) -> DataFrame:
    """
    Aggregate index_trends_silver to monthly grain per index.

    month_open  = close on the first trading day of the month
    month_close = close on the last trading day of the month
    monthly_return_pct = (month_close - month_open) / month_open * 100

    MoM / YoY use the self-join pattern: shift trade_month_date by 1 / 12
    months and join back so the diff is computed as a column.

    Rolling 3m and 6m windows are partitioned by index_name — never global.
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
            F.count("*").alias("total_trading_days"),
            F.avg("daily_return_pct").alias("avg_daily_return_pct"),
            F.avg("pe").alias("avg_pe"),
            F.avg("pb").alias("avg_pb"),
            F.avg("div_yield").alias("avg_div_yield"),
            F.trunc(F.min("trade_date"), "MM").cast("date").alias("trade_month_date"),
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
    )

    # MoM: shift current month forward by 1 month so it aligns with the next month's row
    prev_month = base.select(
        F.add_months(F.col("trade_month_date"), 1).cast("date").alias("trade_month_date"),
        F.col("index_name"),
        F.col("monthly_return_pct").alias("_prev_month_return"),
    )

    # YoY: shift current month forward by 12 months
    prev_year = base.select(
        F.add_months(F.col("trade_month_date"), 12).cast("date").alias("trade_month_date"),
        F.col("index_name"),
        F.col("monthly_return_pct").alias("_prev_year_return"),
    )

    w_roll = Window.partitionBy("index_name").orderBy("trade_month_date")
    w_roll_3m = w_roll.rowsBetween(-2, 0)
    w_roll_6m = w_roll.rowsBetween(-5, 0)

    return (
        base
        .join(prev_month, on=["trade_month_date", "index_name"], how="left")
        .join(prev_year, on=["trade_month_date", "index_name"], how="left")
        .withColumn(
            "return_mom_pct",
            F.col("monthly_return_pct") - F.col("_prev_month_return"),
        )
        .withColumn(
            "return_yoy_pct",
            F.col("monthly_return_pct") - F.col("_prev_year_return"),
        )
        .withColumn("rolling_return_3m", F.sum("monthly_return_pct").over(w_roll_3m))
        .withColumn("rolling_return_6m", F.sum("monthly_return_pct").over(w_roll_6m))
        .drop("_prev_month_return", "_prev_year_return")
    )


def build_index_yearly(monthly_df: DataFrame) -> DataFrame:
    """
    Aggregate monthly grain to yearly grain per index.

    annual_return_pct is computed as the sum of monthly returns (approximate
    chaining), which is standard for DE aggregation tables.
    return_yoy_pct compares this year's annual return to the previous year's.
    """
    agg = (
        monthly_df
        .groupBy("index_name", "index_category", "trade_year")
        .agg(
            F.count("*").alias("total_months"),
            F.sum("total_trading_days").alias("total_trading_days"),
            F.sum("monthly_return_pct").alias("annual_return_pct"),
            F.max("monthly_return_pct").alias("max_monthly_return"),
            F.min("monthly_return_pct").alias("min_monthly_return"),
            F.avg("monthly_return_pct").alias("avg_monthly_return"),
            F.avg("avg_pe").alias("avg_pe"),
            F.avg("avg_pb").alias("avg_pb"),
            F.min("trade_month_date").alias("year_start_date"),
        )
    )

    prev_year = agg.select(
        (F.col("trade_year") + 1).alias("trade_year"),
        F.col("index_name"),
        F.col("annual_return_pct").alias("_prev_annual_return"),
    )

    return (
        agg
        .join(prev_year, on=["trade_year", "index_name"], how="left")
        .withColumn("return_yoy_pct", F.col("annual_return_pct") - F.col("_prev_annual_return"))
        .drop("_prev_annual_return")
    )


def build_trends_exec_summary(
    daily_df: DataFrame,
    yearly_df: DataFrame,
) -> DataFrame:
    """
    Single-row executive KPI snapshot using the dummy-key broadcast pattern
    so it is safe to evaluate during DLT Graph Initialisation (no .collect()).
    """
    anchor_df = (
        daily_df
        .agg(F.max("trade_date").alias("data_through_date"))
        .withColumn("_dummy", F.lit(1))
    )

    current_year_df = (
        daily_df
        .withColumn("_dummy", F.lit(1))
        .join(F.broadcast(anchor_df), on="_dummy", how="inner")
        .withColumn("is_current_year", F.year("trade_date") == F.year("data_through_date"))
        .filter(F.col("is_current_year"))
    )

    # YTD return per index: (latest close - first close of year) / first close of year * 100
    w_asc = Window.partitionBy("index_name").orderBy("trade_date")
    w_desc = Window.partitionBy("index_name").orderBy(F.desc("trade_date"))

    current_year_ranked = (
        current_year_df
        .withColumn("_rn_asc", F.row_number().over(w_asc))
        .withColumn("_rn_desc", F.row_number().over(w_desc))
    )

    ytd_open = (
        current_year_ranked.filter(F.col("_rn_asc") == 1)
        .select("index_name", F.col("close").alias("_ytd_open"))
    )
    ytd_close = (
        current_year_ranked.filter(F.col("_rn_desc") == 1)
        .select("index_name", F.col("close").alias("_ytd_close"), "pe", "pb")
    )

    ytd_returns = (
        ytd_open.join(ytd_close, on="index_name", how="inner")
        .withColumn(
            "ytd_return_pct",
            F.when(
                F.col("_ytd_open") > 0,
                (F.col("_ytd_close") - F.col("_ytd_open")) / F.col("_ytd_open") * 100.0,
            ),
        )
    )

    def get_top_index(df: DataFrame, order_dir, alias_return: str, alias_name: str) -> DataFrame:
        w_rank = Window.orderBy(order_dir("ytd_return_pct"))
        return (
            df
            .withColumn("_rank", F.row_number().over(w_rank))
            .filter(F.col("_rank") == 1)
            .select(
                F.col("index_name").alias(alias_name),
                F.col("ytd_return_pct").alias(alias_return),
            )
            .withColumn("_dummy", F.lit(1))
        )

    best = get_top_index(ytd_returns, F.desc, "best_ytd_return_pct", "best_ytd_index")
    worst = get_top_index(ytd_returns, F.asc, "worst_ytd_return_pct", "worst_ytd_index")

    nifty50_latest = (
        ytd_close.filter(F.col("index_name") == "NIFTY 50")
        .select(
            F.col("pe").alias("nifty50_latest_pe"),
            F.col("pb").alias("nifty50_latest_pb"),
        )
        .withColumn("_dummy", F.lit(1))
    )

    total_indices = (
        daily_df
        .agg(F.countDistinct("index_name").alias("total_indices"))
        .withColumn("_dummy", F.lit(1))
    )

    return (
        anchor_df
        .join(total_indices, on="_dummy", how="left")
        .join(best, on="_dummy", how="left")
        .join(worst, on="_dummy", how="left")
        .join(nifty50_latest, on="_dummy", how="left")
        .drop("_dummy")
        .withColumn("kpi_generated_ts", F.current_timestamp())
    )

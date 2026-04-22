"""
Pure PySpark helpers shared across all three use cases.
DataFrame-in / DataFrame-out — no DLT or Databricks dependencies.
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window


def normalize_index_frame(
    df: DataFrame,
    index_name: str,
    index_category: str,
) -> DataFrame:
    """
    Normalise a single bronze equity-index DataFrame into the unified
    index_daily_silver schema.

    - Casts Date STRING -> trade_date DATE
    - Renames PE/PB/DivYield to snake_case (pe, pb, div_yield)
    - Adds index_name and index_category discriminator literals
    - Adds is_zero_ohlc flag (True when Open/High/Low are null or zero —
      these are inception rows not suitable for range/return calculations)
    - Group-B files (no Volume/Turnover in bronze): adds null columns so
      the union with Group-C files has a consistent schema
    """
    df = (
        df
        .withColumn("trade_date", F.to_date(F.col("Date"), "yyyy-MM-dd"))
        .withColumnRenamed("PE", "pe")
        .withColumnRenamed("PB", "pb")
        .withColumnRenamed("DivYield", "div_yield")
        .withColumnRenamed("Open", "open")
        .withColumnRenamed("High", "high")
        .withColumnRenamed("Low", "low")
        .withColumnRenamed("Close", "close")
        .withColumn("index_name", F.lit(index_name))
        .withColumn("index_category", F.lit(index_category))
        .withColumn(
            "is_zero_ohlc",
            F.col("open").isNull() | F.col("high").isNull() | F.col("low").isNull(),
        )
        .drop("Date")
    )

    if "Volume" not in df.columns:
        df = (
            df
            .withColumn("volume", F.lit(None).cast("double"))
            .withColumn("turnover", F.lit(None).cast("double"))
        )
    else:
        df = (
            df
            .withColumnRenamed("Volume", "volume")
            .withColumnRenamed("Turnover", "turnover")
        )

    return df.select(
        "trade_date",
        "index_name",
        "index_category",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "pe",
        "pb",
        "div_yield",
        "is_zero_ohlc",
        "ingestion_ts",
    )


def dedupe_by_date(df: DataFrame) -> DataFrame:
    """
    Remove duplicate (index_name, trade_date) rows, keeping the row with
    the higher close value. Handles the known NIFTY SMALLCAP 250 inception
    duplicate (2005-04-01 appears twice with the same close).
    """
    w = Window.partitionBy("index_name", "trade_date").orderBy(F.desc("close"))
    return (
        df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def compute_daily_returns(df: DataFrame) -> DataFrame:
    """
    Add prev_close and daily_return_pct columns.

    CRITICAL: the window is partitioned by index_name so that the lag
    never crosses the boundary between two different index series.
    The first row for each index produces null daily_return_pct.
    """
    w = Window.partitionBy("index_name").orderBy("trade_date")
    return (
        df
        .withColumn("prev_close", F.lag("close", 1).over(w))
        .withColumn(
            "daily_return_pct",
            F.when(
                F.col("prev_close").isNotNull() & (F.col("prev_close") > 0),
                (F.col("close") - F.col("prev_close")) / F.col("prev_close") * 100.0,
            ),
        )
    )


def enrich_calendar_parts(df: DataFrame) -> DataFrame:
    """
    Add calendar dimension columns derived from trade_date:
    trade_year, trade_month, trade_quarter, trade_week, day_of_week,
    is_month_end, is_quarter_end.
    """
    return (
        df
        .withColumn("trade_year", F.year("trade_date"))
        .withColumn("trade_month", F.month("trade_date"))
        .withColumn("trade_quarter", F.quarter("trade_date"))
        .withColumn("trade_week", F.weekofyear("trade_date"))
        .withColumn("day_of_week", F.dayofweek("trade_date"))
        .withColumn(
            "is_month_end",
            F.col("trade_date") == F.last_day("trade_date"),
        )
        .withColumn(
            "is_quarter_end",
            F.col("is_month_end") & F.col("trade_month").isin(3, 6, 9, 12),
        )
    )

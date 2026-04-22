"""
Silver table for India VIX. Kept separate from equity index silver because
VIX has a different schema (no PE/PB/Volume) and is joined into UC2 as a
dimension rather than processed as a price series.
"""
import dlt

from pyspark.sql.functions import col, to_date, current_timestamp

spark.sql("USE CATALOG nifty_de")
spark.sql("USE SCHEMA silver")

CATALOG = "nifty_de"
BRONZE_VIX_TABLE = f"{CATALOG}.bronze.raw_indiavix"


@dlt.table(
    name="vix_daily_silver",
    comment="Cleaned India VIX daily series with snake_case columns and validated close",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("trade_date_not_null", "trade_date IS NOT NULL")
@dlt.expect_or_drop("valid_close", "close > 0")
@dlt.expect("vix_reasonable_range", "close BETWEEN 8 AND 100")
def vix_daily_silver():
    return (
        dlt.read(BRONZE_VIX_TABLE)
        .withColumn("trade_date", to_date(col("Date"), "yyyy-MM-dd"))
        .withColumnRenamed("Open", "open")
        .withColumnRenamed("High", "high")
        .withColumnRenamed("Low", "low")
        .withColumnRenamed("Close", "close")
        .withColumnRenamed("Previous", "previous_close")
        .withColumnRenamed("Change", "change_abs")
        .withColumnRenamed("PctChange", "change_pct")
        .drop("Date")
        .select(
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "previous_close",
            "change_abs",
            "change_pct",
            "ingestion_ts",
        )
    )

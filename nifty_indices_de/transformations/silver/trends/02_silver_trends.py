"""
UC1 (Market Trend Analysis) silver table.
Enriches index_daily_silver with daily returns and calendar parts.
Zero-OHLC inception rows are excluded here — they have no tradeable data.
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

spark.sql("USE CATALOG nifty_de")
spark.sql("USE SCHEMA silver")

CATALOG = "nifty_de"
SILVER_INDEX_DAILY = f"{CATALOG}.silver.index_daily_silver"


@dlt.table(
    name="index_trends_silver",
    comment="UC1: daily price series enriched with returns and calendar dimensions",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("trade_date_not_null", "trade_date IS NOT NULL")
@dlt.expect_or_drop("close_positive", "close > 0")
@dlt.expect("return_range_sanity", "daily_return_pct IS NULL OR daily_return_pct BETWEEN -30 AND 30")
def index_trends_silver():
    base = (
        dlt.read(SILVER_INDEX_DAILY)
        .filter("is_zero_ohlc = false")
    )
    with_returns = compute_daily_returns(base)
    return enrich_calendar_parts(with_returns)

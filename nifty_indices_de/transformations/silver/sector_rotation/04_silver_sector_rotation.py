"""
UC3 (Sector Rotation Analysis) silver table.
Aggregates sector and broad indices to monthly grain and computes relative
strength against the NIFTY 50 benchmark.
"""
import dlt
import os
import sys

_cwd = os.getcwd()
if "nifty_indices_de" in _cwd:
    _utilities_dir = os.path.join(
        _cwd.split("nifty_indices_de")[0], "nifty_indices_de", "utilities"
    )
else:
    _utilities_dir = os.path.join(_cwd, "utilities")
if _utilities_dir not in sys.path:
    sys.path.insert(0, _utilities_dir)

from sector_rotation_logic import (  # noqa: E402
    build_sector_monthly_returns,
    join_benchmark_returns,
    rank_sectors_by_month,
)

spark.sql("USE CATALOG nifty_de")
spark.sql("USE SCHEMA silver")

CATALOG = "nifty_de"
SILVER_INDEX_DAILY = f"{CATALOG}.silver.index_daily_silver"


@dlt.table(
    name="sector_monthly_returns_silver",
    comment="UC3: monthly returns per sector/broad index with NIFTY 50 relative strength",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("trade_month_date_not_null", "trade_month_date IS NOT NULL")
@dlt.expect_or_drop("index_name_not_null", "index_name IS NOT NULL")
@dlt.expect("return_range_sanity", "monthly_return_pct IS NULL OR monthly_return_pct BETWEEN -60 AND 60")
def sector_monthly_returns_silver():
    base = (
        dlt.read(SILVER_INDEX_DAILY)
        .filter("is_zero_ohlc = false")
        .filter("index_category IN ('sector', 'broad')")
    )
    monthly = build_sector_monthly_returns(base)
    with_rs = join_benchmark_returns(monthly)
    return rank_sectors_by_month(with_rs)

"""
UC1 (Market Trend Analysis) gold tables.
Monthly trend KPIs, yearly trend KPIs, and single-row executive summary.
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

from trends_logic import (  # noqa: E402
    build_index_monthly,
    build_index_yearly,
    build_trends_exec_summary,
)

spark.sql("USE CATALOG nifty_de")
spark.sql("USE SCHEMA gold")

CATALOG = "nifty_de"
SILVER_TRENDS = f"{CATALOG}.silver.index_trends_silver"


@dlt.table(
    name="gold_index_monthly_trends",
    comment="UC1 Gold: monthly return KPIs per index with MoM, YoY and rolling 3m/6m windows",
    table_properties={"quality": "gold"},
)
@dlt.expect("monthly_return_range", "monthly_return_pct IS NULL OR monthly_return_pct BETWEEN -60 AND 60")
def gold_index_monthly_trends():
    return build_index_monthly(dlt.read(SILVER_TRENDS))


@dlt.table(
    name="gold_index_yearly_trends",
    comment="UC1 Gold: annual return KPIs per index with YoY comparison",
    table_properties={"quality": "gold"},
)
def gold_index_yearly_trends():
    monthly = build_index_monthly(dlt.read(SILVER_TRENDS))
    return build_index_yearly(monthly)


@dlt.table(
    name="gold_trends_exec_summary",
    comment="UC1 Gold: single-row executive snapshot — best/worst YTD index, NIFTY 50 PE/PB",
    table_properties={"quality": "gold"},
)
def gold_trends_exec_summary():
    daily = dlt.read(SILVER_TRENDS)
    monthly = build_index_monthly(daily)
    yearly = build_index_yearly(monthly)
    return build_trends_exec_summary(daily, yearly)

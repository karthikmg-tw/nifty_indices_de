"""
UC2 (Volatility Pattern Analysis) gold tables.
Daily rolling volatility fact, monthly volatility summary, and executive summary.
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

from volatility_logic import (  # noqa: E402
    build_volatility_monthly_summary,
    build_volatility_exec_summary,
)

spark.sql("USE CATALOG nifty_de")
spark.sql("USE SCHEMA gold")

CATALOG = "nifty_de"
SILVER_VOLATILITY = f"{CATALOG}.silver.index_volatility_silver"


@dlt.table(
    name="gold_index_rolling_volatility",
    comment="UC2 Gold: full daily fact — rolling vol, drawdown, VIX context per index per day",
    table_properties={"quality": "gold"},
)
@dlt.expect("drawdown_non_positive", "drawdown_pct IS NULL OR drawdown_pct <= 0")
def gold_index_rolling_volatility():
    return dlt.read(SILVER_VOLATILITY)


@dlt.table(
    name="gold_volatility_monthly_summary",
    comment="UC2 Gold: monthly volatility aggregation with instability flag per index",
    table_properties={"quality": "gold"},
)
def gold_volatility_monthly_summary():
    return build_volatility_monthly_summary(dlt.read(SILVER_VOLATILITY))


@dlt.table(
    name="gold_volatility_exec_summary",
    comment="UC2 Gold: single-row snapshot — most/least volatile, current VIX regime, historical extremes",
    table_properties={"quality": "gold"},
)
def gold_volatility_exec_summary():
    daily_vol = dlt.read(SILVER_VOLATILITY)
    monthly_vol = build_volatility_monthly_summary(daily_vol)
    return build_volatility_exec_summary(daily_vol, monthly_vol)

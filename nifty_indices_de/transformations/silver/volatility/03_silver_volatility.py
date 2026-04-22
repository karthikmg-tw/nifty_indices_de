"""
UC2 (Volatility Pattern Analysis) silver table.
Adds spread, true range, rolling volatility, drawdown, and VIX regime
to the daily equity series. VIX is joined on trade_date (LEFT join so
post-2021 dates produce null VIX columns, not dropped rows).
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

from index_common import compute_daily_returns  # noqa: E402
from volatility_logic import (  # noqa: E402
    compute_high_low_spread,
    compute_true_range,
    build_rolling_volatility,
    build_drawdown,
    classify_vix_regime,
)

from pyspark.sql import functions as F

spark.sql("USE CATALOG nifty_de")
spark.sql("USE SCHEMA silver")

CATALOG = "nifty_de"
SILVER_INDEX_DAILY = f"{CATALOG}.silver.index_daily_silver"
SILVER_VIX = f"{CATALOG}.silver.vix_daily_silver"


@dlt.table(
    name="index_volatility_silver",
    comment="UC2: daily volatility metrics with VIX regime per index",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("trade_date_not_null", "trade_date IS NOT NULL")
@dlt.expect_or_drop("close_positive", "close > 0")
@dlt.expect("true_range_non_negative", "true_range IS NULL OR true_range >= 0")
def index_volatility_silver():
    base = (
        dlt.read(SILVER_INDEX_DAILY)
        .filter("is_zero_ohlc = false")
    )

    vix = (
        dlt.read(SILVER_VIX)
        .select(F.col("trade_date"), F.col("close").alias("vix_close"))
    )

    with_returns = compute_daily_returns(base)
    with_spread = compute_high_low_spread(with_returns)
    with_tr = compute_true_range(with_spread)
    with_rolling_vol = build_rolling_volatility(with_tr, windows=[21, 63])
    with_drawdown = build_drawdown(with_rolling_vol)

    with_vix = with_drawdown.join(vix, on="trade_date", how="left")

    return classify_vix_regime(with_vix)

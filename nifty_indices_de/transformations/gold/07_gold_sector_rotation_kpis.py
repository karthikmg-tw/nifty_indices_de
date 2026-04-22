"""
UC3 (Sector Rotation Analysis) gold tables.
Monthly sector performance with momentum signals, leaders/laggards table,
and single-row executive summary.
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

from sector_rotation_logic import (  # noqa: E402
    compute_rolling_relative_strength,
    classify_momentum_signal,
    build_leaders_laggards,
    build_sector_exec_summary,
)

spark.sql("USE CATALOG nifty_de")
spark.sql("USE SCHEMA gold")

CATALOG = "nifty_de"
SILVER_SECTOR = f"{CATALOG}.silver.sector_monthly_returns_silver"


@dlt.table(
    name="gold_sector_monthly_performance",
    comment="UC3 Gold: monthly returns, relative strength, momentum signal per sector",
    table_properties={"quality": "gold"},
)
@dlt.expect("trade_month_date_not_null", "trade_month_date IS NOT NULL")
def gold_sector_monthly_performance():
    base = dlt.read(SILVER_SECTOR)
    with_rolling = compute_rolling_relative_strength(base, windows=[3, 6])
    return classify_momentum_signal(with_rolling)


@dlt.table(
    name="gold_sector_leaders_laggards",
    comment="UC3 Gold: top-3 leaders and bottom-3 laggard sectors per month",
    table_properties={"quality": "gold"},
)
@dlt.expect("position_label_not_null", "position_label IS NOT NULL")
def gold_sector_leaders_laggards():
    base = dlt.read(SILVER_SECTOR)
    with_rolling = compute_rolling_relative_strength(base, windows=[3, 6])
    with_signal = classify_momentum_signal(with_rolling)
    return build_leaders_laggards(with_signal, n=3)


@dlt.table(
    name="gold_sector_exec_summary",
    comment="UC3 Gold: single-row snapshot — current leader/lagger, best/worst YTD sector",
    table_properties={"quality": "gold"},
)
def gold_sector_exec_summary():
    base = dlt.read(SILVER_SECTOR)
    with_rolling = compute_rolling_relative_strength(base, windows=[3, 6])
    with_signal = classify_momentum_signal(with_rolling)
    leaders_laggards = build_leaders_laggards(with_signal, n=3)
    return build_sector_exec_summary(with_signal, leaders_laggards)

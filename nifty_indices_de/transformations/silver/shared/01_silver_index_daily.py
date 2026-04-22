"""
Unified silver table for all 13 equity indices.
Unions each bronze table through normalize_index_frame(), then deduplicates.
This is the single source of truth for UC1, UC2, and UC3 silver tables.
"""
import dlt
import os
import sys
from functools import reduce

_cwd = os.getcwd()
if "nifty_indices_de" in _cwd:
    _utilities_dir = os.path.join(
        _cwd.split("nifty_indices_de")[0], "nifty_indices_de", "utilities"
    )
else:
    _utilities_dir = os.path.join(_cwd, "utilities")
if _utilities_dir not in sys.path:
    sys.path.insert(0, _utilities_dir)

from index_common import normalize_index_frame, dedupe_by_date  # noqa: E402

spark.sql("USE CATALOG nifty_de")
spark.sql("USE SCHEMA silver")

CATALOG = "nifty_de"

# (bronze_table_name, display_name, category)
_INDEX_REGISTRY = [
    (f"{CATALOG}.bronze.raw_nifty50",        "NIFTY 50",          "broad"),
    (f"{CATALOG}.bronze.raw_niftynext50",     "NIFTY NEXT 50",     "broad"),
    (f"{CATALOG}.bronze.raw_nifty100",        "NIFTY 100",         "broad"),
    (f"{CATALOG}.bronze.raw_nifty500",        "NIFTY 500",         "broad"),
    (f"{CATALOG}.bronze.raw_niftymidcap150",  "NIFTY MIDCAP 150",  "size"),
    (f"{CATALOG}.bronze.raw_niftysmalcap250", "NIFTY SMALLCAP 250","size"),
    (f"{CATALOG}.bronze.raw_niftymicrocap250","NIFTY MICROCAP 250","size"),
    (f"{CATALOG}.bronze.raw_niftyauto",       "NIFTY AUTO",        "sector"),
    (f"{CATALOG}.bronze.raw_niftybank",       "NIFTY BANK",        "sector"),
    (f"{CATALOG}.bronze.raw_niftyfmcg",       "NIFTY FMCG",        "sector"),
    (f"{CATALOG}.bronze.raw_niftyit",         "NIFTY IT",          "sector"),
    (f"{CATALOG}.bronze.raw_niftymetal",      "NIFTY METAL",       "sector"),
    (f"{CATALOG}.bronze.raw_niftypharma",     "NIFTY PHARMA",      "sector"),
]


@dlt.table(
    name="index_daily_silver",
    comment="Unified daily series for all 13 equity indices with is_zero_ohlc flag",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("trade_date_not_null", "trade_date IS NOT NULL")
@dlt.expect_or_drop("valid_close", "close > 0")
@dlt.expect_or_drop("index_name_not_null", "index_name IS NOT NULL")
@dlt.expect("ohlc_sanity", "is_zero_ohlc OR (high >= low AND high >= close AND low <= close)")
def index_daily_silver():
    frames = [
        normalize_index_frame(dlt.read(table), name, category)
        for table, name, category in _INDEX_REGISTRY
    ]
    unioned = reduce(lambda a, b: a.union(b), frames)
    return dedupe_by_date(unioned)

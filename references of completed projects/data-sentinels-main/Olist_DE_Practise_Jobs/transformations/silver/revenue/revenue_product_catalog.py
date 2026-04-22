"""
/// Summary : Silver product catalog for Use Case 1 (Revenue & Growth
///           Analytics). Joins raw_products with the Portuguese->English
///           category translation table, coalescing missing translations
///           so that category-level revenue reporting is robust.
/// Data    : raw_products, raw_product_names_translation
"""

import dlt
import os
import sys

# Shared pure-logic module lives in `utilities/` (used by silver and gold).
_cwd = os.getcwd()
if "Olist_DE_Practise_Jobs" in _cwd:
    _utilities_dir = os.path.join(
        _cwd.split("Olist_DE_Practise_Jobs")[0],
        "Olist_DE_Practise_Jobs",
        "utilities",
    )
else:
    _utilities_dir = os.path.join(_cwd, "utilities")
if _utilities_dir not in sys.path:
    sys.path.insert(0, _utilities_dir)

from revenue_logic import build_product_catalog  # noqa: E402


spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA silver")


BRONZE_PRODUCTS_TABLE = "data_sentinals.bronze.raw_products"
BRONZE_TRANSLATION_TABLE = "data_sentinals.bronze.raw_product_names_translation"


@dlt.table(
    name="product_catalog_silver",
    comment="Product catalog with English category translation (coalesced to 'unknown' when missing)",
)
@dlt.expect_or_drop("product_id_not_null", "product_id IS NOT NULL")
@dlt.expect("category_not_null", "product_category_en IS NOT NULL")
def product_catalog_silver():
    products = dlt.read(BRONZE_PRODUCTS_TABLE)
    translation = dlt.read(BRONZE_TRANSLATION_TABLE)
    return build_product_catalog(products, translation)

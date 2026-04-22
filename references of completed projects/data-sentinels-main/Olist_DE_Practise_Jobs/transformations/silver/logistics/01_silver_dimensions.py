import dlt
from pyspark.sql import functions as F

spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA silver")



# -------------------------------------------------------------------
# CONFIG
# Update these only if your Bronze catalog/schema/table names differ.
# -------------------------------------------------------------------
BRONZE_GEO_TABLE = "data_sentinals.bronze.raw_geolocation"
BRONZE_CUSTOMERS_TABLE = "data_sentinals.bronze.raw_customers"
BRONZE_SELLERS_TABLE = "data_sentinals.bronze.raw_olist_sellers"


# Build a standardized geolocation lookup by zip prefix and state
# This avoids duplicate joins from the raw geolocation table.
@dlt.table(
    name="geo_zip_prefix_silver",
    comment="Standardized geolocation lookup"
)
@dlt.expect_or_drop("zip_not_null", "zip_code_prefix IS NOT NULL")
@dlt.expect("valid_state", "length(geo_state) = 2")
def geo_zip_prefix_silver():

    # Read raw geolocation table from Bronze
    geo = spark.read.table(BRONZE_GEO_TABLE)

    # Standardize zip, city, state, and coordinate fields
    geo_std = (
        geo.select(
            F.col("geolocation_zip_code_prefix").cast("int").alias("zip_code_prefix"),
            F.lower(F.trim("geolocation_city")).alias("geo_city"),
            F.upper(F.trim("geolocation_state")).alias("geo_state"),
            F.col("geolocation_lat").cast("double").alias("lat"),
            F.col("geolocation_lng").cast("double").alias("lng")
        )
    )

    # Aggregate to one representative row per zip prefix + state
    return (
        geo_std.groupBy("zip_code_prefix", "geo_state")
        .agg(
            F.avg("lat").alias("avg_lat"),
            F.avg("lng").alias("avg_lng"),
            F.first("geo_city", ignorenulls=True).alias("geo_city")
        )
    )


# Build customer geography dimension enriched with coordinates
@dlt.table(
    name="customer_geo_silver",
    comment="Customer dimension with geo enrichment"
)
@dlt.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
def customer_geo_silver():

    # Read Bronze customers and the Silver geo lookup
    customers = spark.read.table(BRONZE_CUSTOMERS_TABLE)
    geo = dlt.read("geo_zip_prefix_silver")

    # Standardize customer geography fields
    customers_std = (
        customers.select(
            "customer_id",
            "customer_unique_id",
            F.col("customer_zip_code_prefix").cast("int"),
            F.lower(F.trim("customer_city")).alias("customer_city_std"),
            F.upper(F.trim("customer_state")).alias("customer_state")
        )
    )

    # Enrich customers with representative latitude and longitude
    return (
        customers_std.alias("c")
        .join(
            geo.alias("g"),
            (F.col("c.customer_zip_code_prefix") == F.col("g.zip_code_prefix")) &
            (F.col("c.customer_state") == F.col("g.geo_state")),
            "left"
        )
        .select(
            "c.*",
            F.col("g.avg_lat").alias("customer_lat"),
            F.col("g.avg_lng").alias("customer_lng")
        )
    )


# Build seller geography dimension enriched with coordinates
@dlt.table(
    name="seller_geo_silver",
    comment="Seller dimension with geo enrichment"
)
@dlt.expect_or_drop("seller_id_not_null", "seller_id IS NOT NULL")
def seller_geo_silver():

    # Read Bronze sellers and the Silver geo lookup
    sellers = spark.read.table(BRONZE_SELLERS_TABLE)
    geo = dlt.read("geo_zip_prefix_silver")

    # Standardize seller geography fields
    sellers_std = (
        sellers.select(
            "seller_id",
            F.col("seller_zip_code_prefix").cast("int"),
            F.lower(F.trim("seller_city")).alias("seller_city_std"),
            F.upper(F.trim("seller_state")).alias("seller_state")
        )
    )

    # Enrich sellers with representative latitude and longitude
    return (
        sellers_std.alias("s")
        .join(
            geo.alias("g"),
            (F.col("s.seller_zip_code_prefix") == F.col("g.zip_code_prefix")) &
            (F.col("s.seller_state") == F.col("g.geo_state")),
            "left"
        )
        .select(
            "s.*",
            F.col("g.avg_lat").alias("seller_lat"),
            F.col("g.avg_lng").alias("seller_lng")
        )
    )
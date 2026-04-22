"""
Purpose: Config-driven ingestion of 14 Nifty index CSV files into bronze layer.
         Adds ingestion_ts to every table. DQ rules drop invalid rows at ingest.
         Volume path: /Volumes/nifty_de/raw/ingestion_layer/
"""
import dlt
import json
import logging
import os

from pyspark.sql.functions import current_timestamp

logger = logging.getLogger("DLTLogger")
logger.setLevel(logging.INFO)

spark.sql("USE CATALOG nifty_de")
spark.sql("USE SCHEMA bronze")

import index_common as _ic
_utilities_dir = os.path.dirname(os.path.abspath(_ic.__file__))
CONFIG_PATH = os.path.join(_utilities_dir, "ingestion_config.json")

try:
    with open(CONFIG_PATH, "r") as f:
        pipeline_config = json.load(f)
except Exception as e:
    logger.info(f"Error loading config file: {e}")
    raise RuntimeError(f"Cannot load ingestion config from {CONFIG_PATH}: {e}")

BASE_PATH = pipeline_config["base_path"]
files_to_load = pipeline_config["files"]

for file_name, config in files_to_load.items():

    def create_ingestion_table(current_file=file_name, current_config=config):

        table_name = current_config["table_name"]
        table_schema = current_config.get("schema")
        dq_rules = current_config.get("dq_rules", {})
        file_format = current_config.get("file_format")
        has_header = current_config.get("header")
        file_delimiter = current_config.get("delimiter")

        @dlt.table(
            name=table_name,
            comment=f"Raw batch ingestion for {current_file}",
            table_properties={"quality": "bronze"}
        )
        @dlt.expect_all_or_drop(dq_rules)
        def ingest_data():
            full_path = f"{BASE_PATH}{current_file}"

            df = (
                spark.read
                .format(file_format)
                .option("header", has_header)
                .option("delimiter", file_delimiter)
                .schema(table_schema)
                .load(full_path)
                .withColumn("ingestion_ts", current_timestamp())
            )

            if df.isEmpty():
                raise ValueError(f"Data Quality Failure: {current_file} contains zero records!")

            return df

    create_ingestion_table()

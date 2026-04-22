# path where the files are uploaded
# /Volumes/data_sentinals/raw/ingestion_layer

"""
Purpose: This code file will create tables for the raw data to be ingested into bronze layer.
In addition to the existing columns this transformation will add ingestion_ts as addition column.

"""
import dlt
import json
from pyspark.sql.functions import * 
import logging
import os
# Set up logging configuration
logger = logging.getLogger("DLTLogger")
logger.setLevel(logging.INFO)

# Bronze layer
spark.sql("USE SCHEMA bronze")

# current_user = spark.sql("SELECT current_user()").first()[0]
# user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
# user_name = "Olist_DE_Practise_Jobs/utilities/ingestion_config.json"
# logger.info("Current user value is ."+user_name)
# CONFIG_PATH = f"/Workspace/Users/{user_name}/data-sentinels/Olist_DE_Practise_Jobs/utilities/ingestion_config.json"

current_script_path = os.getcwd()
repo_root = current_script_path.split("Olist_DE_Practise_Jobs")[0] + "Olist_DE_Practise_Jobs"
CONFIG_PATH = os.path.join(repo_root, "utilities", "ingestion_config.json")

try:
    with open(CONFIG_PATH, "r") as f:
        pipeline_config = json.load(f)
except Exception as e:
    logger.info(f"Error loading config file: {e}")
    


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
            
            # Read the CSV and append the ingestion timestamp
            df = (
                spark.read
                .format(file_format)
                .option("header", has_header)
                .option("delimiter", file_delimiter)
                .schema(table_schema) 
                .load(full_path)
                .withColumn("ingestion_ts", current_timestamp()) # <--- Adds the timestamp column
            )
            
            if df.isEmpty():
                raise ValueError(f"Data Quality Failure: {current_file} contains zero records!")
                
            return df
            
    create_ingestion_table()
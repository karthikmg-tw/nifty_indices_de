"""
/// Summary : pytest fixtures for Use Case 1 unit tests. Provides a
///           session-scoped local SparkSession and puts the utilities
///           module on the import path so `revenue_logic` can be
///           imported without needing the Databricks DLT runtime.
"""

import os
import sys

import pytest
from pyspark.sql import SparkSession


UTILITIES_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "utilities",
    )
)
if UTILITIES_DIR not in sys.path:
    sys.path.insert(0, UTILITIES_DIR)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("data-sentinels-uc1-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

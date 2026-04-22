"""
pytest fixtures for Nifty Indices DE unit tests.
Provides a session-scoped local SparkSession and injects the utilities/
directory onto sys.path so all logic modules can be imported without
any DLT or Databricks runtime dependency.
"""
import os
import sys

import pytest
from pyspark.sql import SparkSession

UTILITIES_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "utilities")
)
if UTILITIES_DIR not in sys.path:
    sys.path.insert(0, UTILITIES_DIR)

# Java 17+ changed javax.security.auth.Subject.getSubject; re-open the
# module so Hadoop UserGroupInformation can still call it on Java 17+.
_JAVA_OPTS = " ".join([
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.util=ALL-UNNAMED",
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
])


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("nifty-de-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", _JAVA_OPTS)
        .config("spark.executor.extraJavaOptions", _JAVA_OPTS)
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

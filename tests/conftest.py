"""Pytest config."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request: pytest.FixtureRequest) -> SparkSession:
    """Pytest fixture for creating the spark session.

    Creating a fixture enables it to reuse the spark contexts across all tests.
    """
    spark = SparkSession.builder.getOrCreate()
    request.addfinalizer(spark.stop)
    return spark

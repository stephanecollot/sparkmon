"""Test utils."""
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Get Spark Session."""
    spark = SparkSession.builder.getOrCreate()
    return spark

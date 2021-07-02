"""Test cases for the application module."""
import time

from pyspark.sql import SparkSession

import sparkmon


def test_create_application_from_spark(spark: SparkSession) -> None:
    """Basic test."""
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(application, period=5)
    mon.start()

    time.sleep(13)
    mon.stop()
    assert mon.cnt == 3


def test_create_application_from_link(spark: SparkSession) -> None:
    """Basic test."""
    application = sparkmon.create_application_from_link()

    mon = sparkmon.SparkMon(application, period=5)
    mon.start()

    time.sleep(13)
    mon.stop()
    assert mon.cnt == 3

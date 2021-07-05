"""Test cases for the application module."""
import time

import sparkmon
from .utils import get_spark


def test_create_application_from_spark() -> None:
    """Basic test."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(application, period=5)
    mon.start()

    time.sleep(13)
    mon.stop()
    assert mon.cnt == 3


def test_create_application_from_link() -> None:
    """Basic test."""
    get_spark()
    application = sparkmon.create_application_from_link()

    mon = sparkmon.SparkMon(application, period=5)
    mon.start()

    time.sleep(13)
    mon.stop()
    assert mon.cnt == 3


def test_application_other() -> None:
    """Test parse_db."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(application, period=1)
    mon.start()
    time.sleep(3)
    mon.application.parse_db()
    mon.stop()


def test_exception() -> None:
    """Test exception."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(application, period=1)
    mon.start()
    spark.stop()

    mon.stop()

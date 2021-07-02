"""Test cases for the application module."""
import pytest
import sparkmon
import time


def test_create_application_from_spark(spark) -> None:
    """Basic test"""
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(application, period=5)
    mon.start()

    time.sleep(12)
    mon.stop()
    assert mon.cnt == 3


def test_create_application_from_link(spark) -> None:
    """Basic test"""
    application = sparkmon.create_application_from_link()

    mon = sparkmon.SparkMon(application, period=5)
    mon.start()

    time.sleep(12)
    mon.stop()
    assert mon.cnt == 3

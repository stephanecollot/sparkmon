"""Test cases for the application module."""
import time

import sparkmon
from .utils import get_random_df
from .utils import get_spark


def test_create_application_from_spark() -> None:
    """Basic test."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(application, period=5)
    mon.start()

    time.sleep(14)
    mon.stop()
    assert mon.update_cnt == 2


def test_create_application_from_link() -> None:
    """Basic test."""
    get_spark()
    application = sparkmon.create_application_from_link()

    mon = sparkmon.SparkMon(application, period=1)
    mon.start()

    time.sleep(3)
    mon.stop()
    assert mon.update_cnt >= 1


def test_create_context_manager_application_from_link() -> None:
    """Basic test."""
    get_spark()
    application = sparkmon.create_application_from_link()

    with sparkmon.SparkMon(application, period=1) as mon:
        time.sleep(3)

    assert mon.update_cnt >= 2


def test_sparkmon_direct_from_spark() -> None:
    """Basic test."""
    spark = get_spark()

    with sparkmon.SparkMon(spark, period=1) as mon:
        time.sleep(3)

    assert mon.update_cnt >= 2

    try:
        sparkmon.SparkMon("string", period=5)
    except TypeError:
        assert True
    else:
        raise AssertionError()


def test_application_other() -> None:
    """Test parse_db."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)
    application.debug = True

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

    assert mon.is_main_thread_alive()

    spark.stop()

    mon.stop()


def test_stages_tasks() -> None:
    """Test stages and metrics."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(application, period=1, callbacks=[sparkmon.callbacks.log_to_mlflow])
    mon.start()

    # Some "long" jobs
    df1 = spark.createDataFrame(get_random_df(10000)).repartition(10)
    df2 = spark.createDataFrame(get_random_df(10000)).repartition(100)

    df1 = df1.select(["A", "B", "C"])
    df2 = df2.select(["A", "D"])

    df = df1.join(df2, on="A")
    df = df.groupby("A").mean()
    df.toPandas()

    time.sleep(4)

    assert len(mon.application.get_tasks_df()) > 10
    assert len(mon.application.stages_df) > 2
    mon.stop()

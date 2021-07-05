"""Test cases for the callbacks."""
import time
from pathlib import Path

import sparkmon
from .utils import get_spark


def test_plot_to_image() -> None:
    """Basic test."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(
        application, period=5, callbacks=[sparkmon.callbacks.plot_to_image]
    )
    mon.start()

    time.sleep(13)
    mon.stop()
    assert mon.cnt >= 2


def test_mlflow() -> None:
    """Basic test."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(
        application, period=5, callbacks=[sparkmon.callbacks.log_to_mlfow]
    )
    mon.start()

    time.sleep(13)
    mon.stop()

    active_run = sparkmon.mlflow_utils.active_run()
    artifact_uri = Path(active_run.info.artifact_uri)
    assert (artifact_uri / "sparkmon/plot.png").stat().st_size > 10
    assert (artifact_uri / "sparkmon/executors_db.csv").stat().st_size > 10

    assert mon.cnt >= 2

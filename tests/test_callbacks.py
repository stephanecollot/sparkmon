"""Test cases for the callbacks."""
import pathlib
import time
import urllib
from functools import partial

import sparkmon
from .utils import get_spark


def file_uri_to_path(file_uri, path_class=pathlib.Path):
    """This function returns a pathlib.PurePath object for the supplied file URI.

    :param str file_uri: The file URI ...
    :param class path_class: The type of path in the file_uri. By default it uses
        the system specific path pathlib.PurePath, to force a specific type of path
        pass pathlib.PureWindowsPath or pathlib.PurePosixPath
    :returns: the pathlib.PurePath object
    :rtype: pathlib.PurePath
    """
    windows_path = isinstance(path_class(), pathlib.PureWindowsPath)
    file_uri_parsed = urllib.parse.urlparse(file_uri)
    file_uri_path_unquoted = urllib.parse.unquote(file_uri_parsed.path)
    if windows_path and file_uri_path_unquoted.startswith("/"):
        result = path_class(file_uri_path_unquoted[1:])
    else:
        result = path_class(file_uri_path_unquoted)
    if not result.is_absolute():
        raise ValueError("Invalid file uri {} : resulting path {} not absolute".format(file_uri, result))
    return result


def test_plot_to_image() -> None:
    """Basic test."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(application, period=3, callbacks=[sparkmon.callbacks.plot_to_image])
    mon.start()

    time.sleep(14)
    mon.stop()
    assert mon.update_cnt >= 1


def test_mlflow() -> None:
    """Basic test."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(application, period=3, callbacks=[sparkmon.callbacks.log_to_mlflow])
    mon.start()

    time.sleep(14)
    mon.stop()
    assert mon.update_cnt >= 2

    active_run = sparkmon.mlflow_utils.active_run()
    artifact_uri = file_uri_to_path(active_run.info.artifact_uri)
    assert (artifact_uri / "sparkmon/plot.png").stat().st_size > 100
    assert (artifact_uri / "sparkmon/timeseries.csv").stat().st_size > 10


def test_mlflow_directory() -> None:
    """Basic test."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(
        application,
        period=1,
        callbacks=[partial(sparkmon.callbacks.log_to_mlflow, directory="sparkmon2")],
    )
    mon.start()

    time.sleep(3)
    mon.stop()
    assert mon.update_cnt >= 1

    active_run = sparkmon.mlflow_utils.active_run()
    artifact_uri = file_uri_to_path(active_run.info.artifact_uri)
    assert (artifact_uri / "sparkmon2/plot.png").stat().st_size > 100
    assert (artifact_uri / "sparkmon2/timeseries.csv").stat().st_size > 10


def test_sparkmon_title() -> None:
    """Basic test."""
    spark = get_spark()

    with sparkmon.SparkMon(
        spark, period=1, title_prefix="test prefix ", callbacks=[sparkmon.callbacks.plot_to_image]
    ) as mon:
        time.sleep(3)

    assert mon.update_cnt >= 1

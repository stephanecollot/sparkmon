"""Test cases for the callbacks."""
import pathlib
import time
import urllib
from functools import partial

import mlflow

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

    mon = sparkmon.SparkMon(application, period=1, callbacks=[sparkmon.callbacks.plot_to_image])
    mon.start()

    time.sleep(5)
    mon.stop()
    assert mon.update_cnt >= 1


def test_mlflow() -> None:
    """Basic test."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(application, period=1, callbacks=[sparkmon.callbacks.log_to_mlflow])
    mon.start()

    time.sleep(6)
    mon.stop()
    assert mon.update_cnt >= 1

    plot_path = file_uri_to_path(mlflow.get_artifact_uri(artifact_path="sparkmon/plot.png"))
    time_path = file_uri_to_path(mlflow.get_artifact_uri(artifact_path="sparkmon/timeseries.csv"))
    assert plot_path.stat().st_size > 100
    assert time_path.stat().st_size > 10


def test_mlflow_directory_title() -> None:
    """Basic test."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(
        application,
        period=1,
        title_prefix="sparkmon2 title ",
        callbacks=[partial(sparkmon.callbacks.log_to_mlflow, directory="sparkmon2")],
    )
    mon.start()

    time.sleep(7)
    mon.stop()
    time.sleep(3)
    assert mon.update_cnt >= 1

    plot_path = file_uri_to_path(mlflow.get_artifact_uri(artifact_path="sparkmon2/plot.png"))
    time_path = file_uri_to_path(mlflow.get_artifact_uri(artifact_path="sparkmon2/timeseries.csv"))
    assert plot_path.stat().st_size > 100
    assert time_path.stat().st_size > 10

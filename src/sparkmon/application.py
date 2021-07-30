# Copyright (c) 2021 ING Wholesale Banking Advanced Analytics
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to
# use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
# FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
# IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""Spark communication interface with its API, and managing historical API calls."""
import warnings
from datetime import datetime
from typing import Any
from typing import Dict

import pandas as pd
import psutil
import requests
import urlpath
from pyspark.sql import SparkSession

import sparkmon
from sparkmon.utils import flatten_dict
from sparkmon.utils import get_memory_process
from sparkmon.utils import get_memory_user

API_APPLICATIONS_LINK = "api/v1/applications"
WEB_URL = "http://localhost:4040"


class Application:
    """This class is an helper to query Spark API and save historical."""

    def __init__(self, application_id: str, web_url: str = WEB_URL, debug: bool = False) -> None:
        """An application is define by the Spark UI link and an application id.

        :param application_id: Spark applicationId
        :param web_url: Spark REST API server
        :param debug: debug mode for development purposes, it store the row data
        """
        self.web_url = web_url
        self.application_id = application_id
        self.debug = debug

        self.executors_db: Dict[Any, Any] = {}  # The key is timestamp
        self.timeseries_db: Dict[Any, Any] = {}  # The key is timestamp
        self.stages_df: pd.DataFrame = pd.DataFrame()
        self.tasks_db: Dict[str, Dict[Any, Any]] = {}  # The key is stageId.attemptId

    def get_executors_info(self) -> pd.DataFrame:
        """Retrieve executors info."""
        executors_df = pd.read_json(
            urlpath.URL(
                self.web_url,
                API_APPLICATIONS_LINK,
                self.application_id,
                "executors",
            )
        )
        return executors_df

    def log_executors_info(self) -> None:
        """Add a new executor info in the dict database."""
        executors_df = self.get_executors_info()

        now = pd.to_datetime(datetime.now())
        if self.debug:
            self.executors_db[now] = executors_df  # Storing full row data

        self.timeseries_db[now] = self.parse_executors(executors_df)
        # Remark: local_memory_pct is a percentage, doesn't really work if you are running an kube
        self.timeseries_db[now]["local_memory_pct"] = psutil.virtual_memory()[2]  # Local machine memory usage
        self.timeseries_db[now]["process_memory_usage"] = get_memory_process()  # Local machine memory usage
        self.timeseries_db[now]["user_memory_usage"] = get_memory_user()  # Local machine memory usage

    def parse_db(self) -> None:
        """Re-parse the full executors_db, usefull if you change the parsing function, for development."""
        if not self.debug:
            print("sparkmon: Warning, parse_db should be used with debug=True")
        for t, executors_df in self.executors_db.items():
            self.timeseries_db[t].update(self.parse_executors(executors_df))

    def plot(self) -> None:
        """Plotting."""
        sparkmon.plot_timeseries(self.get_timeseries_db_df(), title=self.application_id)

    @staticmethod
    def parse_executors(executors_df: pd.DataFrame) -> Dict[Any, Any]:
        """Convert an executors DataFrame to a dictionnary.

        More spefically we aggregate metrix from all executors into one.
        """
        if len(executors_df) == 0:
            return {}

        memoryMetrics = executors_df["memoryMetrics"].dropna()
        memoryMetrics_df = pd.DataFrame.from_records(memoryMetrics, index=memoryMetrics.index)
        executors_df = executors_df.join(memoryMetrics_df)

        if "peakMemoryMetrics" in executors_df.columns:
            peakMemoryMetrics = executors_df["peakMemoryMetrics"].dropna()
            peakMemoryMetrics_df = pd.DataFrame.from_records(peakMemoryMetrics, index=peakMemoryMetrics.index)
            executors_df = executors_df.join(peakMemoryMetrics_df)

        executors_df["usedOnHeapStorageMemoryPct"] = (
            executors_df["usedOnHeapStorageMemory"] / executors_df["totalOnHeapStorageMemory"] * 100
        )
        executors_df["usedOffHeapStorageMemoryPct"] = (
            executors_df["usedOffHeapStorageMemory"] / executors_df["totalOffHeapStorageMemory"] * 100
        )
        executors_df["memoryUsedPct"] = executors_df["memoryUsed"] / executors_df["maxMemory"] * 100

        # Columns to aggregate
        cols = [
            "memoryUsedPct",
            "usedOnHeapStorageMemoryPct",
            "usedOffHeapStorageMemoryPct",
            "totalOnHeapStorageMemory",
            "totalOffHeapStorageMemory",
            "ProcessTreePythonVMemory",
            "ProcessTreePythonRSSMemory",
            "JVMHeapMemory",
            "JVMOffHeapMemory",
            "OffHeapExecutionMemory",
            "OnHeapExecutionMemory",
        ]

        # Filter columns that are not on the DataFrame
        cols = [col for col in cols if col in executors_df.columns]

        # Obtain aggregated values
        # Let's filter: "RuntimeWarning: Mean of empty slice"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=RuntimeWarning)
            res = executors_df[cols].agg(["max", "mean", "median", "min"])

        # Convert to dict
        d: Dict[str, Any] = {
            f"{key}_{agg}": value for key, values in res.to_dict().items() for agg, value in values.items()
        }

        d["numActive"] = len(executors_df.query("isActive"))
        d["memoryUsed_sum"] = executors_df["memoryUsed"].sum()
        d["maxMemory_sum"] = executors_df["maxMemory"].sum()
        d["memoryUsed_sum_pct"] = executors_df["memoryUsed"].sum() / executors_df["maxMemory"].sum() * 100
        d["memoryUsedPct_driver"] = executors_df.iloc[0]["memoryUsedPct"]

        return d

    def log_stages(self) -> None:
        """Retrieve stages."""
        url = urlpath.URL(self.web_url, API_APPLICATIONS_LINK, self.application_id, "stages")
        self.stages_df = pd.read_json(url)

    def log_tasks(self) -> None:
        """Retrieve tasks."""
        for _, row in self.stages_df.iterrows():
            stage_uid = f"{row['stageId']}.{row['attemptId']}"

            # Initialize
            if self.tasks_db.get(stage_uid) is None:
                self.tasks_db[stage_uid] = {"tasks": None, "stage_last_status": None}

            # Don't query again what is done
            if self.tasks_db[stage_uid]["stage_last_status"] in ["COMPLETE", "SKIPPED", "FAILED"]:
                continue

            url = urlpath.URL(
                self.web_url,
                API_APPLICATIONS_LINK,
                self.application_id,
                "stages",
                str(row["stageId"]),
                str(row["attemptId"]),
            )

            stage_detail_r = requests.get(url)
            stage_detail = stage_detail_r.json()

            tasks = stage_detail["tasks"]

            # Flatten the dictionnary
            tasks = [flatten_dict(tasks[k]) for k in tasks.keys()]

            self.tasks_db[stage_uid]["tasks"] = tasks
            self.tasks_db[stage_uid]["stage_last_status"] = row["status"]

    def get_tasks_df(self) -> pd.DataFrame:
        """Return all tasks info into a DataFrame."""
        tasks_list = []
        # Iter stages
        for k in self.tasks_db.keys():
            tasks = self.tasks_db[k]["tasks"]
            # Iter tasks
            for t in tasks:
                t["stage_uid"] = k
                tasks_list.append(t)
        tasks_df = pd.DataFrame(tasks_list)
        return tasks_df

    def get_timeseries_db_df(self) -> pd.DataFrame:
        """Return timeseries_db info into a DataFrame."""
        timeseries_db_df = pd.DataFrame(self.timeseries_db).T
        return timeseries_db_df

    def log_all(self) -> None:
        """Updating all information."""
        self.log_executors_info()
        self.log_stages()
        self.log_tasks()


def get_application_ids(web_url: str = WEB_URL) -> pd.DataFrame:
    """Retrieve available application id."""
    applications_df = pd.read_json(urlpath.URL(web_url, API_APPLICATIONS_LINK))
    return applications_df


def create_application_from_link(index: int = 0, web_url: str = WEB_URL) -> Application:
    """Create an Application.

    :param index: Application index in the application list
    :param web_url: Spark REST API server
    """
    applications_df = get_application_ids(web_url)
    application_id = applications_df["id"].iloc[index]

    application = Application(application_id, web_url)

    return application


def create_application_from_spark(spark: SparkSession) -> Application:
    """Create an Application from Spark Session."""
    web_url = spark.sparkContext.uiWebUrl
    application_id = spark.sparkContext.applicationId

    application = Application(application_id, web_url)

    return application

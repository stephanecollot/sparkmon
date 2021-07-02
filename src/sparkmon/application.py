"""Spark communication interface with its API, and managing historical API calls."""
from datetime import datetime
from typing import Any
from typing import Dict

import pandas as pd
import psutil
import urlpath

import sparkmon
from sparkmon.utils import get_memory

API_APPLICATIONS_LINK = "api/v1/applications"
SPARK_SESSION_LINK = "http://localhost:4040"


class Application:
    """This class is an helper to query Spark API and save historical."""

    def __init__(self, spark_session_link: str, application_id: str) -> None:
        """An application is define by the Spark UI link and an application id."""
        self.spark_session_link = spark_session_link
        self.application_id = application_id

        self.executors_db = {}

    def get_executors_info(self) -> Dict[Any, Any]:
        """Retrieve executors info."""
        executors_df = pd.read_json(
            urlpath.URL(
                self.spark_session_link,
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
        self.executors_db[now] = {
            "executors_df": executors_df,  # Storing full row data
            "local_memory_pct": psutil.virtual_memory()[
                2
            ],  # Local machine memory usage
            "process_memory_usage": get_memory(),  # Python process memory usage in bytes
        }
        self.executors_db[now].update(self.parse_executors(executors_df))

    def parse_db(self) -> None:
        """Re-parse the full executors_db, usefull if you change the parsing function, for development."""
        for t, v in self.executors_db.items():
            executors_df = v["executors_df"]
            self.executors_db[t].update(self.parse_executors(executors_df))

    def plot(self) -> None:
        """Plotting."""
        sparkmon.plot_db(self.executors_db)

    @staticmethod
    def parse_executors(executors_df: pd.DataFrame) -> Dict[Any, Any]:
        """Convert an executors DataFrame to a dictionnary.

        More spefically we aggregate metrix from all executors into one.
        """
        memoryMetrics = executors_df["memoryMetrics"].dropna()
        memoryMetrics_df = pd.DataFrame.from_records(
            memoryMetrics, index=memoryMetrics.index
        )
        executors_df = executors_df.join(memoryMetrics_df)

        if "peakMemoryMetrics" in executors_df.columns:
            peakMemoryMetrics = executors_df["peakMemoryMetrics"].dropna()
            peakMemoryMetrics_df = pd.DataFrame.from_records(
                peakMemoryMetrics, index=peakMemoryMetrics.index
            )
            executors_df = executors_df.join(peakMemoryMetrics_df)

        executors_df["usedOnHeapStorageMemoryPct"] = (
            executors_df["usedOnHeapStorageMemory"]
            / executors_df["totalOnHeapStorageMemory"]
            * 100
        )
        executors_df["usedOffHeapStorageMemoryPct"] = (
            executors_df["usedOffHeapStorageMemory"]
            / executors_df["totalOffHeapStorageMemory"]
            * 100
        )
        executors_df["memoryUsedPct"] = (
            executors_df["memoryUsed"] / executors_df["maxMemory"] * 100
        )

        def mmm(d, executors_df, col):
            if col not in executors_df.columns:
                return d
            d[f"{col}_max"] = executors_df[col].max()
            d[f"{col}_mean"] = executors_df[col].mean()
            d[f"{col}_min"] = executors_df[col].min()
            d[f"{col}_median"] = executors_df[col].median()

            return d

        d = {}
        d = mmm(d, executors_df, "memoryUsedPct")
        d = mmm(d, executors_df, "usedOnHeapStorageMemoryPct")
        d = mmm(d, executors_df, "usedOffHeapStorageMemoryPct")
        d = mmm(d, executors_df, "totalOnHeapStorageMemory")
        d = mmm(d, executors_df, "totalOffHeapStorageMemory")
        d = mmm(d, executors_df, "ProcessTreePythonVMemory")
        d = mmm(d, executors_df, "ProcessTreePythonRSSMemory")
        d = mmm(d, executors_df, "JVMHeapMemory")
        d = mmm(d, executors_df, "JVMOffHeapMemory")
        d = mmm(d, executors_df, "OffHeapExecutionMemory")
        d = mmm(d, executors_df, "OnHeapExecutionMemory")
        d["numActive"] = len(executors_df.query("isActive"))
        d["memoryUsed_sum"] = executors_df["memoryUsed"].sum()
        d["maxMemory_sum"] = executors_df["maxMemory"].sum()
        d["memoryUsed_sum_pct"] = (
            executors_df["memoryUsed"].sum() / executors_df["maxMemory"].sum() * 100
        )
        d["memoryUsedPct_driver"] = executors_df.iloc[0]["memoryUsedPct"]

        return d


def get_application_ids(spark_session_link=SPARK_SESSION_LINK) -> pd.DataFrame:
    """Retrieve available application id."""
    applications_df = pd.read_json(
        urlpath.URL(spark_session_link, API_APPLICATIONS_LINK)
    )
    return applications_df


def create_application_from_link(
    id=0, spark_session_link=SPARK_SESSION_LINK
) -> Application:
    """Create an Application."""
    applications_df = get_application_ids(spark_session_link)
    application_id = applications_df["id"].iloc[id]

    application = Application(spark_session_link, application_id)

    return application


def create_application_from_spark(spark) -> Application:
    """Create an Application from Spark Session."""
    spark_session_link = spark.sparkContext.uiWebUrl
    application_id = spark.sparkContext.applicationId

    application = Application(spark_session_link, application_id)

    return application

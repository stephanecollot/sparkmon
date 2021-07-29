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
"""List of default callbacks."""
from pathlib import Path

import matplotlib
import matplotlib.pyplot as plt

from sparkmon import Application
from sparkmon.mlflow_utils import log_file


def plot_to_image(application: Application, path: str = "sparkmon.png") -> None:
    """Plot and save to image.

    Not compatible with live_plot_notebook().
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # To avoid the following error: "RuntimeError: main thread is not in main loop"
    backend = "agg"
    if matplotlib.get_backend() != backend:
        print("set backend")
        plt.switch_backend(backend)

    application.plot()
    plt.savefig(path)

    # To avoid memory leak
    plt.clf()
    plt.close()


def plot_to_mlflow(application: Application, path: str = "sparkmon/plot.png") -> None:
    """Log image to mlflow.

    Not compatible with live_plot_notebook().
    """
    # To avoid the following error: "RuntimeError: main thread is not in main loop"
    backend = "agg"
    if matplotlib.get_backend() != backend:
        print("set backend")
        plt.switch_backend(backend)

    application.plot()
    with log_file(path) as fp:
        plt.savefig(fp.name)

    # To avoid memory leak
    plt.clf()
    plt.close()


def log_timeseries_db_to_mlflow(application: Application, path: str = "sparkmon/timeseries.csv") -> None:
    """Log timeseries_db to mlflow."""
    timeseries_db_df = application.get_timeseries_db_df()
    with log_file(path) as fp:
        timeseries_db_df.to_csv(fp, index=False)


def log_tasks_to_mlflow(application: Application, path: str = "sparkmon/tasks.csv") -> None:
    """Log tasks to mlflow."""
    with log_file(path) as fp:
        application.get_tasks_df().to_csv(fp, index=False)


def log_stages_to_mlflow(application: Application, path: str = "sparkmon/stages.csv") -> None:
    """Log tasks to mlflow."""
    with log_file(path) as fp:
        application.stages_df.to_csv(fp, index=False)


def log_to_mlflow(application: Application) -> None:
    """Log to mlflow."""
    plot_to_mlflow(application)
    log_timeseries_db_to_mlflow(application)
    log_tasks_to_mlflow(application)
    log_stages_to_mlflow(application)

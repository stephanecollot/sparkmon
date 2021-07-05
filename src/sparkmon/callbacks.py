"""List of default callbacks."""
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from sparkmon import Application
from sparkmon.mlflow_utils import log_file


def plot_to_image(application: Application, path: str = "sparkmon.png") -> None:
    """Plot and save to image."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # To avoid the following error: "RuntimeError: main thread is not in main loop"
    plt.switch_backend("agg")

    application.plot()
    plt.savefig(path)


def plot_to_mlflow(application: Application, path: str = "sparkmon/plot.png") -> None:
    """Log image to mlflow."""
    # To avoid the following error: "RuntimeError: main thread is not in main loop"
    plt.switch_backend("agg")

    application.plot()
    with log_file(path) as fp:
        plt.savefig(fp.name)


def log_executors_db_to_mlflow(
    application: Application, path: str = "sparkmon/executors_db.csv"
) -> None:
    """Log executors_db to mlflow."""
    executors_db_df = pd.DataFrame(application.executors_db).T
    with log_file(path) as fp:
        executors_db_df.to_csv(fp, index=False)


def log_to_mlfow(application: Application) -> None:
    """Log executors_db to mlflow."""
    plot_to_mlflow(application)
    log_executors_db_to_mlflow(application)

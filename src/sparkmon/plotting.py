"""Plotting utilities."""
from typing import Any
from typing import Callable
from typing import Dict

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
from IPython import display

import sparkmon
from sparkmon.utils import convert_size


def plot_max_value(ax: matplotlib.axes.Axes, s: Any, string_rep: Callable = lambda x: f"{x:0.2f}") -> None:
    """Helping function to add the maximum value on the right side of a plot."""
    ax.annotate(
        string_rep(s.max()),
        xy=(1, s.max()),
        xytext=(8, 0),
        xycoords=("axes fraction", "data"),
        textcoords="offset points",
    )


def mmm_plot(
    ax: matplotlib.axes.Axes,
    value: str,
    executors_db_df: pd.DataFrame,
    title_prefix: str,
    pct: bool = False,
    ylim: Any = None,
) -> None:
    """Helping function to plot the aggregations of a metric."""
    ax.set_title(f"{title_prefix}{value}:")
    if f"{value}_max" not in executors_db_df.columns:
        return

    executors_db_df[f"{value}_max"].plot(label="max", ax=ax, ylim=ylim)
    executors_db_df[f"{value}_median"].plot(label="median", ax=ax, ylim=ylim)
    executors_db_df[f"{value}_mean"].plot(label="mean", ax=ax, ylim=ylim)
    executors_db_df[f"{value}_min"].plot(label="min", ax=ax, ylim=ylim)
    if not pct:
        ticks = [convert_size(x) for x in ax.get_yticks()]
        ax.set_yticks(
            ax.get_yticks()
        )  # Necessary to remove UserWarning: FixedFormatter should only be used together with FixedLocator
        ax.set_yticklabels(ticks)
    ax.legend()
    plot_max_value(ax, executors_db_df[f"{value}_max"], convert_size)


def plot_db(executors_db: Dict[Any, Any]) -> None:
    """Plot executors DB."""
    executors_db_df = pd.DataFrame(executors_db).T

    if len(executors_db_df) == 0:
        return

    fig = plt.figure(constrained_layout=False, figsize=(28, 16))
    gs = fig.add_gridspec(9, 2)

    ax = fig.add_subplot(gs[0, 0])
    ax.set_title("1. Num active executors:")
    executors_db_df["numActive"].plot(ax=ax)
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[0, 1])
    ax.set_title("2. Total memory used percentage:")
    executors_db_df["memoryUsed_sum_pct"].plot(ax=ax, ylim=(0, 100))
    plot_max_value(ax, executors_db_df["memoryUsed_sum_pct"])
    executors_db_df["local_memory_pct"].plot(ax=ax, ylim=(0, 100))
    plot_max_value(ax, executors_db_df["local_memory_pct"])
    ax.legend()
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[1:3, 0])
    # mmm_plot(ax, 'memoryUsedPct', executors_db_df, '3. ', pct=True, ylim=(0,100))
    mmm_plot(ax, "OnHeapExecutionMemory", executors_db_df, "3. Peak ")
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[1:3, 1])
    mmm_plot(ax, "OffHeapExecutionMemory", executors_db_df, "4. Peak ")
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[3:5, 0])
    mmm_plot(ax, "totalOnHeapStorageMemory", executors_db_df, "5. ")
    # mmm_plot(ax, 'usedOnHeapStorageMemory', executors_db_df, '5. ')  # 0
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[3:5, 1])
    ax.set_title("6. Driver memory percentage:")
    executors_db_df["memoryUsedPct_driver"].plot(label="driver", ax=ax)
    plot_max_value(ax, executors_db_df["memoryUsedPct_driver"])
    # mmm_plot(ax, 'totalOffHeapStorageMemory', executors_db_df, '6. ')
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[5:7, 0])
    mmm_plot(ax, "ProcessTreePythonRSSMemory", executors_db_df, "7. Peak ")
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[5:7, 1])
    mmm_plot(ax, "ProcessTreePythonVMemory", executors_db_df, "8. Peak ")
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[7:9, 0])
    mmm_plot(ax, "JVMHeapMemory", executors_db_df, "9. Peak ")

    ax = fig.add_subplot(gs[7:9, 1])
    mmm_plot(ax, "JVMOffHeapMemory", executors_db_df, "10. Peak ")


def plot_notebook(application: sparkmon.Application) -> None:
    """Plot for notebook."""
    display.clear_output(True)
    application.plot()
    plt.show()

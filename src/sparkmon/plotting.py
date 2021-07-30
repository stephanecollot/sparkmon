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
"""Plotting utilities."""
from typing import Any
from typing import Callable

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


def ax_convert_size(ax: matplotlib.axes.Axes):
    """Convert byte size for displaying tick values."""
    ticks = [convert_size(x) for x in ax.get_yticks()]
    ax.set_yticks(
        ax.get_yticks()
    )  # Necessary to remove UserWarning: FixedFormatter should only be used together with FixedLocator
    ax.set_yticklabels(ticks)


def mmm_plot(
    ax: matplotlib.axes.Axes,
    value: str,
    timeseries_db_df: pd.DataFrame,
    title_prefix: str,
    pct: bool = False,
    ylim: Any = (0, None),
) -> None:
    """Helping function to plot the aggregations of a metric."""
    ax.set_title(f"{title_prefix}{value}:")
    if f"{value}_max" not in timeseries_db_df.columns:
        return

    timeseries_db_df[f"{value}_max"].plot(label="max", ax=ax, ylim=ylim)
    timeseries_db_df[f"{value}_median"].plot(label="median", ax=ax, ylim=ylim)
    timeseries_db_df[f"{value}_mean"].plot(label="mean", ax=ax, ylim=ylim)
    timeseries_db_df[f"{value}_min"].plot(label="min", ax=ax, ylim=ylim)
    if not pct:
        ax_convert_size(ax)
    ax.legend()
    plot_max_value(ax, timeseries_db_df[f"{value}_max"], convert_size)


def prepare_axis(ax: matplotlib.axes.Axes) -> None:
    """Prepare subplot ax to have no whitespace before and after the plotlines.

    To call before any plotting.
    """
    ax.margins(x=0)


def plot_timeseries(timeseries_db_df: pd.DataFrame, title: str = None) -> matplotlib.figure.Figure:
    """Plot timeseries DB."""
    if len(timeseries_db_df) == 0:
        return

    fig = plt.figure(constrained_layout=False, figsize=(28, 16), tight_layout={"pad": 0})
    gs = fig.add_gridspec(9, 2)
    if title is not None:
        fig.suptitle(title, y=1)

    ax = fig.add_subplot(gs[0, 0])
    prepare_axis(ax)
    ax.set_title("1. Num active executors:")
    timeseries_db_df["numActive"].plot(ax=ax)
    ax.set_xticklabels([])
    ax.set_ylim(bottom=0)

    ax = fig.add_subplot(gs[0, 1])
    prepare_axis(ax)
    ax.set_title("2. Total memory used percentage:")
    timeseries_db_df["memoryUsed_sum_pct"].plot(ax=ax, ylim=(0, 100))
    plot_max_value(ax, timeseries_db_df["memoryUsed_sum_pct"])
    timeseries_db_df["local_memory_pct"].plot(ax=ax, ylim=(0, 100))
    plot_max_value(ax, timeseries_db_df["local_memory_pct"])
    ax.legend()
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[1:3, 0])
    prepare_axis(ax)
    # mmm_plot(ax, 'memoryUsedPct', timeseries_db_df, '3. ', pct=True, ylim=(0,100))
    mmm_plot(ax, "OnHeapExecutionMemory", timeseries_db_df, "3. Peak ")
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[1:3, 1])
    prepare_axis(ax)
    # mmm_plot(ax, "OffHeapExecutionMemory", timeseries_db_df, "4. Peak ")  # off heap is disable by default in Spark
    ax.set_title("4. Local memory usage:")
    timeseries_db_df["process_memory_usage"].plot(ax=ax)  # We should put only 1 ylim, the last one
    plot_max_value(ax, timeseries_db_df["process_memory_usage"], convert_size)
    timeseries_db_df["user_memory_usage"].plot(ax=ax, ylim=(0, None))
    plot_max_value(ax, timeseries_db_df["user_memory_usage"], convert_size)
    ax_convert_size(ax)
    ax.legend()
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[3:5, 0])
    prepare_axis(ax)
    mmm_plot(ax, "totalOnHeapStorageMemory", timeseries_db_df, "5. ")
    # mmm_plot(ax, 'usedOnHeapStorageMemory', timeseries_db_df, '5. ')  # 0
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[3:5, 1])
    prepare_axis(ax)
    ax.set_title("6. Driver memory percentage:")
    timeseries_db_df["memoryUsedPct_driver"].plot(label="driver", ax=ax)
    plot_max_value(ax, timeseries_db_df["memoryUsedPct_driver"])
    # mmm_plot(ax, 'totalOffHeapStorageMemory', timeseries_db_df, '6. ')
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[5:7, 0])
    prepare_axis(ax)
    mmm_plot(ax, "ProcessTreePythonRSSMemory", timeseries_db_df, "7. Peak ")
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[5:7, 1])
    prepare_axis(ax)
    mmm_plot(ax, "ProcessTreePythonVMemory", timeseries_db_df, "8. Peak ")
    ax.set_xticklabels([])

    ax = fig.add_subplot(gs[7:9, 0])
    prepare_axis(ax)
    mmm_plot(ax, "JVMHeapMemory", timeseries_db_df, "9. Peak ")

    ax = fig.add_subplot(gs[7:9, 1])
    prepare_axis(ax)
    mmm_plot(ax, "JVMOffHeapMemory", timeseries_db_df, "10. Peak ")

    return fig


def plot_notebook(application: sparkmon.Application) -> None:
    """Plot for notebook."""
    display.clear_output(True)
    application.plot()
    plt.show()

    # To avoid memory leak
    plt.clf()
    plt.close()

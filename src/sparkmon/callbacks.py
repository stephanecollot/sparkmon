"""List of default callbacks."""
from pathlib import Path

import matplotlib.pyplot as plt

from sparkmon import Application


def callback_plot_to_image(
    application: Application, path: str = "sparkmon.png"
) -> None:
    """Plot and save to image."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # To avoid the following error: "RuntimeError: main thread is not in main loop"
    plt.switch_backend("agg")

    application.plot()
    plt.savefig(path)

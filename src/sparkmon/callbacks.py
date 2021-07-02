"""List of default callbacks."""
import matplotlib.pyplot as plt

from sparkmon import Application


def callback_plot_to_image(application: Application) -> None:
    """Plot and save to image."""
    application.plot()
    plt.savefig("books_read.png")

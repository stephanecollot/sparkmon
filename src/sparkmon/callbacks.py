"""List of default callbacks."""
from sparkmon import Application


def callback_plot_to_image(application: Application) -> None:
    """Plot for notebook."""
    # display.clear_output(True)
    application.plot()
    # plt.show()

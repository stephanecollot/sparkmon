"""List of default callbacks."""
import matplotlib.pyplot as plt
from IPython import display
from sparkmon import Application


def callback_plot_to_image(application: Application):
    """Plot for notebook."""
    #display.clear_output(True)
    application.plot()
    #plt.show()

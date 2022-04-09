"""Testing plotting."""
import matplotlib.pyplot as plt
import sparkmon

from .utils import get_spark


def test_plotting() -> None:
    """Test plotting notebook."""
    spark = get_spark()
    application = sparkmon.create_application_from_spark(spark)

    mon = sparkmon.SparkMon(application, period=1)
    mon.start()

    # Necessary in order to not have a new window with the image that is blocking the test until it is closed
    plt.ion()

    mon.live_plot_notebook(n_iter=2)

    mon.stop()

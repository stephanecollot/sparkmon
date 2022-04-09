"""Command-line interface."""
import click

import sparkmon


@click.command()
@click.version_option()
@click.option(
    "-i",
    "--index",
    default=0,
    show_default=True,
    help="Application index or row number.",
)
@click.option("-u", "--url", default=sparkmon.WEB_URL, show_default=True, help="Spark UI web URL.")
@click.option("-p", "--period", default=5, show_default=True, help="Update period in seconds.")
def main(index: int, url: str, period: int) -> None:
    """Command line interface to launch sparkmon, it will monitor your Spark Application.

    The default callback is saving the memoring graph in ./sparkmon.png
    """
    application = sparkmon.create_application_from_link(index, url)

    mon = sparkmon.SparkMon(application, period=period, callbacks=[sparkmon.callbacks.plot_to_image])
    mon.start()
    click.confirm("Press Enter to stop...", abort=False, default=True, show_default=False)
    mon.stop()


if __name__ == "__main__":
    main(prog_name="sparkmon")  # pragma: no cover

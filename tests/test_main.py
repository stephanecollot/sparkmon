"""Test cases for the __main__ module."""
import time

import pytest
from click.testing import CliRunner

from .utils import get_spark
from sparkmon import __main__


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for invoking command-line interfaces."""
    return CliRunner()


def test_main(runner: CliRunner) -> None:
    """It exits with a status code of zero."""
    get_spark()
    result = runner.invoke(__main__.main)

    time.sleep(10)
    result.return_value = True

    assert result.exit_code == 0

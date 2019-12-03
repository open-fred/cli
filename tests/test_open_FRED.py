
from click.testing import CliRunner

from open_FRED.cli import main


def test_main():
    runner = CliRunner()
    result = runner.invoke(main, [])

    assert result.output == ''
    assert result.exit_code == 1

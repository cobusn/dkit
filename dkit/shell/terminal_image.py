"""
Display a matplotlib figure inline in a terminal, via chafa

chafa (https://hpjansson.org/chafa/) auto-detects the terminal's graphics
protocol -- kitty, iTerm2, sixel -- and falls back to ANSI/Unicode blocks
when the terminal supports none of them, so this module does not need to
know or care which one is in use. It shells out to the binary rather than
depending on a Python graphics-protocol package: those tend to be thin and
irregularly maintained, where chafa is a widely packaged, actively
maintained C library.

Not bundled with dkit -- it is a system package:

* Ubuntu/Debian: ``apt install chafa`` (universe component)
* RHEL/Fedora: ``dnf install epel-release && dnf install chafa`` (EPEL on
  RHEL; base repos on Fedora)
"""
import shutil
import subprocess
import tempfile
from pathlib import Path

from matplotlib.figure import Figure

from ..exceptions import DKitShellException


def is_available() -> bool:
    """True if the ``chafa`` binary is on PATH"""
    return shutil.which("chafa") is not None


def show(fig: Figure, dpi: int = 150) -> None:
    """render a figure to a temporary PNG and display it with chafa

    args:
        fig: the figure to display.  Not closed: the caller decides that.
        dpi: resolution of the rendered PNG.  chafa downsamples for the
            terminal in any case; this mainly affects how sharp small text
            in the figure looks before that downsampling.

    raises:
        DKitShellException: if the ``chafa`` binary is not on PATH
    """
    if not is_available():
        raise DKitShellException(
            "chafa is not installed. On Ubuntu/Debian: 'apt install chafa'. "
            "On RHEL/Fedora: 'dnf install epel-release && dnf install chafa'."
        )
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "plot.png"
        fig.savefig(path, dpi=dpi, bbox_inches="tight")
        subprocess.run(["chafa", str(path)], check=True)


def show_histogram(histogram, dpi: int = 150, **options) -> None:
    """Render a Histogram with Plot2 and display it in the terminal.

    Args:
        histogram: Precomputed histogram to display.
        dpi: Resolution used when converting the figure to a PNG.
        **options: Options passed to ``dkit.plot2.plot_histogram``.
    """
    from ..plot2 import plot_histogram

    options.setdefault("theme", "dkit-console")
    show(plot_histogram(histogram, **options), dpi=dpi)

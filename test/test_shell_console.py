import sys
import types
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

sys.path.insert(0, "..")  # noqa

from dkit.data.histogram import Bin, Histogram
from dkit.shell.console import render_histogram


class TestRenderHistogram(unittest.TestCase):

    def test_render_histogram_uses_existing_bins(self):
        calls = []

        def hist_aggregated(counts, bins, **kwargs):
            calls.append((counts, bins, kwargs))
            return "rendered"

        fake_plotille = types.SimpleNamespace(
            hist_aggregated=hist_aggregated,
        )
        histogram = Histogram([
            Bin(0, 1, 2),
            Bin(1, 3, 5),
        ])

        output = StringIO()
        with patch.dict(sys.modules, {"plotille": fake_plotille}), \
                redirect_stdout(output):
            result = render_histogram(histogram, width=40, log_scale=True)

        self.assertEqual(result, "rendered")
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(calls, [(
            [2, 5], [0, 1, 3],
            {"width": 40, "log_scale": True, "lc": "bright_green"},
        )])

    def test_render_empty_histogram(self):
        self.assertEqual(render_histogram(Histogram([])), "<empty histogram>")


if __name__ == "__main__":
    unittest.main()

# Copyright (c) 2020, Cobus Nel
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import unittest
import sys
sys.path.insert(0, "..")  # noqa
from dkit.plot.matplotlib import MPLBackend
from dkit.doc.canned import (
    control_chart_plot,
)

from sample_data import control_chart_data


class TestCanned(unittest.TestCase):

    def test_control_chart_plot(self):
        plt = control_chart_plot(
            control_chart_data,
            x="index",
            y="sales",
            y_hat="mean",
            ucl="upper",
            lcl="lower",
            title="Monthly Sales",
            file_name="cc_plot.svg",
            x_title="months",
            y_title="sales",
            width=15,
            height=6

        )
        MPLBackend("svg").render(plt.as_dict(), "plots/cc_plot.svg")


if __name__ == '__main__':
    unittest.main()

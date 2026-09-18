# Copyright (c) 2026 Cobus Nel
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
"""API-generated content for the style-pack report project."""

from pathlib import Path

from dkit.doc2 import document as doc
from dkit.doc2.builder import DocumentCode
from dkit.plot2 import Plot, geom, scale


class Summary(DocumentCode):
    """Provide a chart and a canonical Doc2 table to the report template."""

    rows = [
        {"service": "Data platform", "status": "on track", "completion": 92},
        {
            "service": "Document automation",
            "status": "on track",
            "completion": 84,
        },
        {
            "service": "Customer migration",
            "status": "at risk",
            "completion": 68,
        },
    ]

    @doc.wrap_json
    def table(self) -> doc.Table:
        """Return an API-generated table for insertion into Markdown.

        Returns:
            A canonical Doc2 table with heading and numeric formatting.
        """
        return doc.Table(
            self.rows,
            [
                doc.Column("service", "Service", width=6),
                doc.Column("status", "Status", width=4),
                doc.Column(
                    "completion",
                    "Completion",
                    width=3,
                    align="right",
                    format_="{0}%",
                ),
            ],
        )

    def _render_plot(self, plot: Plot):
        """Render a plot2 specification and return pyplot for Doc2."""
        import matplotlib.pyplot as pyplot

        Path("plots").mkdir(exist_ok=True)
        plot.render(self.rows)
        return pyplot

    @doc.wrap_matplotlib(
        filename="plots/delivery_completion.png",
        width=16,
        height=6,
    )
    def chart(self):
        """Create a bar chart with the declarative plot2 API."""
        return self._render_plot(
            Plot(
                geom.Bar("Completion", x="service", y="completion"),
                x=scale.Categorical("Service"),
                y=scale.Linear("Completion (%)", limits=(0, 100)),
                title="Delivery completion",
                width=16,
                height=6,
            )
        )

    @doc.wrap_matplotlib(
        filename="plots/delivery_completion_line.png",
        width=16,
        height=6,
    )
    def line_chart(self):
        """Create a line chart with the declarative plot2 API."""
        return self._render_plot(
            Plot(
                geom.Line(
                    "Completion",
                    x="service",
                    y="completion",
                    marker="o",
                ),
                x=scale.Categorical("Service"),
                y=scale.Linear("Completion (%)", limits=(0, 100)),
                title="Completion profile",
                width=16,
                height=6,
            )
        )

    @doc.wrap_matplotlib(
        filename="plots/delivery_completion_scatter.png",
        width=16,
        height=6,
    )
    def scatter_chart(self):
        """Create a scatter chart with the declarative plot2 API."""
        return self._render_plot(
            Plot(
                geom.Scatter("Completion", x="service", y="completion"),
                x=scale.Categorical("Service"),
                y=scale.Linear("Completion (%)", limits=(0, 100)),
                title="Completion observations",
                width=16,
                height=6,
            )
        )

    @doc.wrap_matplotlib(
        filename="plots/delivery_completion_treemap.png",
        width=16,
        height=6,
    )
    def treemap(self):
        """Create a treemap with the declarative plot2 API."""
        return self._render_plot(
            Plot(
                geom.TreeMap(
                    "service",
                    "completion",
                    "status",
                    value_format="{:.0f}%",
                ),
                title="Completion by service",
                width=16,
                height=6,
            )
        )

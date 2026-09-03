"""
plot2_extensions_slope_shared.py
==================================
One SlopePlot instance drawing two panels, so each month keeps the colour it
had in the crowded overview even though the second panel zooms in on half
the year. This depends on drawing order: colours are assigned in order of
first appearance, so drawing the subset first would give it colours one
through six and shift every other month in the overview.

Output: plots/plot2_extensions_slope_shared.png
"""
import sys; sys.path.insert(0, "..")  # noqa

import matplotlib.pyplot as plt

from plot2_data import MONTHS, by_decade, nottem

from dkit.plot2 import get_theme, save_figure
from dkit.plot2.matplotlib_extra import SlopePlot


rows = by_decade(nottem())

sp = SlopePlot(value_format="{:,.1f}")
theme = get_theme("dkit-light")
with theme.context():
    fig, axes = plt.subplots(1, 2, figsize=(16, 9))
for ax, months, label in ((axes[0], MONTHS, "All twelve months"),
                          (axes[1], MONTHS[6:], "Jul-Dec")):
    sp.draw([r for r in rows if r["month"] in months],
            "month", "decade", "mean", title=label,
            y_label="Temperature (F)", theme=theme, ax=ax)
fig.suptitle("Mean temperature by month, 1920s vs 1930s")

filename = "plots/plot2_extensions_slope_shared.png"
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

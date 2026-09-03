"""
plot2_quick_grid.py
====================
ax=: every entry point draws into a supplied Axes, so quick functions compose
into any matplotlib layout you care to build.

Output: plots/plot2_quick_grid.png
"""
import sys; sys.path.insert(0, "..")  # noqa

import matplotlib.pyplot as plt

from plot2_data import by_month, nottem

from dkit.plot2 import get_theme, quick, save_figure


monthly = nottem()
rows = by_month(monthly)

theme = get_theme("dkit-light")
with theme.context():
    fig, axes = plt.subplots(2, 2, figsize=(11, 6))

quick.bar(rows, x="month", y="mean", ax=axes[0][0], title="bar")
quick.line(monthly, x="date", y="temp", ax=axes[0][1], title="line")
quick.area(rows, x="month", y="high", ax=axes[1][0], title="area")
quick.hist(monthly, "temp", bins=15, ax=axes[1][1], title="hist")
fig.tight_layout()

filename = "plots/plot2_quick_grid.png"
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

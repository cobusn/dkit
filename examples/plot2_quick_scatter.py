"""
plot2_quick_scatter.py
=======================
quick.scatter: size= may name a field, giving one marker area per row.

Output: plots/plot2_quick_scatter.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import quick, save_figure


rows = by_month(nottem())

filename = "plots/plot2_quick_scatter.png"
fig = quick.scatter(rows, x="mean", y="high", size="mean",
                     title="Mean against maximum", xlabel="Monthly mean (F)",
                     ylabel="Warmest year (F)")
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

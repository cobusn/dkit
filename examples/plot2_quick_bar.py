"""
plot2_quick_bar.py
===================
quick.bar: one call, one chart. No scale is named -- "month" holds strings so
the x axis is categorical, and "mean" holds floats so the y axis is linear.

Output: plots/plot2_quick_bar.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import quick, save_figure


rows = by_month(nottem())

filename = "plots/plot2_quick_bar.png"
fig = quick.bar(rows, x="month", y="mean", title="Average by month",
                 xlabel="Month", ylabel="Temperature (F)")
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

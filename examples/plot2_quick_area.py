"""
plot2_quick_area.py
====================
quick.area, coloured with a semantic name.

Output: plots/plot2_quick_area.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import quick, save_figure


rows = by_month(nottem())

filename = "plots/plot2_quick_area.png"
fig = quick.area(rows, x="month", y="high", title="Warmest year on record",
                  xlabel="Month", ylabel="Temperature (F)", color="negative")
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

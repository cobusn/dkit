"""
plot2_quick_line.py
====================
quick.line: "date" holds date objects, so this gets a real datetime axis
with calendar ticks -- without the caller asking for one.

Output: plots/plot2_quick_line.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import nottem

from dkit.plot2 import quick, save_figure


monthly = nottem()

filename = "plots/plot2_quick_line.png"
fig = quick.line(monthly, x="date", y="temp", title="Monthly mean, 1920-1939",
                  ylabel="Temperature (F)")
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

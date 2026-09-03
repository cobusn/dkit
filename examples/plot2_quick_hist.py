"""
plot2_quick_hist.py
====================
quick.hist. Binning is dkit.data.histogram's job; hist just draws the result.

Output: plots/plot2_quick_hist.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import nottem

from dkit.plot2 import quick, save_figure


monthly = nottem()

filename = "plots/plot2_quick_hist.png"
fig = quick.hist(monthly, "temp", bins=20, title="Distribution of monthly means",
                  xlabel="Temperature (F)")
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

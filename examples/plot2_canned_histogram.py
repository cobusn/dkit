"""
plot2_canned_histogram.py
==========================
canned.histogram: quick.hist draws the bars, canned.histogram adds the mean
line a report wants -- which is the whole difference between the two
surfaces.

Output: plots/plot2_canned_histogram.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import nottem

from dkit.plot2 import canned, save_figure


monthly = nottem()

filename = "plots/plot2_canned_histogram.png"
fig = canned.histogram(
    monthly, "temp", bins=15, label="Months", mean_label="20 year mean",
    title="How often a month is warm", xlabel="Temperature (F)",
)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

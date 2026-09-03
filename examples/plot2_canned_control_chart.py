"""
plot2_canned_control_chart.py
==============================
canned.control_chart: a band, a centre line, and the breaches marked. The
limits are fields on the rows, not something the chart computes -- which
limits are right is a decision about the process. Here it is two standard
deviations either side of the twenty year mean.

Output: plots/plot2_canned_control_chart.png
"""
import sys; sys.path.insert(0, "..")  # noqa

import statistics

from plot2_data import nottem

from dkit.plot2 import canned, save_figure, scale


monthly = nottem()
temps = [row["temp"] for row in monthly]
mean, sigma = statistics.mean(temps), statistics.stdev(temps)
observed = [
    dict(row, expected=mean, ucl=mean + 2 * sigma, lcl=mean - 2 * sigma)
    for row in monthly
    if row["year"] < 1925
]

filename = "plots/plot2_canned_control_chart.png"
fig = canned.control_chart(
    observed, x="date", y="temp", label="Monthly mean",
    breach_label="Unusual month", limits_label="Two sigma",
    expected_label="20 year mean",
    title="Nottingham Castle, 1920-1924",
    xlabel="Month", ylabel="Temperature (F)",
    xscale=scale.Time("Month", format="%b %Y"),
)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

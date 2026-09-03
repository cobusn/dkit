"""
plot2_canned_quadrant.py
==========================
canned.quadrant: a scatter cut into four by two reference lines. One row per
entity per period is all BostonMatrix needs; here the entity is a calendar
month and the period a year, so "growth" is whether that month has been
warming over the trailing window. The cuts are passed in rather than derived
from the plotted rows, so a filtered chart still divides where the whole
population divides.

Output: plots/plot2_canned_quadrant.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import nottem

from dkit.data.boston import BostonMatrix
from dkit.plot2 import canned, save_figure


monthly = nottem()
matrix = BostonMatrix(
    monthly, id_field="month", sequence_field="year", value_field="temp",
    window_size=8,
)

filename = "plots/plot2_canned_quadrant.png"
fig = canned.quadrant(
    matrix.classified, x=matrix.alias_growth, y=matrix.alias_median,
    y_center=matrix.median, label="Months",
    title=f"Warm and warming, {matrix.last_sequence}",
    xlabel="Warming per year (F)", ylabel="Median temperature (F)",
)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

for n in (1, 2, 3, 4):
    names = [row["month"] for row in matrix.quadrant(n)]
    print(f"Q{n}: {', '.join(names) or '-'}")

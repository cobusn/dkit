"""
plot2_canned_pareto.py
=======================
canned.pareto: ranked bars with the cumulative share on a second axis. The
analysis is built once, from dkit.data.pareto.ParetoAnalysis, with no
matplotlib import -- so a script can print the vital few without drawing
anything.

Output: plots/plot2_canned_pareto.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import titanic, titanic_groups

from dkit.data.pareto import ParetoAnalysis
from dkit.plot2 import canned, save_figure


fares = ParetoAnalysis(titanic_groups(titanic()), value_field="revenue",
                        label_field="group")

vital_few = fares.top_n_percent(80.0)
print(f"{len(vital_few)} of {len(fares)} groups are 80% of the revenue:")
for row in vital_few:
    print(f"  {row['group']:<24} {row['cum_percent']:5.1f}%")

filename = "plots/plot2_canned_pareto.png"
fig = canned.pareto(
    fares, label="Fares", cumulative_label="Cumulative share",
    title="Where the fare revenue came from", ylabel="Fares paid",
    rotation=45,
)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

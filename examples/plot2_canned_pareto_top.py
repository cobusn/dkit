"""
plot2_canned_pareto_top.py
============================
canned.pareto with top=: limits the bars without touching the arithmetic --
the last bar still reports its share of the whole, not of what is drawn.

Output: plots/plot2_canned_pareto_top.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import titanic, titanic_groups

from dkit.data.pareto import ParetoAnalysis
from dkit.plot2 import canned, save_figure


fares = ParetoAnalysis(titanic_groups(titanic()), value_field="revenue",
                        label_field="group")

filename = "plots/plot2_canned_pareto_top.png"
fig = canned.pareto(fares, top=4, title="The same analysis, top four only",
                     ylabel="Fares paid", rotation=45)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

"""
plot2_extensions_treemap_shared_colors.py
===========================================
The standalone classes exist as objects rather than functions because an
instance remembers which colour it gave each category: the same port is the
same colour in both panels here, even though the second holds a subset of
the first.

Output: plots/plot2_extensions_treemap_shared_colors.png
"""
import sys; sys.path.insert(0, "..")  # noqa

import matplotlib.pyplot as plt

from plot2_data import titanic, titanic_groups

from dkit.data import aggregation as agg
from dkit.plot2 import get_theme, save_figure
from dkit.plot2.matplotlib_extra import TreeMap


passengers = titanic()
groups = titanic_groups(passengers)
by_port = list(
    (agg.Aggregate() + agg.GroupBy("port") + agg.Count("fare").alias("passengers"))
    (passengers)
)
# a copy, not a view: groups is used again elsewhere
third = [dict(r) for r in groups if r["class"] == "Third"]

ports = TreeMap(value_format="{:,.0f}")
theme = get_theme("dkit-light")
with theme.context():
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
ports.draw(by_port, "port", "passengers", title="All passengers",
           theme=theme, ax=axes[0])
ports.draw(third, "port", "passengers", title="Third class only",
           theme=theme, ax=axes[1])
fig.suptitle("One instance, two draws: Cherbourg keeps its colour")

filename = "plots/plot2_extensions_treemap_shared_colors.png"
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

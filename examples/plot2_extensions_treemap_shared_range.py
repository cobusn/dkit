"""
plot2_extensions_treemap_shared_range.py
==========================================
A sequential treemap has no categories to remember, so the same "stays
comparable across panels" job is done by handing both draws one color_range.
Without it each panel would range over its own data and the same colour
would mean two different counts.

Output: plots/plot2_extensions_treemap_shared_range.png
"""
import sys; sys.path.insert(0, "..")  # noqa

import matplotlib.pyplot as plt

from plot2_data import titanic, titanic_groups

from dkit.plot2 import get_theme, save_figure
from dkit.plot2.matplotlib_extra import TreeMap


groups = titanic_groups(titanic())
counts = [r["passengers"] for r in groups]

shaded = TreeMap(norm="linear", value_format="{:,.0f}")
theme = get_theme("dkit-light")
with theme.context():
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
for ax, name in ((axes[0], "First"), (axes[1], "Third")):
    shaded.draw([r for r in groups if r["class"] == name], "port", "passengers",
                title=f"{name} class", color_range=(min(counts), max(counts)),
                theme=theme, ax=ax)
fig.suptitle("One color_range, two draws: the colour bars agree")

filename = "plots/plot2_extensions_treemap_shared_range.png"
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

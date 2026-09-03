"""
plot2_quick_scale_override.py
==============================
Overriding the inferred scale. Inference is a default, not a rule: pass a
scale to say something the data cannot -- here, that the axis should read as
a percentage of the maximum.

Output: plots/plot2_quick_scale_override.png
"""
import sys; sys.path.insert(0, "..")  # noqa

from plot2_data import by_month, nottem

from dkit.plot2 import quick, save_figure, scale


rows = by_month(nottem())
peak = max(r["high"] for r in rows)
for row in rows:
    row["share"] = row["mean"] / peak

filename = "plots/plot2_quick_scale_override.png"
fig = quick.bar(
    rows, x="month", y="share", title="Share of the record high",
    xscale=scale.Categorical("Month", rotation=45),
    yscale=scale.Percent("Share of maximum", whole=1.0),
)
save_figure(fig, filename, dpi=110)
print(f"wrote {filename}")

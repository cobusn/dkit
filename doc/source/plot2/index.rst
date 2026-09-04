*****
plot2
*****

.. toctree::
   :maxdepth: 1

   example_usage
   themes
   scales
   declarative_plot
   geoms
   quick
   canned
   extensions

.. automodule:: dkit.plot2

Overview
========

``dkit.plot2`` is a matplotlib toolkit for producing a *family* of plots that
share one look and feel: colours, colour maps, fonts, semantic colours and
number formats. It replaces ``dkit.plot``, which serialised a
ggplot-inspired grammar to JSON and rendered it through one of three
backends.

Data is always **rows**: an iterable of mappings, with fields referenced by
name. Nothing is computed for you -- binning and aggregation stay in
``dkit.data``, and plot2 draws the result.

Two rules hold everywhere in the package, and are worth knowing before
anything else:

* **Every entry point accepts ``ax=``.** Given an Axes, a plot draws into it
  and creates no figure of its own. That is what makes faceting, embedding
  into a hand-built grid, and ``dkit.doc2.document.wrap_matplotlib``
  integration possible.
* **Themes are applied scoped.** Rendering happens inside
  ``theme.context()``, so a themed plot leaves global rcParams untouched and
  cannot restyle whatever is drawn next.

Two surfaces
------------

There is one implementation with two surfaces over it.

**The declarative core** (:doc:`declarative_plot`). Layers are values and a
:class:`~dkit.plot2.plot.Plot` is a reusable specification that draws
nothing until :meth:`~dkit.plot2.plot.Plot.render` is called:

.. code-block:: python

   from dkit.plot2 import Plot, geom, scale

   p = Plot(
       geom.Bar("Revenue", x="month", y="revenue"),
       geom.Line("Target", x="month", y="target", color="highlight"),
       x=scale.Categorical("Month"),
       y=scale.Linear("Revenue"),
       title="2026 Sales",
   )
   fig = p.render(rows)             # reusable: p.render(other_rows)
   p2 = p.add(geom.HLine(0.0))      # a NEW Plot; p is unchanged

**The recipe functions** (:doc:`quick` and :doc:`canned`). One call per
chart, for the common case. :mod:`dkit.plot2.quick` covers single-series
charts and :mod:`dkit.plot2.canned` the multi-layer report charts:

.. code-block:: python

   from dkit.plot2 import canned, quick

   fig = quick.bar(rows, x="month", y="revenue", title="2026 Sales")
   fig = canned.pareto(rows, value_field="sales", entity_name="Product")

The recipe functions contain no drawing logic: each one is a signature,
argument defaulting, and one declarative ``Plot`` expression. Once the
*layer structure* becomes the caller's choice -- an overlay, a threshold, a
twin axis -- use ``Plot`` directly rather than looking for a keyword
argument.

Saving
------

:meth:`~dkit.plot2.plot.Plot.save` renders and writes in one call, inside
the theme's context, so the style sheet's ``savefig.*`` rcParams apply. A
figure *returned* by a ``quick`` or ``canned`` function arrives outside that
context, and saving it with a plain ``fig.savefig`` silently loses those
settings -- including ``savefig.bbox: tight``, which is what keeps axis
labels from being clipped. Use :func:`~dkit.plot2.plot.save_figure` for a
returned figure:

.. code-block:: python

   Plot(geom.Line("a", x="date", y="value")).save(rows, "line.png")

   fig = quick.bar(rows, x="month", y="revenue")
   save_figure(fig, "revenue.png")

.. autofunction:: dkit.plot2.plot.save_figure

See :doc:`../howto_plot2_styles` for how to author a style sheet, and
:doc:`example_usage` for the runnable scripts behind every figure on these
pages.

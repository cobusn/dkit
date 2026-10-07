*************
Example Usage
*************

Every figure on these pages comes from a runnable script in ``examples/``,
one script per plot. Each writes its figure to ``examples/plots/``, and
``examples/plot2_data.py`` holds the shared data loading -- not an example
itself, just what every script below imports rather than repeating.

.. _example-usage-declarative-plot:

Declarative Plot
=================

A real datetime axis
---------------------

scale.Time hands the axis to matplotlib's date locator, so ticks land on
real calendar boundaries rather than on row numbers -- a case ``dkit.plot``
(v1) could not express, because it forced every axis onto integer positions.

.. literalinclude:: ../../../examples/plot2_declarative_time_axis.py
   :language: python

.. image:: ../../../examples/plots/plot2_declarative_time_axis.png
   :align: center

Layering: a band, a line and a filtered scatter
-------------------------------------------------

Each layer reads the same rows. ``where`` filters one layer only, which is
how the outlying months get their own colour without a second data set.

.. literalinclude:: ../../../examples/plot2_declarative_band_scatter.py
   :language: python

.. image:: ../../../examples/plots/plot2_declarative_band_scatter.png
   :align: center

Bars, coloured by sign
-----------------------

``color="signed"`` picks the theme's positive, negative or neutral colour
per row. dkit.plot (v1) needed two overlapping bar series for the same
effect.

.. literalinclude:: ../../../examples/plot2_declarative_signed_bars.py
   :language: python

.. image:: ../../../examples/plots/plot2_declarative_signed_bars.png
   :align: center

Grouped bars
-------------

Grouping is two layers at half width, shifted by ``offset``.

.. literalinclude:: ../../../examples/plot2_declarative_grouped_bars.py
   :language: python

.. image:: ../../../examples/plots/plot2_declarative_grouped_bars.png
   :align: center

A twin right-hand axis
-----------------------

``axis="right"`` puts a layer on a second y axis with its own scale. Any
geom can use it, and the legend still collects both axes.

.. literalinclude:: ../../../examples/plot2_declarative_twin_axis.py
   :language: python

.. image:: ../../../examples/plots/plot2_declarative_twin_axis.png
   :align: center

Area and stem
--------------

.. literalinclude:: ../../../examples/plot2_declarative_area_stem.py
   :language: python

.. image:: ../../../examples/plots/plot2_declarative_area_stem.png
   :align: center

Horizontal bars
-----------------

``x`` is always the horizontal field and ``y`` the vertical one, whichever
way the bars point, so a horizontal bar chart is a Linear x and a
Categorical y.

.. literalinclude:: ../../../examples/plot2_declarative_horizontal_bars.py
   :language: python

.. image:: ../../../examples/plots/plot2_declarative_horizontal_bars.png
   :align: center

.. _example-usage-themes:

Themes
======

Comparing the bundled themes
------------------------------

The same ``Plot``, rendered under each bundled theme. It is defined once;
``replace()`` derives a new ``Plot`` with a different theme rather than
mutating this one, so the original stays reusable.

.. literalinclude:: ../../../examples/plot2_themes_compare.py
   :language: python

.. image:: ../../../examples/plots/plot2_themes_compare_dkit-light.png
   :align: center

.. image:: ../../../examples/plots/plot2_themes_compare_dkit-dark.png
   :align: center

.. image:: ../../../examples/plots/plot2_themes_compare_dkit-print.png
   :align: center

A custom theme
---------------

``Theme.replace()`` states only what differs, and keeps everything else --
fonts, figure size, grid -- from the theme it derives from.

.. literalinclude:: ../../../examples/plot2_themes_custom.py
   :language: python

.. image:: ../../../examples/plots/plot2_themes_custom.png
   :align: center

Registering a theme and making it the default
-------------------------------------------------

``register_theme()`` buys reaching a theme by *name*. ``set_default_theme()``
buys not repeating ``theme=`` on every plot.

.. literalinclude:: ../../../examples/plot2_themes_registry.py
   :language: python

.. image:: ../../../examples/plots/plot2_themes_registry.png
   :align: center

.. _example-usage-quick:

Quick
=====

One call, one chart
---------------------

``quick`` infers each axis from the data: a string field gets a categorical
axis, a date field a real time axis, and a number a linear one.

.. literalinclude:: ../../../examples/plot2_quick_bar.py
   :language: python

.. image:: ../../../examples/plots/plot2_quick_bar.png
   :align: center

.. literalinclude:: ../../../examples/plot2_quick_line.py
   :language: python

.. image:: ../../../examples/plots/plot2_quick_line.png
   :align: center

.. literalinclude:: ../../../examples/plot2_quick_area.py
   :language: python

.. image:: ../../../examples/plots/plot2_quick_area.png
   :align: center

.. literalinclude:: ../../../examples/plot2_quick_scatter.py
   :language: python

.. image:: ../../../examples/plots/plot2_quick_scatter.png
   :align: center

.. literalinclude:: ../../../examples/plot2_quick_hist.py
   :language: python

.. image:: ../../../examples/plots/plot2_quick_hist.png
   :align: center

Overriding the inferred scale
--------------------------------

Inference is a default, not a rule: pass a scale to say something the data
cannot -- here, that the axis should read as a percentage of the maximum.

.. literalinclude:: ../../../examples/plot2_quick_scale_override.py
   :language: python

.. image:: ../../../examples/plots/plot2_quick_scale_override.png
   :align: center

``ax=``: several quick calls on one figure
---------------------------------------------

Every entry point draws into a supplied Axes, so quick functions compose
into any matplotlib layout you care to build.

.. literalinclude:: ../../../examples/plot2_quick_grid.py
   :language: python

.. image:: ../../../examples/plots/plot2_quick_grid.png
   :align: center

Faceting
---------

:meth:`~dkit.plot2.plot.Plot.facet` draws one panel per group, with the
scales collected from all the rows so that the panels stay comparable.

.. literalinclude:: ../../../examples/plot2_quick_facet.py
   :language: python

.. image:: ../../../examples/plots/plot2_quick_facet.png
   :align: center

``share_y=False`` gives each panel its own y range instead of a shared one.

.. literalinclude:: ../../../examples/plot2_quick_facet_free.py
   :language: python

.. image:: ../../../examples/plots/plot2_quick_facet_free.png
   :align: center

.. _example-usage-canned:

Canned
======

A control chart
-----------------

The limits are fields on the rows, not something the chart computes: which
limits are right is a decision about the process.

.. literalinclude:: ../../../examples/plot2_canned_control_chart.py
   :language: python

.. image:: ../../../examples/plots/plot2_canned_control_chart.png
   :align: center

A histogram with its mean marked
-----------------------------------

``quick.hist`` draws the bars; ``canned.histogram`` adds the line a report
wants, which is the whole difference between the two surfaces.

.. literalinclude:: ../../../examples/plot2_canned_histogram.py
   :language: python

.. image:: ../../../examples/plots/plot2_canned_histogram.png
   :align: center

A pareto chart
---------------

The analysis is built once, from ``dkit.data.pareto.ParetoAnalysis``, with
no matplotlib import -- so a script can print the vital few without drawing
anything.

.. literalinclude:: ../../../examples/plot2_canned_pareto.py
   :language: python

.. image:: ../../../examples/plots/plot2_canned_pareto.png
   :align: center

``top=`` limits the bars without touching the arithmetic: the last bar
still reports its share of the whole, not of what is drawn.

.. literalinclude:: ../../../examples/plot2_canned_pareto_top.py
   :language: python

.. image:: ../../../examples/plots/plot2_canned_pareto_top.png
   :align: center

A boston matrix and its quadrant chart
------------------------------------------

The cuts are passed in rather than derived from the plotted rows, so a
filtered chart still divides where the whole population divides.

.. literalinclude:: ../../../examples/plot2_canned_quadrant.py
   :language: python

.. image:: ../../../examples/plots/plot2_canned_quadrant.png
   :align: center

The same analyses in a document
-----------------------------------

``doc.wrap_matplotlib`` takes any function returning a figure, so a canned
chart goes into a report with no plot grammar to serialise and no decorator
to remember. ``dkit.doc2.canned`` tabulates the same analysis objects used
to draw the charts, so a chart and its table cannot disagree.

.. literalinclude:: ../../../examples/plot2_canned_report.py
   :language: python

.. image:: ../../../examples/plots/plot2_canned_report_pareto.png
   :align: center

.. _example-usage-geoms:

Geoms
=====

A heat map
-----------

A value over two categorical dimensions at once. Twenty years by twelve
months is 240 cells: a line per year would be unreadable, and the seasonal
band is obvious here. Unlike TreeMap or Slope, a heat map is an ordinary
geom -- it draws inside two Categorical scales.

.. literalinclude:: ../../../examples/plot2_geoms_heatmap.py
   :language: python

.. image:: ../../../examples/plots/plot2_geoms_heatmap.png
   :align: center

Inside a Plot, annotated
--------------------------

``annotate=True`` writes each value into its cell in whichever of black or
white is readable there. ``where=`` drops the one group holding a single
passenger, whose mean is not worth reading -- the cell is left blank rather
than drawn as zero, because a gap and a real zero are different facts.

.. literalinclude:: ../../../examples/plot2_geoms_heatmap_annotated.py
   :language: python

.. image:: ../../../examples/plots/plot2_geoms_heatmap_annotated.png
   :align: center

Faceting a heat map
---------------------

The colour range has to be given explicitly: scales are shared across
panels but a colour map is a layer's own business, so each panel would
otherwise range over its own data and the two pictures could not be
compared.

.. literalinclude:: ../../../examples/plot2_geoms_heatmap_facet.py
   :language: python

.. image:: ../../../examples/plots/plot2_geoms_heatmap_facet.png
   :align: center

.. _example-usage-extensions:

Extensions
==========

The quick calls
-----------------

One rectangle per row, its area proportional to the passenger count. The
cells are coloured by class, so the three blocks read as blocks even though
squarify lays them out for aspect ratio rather than by group.

.. literalinclude:: ../../../examples/plot2_extensions_quick_treemap.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_quick_treemap.png
   :align: center

What moved, and by how much. Every month warmed, but not equally: the
crossing lines are the point of the chart.

.. literalinclude:: ../../../examples/plot2_extensions_quick_slope.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_quick_slope.png
   :align: center

Sequential colouring
-----------------------

``norm=`` switches the treemap from one colour per category to a colour
ramp over the value being plotted, with a colour bar to read it by.

.. literalinclude:: ../../../examples/plot2_extensions_treemap_sequential.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_treemap_sequential.png
   :align: center

``norm="log"`` for the same data: the group sizes here span 1 to 142, and on
a linear ramp everything below about thirty is the same dark colour; the log
ramp separates the small groups, at the cost of overstating their
differences.

.. literalinclude:: ../../../examples/plot2_extensions_treemap_log.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_treemap_log.png
   :align: center

The standalone classes, drawn more than once
------------------------------------------------

The reason these classes exist as objects rather than functions: an
instance remembers which colour it gave each category, so the same port is
the same colour in both panels even though the second holds a subset of the
first.

.. literalinclude:: ../../../examples/plot2_extensions_treemap_shared_colors.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_treemap_shared_colors.png
   :align: center

A sequential treemap has no categories to remember, so the same job is done
by handing both draws one ``color_range``.

.. literalinclude:: ../../../examples/plot2_extensions_treemap_shared_range.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_treemap_shared_range.png
   :align: center

The slope plot the same way: the second panel zooms in on the second half
of the year, and because one instance drew both, each month keeps the
colour it had in the crowded overview. This depends on drawing order --
colours are assigned in order of first appearance, so drawing the subset
first would shift every other month in the overview.

.. literalinclude:: ../../../examples/plot2_extensions_slope_shared.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_slope_shared.png
   :align: center

Inside a Plot
--------------

The geoms buy what the standalone classes do not have: a theme resolved by
name, a title, a figure size in centimetres, ``where=``, ``save()`` and
``facet()``. Everything else is passed straight through to the wrapped
class.

.. literalinclude:: ../../../examples/plot2_extensions_treemap_geom.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_treemap_geom.png
   :align: center

``geom.Slope`` takes its value-axis label from the Plot's y scale, which is
the one part of the ordinary scale machinery an exclusive layer still uses.

.. literalinclude:: ../../../examples/plot2_extensions_slope_geom.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_slope_geom.png
   :align: center

Faceting a treemap
--------------------

One Plot, one panel per class. The layer holds a single wrapped TreeMap
instance which every panel draws through, so a port keeps its colour across
the panels -- exactly what makes the three panels comparable.

.. literalinclude:: ../../../examples/plot2_extensions_treemap_facet.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_treemap_facet.png
   :align: center

A calendar heatmap
---------------------

One square per day, shaded by value -- the GitHub contribution chart shape.
``start_date``/``end_date`` default to the data's own span, which here is
440 days, not a calendar year.

.. literalinclude:: ../../../examples/plot2_extensions_calendar_heatmap.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_calendar_heatmap.png
   :align: center

``vcenter=0`` for signed data: a day with no net change is pinned to the
middle of the colour map, rather than to whichever end a plain linear scale
happens to put "zero" at.

.. literalinclude:: ../../../examples/plot2_extensions_calendar_heatmap_diverging.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_calendar_heatmap_diverging.png
   :align: center

A calendar heatmap draws one span per ``draw()`` call, so several spans next
to each other -- one calendar per year, here -- come from
:meth:`~dkit.plot2.plot.Plot.facet` rather than from the layer itself.

.. literalinclude:: ../../../examples/plot2_extensions_calendar_heatmap_facet.py
   :language: python

.. image:: ../../../examples/plots/plot2_extensions_calendar_heatmap_facet.png
   :align: center

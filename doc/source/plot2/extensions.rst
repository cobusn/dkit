**********
Extensions
**********

Treemap and slope plot answer questions the ordinary geoms cannot. A treemap
shows how a total divides up when there are too many parts for a bar chart.
A slope plot shows what moved between two points, and by how much, for many
series at once. Both own their whole Axes rather than adding marks to a pair
of scales, so they come in two forms:

* a standalone class in :mod:`dkit.plot2.matplotlib_extra`, built once and
  drawn repeatedly. Colours it has assigned persist between draws, which is
  what makes a set of plots readable side by side.
* a geom wrapping that class, so a ``Plot`` can give it a theme, a title, a
  figure size, ``save()`` and ``facet()``.

Because they own their whole Axes, they cannot share a plot with an ordinary
geom -- mixing is refused at construction time rather than producing a
picture with two things drawn over each other:

.. code-block:: python

   from dkit.plot2 import Plot, geom

   Plot(
       geom.TreeMap("cell", "passengers"),
       geom.Line("Fare", x="cell", y="mean_fare"),
   )
   # DKitPlotException: an exclusive layer cannot share a plot

See :ref:`example-usage-extensions` for runnable examples.

Geom adapters
=============

.. automodule:: dkit.plot2.geom.standalone

.. autoclass:: dkit.plot2.geom.standalone.TreeMap
   :members:

.. autoclass:: dkit.plot2.geom.standalone.Slope
   :members:

Standalone classes
===================

.. automodule:: dkit.plot2.matplotlib_extra

.. autoclass:: dkit.plot2.matplotlib_extra.TreeMap
   :members:
   :inherited-members:

.. autoclass:: dkit.plot2.matplotlib_extra.SlopePlot
   :members:
   :inherited-members:

.. autofunction:: dkit.plot2.matplotlib_extra.contrast_color

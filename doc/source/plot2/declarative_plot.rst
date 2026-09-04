****************
Declarative Plot
****************

.. automodule:: dkit.plot2.plot

Plot
====

.. autoclass:: dkit.plot2.plot.Plot
   :members:

Layer
=====

A layer is one drawn thing. Its first positional argument is its label,
which becomes its legend entry, and every layer accepts ``where=`` to filter
its own rows, ``axis=`` to choose the left or right y axis, and ``color=``
taking a literal colour, a semantic name (``"positive"``, ``"negative"``,
``"neutral"``, ``"highlight"``), ``"signed"`` for per-row colouring by sign,
or None for the next colour in the theme's cycle.

.. inheritance-diagram:: dkit.plot2.geom.Line dkit.plot2.geom.Bar
                         dkit.plot2.geom.Scatter dkit.plot2.geom.Area
                         dkit.plot2.geom.Band dkit.plot2.geom.Stem
                         dkit.plot2.geom.HeatMap dkit.plot2.geom.HLine
                         dkit.plot2.geom.VLine dkit.plot2.geom.Text
                         dkit.plot2.geom.TreeMap dkit.plot2.geom.Slope

.. autoclass:: dkit.plot2.plot.Layer
   :members:

.. autoclass:: dkit.plot2.plot.RenderContext
   :members:

Rows
====

Every entry point consumes rows through a :class:`~dkit.plot2.frame.Frame`,
which holds them, applies ``where=`` filters, and resolves field names to
values. A caller building a ``Plot`` directly never constructs one --
``render()`` and ``save()`` build it from whatever rows are passed in -- but
it is what a custom geom's :meth:`~dkit.plot2.plot.Layer.draw` reads through
``RenderContext.frame``.

.. autoclass:: dkit.plot2.frame.Frame
   :members:

See :ref:`example-usage-declarative-plot` for runnable examples.

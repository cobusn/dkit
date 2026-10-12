*************************************
How to: write a project-specific geom
*************************************

The built-in geoms in :doc:`geoms` and the Axes-owning plots in
:doc:`extensions` cover the common cases, but a project occasionally needs a
chart type dkit does not ship. That geom does not need to live inside dkit:
it lives in the project's own code, subclasses
:class:`~dkit.plot2.geom.base.Geom`, and from then on composes with every
built-in geom, scale and theme the same way :class:`~dkit.plot2.geom.line.Line`
or :class:`~dkit.plot2.geom.bar.Bar` do.

The contract
============

A geom is a value, not a side effect: ``__init__`` only stores its
configuration, and all the drawing happens in one method, called once per
render:

.. code-block:: python

   class MyGeom(Geom):
       def draw(self, ctx: RenderContext) -> None:
           ...

:class:`~dkit.plot2.plot.RenderContext` is what makes that split possible --
it carries everything specific to *this* render (the Axes, the filtered
rows, the active theme, the scales) so the geom object itself holds no
state and can be reused across several plots. The pieces a ``draw()``
typically needs:

``ctx.x_values(field)`` / ``ctx.y_values(field)``
   the field's values, already encoded through the plot's x or y scale --
   a category turned into a tick position, a date into a matplotlib date
   number. Draw with these, never with the raw row values.

``ctx.ax``
   the Axes to draw on: the primary one, or the twin created for
   ``axis="right"``. Call ordinary matplotlib methods on it.

``self._color(ctx)``
   this layer's resolved colour, honouring ``color="positive"`` and the
   other semantic names, an explicit matplotlib colour, or the theme's
   categorical cycle when ``color`` was left as None. ``ctx.color(spec)``
   resolves any *other* colour-like argument the same way, which is how a
   geom with more than one colour (e.g. a low end and a high end) stays
   theme-aware too.

``self._common(**extra)``
   ``label``, ``alpha`` and ``zorder`` as matplotlib keywords, merged with
   whatever else is passed, with every ``None`` dropped. Omitting a keyword
   lets the style sheet decide, so a geom must never pass ``None`` for
   something it has no opinion about -- that would silently override the
   theme with a matplotlib default. Reach for ``self._common()`` instead of
   building that dict by hand.

Only the arguments that are not already on :class:`~dkit.plot2.geom.base.Geom`
--``label``, ``x``, ``y``, ``color``, ``alpha``, ``where``, ``axis``,
``zorder`` -- need a docstring: inherit the rest by example from an existing
geom in ``dkit/plot2/geom/``, e.g. ``line.py`` or ``bar.py``.

A worked example: Dumbbell
===========================

A dumbbell plot compares two values per category -- a before/after score,
here -- with a line between them and a marker at each end. It is not a geom
dkit ships, and does not need to become one to be used like one:

.. literalinclude:: ../../../examples/plot2_howto_custom_geom.py
   :language: python

.. image:: ../../../examples/plots/plot2_howto_custom_geom.png
   :align: center

A few points this example leans on:

* ``low`` and ``high`` are plain field names, resolved through
  ``ctx.y_values()`` like any geom's ``y`` -- a custom geom is free to take
  more than one value field; it is only the *base class's* ``x``/``y`` pair
  that every geom shares.
* ``low_color``/``high_color`` go through ``ctx.color()``, not used
  directly, so ``low_color="neutral"`` resolves against the active theme
  the same way ``color="positive"`` would on a built-in geom.
* The label sits on exactly one artist -- the ``high`` scatter, drawn once
  for the whole series -- rather than on the per-category line, which is
  drawn in a loop and would otherwise add one identical legend entry per
  category. :class:`~dkit.plot2.geom.line.Area` makes the same choice, for
  the same reason, between its fill and its line.

Discrete axes and ``domain_values``
====================================

:meth:`Layer.domain_values() <dkit.plot2.plot.Layer.domain_values>` tells a
*discrete* scale (:class:`~dkit.plot2.scale.Categorical`, not
:class:`~dkit.plot2.scale.Linear`) which values a layer contributes, so the
scale can order and position them before any ``draw()`` runs. The base
implementation reads it from ``self.x`` or ``self.y`` -- correct for
``Dumbbell``'s categorical ``x``, since that is still one field, and the
reason the example above needs no override. A geom whose *value* axis is
itself discrete and spread across more than one field would need to
override it, along the lines of:

.. code-block:: python

   def domain_values(self, frame, which="x"):
       if which == "y":
           return frame.values(self.low) + frame.values(self.high)
       return super().domain_values(frame, which)

An Axes-owning plot instead
============================

``Dumbbell`` adds marks to a shared pair of scales, which is what most new
chart types want. A chart that lays out its own coordinate system entirely
-- a treemap's nested rectangles, a slope plot's two label columns -- does
not fit that contract and should follow :class:`~dkit.plot2.matplotlib_extra.TreeMap`,
:class:`~dkit.plot2.matplotlib_extra.SlopePlot` or
:class:`~dkit.plot2.matplotlib_extra.CalendarHeatmap` instead: a plain class
that takes an ``ax=`` and draws its own Axes contents directly, optionally
wrapped as a layer the way :mod:`dkit.plot2.geom.standalone` wraps those
three, to pick up a theme, a title, figure sizing and ``facet()`` for free.
See :doc:`extensions` for how that wrapping works.

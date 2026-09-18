***********************************
How to: author a plot2 style sheet
***********************************

:mod:`dkit.plot2` styles plots in two layers, and knowing which layer owns
what saves most of the trouble:

* **rcParams live in native matplotlib ``.mplstyle`` files.** Fonts, figure
  size, colours of axes and grid, spine visibility, tick direction, the
  series colour cycle, legend and save behaviour — anything matplotlib can
  already express is expressed there, and matplotlib validates it.
* **:class:`~dkit.plot2.theme.Theme` adds only what rcParams cannot express.**
  Sequential and diverging colour maps, the four semantic colours
  (``positive``, ``negative``, ``neutral``, ``highlight``) and default number
  and date formats. There is no rcParam for "the colour a loss is drawn in",
  which is the whole reason ``Theme`` exists.

So a house style is usually one ``.mplstyle`` file plus a small ``Theme``
that names it.

Writing the style sheet
=======================

A style sheet is a plain text file of ``key: value`` lines:

.. code-block:: ini

   # house.mplstyle: our style, layered on top of dkit-light

   font.family:            sans-serif
   font.size:              10

   axes.titleweight:       bold
   axes.spines.top:        False
   axes.spines.right:      False
   axes.grid:              True
   axes.grid.axis:         y
   axes.prop_cycle:        cycler('color', ['1f3b57', 'c8007c', 'e8a33d'])

   legend.fontsize:        x-small

Two gotchas account for most first attempts failing:

**``#`` starts a comment, so colours are written bare.** Write ``05386E``,
not ``#05386E`` — the leading ``#`` turns the rest of the line into a comment
and matplotlib then reports a missing value. This is easy to miss because
every other place in the toolkit, including ``Theme``'s own semantic
colours, takes ordinary ``#RRGGBB`` strings.

**``figure.figsize`` is in inches, while plot2's ``width`` and ``height`` are
in centimetres.** ``figure.figsize: 6.30, 2.76`` is 16cm × 7cm; the same size
through the toolkit is ``Theme(width=16.0, height=7.0)`` or
``Plot(width=16.0, height=7.0)``. Both routes end up in ``figure.figsize``,
so whichever is applied last wins, and :data:`~dkit.plot2.theme.CM_TO_INCH` is the
conversion if you need to do it by hand.

The authoritative list of valid keys is the matplotlibrc file shipped with
your matplotlib:

.. code-block:: sh

   python -c "import matplotlib; print(matplotlib.matplotlib_fname())"

An invalid key is not silently ignored — matplotlib raises when the sheet is
applied, which is why the bundled styles have a test that does nothing but
load every one of them.

Using it
========

``rc`` accepts a bundled name, a path to a file, an rcParams mapping, or a
list of any of those applied in order, later winning. That layering is how
the bundled themes are built, and how a house sheet states only its
differences:

.. code-block:: python

   from dkit.plot2 import Theme, register_theme, set_default_theme, get_theme

   house = Theme(
       rc=["dkit-light", "house.mplstyle"],
       positive="#1b7f4b",
       negative="#a4243b",
       highlight="#c8007c",
       number_format="{x:,.1f}",
   )

Deriving from an existing theme, which keeps everything you did not mention:

.. code-block:: python

   house = get_theme("dkit-light").replace(highlight="#c8007c")

A ``Theme`` is accepted anywhere a theme name is, so nothing further is
needed to use it:

.. code-block:: python

   fig = quick.bar(rows, x="month", y="revenue", theme=house)

Registering it buys reaching it *by name* — from a configuration file, a
command line argument, or a plot written before the theme existed. Setting a
default buys not repeating ``theme=``:

.. code-block:: python

   register_theme("house", house)
   set_default_theme("house")

   fig = quick.bar(rows, x="month", y="revenue")     # uses house

``set_default_theme`` returns the spec that was in effect, which makes save
and restore a single line. It is a global mutation: reach for it in a script,
a notebook or an application's startup, and pass ``theme=`` explicitly in
library code.

Bundled styles
==============

Three sheets ship with the package, and ``dkit-dark`` and ``dkit-print`` are
both meant to be layered *on top of* ``dkit-light`` rather than used alone:

``dkit-light``
   the default: white background, 16cm × 7cm, sans-serif, y grid only

``dkit-dark``
   colour overrides only, for a dark background

``dkit-print``
   300 dpi, serif, no grid, type 42 fonts for PDF and PS

:func:`~dkit.plot2.theme.bundled_styles` lists them, and
:func:`~dkit.plot2.theme.resolve_style` shows what any spec resolves to.

Choosing colours
================

Three constraints are worth designing to, because they are not obvious until
a figure looks wrong:

**Keep ``highlight`` out of the cycle.** ``highlight`` is for the one series
that matters, so it has to be a colour ``axes.prop_cycle`` cannot also
produce. If it duplicates a cycle entry, a highlighted layer is
indistinguishable from whichever series happens to draw in that slot. The
bundled themes reserve a magenta for it, which none of their cycles contains.

**Keep the semantic four distinct from each other.** ``positive`` and
``negative`` carry meaning — ``geom.Bar(color="signed")`` colours every bar
by the sign of its own value — and ``neutral`` has to read as de-emphasised
against both.

**Order the cycle by how often it is used.** The first entry draws the first
layer of every plot, so it should be the colour the house style is known by,
and adjacent entries should be distinguishable side by side in a legend, not
merely different.

Saving
======

``savefig.*`` keys — including ``savefig.bbox: tight``, which the bundled
sheets set to stop axis labels being clipped — are rcParams like any other,
so they only apply inside the theme's context. :meth:`Plot.save
<dkit.plot2.plot.Plot.save>` saves within that context and gets them. A figure
*returned* by a ``quick`` or ``canned`` function arrives outside it, so save
those with :func:`~dkit.plot2.plot.save_figure` rather than ``fig.savefig``:

.. code-block:: python

   from dkit.plot2 import save_figure

   fig = quick.bar(rows, x="month", y="revenue")
   save_figure(fig, "revenue.png", dpi=110)

What plot2 does not read
========================

``dkit.plot`` v1 read a ``matplotlib:`` block out of a YAML stylesheet
(``examples/stylesheet.yaml``). plot2 does not: there is no
``Theme.from_yaml`` and no YAML bridge. Style state belongs in a file
matplotlib itself validates, and a theme is ordinary Python.

Using a document style pack
===========================

Document style packs use the same two-layer model. The manifest supplies the
semantic chart palette, dimensions, and typography role, while the pack's
``screen.mplstyle`` or ``print.mplstyle`` supplies native matplotlib settings.
The document builder applies the selected variant in a scoped context:

.. code-block:: console

   dk build doc --style dkit-blue --format html \
      --title "Quarterly report" --output report.html report.md

The context is restored after the build, so selecting a document style does
not change the global plot2 default or affect later plots. Code that creates
plots independently can load a registered pack through the style-pack API
and pass the resulting ``Theme`` explicitly.

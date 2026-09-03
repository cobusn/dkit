**************************************
How to: CLI-driven data exploration
**************************************

The :doc:`tutorial` and :doc:`howto_etl_model` used the Python API
directly. Everything shown there — inspecting a file, inferring a
schema, and converting between formats — can also be done from the
shell with the ``dk`` command, without writing a script. This guide
works through the same workflow using ``dk xplore`` and ``dk schemas``
against the sample ``titanic.csv`` file, then runs the resulting
conversion with ``dk run etl``.

See the :doc:`cli` reference for the full option list of every
subcommand used here.

Looking at a file
====================

``dk xplore head`` prints the first few rows of any supported source as
a table, without needing a model or a schema:

.. code-block:: bash

   dk xplore head examples/data/titanic.csv -n 3

.. code-block:: text

     PassengerId    Pclass  Name                              Sex       Age    SibSp    Parch    Ticket    Fare  Cabin    Embarked
   -------------  --------  --------------------------------  ------  -----  -------  -------  --------  ------  -------  ----------
             892         3  Kelly, Mr. James                  male    34.50        0        0    330911    7.83           Q
             893         3  Wilkes, Mrs. James (Ellen Needs)  female  47.00        1        0    363272    7.00           S
             894         2  Myles, Mr. Thomas Francis         male    62.00        0        0    240276    9.69           Q

``dk xplore peek`` shows the same rows as JSON instead of a table, and
``dk xplore fields`` lists just the field names:

.. code-block:: bash

   dk xplore fields examples/data/titanic.csv

.. code-block:: text

   Age  Cabin  Embarked  Fare  Name  Parch  PassengerId  Pclass  Sex  SibSp  Ticket

Summarising and counting
===========================

``dk xplore summary`` reports descriptive statistics for one numeric
field:

.. code-block:: bash

   dk xplore summary examples/data/titanic.csv -d Pclass

.. code-block:: text

   Observations:       418
   Minimum:            1.000000
   Maximum:            3.000000
   Mean:               2.265550
   Median:             3.000000
   Standard deviation: 0.841840
   Variance:           0.708690
   IQR:                2.000000

``dk xplore count`` groups by a field and counts occurrences;
``dk xplore distinct`` lists the distinct values of a field instead:

.. code-block:: bash

   dk xplore count examples/data/titanic.csv -d Sex

.. code-block:: text

   Sex       count
   ------  -------
   male        266
   female      152

.. note::
   ``summary`` and ``histogram`` require the field to be fully numeric
   with no blank values. ``Fare`` in the sample file has one blank
   value, which raises a ``ValueError``; ``Pclass``, ``SibSp`` and
   ``Parch``, used below, have none.

Plotting and histograms
===========================

``dk xplore histogram`` draws a histogram for a numeric field, and
``dk xplore plot`` draws one field against another. Both are built on
:doc:`plot2/index`, and both default to writing straight to the terminal
rather than a file.

Titanic's numeric fields are mostly small, discrete counts (``Pclass``,
``SibSp``, ``Parch``), which do not make for much of a picture, so this
section switches to ``examples/data/mpg.jsonl`` -- fuel economy for 117
cars, with continuous, correlated fields that plot well:

.. code-block:: bash

   dk xplore histogram -d displ examples/data/mpg.jsonl

This renders inline as a real image, not ASCII art — ``dk`` shells out to
`chafa <https://hpjansson.org/chafa/>`_, which draws it using whichever
graphics protocol your terminal supports (Kitty, iTerm2 or Sixel), falling
back to coloured Unicode block characters if it supports none of them. That
means what you see depends on the terminal: a modern one shows a proper
raster image; an old one, or a plain SSH session through a dumb pipe, still
shows something readable. If ``chafa`` is not installed, both commands
raise an error naming the package to install (``chafa`` is packaged for
Ubuntu/Debian in ``universe``, and for RHEL/Fedora via EPEL).

A terminal render cannot be captured as a static page, so here is the same
command's ``-o`` output instead -- the picture is identical either way, only
the destination differs:

.. image:: ../../examples/plots/dk_xplore_histogram.png
   :align: center

``dk xplore plot`` takes an ``-x`` and a ``-y`` field, and a ``--type``:

.. code-block:: bash

   dk xplore plot -x cty -y hwy --type scatter examples/data/mpg.jsonl

.. image:: ../../examples/plots/dk_xplore_plot_scatter.png
   :align: center

``--type`` also accepts ``bar``, ``line``, ``area`` and ``impulse``
(``scatter`` is the default). ``-x`` can be omitted to plot a field against
its row position instead of another field.

Pass ``-o`` to either command to write a PNG (or any format matplotlib
writes) instead of drawing in the terminal, and ``--theme`` to pick a
different plot2 theme -- both commands default to ``dkit-dark``, which
suits a terminal background better than a white plot would:

.. code-block:: bash

   dk xplore histogram -d displ -o displ.png examples/data/mpg.jsonl
   dk xplore plot -x cty -y hwy --theme dkit-light examples/data/mpg.jsonl

.. image:: ../../examples/plots/dk_xplore_plot_light.png
   :align: center

See :doc:`plot2/index` for what each theme and ``--type`` looks like
rendered to a file -- every figure there is a real ``dk`` command's output,
just saved rather than shown inline. ``dk xplore qhist`` is a lighter
alternative to ``histogram`` for a field: it draws directly in text with
``plotille`` instead of ``chafa``, so it needs no external binary, at the
cost of a coarser picture that cannot use plot2's themes.

Reading from a pipe
=======================

Every ``dk`` subcommand that takes an input file also accepts a source
*URI* in place of one, and ``jsonl:///stdio`` is one such URI: it reads
JSONL from standard input instead of a file, which is what lets one ``dk``
command feed another:

.. code-block:: bash

   cat examples/data/titanic.csv | dk xplore head "jsonl:///stdio" -n 3

``-`` is shorthand for exactly that -- the same convention ``cat``, ``tar``
and ``jq`` use for "read from stdin" -- and is equivalent to typing
``jsonl:///stdio`` in full:

.. code-block:: bash

   cat examples/data/titanic.csv | dk xplore head - -n 3

Piping only ever means JSONL: it is dkit's own interchange format between
its tools, and already the default for ``-o`` on write. There is no ``-``
shorthand for piping CSV or another dialect in -- write ``jsonl:///stdio``
elsewhere in a pipeline, or convert with ``dk run etl`` first.

Finding duplicates
=====================

``dk xplore duplicates`` groups by one or more fields and prints each
distinct combination once, which is a quick way to check whether a
field (or field combination) you expect to be unique actually is:

.. code-block:: bash

   dk xplore duplicates examples/data/titanic.csv -d Pclass --table

.. code-block:: text

     Pclass
   --------
          3
          2
          1

Inferring and storing a schema
=================================

``dk schemas infer`` prints the inferred schema for a file, the same way
``Entity.from_iterable`` does in Python. Passing ``-e`` also saves the
resulting entity to the model file:

.. code-block:: bash

   dk schemas infer examples/data/titanic.csv -e passenger

.. code-block:: text

   PassengerId: Integer()
   Pclass: Integer()
   Name: String(str_len=59)
   Sex: String(str_len=6)
   Age: String(str_len=4)
   SibSp: Integer()
   Parch: Integer()
   Ticket: String(str_len=18)
   Fare: Float()
   Cabin: String(str_len=15)
   Embarked: String(str_len=1)

``dk schemas ls`` lists the entities stored on the model, and
``dk schemas print`` shows the full field list for one entity:

.. code-block:: bash

   dk schemas ls
   dk schemas print -e passenger

Exporting a schema
=====================

``dk schemas export`` writes a stored entity's schema out in another
system's type language — Apache Arrow, SQL DDL, Apache Spark, Pandas
dtypes, Python dataclasses, or a GraphViz entity-relation diagram:

.. code-block:: bash

   dk schemas export passenger -t pyarrow

.. code-block:: python

   import pyarrow as pa


   # passenger
   schema_passenger = pa.schema(
       [
           pa.field("Age", pa.string()),
           pa.field("Cabin", pa.string()),
           pa.field("Embarked", pa.string()),
           pa.field("Fare", pa.float32()),
           pa.field("Name", pa.string()),
           pa.field("Parch", pa.int32()),
           pa.field("PassengerId", pa.int32()),
           pa.field("Pclass", pa.int32()),
           pa.field("Sex", pa.string()),
           pa.field("SibSp", pa.int32()),
           pa.field("Ticket", pa.string()),
       ]
   )

Running the conversion
=========================

With the ``passenger`` entity stored on the model, ``dk run etl``
converts the file and applies that entity's schema in the same step —
the CLI equivalent of the ``entity(src)`` coercion shown in
:doc:`howto_etl_model`:

.. code-block:: bash

   dk run etl examples/data/titanic.csv -e passenger -o titanic.parquet

Where to go next
===================

- :doc:`howto_etl_model` — the same connections, endpoints, and schema
  concepts driven from Python instead of the shell.
- :doc:`cli` — full reference for every ``dk`` subcommand, including
  ``dk xplore view`` (an interactive curses grid) and ``dk xplore plot``
  (line, bar, area, scatter and impulse charts, drawn with plot2), which are
  easiest to explore directly in a terminal rather than read about.

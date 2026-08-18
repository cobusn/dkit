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

``dk xplore histogram`` draws a console histogram (using gnuplot, with
colour if your terminal supports it) for a numeric field —
``dk xplore qhist`` produces a quicker, coarser version of the same
thing.

.. note::
   ``summary`` and ``histogram`` require the field to be fully numeric
   with no blank values. ``Fare`` in the sample file has one blank
   value, which raises a ``ValueError``; ``Pclass``, used above, has
   none.

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
  (plot-grammar charts), which are easiest to explore directly in a
  terminal rather than read about.

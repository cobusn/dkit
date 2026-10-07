# High priority (correctness / safety)
* `dkit.etl`: validations on parsing Decimal(precision=X, scale=Y), will
  currently parse without defaults or errors -- silent data corruption risk
* `dkit.etl`: fix float_32 and float_64 for explicit types
* `dkit.data`: file locking for JSONDB (concurrency / corruption risk)

# Medium priority (test coverage debt)
* improve test coverage (including for dk) -- split into concrete per-module
  items as they are tackled, rather than tracking as one open-ended item
* `dkit.doc2`: unit test coverage for `md_to_doc.py`: direct tests asserting
  canonical elements (Str/Bold/Emph/Link/Heading/Image/List/CodeBlock/etc.)
  per markdown construct -- currently only covered indirectly via
  full-document renders
* `dkit.doc2`: unit test coverage for the `latex.py` DSL: direct tests for
  the tex builder classes (Bold, Href, Itemize, Enumerate, Table, etc.) in
  isolation -- currently only covered indirectly via full-document renders
* `dkit.data`: properly test DataTrie (test the existing implementation
  before considering any expansion)

# Medium priority (architecture / deprecation)
* `dkit.base`: deprecate this package; remove ConfiguredApplication
  boilerplate and old base classes
* `dkit.plot2`: deprecate the old plot and ggplot interfaces, create a more
  modern plot2 that is helpers to quickly generate plots instead of crafting
  from scratch. Use Matplotlib only -- verify current state of plot2 first,
  as it already appears in the package list
* `dkit.data`: refactor EDA (Exploratory Data Analysis) -- needs scoping
  before starting
* `dkit.data`: expand DataTrie (after test coverage above is in place)
* `dkit.doc2`:
  * support for autolink in Reportlab Renderer
  * table of contents for Reportlab, Latex and DocX

# Low priority (build / packaging)
* run tests for multiple python versions before build upload
* build wheels for multiple python versions at once
* optimise imports (use lazy loading where possible)

# Housekeeping
* resolve conflict: `data/.placeholder` and `output/.placeholder` are
  currently staged for commit while this list calls for ignoring those
  directories -- decide one direction before acting
* ignore the root `output/` directory in `.gitignore`; migrate tests that
  generate artifacts to pytest `tmp_path` where practical
* ignore the root `data/` directory in `.gitignore`; migrate tests that
  generate artifacts to pytest `tmp_path` where practical
* clean up test data files for tests
* find out where dkit/utilities/numeric.py is used and if we can remove scipy
  (also used in window functions)

# Someday / Maybe 
* AWS S3 integration (s3fs..) [e.g dk r etl s3://bucket/products.parquet -0 products.xlsx]
* Handle Dicts and List data types 
* type guesser guesses numpy ints as string (should it generate an error)
* data analyser feature (automated data analysis with report output)
* review pandoc templates for anything useful: https://github.com/Wandmalfarbe/pandoc-latex-template/tree/master/examples
* https://julien.danjou.info/finding-definitions-from-a-source-file-and-a-line-number-in-python/
* pgfplotstable http://ftp.sun.ac.za/ftp/CTAN/graphics/pgf/contrib/pgfplots/doc/pgfplotstable.pdf
* optimise mpak schema to use integers/floats for storing dates

# Done
* support for Python 3.14
* properly integrate the CSS components and examples
* Hierarchical treemap replacement using `squarify`
* protobuf integration
* Upgrade to sqlalchemy 2
* Upgrade to mistune 2
* Update README 
* factor out Cerberus for a more modern replacement
* apache parquet integration 
* remove dataclass_wizard dependency
* allow options to be passed for opening a network database connection. the options should be stored in the connection settings..

# Ongoing 
* improve test coverage (including for dk)

# Next
* `dkit.base`:
  * deprecate this package
  * remove ConfiguredApplication boilerplate and old base classes
* `dkit.plot2`: deprecate the old plot and ggplot interfaces, create a more modern
  plot2 that is helpers to quickly generate plots instead of crafting from
  scratch. Use Matplotlib only
  * Hierarchical treemap replacement using `squarify`
* `dkit.data`:
  * refactor EDA (Exploratory Data Analysis)
  * expand and properly test DataTrie
  * file locking for JSONDB
* `dkit.etl`:
  * fix float_32 and float_64 for explicit types
  * validations on parsing Decimal(precision=X, scale=Y), will currently parse
    without defaults or errors;
* `dkit.doc2` 
  * properly integrate the CSS components and examples
  * support for autolink in Reportlab Renderer
  * table of contents for Reportlab, Latex and DocX
  * unit test coverage for `md_to_doc.py`: direct tests asserting canonical
    elements (Str/Bold/Emph/Link/Heading/Image/List/CodeBlock/etc.) per
    markdown construct -- currently only covered indirectly via
    full-document renders
  * unit test coverage for the `latex.py` DSL: direct tests for the tex
    builder classes (Bold, Href, Itemize, Enumerate, Table, etc.) in
    isolation -- currently only covered indirectly via full-document renders
* build system, compatibility and imports
  * support for Python 3.14
  * run tests for multiple versions before build upload
  * build wheels for multiple python versions at once
  * optimise imports (use lazy loading where possible)

# Clean-up
* clean up test data files for tests
* find out where dkit/utilities/numeric.py is used and if we can remove scipy
  (also used in window functions)

# Someday / Maybe 
* AWS S3 integration (s3fs..) [e.g dk r etl s3://bucket/products.parquet -0 products.xlsx]
* Handle Dicts and List data types 
* type guesser guesses numpy ints as string (should it generate an error)
* refactor etl.writer
* data analyser feature (automated data analysis with report output)
* review pandoc templates for anything useful: https://github.com/Wandmalfarbe/pandoc-latex-template/tree/master/examples
* https://julien.danjou.info/finding-definitions-from-a-source-file-and-a-line-number-in-python/
* pgfplotstable http://ftp.sun.ac.za/ftp/CTAN/graphics/pgf/contrib/pgfplots/doc/pgfplotstable.pdf
* optimise mpak schema to use integers/floats for storing dates

# Done
* protobuf integration
* Upgrade to sqlalchemy 2
* Upgrade to mistune 2
* Update README 
* factor out Cerberus for a more modern replacement
* apache parquet integration 
* remove dataclass_wizard dependency
* allow options to be passed for opening a network database connection. the options should be stored in the connection settings..

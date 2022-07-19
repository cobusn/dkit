# Next
* upgrade to mistune 2
* AWS S3 integration
* support for autolink in Reportlab Renderer
* generate report folder structure and configuration files (via dk build (init-tex | init-rl))

# Backlog 
* fix python/cannonical type conversion (e.g. python float must be double)
* refactor etl.writer
* factor out Cerberus for a more modern replacement
* update for Python 3.10
* optimise imports
* reportlab: document structure added to pdf 
* table of contents for Reportlab documents
* change major imports to lazy loading
* integrate plantuml for documentation?
* data analyser feature (automated data analysis with report output)
* review pandoc templates for anything useful: https://github.com/Wandmalfarbe/pandoc-latex-template/tree/master/examples
* https://julien.danjou.info/finding-definitions-from-a-source-file-and-a-line-number-in-python/
* protobuf integration
* apache parquet integration 
* pgfplotstable http://ftp.sun.ac.za/ftp/CTAN/graphics/pgf/contrib/pgfplots/doc/pgfplotstable.pdf
* allow options to be passed for opening a network database connection. the options should be stored in the connection settings..
* optimise mpak schema to use integers/floats for storing dates
* optimise plots to define grammar and then apply data at plot instead of defining the data upfront.

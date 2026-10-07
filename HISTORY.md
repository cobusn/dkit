# 26.09.1

## Added
- doc2 commit
- add JobTracker for skip-already-processed row during processing
- added release build utilities
- added email.css for use with generating mail documents
- added OFL License for fonts
- added jsonl_tabulate.py

## Changed
- replace boltons.statsutils with numpy/stdlib, drop dependency
- file cleanup
- removed old doc/plot packages in favor of doc2/plot2
- documentation reorganisation
- documentation update
- updated install requirements
- update sphinx conf
- increased number of records used to determine arrow schema
- build: fix editable install and clean up generated Cython/cffi artifacts
- updated requirements.txt

## Fixed
- move quantile_bins into iteration.py and fix IndexError on constant data
- correct Accumulator merge bug and compile as Cython extension type
- fixed new xxhash encoding requirement
- clean up examples

## Summary
- 251 files changed
- 17573 insertions(+), 9162 deletions(-)

# 22.7.4
* bugfix in avro extension

# 22.7.3
* added performance tests for avro extension
* fixed bug in avro extension

# 22.7.2
* ability to read and write avro files
* added ability to read document author,email,contact information from
  configuration file
* changed default log level to logging.INFO as per module level variable
  in `utilities/log_helper.py`

# 22.7.1
* convert markdown documents to PDF (using dk build doc)
* resolved sqlalchemy extension related bugs in tests

DKit (Data Toolkit) 
==================

Data processing toolkit.  General purpose data
processing library in Python:

* ETL
  - maintain schemas
  - schema transforms
  - transform from one format to the other
  - support many different formats (see below)
* Data Exploration
* Data manipulation
* Report generation using Latex and Reportlab
* extensive test coverage (>70%)

# Data formats
Include extensions that facilitate reading data, 
transforming it and then and writing to any of the 
following formats:

* Parquet (using pyarrow)
* SQL (using any SQLAlchemy enabled database)
* Messagepack
* HDF5
* XML
* json and jsonl
* CSV
* Excel
* Apache Avro

# Schema generated
Support schema generation for the following:

* Apache Arrow
* Apache Avro
* SQL (via Sqlalchemy)
* Spark

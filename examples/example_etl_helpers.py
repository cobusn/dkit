from etl_job import SQLExtractor, ParquetLoader
from dkit.etl.extensions.ext_sql_alchemy import SQLServices
from pyarrow.fs import LocalFileSystem


class OrderExtract(SQLExtractor):

    def get_sql(self, table):
        sql = f"""
        Select
            *
        from {table}
        """
        return sql


def run_job(table="Customers"):
    """create order job"""

    services = SQLServices.from_file()

    # Define extract
    extract = OrderExtract(services, "northwind")
    schema = extract.extract_schema(table)

    # Define loader
    fs = LocalFileSystem()
    load = ParquetLoader(schema, fs, "parquet")

    print(load.make_ddl(table))

    # run job
    sql = extract.get_sql(table)
    load(
        extract(sql)
    )


if __name__ == "__main__":
    run_job()

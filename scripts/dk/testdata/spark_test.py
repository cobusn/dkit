from pyspark.sql import types
from pyspark.sql.types import (
    StructType,
    StringType,
    LongType,
    FloatType
    )



# mpg
schema_mpg = StructType(
    [
        types.StructField("class", StringType(), True),
        types.StructField("cty", LongType(), True),
        types.StructField("cyl", LongType(), True),
        types.StructField("displ", FloatType(), True),
        types.StructField("drv", StringType(), True),
        types.StructField("fl", StringType(), True),
        types.StructField("hwy", LongType(), True),
        types.StructField("index", LongType(), True),
        types.StructField("manufacturer", StringType(), True),
        types.StructField("model", StringType(), True),
        types.StructField("trans", StringType(), True),
        types.StructField("year", LongType(), True),
        ]
)

entity_map = {
    "mpg": schema_mpg,
}

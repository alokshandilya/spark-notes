from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import Row, StringType, StructField, StructType


def to_date_df(df: DataFrame, fmt: str, fld: str):
    return df.withColumn(fld, to_date(col(fld), fmt))


"""
Approach of laoding a dataframe from a file and executing test cases has 2 problems:

1. large project might need hundreds of small data files to test functions.
2. build pipeline might run slow due to loading hundreds of sample files and increased I/O.

We prefer to use a well crafted and limited number of sample data file to check some of
the critical business scenarios.

For testing small functions, we create dataframe on the fly.
"""

my_schema = StructType(
    [
        StructField("ID", StringType()),
        StructField("EventDate", StringType()),
    ]
)

my_rows = [
    Row("123", "04/05/2020"),
    Row("124", "4/5/2020"),
    Row("125", "04/5/2020"),
    Row("126", "4/05/2020"),
]

spark: SparkSession = (
    SparkSession.builder.master("local[3]").appName("DataFrameRows").getOrCreate()  # type: ignore
)

my_rdd = spark.sparkContext.parallelize(my_rows, 2)

my_df: DataFrame = spark.createDataFrame(my_rdd, my_schema)

my_df.show()
my_df.printSchema()

new_df = to_date_df(my_df, "M/d/y", "EventDate")
new_df.printSchema()
new_df.show()

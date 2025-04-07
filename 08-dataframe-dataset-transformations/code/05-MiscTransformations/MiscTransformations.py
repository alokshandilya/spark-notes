from lib.logger import Log4J
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

app_name = "MiscTransformations"
spark: SparkSession = (
    SparkSession.builder.master("local[3]")  # type: ignore
    .appName(app_name)
    .getOrCreate()
)

logger = Log4J(spark)

logger.info(msg=f"Starting {app_name}!!")

######################################
#  quick method to create dataframe  #
######################################
# mainly for testing, exploring some techniques
# skipped parallizing the data, creating RDD, creating schema definition etc.

dataList = [
    ("Ravi", 28, 1, 2002),
    ("Abdul", 23, 5, 81),
    ("John", 12, 12, 6),
    ("Rosy", 7, 8, 63),
    ("Abdul", 23, 5, 81),
]

rawDF = spark.createDataFrame(dataList)
rawDF.show()
rawDF.printSchema()  # can use namedtuple to create schema
"""
root
 |-- _1: string (nullable = true)
 |-- _2: long (nullable = true)
 |-- _3: long (nullable = true)
 |-- _4: long (nullable = true)
"""

# quick way to attach column names
rawDFCol = spark.createDataFrame(dataList).toDF("name", "day", "month", "year")
# toDF() returns a new DataFrame that with new specified column names
rawDFCol.show()
rawDFCol.printSchema()

"""
root
 |-- name: string (nullable = true)
 |-- day: long (nullable = true)
 |-- month: long (nullable = true)
 |-- year: long (nullable = true)
"""

###################
#  some problems  #
###################

# changed to string to show the problem
dataList = [
    ("Ravi", "28", "1", "2002"),
    ("Abdul", "23", "5", "81"),
    ("John", "12", "12", "6"),
    ("Rosy", "7", "8", "63"),
    ("Abdul", "23", "5", "81"),
]

rawDF = spark.createDataFrame(dataList).toDF("name", "day", "month", "year")
rawDF.show()
rawDF.printSchema()

"""
root
 |-- name: string (nullable = true)
 |-- day: string (nullable = true)
 |-- month: string (nullable = true)
 |-- year: string (nullable = true)
"""

############################################
#  how to add monotonically increasing id  #
############################################
# from pyspark.sql.functions import monotonically_increasing_id

# generates a monotically increasing integer value which is unique across all
# partitions. However, the values are not guaranteed to be consecutive.

rawDF = (
    spark.createDataFrame(dataList)
    .toDF(
        "name",
        "day",
        "month",
        "year",
    )
    .repartition(3)  # in local, to sense real behvaior, remove in production
)

rawDF.show()

DF1 = rawDF.withColumn("id", monotonically_increasing_id())
# withColumn(colName: str, col: Column) -> "DataFrame"
# Returns a new `DataFrame` by adding a column or replacing the
# existing column that has the same name.
DF1.show()
DF1.printSchema()

""" for repartition(1) id is consecutive
+-----+---+-----+----+-----------+
| name|day|month|year|         id|
+-----+---+-----+----+-----------+
| Ravi| 28|    1|2002|          0|
|Abdul| 23|    5|  81|          1|
|Abdul| 23|    5|  81| 8589934592|
| John| 12|   12|   6|17179869184|
| Rosy|  7|    8|  63|17179869185|
+-----+---+-----+----+-----------+

root
 |-- name: string (nullable = true)
 |-- day: string (nullable = true)
 |-- month: string (nullable = true)
 |-- year: string (nullable = true)
 |-- id: long (nullable = false)
"""


logger.info(msg=f"Stopping {app_name}!!")
spark.stop()

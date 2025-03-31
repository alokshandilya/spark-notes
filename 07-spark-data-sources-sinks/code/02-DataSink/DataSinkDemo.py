from lib.logger import Log4J
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

spark = SparkSession.builder.master("local[3]").appName("SparkSinkDemo").getOrCreate()

logger = Log4J(spark)

logger.info("Starting Spark Data Sink!!")

flightTimeParquetDF = spark.read.format("parquet").load("dataSource/flight*.parquet")

logger.info("Num Partitions before: " + str(flightTimeParquetDF.rdd.getNumPartitions()))
logger.info(flightTimeParquetDF.groupBy(spark_partition_id()).count().show())

# make 5 partitions
partitionedDF = flightTimeParquetDF.repartition(5)
logger.info("Num Partitions after: " + str(partitionedDF.rdd.getNumPartitions()))
partitionedDF.groupBy(spark_partition_id()).count().show()

"""
spark-avro module is external and not included in spark-submit, spark-shell.

spark-submit --packages org.apache.spark:spark-avro_2.12:3.5.5 SparkApp.py

or add this to spark-defaults.conf
spark.jars.packages    org.apache.spark:spark-avro_2.12:3.5.5
"""

# partitionedDF.write.format("avro").mode("overwrite").option(
#     "path", "dataSink/avro/"
# ).save()

flightTimeParquetDF.write.format("json").mode("overwrite").option(
    "path", "dataSink/json/"
).partitionBy(
    "OP_CARRIER",  # will not have these 2 in the data sink
    "ORIGIN",  # because of directory structure and redundancy
).option("maxRecordsPerFile", 10000).save()

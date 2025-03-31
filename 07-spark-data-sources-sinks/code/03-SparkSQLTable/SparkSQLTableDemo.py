from lib.logger import Log4J
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[3]")
        .appName("SparkSQLTableDemo")
        .enableHiveSupport()  # allow the connectivity to a persistent hive metastore
        .getOrCreate()
    )

    logger = Log4J(spark)

    logger.info("Starting Spark SQL Table Demo!!")

    flightTimeParquetDF = spark.read.format("parquet").load("dataSource/")

    """
    - create a managed table and save dataframe to the spark table
    - in real scenario, you will process your data and save the output dataframe.
    - here, we are not doing processing, just saving the dataframe to a spark managed table
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    flightTimeParquetDF.write.format("csv").mode("overwrite").bucketBy(
        5, "OP_CARRIER", "ORIGIN"
    ).saveAsTable("flight_data_tbl")

    logger.info(spark.catalog.listTables("AIRLINE_DB"))

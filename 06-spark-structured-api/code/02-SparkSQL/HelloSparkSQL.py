import sys

from lib.logger import Log4J
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[3]").appName("HelloSparkSQL").getOrCreate()
    )

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error(msg="Usage: HelloSparkSQL <file>")
        exit(-1)

    logger.info(msg="Hello, Spark SQL!!")

    # survey_df = (
    #     spark.read.option("header", "true")
    #     .option("inferSchema", "true")
    #     .csv(sys.argv[1])
    # )

    survey_df = spark.read.csv(sys.argv[1], header=True, inferSchema=True)

    survey_df.createOrReplaceTempView("survey_tbl")
    count_df = spark.sql(
        "SELECT country, count(1) as count FROM survey_tbl GROUP BY country"
    )

    count_df.show()

    logger.info(count_df.collect())

    logger.info(msg="Stopping Spark Application!!")
    spark.stop()

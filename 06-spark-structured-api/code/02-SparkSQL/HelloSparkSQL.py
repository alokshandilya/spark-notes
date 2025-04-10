import sys

from lib.logger import Log4J
from pyspark.sql import DataFrame, SparkSession

if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder.master("local[3]")  # type: ignore
        .appName("HelloSparkSQL")
        .getOrCreate()
    )

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error(msg="Usage: HelloSparkSQL <file>")
        exit(-1)

    logger.info(msg="Hello, Spark SQL!!")

    # survey_df: DataFrame = spark.read.csv(
    #     sys.argv[1],
    #     header=True,
    #     inferSchema=True,
    # )

    survey_df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(sys.argv[1])
    )

    survey_df.createOrReplaceTempView("survey_tbl")
    count_df: DataFrame = spark.sql(
        "SELECT country, count(1) as count FROM survey_tbl GROUP BY country"
    )

    count_df.show()

    logger.info(count_df.collect())

    logger.info(msg="Stopping Spark Application!!")
    spark.stop()

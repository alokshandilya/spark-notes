from lib.logger import Log4J
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, column

if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder.master("local[3]")  # type: ignore
        .appName("ExploringColumns")
        .getOrCreate()
    )

    logger: Log4J = Log4J(spark)

    logger.info(msg="Starting ExploringColumns!!")

    airlineDF: DataFrame = (
        spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load("data/sample.csv")
    )

    # using column strings
    airlineTrimDF: DataFrame = airlineDF.select("Origin", "Dest", "Distance").limit(10)  # noqa: E501

    airlineTrimDF.show(10)
    logger.info(airlineTrimDF.collect())

    # using columns objects (many ways)
    # - column, col, <df>.<column>
    airlineTrimDF.select(column("Origin"), col("Dest"), airlineTrimDF.Distance).show(10)  # noqa: E501

    logger.info(msg="Stopping ExploringColumns!!")
    spark.stop()

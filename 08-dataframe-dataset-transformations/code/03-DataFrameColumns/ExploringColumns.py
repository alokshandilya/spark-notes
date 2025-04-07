import os
import sys

from lib.logger import Log4J
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, column


def create_spark_session(app_name: str) -> SparkSession:
    """
    Creates a SparkSession with the given app name.

    Args:
        app_name (str): The name of the Spark application.

    Returns:
        SparkSession: The created SparkSession.
    """
    try:
        spark: SparkSession = (
            SparkSession.builder.master("local[3]")  # type: ignore
            .appName(app_name)
            .getOrCreate()
        )
        return spark
    except Exception as e:
        print(f"Error creating SparkSession: {e}")
        sys.exit(1)


def read_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Reads data from a CSV file into a DataFrame.

    Args:
        spark (SparkSession): The SparkSession to use.
        file_path (str): The path to the CSV file.

    Returns:
        DataFrame: The DataFrame containing the data.
    """
    try:
        airlineDF: DataFrame = (
            spark.read.format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(file_path)
        )
        return airlineDF
    except Exception as e:
        print(f"Error reading data from {file_path}: {e}")
        sys.exit(1)


def transform_data(df: DataFrame, limit: int) -> DataFrame:
    """
    Selects specific columns and limits the number of rows in a DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        limit (int): maximum number of rows to include in the output DataFrame.

    Returns:
        DataFrame: The transformed DataFrame.
    """
    try:
        airlineTrimDF: DataFrame = df.select(
            "Origin",
            "Dest",
            "Distance",
        ).limit(limit)
        return airlineTrimDF
    except Exception as e:
        print(f"Error transforming data: {e}")
        sys.exit(1)


def main():
    """
    Main function to execute the data processing pipeline.
    """
    app_name: str = "ExploringColumns"
    spark: SparkSession = create_spark_session(app_name)
    logger: Log4J = Log4J(spark)

    logger.info(msg=f"Starting {app_name}!!")

    file_path: str = "data/sample.csv"
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        sys.exit(1)

    airlineDF: DataFrame = read_data(spark, file_path)

    limit: int = 10
    airlineTrimDF: DataFrame = transform_data(airlineDF, limit)

    airlineTrimDF.show(limit)
    logger.info(airlineTrimDF.collect())

    # using columns objects (many ways)
    # - column, col, <df>.<column>
    airlineTrimDF.select(
        column("Origin"),
        col("Dest"),
        airlineTrimDF.Distance,
    ).show(limit)

    logger.info(msg=f"Stopping {app_name}!!")
    spark.stop()


if __name__ == "__main__":
    main()

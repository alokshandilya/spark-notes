from lib.logger import Log4J
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder.appName("LogFileDemo").getOrCreate()  # type: ignore
    )
    logger: Log4J = Log4J(spark)

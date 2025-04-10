import sys
from typing import List

from lib.logger import Log4J
from lib.utils import count_by_country, get_spark_app_config, load_data_csv_df
from pyspark.sql import DataFrame, Row, SparkSession


def main() -> None:
    conf = get_spark_app_config()

    spark: SparkSession = SparkSession.builder.config(  # type: ignore
        conf=conf
    ).getOrCreate()

    logger: Log4J = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error(msg="Usage: HelloSpark <file>")
        exit(-1)

    logger.info(msg="Hello, Spark!!")

    survey_raw_df: DataFrame = load_data_csv_df(spark, sys.argv[1])

    # 2 partitions
    partitioned_survey_df: DataFrame = survey_raw_df.repartition(2)

    # Transformations
    count_df: DataFrame = count_by_country(partitioned_survey_df)

    # Action
    collected_results: List[Row] = count_df.collect()

    logger.info(collected_results)  # returns a list of Row objects

    #     count_df.printSchema()
    #
    #     logger.info(msg="Stopping Spark Application!!")
    #
    #     # conf_out = spark.sparkContext.getConf()
    #     # logger.info(msg=conf_out.toDebugString())

    input("Stopping Spark Application!! Press Enter to continue...")
    spark.stop()


if __name__ == "__main__":
    main()

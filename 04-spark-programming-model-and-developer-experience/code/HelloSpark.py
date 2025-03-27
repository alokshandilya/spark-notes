import sys

from lib.logger import Log4J
from lib.utils import count_by_country, get_spark_app_config, load_data_csv_df
from pyspark.sql import SparkSession

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error(msg="Usage: HelloSpark <file>")
        exit(-1)

    logger.info(msg="Hello, Spark!!")

    survey_raw_df = load_data_csv_df(spark, sys.argv[1])
    partitioned_survey_df = survey_raw_df.repartition(2)  # 2 partitions

    # Transformations
    count_df = count_by_country(partitioned_survey_df)

    # Action
    logger.info(count_df.collect())  # returns a list of Row objects

    #     count_df.printSchema()
    #
    #     logger.info(msg="Stopping Spark Application!!")
    #
    #     # conf_out = spark.sparkContext.getConf()
    #     # logger.info(msg=conf_out.toDebugString())

    input("Stopping Spark Application!! Press Enter to continue...")
    spark.stop()

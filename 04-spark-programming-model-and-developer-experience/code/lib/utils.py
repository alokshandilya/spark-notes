import configparser

from pyspark import SparkConf


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for key, val in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)

    return spark_conf


def load_data_csv_df(spark, data_file):
    return spark.read.csv(data_file, header=True, inferSchema=True)


def count_by_country(survey_df):
    return (
        survey_df.where("age < 40")
        .select("age", "gender", "country", "state")
        .groupBy("country")
        .count()
    )

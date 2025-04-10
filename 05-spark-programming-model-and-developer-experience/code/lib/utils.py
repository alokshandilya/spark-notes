import configparser

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession


def get_spark_app_config() -> SparkConf:
    """
    Reads Spark application configurations from the 'spark.conf' file.

    Returns:
        SparkConf: A SparkConf object configured with settings from the file.
    """
    spark_conf: SparkConf = SparkConf()
    config: configparser.ConfigParser = configparser.ConfigParser()
    config.read("spark.conf")

    for key, val in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)

    return spark_conf


def load_data_csv_df(spark: SparkSession, data_file: str) -> DataFrame:
    """
    Loads data from a CSV file into a Spark DataFrame.

    Args:
        spark (SparkSession): The SparkSession to use.
        data_file (str): The path to the CSV file.

    Returns:
        DataFrame: A Spark DataFrame containing the data from the CSV file.
    """
    return spark.read.csv(data_file, header=True, inferSchema=True)


def count_by_country(survey_df: DataFrame) -> DataFrame:
    """
    Filters a DataFrame to include only respondents under 40, then groups by
    country and counts the number of respondents in each country.

    Args:
        survey_df (DataFrame): The input DataFrame containing survey data with
                               columns like 'age', 'gender', 'country', 'state'

    Returns:
        DataFrame: A DataFrame containing count of respondents in each country,
                   limited to those under 40 years of age.
    """
    return (
        survey_df.where("age < 40")
        .select("age", "gender", "country", "state")
        .groupBy("country")
        .count()
    )

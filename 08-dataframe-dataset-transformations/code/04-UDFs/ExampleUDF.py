import re

from lib.logger import Log4J
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import expr, udf
from pyspark.sql.types import StringType


def parse_gender(gender: str) -> str:
    # Male, Female, Unknown
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"

    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder.master("local[3]")  # type: ignore
        .appName("ExampleUDF")
        .getOrCreate()
    )

    logger = Log4J(spark)

    logger.info(msg="Starting ExampleUDF!!")

    survey_df: DataFrame = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/survey.csv")
    )

    # Gender column has no fixed options, see survey.csv
    survey_df.select("Age", "Gender", "Country").show(10)

    # 1. Column object expression
    gender_udf = udf(parse_gender, StringType())
    logger.info(msg="Catalog Entry:")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]  # noqa: E501
    survey_df2 = survey_df.withColumn("Gender", gender_udf("Gender"))
    # withColumn() transformations allows to transform a single column
    # without impacting other columns in the dataframe.
    # it takess 2 arguments: column_name, column_expression

    survey_df2.select("Age", "Gender", "Country").show(10)

    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    logger.info(msg="Catalog Entry:")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]  # noqa: E501
    survey_df3 = survey_df.withColumn(
        "Gender",
        expr("parse_gender_udf(Gender)"),
    )

    survey_df3.select("Age", "Gender", "Country").show(10)

    logger.info(msg="Stopping ExampleUDF!!")
    spark.stop()

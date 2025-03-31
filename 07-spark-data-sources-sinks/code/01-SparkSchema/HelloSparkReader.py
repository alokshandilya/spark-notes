from lib.logger import Log4J
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, IntegerType, StringType, StructField, StructType

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("SparkSchema").getOrCreate()
    logger = Log4J(spark)

    # Schema
    # schema using StructType, StructField
    flightSchemaStruct = StructType(
        [
            StructField("FL_DATE", DateType()),
            StructField("OP_CARRIER", StringType()),
            StructField("OP_CARRIER_FL_NUM", IntegerType()),
            StructField("ORIGIN", StringType()),
            StructField("ORIGIN_CITY_NAME", StringType()),
            StructField("DEST", StringType()),
            StructField("DEST_CITY_NAME", StringType()),
            StructField("CRS_DEP_TIME", IntegerType()),
            StructField("DEP_TIME", IntegerType()),
            StructField("WHEELS_ON", IntegerType()),
            StructField("TAXI_IN", IntegerType()),
            StructField("CRS_ARR_TIME", IntegerType()),
            StructField("ARR_TIME", IntegerType()),
            StructField("CANCELLED", IntegerType()),
            StructField("DISTANCE", IntegerType()),
        ]
    )

    # Read CSV
    flightTimeCsvDF = (
        spark.read.format("csv")
        .option("header", "true")
        .schema(flightSchemaStruct)
        .option("mode", "FAILFAST")
        .option("dateFormat", "M/d/y")
        .load("data/flight*.csv")
    )

    flightTimeCsvDF.show(5)

    logger.info(msg="CSV schema: " + flightTimeCsvDF.schema.simpleString())

    # Read JSON
    # Json automically infers the schema (but it is not always correct)
    # schema using DDL format
    flightSchemaDDL = """
        FL_DATE DATE,
        OP_CARRIER STRING,
        OP_CARRIER_FL_NUM INT,
        ORIGIN STRING,
        ORIGIN_CITY_NAME STRING,
        DEST STRING,
        DEST_CITY_NAME STRING,
        CRS_DEP_TIME INT,
        DEP_TIME INT,
        WHEELS_ON INT,
        TAXI_IN INT,
        CRS_ARR_TIME INT,
        ARR_TIME INT,
        CANCELLED INT,
        DISTANCE INT
    """

    flightTimeJsonDF = (
        spark.read.format("json")
        .schema(flightSchemaDDL)
        .option("dateFormat", "M/d/y")
        .load("data/flight*.json")
    )

    flightTimeJsonDF.show(5)

    logger.info(msg="JSON schema: " + flightTimeJsonDF.schema.simpleString())

    # Read Parquet
    flightTimeParquetDF = spark.read.format("parquet").load("data/flight*.parquet")

    flightTimeParquetDF.show(5)

    logger.info(msg="Parquet schema: " + flightTimeParquetDF.schema.simpleString())

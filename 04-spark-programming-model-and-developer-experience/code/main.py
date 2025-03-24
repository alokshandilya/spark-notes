from pyspark.sql import SparkSession

from lib.logger import Log4J

spark: SparkSession = (
    SparkSession.builder.master("local[3]")
    .appName("Hello Spark Demo Application")
    .getOrCreate()
)

logger = Log4J(spark)

logger.info("Starting HelloSpark Demo Application")

df = spark.read.csv("persons.csv", header=True, inferSchema=True)

logger.info("Finished reading data from file")

df.show()
df.printSchema()

logger.info("Finished processing data")

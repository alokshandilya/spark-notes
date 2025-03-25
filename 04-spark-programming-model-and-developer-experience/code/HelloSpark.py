import sys

from lib.logger import Log4J
from lib.utils import get_spark_app_config, load_data_csv_df
from pyspark.sql import SparkSession

conf = get_spark_app_config()

spark = SparkSession.builder.config(conf=conf).getOrCreate()

logger = Log4J(spark)

logger.info(msg="Hello, Spark!")

if len(sys.argv) != 2:
    logger.error(msg="Usage: HelloSpark <file>")
    sys.exit(-1)

df = load_data_csv_df(spark, sys.argv[1])
df.show()
df.printSchema()

logger.info(msg="Stopping Spark Application")

# conf_out = spark.sparkContext.getConf()
# logger.info(msg=conf_out.toDebugString())

spark.stop()

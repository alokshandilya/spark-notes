import sys
from collections import namedtuple

from lib.logger import Log4J
from pyspark import SparkConf
from pyspark.sql import SparkSession

# namedtuple or class: to give schema to data (name, datatype to each column)
SurveyRecord = namedtuple("SurveyRecord", ["age", "gender", "country", "state"])


if __name__ == "__main__":
    conf = SparkConf()

    conf.setMaster("local[3]").setAppName("HelloRdd")

    """
    - SparkSession in newer version as an improvement over SparkContext
    - SparkSession still holds SparkContext and uses it internally

    from pyspark import SparkContext

    sc = SparkContext(conf=conf)
    """
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    sc = spark.sparkContext  # sparkContext for RDD APIs
    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error(msg="Usage: HelloRdd <file>")
        exit(-1)

    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)  # 2 partitions

    # give a structure to the data to the CSV file
    # I/P: line of text, O/P: list of text/strings
    colsRDD = partitionedRDD.map(lambda line: line.replace('"', "").split(","))

    selectRDD = colsRDD.map(
        lambda cols: SurveyRecord(int(cols[0]), cols[1], cols[2], cols[3])
    )

    filteredRDD = selectRDD.filter(lambda r: r.age < 40)

    kvRDD = filteredRDD.map(lambda r: (r.country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colsList = countRDD.collect()

    for x in colsList:
        logger.info(x)

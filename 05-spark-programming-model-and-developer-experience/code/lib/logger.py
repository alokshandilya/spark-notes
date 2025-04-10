# SPARK_HOME/conf/spark-defaults.conf (/opt/apache-spark/ is my SPARK_HOME)
# -> add this line
"""
spark.driver.extraJavaOptions -Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=sparkapp
"""  # noqa: E501

from pyspark.sql import SparkSession


class Log4J:
    """log4j logger class

    :param spark: SparkSession object
    """

    def __init__(self, spark: SparkSession):
        """
        NOTE:

        - spark._jvm: special attribute of SparkSession object which provides a
            gateway to the JVM instance that Spark is running within. It's a bridge
            that allows Python code to call Java methods and access Java classes.

        - org.apache.log4j: Log4j is a widely used Java logging library.
            org.apache.log4j is the package name of the Log4j library of Java.
            Spark itself uses Log4j extensively for its own logging.
        """  # noqa: E501
        log4j = spark._jvm.org.apache.log4j  # type: ignore

        root_class = "com.example.sparkapp"
        conf = spark.sparkContext.getConf()
        app_name: str = conf.get("spark.app.name") or ""
        self.logger: Log4J = log4j.LogManager.getLogger(  # type: ignore
            root_class + "." + app_name  # type: ignore
        )

    def warn(self, msg):
        self.logger.warn(msg)

    def info(self, msg):
        self.logger.info(msg)

    def error(self, msg):
        self.logger.error(msg)

from lib.logger import Log4J
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import regexp_extract, substring_index

if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder.appName("LogFileDemo").getOrCreate()  # type: ignore  # noqa: E501
    )
    logger: Log4J = Log4J(spark)

    logger.info("Starting LogFileDemo!!!")

    file_df: DataFrame = spark.read.text("data/apache_logs.txt")
    # file_df.show()
    # file_df.printSchema()

    # every record in data/apache_logs.txt follows standard apache log file
    # format with following information:
    # - IP, client, user, datetime, cmd, request
    # - protocol, status, bytes, referre, userAgent

    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'  # noqa: E501

    logs_df: DataFrame = file_df.select(
        regexp_extract("value", log_reg, 1).alias("ip"),
        regexp_extract("value", log_reg, 4).alias("date"),
        regexp_extract("value", log_reg, 6).alias("request"),
        regexp_extract("value", log_reg, 10).alias("referrer"),
    )

    # logs_df.show()
    logs_df.printSchema()

    logs_df.withColumn("referrer", substring_index("referrer", "/", 3)).where(
        "trim(referrer) != '-'"
    ).groupBy("referrer").count().show(100, truncate=False)

    logger.info("Finished LogFileDemo!!!")
    spark.stop()

"""
####################
###### OUTPUT ######
####################
"""

#   25/04/02 18:29:49 WARN NativeCodeLoader: Unable to load native-hadoop
#   library for your platform... using builtin-java classes where applicable
#
#   root
#    |-- ip: string (nullable = true)
#    |-- date: string (nullable = true)
#    |-- request: string (nullable = true)
#    |-- referrer: string (nullable = true)
#
#   +--------------------------------------+-----+
#   |referrer                              |count|
#   +--------------------------------------+-----+
#   |http://ijavascript.cn                 |1    |
#   |http://www.google.co.tz               |1    |
#   |http://www.google.ca                  |6    |
#   |https://www.google.hr                 |2    |
#   |https://www.google.ch                 |1    |
#   |http://www.google.ru                  |6    |
#   |http://www.raspberrypi-spanish.es     |1    |
#   |http://semicomplete.com               |2001 |
#   |http://manpages.ubuntu.com            |2    |
#   |http://kufli.blogspot.fr              |1    |
#   |http://www.bing.com                   |6    |
#   |http://rungie.com                     |1    |
#   |http://www.google.co.th               |2    |
#   |https://www.google.cz                 |5    |
#   |http://danceuniverse.ru               |3    |
#   |http://www.google.co.uk               |14   |
#   |http://www.google.rs                  |1    |
#   |http://kufli.blogspot.in              |1    |
#   |http://t.co                           |3    |
#   |http://www.google.nl                  |4    |
#   |https://www.google.hu                 |1    |
#   |http://search.daum.net                |2    |
#   |http://avtoads.net                    |3    |
#   |http://www.google.com.sg              |2    |
#   |http://www.google.lv                  |2    |
#   |http://mishura-optom.ru               |3    |
#   |http://www.baidu.com                  |3    |
#   |http://ubuntuforums.org               |4    |
#   |http://bruteforce.gr                  |1    |
#   |https://www.google.co.th              |2    |
#   |http://kufli.blogspot.nl              |1    |
#   |http://www.google.az                  |1    |
#   |http://www.google.at                  |1    |
#   |http://www.google.com.tw              |1    |
#   |http://www.experts-exchange.com       |1    |
#   |http://lifehacker.com                 |1    |
#   |http://www.ibm.com                    |1    |
#   |http://blog.float.tw                  |3    |
#   |http://zoomq.qiniudn.com              |1    |
#   |http://www.davidsoncustom.com         |1    |
#   |http://zolotoy-lis.ru                 |3    |
#   |http://www.google.com.tr              |1    |
#   |https://www.google.ca                 |3    |
#   |http://antonio-zabila.blogspot.com.es |1    |
#   |http://translate.googleusercontent.com|2    |
#   |http://www.google.co.jp               |2    |
#   |http://tuxradar.com                   |12   |
#   |http://serverfault.com                |1    |
#   |http://doc.ubuntu-fr.org              |1    |
#   |http://znakomstvaonlain.ru            |3    |
#   |https://www.google.com.tw             |1    |
#   |https://www.google.fi                 |3    |
#   |https://www.google.si                 |1    |
#   |http://kufli.blogspot.com.au          |1    |
#   |http://www.google.com.gh              |1    |
#   |https://duckduckgo.com                |3    |
#   |http://image.baidu.com                |1    |
#   |http://images.google.com              |1    |
#   |http://www.xiaofang.me                |1    |
#   |http://forums.opensuse.org            |1    |
#   |http://www.google.iq                  |1    |
#   |http://www.google.com.ar              |3    |
#   |http://tex.stackexchange.com          |1    |
#   |https://www.google.com.ar             |2    |
#   |https://www.google.nl                 |6    |
#   |http://blog.sleeplessbeastie.eu       |1    |
#   |https://www.google.sk                 |1    |
#   |http://www.google.com.ua              |1    |
#   |http://www.google.si                  |1    |
#   |http://genus-industri.us              |1    |
#   |https://www.google.pl                 |3    |
#   |https://www.google.co.mz              |1    |
#   |http://installion.co.uk               |1    |
#   |http://www.adictosalared.com          |1    |
#   |http://jquerylist.com                 |1    |
#   |http://r.duckduckgo.com               |11   |
#   |http://blackwitchcraft.ru             |3    |
#   |https://www.google.com.au             |2    |
#   |http://www.google.dz                  |1    |
#   |http://simplestcodings.blogspot.in    |1    |
#   |http://en.wikipedia.org               |10   |
#   |http://kufli.blogspot.co.uk           |1    |
#   |http://www.logstash.net               |3    |
#   |http://pandce.proboards.com           |1    |
#   |http://www.reproductive-fitness.com   |1    |
#   |http://es.wikipedia.org               |1    |
#   |http://stackoverflow.com              |34   |
#   |http://kherson-apartments.ru          |3    |
#   |http://community.spiceworks.com       |2    |
#   |https://www.google.com.ua             |2    |
#   |http://www.google.fi                  |4    |
#   |https://www.google.co.za              |1    |
#   |http://www.google.md                  |1    |
#   |http://www.google.co.ve               |1    |
#   |http://www.google.se                  |1    |
#   |http://www.google.com                 |123  |
#   |https://www.google.com.br             |9    |
#   |http://kufli.blogspot.de              |5    |
#   |http://www.google.com.au              |4    |
#   |https://www.google.it                 |4    |
#   +--------------------------------------+-----+
#   only showing top 100 rows

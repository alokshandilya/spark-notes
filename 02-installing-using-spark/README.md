# Installing Spark

these are the options to run spark.

- **Local mode:** command line REPL
- **Python IDE:** PyCharm etc.
- **Databricks cloud:** notebooks
- **Other notebooks:** Jupyter Notebooks etc.
- **Other options:** cloud offerings like AWS EMR, Azure HDInsight, Google Cloud Dataproc etc.

> while spark development and unit testing activity happens mainly in local mode, but knowledge of spark cluster is also important for production deployment.

> I installed `apache-spark` from AUR on my Arch Linux system and set `SPARK_HOME` in my zsh config.
>
> > `export SPARK_HOME="/opt/apache-spark"`

- `$SPARK_HOME/conf/spark-defaults.conf` (/opt/apache-spark/ is my SPARK_HOME)
- add this line
- `spark.driver.extraJavaOptions -Dlog4j.configuration=file:log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=sparkapp`

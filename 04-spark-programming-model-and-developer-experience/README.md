## Creating Spark Project Build Configuration

### Pre-requisites

- Python, Java
- IDE
- Spark binaries
- `SPARK_HOME` environment variable
- `HADOOP_HOME` environment variable
  - for windows, the `winutils.exe` file is also needed

> I installed `apache-spark` from AUR on my Arch Linux system, skipping these steps.

### Configure Spark Application Logs

Spark projects uses `log4j` for logging.It's similar to python logging. [docs - python logging](https://docs.python.org/3/howto/logging.html).

It's a 3 step process:

- create a `log4j` configuration file.
  - `log4j.properties` file _(`log4j2.properties` for `log4j2`)_.
- configure spark JVM to pickup the `log4j` configuration file.
- create a python class to get spark's `log4j` instance and use it.

> Apache `Log4j 2` is an upgrade to `Log4j` that provides significant improvements over its predecessor, `Log4j 1.x`, and provides many of the improvements available in Logback while fixing some inherent problems in Logback’s architecture.

3 components of `log4j`:

- `Logger` - set of APIs which we use from our application.
- `Configurations` - defined in `log4j.properties` file, are loaded by the logger at runtime.
  - defined in hierarchical manner.
  - topmost hierarchy is `rootCategory`. eg: `log4j.rootCategory=WARN, console`.
    1. for any hierarchy, category we define 2 thigns: log-level and list of appenders. `log4j` supports multiple log-levels such as `DEBUG`, `INFO`, `WARN`, `ERROR`
    2. Logging levels, in ascending order of severity, are: `DEBUG`, `INFO`, `WARN`, `ERROR`, and `FATAL`.
    3. By setting it to `WARN`, you're filtering out `DEBUG` and `INFO` messages, which are typically used for detailed debugging and informational purposes.
- `Appenders` - output destinations such as console, log files etc, are configured in `log4j.properties` file.

`# log4j.properties` file

```python

# Set everything to be logged to the console
log4j.rootCategory=WARN, console

# Define console appender
"""
are standard and remain same in most of the projects

- this section with the previous line of rootCatergory sets up root level
  log4j configuration and will stop all the log messages sent by the Spark
  and other packages except warnings and errors, we will get clean and minimal
  log output.
"""
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.out
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Application Log
"""
- 2nd log level specific to the spark application.
  - targetting a specific logger (com.example.sparkapp, adjust package name)
  - we are stopping log messages from com.example.sparkapp from being processed
    by it's parent loggers preventing duplicate logs.
"""
log4j.logger.com.example.sparkapp=INFO, console, file
log4j.additivity.com.example.sparkapp=false

# Define file appender
log4j.appender.file=org.apache.log4j.RollingFileAppender
"""
- spark.yarn.app.container.log.dir variable: to find the log file directory location
- ${logfile.name}.log: to find log file name
"""
log4j.appender.file.File=${spark.yarn.app.container.log.dir}/${logfile.name}.log
log4j.appender.file.ImmediateFlush=true
log4j.appender.file.Append=false
log4j.appender.file.MaxFileSize=500MB
log4j.appender.file.MaxBackupIndex=2
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.conversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Recommendations from Spark log4j configuration template
log4j.logger.org.apache.spark.repl.Main=WARN
log4j.logger.org.spark_project.jetty=WARN
log4j.logger.org.spark_project.jetty.util.component.AbstractLifeCycle=ERROR
log4j.logger.org.apache.spark.repl.SparkIMain$exprTyper=INFO
log4j.logger.org.apache.spark.repl.SparkILoop$SparkILoopInterpreter=INFO
log4j.logger.org.apache.parquet=ERROR
log4j.logger.parquet=ERROR
log4j.logger.parquet.hadoop.hive.metastore.RetryingHMSHandler=FATAL
log4j.logger.org.apache.hadoop.hive.ql.exec.FunctionRegistry=ERROR
```

> As Spark follows Master-Slave Architecture and application has a driver and multiple executor processes. All these executors are individual JVMs running on different machines in the clustor. The driver is running on a machine/node and the executors are running on different machines/nodes and there is no control over it, we don't know what is going to be executed and on which machine. All of this is managed by Clutser Manager.

> Imagine, all the executors and the driver create their own logs. the cluster manager manages the logs by collecting all the logs to a single predefined location.

<p align="center">
  <img src="https://github.com/user-attachments/assets/52064cfb-e003-49c2-b9e3-86e9cd34aaa6" width="75%">
</p>

we can add JVM Parameters to `SPARK_HOME/conf/spark-defaults.conf` file to configure the log4j properties file. Add this line

```
spark.driver.extraJavaOptions -Dlog4j.configuration=file:/path/to/log4j.properties -Dspark.yarn.app.container.log.dir=app-logs -Dlogfile.name=sparkapp
```

- all these 3 variables will reach the logger. `log4j` is built this way to be able to read the JVM parameters and use them in the log4j configuration file.
- if the `log4j.properties` file is in the project root, we can use `file:log4j.properties` as the path.

## Creating Spark Session

`SparkSession` object is the driver _(one can argue if main method is driver instead)_

- in `spark-shell`, `pyspark` etc. it's already created as `spark` object.
- in a spark application, we need to create it.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
```

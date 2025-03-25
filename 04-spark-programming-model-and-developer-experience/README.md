## Creating Spark Project Build Configuration

### Pre-requisites

- Python, Java
- IDE
- Spark binaries
- `SPARK_HOME` environment variable
- `HADOOP_HOME` environment variable
  - for windows, the `winutils.exe` file is also needed

> I installed `apache-spark` from AUR on my Arch Linux system and set `SPARK_HOME` in my zsh config.
>
> > `export SPARK_HOME="/opt/apache-spark"`

### Configure Spark Application Logs

Spark projects uses `log4j` for logging. It's similar to python logging. [docs - python logging](https://docs.python.org/3/howto/logging.html).

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

`# log4j.properties` file _(in code directory)_

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
- in a spark application, we need to create it often named `spark`.
- `SparkSession` is a singleton object, we can have only one active `SparkSession` object per spark application.
  > we can't have more than 1 driver in a spark application.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Hello Spark").master("local[3]").getOrCreate()
```

`SparkSession` is highly configurable, we can set configurations like `appName`, `master` etc.

## Configuring Spark Session

- Environment variables
  - for setting local dev environment
  - eg. `SPARK_HOME`, `HADOOP_HOME`, `PYSPARK_PYTHON` etc.
  - mostly ignored by developer except for setting up local dev environment.
  - mainly used by cluster admins.
- `$SPARK_HOME/conf/spark-defaults.conf`
  - for jvm varibles such as log4j configuration file, log file location and name
  - to set default configurations for all the applications
  - mostly ignored by developer except for setting up local dev environment.
  - mainly used by cluster admins.
- `spark-submit` command line arguments

  - **_used by developers._**
  - eg. `--master` etc.
  - can accept any spark config using `--conf` option

  ```bash
  spark-submit --master local[3] --conf spark.app.name="Hello Spark" --conf spark.eventLog.enabled=false HelloSpark.py
  ```

- `SparkConf` object

  - **_used by developers._**
  - to set configurations programmatically.

  ```python
  from pyspark.sql import SparkSession

  spark = SparkSession.builder.appName("Hello Spark").master("local[3]").getOrCreate()
  ```

  ```python
  from pyspark import SparkConf
  from pyspark.sql import SparkSession

  conf = SparkConf()

  # should know actual config property name string
  conf.set("spark.app.name", "Hello Spark")
  conf.set("spark.master", "local[3]")

  spark = SparkSession.builder.config(conf=conf).getOrCreate()
  ```

  > **Spark Docs:** [_application configuration property name strings_](https://spark.apache.org/docs/latest/configuration.html#application-properties)

<p align="center">
    <img src="https://github.com/user-attachments/assets/961807bc-2c9a-4fdc-9e9d-bb9beea38571" width="75%">
</p>

### When to use which configuration method?

- dont make your application depend on environment variables and `spark-defaults.conf` file.
- spark properties can be grouped into 2 categories:
  - **Deployment related configs:** `spark.driver.memory`, `spark.exector.instances` etc. these depend on deployment mode and cluster manager being chosen. We often set them from `spark-submit` command line.
  - **Control spark application runtime behaviour:**: `spark.task.maxFailures`, `spark.sql.shuffle.partitions` etc. these are set programmatically using `SparkConf` object.

> `spark-submit --help` to see options for `spark-submit` command.

## DataFrame

- 2D table like data structure inspired by Pandas DataFrame.
- **distributed table with _named columns_ and _well defined schema_**, each column has a specific data type such as integer, float, string, timestamp etc.
- optimized for distributed processing across clusters.

`SparkSession` offers a `read` method to read data from a file (csv, json etc.) which mostly will be stored in a distributed storage like HDFS, cloud storage (S3 etc). All these distributed storage systems are designed to partition the data file and store those partitions across the destributed storage nodes.

Each storage node may have one or more partitions of the data file. Spark DataFrameReader reads the data file, since the data is already partitioned so the DataFrameReader reads them as a bunch of in-memory partitions.

> A DataFrame can thus be considered as a bunch of smaller dataframes each logically representing a partition.

<p align="center">
    <img src="https://github.com/user-attachments/assets/409df9a7-7cb4-4bf5-bbb6-e066b32fff34" width="75%">
</p>



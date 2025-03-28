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

`# log4j.properties` file [code](code/log4j.properties)

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

**How DataFrame is distributed data structure?**  
**How it helps to perform distributed processing?**

`SparkSession` offers a `read` method to read data from a file (csv, json etc.) which mostly will be stored in a distributed storage like HDFS, cloud storage (S3 etc). All these distributed storage systems are designed to partition the data file and store those partitions across the destributed storage nodes.

Each storage node may have one or more partitions of the data file. Spark DataFrameReader reads the data file, since the data is already partitioned so the DataFrameReader reads them as a bunch of in-memory partitions.

> A DataFrame can thus be considered as a bunch of smaller dataframes each logically representing a partition.

<p align="center">
    <img src="https://github.com/user-attachments/assets/409df9a7-7cb4-4bf5-bbb6-e066b32fff34" width="75%">
</p>

`spark.read.csv()` where `spark` is the `SparkSession` object (driver). So, in order to read the file, the driver reaches out to cluster manager and storage manager to get details of the data file partitions. At runtime, driver knows how to read data file and how many partitions are there.

Hence, it creates a logical in-memory data structure called `DataFrame`. Nothing is loaded in the memory yet, it's just a logical structure with enough information to actually load it.

Driver again reaches out to cluster manager and asks for containers. Once those containers are allocated, the driver starts the executors on those containers. Each executor is a JVM process with some assigned CPU cores and memory.

Now the driver is ready to distribute the data file partitions to the executors. The driver sends the data file partitions to the executors. The executors load the data file partitions into memory and start processing the data.

<table>
    <tr>
        <td>
            <img src="https://github.com/user-attachments/assets/8490697a-da74-4a4f-adee-938fb2e0a586">
        </td>
        <td>
            <img src="https://github.com/user-attachments/assets/201b1c6c-9d1d-40b4-812e-89bdebfefc4f">
        </td>
</table>

- each executor core is assigned it's own data partition/s to work upon.
- spark tries to minimize the network bandwidth for loading data from the physical storage to the JVM memory.
  > it's an internal spark optimization, while assigning partitions to the executors, spark tries to allocate partitions which are closest to the executors in the network. However, such data locality is not always possible. Spark and the cluter manager tries to achieve the best possible data localization.

### Transformations

#### Narrow Dependency vs Wide Dependency Transformations

RDDs (Resilient Distributed Datasets) are Spark's core abstraction, representing immutable, partitioned collections enabling parallel operations. They are fault-tolerant due to lineage, which allows reconstruction of lost partitions, and distributed for efficient cluster processing. RDDs support **transformations** (lazy operations creating new RDDs, like `map` or `filter`) and **actions** (triggering computation and returning results, such as `count` or `collect`). While DataFrames and Datasets are preferred for structured data due to schema enforcement and optimization, RDDs remain valuable for unstructured data and situations requiring fine-grained control. Understanding narrow and wide transformations, which dictate data shuffling, is crucial for performance optimization. Spark's `read` operation defines data sources, acting as a setup rather than immediate execution, though `inferSchema=True` and certain file system interactions can trigger actions.

> Spark Dataframe is an abstraction built on top of RDDs, offering a higher-level, structured way to work with data, while RDDs are the fundamental, low-level data structure in Spark. Resilient distributed datasets (RDDs) and DataFrames are two storage organization strategies used in Apache Spark. RDD is a collection of data objects across nodes in an Apache Spark cluster, while a DataFrame is similar to a standard database table where the schema is laid out into columns and rows.

RDDs support two types of operations: **transformations**, which create a new dataset from an existing one, and **actions**, which return a value to the driver program after running a computation on the dataset. For example, `map` is a transformation that passes each dataset element through a function and returns a new RDD representing the results. On the other hand, `reduce` is an action that aggregates all the elements of the RDD using some function and returns the final result to the driver program (although there is also a parallel `reduceByKey` that returns a distributed dataset).

All transformations in Spark are _lazy_, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program. This design enables Spark to run more efficiently. For example, we can realize that a dataset created through `map` will be used in a `reduce` and return only the result of the `reduce` to the driver, rather than the larger mapped dataset.

By default, each transformed RDD may be recomputed each time you run an action on it. However, you may also _persist_ an RDD in memory using the `persist` (or `cache`) method, in which case Spark will keep the elements around on the cluster for much faster access the next time you query it. There is also support for persisting RDDs on disk, or replicated across multiple nodes.

Spark data processing is all about creating a **DAG (Directed Acyclic Graph)** of operations

- these operations are transformations and actions.
- **Transformations**: are lazy operations to transform one dataframe to
  another dataframe without modifying the original dataframe. eg. `where`.
  - **narrow dependency transformation**: transformation performed
    independently on a single partition to produce valid results. eg. `where`.
  - **wide dependency transformation**: transformation that requires data from
    other partitions to produce valid results. eg. `groupBy`.

<table>
    <tr>
        <td>
            <img src="https://github.com/user-attachments/assets/e4368896-0e8d-4f5a-a0b2-35cf09987a5a">
        </td>
        <td>
            <img src="https://github.com/user-attachments/assets/07a96502-8679-442c-a6ae-10c6ecf63c70">
        </td>
    </tr>
</table>

<p align="center">
    <img src="https://github.com/user-attachments/assets/96fddb8b-a4e0-4f7c-8d3a-dd06cb431478" width="75%">
</p>

##### Lazy Evaluation

- it's a functional programming technique.

```python
spark = SparkSession.builder.config(conf=conf).getOrCreate()

survey_df = load_survey_df(spark, sys.argv[1])
filtered_df = survey_df.where("Age < 40")
selected_df = filtered_df.select("Age", "Gender", "Country", "state")
grouped_df = selected_df.groupBy("Country")
count_df = grouped_df.count()

count_df.show()
```

- Here, we are using builder pattern to create a DAG (Directed Acyclic Graph)
  of transformations. All of this go to spark driver, spark driver creates an
  optimized execution plan and sends it to the executors.

> These statements are not executed as individual operations but they are converted into an optimized execution plan which is triggered/terminated by an action.

### Actions

- READ, WRITE, COLLECT, SHOW etc.
  - eg. `df.show()`

<table>
    <tr>
        <td>
            <img src="https://github.com/user-attachments/assets/07d42eeb-91ba-4b2c-a072-c3ee7be3a850">
        </td>
        <td>
            <img src="https://github.com/user-attachments/assets/89d99ee5-0d53-4d4f-b0dd-9b3ae7b4d17c">
        </td>
    </tr>
</table>

## Jobs, Stages and Tasks

- **Job:** A job in Spark _corresponds to an **action**_ (e.g, `collect`, `count`, `show`) that triggers the execution of a set of transformations. Each job can consist of one or more stages.
- **Stage:** A stage is a _group of tasks that can be executed together without shuffling data/Exchange_. Stages are separated by shuffle boundaries (wide transformations like `groupBy`, `join`, etc.).
- **Task:** A task is the _smallest unit of work in Spark_, corresponding to processing a partition of data within a stage.

> spark break the code into sections separated by actions. Each section is called a job. Each job is further divided into stages. Each stage is further divided into tasks.

- reading a file, infer schema are 2 intenal actions. Hence, they are 2 jobs.
- `repartition` is a wide transformation, hence it's a stage.
- `groupBy` is a wide transformation, hence it's a stage.
- `collect` is an action, hence it's a job.

<p align="center">
    <img src="https://github.com/user-attachments/assets/82a1cd53-5290-41f4-95b2-fbfefad8cc5e" width="75%">
</p>

- `repartition`, `select`, `where`, `groupBy` and `count`: all these are planned as single job triggered by `collect` action.
- spark will create a DAG for each job and break it into stages separated by a shuffle/exchange _(internal buffer)_ operation.

<table>
    <tr>
        <td>
            <img src="https://github.com/user-attachments/assets/ebf9965e-4be9-424a-b937-3b2e930052bb">
        </td>
        <td>
            <img src="https://github.com/user-attachments/assets/9ef36720-561c-49f8-8947-4d164cd03eba">
        </td>
    </tr>
</table>

```python
import configparser
import sys

from pyspark import SparkConf


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for key, val in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)

    return spark_conf
```

```python
conf = get_spark_app_config()
spark = SparkSession.builder.config(conf=conf).getOrCreate()
```

`# spark.conf`
```
[SPARK_APP_CONFIGS]
spark.app.name = Hello Spark
spark.master = local[3]
spark.sql.shuffle.partitions = 2
spark.sql.adaptive.enabled = false
# app.author = Alok Shandilya
```

> AQE (adaptive query execution) can otherwise create different number of jobs in this case.

### Unit testing

- `unittest` module
- see example here [code](code/test_utils.py)

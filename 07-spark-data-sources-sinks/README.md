## Data Sources and Sinks

- spark is used to process large volumes of data.
- any processing engine must read data from some data source.
- spark data sources can be classified into two categories: **external** and **internal**.
- **External**: data from some source system eg. oracle, sql server db, some application server such as application logs. All these systems are external to the data lake.
  - **JDBC (java database connectivity) data sources**: Oracle, SQL Server, PostgreSQL, MySQL, etc.
  - **NoSQL data sources**: MongoDB, Cassandra etc.
  - **Cloud Data Warehouses**: Snowflake, Amazon Redshift etc.
  - **Stream Integrators**: Kafka, Kinesis, etc.
    > can't process data from these sources directly. We need to read them and create a dataframe or a dataset. There are **2 approaches**:
    >
    > 1. bring data to data lake and store them in the data lake distributed storage. Most common approach is to use a suitable data integration tool like Talend, Informatica, HVR, AWS Glue, etc. **Preferred for batch processing requirements.**
    > 2. use spark datasource API to directly connect with these systems. **Preferred for streaming processing requirements.**
- **Internal**: it's the distributed storage of the data lake like HDFS or cloud based storage (Amazon S3, Azure Blob, Google Cloud etc.). Data is stored in these systems as data files. Reading data from these systems is same process the difference lies in data formats.
  - **file formats**: CSV, JSON, Parquet, Avro, Plain Text, ORC, etc.
  - **2 more options**: Spark SQL Tables, Delta Lake _(these are also data files but they include some meta data outside the data file)_.
- **Data Sinks**: final destination of the data after processing. So, data is loaded from some internal/external source, handling/processing using Spark APIs.
  - save processed data to some external/internal system.
  - these systems could be data file in data lake storage.
  - could be external system such as JDBC, NoSQL database etc.

> Data Source is about reading the data while Data Sink is about writing the data.

Same as Sources, spark allows to write data in variety of file formats, SQL tables, delta lake. Spark also allows to write data directly to bunch of external systems such as JDBC, NoSQL databases.

> It's not recommended to write read/write data from/to the external systems.

<p align="center">
    <img src="https://github.com/user-attachments/assets/e113c784-8e96-4950-8528-231b8331cbac" width="75%">
</p>

### Spark Data Source API

- spark offers a standardized API to work with data sources. These APIs have a well-defined format and a recommended pattern for use.
- `DataFrameReader` API: [docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html)

**General Structure**:

```
DataFrameReader
    .format(...)
    .option("key", "value")
    .schema(...)
    .load()
```

**Indicative Example**:

```python
spark.read
    .format("csv")
    .option("header", "true")
    .option("path", "data/mycsvfiles/")
    .option("mode", "FAILFAST")  # 3 options:
    .schema(mySchema)
    .load()
```

- **`spark.read` type**: `pyspark.sql.readwriter.DataFrameReader`
- **built-in formats**: csv, json, parquet, orc, jdbc etc
- **community formats**: cassandra, mongodb, avro, xml, hbase, redshift etc.
  - **option**: every data source has its own set of options to determine how the dataframereader is going to read the data.
  - `header` option here is specific to CSV file format. It tells the reader to read the first line of the file as header.
    > look into documentation of the data source to find the options avaialble for that data source.
- **mode**: for read-mode options. Reading data from a source file especially a semi-structured data sources such as CSV, JSON, XML may encounter a currupt or malformed record.
  - read-mode specify what will happen when a malformed record is encountered.
  - **3 read modes**: `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`
    1. **PERMISSIVE**: default mode. it sets all the fields of the malformed record to `null` and places the currupted record in a string column called `_corrupt_record`. This is useful for debugging and allows you to inspect the malformed records later.
    2. **DROPMALFORMED**: it will drop the malformed record and continue reading the rest of the records. Only valid records will be returned in the DataFrame. This is useful when you want to ignore the bad records and only keep the good ones.
    3. **FAILFAST**: it will throw an error and stop reading the file as soon as it encounters a malformed record. This is useful when you want to ensure that all records are valid and you don't want to process any bad records.
- **schema**: it's optional for 2 reasons (infer schema in many cases, some data sources like parquet, avro etc. come with well-defined schema inside the data source itself).
  - **explicit**
  - **infer schema**
  - **implicit**: like parquet, avro etc.
- :star2: refer [code]()

> DataFrameReader also comes with some shortcuts and variations. eg. `csv()`, `json()` etc. It's recommended not to use these shortcuts as they are not consistent with the rest of the API. It's better to use the `format()` method and specify the format explicitly for better code maintainability and readability.

> use parquet file formats wherever possible.

## Creating Spark DataFrame Schema

- schema inference doesn't work for all data sources. _(see code above for csv,
  json)_
- Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame. This conversion can be done using `SparkSession.read.json` on a JSON file.
- Note that the file that is offered as a _json file_ is not a typical JSON file. Each line must contain a separate, self-contained valid JSON object. [For more information, please see JSON Lines text format, also called newline-delimited JSON](https://jsonlines.org/).
- For a regular multi-line JSON file, set the `multiLine` parameter to `True`.

to explicitly setting schema for dataframes see [code]()

- dataframe schema is all about setting the column name and appropriate data
  types. _one should know spark supported data types._
  - [PySpark DataTypes](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html)
- some are below:
  | # | Spark Types | Scala Types | Python Types |
  | :-: | ------------- | -------------------- | -------------------- |
  | 1. | IntegerType | Int | Int |
  | 2. | LongType | Long | Long |
  | 3. | FloatType | Float | Float |
  | 4. | DoubleType | Double | Float |
  | 5. | StringType | String | String |
  | 6. | DateType | java.sql.Datate | datetime.date |
  | 7. | TimestampType | java.sql.Timestamp | datetime.datetime |
  | 8. | ArrayType | scala.collection.Seq | list, tuple or array |
  | 9. | MapType | scala.collection.Map | dict |

> we define spark dataframe schema using Spark Types.

##### Why spark maintains it's own types?, Why don't we simply use language specific types?

Spark is like a compiler, it compiles high level API code
into low level RDD operations. During this compilation, it generates
different execution plans and also perform optimizations. This is not
possible for spark engine without maintaining it's own types. It's not spark
specific, every SQL database would have a set of SQL data types. Similarly,
spark also works on Spark Types.

Spark allows to define schema in 2 ways:

- **Programatically**: a spark dataframe schema is a `StructType` which is made
  of list of `StructField`. `StructField` takes 2 mandatory arguments
  _(column_name, data_type)_
  - `StructType`: represents the schema of a DataFrame. It is a collection of `StructField` objects.
  - `StructField`: represents a single field in the schema. It contains the name, data type, and whether the field can be null or not.
  - refer code: [code]()
- **Using DDL (Data Definition Language) string** _(much simpler)_
- refer code: [code]()

```python
employeeSchemaStruct = StructType([
    StructField("name", StringType()),
    StructField("age", IntegerType()),
    StructField("join_date", DateType()),
])

employeeSchemaDF = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(employeeSchemaStruct) \
    .option("mode", "FAILFAST") \
    .load("data/employee.csv") \
```

- `StructType` represents a dataframe **row structure**.
- `StructField` is a **column definition**.

## Spark Data Sink API

- `DataFrameWriter` API: [docs](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html)

**General Structure**:

```
DataFrameWriter
    .format(...)
    .option(...)
    .partitionBy(...)
    .bucketBy(...)
    .sortBy(...)
    .save()
```

**Indicative Example**:

```python
DataFrame.write
    .format("parquet")
    .mode("saveMode")
    .option("path", "/data/flights/")
    .save()
```

- **`format`**: specifies the format of the data to be written.
  - _by default_, it is set to `parquet`.
  - **built-in formats**: csv, json, parquet, orc, jdbc etc.
  - **community formats**: cassandra, mongodb, avro, xml, hbase, redshift etc.
- **`option`**: specifies the options for the data source.
- **`mode`**: specifies the save mode for the data source.
  - **4 save modes**: `append`, `overwrite`, `ignore`, `error` (default)
    1. **append**: appends the data to the existing data in the data source.
    2. **overwrite**: overwrites the existing data in the data source with the new data.
    3. **ignore**: ignores the new data if the data source already exists.
    4. **errorIfExists**: throws an error if the data source already exists.

### Spark File Layout

1. Number of files and file size.
2. Organizing output in partitions and buckets.
3. Storing sorted data.

Dataframes are partitioned. when we write dataframe to file system, we get one output file per partition (_default behaviour_). Each partition is written by an execution core in parallel. This default behaviour can be tweaked:

- repartitioning the dataframe before writing it to file system.

  - `DataFrame.repartition(n)`: repartitions the dataframe into `n` partitions, could be blind repartitioning which will not help in most of the situations.
  - `DataFrame.partitionBy(col1, col2)`: repartition data based on key column (_single such as `country_code`, or composite column such as `country_code` + `state_code`_). Helps to break the data logically. Helps to improve the spark SQL performance using partition pruning technique.
    1. `maxRecordPerFile`: allows limiting number of records per file. Helps to control the file size based on the number of records and prevent from creating huge and inefficient files.
  - `DataFrame.bucketBy(n, col1, col2)`: partition data into fixed number of pre-defined buckets, known as _bucketing_. However, it's only avaialble on spark managed tables.
    1. `sortBy()`: sort the data within each bucket, create sorted buckets.

  > `partitionBy`, `bucketBy` are 2 options to logically partition the data.

Paritioning data into equal chunks may not make perfect sense in most of the cases. However, we do want to partition the data _(because we will be working with massive volumes, partitioning have 2 benefits **parallel processing**, **partition elimination for certain read operations**)_

The random and equal paritions achieves parallel processing but not partition elimination. So, we might want to partition data for specific columns using `partitionBy`.

<p align="center">
    <img src="https://github.com/user-attachments/assets/5446409a-3b9a-418a-bdc8-b3aa39ef9f01" width="75%">
</p>

Here, in the json file got in `OP_CARRIER=HP/ORIGIN=ABQ` will not have `OP_CARRIER`, `ORIGIN` because of redundancy.

`OP_CARRIER=DL/ORIGIN=ATL` is the biggest file got with ~19K records. We can use `maxRecordPerFile` for 10k _(it will split the result file into 2 files) [see](code/02-DataSink/dataSink/json/OP_CARRIER=DL/ORIGIN=ATL/)._

```python
.option("maxRecordsPerFile", 10000).save()
```

## Spark Databases and Tables

Apache spark is not only a set of APIs and a processing engine, it's can also be seen as a database in itself. We can create database in spark, in the database we can create **tables** and **views**.

- table has _table data (as datafiles in the distributed storage)_ and _table metadata (stored in **catalog metastore**)_.
  - by default, the datafile is in parquet format, but we can change it to any other format.
  - catalog metastore holds the information about table, it's data such as schema, table name, database name, column names, partitions, the physical location where the actual data resides.
  - by default, the catalog metastore is in memory which is maintained per
    spark session, and goes away when session ends.
  - for persistent, durable metastore: _apache hive metastore_.

Spark allows to create **2 types of tables**:

- **Managed Tables**: _manages both metadata and data_.
  - creating managed table `dataframe.write.saveAsTable("table_name")`, spark
    does 2 things: create and store metadata in the catalog metastore, write data
    inside a predefined directory location `spark.sql.warehouse.dir`, which is
    set up by cluster admin.
    > we can't and shouln't change this on runtime.
- **Unmanaged tables (external tables)**: same with respect to metadata but
  different in terms of data storage location.

  - create metadata in the catalog metastore.
  - must specify the data directory location for the table.
  - give flexibility to store data at your preferred location.
    > suppose we have data stored somewhere and we want to perform spark sql
    > queries on that data. eg. `SELECT count(*) FROM ...`, Spark SQL engine does
    > not know anything about this data. We can create an unmanaged table and map
    > the same data to a spark table, now spark will create metadata and store
    > data. This will allow you to execute spark sql on this data.

  > **Note**: if we drop a managed table, it will drop both metadata and data.
  > If we drop an unmanaged table, it will only drop the metadata but not the
  > data. Unmanaged table are designed for temporarily mapping you existing
  > data and using it in spark sql. Once you are done using it, drop them and
  > only metadata goes away, leaving the data files intact.

  > We prefer managed tables because it offers additional features such as
  > bucketing, sorting. All future improvements in spark sql will target
  > managed tables, unmanaged tables are external tables and spark doesn't have
  > enough control on them. They are designed for reusing existing data in
  > spark sql and it should be used in those scenarios only.

```sql
CREATE TABLE table_name (col1 data_type, col2 data_type, ...)
USING PARQUET
LOCATION "data_file_location"
```

<table>
    <tr>
        <td>
            <img src="https://github.com/user-attachments/assets/5b8893c7-52cf-4045-8613-22087666155d">
        </td>
        <td>
            <img src="https://github.com/user-attachments/assets/f76b38f4-1a92-4b12-9bf9-9feaaa3ee1b3">
        </td>
    </tr>
    <tr>
        <td>
            <img src="https://github.com/user-attachments/assets/eb0b8863-62bd-4adc-a575-120d7e926a24">
        </td>
        <td>
            <img src="https://github.com/user-attachments/assets/26852c2c-e583-470a-9036-5b77753b7ba5">
        </td>
    </tr>
    <tr>
        <td>
            <img src="https://github.com/user-attachments/assets/dc2eb63e-439c-4f00-812c-a95adefeef45">
        </td>
    </tr>
</table>

<table>
    <tr>
        <td>
            <img src="https://github.com/user-attachments/assets/7a1b3295-496e-4494-857c-ab99c6428a0e">
        </td>
        <td>
            <img src="https://github.com/user-attachments/assets/2e7241ac-d0b4-41b3-8e63-4800ada227e3">
        </td>
    </tr>
</table>

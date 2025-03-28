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

# Introduction

## History

- for our applications which did require storing and processing data.
  - for a long time it was enough to store data in a single machine.
  - as the data grew over time and single machine was not enough even with advanced hardware.
- Google published a paper in 2004 on **Google File System (GFS)** and **MapReduce**.
  - GFS is a distributed file system that can store large amount of data across multiple machines.
  - MapReduce is a programming model to process large amount of data in parallel across multiple machines.
  - led to the development of a similar open source implementation called **Hadoop** having **HDFS (Hadoop Distributed File System)** and **Hadoop MapReduce**.

## Data Lake

- before HDFS and MapReduce in Hadoop, we had Data Warehouses (Teradata, Exadata etc.)
  - in datawarehouses, we make data piplelines to collect data from bunch of OLTP systems and brought them to data Warehouse, process data to extract business insights and use it to make right business decisions.

<p align="center">
    <img src="https://github.com/user-attachments/assets/57c44f35-4dbf-419f-8f54-f663af9a95ef" width="75%">
</p>

> A **data lake** is a centralized system that stores, processes, and ingests large amounts of data in its original form. It can store all types of data, including structured, unstructured, and semi-structured data

4 key capabilities of Data Lake:

- **Ingest:** data collection and ingestion
- **Store:** data storage and management
- **Process:** data processing and transformation
- **Consume:** data access and retrieval

#### Storage Layer

> The core of the data lake is the storage infrastructure (could be on-premise HDFS or cloud object stores like Amazon S3, Azure Blob, Google Cloud etc, cloud object stores are popular due to scalability, high availability access, low cost)

#### Ingestion layer

The notion of the data lake recommends to bring data to the lake in raw format.

- it means to ingest data into the lake and preserve an unmodified immutable copy of the data.
- the ingest block of the data lake is all about indentifying, implementing and managing the right tools to bring data from the source system to the data lake.
- no single tool is suited for all cases. so we have many options here (HVR, AWS Glue, informatica, talend etc.)

#### Processing Layer

the layer where all computation happens (initial data quality check, transforming and preparing data, correlating, aggregating and analyzing, applying machine learning model).

all this is to be done on a large scale, so processing layers can be split in 2 parts.

- **Data processing framework**
  - core development framework allowing to design and develop distributed computing applications. **Apache Spark** falls in this place.
- **Orchestration framework**
  - responsible for the formation of the cluster, managing resources, scaling up/down etc.
  - 3 competitors (Hadoop YARN, Kubernetes, Apache Mesos)

#### Consume Layer

to consume data from the data lake. Data lake can be understood simply as a repository of raw and processed data.

> Consumption is all about putting that data for real life usage but the consumption requirement will come in all possible formats (data analyst, data scientist, application dashboard, REST interface, file download, JDBC/ODBC connectors, Search)

- every data consumer comes with a different requirement and expectations complicating the consumption layer.
- we have many tool options for this layer too.

---

<p align="center">
    <img src="https://github.com/user-attachments/assets/fd75d8f7-3229-4a92-8b1a-f2893c0ceeb8" width="75%">
</p>

> we need many additional capabilities for implementation of our data lake like security, workflow, metadata, data lifecycle, montoring, operations.

## Apache Spark

- most popular, most widely adopted data processing framework in a data lake.
- a unified analytics engine for large-scale data processing.
- a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.

## Spark Ecosystem

<p align="center">
    <img src="https://github.com/user-attachments/assets/a1fec4f3-75e9-4aa8-be57-5375d14ebbe8" width="75%">
</p>

> Spark does not offer cluster management and storage management. All it offers is to run data processing workload managed by spark compute engine.

Spark Engine is responsible for breaking down the data processing work into smaller tasks and scheduling the tasks on the cluster for parallel execution, providing data to these tasks, managing and monitoring these tasks, provide fault tolerance when a job fails, and finally, interact with the cluster manager and the storage system.

- **Spark Core layer:** includes spark engine (a distributed computing engine) and set of core APIs, it all runs on a cluster of machines.
  - cluster management is done by other frameworks like Hadoop YARN, Apache Mesos or Kubernetes. also, known as resource manager, container orchestrator.
  - spark also does not come with a inbuilt storage system, it allow to process the data which is stored in variety of storages (like HDFS, S3, Azure Blob, GCS, CFS)

> Spark Core APIs offer a set of APIs in different languages (Scala, Java, Python, R) to develop distributed computing applications. In early releases, these APIs are built on top of RDDs (Resilient Distributed Datasets) but later releases, Spark introduced DataFrames and Datasets APIs which are built on top of RDDs as the core APIs are bit tricky to use, it's recommended to not use these APIs directly.
>
> > these APIs offers highest level of flexibility for some complex data processing problems.

**the topmost layer is of prime interest**. this layer is set of libraries, packages, APIs, DSLs (domain specific languages) built by community over and above core APIs. this layers is grouped into 4 categories:

- **SQL and DataFrames:** Spark SQL, DataFrames, Datasets
  - functional programming style APIs.
- **Streaming and Real-Time:** Spark Streaming, Structured Streaming
  - for continuos and unbounded stream of data.
- **Machine Learning:** MLlib
  - for machine learning, deep learning and AI requirements.
- **Graph Processing:** GraphX
  - for graph processing requirements.

> this grouping is only for understanding, in reality, these libraries are not independent of each other. they are built on top of core APIs and can be used together in a single application.

### Why Spark?

- **Abstraction:** Spark abstracts the complexity of distributed computing (that you are coding to execute your program in a cluster of machines), we'll be working on tables using SQL queries, RDDs, dataframes.
- **Unified Platform:** combines capabilities of SQL queries, batch processing, stream processing, structured and semi-structured data processing, graph processing, machine learning, deep learning, AI etc.
- **Ease of use:** in comparison to Hadoop MapReduce, Spark code is shorter, simpler and more readable.

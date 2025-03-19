# Introduction

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

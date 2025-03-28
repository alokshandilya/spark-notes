## Introduction to Spark APIs

Spark was developed with the goal of simplifying and improving upon the Hadoop MapReduce programming model. To achieve this, Spark introduced Resilient Distributed Datasets (RDDs) as its core abstraction, which provides a fault-tolerant, distributed data structure that supports both batch and interactive processing.

Additionally, Spark employs a Directed Acyclic Graph (DAG) execution engine to optimize job execution by breaking tasks into stages and minimizing data shuffling, enabling faster and more efficient processing compared to Hadoop MapReduce.

- **RDD APIs:** core, most challenging, offers most flexibility but lacks optimization by catalyst optimizer.
- **Catalyst Optimizer:** optimizes Spark SQL, Dataframe APIs, Dataset APIs.
  - lays out optimized execution plan.
- **Spark SQL:** most convinient
  - we don't get debugging, logs, unit testing etc. that a programming language has.
  - used for basic SQL operations.
  - a sophisticated data pipeline will be using Dataframe APIs instead.
- **Dataframe APIs**: most important
- **Dataset APIs**: language native APIs in Scala and Java.
  - these APIS are strongly typed objects in JVM based in languages.
  - not applicable in dynamically typed languages like Python.

<p align="center">
    <img src="https://github.com/user-attachments/assets/1ffd2f41-aa86-478a-bdb0-b8a08b4cf3ae" width="75%">
</p>

### RDD APIs - Introduction

- **RDDs** - Resilient Distributed Datasets
- is a data structure (dataset) to hold data records.
  - dataframes are built on top of RDDs.
- are language native objects and **don't have a schema, row-column structure**.
- can create a RDD from reading a file, are internally broken into partitions to form distributed collection same as dataframes.
- are resilient (fault-tolerant) as they also store information of how they are created.

> assume an RDD partition is assigned to an executor core for processing. If the executor fails/crashes, the driver notices the failure and reassigns the partition to another executor core. The new executor core will realod the partition and continue processing. This is possible because each RDD partition store information of how it was created, how to process it.

> RDD partition can be re-created and re-processed anywhere in the cluster.

### How to create an RDD?

:star2: [code](code/01-RDD/HelloRdd.py) - _create a simple RDD_

**Note**:

- spark engine doesn't know data structure inside the RDD.
- spark could not look inside the lambda functions.

These 2 things make RDDs less optimized compared to Dataframe APIs because spark could not optimize the execution plan.

## Spark SQL

- most convinient
- works only tables and views.
- sparks allows to register a dataframe as a view, using `createOrReplaceTempView` method.
- [code](code/02-SparkSQL/HelloSparkSQL.py) - _create a simple Spark SQL_

<p align="center">
    <img src="https://github.com/user-attachments/assets/8bf402f3-a9a0-4f88-bab3-be194315c417" width="75%">
</p>

SparkSQL, Dataframe APIs, Dataset APIs are internally powered by Spark SQL Engine, which is a compiler that optimizes the code and generates efficient java bytecode.

- **Analysis:** reads the code and creates abstract syntax tree (AST) for SQL or dataframe queries. Column names, tables, view names, SQL functions are resolved, we might get a runtime error as analysis error at this stage when the names are not resolved.
- **Logical Optimization:** SQL Engine will applies rules based optimization and construct a set of multiple execution plans, the catalyst optimizer will use cost based optimization to assign a cost to each plan. It includes standard SQL optimization techniques like predicate pushdown, projection pruning, boolean expression simplification, constant folding, etc.
- **Physical Planning:** the SQL Engine picks the most effective logical plan and generates a physical plan (set of RDD operations determining how plan will execute on cluster).
- **Code Generation:** generate efficient Java bytecode to run on each machine, was introduced in spark 2.0 from project Tungsten.

> Project Tungsten was initiated to apply ideas from modern compilers and MPP databases (massively parallel processing databases) and make spark run more efficiently.

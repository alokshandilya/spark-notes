# Transformation: Introduction

Whole point of data engineering in apache spark is to be able to read the data, apply a series of transformations and write it back for consumption.

_Data Source_ $\to$ _Transform_ $\to$ _Data Sink_

> In spark, we read the data and create one of these 2 (_DataFrame_, _DB table_), both are same but different interfaces.
>
> - _dataframe is programmatic interface of the data._
> - _db table is sql interface of the data._
>
> 2 approaches for transforamtions (programming approach on dataframes), (sql expressions on db tables).

<table>
    <tr>
        <td>
            <img src="https://github.com/user-attachments/assets/ffab40ae-2aca-4115-8518-6cc92fea58c6">
        </td>
        <td>
            <img src="https://github.com/user-attachments/assets/6376f5d1-a79d-4792-8938-68405294530e">
        </td>
    </tr>
</table>

**Transformations** are operations in Spark that you apply to a distributed dataset (like a DataFrame or Dataset) to create a **new** distributed dataset. A key characteristic of transformations is that they **do not modify the original data**; instead, they return a new, transformed version of the data, providing the property of **immutability**. For example, operations like `select()` or `filter()` will not change the original DataFrame but will produce a new DataFrame with the results of the operation.

Here are some important aspects of transformations based on the sources:

- **Lazy Evaluation:** All transformations in Spark are evaluated **lazily**. This means that when you apply a transformation, the result is not computed immediately. Instead, Spark records the transformation and builds up a **lineage** of operations. The actual computation only happens when an **action** is invoked or when the data is "touched" (read from or written to disk). This lazy evaluation strategy allows Spark to **optimize** the entire query execution plan by rearranging or coalescing transformations for efficiency.
- **Lineage and Fault Tolerance:** Because Spark keeps track of each transformation in a lineage and DataFrames are immutable, it can achieve **fault tolerance**. If a node fails during computation, Spark can recompute the lost partitions by replaying the recorded lineage from the original data.
- **Types of Transformations:** Transformations can be broadly categorized based on their dependencies:
  - **Narrow Transformations:** In a narrow transformation, a single output partition can be computed from a single input partition without any exchange of data between partitions. Examples include `filter()`, `select()`, `map()`, and operations using functions like `contains()`.
  - **Wide Transformations:** Wide transformations (also known as shuffles) require data from multiple partitions to be combined to produce the output partitions. Examples include `groupBy()`, `orderBy()`, and `join()`. These operations involve data being shuffled across the network between executors, which can be resource-intensive.
- **Structured Streaming:** In the context of Structured Streaming, only DataFrame operations that can be executed **incrementally** on continuous data streams are supported as transformations. These are further classified as **stateless** (e.g., `select()`, `filter()`, `map()`) and **stateful** (e.g., `groupBy()`, `join()`, aggregations). Stateful transformations require maintaining state across micro-batches.
- **Examples of Transformations:** The sources provide numerous examples of transformations:
  - `orderBy()`, `show()` (Note: `show()` is an action, the table lists it incorrectly), `groupBy()`, `filter()`, `select()`, `join()`.
  - `map()`, `reduce()`, `aggregate()` (for Datasets).
  - `withColumn()` (to add or modify a column), `withColumnRenamed()` (to rename a column), `alias()` (to give a new name to a transformed column).
  - `cast()` (for type casting).
  - `groupBy()` (for grouping data).
  - `pivot()` (to swap columns and rows).
  - Window functions (like `row_number()`, `rank()`, `dense_rank()`).

In essence, transformations define a series of steps to manipulate your distributed data in a declarative way. Spark then optimizes and executes these steps in parallel when an action is finally triggered.

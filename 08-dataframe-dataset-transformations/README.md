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

### Working with Dataframe Rows

These are the 3 scenarios where you may need to work with the row object. _1, 2 are also helpful in unit testing._

<p align="center">
    <img src="https://github.com/user-attachments/assets/dedaaca1-6bc3-443d-9990-9cbefd4db71c" width="75%">
</p>

- In PySpark, a **row** is represented by a `Row` object from the `pyspark.sql` module.
  > Conceptually, a row in a Spark DataFrame is similar to a row in a relational database table. It's an ordered collection of values, where each value corresponds to a column in the DataFrame. Rows within a DataFrame can contain columns of the same or different data types (e.g., integer, string, array, map).

**1. Creating Rows:**

You can explicitly create `Row` objects in PySpark:

```python
from pyspark.sql import Row

# Creating a Row object
person = Row(name="Alice", age=30, city="New York")
print(person)
```

You can access elements within a `Row` object by their index (0-based) or by the field name if the `Row` was created with named fields:

```python
print(person)  # Access by index
print(person.name) # Access by field name
```

Furthermore, you can create DataFrames from a list of `Row` objects, often used for quick interactivity or testing:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

data = [
    Row("Bob", 25, "Los Angeles"),
    Row("Charlie", 35, "Chicago")
]

df = spark.createDataFrame(data, schema)
df.show()
```

**2. Rows as Part of DataFrames:**

A DataFrame in PySpark is essentially a distributed collection of `Row` objects, organized under a schema that defines the names and data types of each column. When you perform operations on a DataFrame, you are often working with these underlying rows, either individually or in groups. In Scala, the DataFrame is type-aliased to `Dataset[Row]`, emphasizing this structure.

**3. Operations Involving Rows:**

While you don't typically iterate over rows in a PySpark DataFrame in the same way you might with a Pandas DataFrame (due to Spark's distributed nature), many DataFrame operations implicitly work with rows to achieve data transformation and analysis.

- **Filtering Rows:** The `where()` or `filter()` methods allow you to select rows that satisfy a given condition:

  ```python
  ca_count_mnm_df = (mnm_df
      .select("State", "Color", "Count")
      .where(mnm_df.State == "CA") # Filtering rows where State is "CA"
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy("Total", ascending=False))
  ca_count_mnm_df.show(n=10, truncate=False)
  ```

  In the above example, `.where(mnm_df.State == "CA")` filters the DataFrame to include only those rows where the value in the "State" column is equal to "CA".

- **Selecting Columns (Projections):** While focused on columns, the `select()` method determines which fields from each row are included in the resulting DataFrame:

  ```python
  selected_df = mnm_df.select("State", "Color") # Each row in selected_df will only have "State" and "Color" fields
  selected_df.show(5)
  ```

- **Adding Columns:** The `withColumn()` method adds a new column to the DataFrame. This operation affects each row by adding a new field with a computed or constant value:

  ```python
  from pyspark.sql.functions import expr

  foo2 = (foo.withColumn(
             "status",
             expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
          ))
  foo2.show()
  ```

  Here, a new column "status" is added to each row based on the value of the "delay" column.

- **Modifying Column Values:** `withColumn()` can also be used to modify the values of an existing column for each row based on some transformation. You can achieve this by using the same column name as the first argument to `withColumn()`.

- **Dropping Columns:** The `drop()` method removes specified columns from the DataFrame, effectively reducing the number of fields in each row of the new DataFrame:

  ```python
  foo3 = foo2.drop("delay") # The "delay" field will be removed from each row in foo3
  foo3.show()
  ```

- **Renaming Columns:** The `withColumnRenamed()` method changes the name of a column, thus changing the name of a field in each row:

  ```python
  foo4 = foo3.withColumnRenamed("status", "flight_status") # The "status" field in each row is now "flight_status"
  foo4.show()
  ```

- **Sorting Rows:** The `sort()` or `orderBy()` methods reorder the rows in the DataFrame based on the values in one or more columns:

  ```python
  df.sort(col("age").desc()).show() # Rows are ordered by the "age" field in descending order
  ```

- **Grouping and Aggregation:** The `groupBy()` method groups rows that have the same values in specified columns. Subsequently, aggregation functions (like `count()`, `sum()`, `avg()`, etc.) are applied to these groups of rows to compute summary statistics:

  ```python
  grouped_df = mnm_df.groupBy("State", "Color").agg(count("Count").alias("Total"))
  grouped_df.show()
  ```

  Here, rows with the same "State" and "Color" are grouped together, and then the `count()` aggregation function is applied to the "Count" column within each group.

- **User-Defined Functions (UDFs):** UDFs allow you to apply custom logic to each row (or individual column values within each row). You can register Python functions as UDFs to perform row-level transformations:

  ```python
  from pyspark.sql.functions import udf
  from pyspark.sql.types import StringType

  def categorize_age(age):
      if age < 18:
          return "Minor"
      elif age < 65:
          return "Adult"
      else:
          return "Senior"

  categorize_age_udf = udf(categorize_age, StringType())
  df_with_category = df.withColumn("age_category", categorize_age_udf(df["age"]))
  df_with_category.show()
  ```

  In this example, the `categorize_age` function is applied to the "age" column of each row to create a new "age_category" column. For better performance in many scenarios, consider using Pandas UDFs (vectorized UDFs) which operate on batches of rows.

- **Higher-Order Functions:** When dealing with array or map columns within rows, Spark provides higher-order functions like `transform()`, `filter()`, `exists()`, and `reduce()` to operate on the elements of these complex data types within each row.

- **Window Functions:** Window functions perform calculations across a set of DataFrame rows that are related to the current row. They are useful for tasks like calculating running totals, ranks, or moving averages based on the ordering and partitioning of rows.

- **Actions on DataFrames:** Actions like `show()`, `collect()`, `foreach()`, and writing data to external storage trigger the execution of the DataFrame's lineage and involve processing the rows. `collect()` brings all the rows to the driver's memory (use with caution on large datasets), while `foreach()` allows you to apply a function to each row.

  ```python
  for row in df.collect():
      print(row)

  def process_row(row):
      # Perform some operation on each row
      print(f"Name: {row.name}, Age: {row.age}")

  # This will execute on the driver
  # df.foreach(process_row) # Use with caution, operations happen on the driver

  # This will execute on the executors
  # df.rdd.foreach(process_row)
  ```

> Understand that most DataFrame transformations in Spark are **lazy**. They define a series of operations that will be performed, but the actual computation on the rows happens only when an action is triggered.

> Code example
>
> - [_code_](code/01-DataFrameRows/DataFrameRows.py)
> - [_code_](code/01-DataFrameRows/DataFrameRows_Test.py) for _test case_

#### 3 Scenarios when we directly work with row objects

- manually creating rows and dataframe. (_already discussed_)
- collecting dataframe rows to the driver. (_already discussed_)
- _work with an individual row in spark transformation._

Spark dataframe offers many transformation functions. We use these methods when dataframe has schema, if we don't have proper schema for dataframe (not having column structure).

<p align="center">
    <img src="https://github.com/user-attachments/assets/c753b633-04c7-4ce7-b542-31ba87064c32" width="75%">
</p>

We need an extra step to create a column structure and then transformation. Work with row only and transform it into a column structure.

- see code [link](code/02-DataFrameRows2/LogFileDemo.py)

## Working with Dataframe Columns

working with a dataframe requires clear answer to questions lke:

1. What is a Column and how to reference it?
2. How to create column expressions.

<p align="center">
    <img src="https://github.com/user-attachments/assets/5a24f9b8-b3c4-465f-9e29-61bc983fc501" width="75%">
</p>

```python
airlineDF: DataFrame = (
    spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("data/sample.csv")
)

# using column strings
airlineTrimDF: DataFrame = airlineDF.select("Origin", "Dest", "Distance").limit(10)  # noqa: E501

airlineTrimDF.show(10)
logger.info(airlineTrimDF.collect())

# using columns objects (many ways)
# - column, col, <df>.<column>
airlineTrimDF.select(column("Origin"), col("Dest"), airlineTrimDF.Distance).show(10)
```

##### How to create column expressions?

2 ways:

- string expressions or SQL expressions

  ```python
  # string expressions or SQL expressions
  airlinesDFCol: DataFrame = airlinesDF.select("Origin", "Dest", "Distance", "Year", "Month", "DayofMonth")

  airlinesDFCol.show(10)

  # airlinesDF.select("Origin", "Dest", "Distance", "to_date(concat(Year, Month, DayofMonth), 'yyyyMMdd') as FlightDate").show(10)

  # shows error: selet method accepts column strings, column object and not column expressions
  # expr(): convert an expression to a column object

  from pyspark.sql.functions import expr

  columnExprDF: DataFrame = airlinesDF.select("Origin", "Dest", "Distance", expr("to_date(concat(Year, Month, DayofMonth), 'yyyyMMdd') as FlightDate"))

  columnExprDF.show(10)
  ```

- column object expressions

  ```python
  # use column objects and build expression, avoid using strings and apply column objects and functions

  # airlinesDF.select("Origin", "Dest", "Distance", expr("to_date(concat(Year, Month, DayofMonth), 'yyyyMMdd') as flightDate")).show(10)
  from pyspark.sql.functions import to_date, concat

  airlinesDFColObjDF: DataFrame = airlinesDF.select("Origin", "Dest", "Distance", to_date(concat("Year", "Month", "DayofMonth"), 'yyyyMMdd').alias("FlightDate"))

  airlinesDFColObjDF.show(10)
  ```

> ### 3 docs links
>
> - [dataframe](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.html)
> - [column](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.html)
> - [built-in functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)

## Creating and Using UDFs

spark allows to create UDF and use them in these 2 types of expressions (_above_).

- string expressions or SQL expressions
- column object expressions

we already learnt to refer dataframe columns, applying some maths and using
built-in functions. However, spark also allows to create UDFs (User-Defined
Functions) and use them in the above 2 types of expressions.

```python
def parse_gender(gender: str) -> str:
    # Male, Female, Unknown
    female_pattern = r"^f$|f.m|w.m"  # f, f*m*, w*m*
    male_pattern = r"^m$|ma|m.l"  # m, ma*, m*l*

    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"
```

- **how to use this UDF?**
  - _create an expression and use it._

#### Column Object Expression

```python
gender_udf = udf(parse_gender, StringType())

logger.info(msg="Catalog Entry:")
[logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]  # noqa: E501

# survey_df2 = survey_df.withColumn("Gender", parse_gender("Gender"))
survey_df2 = survey_df.withColumn("Gender", gender_udf("Gender"))
```

- `withColumn()` transformation allows to transform a single column without impacting other columns in the dataframe. It takess _2 arguments_: `column_name`, `column_expression`
  - we can't simply use a function in a column object expression, we need to
    register our custom function to the driver and make it a UDF.
  - `udf()` to register the python function using the name of the local python function. Return type: optional, default is `StringType()`.
  - `udf()` will register it and return a reference to the registered UDF.

> this way is to register your function as a dataframe UDF. This will not
> register the UDF in the catalog. It will only create UDF and serialize the
> function to the executor.

> If you want to use the function in a dataframe column object expression, then
> you must register it as a UDF using `udf()` (_above example_).

- 3 step process to use a UDF:
  - create your function
  - register it as a UDF and get the reference
    > function is registered in spark session, driver will serialize and send
    > this function to the executors. Executors can run this function.
  - finally, use your function in expression.

#### String/SQL Expression

The registration process is different, we need to register it as a SQL function
and it should go to the catalog. That's done using `spark.udf.register()` _(2
arguments: `function_name`, `signature_of_function`)_.

> this way is to register as a SQL function. This will also create a entry
> in the catalog.

> If you want to use the function in a SQL expression, then you must register
> it like this.

```python
spark.udf.register("parse_gender_udf", parse_gender, StringType())

logger.info(msg="Catalog Entry:")
[logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

survey_df3 = survey_df.withColumn(
    "Gender",
    expr("parse_gender_udf(Gender)"),
)
```

> see [_code_](code/04-UDFs/ExampleUDF.py) for full code example with logs.

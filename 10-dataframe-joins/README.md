# Spark Joins in PySpark

- [Dataframe Joins and column name ambiguity](code/01-BasicJoins.ipynb)
- [Outer Joins in Dataframe](code/02-OuterJoins.ipynb)
- Internals of Spark Join and shuffle
- Optimizing your joins
- Implementing Bucket Joins

Joining is a fundamental operation used to combine two or more DataFrames based on related columns, similar to SQL joins. PySpark supports various join types like inner, outer, left, right, and cross joins.

Understanding join operations, including some internals, is crucial for writing efficient Spark applications and can help prevent performance bottlenecks or OutOfMemory (OOM) errors on the cluster.

## Core Concepts

For a join operation between two DataFrames:

- **`left` DataFrame**: The DataFrame you call the `.join()` method on (e.g., `left_df` in `left_df.join(...)`).
- **`right` DataFrame**: The DataFrame passed as the first argument to the `.join()` method (e.g., `right_df` in `left_df.join(right_df, ...)`).

### 1. Join Condition / Expression (`on` parameter)

This defines _how_ the rows from the two DataFrames should be matched. It's specified using the `on` parameter (or as the second positional argument) and can be provided in several ways:

- **String:** For an equi-join on a single column _with the same name_ in both DataFrames.
  ```python
  # Joins on 'user_id' column present in both DataFrames
  left_df.join(right_df, on="user_id", how="inner")
  ```
- **List of Strings:** For an equi-join on multiple columns _with the same names_ in both DataFrames.

  ```python
  # Joins where 'col1' and 'col2' match in both DataFrames
  left_df.join(right_df, on=["col1", "col2"], how="inner")
  ```

  - _Note:_ When using string or list of strings for the `on` condition, the resulting DataFrame will contain the join key columns _only once_.

- **Column Expression (Boolean):** The most flexible method. Allows for:

  - Joining on columns with _different names_.
  - Performing non-equi joins (e.g., using `>`, `<`, etc., although less common and potentially less performant).
  - Explicitly handling potential column name ambiguities.

  ```python
  # Join condition using Column objects
  join_expr = left_df.prod_id == right_df.product_code

  left_df.join(right_df, on=join_expr, how="left")
  ```

  - _Note:_ When using a `Column` expression like `left_df.colA == right_df.colB`, the resulting DataFrame typically includes _both_ `colA` and `colB`. If `colA` and `colB` had the same name (e.g., `left_df.id == right_df.id`), using this syntax prevents ambiguity but results in two 'id' columns. You might need to rename or drop one afterwards.

### 2. Join Type (`how` parameter)

This defines _which_ rows are included in the result, specified using the `how` parameter (or as the third positional argument).

- **`inner`** (default): Returns only rows where the join condition is met in _both_ DataFrames.
- **`outer`** (or `full`, `full_outer`): Returns all rows from _both_ DataFrames. Uses `null` for columns of the DataFrame where the join condition didn't find a match.
- **`left`** (or `left_outer`): Returns all rows from the `left` DataFrame and matching rows from the `right` DataFrame. Uses `null` for columns from the `right` DataFrame where there's no match.
- **`right`** (or `right_outer`): Returns all rows from the `right` DataFrame and matching rows from the `left` DataFrame. Uses `null` for columns from the `left` DataFrame where there's no match.
- **`cross`**: Returns the Cartesian product of both DataFrames (all possible combinations of rows). _Use with extreme caution_, as the result size can be massive (`rows_left * rows_right`). Requires `spark.sql.crossJoin.enabled=true` in some Spark versions or explicitly using the `.crossJoin()` method.
- **`left_semi`**: Returns only rows from the `left` DataFrame for which there is at least one match in the `right` DataFrame based on the join condition. It acts like a filter and only includes columns from the `left` DataFrame.
- **`left_anti`**: Returns only rows from the `left` DataFrame for which there is _no_ match in the `right` DataFrame based on the join condition. It also acts like a filter and only includes columns from the `left` DataFrame.

```python
join_type = "inner"  # or "outer", "left", "right", "cross", "left_semi", "left_anti"
```

### Basic Syntax

```python
# Using keyword arguments (recommended for clarity)
result_df = left_df.join(right_df, on=join_condition, how=join_type)

# Using positional arguments
result_df = left_df.join(right_df, join_condition, join_type)
```

### Internals & Performance Note

Spark optimizes joins using different physical **join strategies**:

- **Broadcast Hash Join (BHJ):** If one DataFrame is small enough (below `spark.sql.autoBroadcastJoinThreshold`, default 10MB), Spark can _broadcast_ it to all executors. The join then happens efficiently as a local hash lookup on each executor holding partitions of the larger DataFrame. This avoids expensive data shuffling across the network. This is often the most performant strategy when applicable.
- **Shuffle Sort Merge Join (SMJ):** If both DataFrames are large, Spark typically uses SMJ. This involves shuffling data from both DataFrames across the cluster so that rows with the same join keys end up on the same partition. Then, rows within each partition are sorted by the join key, and the join is performed by merging the sorted data. Shuffling is costly.
- Other strategies exist (e.g., Shuffle Hash Join, Broadcast Nested Loop Join).

Understanding these helps diagnose performance issues. If you have a join between a large and a small DataFrame, ensuring the small one is broadcast (by adjusting the threshold or using a `broadcast()` hint) can significantly improve performance and reduce the risk of OOM errors caused by large shuffles.

> see [code](code/01-BasicJoins/BasicJoins.ipynb) for examples and _handling column name ambiguity_.

## Outer Joins

A full outer join combines two DataFrames based on a join condition and includes **all rows from both** the left and the right DataFrame.

**How it Works:**

1.  **Matching Rows:** If rows from the left and right DataFrames match according to the join condition, the resulting row contains columns from both DataFrames.
2.  **Non-Matching Rows (Left):** If a row exists in the left DataFrame but has _no matching row_ in the right DataFrame based on the condition, it is still included in the result. The columns corresponding to the right DataFrame will be filled with `null` for this row.
3.  **Non-Matching Rows (Right):** If a row exists in the right DataFrame but has _no matching row_ in the left DataFrame based on the condition, it is also included in the result. The columns corresponding to the left DataFrame will be filled with `null` for this row.

**Use Case:**

Full outer joins are useful when you want a complete picture of all records from both datasets, regardless of whether they have matches in the other dataset. It helps identify records present in one dataset but missing in the other.

**PySpark Syntax:**

You use the `.join()` method on a DataFrame and specify the join type using the `how` parameter. The valid strings for a full outer join are:

- `"outer"`
- `"full"`
- `"full_outer"` (most explicit)

```python
from pyspark.sql import SparkSession

# Assume spark is an active SparkSession

# Sample DataFrames
data1 = [(1, "Alice", "HR"), (2, "Bob", "IT"), (3, "Charlie", "Sales")]
columns1 = ["id", "name", "dept_left"]
df1 = spark.createDataFrame(data1, columns1)

data2 = [(1, "New York"), (2, "London"), (4, "Tokyo")]
columns2 = ["id", "location"]
df2 = spark.createDataFrame(data2, columns2)

print("Left DataFrame (df1):")
df1.show()

print("Right DataFrame (df2):")
df2.show()

# --- Performing the Full Outer Join ---
# We join on the 'id' column.
outer_join_df = df1.join(
    df2,
    on="id",         # Join condition (equi-join on 'id' column)
    how="outer"      # Specify the join type as full outer
)

print("Result of Full Outer Join:")
outer_join_df.show()

# spark.stop()
```

**Explanation of the Example Output:**

- **Rows with id 1 and 2:** These IDs exist in both `df1` and `df2`, so the resulting rows contain columns from both (`id`, `name`, `dept_left`, `location`).
- **Row with id 3:** This ID exists only in `df1`. The result includes this row, but the columns from `df2` (`location`) are `null`.
- **Row with id 4:** This ID exists only in `df2`. The result includes this row, but the columns from `df1` (`name`, `dept_left`) are `null`.

**Key Points:**

- **`null` Values:** Be prepared to handle `null` values in the resulting DataFrame, as they indicate the absence of a match in one of the original DataFrames.
- **Join Condition:** The `on` parameter (or a Column expression) defines how rows are matched.
- **Column Ambiguity:** If the DataFrames share column names _other than_ the join keys used in an equi-join `on="col"` syntax, those columns might become ambiguous. Use techniques like renaming columns before the join or using DataFrame aliases if needed.

### Left Join, Right Join, and Full Outer Join

When we talk about "outer joins", we generally refer to join types that preserve rows from one or both tables/DataFrames even if there isn't a matching row in the other table/DataFrame based on the join condition. They fill in the columns from the non-matching side with `null` values.

There are three main types of outer joins in SQL and PySpark:

1.  **Left Outer Join (or Left Join)**

    - **Behavior:** Keeps **all rows** from the **left** DataFrame. It includes matching rows from the right DataFrame based on the join condition. If a row from the left DataFrame has no match in the right DataFrame, the columns corresponding to the right DataFrame will be filled with `null` values in the result.
    - **PySpark `how` parameter:** `"left"`, `"left_outer"`

2.  **Right Outer Join (or Right Join)**

    - **Behavior:** Keeps **all rows** from the **right** DataFrame. It includes matching rows from the left DataFrame based on the join condition. If a row from the right DataFrame has no match in the left DataFrame, the columns corresponding to the left DataFrame will be filled with `null` values in the result.
    - **PySpark `how` parameter:** `"right"`, `"right_outer"`

3.  **Full Outer Join (or Outer Join)**
    - **Behavior:** Keeps **all rows** from **both** the left and the right DataFrames.
      - If a row from the left DataFrame has no match in the right, the right-side columns are `null`.
      - If a row from the right DataFrame has no match in the left, the left-side columns are `null`.
      - If rows match, columns from both are included.
    - **PySpark `how` parameter:** `"outer"`, `"full"`, `"full_outer"`

In essence:

- **Left Outer:** _All of Left, Matches of Right._
- **Right Outer:** _All of Right, Matches of Left._
- **Full Outer:** _All of Left, All of Right._

> see [code](code/02-OuterJoins.ipynb) for examples.

## Internals of Spark Joins

Spark Joins are one of the most common causes for slowing down your application.

Spark implements 2 approaches to join DataFrames:

1.  **Shuffle Sort Merge Join (SMJ)**
2.  **Broadcast Hash Join (BHJ)**

Spark Joins are fundamental operations, but they can significantly impact application performance, especially when dealing with large datasets. Spark employs several join strategies, with two primary ones being:

### Shuffle Sort Merge Join (SMJ)

SMJ is often Spark's default join strategy when neither table is small enough to be broadcast efficiently (see Broadcast Hash Join). Its underlying principles are reminiscent of classic MapReduce patterns.

**The Challenge: Distributed Data**

Imagine two large DataFrames, `dfA` and `dfB`, that you want to join on a specific key column. In a Spark cluster:

- Each DataFrame is divided into multiple partitions.
- These partitions are distributed across different worker nodes (executors) in the cluster.
- A single executor might hold partitions from `dfA` and `dfB`, but the records needed to perform the join (i.e., records with the _same_ join key) might reside on entirely different partitions and, consequently, different executors.

**Direct Join Issue:** You cannot perform a complete join locally on any single executor because the matching records required for the join are likely scattered across the cluster. All records with the same join key value from _both_ DataFrames must be brought together onto the same executor to be joined.

**The SMJ Solution: A Multi-Stage Process**

SMJ tackles this distributed data challenge in primarily two major phases: Shuffle and Sort-Merge.

**Phase 1: Shuffle**

This phase reorganizes the data across the network so that records with the same join key from both DataFrames end up in the same partition on the same executor.

1.  **Map/Shuffle Write:**

    - Spark tasks read the input partitions of _both_ DataFrames (`dfA` and `dfB`).
    - For each record, they extract the join key.
    - Based on the join key (typically using a hash function), Spark determines which _output_ shuffle partition the record belongs to. The number of these output partitions is determined by the `spark.sql.shuffle.partitions` configuration (default is 200).
    - Each task writes its records to local shuffle files, partitioned according to the target shuffle partition determined by the key.

2.  **Shuffle Read/Fetch:**
    - Tasks for the _next_ stage (the Sort-Merge stage) are launched. Each task is responsible for processing one of the target shuffle partitions created in the previous step.
    - To do this, each task reads the corresponding shuffle blocks (the relevant portion of the shuffle files) from _all_ the executors that wrote data for that specific partition in the Shuffle Write step.
    - This process of transferring partitioned data blocks across the network from the writers (map side) to the readers (reduce side) is the **Shuffle Operation**.

**Why Shuffle is Expensive:** This data transfer across the cluster network is often the most time-consuming part of the SMJ join. It involves:

- **Network I/O:** Sending potentially large amounts of data between executors.
- **Disk I/O:** Writing and reading shuffle files to/from local disks.
- **Serialization/Deserialization:** Converting data to and from formats suitable for network transfer or disk storage.
  The performance is heavily dependent on network bandwidth, disk speed, data size, and potential data skew (where some keys have vastly more records than others, overloading specific partitions/tasks).

**Phase 2: Sort-Merge**

Once the shuffle phase is complete, each executor holds one or more shuffle partitions. Critically, each shuffle partition now contains all the records from _both_ original DataFrames that share the same range of join keys assigned to that partition.

1. **Sort:** Within each shuffle partition, Spark sorts the data from `dfA` and the data from `dfB` _independently_ based on the join key.
2. **Merge:** With both datasets now sorted by the join key within the partition, Spark can efficiently perform the join. It iterates through the two sorted datasets simultaneously (similar to the merge step in merge-sort), comparing keys. When matching keys are found, the corresponding records are combined (joined) to produce the output records. This merge process is computationally efficient because the data is pre-sorted.

The results from the merge step across all parallel tasks/partitions form the final joined DataFrame.

**Key Takeaways & Performance Considerations:**

- SMJ requires shuffling data across the network, which is often the primary bottleneck.
- The process involves partitioning data by join key (Shuffle Write), transferring data (Shuffle Read), sorting data within new partitions, and finally merging the sorted data.
- **Tuning SMJ often involves optimizing the Shuffle operation:**
  - Adjusting `spark.sql.shuffle.partitions` appropriately for the cluster size and data volume.
  - Addressing data skew (ensuring keys are distributed relatively evenly).
  - Ensuring adequate cluster resources (network bandwidth, memory, disk I/O).

SMJ is a robust join strategy for large datasets but understanding the shuffle cost is crucial for performance tuning in Spark.

<p align="center">
    <img src="https://github.com/user-attachments/assets/26e1463c-a022-40cb-a62b-07e4598c422e" width="75%">
</p>

- for the code (_link above_) (3 partition, 3 threads in local)
  - join operation accomplished in 3 stages, `stage2`, `stage3` for creating **Map Exchange** for 2 dataframes.
  - _lines from one stage to another are indicating shuffle_
  - data is moving from **Map Exchange** to **Reduce Exchange** (shuffle) and then to **Map Merge**.

<p align="center">
    <img src="https://github.com/user-attachments/assets/3be4db2c-d93f-4275-bef9-18f961a8c418" width="75%">
</p>

- stage 2, 3 were doing **shuffle write**
  - both stages in _3 parallel tasks, because both the dataframes had 3 partitions._
- stage 4 was doing **shuffle read**, that too in _3 parallel tasks_
  - because se had set shuffle partitions to 3, `spark.conf.set("spark.sql.shuffle.partitions", 3)`

# Optimizing Joins

Joining dataframes can bring 2 scenarios:

- Joining large dataframe with another large dataframe
- Joining large dataframe with a small dataframe
  - **small** dataframe (_dataframe can fit in memory of a single executor_)
  - **large** dataframe (_large enough to not fit in the memory of a single executor_)
    > if dataframe is more than a few GBs it should be considered large and should be broken down into smaller partitions to achieve parallel processing.

## Optimizing Large DataFrame Joins in Spark

When joining two large DataFrames in Apache Spark, the operation typically involves a **shuffle**, where data is redistributed across the cluster based on the join keys. This shuffle is often expensive in terms of network I/O, disk I/O, and CPU usage. Optimizing these joins is critical for performance.

- **Large DataFrame:** A DataFrame too large to fit into a single executor's memory. It must be processed in partitions distributed across the cluster. DataFrames larger than a few GBs generally fall into this category.

### Considerations for Joining Two Large DataFrames

Optimizing large-to-large joins primarily revolves around minimizing the amount of data shuffled and maximizing the efficiency and parallelism of the shuffle and join tasks. Key areas include:

1.  **Fundamental Pre-Join Optimizations (Reducing Data)**
2.  **Shuffle Partitions and Parallelism**
3.  **Key Distribution (Data Skew)**

#### 1. Fundamental Pre-Join Optimizations

Before even considering the join mechanism, reduce the size of the DataFrames involved as much as possible. This is often the most impactful optimization. The less data Spark needs to shuffle and process, the faster the join will be.

- **Filter Early and Often:** Remove rows that are not needed for the final result _before_ the join operation. This significantly cuts down the amount of data sent through the shuffle.
  - _Example:_ Consider joining a global sales table with US warehouse events.
  - `global_online_sales`: Contains sales from all countries.
    | order_id | city | product_code | store_code |
    | :------- | :------- | :----------- | :--------- |
    | s001 | New York | ABX-512 | A00007 |
    | s002 | London | ABC-123 | A00123 |
  - `us_warehouse_events`: Contains events only for US warehouses.
    | order_id | city | warehouse_code | event_type | event_date |
    | :------- | :------- | :------------- | :----------- | :--------- |
    | s001 | New York | WH-123 | Order Placed | 2023-01-01 |
  - If performing a `left join` from `us_warehouse_events` to `global_online_sales` on `order_id`, orders from non-US cities in `global_online_sales` (like London) will never match. It's much more efficient to filter `global_online_sales` to include _only_ US orders _before_ attempting the join. This requires knowing your data or potentially using intermediate lookups (e.g., a `country_city` mapping).
- **Project Early:** Select only the columns necessary for the join and subsequent operations. Carrying unnecessary columns through the shuffle adds overhead. Use `select()` or `drop()` to keep only essential data.
- **Aggregate Before Joining (If Possible):** If the final goal involves aggregation _after_ the join, check if some or all of that aggregation can happen _before_ the join. For instance, if joining sales data (`order_id`, `store_id`, `amount`) with store details (`store_id`, `region`) to get total sales per region, calculating total sales per `store_id` _first_ reduces the sales DataFrame size significantly before joining it with the store details.

**The objective is always to minimize DataFrame size as early as possible in your Spark job.** Smaller DataFrames lead to smaller shuffles and faster joins.

#### 2. Shuffle Partitions and Parallelism

When Spark performs a shuffle join, it hashes the join keys and redistributes rows so that all rows with the same key end up in the same partition, ready for joining. The degree of parallelism during this join phase depends on configuration and data characteristics.

**Key Factors:**

- **Number of Executors & Cores:** Determines the maximum number of tasks that can run concurrently.
- **`spark.sql.shuffle.partitions`:** This configuration parameter sets the number of partitions that are created _after_ a shuffle operation. Each partition will be processed by a task in the subsequent stage.
- **Number of Unique Join Keys:** The number of distinct values in the join key column(s).
- **Determining Parallelism:**
  - The maximum parallelism for the join processing stage is limited by `min(spark.sql.shuffle.partitions, total_available_executor_cores)`.
  - If `spark.sql.shuffle.partitions` is set to 400 and you have 500 available cores, the parallelism is limited to 400 because there are only 400 data partitions to process.
  - **Crucially, even with high partitions and cores, the effective parallelism can be limited by the number of unique join keys.** If you join `sales` (`order_id`, `product_id`, `units`) with `products` (`product_id`, `product_name`) on `product_id`, and there are only 200 unique products, Spark can conceptually only create 200 groups of data (one for each `product_id`) during the shuffle read phase. All data for a single `product_id` must be processed together by one task. In this scenario, even with 500 cores and `spark.sql.shuffle.partitions=400`, you might only achieve parallelism of up to 200 tasks actively performing the join logic simultaneously. Tasks processing partitions corresponding to non-existent keys (if partitions > unique keys) or tasks waiting for skewed keys (see next section) might sit idle or finish quickly.
- **Tuning:**
  - Set `spark.sql.shuffle.partitions` appropriately for your cluster size and data. A common starting point is 2-4 times the total number of executor cores. Too few partitions lead to underutilization; too many can cause scheduling overhead and potentially tiny, inefficient tasks.
  - If unique key cardinality is the bottleneck on a large cluster, increasing parallelism might require techniques like "salting" (discussed under Key Distribution) if feasible.

#### 3. Key Distribution (Data Skew)

An even distribution of data across join keys is ideal for shuffle joins. **Data skew** occurs when one or a few join key values are significantly more frequent than others.

- **Problem:** Consider joining `sales` and `products` on `product_id`. If you have a few "fast-moving" products with millions of sales transactions each, while most others are "slow-moving" with few transactions:

  - During the shuffle, all transactions for a single `product_id` are sent to the same partition/task.
  - The tasks responsible for the fast-moving products receive vastly more data than others.
  - These "straggler" tasks take much longer to complete, delaying the entire join operation, even if 99% of the other tasks finish quickly. The join isn't complete until _all_ tasks are done.

- **Detection:**

  - Monitor the Spark UI's "Stages" tab during or after the join.
  - Look at the task list for the relevant stage. Check metrics like "Duration," "Shuffle Read Size," or "Records Read." Significant outliers (tasks taking much longer or reading much more data) strongly suggest skew.

- **Mitigation Strategies:**
  1. **Filter Skewed Keys:** If the highly frequent keys represent bad data, nulls, or values irrelevant to the analysis, filter them out _before_ the join.
  2. **Salting:** Intentionally modify the join keys for skewed values to distribute their data across multiple partitions.
     - Identify the skewed keys (e.g., `product_123`).
     - In the larger DataFrame, append a random suffix (salt) to the skewed keys (e.g., `product_123_1`, `product_123_2`). Keep non-skewed keys as they are or add a default salt (`product_456_0`).
     - In the smaller of the two large DataFrames (or whichever is more feasible to modify), duplicate the rows for the skewed keys, adding the corresponding salt values (e.g., create multiple copies of the `product_123` record, one for each salt value used).
     - Join on the new salted key. This breaks the large partition for `product_123` into multiple smaller partitions, distributing the load.
     - _Requires careful implementation and increases the size of one DataFrame._
  3. **Adaptive Query Execution (AQE):** Spark 3.0+ includes AQE (`spark.sql.adaptive.enabled=true`). Its Skew Join optimization (`spark.sql.adaptive.skewJoin.enabled=true`) can automatically detect and handle skew during runtime by splitting oversized shuffle partitions/tasks into smaller ones. Check the Spark UI to see if AQE successfully applied this optimization. While powerful, it may not solve all skew scenarios.

Shuffle joins between large DataFrames can be problematic due to:

- **Huge Data Volumes:** Address by aggressive filtering, column projection, and pre-aggregation.
- **Parallelism Limits:** Tune `spark.sql.shuffle.partitions` relative to cluster resources, but be aware of limits imposed by unique key counts.
- **Uneven Shuffle Distribution (Key Skew):** Detect via Spark UI. Address by filtering skewed keys, applying salting techniques, or leveraging Spark's AQE skew join optimization.

Successfully optimizing these joins requires understanding your data's characteristics (size, key distribution) and applying these techniques iteratively while monitoring performance through the Spark UI.

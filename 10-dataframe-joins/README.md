# Spark Joins in PySpark

- [Dataframe Joins and column name ambiguity](code/01-BasicJoins/BasicJoins.ipynb)
- Outer Joins in Dataframe
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

# Aggregations in Apache Spark

3 broad categories:

- **Simple Aggregations**
- **Grouping Aggregations**
- **Windowing Aggregations**

> there are more aggregations mainly used my data scientists and data analysts.

> All aggregations in spark are implemented via in-built applications.

**Aggregate Functions**: are used to perform calculations on a set of values and return a single value.

- eg: `avg()`, `count()`, `sum()`, `min()`, `max()` etc.
- _used for simple and grouping aggregations._

**Window Functions**: are used to perform calculations across a set of rows that are related to the current row. They are typically used to perform calculations on a sliding window of data, such as calculating running totals or moving averages.

- eg: `lead()`, `lag()`, `rank()`, `dense_rank()`, `cume_dist()` etc.
- _used for windowing aggregations._

> see [_code_](code/01-Aggregate/Aggregate.ipynb)

## Exercise

- based on this [_dataset_](code/01-Aggregate/data/invoices.csv)

<p align="center">
    <img src="https://github.com/user-attachments/assets/7db0d2f0-dc88-4f3b-8e88-52a1fa7b9f21" width="75%">
</p>

- group by `Country` and `InvoiceNo`
- `InvoiceValue` is sum of [`Quantity` * `UnitPrice`]

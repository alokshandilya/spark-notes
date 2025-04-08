from lib.logger import Log4J
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, count, count_distinct, expr, round, sum

app_name: str = "Aggregate Example"

spark: SparkSession = (
    SparkSession.builder.master("local[3]")  # type: ignore
    .appName(app_name)
    .getOrCreate()
)

logger: Log4J = Log4J(spark)

logger.info(msg=f"Starting {app_name}!!")

invoice_df: DataFrame = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/invoices.csv")
)

# summarized whole dataframe and got 1 single row in the result.
# these are simple aggregations.

invoice_df.select(
    count("*").alias("Total Count"),
    sum("Quantity").alias("Total Quantity"),
    avg("UnitPrice").alias("Average Unit Price"),
    count_distinct("InvoiceNo").alias("Distinct Invoice Count"),
).show()

# +-----------+--------------+------------------+----------------------+
# |Total Count|Total Quantity|Average Unit Price|Distinct Invoice Count|
# +-----------+--------------+------------------+----------------------+
# |     541909|       5176450| 4.611113626088481|                 25900|
# +-----------+--------------+------------------+----------------------+

# these aggregates are functions, so we can use them in column object
# expression or SQL like string expression.
invoice_df.selectExpr(
    "COUNT(1) AS `Total Count`",  # count even null values
    "COUNT(StockCode) AS `Total Stock Count`",  # count non-null values
    "SUM(Quantity) AS `Total Quantity`",
    "AVG(UnitPrice) AS `Average Unit Price`",
).show()

# +-----------+-----------------+--------------+------------------+
# |Total Count|Total Stock Count|Total Quantity|Average Unit Price|
# +-----------+-----------------+--------------+------------------+
# |     541909|           541908|       5176450| 4.611113626086849|
# +-----------+-----------------+--------------+------------------+

# Exercise 1: See README.md and dataset code/data/invoices.csv
# SELECT
invoice_df.createOrReplaceTempView("invoices")

exercise1 = spark.sql(
    """
    SELECT
        Country,
        InvoiceNo,
        SUM(Quantity) AS TotalQuantity,
        ROUND(SUM(Quantity * UnitPrice), 2) AS InvoiceValue
    FROM
        invoices
    GROUP BY
        Country,
        InvoiceNo
    """
)

exercise1.show(5)

# +--------------+---------+-------------+------------+
# |       Country|InvoiceNo|TotalQuantity|InvoiceValue|
# +--------------+---------+-------------+------------+
# |United Kingdom|   536446|          329|      440.89|
# |United Kingdom|   536508|          216|      155.52|
# |United Kingdom|   537018|           -3|         0.0|
# |United Kingdom|   537401|          -24|         0.0|
# |United Kingdom|   537811|           74|      268.86|
# +--------------+---------+-------------+------------+

# OR

# agg(*exprs: Column) -> DataFrame
# Compute aggregates and returns the result as a `DataFrame`.

exercise1_alt = invoice_df.groupBy("Country", "InvoiceNo").agg(
    sum("Quantity").alias("TotalQuantity"),
    expr("ROUND(SUM(Quantity * UnitPrice), 2)").alias("InvoiceValue"),
    # OR
    round(sum(expr("Quantity * UnitPrice")), 2).alias("InvoiceValueExpr"),
)

exercise1_alt.show(5)

# +--------------+---------+-------------+------------+----------------+
# |       Country|InvoiceNo|TotalQuantity|InvoiceValue|InvoiceValueExpr|
# +--------------+---------+-------------+------------+----------------+
# |United Kingdom|   536446|          329|      440.89|          440.89|
# |United Kingdom|   536508|          216|      155.52|          155.52|
# |United Kingdom|   537018|           -3|         0.0|             0.0|
# |United Kingdom|   537401|          -24|         0.0|             0.0|
# |United Kingdom|   537811|           74|      268.86|          268.86|
# +--------------+---------+-------------+------------+----------------+

logger.info(msg=f"Stopping {app_name}!!")
spark.stop()

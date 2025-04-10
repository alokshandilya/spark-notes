from pyspark.sql import DataFrame, SparkSession

spark: SparkSession = (
    SparkSession.builder.master("local[3]")  # type: ignore
    .appName("BucketJoin")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.set("spark.sql.adaptive.enabled", "false")

df1: DataFrame = spark.read.format("json").load("data/d1/")
df2: DataFrame = spark.read.json("data/d2/")

# df1.show()
# df2.show()

""" this could be in a different spark application which ran before
spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
spark.sql("USE MY_DB")

df1.coalesce(1).write.bucketBy(3, "id").saveAsTable("MY_DB.flight_data1")
df2.coalesce(1).write.bucketBy(3, "id").saveAsTable("MY_DB.flight_data2")
"""

df3: DataFrame = spark.read.table("MY_DB.flight_data1")
df4: DataFrame = spark.read.table("MY_DB.flight_data2")

# as the table are small, spark might automatically broadcast join them
# for learning, we will disable it
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

join_expr = df3.id == df4.id
join_df: DataFrame = df3.join(df4, join_expr, "inner")

join_df.collect()

# http://localhost:4040/jobs/SQL/ (no shuffle/exchange, no broadcast)
# (just SortMergeJoin)
input("Press Enter to continue...")  # for Spark UI

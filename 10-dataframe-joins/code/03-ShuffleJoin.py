from pyspark.sql import DataFrame, SparkSession

# see data/df1, data/df2 (3 files each, to get 3 partitions)
spark: SparkSession = (
    # 3 parallel threads
    SparkSession.builder.master("local[3]")  # type: ignore
    .appName("ShuffleJoin")
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "3")
spark.conf.set("spark.sql.adaptive.enabled", "false")

flight_time_df1: DataFrame = spark.read.format("json").load("data/d1/")
flight_time_df2: DataFrame = spark.read.format("json").load("data/d2/")

# flight_time_df1.show()
# flight_time_df2.show()

join_expr = flight_time_df1.id == flight_time_df2.id
join_df = flight_time_df1.join(flight_time_df2, join_expr, "inner")

# join is a transformation, we need an action to trigger it like show()
# alfo foreach() is also an action.

# .foreach() is an action in PySpark Structured Streaming that enables you to
# define custom logic to write output of streaming query to external storage
# systems.

# join_df.foreach(lambda f: None)
join_df.collect()

# see spark UI @ http://localhost:4040/jobs/
# also, see http://localhost:4040/SQL/ for Exhange/Shuffle, SortMergeJoin
input("Press Enter to continue...")  # for Spark UI

from datetime import date
from unittest import TestCase

from DataFrameRows import to_date_df
from pyspark.sql import SparkSession
from pyspark.sql.types import Row, StringType, StructField, StructType


class RowDemoTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark: SparkSession = (
            SparkSession.builder.master("local[3]")  # type: ignore
            .appName("DataFrameRows_Test")
            .getOrCreate()
        )

        my_schema = StructType(
            [
                StructField("ID", StringType()),
                StructField("EventDate", StringType()),
            ]
        )

        my_rows = [
            Row("123", "04/05/2020"),
            Row("124", "4/5/2020"),
            Row("125", "04/5/2020"),
            Row("126", "4/05/2020"),
        ]

        my_rdd = cls.spark.sparkContext.parallelize(my_rows, 2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

    def test_data_type(self):
        # collect() will return list of row to driver
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertIsInstance(row["EventDate"], date)

    def test_date_value(self):
        rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
        for row in rows:
            self.assertEqual(row["EventDate"], date(2020, 4, 5))


# Output

"""pytest DataFrameRows_Test.py
======================================================================== test session starts ========================================================================
platform linux -- Python 3.13.2, pytest-8.3.5, pluggy-1.5.0
rootdir: /home/aloks/spark-notes/08-dataframe-dataset-transformations/code/01-DataFrameRows
collected 2 items

DataFrameRows_Test.py ..                                                                                                                                      [100%]

======================================================================== 2 passed in 11.98s =========================================================================
"""

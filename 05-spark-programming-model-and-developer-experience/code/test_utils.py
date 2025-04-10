from typing import Dict, List
from unittest import TestCase

from lib.utils import count_by_country, load_data_csv_df
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import Row


class TestUtils(TestCase):
    spark: SparkSession

    @classmethod
    def setUpClass(cls) -> None:
        """
        hook method for setting up class fixture before running tests in the
        class
        """
        cls.spark = (
            SparkSession.builder.master("local[3]")  # type: ignore
            .appName("HelloSparkTest")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        """
        hook method for deconstructing the class fixture after running all
        tests in the class
        """
        cls.spark.stop()

    def test_datafile_loading(self) -> None:
        sample_df: DataFrame = load_data_csv_df(self.spark, "data/survey.csv")
        result_count: int = sample_df.count()
        self.assertEqual(result_count, 16, "Record count should be 16")

    def test_country_count(self) -> None:
        sample_df: DataFrame = load_data_csv_df(self.spark, "data/survey.csv")
        count_list: List[Row] = count_by_country(sample_df).collect()

        count_dict: Dict[str, int] = dict()
        for row in count_list:
            count_dict[row["country"]] = row["count"]

        self.assertEqual(count_dict["UK"], 2, "UK count should be 2")
        self.assertEqual(count_dict["Australia"], 1, "Australia count should be 1")
        self.assertEqual(count_dict["India"], 4, "India count should be 4")
        self.assertEqual(count_dict["Germany"], 1, "Germany count should be 4")
        self.assertEqual(count_dict["Canada"], 1, "Canada count should be 4")
        self.assertEqual(count_dict["Japan"], 1, "Japan count should be 4")

from unittest import TestCase

from lib.utils import count_by_country, load_data_csv_df
from pyspark.sql import SparkSession


class TestUtils(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = (
            SparkSession.builder.master("local[3]")
            .appName("HelloSparkTest")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_datafile_loading(self):
        sample_df = load_data_csv_df(self.spark, "data/survey.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 16, "Record count should be 16")

    def test_country_count(self):
        sample_df = load_data_csv_df(self.spark, "data/survey.csv")
        count_list = count_by_country(sample_df).collect()

        count_dict = dict()
        for row in count_list:
            count_dict[row["country"]] = row["count"]

        self.assertEqual(count_dict["UK"], 2, "UK count should be 2")
        self.assertEqual(count_dict["Australia"], 1, "Australia count should be 1")
        self.assertEqual(count_dict["India"], 4, "India count should be 4")
        self.assertEqual(count_dict["Germany"], 1, "Germany count should be 4")
        self.assertEqual(count_dict["Canada"], 1, "Canada count should be 4")
        self.assertEqual(count_dict["Japan"], 1, "Japan count should be 4")

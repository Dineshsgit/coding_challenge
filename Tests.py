import unittest
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import Row

from Main import get_most_common_trees,get_cherry_plum_count,get_banyan_fig_count,get_street_with_more_trees


class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        return SparkSession.builder.master('local[*]') \
        .appName('my - local - testing - pyspark - context') \
        .getOrCreate()

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class SimpleTest(PySparkTest):

   # Mocking the data in such a way that only required columns(i.e treeName) are passed in DF to Function
    def test_get_most_common_trees(self):
        test_list = ["Banyan", "Fig", "Cherry Plum", "Banyan", "Banyan"]
        test_df = self.spark.createDataFrame(list(map(lambda x: Row(tree_name=x), test_list)))
        assert(get_most_common_trees(test_df).take(1)[0]['tree_name'] == 'Banyan')

   # Mocking the data in such a way that only required columns(i.e treeName and legal status) are passed in DF to Function
    def test_get_cherry_plum_count(self):
        test_list = [["Cherry Plum", "DPW Maintained"],["Cherry Plum", "Not Maintained"],["Cherry Plum", "DPW Maintained"],
                     ["green Plum", "DPW Maintained"]]
        schema = StructType([StructField("tree_name", StringType()), StructField("legal_status", StringType())])
        test_df = self.spark.createDataFrame(test_list, schema)
        assert(get_cherry_plum_count(test_df) == 2)

    # Mocking only tree_name and permit notes columns to be able to make the banyan method pass
    def test_get_banyan_fig_count(self):
        test_list = [["Banyan Fig", "Permit Number 1234"], ["Cherry Plum", "Not Maintained"],
                     ["Cherry Plum", "DPW Maintained"], ["Banyan Fig", "No Permit"],
                     ["Banyan Fig", "Permit Number 6789"]]
        schema = StructType([StructField("tree_name", StringType()), StructField("permit_notes", StringType())])
        test_df = self.spark.createDataFrame(test_list, schema)
        assert(get_banyan_fig_count(test_df) == 2)

    # Mocking only street_name column
    def test_get_street_with_more_trees(self):
        test_list = ["Sunset", "San", "Mission", "Sunset", "21"]
        test_df = self.spark.createDataFrame(list(map(lambda x: Row(street_name=x), test_list)))
        assert(get_street_with_more_trees(test_df).take(1)[0]['street_name'] == 'Sunset')



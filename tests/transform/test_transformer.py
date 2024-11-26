import unittest
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

from schema.schemas import Schema
from transform.transformer import ArticleTransformer


class TransformerTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        conf = SparkConf()
        conf.setAppName('test transformer')
        conf.setMaster('local[*]')
        conf.set('spark.sql.session.timezone', 'BST')
        conf.set('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.18.0,org.postgresql:postgresql:42.7.4')
        cls.spark = SparkSession.builder.config(conf=conf).getOrCreate()

        test_file = "tests/fixtures/test.xml"


        cls.transformer = ArticleTransformer(
            test_file, "MedlineCitation", Schema.medline_citation(),
            cls.spark
        )

    @classmethod
    def tearDownClass(cls) -> None:
        # clean up
        cls.spark.stop()

    def test_transform(self):
        """
        Using fixture data to test transformation output as expected. Things to look out:
            - column value
            - check_dtype
            - check_column_names
            - check_columns_in_order

        inputs:
         - test xml:
        fixtures/test.xml

        expected:
            +---------+--------------+------------------+
            |JournalTitle|JournalISSN|Abstract|JournalPublicationDate|ArticleTitle|AuthorList|DateRevised|Subject|
            +---------+--------------+------------------+
            |Biochemical and biophysical research communications|0006X|NULL|17 Nov 1975|Isolation and purification of cytokininchromatography.|K Yoshida K, T Takegami T|2023-12-13 |Nicotiana, Hydrogen-Ion Concentration, Cytokinins|
            +---------+--------------+------------------+

        """
        expected = [
            ("Biochemical and biophysical research communications", "0006X", "abc", "17 Nov 1975", "Isolation and purification of cytokininchromatography.", "K Yoshida K, T Takegami T", "2023-12-13", "Nicotiana, Hydrogen-Ion Concentration, Cytokinins"),
        ]
        df_expected = self.spark.createDataFrame(data=expected, schema=["JournalTitle", "JournalISSN", "Abstract", "JournalPublicationDate", "ArticleTitle", "AuthorList", "DateRevised", "Subject"])
        df_actual = self.transformer.transform()
        df_actual.show(truncate=False)
        assert_pyspark_df_equal(df_actual, df_expected,
                                check_dtype=True,
                                check_column_names=True,
                                check_columns_in_order=True
                                )


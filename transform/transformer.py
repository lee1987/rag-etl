from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, concat_ws, lit, expr
from transform.custom_udf import CustomUDF


class ArticleTransformer(object):
    def __init__(self,
                 xml_file: str, rowTag: str, xml_file_schema: StructType,
                 spark: SparkContext):
        self.__xml_file = xml_file
        self.__rowTag = rowTag
        self.__xml_file_schema = xml_file_schema
        self.__spark = spark

    def transform(self) -> DataFrame:
        df = self._load(self.__xml_file, self.__rowTag, self.__xml_file_schema)

        df = (df.select(
            col('Article.Journal.Title').alias('JournalTitle'),
            col('Article.Journal.ISSN').alias('JournalISSN'),
            col('Article.Abstract.AbstractText').alias('Abstract'),
            concat_ws(' ', col('Article.Journal.JournalIssue.PubDate.Day'),
                      col('Article.Journal.JournalIssue.PubDate.Month'),
                      col('Article.Journal.JournalIssue.PubDate.Year')).alias('JournalPublicationDate'),
            col('Article.ArticleTitle').alias('ArticleTitle'),
            col('Article.AuthorList').alias('AuthorList'),
            concat_ws('-', col('DateRevised.Year'), col('DateRevised.Month'), col('DateRevised.Day')).alias(
                'DateRevised'),
            col('MeshHeadingList').alias('Subject'),
        ).withColumn('Subject', CustomUDF.array_flatten_udf(col('Subject'), lit('DescriptorName')))
              .withColumn('AuthorList', CustomUDF.array_flatten_fullname_udf(col('AuthorList'))))
        return df

    def addUUID(self, df: DataFrame) -> DataFrame:
        return df.withColumn("UUID", expr("uuid()"))

    def _load(self, path: str, rowTag: str, schema: StructType) -> DataFrame:
        df = (self.__spark.read.option('encoding', 'UTF-8')
              .format("xml")
              .options(rowTag=rowTag)
              .schema(schema)
              .load(path))
        return df

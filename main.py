from pyspark.sql import SparkSession, DataFrame
from transform.transformer import ArticleTransformer
from schema.schemas import Schema


def main():
    spark = SparkSession.builder.appName("interview").getOrCreate()
    transformer = ArticleTransformer("data/", "MedlineCitation", Schema.medline_citation(), spark)
    df = transformer.transform()
    # Add primary key by adding UUID
    df = transformer.addUUID(df)
    # print to stdout
    #write(df, "csv")
    db_write(df)

def write(df: DataFrame, saveTo: str):
    df.coalesce(1).write.mode("overwrite").option("header", "true").format("csv").save(saveTo)

def db_write(df: DataFrame):
    # re-write the existing table @todo: CHANGE THIS
    df.write.format("jdbc") \
      .option("driver", "org.postgresql.Driver") \
      .option("url", "jdbc:postgresql://localhost:5432/interview") \
      .option("dbtable", "articles") \
      .option("user", "postgres") \
      .option("password", "pass1") \
      .mode("overwrite") \
      .save()

if __name__ == "__main__":
    main()

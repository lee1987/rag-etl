from pyspark.sql.types import *


class Schema(object):
    @staticmethod
    def medline_citation():
        return (
            StructType([
                StructField("Article", StructType([
                    StructField("ArticleTitle", StringType()),
                    StructField("Abstract", StructType([
                        StructField("AbstractText", StringType()),
                    ])),
                    StructField("Journal", StructType([
                        StructField("ISSN", StringType()),
                        StructField("Title", StringType()),
                        StructField("JournalIssue", StructType([
                            StructField("PubDate", StructType([
                                StructField("Year", StringType()),
                                StructField("Month", StringType()),
                                StructField("Day", StringType()),
                            ])),
                        ]))
                    ])),
                    StructField("AuthorList", ArrayType(StructType([
                        StructField("Author", ArrayType(StructType([
                            StructField("LastName", StringType()),
                            StructField("ForeName", StringType()),
                            StructField("Initials", StringType())
                        ]))),
                    ]))),
                ])),
                StructField("DateRevised", StructType([
                    StructField("Year", IntegerType()),
                    StructField("Month", IntegerType()),
                    StructField("Day", IntegerType())
                ])),
                StructField("MeshHeadingList", ArrayType(StructType([
                    StructField("MeshHeading", ArrayType(StructType([
                        StructField("DescriptorName", StringType()),
                    ]))),
                ])))

            ])
        )

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


class CustomUDF(object):
    @staticmethod
    @udf(returnType=StringType())
    def array_flatten_fullname_udf(arr):
        arr_subject = set()
        if not arr:
            return '';

        for a in arr[0]:
            for b in a:
                arr_fields = b.asDict()
                name_output = "{0} {1} {2}"
                arr_subject.add(
                    name_output.format(arr_fields["Initials"], arr_fields["LastName"], arr_fields["ForeName"])
                )
        return ", ".join(arr_subject)

    @staticmethod
    @udf (returnType=StringType())
    def array_flatten_udf(arr, key):
        arr_subject = set()
        if not arr:
            return '';

        for a in arr[0]:
            for b in a:
                arr_subject.add(b.asDict()[key])
        return ", ".join(list(arr_subject))
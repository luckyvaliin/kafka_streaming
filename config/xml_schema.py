from pyspark.sql.types import StructType, StructField, StringType

# Define the schema for your XML data
xml_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("details", StructType([
        StructField("age", StringType(), True),
        StructField("city", StringType(), True)
    ]), True)
])
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.types import StructType, StructField, StringType

# Define the schema for the XML data
xml_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("details", StructType([
        StructField("age", StringType(), True),
        StructField("city", StringType(), True)
    ]), True)
])

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("StructuredKafkaXMLPublisher") \
        .getOrCreate()

    # Read XML data from a source
    xml_data = spark.read \
        .format("xml") \
        .option("rowTag", "person") \
        .schema(xml_schema) \
        .load("path/to/xml")

    # Perform any required processing on the XML data
    processed_data = xml_data.select("id", "name", "details.age", "details.city")

    # Write the processed data to Kafka
    kafka_output_topic = "topic_person_details"
    kafka_output_bootstrap_servers = "localhost"

    query = processed_data \
        .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_output_bootstrap_servers) \
        .option("topic", kafka_output_topic) \
        .outputMode("append") \
        .option("checkpointLocation", "path/to/checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()

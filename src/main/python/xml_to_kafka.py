from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from config.kafka_params import kafka_topic, kafka_bootstrap_servers
from config.xml_schema import xml_schema

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("XMLToKafkaPublisher") \
        .getOrCreate()

    # Read XML data from a source
    xml_data = spark.read \
        .format("xml") \
        .option("rowTag", "person") \
        .schema(xml_schema) \
        .load("path/to/xml")

    # Perform any required processing on the XML data
    processed_data = xml_data.select("id", "name", "details.age", "details.city")

    query = processed_data \
        .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", kafka_topic) \
        .outputMode("append") \
        .option("checkpointLocation", "path/to/checkpoint") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()

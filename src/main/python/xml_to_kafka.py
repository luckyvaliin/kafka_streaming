from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from config.kafka_params import kafka_topic, kafka_bootstrap_servers
from config.xml_schema import xml_schema
import logging

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("XMLToKafkaPublisher") \
        .getOrCreate()

    # Initialize the logger
    logger = logging.getLogger(__name__)

    # Configure the logger
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    try:
        # Read XML data from a source
        xml_data = spark.read \
            .format("xml") \
            .option("rowTag", "person") \
            .schema(xml_schema) \
            .load("path/to/xml")

        # Perform any required processing on the XML data
        processed_data = xml_data.select("id", "name", "details.county", "details.city")

        query = processed_data \
            .selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value") \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("topic", kafka_topic) \
            .outputMode("append") \
            .start()

        query.awaitTermination()

    except Exception as e:
        # Log any exceptions
        logger.error(f"Error in PySpark job: {e}", exc_info=True)

    finally:
        # Stop the Spark session
        spark.stop()


if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
from config.kafka_params import kafka_topic, kafka_bootstrap_servers
from config.xml_schema import xml_schema
from dynamo_chkpointing import *
import logging

try:
    # Initialize the logger
    logger = logging.getLogger(__name__)

    # Configure the logger
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create a Spark session
    spark = (SparkSession.builder.appName("KafkaToHDFSConsumer")
             .config("enable.auto.commit", "False")
             .getOrCreate())

    # Read XML data from Kafka
    df = spark.readStream.format("kafka") \
        .option("subscribe", kafka_topic) \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("startingOffsets", "earliest") \
        .load()
    df.persist()

    # Get the offset information as a concatenated string
    offsetKey = df.take(1).selectExpr("concat(topic, '|', partition) as key")
    checkpoint_to_start = get_latest_checkpoint(offsetKey)

    if checkpoint_to_start != 0:
        df = spark.readStream.format("kafka") \
            .option("subscribe", kafka_topic) \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("startingOffsets", offsetKey+checkpoint_to_start) \
            .load()

    df.persist()

    # Deserialize the XML data using the specified schema
    df = (df.selectExpr("CAST(value AS STRING)")
          .selectExpr("from_xml(value, '{}') as data".format(xml_schema.json())))

    # Explode the data field to get individual columns
    df = df.selectExpr("data.id", "data.name", "data.details.county", "data.details.city")

    # Define the HDFS output location
    output_path = "hdfs://destination_hdfs_path"

    # Write the streaming data to HDFS
    query = df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_path) \
        .start()

    processed_offset = df.selectExpr(max("offset"))
    save_checkpoint(offsetKey, processed_offset)

    # Wait for the streaming query to terminate
    query.awaitTermination()

except Exception as e:
    # Log any exceptions
    logger.error(f"Error in PySpark job: {e}", exc_info=True)

finally:
    # Stop the Spark session
    spark.stop()
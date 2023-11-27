from pyspark.sql import SparkSession
from config.kafka_params import kafka_topic, kafka_bootstrap_servers
from config.xml_schema import xml_schema

# Create a Spark session
spark = SparkSession.builder.appName("KafkaToHDFSConsumer").getOrCreate()

# Read XML data from Kafka
df = spark.readStream.format("kafka") \
    .option("subscribe", kafka_topic) \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .load()

# Deserialize the XML data using the specified schema
df = (df.selectExpr("CAST(value AS STRING)")
      .selectExpr("from_xml(value, '{}') as data".format(xml_schema.json())))

# Explode the data field to get individual columns
df = df.selectExpr("data.id", "data.name", "data.details.age", "data.details.city")

# Define the HDFS output location
output_path = "hdfs://destination_hdfs_path"

# Write the streaming data to HDFS
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", "/path/to/checkpoint") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()

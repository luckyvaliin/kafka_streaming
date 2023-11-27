from pyspark.sql import SparkSession
import logging

# Create a Spark session
spark = SparkSession.builder \
    .appName("HDFSToProcessedZone") \
    .getOrCreate()

# Initialize the logger
logger = logging.getLogger(__name__)

# Configure the logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Raw data location
raw_data_location = "hdfs://some-path/raw-data-path"

# Processed data location
processed_data_location = "s3://processed-bucket/processed-data-path"

try:
    # Read Parquet data from the raw location
    raw_data = spark.read.parquet(raw_data_location)

    # Perform your data processing/transformation here if needed
    # For example, you can add a new column, filter data, aggregate, etc.
    processed_data = raw_data.select("id", "name").filter("details.county = 'Forsyth'")

    # Write the processed data to the S3 processed zone
    processed_data.write \
        .partitionBy("dob") \
        .mode("append") \
        .parquet(processed_data_location)

except Exception as e:
    # Log any exceptions
    logger.error(f"Error in PySpark job: {e}", exc_info=True)

finally:
    # Stop the Spark session
    spark.stop()


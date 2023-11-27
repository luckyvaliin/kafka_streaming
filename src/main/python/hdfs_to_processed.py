from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("HDFSToProcessedZone") \
    .getOrCreate()

# Raw data location
raw_data_location = "hdfs://some-path/raw-data-path"

# Processed data location
processed_data_location = "s3://processed-bucket/processed-data-path"

# Read Parquet data from the raw location
raw_data = spark.read.parquet(raw_data_location)

# Perform your data processing/transformation here if needed
# For example, you can add a new column, filter data, aggregate, etc.
processed_data = raw_data.select("id", "name").filter("details.age > 18")

# Write the processed data to the S3 processed zone
processed_data.write \
    .mode("overwrite") \
    .parquet(processed_data_location)

# Stop the Spark session
spark.stop()

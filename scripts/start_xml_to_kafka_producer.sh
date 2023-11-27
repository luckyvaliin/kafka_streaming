#!/bin/bash

# Specify the location of the Spark installation
# Optional if its a cluster
SPARK_HOME="/path/to/spark"

# Specify the Python script you want to run
PY_SCRIPT="../some_path/kafka_streaming/src/main/python/xml_to_kafka.py"

# Set the Spark master URL
SPARK_MASTER="yarn"

# Additional Spark configuration options if needed
# For example, to configure memory settings:
SPARK_CONFIG="--conf spark.executor.memory=2g --conf spark.driver.memory=1g"

# Run the PySpark application
$SPARK_HOME/bin/spark-submit \
  --master $SPARK_MASTER \
  $SPARK_CONFIG \
  $PY_SCRIPT

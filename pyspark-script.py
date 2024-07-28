from pyspark.sql import SparkSession
from datetime import datetime
import logging

# Initialize Spark session
spark = SparkSession.builder.appName("HelloWorldApp").getOrCreate()

# Get Spark logger
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)

# Log Hello, World!
logger.info("Hello, World!")

# Get the current date and time
current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Log the current date and time
logger.info(f"Current Date and Time: {current_date}")

# Stop the Spark session
spark.stop()

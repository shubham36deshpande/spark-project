from pyspark.sql import SparkSession
import logging

# Initialize Spark session
spark = SparkSession.builder.appName("ReadCSVSaveToS3").getOrCreate()

# Get Spark logger
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)

# Define S3 paths
input_path = "s3://my-airflow-sample-bucke/input/movie_details.csv"
output_path = "s3://my-airflow-sample-bucke/output/movie_details.csv"

# Log the input and output paths
logger.info(f"Reading CSV from: {input_path}")
logger.info(f"Saving output to: {output_path}")

# Read the CSV file from S3
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Log the schema of the DataFrame
logger.info("Schema of the DataFrame:")
df.printSchema()

# Save the DataFrame to another S3 bucket in CSV format
df.write.csv(output_path, mode="overwrite", header=True)

# Log completion
logger.info("CSV file has been successfully saved to the output path.")

# Stop the Spark session
spark.stop() 

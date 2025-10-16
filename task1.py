from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Define the output directory for the CSV files
OUTPUT_DIR = "/workspaces/ITCS6190-L8-SparkSQL/ride-sharing-analytics/outputs/task_1"

# Read streaming data from socket
def read_streaming_data_from_socket(spark_session, host="localhost", port=9999, data_schema=schema, output_directory=OUTPUT_DIR):
    """
    Reads streaming data from a TCP socket, parses the JSON data,
    and writes it incrementally to CSV files.
    """
    print(f"Reading streaming data from socket {host}:{port}...")

    # 1. Read the raw data stream from the socket
    # The 'value' column contains the raw string
    raw_stream_df = spark_session.readStream \
        .format("socket") \
        .option("host", host) \
        .option("port", port) \
        .load()

    # 2. Parse JSON data into columns using the defined schema
    # Use from_json() and then flatten the result with select("data.*")
    parsed_stream_df = raw_stream_df.select(
        from_json(col("value"), data_schema).alias("data")
    ).select("data.*")
    
    # 3. Print parsed data to CSV files
    csv_query = parsed_stream_df.writeStream \
        .outputMode("append") \
        .format("csv") \
        .option("header", "true") \
        .option("path", output_directory) \
        .option("checkpointLocation", os.path.join(output_directory, "checkpoint")) \
        .trigger(processingTime='5 seconds') \
        .start()

    print(f"Spark Structured Streaming query started. Writing CSV files to '{output_directory}'...")
    print("Press Ctrl+C to stop.")
    
    return csv_query

# ----------------------------------------------------------------------
# Main execution block to demonstrate the usage
if __name__ == "__main__":
    # Ensure the output directory is clean for a new run
    if os.path.exists(OUTPUT_DIR):
        print(f"Removing old output directory: {OUTPUT_DIR}")
        import shutil
        try:
            shutil.rmtree(OUTPUT_DIR)
        except OSError as e:
            print(f"Error removing directory {e}")

    # 2. Start the Spark reader and CSV writer
    # This connects to the server and starts processing
    streaming_query = read_streaming_data_from_socket(spark)

    # 3. Wait for the termination of the query (e.g., manually stopping it with Ctrl+C)
    try:
        streaming_query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming query...")
        streaming_query.stop()
        spark.stop()
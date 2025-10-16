from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import to_timestamp, col
import os

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics_Task3").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(),True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
raw_stream_df = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()
print("\nWaiting for data on localhost:9999...")

# Parse JSON data into columns using the defined schema
parsed_struct_df = raw_stream_df.select(
    from_json(col("value"), schema).alias("json_data")
)
parsed_stream_df = parsed_struct_df.select("json_data.*")

# Convert timestamp column to TimestampType and add a watermark
converted_df = parsed_stream_df.withColumn("timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
converted_df = converted_df.withWatermark("timestamp", "10 minutes")

# Perform windowed aggregation: sum of fare_amount over a 5-minute window sliding by 1 minute
windowed_agg_df = converted_df \
    .groupBy(
        # Group by the 5-minute window, sliding every 1 minute
        window(col("timestamp"), "5 minutes", "1 minute"), \
    col("driver_id")) \
    .agg(
        sum(col("fare_amount")).alias("window_total_fare")
    )

# Extract window start and end times as separate columns
final_output_df = windowed_agg_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("driver_id"),
    col("window_total_fare")
)

# Define a function to write each batch to a CSV file with column names
def save_to_csv(df, batch_id):
    if not df.isEmpty():
        base_dir = "/workspaces/ITCS6190-L8-SparkSQL/ride-sharing-analytics/outputs/task_3"
        # Output path is a unique directory for this batch
        output_path = f"{base_dir}/csv_batch_{batch_id}"
        
        # Ensure base directory exists
        os.makedirs(base_dir, exist_ok=True)
        
        # Save the batch DataFrame as a CSV file with headers included
        df.write \
            .format("csv") \
            .mode("overwrite") \
            .option("header", "true") \
            .save(output_path)
        
        print(f"INFO: Successfully wrote batch {batch_id} to directory: {output_path}")

# Use foreachBatch to apply the function to each micro-batch
query = final_output_df.writeStream \
    .foreachBatch(save_to_csv) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/task3_windowed") \
    .start()

query.awaitTermination()
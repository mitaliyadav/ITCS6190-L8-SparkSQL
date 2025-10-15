from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import to_timestamp, col

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics_Task2").getOrCreate()

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

# Compute aggregations: total fare and average distance grouped by driver_id
query_df = converted_df \
    .groupBy(col("driver_id")) \
    .agg(
        sum(col("fare_amount")).alias("total_fare"),
        avg(col("distance_km")).alias("avg_distance")
    )

# Define a function to write each batch to a CSV file
def save_to_csv(df, batch_id):
    # Save the batch DataFrame as a CSV file with the batch ID in the filename
    if not df.isEmpty():
        # Add a timestamp to the filename for uniqueness
        output_path = f"/workspaces/ITCS6190-L8-SparkSQL/ride-sharing-analytics/outputs/task_2"
        df.coalesce(1).write \
            .format("csv") \
            .mode("append") \
            .option("header", "true") \
            .save(output_path)

# Use foreachBatch to apply the function to each micro-batch
query = query_df.writeStream \
    .foreachBatch(save_to_csv) \
    .outputMode("complete") \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", "/tmp/checkpoints/task2_agg") \
    .start()

query.awaitTermination()

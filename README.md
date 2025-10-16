# Ride Sharing Analytics Using Spark Streaming and Spark SQL.
---
## **Prerequisites**
Before starting the assignment, ensure you have the following software installed and properly configured on your machine:
1. **Python 3.x**:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. **PySpark**:
   - Install using `pip`:
     ```bash
     pip install pyspark
     ```

3. **Faker**:
   - Install using `pip`:
     ```bash
     pip install faker
     ```

---

## **Setup Instructions**

### **1. Project Structure**

Ensure your project directory follows the structure below:

```
ride-sharing-analytics/
├── outputs/
│   ├── task_1
│   |    └── CSV files of task 1.
|   ├── task_2
│   |    └── CSV files of task 2.
|   └── task_3
│       └── CSV files of task 3.
├── task1.py
├── task2.py
├── task3.py
├── data_generator.py
└── README.md
```

- **data_generator.py/**: generates a constant stream of input data of the schema (trip_id, driver_id, distance_km, fare_amount, timestamp)  
- **outputs/**: CSV files of processed data of each task stored in respective folders.
- **README.md**: Assignment instructions and guidelines.
  
---

### **2. Running the Analysis Tasks**

You can run the analysis tasks either locally.

1. **Execute Each Task **: The data_generator.py should be continuosly running on a terminal. open a new terminal to execute each of the tasks.
   ```bash
     python data_generator.py
     python task1.py
     python task2.py
     python task3.py
   ```

2. **Verify the Outputs**:
   Check the `outputs/` directory for the resulting files:
   ```bash
   ls outputs/
   ```

---

## **Overview**

In this assignment, we will build a real-time analytics pipeline for a ride-sharing platform using Apache Spark Structured Streaming. we will process streaming data, perform real-time aggregations, and analyze trends over time.

## **Objectives**

By the end of this assignment, you should be able to:

1. Task 1: Ingest and parse real-time ride data.
2. Task 2: Perform real-time aggregations on driver earnings and trip distances.
3. Task 3: Analyze trends over time using a sliding time window.

---

## **Task 1: Basic Streaming Ingestion and Parsing**

1. Ingest streaming data from the provided socket (e.g., localhost:9999) using Spark Structured Streaming.
2. Parse the incoming JSON messages into a Spark DataFrame with proper columns (trip_id, driver_id, distance_km, fare_amount, timestamp).

## **Logic Flow:**
The task1.py script is the foundational data pipeline. It reads raw JSON data strings directly from the streaming source (TCP socket), parses them into a structured DataFrame using a predefined schema, and then writes the records to a target directory in CSV format. It operates in append output mode and uses a 5-second processing time trigger.

1. Initialize Spark Session: Creates a Spark session named RideSharingAnalytics.
2. Define Schema: Defines the StructType schema for the incoming ride-sharing JSON records (trip ID, driver ID, distance, fare, timestamp).
3. Read Stream: Connects to the TCP socket (localhost:9999) using spark.readStream.
4. Parse Data: Uses from_json to parse the raw string value column into structured columns based on the schema.
5. Start Query: Initiates the streaming query using writeStream with:
6. outputMode("append"): Writes only new records to the sink.
7. format("csv"): Specifies the output format.
8. trigger(processingTime='5 seconds'): Processes a batch of data every 5 seconds.

## *Sample Output*
<img width="609" height="152" alt="image" src="https://github.com/user-attachments/assets/16df3fde-7e53-4d46-ba52-3d7294eb3b74" />

---

## **Task 2: Real-Time Aggregations (Driver-Level)**

1. Aggregate the data in real time to answer the following questions:
  • Total fare amount grouped by driver_id.
  • Average distance (distance_km) grouped by driver_id.
2. Output these aggregations to the console in real time.

## **Logic Flow:**

1. Setup & Parsing: Steps 1-4 are similar to Task 1 (initialization, schema definition, reading, and parsing).
2. Timestamp Conversion & Watermarking: Converts the timestamp string to TimestampType and applies a 10-minute watermark to manage event-time skew.
3. Aggregation: Groups the data by driver_id and calculates:
     sum(fare_amount) as total_fare.
     avg(distance_km) as avg_distance.
4. Start Query: Initiates the streaming query using writeStream with:
     outputMode("complete"): Rewrites the entire state (all drivers' current totals) on every batch.
     foreachBatch(save_to_csv): Custom function to write each batch's result to CSV.
     trigger(processingTime="10 seconds"): Processes a batch of data every 10 seconds.

## **Sample Output**
<img width="331" height="596" alt="image" src="https://github.com/user-attachments/assets/c1df8f16-d2d6-40ed-b7ad-245424df6131" />

---

## **Task 3: Windowed Time-Based Analytics**

1. Convert the timestamp column to a proper TimestampType.
2. Perform a 5-minute windowed aggregation on fare_amount (sliding by 1 minute and watermarking by 1 minute).

## **Instructions:**

1. Convert the string-based timestamp column to a TimestampType column (e.g., event_time).
2. Use Spark’s window function to aggregate over a 5-minute window, sliding by 1 minute, for the sum of fare_amount.
3. Output the windowed results to csv.

## **Logic Flow**

1. Timestamp Conversion & Watermarking: Converts the timestamp string and applies a 1-minute watermark.
2. Windowed Aggregation: Groups the data by a time window:
    Uses window(col("timestamp"), "1 minute", "1 minute"). This defines a 1-minute window that slides forward by 1 minute, creating non-overlapping (tumbling) windows.
    Calculates avg(col("fare_amount")) as window_avg_fare.
3. Output Selection: Extracts the window.start and window.end columns to explicitly show the aggregation period.
4. Start Query: Initiates the streaming query using writeStream with:
    outputMode("append"): Writes a new aggregate row every time a 1-minute window is closed and calculated.
    foreachBatch(save_to_csv): Custom function to write each batch's result to a unique output directory.
    trigger(processingTime="10 seconds"): Processes a batch of data every 10 seconds.

## **Sample Output**
<img width="498" height="71" alt="image" src="https://github.com/user-attachments/assets/71119f09-168f-4bfa-83fa-11aea3f85209" />

---

## 📬 Submission Checklist

- [ ] Python scripts 
- [ ] Output files in the `outputs/` directory  
- [ ] Completed `README.md`  
- [ ] Commit everything to GitHub Classroom  
- [ ] Submit your GitHub repo link on canvas

---


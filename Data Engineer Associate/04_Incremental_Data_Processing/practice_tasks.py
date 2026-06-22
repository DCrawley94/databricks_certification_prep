# Databricks notebook source
# DBTITLE 1,Topic 4: Incremental/Streaming - Practice
# MAGIC %md
# MAGIC # Topic 4: Incremental Data Processing & Streaming
# MAGIC
# MAGIC ## Focus: Auto Loader, Structured Streaming, Checkpointing, Triggers (Exam-Aligned)
# MAGIC
# MAGIC 15 exercises + 10 MCQs + 2 challenges + 2 applieds covering **exam-relevant** topics:
# MAGIC - Auto Loader configuration and options
# MAGIC - Schema inference, enforcement, and evolution
# MAGIC - Batch triggers (once, availableNow)
# MAGIC - Directory listing vs file notification
# MAGIC - COPY INTO syntax
# MAGIC - Production-ready patterns for serverless
# MAGIC
# MAGIC **Exercises 1-5**: Foundation (basic streaming reads, Auto Loader setup, triggers)
# MAGIC **Exercises 6-10**: Intermediate (schema evolution, format variations, transformations)
# MAGIC **Exercises 11-15**: Advanced (COPY INTO, schema enforcement, production patterns)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Serverless Note**: All exercises use Unity Catalog Volumes (`/Volumes/workspace/default/streaming_data/`) for source files, checkpoints, and schema locations. This is the required pattern for serverless compute.

# COMMAND ----------

# DBTITLE 1,Setup
from pyspark.sql.functions import col, current_timestamp, window
import time

def setup():
    # Create sample streaming source in Unity Catalog Volume
    sample_stream = [
        (1, "2024-06-01 10:00:00", "event1"),
        (2, "2024-06-01 10:01:00", "event2"),
        (3, "2024-06-01 10:02:00", "event3")
    ]
    df = spark.createDataFrame(sample_stream, ["id", "timestamp", "event_type"])

    # Create volume if it doesn't exist
    spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.streaming_data")

    # Write JSON source for Exercises 1, 6, 10, 14
    df.write.mode("overwrite").json(
        "/Volumes/workspace/default/streaming_data/json_source"
    )

    # Write CSV source for Exercises 2-5, 9, 12, 15
    df.write.mode("overwrite").option("header", "true").csv(
        "/Volumes/workspace/default/streaming_data/csv_source"
    )

    # Write Parquet source for Exercise 7
    df.write.mode("overwrite").parquet(
        "/Volumes/workspace/default/streaming_data/parquet_source"
    )

    # Write copy source for Exercise 11
    df.write.mode("overwrite").json(
        "/Volumes/workspace/default/streaming_data/copy_source"
    )

    # Write mixed source for Exercise 13 (both CSV and JSON)
    df.select("id", "event_type").write.mode("overwrite").option("header", "true").csv(
        "/Volumes/workspace/default/streaming_data/mixed_source/data.csv"
    )
    df.select("id", "timestamp").write.mode("overwrite").json(
        "/Volumes/workspace/default/streaming_data/mixed_source/data.json"
    )

    # Write evolving CSV for Challenge 2 (starts with 3 columns)
    df.write.mode("overwrite").option("header", "true").csv(
        "/Volumes/workspace/default/streaming_data/csv_evolving"
    )

    print("Setup complete!")
    print("Sources created in /Volumes/workspace/default/streaming_data/:")
    print("  - json_source (JSON files)")
    print("  - csv_source (CSV files with header)")
    print("  - parquet_source (Parquet files)")
    print("  - copy_source (for COPY INTO)")
    print("  - mixed_source (CSV + JSON for Ex 13)")
    print("  - csv_evolving (for Challenge 2)")

# COMMAND ----------

# DBTITLE 1,Exercises 1-5
# MAGIC %md
# MAGIC ## Exercise 1: Read JSON Stream
# MAGIC Read streaming data from `/Volumes/workspace/default/streaming_data/json_source` in JSON format.
# MAGIC
# MAGIC ## Exercise 2: Auto Loader Reader
# MAGIC Create a reusable Auto Loader reader (`ex2`) that reads streaming CSV from `/Volumes/workspace/default/streaming_data/csv_source`. Must include:
# MAGIC - Schema inference enabled
# MAGIC - Schema location at `/Volumes/workspace/default/streaming_data/schemas`
# MAGIC
# MAGIC This reader will be used as the source for exercises 3-4.
# MAGIC
# MAGIC ## Exercise 3: Complete Write Operation with Trigger Once
# MAGIC Using `ex2` as the source, write a complete streaming query that:
# MAGIC - Writes to Delta format at Volume path `/Volumes/workspace/default/streaming_data/output`
# MAGIC - Uses checkpoint location `/Volumes/workspace/default/streaming_data/checkpoints/stream1`
# MAGIC - Runs once (one-time batch processing)
# MAGIC
# MAGIC **Exam Note**: All streaming write operations require an explicit trigger (once, availableNow, or processingTime).
# MAGIC
# MAGIC ## Exercise 4: Complete Write Operation with Available Now
# MAGIC Using `ex2` as the source (same reader, different stream), write a complete streaming query that:
# MAGIC - Writes to Delta format at Volume path `/Volumes/workspace/default/streaming_data/output_batch`
# MAGIC - Uses checkpoint location `/Volumes/workspace/default/streaming_data/checkpoints/stream2`
# MAGIC - Processes all currently available data in micro-batches
# MAGIC
# MAGIC ## Exercise 5: Processing Time Syntax (Exam Knowledge)
# MAGIC Write the syntax for a trigger that processes data every 5 minutes in continuous mode.
# MAGIC
# MAGIC **Exam Note**: You need to recognize this trigger syntax for the exam. Different compute types support different triggers (once/availableNow for serverless, processingTime for classic clusters).

# COMMAND ----------

# DBTITLE 1,Solutions 1-5
# setup
setup()

# Exercise 1: Read JSON stream from Volume
ex1 = spark.readStream.format("json").load("/Volumes/workspace/default/streaming_data/json_source")

# Exercise 2: Auto Loader reader with schema inference and location
ex2 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas")
    .load("/Volumes/workspace/default/streaming_data/csv_source")
)

# Exercise 3: Complete write operation with trigger once to Volume path
ex3 = (
    ex2.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/stream1")
    .trigger(once=True)
    .start("/Volumes/workspace/default/streaming_data/output")
)

# Exercise 4: Complete write operation with availableNow trigger to Volume path
ex4 = (
    ex2.writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/stream2")
    .trigger(availableNow=True)
    .start("/Volumes/workspace/default/streaming_data/output_batch")
)

# Exercise 5: Processing time trigger syntax (exam knowledge, won't run on serverless)
try:
    ex5 = (
        ex2.writeStream
        .format("delta")
        .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/stream3")
        .trigger(processingTime="5 minutes")
        .start("/Volumes/workspace/default/streaming_data/output_process_time")
    )
except Exception as e:
    print("Processing time trigger not supported on serverless")
    print(f"Error: {e}")

# COMMAND ----------

# DBTITLE 1,Exercise 6: Full Auto Loader Pipeline with Schema Evolution
# MAGIC %md
# MAGIC ## Exercise 6: Full Auto Loader Pipeline with Schema Evolution
# MAGIC Create a complete Auto Loader pipeline for JSON files that:
# MAGIC - Reads from `/Volumes/workspace/default/streaming_data/json_source`
# MAGIC - Enables schema inference AND schema evolution (to handle new columns)
# MAGIC - Sets schema location to `/Volumes/workspace/default/streaming_data/schemas/ex6`
# MAGIC - Writes to Delta table `workspace.default.streaming_ex6`
# MAGIC - Uses checkpoint `/Volumes/workspace/default/streaming_data/checkpoints/ex6`
# MAGIC - Runs once (one-time batch processing)

# COMMAND ----------

# DBTITLE 1,Solution 6
# Your solution for exercise 6
# Write your code here
# setup
setup()

ex6 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex6")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/Volumes/workspace/default/streaming_data/json_source")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/ex6")
    .trigger(once=True)
    .toTable("workspace.default.streaming_ex6")
)

# COMMAND ----------

# DBTITLE 1,Exercise 7: Parquet with Directory Listing Mode
# MAGIC %md
# MAGIC ## Exercise 7: Parquet with Directory Listing Mode
# MAGIC Create Auto Loader pipeline for Parquet files that:
# MAGIC - Reads from `/Volumes/workspace/default/streaming_data/parquet_source`
# MAGIC - Uses directory listing mode explicitly (not file notification)
# MAGIC - Sets schema location to `/Volumes/workspace/default/streaming_data/schemas/ex7`
# MAGIC - Writes to `/Volumes/workspace/default/streaming_data/output_parquet`
# MAGIC - Uses checkpoint `/Volumes/workspace/default/streaming_data/checkpoints/ex7`
# MAGIC - Processes all available data in micro-batches (availableNow)

# COMMAND ----------

# DBTITLE 1,Solution 7
# Your solution for exercise 7
# Write your code here
setup()

ex7 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex7")
    .option("cloudFiles.useNotifications", "false")
    .load("/Volumes/workspace/default/streaming_data/parquet_source")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/ex7")
    .trigger(availableNow=True)
    .start("/Volumes/workspace/default/streaming_data/output_parquet")
)

# COMMAND ----------

# DBTITLE 1,Exercise 8: File Notification Mode Decision
# MAGIC %md
# MAGIC ## Exercise 8: File Notification Mode Decision
# MAGIC When should you use file notification mode instead of directory listing?
# MAGIC Write the Auto Loader option to enable file notification mode.

# COMMAND ----------

# DBTITLE 1,Solution 8
# Your solution for exercise 8
# Write your code here

# I think file notification mode should be used when using cloud storage and files are updated sporadically rather than a regular pattern

ex8_option = "cloudFiles.useNotifications"
ex8_value = "true"

# COMMAND ----------

# DBTITLE 1,Exercise 9: Column Type Inference
# MAGIC %md
# MAGIC ## Exercise 9: Column Type Inference
# MAGIC Create Auto Loader pipeline with automatic column type inference:
# MAGIC - Read CSV from `/Volumes/workspace/default/streaming_data/csv_source`
# MAGIC - Enable column type inference (don't just use strings)
# MAGIC - Set schema location to `/Volumes/workspace/default/streaming_data/schemas/ex9`
# MAGIC - Write to `workspace.default.streaming_ex9`
# MAGIC - Use checkpoint `/Volumes/workspace/default/streaming_data/checkpoints/ex9`
# MAGIC - Run once

# COMMAND ----------

# DBTITLE 1,Solution 9
# Your solution for exercise 9
# Write your code here
setup()

ex9 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex9")
    # .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("/Volumes/workspace/default/streaming_data/csv_source")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/ex9")
    .trigger(once=True)
    .toTable("workspace.default.streaming_ex9")
    .awaitTermination()
)

# COMMAND ----------

# DBTITLE 1,Exercise 10: Data Transformation Pipeline
# MAGIC %md
# MAGIC ## Exercise 10: Data Transformation Pipeline
# MAGIC Build complete pipeline with transformation:
# MAGIC - Read JSON from `/Volumes/workspace/default/streaming_data/json_source` using Auto Loader
# MAGIC - Filter where `id > 1`
# MAGIC - Add column `processed_at` with current timestamp
# MAGIC - Write to Delta table `workspace.default.streaming_ex10`
# MAGIC - Schema location: `/Volumes/workspace/default/streaming_data/schemas/ex10`
# MAGIC - Checkpoint: `/Volumes/workspace/default/streaming_data/checkpoints/ex10`
# MAGIC - Trigger: availableNow

# COMMAND ----------

# DBTITLE 1,Solution 10
# Your solution for exercise 10
# Write your code here
setup()

extract = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex10")
    .load("/Volumes/workspace/default/streaming_data/json_source")
)

transform = (
    extract
    .filter(col("id") > 1)
    .withColumn("processed_at", current_timestamp())
)

load = (
    transform
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/ex10")
    .trigger(availableNow=True)
    .toTable("workspace.default.streaming_ex10")
    .awaitTermination()
)

# COMMAND ----------

# DBTITLE 1,Exercise 11: COPY INTO for Incremental Load
# MAGIC %md
# MAGIC ## Exercise 11: COPY INTO for Incremental Load
# MAGIC Write SQL to incrementally load JSON files from `/Volumes/workspace/default/streaming_data/copy_source/` into table `workspace.default.copy_target`.
# MAGIC Requirements:
# MAGIC - Use COPY INTO (not Auto Loader)
# MAGIC - JSON format
# MAGIC - Should not reload already-processed files

# COMMAND ----------

# DBTITLE 1,Solution 11
# Your solution for exercise 11
# Write your code here

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.default.copy_target
""")

ex11_sql = spark.sql("""
COPY INTO workspace.default.copy_target
FROM "/Volumes/workspace/default/streaming_data/copy_source"
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true')
""")

# COMMAND ----------

# DBTITLE 1,Exercise 12: Schema Enforcement vs Evolution
# MAGIC %md
# MAGIC ## Exercise 12: Schema Enforcement vs Evolution
# MAGIC Create two Auto Loader readers to demonstrate the difference:
# MAGIC - `ex12a`: Schema enforcement (schemaEvolutionMode: "none" + rescuedDataColumn for bad records)
# MAGIC - `ex12b`: Schema evolution (schemaEvolutionMode: "addNewColumns")
# MAGIC
# MAGIC Both read from same CSV source with schema location `/Volumes/workspace/default/streaming_data/schemas/ex12a` and `/Volumes/workspace/default/streaming_data/schemas/ex12b`
# MAGIC
# MAGIC **Key distinction**: 
# MAGIC - `ex12a` with mode "none" will REJECT new columns (they'll appear in rescued data if rescuedDataColumn is set)
# MAGIC - `ex12b` with mode "addNewColumns" will ACCEPT and add new columns to the table automatically
# MAGIC
# MAGIC **Note**: Default behavior when using schemaLocation is "addNewColumns", so you must explicitly set "none" for true enforcement

# COMMAND ----------

# DBTITLE 1,Solution 12
# Your solution for exercise 12
# Write your code here

ex12a = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex12a")
    .option("cloudFiles.schemaEvolutionMode", "none")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load("/Volumes/workspace/default/streaming_data/csv_source")
)

ex12b = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex12b")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("/Volumes/workspace/default/streaming_data/csv_source")
)

# COMMAND ----------

# DBTITLE 1,Exercise 13: Mixed File Types Strategy
# MAGIC %md
# MAGIC ## Exercise 13: Mixed File Types Strategy
# MAGIC You have a directory `/Volumes/workspace/default/streaming_data/mixed_source/` with both CSV and JSON files.
# MAGIC
# MAGIC **Problem**: Auto Loader requires explicit `cloudFiles.format` - there's no automatic file format detection.
# MAGIC
# MAGIC **Task**: Write code using Auto Loader that handles both file types.

# COMMAND ----------

# DBTITLE 1,Solution 13
# Your solution for exercise 13
# Write your code here

reader_1 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex13")
    .load("/Volumes/workspace/default/streaming_data/mixed_source/*.json")
)

reader_2 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex13")
    .load("/Volumes/workspace/default/streaming_data/mixed_source/*.csv")
)

# COMMAND ----------

# DBTITLE 1,Exercise 14: Checkpoint and Schema Location Requirements
# MAGIC %md
# MAGIC ## Exercise 14: Unity Catalog Volume Paths Best Practice
# MAGIC Why should checkpoint and schema locations use Unity Catalog Volume paths (e.g., `/Volumes/catalog/schema/volume/`) instead of legacy DBFS paths (e.g., `/mnt/` or `/dbfs/`)?
# MAGIC
# MAGIC Create an Auto Loader reader that follows Unity Catalog governance best practices:
# MAGIC - Read JSON from `/Volumes/workspace/default/streaming_data/json_source`
# MAGIC - Schema location in a UC Volume
# MAGIC - Include checkpoint location in a UC Volume
# MAGIC - Write to Delta table `workspace.default.uc_volumes_ex14`
# MAGIC - Trigger: once

# COMMAND ----------

# DBTITLE 1,Solution 14
# Your solution for exercise 14
# Write your code here

ex14 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex14")
    .load("/Volumes/workspace/default/streaming_data/json_source")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/ex14")
    .trigger(once=True)
    .toTable("workspace.default.uc_volumes_ex14")
)

# COMMAND ----------

# DBTITLE 1,Exercise 15: Production-Ready Pipeline
# MAGIC %md
# MAGIC ## Exercise 15: Production-Ready Pipeline
# MAGIC Build a complete production-ready Auto Loader pipeline:
# MAGIC - Read CSV from `/Volumes/workspace/default/streaming_data/csv_source`
# MAGIC - Enable column type inference
# MAGIC - Enable schema evolution
# MAGIC - Set schema location in UC Volume
# MAGIC - Filter invalid records (where id IS NOT NULL)
# MAGIC - Add ingestion timestamp column
# MAGIC - Write to Delta table with checkpoint in UC Volume
# MAGIC - Use availableNow trigger for micro-batch processing
# MAGIC - Include error handling with rescuedDataColumn

# COMMAND ----------

# DBTITLE 1,Solution 15
# Your solution for exercise 15
# Write your code here

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex15")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load("/Volumes/workspace/default/streaming_data/csv_source")
    .filter(col("id").isNotNull())
    .withColumn("ingestion_date", current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/ex15/checkpoint")
    .trigger(availableNow=True)
    .toTable("workspace.default.ex15")
)

# COMMAND ----------

# DBTITLE 1,Multiple Choice Questions
# MCQ 1: Default Auto Loader notification mode?
# A) file notification B) directory listing C) both D) none
mcq1 = 'B'

# MCQ 2: What does checkpoint store?
# A) Data B) Offsets C) Schema D) All
mcq2 = 'B'

# MCQ 3: Trigger.Once for?
# A) Real-time B) Batch C) Micro-batch D) Continuous
mcq3 = 'B'

# MCQ 4: Watermark purpose?
# A) Dedupe B) Late data C) Ordering D) Filtering
mcq4 = 'B'

# MCQ 5: Key difference between availableNow and once triggers?
# A) Same behavior B) availableNow uses micro-batches, once uses single batch C) once is faster D) availableNow is for real-time only
mcq5 = 'B'

# COMMAND ----------

# DBTITLE 1,Challenge 1: End-to-End Streaming
# MAGIC %md
# MAGIC ## Challenge 1: Ingestion Method Selection
# MAGIC Given these scenarios, choose the best ingestion method (Auto Loader, COPY INTO, or Lakeflow Connect) and explain why:
# MAGIC
# MAGIC **Scenario A**: Load 50GB of Parquet files once per day from S3 into a Delta table. Files are well-structured and schema is stable.
# MAGIC
# MAGIC **Scenario B**: Continuously ingest JSON files arriving every few seconds from cloud storage. Schema changes occasionally with new fields. Need to process millions of files over time.
# MAGIC
# MAGIC **Scenario C**: Ingest data from Salesforce into Delta table on a schedule. No custom transformation logic needed.
# MAGIC
# MAGIC ### Scenario B Implementation
# MAGIC Write the complete Auto Loader code with these specifications:
# MAGIC - Read JSON from `/Volumes/workspace/default/streaming_data/json_source`
# MAGIC - Schema location: `/Volumes/workspace/default/streaming_data/schemas/challenge1b`
# MAGIC - Checkpoint location: `/Volumes/workspace/default/streaming_data/checkpoints/challenge1b`
# MAGIC - Write to Delta table `workspace.default.challenge1b_realtime`
# MAGIC - Continuous streaming (processingTime trigger every 10 seconds)
# MAGIC - Enable schema evolution to handle new fields
# MAGIC
# MAGIC **Exam Note**: The scenario describes continuous streaming with `processingTime="10 seconds"` trigger. This is important exam syntax to recognize. However, serverless compute only supports `once` and `availableNow` triggers. For serverless execution, you would use `availableNow` instead, but you should know the `processingTime` syntax for the exam.

# COMMAND ----------

# DBTITLE 1,Challenge 1 Solution
# Your solution for Challenge 1
# Write your code here

challenge1_a = "Possiby copy into, if it's just regukar batch processes then copy into would maybe be best here"

challenge1_b = "Autoloader as it is designed to handle large streaming workloads and with schema evolution it can easily deal with additional fields"

challenge1_c = "Lakeflow connect - salesforce is a popualr sass tool and there are managed connectors for that kind of thing. Plus no custom transformation so no needs for complex python logic"


# Scenario B implementation:

(
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/challenge1b")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load("/Volumes/workspace/default/streaming_data/json_source")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/challenge1b")
    .trigger(processingTime="10 seconds")
    .toTable("workspace.default.challenge1b_realtime")
)

# COMMAND ----------

# DBTITLE 1,Challenge 2: Late Data Handling
# MAGIC %md
# MAGIC ## Challenge 2: Schema Evolution Scenario
# MAGIC You have an existing Auto Loader pipeline reading CSV files. A new source system starts adding 3 new columns to the CSV files.
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Pipeline should continue processing without failing
# MAGIC 2. New columns should be captured in the Delta table
# MAGIC 3. Old records should have NULL for new columns
# MAGIC 4. Implement with appropriate Auto Loader options
# MAGIC
# MAGIC **Additional**: What happens if you use `rescuedDataColumn` instead of schema evolution?

# COMMAND ----------

# DBTITLE 1,Challenge 2 Solution
# Your solution for Challenge 2
# Write your code here

challenge2 = None

challenge2_rescued = None

# COMMAND ----------

# DBTITLE 1,Challenge 2: Simulate Schema Evolution
# Helper: Simulate schema evolution by adding new columns to csv_evolving
# Run this AFTER running challenge2 once to see schema evolution in action

from pyspark.sql.functions import lit

# Add 3 new columns to the source data
df_evolved = (
    spark.read.csv("/Volumes/workspace/default/streaming_data/csv_evolving", header=True)
    .withColumn("new_column_1", lit("value1"))
    .withColumn("new_column_2", lit(100))
    .withColumn("new_column_3", lit("2024-06-17"))
)

# Write the evolved schema back
df_evolved.write.mode("append").option("header", "true").csv(
    "/Volumes/workspace/default/streaming_data/csv_evolving"
)

print("Simulated schema evolution: added 3 new columns")
print("Now run challenge2 again to see Auto Loader adapt to new schema")

# COMMAND ----------

# DBTITLE 1,Challenge 3: Multi-Hop Medallion Architecture
# MAGIC %md
# MAGIC ## Challenge 3: Multi-Hop Medallion Architecture
# MAGIC
# MAGIC Build a complete Bronze → Silver → Gold streaming pipeline using the medallion architecture pattern.
# MAGIC
# MAGIC **Context**: You have raw sales data arriving as JSON files. Build a production-ready multi-hop pipeline that:
# MAGIC 1. Ingests raw data (Bronze)
# MAGIC 2. Cleanses and validates (Silver)
# MAGIC 3. Aggregates for analytics (Gold)
# MAGIC
# MAGIC ### Bronze Layer Requirements
# MAGIC Create `bronze_layer` that:
# MAGIC * Reads streaming JSON from `/Volumes/workspace/default/streaming_data/medallion_source`
# MAGIC * Uses Auto Loader with schema inference and evolution
# MAGIC * Sets schema location to `/Volumes/workspace/default/streaming_data/schemas/medallion_bronze`
# MAGIC * Enables rescuedDataColumn for error handling
# MAGIC * Adds `ingestion_timestamp` column with current_timestamp()
# MAGIC * Writes to Delta table `workspace.default.medallion_bronze`
# MAGIC * Uses checkpoint `/Volumes/workspace/default/streaming_data/checkpoints/medallion_bronze`
# MAGIC * Trigger: availableNow
# MAGIC
# MAGIC ### Silver Layer Requirements
# MAGIC Create `silver_layer` that:
# MAGIC * Reads streaming from Bronze Delta table `workspace.default.medallion_bronze`
# MAGIC * Filters out invalid records where:
# MAGIC   * order_id IS NULL
# MAGIC   * customer_id IS NULL
# MAGIC   * quantity <= 0
# MAGIC   * price <= 0
# MAGIC * Casts `date` column from string to date type
# MAGIC * Deduplicates on `order_id` (if duplicates exist, keep the one with latest ingestion_timestamp)
# MAGIC * Adds `processing_timestamp` column with current_timestamp()
# MAGIC * Writes to Delta table `workspace.default.medallion_silver`
# MAGIC * Uses checkpoint `/Volumes/workspace/default/streaming_data/checkpoints/medallion_silver`
# MAGIC * Trigger: availableNow
# MAGIC
# MAGIC ### Gold Layer Requirements
# MAGIC Create `gold_layer` that:
# MAGIC * Reads streaming from Silver Delta table `workspace.default.medallion_silver`
# MAGIC * Aggregates by `date` and `category`
# MAGIC * Calculates:
# MAGIC   * `total_revenue` (sum of quantity * price)
# MAGIC   * `total_quantity` (sum of quantity)
# MAGIC   * `order_count` (count of distinct order_id)
# MAGIC   * `avg_order_value` (total_revenue / order_count)
# MAGIC * Writes to Delta table `workspace.default.medallion_gold`
# MAGIC * Uses checkpoint `/Volumes/workspace/default/streaming_data/checkpoints/medallion_gold`
# MAGIC * Trigger: availableNow
# MAGIC
# MAGIC **Exam Focus**: This pattern tests your understanding of:
# MAGIC * Multi-hop streaming architecture
# MAGIC * Separate checkpoint locations per stream
# MAGIC * Schema management across layers
# MAGIC * Data quality patterns (filtering, validation, deduplication)
# MAGIC * Streaming aggregations
# MAGIC * Production serverless patterns (UC Volumes, availableNow trigger)

# COMMAND ----------

# DBTITLE 1,Challenge 3 Setup
# MAGIC %md
# MAGIC ## Challenge 3: Setup Data Source
# MAGIC
# MAGIC **Run this cell to create the medallion architecture source data.**
# MAGIC
# MAGIC This creates a realistic streaming source from the existing sales_output table with:
# MAGIC * Multiple batches of JSON files
# MAGIC * Some data quality issues (nulls, invalid values) to test Silver layer filtering
# MAGIC * Enough data to demonstrate aggregations in Gold layer

# COMMAND ----------

# DBTITLE 1,Challenge 3: Create Source Data
from pyspark.sql.functions import col, when, lit
import random

# Read existing sales data
source_df = spark.table("workspace.default.sales_output")

# Add some data quality issues to test Silver layer filtering
# Randomly set some values to null or invalid
df_with_issues = (
    source_df
    .withColumn(
        "order_id",
        when(col("order_id") % 20 == 0, lit(None)).otherwise(col("order_id"))
    )
    .withColumn(
        "customer_id",
        when(col("customer_id") % 15 == 0, lit(None)).otherwise(col("customer_id"))
    )
    .withColumn(
        "quantity",
        when(col("quantity") % 25 == 0, lit(-1)).otherwise(col("quantity"))
    )
    .withColumn(
        "price",
        when(col("price") % 30 == 0, lit(0)).otherwise(col("price"))
    )
)

# Write to Volume location as JSON files (simulate streaming source)
df_with_issues.write.mode("overwrite").json(
    "/Volumes/workspace/default/streaming_data/medallion_source"
)

print("✓ Created medallion source data at /Volumes/workspace/default/streaming_data/medallion_source")
print(f"✓ Total records: {df_with_issues.count()}")
print("✓ Data includes intentional quality issues for Silver layer filtering:")
print("  - Some null order_id values")
print("  - Some null customer_id values")
print("  - Some negative quantity values")
print("  - Some zero price values")
print("\nReady for Bronze → Silver → Gold pipeline!")

# COMMAND ----------

# DBTITLE 1,Challenge 3 Solution
# Your solution for Challenge 3
# Write your code here

bronze_layer = None

silver_layer = None

gold_layer = None

# COMMAND ----------

# DBTITLE 1,Applieds
# MAGIC %md
# MAGIC ## Applied 1: Auto Loader Configuration Factory
# MAGIC Create a function that returns a configured Auto Loader reader with best practices based on file format and volume.
# MAGIC
# MAGIC ## Applied 2: Ingestion Method Selector
# MAGIC Create a decision function that recommends Auto Loader, COPY INTO, or Lakeflow Connect based on requirements.

# COMMAND ----------

# DBTITLE 1,Applied Solutions
# Your solutions for applieds 1-2
# Write your code here

def create_auto_loader_reader():
    pass

def recommend_ingestion_method():
    pass

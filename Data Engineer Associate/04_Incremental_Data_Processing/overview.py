# Databricks notebook source
# DBTITLE 1,Topic 4: Incremental Data Processing
# MAGIC %md
# MAGIC # Topic 4: Incremental Data Processing
# MAGIC
# MAGIC ## Introduction
# MAGIC
# MAGIC Incremental processing loads only new or changed data rather than reprocessing everything. This topic covers Auto Loader and Structured Streaming, both critical for efficient data pipelines.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC * Auto Loader (cloud file ingestion)
# MAGIC * Structured Streaming concepts
# MAGIC * Triggers and checkpointing
# MAGIC * Watermarks and late data
# MAGIC * Stream-to-batch joins
# MAGIC
# MAGIC ### Why This Matters for the Exam
# MAGIC * 10% of exam questions (5 questions)
# MAGIC * Auto Loader syntax heavily tested
# MAGIC * Trigger types and checkpointing
# MAGIC * Watermark calculations
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 1: Auto Loader
# MAGIC %md
# MAGIC ## Concept 1: Auto Loader (VERY HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### What is Auto Loader?
# MAGIC
# MAGIC Auto Loader incrementally and efficiently processes new files as they arrive in cloud storage.
# MAGIC
# MAGIC ### Basic Auto Loader Syntax
# MAGIC
# MAGIC ```python
# MAGIC # Read stream with Auto Loader
# MAGIC df = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .option("cloudFiles.schemaLocation", "/path/to/schema") \
# MAGIC     .load("/path/to/input")
# MAGIC
# MAGIC # Write stream
# MAGIC df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/path/to/checkpoint") \
# MAGIC     .start("/path/to/output")
# MAGIC ```
# MAGIC
# MAGIC ### Auto Loader Options
# MAGIC
# MAGIC #### cloudFiles.format (Required)
# MAGIC
# MAGIC ```python
# MAGIC .option("cloudFiles.format", "json")   # JSON files
# MAGIC .option("cloudFiles.format", "csv")    # CSV files
# MAGIC .option("cloudFiles.format", "parquet") # Parquet files
# MAGIC ```
# MAGIC
# MAGIC #### cloudFiles.schemaLocation (Required)
# MAGIC
# MAGIC ```python
# MAGIC # Store inferred schema
# MAGIC .option("cloudFiles.schemaLocation", "/path/to/schema")
# MAGIC ```
# MAGIC
# MAGIC **Purpose**: 
# MAGIC * Stores inferred schema
# MAGIC * Enables schema evolution
# MAGIC * Required for Auto Loader
# MAGIC
# MAGIC #### cloudFiles.inferColumnTypes
# MAGIC
# MAGIC ```python
# MAGIC # Infer types (default: false for CSV, true for JSON/Parquet)
# MAGIC .option("cloudFiles.inferColumnTypes", "true")
# MAGIC ```
# MAGIC
# MAGIC #### cloudFiles.schemaHints
# MAGIC
# MAGIC ```python
# MAGIC # Override inferred types
# MAGIC .option("cloudFiles.schemaHints", "amount DOUBLE, date DATE")
# MAGIC ```
# MAGIC
# MAGIC ### Schema Evolution
# MAGIC
# MAGIC ```python
# MAGIC # Allow new columns
# MAGIC .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# MAGIC
# MAGIC # Rescue unexpected columns
# MAGIC .option("cloudFiles.schemaEvolutionMode", "rescue")
# MAGIC ```
# MAGIC
# MAGIC ### File Notification Modes
# MAGIC
# MAGIC **Directory listing** (default):
# MAGIC * Lists directory for new files
# MAGIC * Works everywhere
# MAGIC * Slower for many files
# MAGIC
# MAGIC **File notification** (recommended for production):
# MAGIC * Uses cloud notifications (S3 events, Azure Event Grid)
# MAGIC * Much faster
# MAGIC * Requires cloud configuration
# MAGIC
# MAGIC ```python
# MAGIC # Enable file notification
# MAGIC .option("cloudFiles.useNotifications", "true")
# MAGIC ```
# MAGIC
# MAGIC ### Complete Auto Loader Example
# MAGIC
# MAGIC ```python
# MAGIC (
# MAGIC     spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "csv")
# MAGIC         .option("cloudFiles.schemaLocation", "/schema/location")
# MAGIC         .option("cloudFiles.inferColumnTypes", "true")
# MAGIC         .option("header", "true")
# MAGIC         .load("/input/path")
# MAGIC         .writeStream
# MAGIC         .format("delta")
# MAGIC         .option("checkpointLocation", "/checkpoint/location")
# MAGIC         .option("mergeSchema", "true")
# MAGIC         .start("/output/path")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "Which format is required for Auto Loader?"
# MAGIC * Answer: `cloudFiles`
# MAGIC
# MAGIC **Question**: "What are the two required Auto Loader options?"
# MAGIC * Answer: `cloudFiles.format` and `cloudFiles.schemaLocation`
# MAGIC
# MAGIC **Question**: "How do you enable schema evolution in Auto Loader?"
# MAGIC * Answer: Set `cloudFiles.schemaEvolutionMode` to `addNewColumns` or `rescue`
# MAGIC
# MAGIC **Common mistake**: Forgetting `checkpointLocation` in writeStream
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 1.5: COPY INTO
# MAGIC %md
# MAGIC ## Concept 1.5: COPY INTO (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### What is COPY INTO?
# MAGIC
# MAGIC COPY INTO is a SQL-based batch command for incrementally loading files into Delta tables. It automatically tracks processed files and skips them on subsequent runs, providing idempotent loads without streaming infrastructure.
# MAGIC
# MAGIC ### Basic COPY INTO Syntax
# MAGIC
# MAGIC ```sql
# MAGIC COPY INTO catalog.schema.table
# MAGIC FROM '/path/to/files/'
# MAGIC FILEFORMAT = JSON
# MAGIC ```
# MAGIC
# MAGIC ### Common Options
# MAGIC
# MAGIC ```sql
# MAGIC -- CSV with header
# MAGIC COPY INTO catalog.schema.table
# MAGIC FROM '/path/to/files/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# MAGIC
# MAGIC -- Parquet
# MAGIC COPY INTO catalog.schema.table
# MAGIC FROM '/path/to/files/'
# MAGIC FILEFORMAT = PARQUET
# MAGIC
# MAGIC -- With file pattern matching
# MAGIC COPY INTO catalog.schema.table
# MAGIC FROM '/path/to/files/'
# MAGIC FILEFORMAT = JSON
# MAGIC FILES = ('file1.json', 'file2.json')
# MAGIC
# MAGIC -- Copy with validation
# MAGIC COPY INTO catalog.schema.table
# MAGIC FROM '/path/to/files/'
# MAGIC FILEFORMAT = JSON
# MAGIC VALIDATE ALL
# MAGIC ```
# MAGIC
# MAGIC ### Key Features
# MAGIC
# MAGIC **Idempotent**: Automatically tracks which files have been loaded and skips them on subsequent runs
# MAGIC
# MAGIC **Batch operation**: Not streaming - runs as a one-time command
# MAGIC
# MAGIC **No infrastructure**: No checkpoints, schema locations, or streaming queries needed
# MAGIC
# MAGIC **File tracking**: Stores metadata about processed files in the target table
# MAGIC
# MAGIC ### Auto Loader vs COPY INTO Comparison
# MAGIC
# MAGIC | Feature | Auto Loader | COPY INTO |
# MAGIC |---------|-------------|----------|
# MAGIC | **Mode** | Streaming (continuous or triggered) | Batch (one-time) |
# MAGIC | **Language** | Python/Scala | SQL |
# MAGIC | **Infrastructure** | Requires checkpoint + schema location | No additional infrastructure |
# MAGIC | **Schema Evolution** | Advanced options (addNewColumns, rescue) | Basic (inferSchema option) |
# MAGIC | **File Discovery** | Directory listing or cloud notifications | Directory scan |
# MAGIC | **Idempotency** | Via checkpoint | Built-in file tracking |
# MAGIC | **Best For** | Near real-time ingestion, complex transformations | Scheduled batch jobs, simple loads |
# MAGIC | **Serverless** | Requires UC Volumes for checkpoint/schema | Works with any path |
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC **Use Auto Loader when**:
# MAGIC * Need near real-time or continuous processing
# MAGIC * Schema changes frequently (advanced evolution needed)
# MAGIC * Processing millions of files over time
# MAGIC * Need complex transformations during ingestion
# MAGIC * Want streaming architecture benefits
# MAGIC
# MAGIC **Use COPY INTO when**:
# MAGIC * Running scheduled batch jobs (daily, hourly)
# MAGIC * Schema is stable or changes infrequently
# MAGIC * Prefer SQL over Python/Scala
# MAGIC * Want simpler setup (no checkpoint management)
# MAGIC * Loading data once per schedule (not continuously)
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "What SQL command loads new files incrementally without reprocessing?"
# MAGIC * Answer: `COPY INTO`
# MAGIC
# MAGIC **Question**: "Does COPY INTO require a checkpoint location?"
# MAGIC * Answer: No - it tracks files automatically in table metadata
# MAGIC
# MAGIC **Question**: "When should you use COPY INTO instead of Auto Loader?"
# MAGIC * Answer: For scheduled batch jobs where you prefer SQL and don't need streaming infrastructure
# MAGIC
# MAGIC **Common trap**: Thinking COPY INTO is streaming - it's batch only
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 2: Structured Streaming Basics
# MAGIC %md
# MAGIC ## Concept 2: Structured Streaming Basics
# MAGIC
# MAGIC ### Read Stream
# MAGIC
# MAGIC ```python
# MAGIC # Read from Delta table
# MAGIC df = spark.readStream.format("delta").load("/path")
# MAGIC
# MAGIC # Read from Kafka
# MAGIC df = spark.readStream \
# MAGIC     .format("kafka") \
# MAGIC     .option("kafka.bootstrap.servers", "host:port") \
# MAGIC     .option("subscribe", "topic") \
# MAGIC     .load()
# MAGIC ```
# MAGIC
# MAGIC ### Write Stream
# MAGIC
# MAGIC ```python
# MAGIC # Append mode (default)
# MAGIC df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/checkpoint") \
# MAGIC     .start("/output")
# MAGIC
# MAGIC # Complete mode (aggregations only)
# MAGIC df.groupBy("category").count() \
# MAGIC     .writeStream \
# MAGIC     .outputMode("complete") \
# MAGIC     .format("memory") \
# MAGIC     .queryName("counts") \
# MAGIC     .start()
# MAGIC
# MAGIC # Update mode (aggregations with Delta)
# MAGIC df.groupBy("category").count() \
# MAGIC     .writeStream \
# MAGIC     .outputMode("update") \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/checkpoint") \
# MAGIC     .start("/output")
# MAGIC ```
# MAGIC
# MAGIC ### Output Modes
# MAGIC
# MAGIC | Mode | Behavior | Use Case |
# MAGIC |------|----------|----------|
# MAGIC | **append** | Only new rows | Raw data ingestion, non-aggregated streams |
# MAGIC | **complete** | All rows every time | Small aggregations, dashboards |
# MAGIC | **update** | Only changed rows | Large aggregations to Delta |
# MAGIC
# MAGIC ### Checkpointing
# MAGIC
# MAGIC **Purpose**: 
# MAGIC * Tracks processing progress
# MAGIC * Enables fault tolerance
# MAGIC * Allows restarts from last processed position
# MAGIC
# MAGIC **Required**: For all production streams
# MAGIC
# MAGIC ```python
# MAGIC .option("checkpointLocation", "/path/to/checkpoint")
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "Which output mode only writes new rows?"
# MAGIC * Answer: `append`
# MAGIC
# MAGIC **Question**: "What's required for fault tolerance in streaming?"
# MAGIC * Answer: `checkpointLocation`
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 3: Triggers
# MAGIC %md
# MAGIC ## Concept 3: Triggers (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### Trigger Types
# MAGIC
# MAGIC #### Default (Micro-batch)
# MAGIC
# MAGIC ```python
# MAGIC # Process as fast as possible
# MAGIC df.writeStream.start()
# MAGIC ```
# MAGIC
# MAGIC **Behavior**: Continuous micro-batches
# MAGIC
# MAGIC #### Fixed Interval
# MAGIC
# MAGIC ```python
# MAGIC # Process every 10 seconds
# MAGIC df.writeStream.trigger(processingTime="10 seconds").start()
# MAGIC
# MAGIC # Process every 5 minutes
# MAGIC df.writeStream.trigger(processingTime="5 minutes").start()
# MAGIC ```
# MAGIC
# MAGIC **Behavior**: Wait for interval, then process all new data
# MAGIC
# MAGIC #### Once
# MAGIC
# MAGIC ```python
# MAGIC # Process all available data once, then stop
# MAGIC df.writeStream.trigger(once=True).start()
# MAGIC ```
# MAGIC
# MAGIC **Use case**: Scheduled batch jobs with streaming code
# MAGIC
# MAGIC #### Available Now
# MAGIC
# MAGIC ```python
# MAGIC # Process all available data in multiple micro-batches, then stop
# MAGIC df.writeStream.trigger(availableNow=True).start()
# MAGIC ```
# MAGIC
# MAGIC **Use case**: Backfill scenarios
# MAGIC
# MAGIC ### Trigger Comparison
# MAGIC
# MAGIC | Trigger | Behavior | Use Case | Exam Frequency |
# MAGIC |---------|----------|----------|----------------|
# MAGIC | Default | Continuous | Real-time processing | Low |
# MAGIC | Fixed Interval | Every N time | Scheduled processing | High |
# MAGIC | Once | Single batch | Batch jobs | Very High |
# MAGIC | Available Now | Multi-batch once | Backfills | Medium |
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "How do you run a stream as a one-time batch job?"
# MAGIC * Answer: Use `trigger(once=True)`
# MAGIC
# MAGIC **Question**: "Which trigger processes data every 5 minutes?"
# MAGIC * Answer: `trigger(processingTime="5 minutes")`
# MAGIC
# MAGIC **Common trap**: Confusing `once` vs `availableNow`
# MAGIC * `once`: Single micro-batch
# MAGIC * `availableNow`: Multiple micro-batches until caught up
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 4: Watermarks & Late Data
# MAGIC %md
# MAGIC ## Concept 4: Watermarks & Late Data
# MAGIC
# MAGIC ### What is a Watermark?
# MAGIC
# MAGIC Watermark defines how long to wait for late data in time-based aggregations.
# MAGIC
# MAGIC ### Watermark Syntax
# MAGIC
# MAGIC ```python
# MAGIC df.withWatermark("timestamp", "1 hour") \
# MAGIC     .groupBy(
# MAGIC         window(col("timestamp"), "10 minutes"),
# MAGIC         col("category")
# MAGIC     ) \
# MAGIC     .count()
# MAGIC ```
# MAGIC
# MAGIC ### How Watermarks Work
# MAGIC
# MAGIC **Watermark calculation**:
# MAGIC ```
# MAGIC watermark = max_event_time - watermark_delay
# MAGIC ```
# MAGIC
# MAGIC **Example**:
# MAGIC * Latest event timestamp: 10:30 AM
# MAGIC * Watermark delay: 1 hour
# MAGIC * Watermark: 9:30 AM
# MAGIC * Events before 9:30 AM are dropped
# MAGIC
# MAGIC ### Window Functions
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import window
# MAGIC
# MAGIC # Tumbling window (non-overlapping)
# MAGIC df.groupBy(window(col("timestamp"), "10 minutes"))
# MAGIC
# MAGIC # Sliding window (overlapping)
# MAGIC df.groupBy(window(col("timestamp"), "10 minutes", "5 minutes"))
# MAGIC ```
# MAGIC
# MAGIC **Tumbling**: `[0-10), [10-20), [20-30)`
# MAGIC
# MAGIC **Sliding**: `[0-10), [5-15), [10-20), [15-25)`
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "What happens to late data beyond the watermark?"
# MAGIC * Answer: It's dropped
# MAGIC
# MAGIC **Question**: "How is watermark calculated?"
# MAGIC * Answer: `max_event_time - watermark_delay`
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Key Takeaways & Exam Focus
# MAGIC %md
# MAGIC ## Key Takeaways & Exam Focus
# MAGIC
# MAGIC ### Most Testable Concepts
# MAGIC
# MAGIC 1. **Auto Loader syntax** (Very High Frequency)
# MAGIC    * `cloudFiles` format
# MAGIC    * Required options: `cloudFiles.format`, `cloudFiles.schemaLocation`
# MAGIC    * Schema evolution modes
# MAGIC
# MAGIC 2. **Trigger types** (Very High Frequency)
# MAGIC    * `once`: Single batch
# MAGIC    * `processingTime`: Fixed interval
# MAGIC    * `availableNow`: Multi-batch once
# MAGIC
# MAGIC 3. **Checkpointing** (High Frequency)
# MAGIC    * Required for fault tolerance
# MAGIC    * `checkpointLocation` option
# MAGIC
# MAGIC 4. **Output modes** (High Frequency)
# MAGIC    * `append`: New rows only
# MAGIC    * `complete`: All rows
# MAGIC    * `update`: Changed rows
# MAGIC
# MAGIC 5. **COPY INTO** (High Frequency)
# MAGIC    * SQL batch incremental loading
# MAGIC    * Idempotent (skips processed files)
# MAGIC    * No checkpoint needed
# MAGIC    * When to use vs Auto Loader
# MAGIC
# MAGIC 6. **Watermarks** (Medium Frequency)
# MAGIC    * Handles late data
# MAGIC    * Calculation: `max_event_time - delay`
# MAGIC
# MAGIC ### COPY INTO Quick Reference
# MAGIC
# MAGIC ```sql
# MAGIC -- Basic template
# MAGIC COPY INTO catalog.schema.table
# MAGIC FROM '/path/to/files/'
# MAGIC FILEFORMAT = JSON
# MAGIC
# MAGIC -- With options
# MAGIC COPY INTO catalog.schema.table
# MAGIC FROM '/path/to/files/'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# MAGIC ```
# MAGIC
# MAGIC ### Auto Loader Quick Reference
# MAGIC
# MAGIC ```python
# MAGIC # Template
# MAGIC spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "<format>") \
# MAGIC     .option("cloudFiles.schemaLocation", "<path>") \
# MAGIC     .load("<input_path>") \
# MAGIC     .writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "<checkpoint_path>") \
# MAGIC     .start("<output_path>")
# MAGIC ```
# MAGIC
# MAGIC ### Trigger Templates
# MAGIC
# MAGIC ```python
# MAGIC # Once (batch job)
# MAGIC .trigger(once=True)
# MAGIC
# MAGIC # Fixed interval
# MAGIC .trigger(processingTime="5 minutes")
# MAGIC
# MAGIC # Available now (backfill)
# MAGIC .trigger(availableNow=True)
# MAGIC ```
# MAGIC
# MAGIC ### Common Mistakes
# MAGIC
# MAGIC * Forgetting `checkpointLocation` (stream will fail)
# MAGIC * Wrong format name (`cloudFiles` not `autoLoader`)
# MAGIC * Missing `cloudFiles.schemaLocation`
# MAGIC * Using wrong output mode for aggregations
# MAGIC
# MAGIC ### Exam Question Patterns
# MAGIC
# MAGIC **Pattern 1**: "How do you incrementally load CSV files from S3?"
# MAGIC * Answer: Use Auto Loader with `format("cloudFiles")` and `option("cloudFiles.format", "csv")`
# MAGIC
# MAGIC **Pattern 2**: "How do you run a streaming query once as a batch?"
# MAGIC * Answer: Use `trigger(once=True)`
# MAGIC
# MAGIC **Pattern 3**: "What's required for streaming fault tolerance?"
# MAGIC * Answer: `checkpointLocation`
# MAGIC
# MAGIC **Pattern 4**: "What SQL command loads files incrementally without reprocessing?"
# MAGIC * Answer: `COPY INTO`
# MAGIC
# MAGIC **Pattern 5**: "When should you use COPY INTO instead of Auto Loader?"
# MAGIC * Answer: Scheduled batch jobs where you prefer SQL and don't need streaming infrastructure
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Additional Resources
# MAGIC
# MAGIC * [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
# MAGIC * [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
# MAGIC * [Streaming Best Practices](https://docs.databricks.com/structured-streaming/index.html)
# MAGIC
# MAGIC **Next**: Practice Auto Loader scenarios in `practice_tasks.py`.

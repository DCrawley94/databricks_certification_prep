# Databricks notebook source
# DBTITLE 1,Topic 4: Streaming - Complete Solutions
# MAGIC %md
# MAGIC # Topic 4: Incremental/Streaming - Solutions
# MAGIC
# MAGIC **Serverless Note**: All solutions use Unity Catalog Volumes for file storage and Unity Catalog managed tables for sinks. This is the recommended pattern for serverless compute.
# MAGIC
# MAGIC ## Key Patterns
# MAGIC
# MAGIC ### Exercise 1: Read Stream
# MAGIC
# MAGIC **Using readStream with JSON format**:
# MAGIC ```python
# MAGIC stream_df = spark.readStream.format("json").load("/Volumes/workspace/default/streaming_data/source")
# MAGIC display(stream_df)
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: readStream creates a streaming DataFrame that continuously processes new files arriving in the source directory.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 2: Auto Loader
# MAGIC
# MAGIC **Using cloudFiles (Auto Loader) with CSV**:
# MAGIC ```python
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "csv") \
# MAGIC     .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/csv_schema") \
# MAGIC     .option("cloudFiles.inferColumnTypes", "true") \
# MAGIC     .option("header", "true") \
# MAGIC     .load("/Volumes/workspace/default/streaming_data/csv_source")
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: 
# MAGIC * `cloudFiles.format` - File format to process
# MAGIC * `cloudFiles.schemaLocation` - Where Auto Loader stores inferred schema
# MAGIC * `cloudFiles.inferColumnTypes` - Infer types beyond just STRING
# MAGIC * `header` - First row contains column names
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 3: Write Stream
# MAGIC
# MAGIC **Writing to Unity Catalog managed table**:
# MAGIC ```python
# MAGIC query = stream_df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/output") \
# MAGIC     .toTable("workspace.default.streaming_output")
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: 
# MAGIC * Use `.toTable()` for Unity Catalog tables (not `.start()`)
# MAGIC * `outputMode("append")` - Add new records only (default for Delta)
# MAGIC * Checkpoint location is mandatory for production
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 4: Checkpoint Location
# MAGIC
# MAGIC **Explicit checkpoint configuration**:
# MAGIC ```python
# MAGIC query = stream_df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/stream1") \
# MAGIC     .toTable("workspace.default.streaming_output")
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: Checkpoint stores stream progress (offsets) and enables fault tolerance. Each stream needs unique checkpoint directory.
# MAGIC
# MAGIC ### Exercise 5: Trigger Once
# MAGIC
# MAGIC **Batch processing of streaming data**:
# MAGIC ```python
# MAGIC query = stream_df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .trigger(once=True) \
# MAGIC     .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/once") \
# MAGIC     .toTable("workspace.default.streaming_output")
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: `trigger(once=True)` processes all available data once and stops. Ideal for scheduled batch jobs using streaming APIs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 6: Watermark
# MAGIC
# MAGIC **Adding watermark for late data handling**:
# MAGIC ```python
# MAGIC watermarked_df = stream_df.withWatermark("timestamp", "10 minutes")
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: 
# MAGIC * Watermark tracks event time progress
# MAGIC * Allows 10 minutes of late arrivals
# MAGIC * Required for stateful operations with `append` mode
# MAGIC * Data older than watermark is dropped
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 7: Window Aggregation
# MAGIC
# MAGIC **Tumbling window aggregation**:
# MAGIC ```python
# MAGIC from pyspark.sql.functions import window, col
# MAGIC
# MAGIC windowed_df = stream_df \
# MAGIC     .withWatermark("timestamp", "10 minutes") \
# MAGIC     .groupBy(
# MAGIC         window(col("timestamp"), "5 minutes"),
# MAGIC         "event_type"
# MAGIC     ) \
# MAGIC     .count()
# MAGIC
# MAGIC query = windowed_df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/windowed") \
# MAGIC     .toTable("workspace.default.windowed_events")
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: 
# MAGIC * 5-minute tumbling windows
# MAGIC * Watermark allows 10 minutes for late data
# MAGIC * outputMode `append` emits windows only after watermark passes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 8: ForeachBatch
# MAGIC
# MAGIC **Custom sink with foreachBatch**:
# MAGIC ```python
# MAGIC def process_batch(batch_df, batch_id):
# MAGIC     """Process each micro-batch with custom logic."""
# MAGIC     # Custom transformation
# MAGIC     enriched = batch_df.withColumn("batch_id", lit(batch_id))
# MAGIC     
# MAGIC     # Write to Unity Catalog table
# MAGIC     enriched.write.format("delta").mode("append").saveAsTable("workspace.default.custom_output")
# MAGIC
# MAGIC query = stream_df.writeStream \
# MAGIC     .foreachBatch(process_batch) \
# MAGIC     .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/custom") \
# MAGIC     .start()
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: foreachBatch enables custom logic per micro-batch. Useful for multiple sinks, custom transformations, or non-streaming APIs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 9: Available Now
# MAGIC
# MAGIC **Micro-batch all available data**:
# MAGIC ```python
# MAGIC query = stream_df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .trigger(availableNow=True) \
# MAGIC     .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/available_now") \
# MAGIC     .toTable("workspace.default.streaming_output")
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: 
# MAGIC * `availableNow=True` processes all available data in one trigger, then stops
# MAGIC * Similar to `once=True` but processes data in multiple micro-batches
# MAGIC * Better for large backlogs than `once=True`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 10: Schema Location
# MAGIC
# MAGIC **Auto Loader schema configuration**:
# MAGIC ```python
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/json_schema") \
# MAGIC     .option("cloudFiles.inferColumnTypes", "true") \
# MAGIC     .load("/Volumes/workspace/default/streaming_data/json_source")
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: Schema location stores the inferred/evolved schema. Auto Loader tracks schema changes and handles evolution automatically.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## MCQ Answers
# MAGIC
# MAGIC **MCQ 1**: Default Auto Loader file format notification?
# MAGIC * **Answer: B) directory listing**
# MAGIC * Auto Loader defaults to directory listing for small-scale sources
# MAGIC * For cloud storage at scale, configure file notification mode (queue-based)
# MAGIC
# MAGIC **MCQ 2**: What does checkpoint store?
# MAGIC * **Answer: B) Offsets and progress information**
# MAGIC * Checkpoint tracks which data has been processed
# MAGIC * Enables exactly-once processing guarantees
# MAGIC * Does NOT store actual data or full schema
# MAGIC
# MAGIC **MCQ 3**: Trigger.Once for?
# MAGIC * **Answer: B) Batch processing**
# MAGIC * Processes all available data once and stops
# MAGIC * Combines streaming semantics with batch execution
# MAGIC * Ideal for scheduled batch jobs
# MAGIC
# MAGIC **MCQ 4**: Watermark purpose?
# MAGIC * **Answer: B) Late data handling**
# MAGIC * Defines how long to wait for late-arriving records
# MAGIC * Enables state cleanup in aggregations
# MAGIC * Required for append mode with windowed aggregations
# MAGIC
# MAGIC **MCQ 5**: availableNow vs Once?
# MAGIC * **Answer: B) availableNow processes all available data in multiple micro-batches**
# MAGIC * `once=True`: Single micro-batch, can fail on very large backlogs
# MAGIC * `availableNow=True`: Multiple micro-batches, better for large backlogs
# MAGIC * Both process all data and stop
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Challenge Solutions
# MAGIC
# MAGIC ### Challenge 1: End-to-End Streaming Pipeline
# MAGIC
# MAGIC **Complete Auto Loader pipeline with transformations and aggregations**:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col, current_timestamp, window, lit
# MAGIC
# MAGIC # Step 1: Read with Auto Loader
# MAGIC source_df = spark.readStream \
# MAGIC     .format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "json") \
# MAGIC     .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/pipeline") \
# MAGIC     .option("cloudFiles.inferColumnTypes", "true") \
# MAGIC     .load("/Volumes/workspace/default/streaming_data/pipeline_source")
# MAGIC
# MAGIC # Step 2: Transform
# MAGIC transformed_df = source_df \
# MAGIC     .withColumn("processed_time", current_timestamp()) \
# MAGIC     .withColumn("date", col("timestamp").cast("date")) \
# MAGIC     .filter(col("event_type").isNotNull())
# MAGIC
# MAGIC # Step 3: Aggregate with windowing
# MAGIC aggregated_df = transformed_df \
# MAGIC     .withWatermark("timestamp", "10 minutes") \
# MAGIC     .groupBy(
# MAGIC         window(col("timestamp"), "5 minutes"),
# MAGIC         "event_type",
# MAGIC         "date"
# MAGIC     ) \
# MAGIC     .count() \
# MAGIC     .withColumnRenamed("count", "event_count")
# MAGIC
# MAGIC # Step 4: Write to Delta with checkpoint
# MAGIC query = aggregated_df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/pipeline") \
# MAGIC     .toTable("workspace.default.event_aggregates")
# MAGIC
# MAGIC # Monitor the stream
# MAGIC print(f"Stream ID: {query.id}")
# MAGIC print(f"Status: {query.status}")
# MAGIC ```
# MAGIC
# MAGIC **Key elements**:
# MAGIC * Auto Loader for scalable ingestion
# MAGIC * Schema inference and evolution
# MAGIC * Transformation and filtering
# MAGIC * Windowed aggregation with watermark
# MAGIC * Checkpoint for fault tolerance
# MAGIC * Unity Catalog Delta table as sink
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Challenge 2: Late Data Handling
# MAGIC
# MAGIC **Scenario**: Events can arrive up to 15 minutes late
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC from pyspark.sql.functions import window, col
# MAGIC
# MAGIC # Configure watermark to allow 15-minute late arrivals
# MAGIC windowed_df = stream_df \
# MAGIC     .withWatermark("timestamp", "15 minutes") \
# MAGIC     .groupBy(
# MAGIC         window(col("timestamp"), "5 minutes"),
# MAGIC         "event_type"
# MAGIC     ) \
# MAGIC     .count()
# MAGIC
# MAGIC # Write with append mode (windows only output after watermark passes)
# MAGIC query = windowed_df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("append") \
# MAGIC     .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/late_data") \
# MAGIC     .toTable("workspace.default.windowed_events")
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * Watermark of 15 minutes allows late data within that window
# MAGIC * Events arriving > 15 minutes late are dropped
# MAGIC * Windows close 15 minutes after the last event in that window
# MAGIC * `append` mode ensures each window is emitted exactly once
# MAGIC
# MAGIC **Alternative with complete mode** (if you need updates):
# MAGIC ```python
# MAGIC # Complete mode re-emits entire result with each trigger
# MAGIC query = windowed_df.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .outputMode("complete") \
# MAGIC     .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/late_data_complete") \
# MAGIC     .toTable("workspace.default.windowed_events_complete")
# MAGIC ```
# MAGIC
# MAGIC **Trade-offs**:
# MAGIC * `append`: More efficient, windows never change after emission
# MAGIC * `complete`: Less efficient but allows window updates, useful for debugging
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ETL Applieds
# MAGIC
# MAGIC ### Applied 1: Checkpoint Validation
# MAGIC
# MAGIC **Purpose**: Verify checkpoint health before restarting a stream
# MAGIC
# MAGIC ```python
# MAGIC def validate_checkpoint(checkpoint_path):
# MAGIC     """
# MAGIC     Validate streaming checkpoint location.
# MAGIC     
# MAGIC     Args:
# MAGIC         checkpoint_path: Path to checkpoint directory in Unity Catalog Volume
# MAGIC     
# MAGIC     Returns:
# MAGIC         Dictionary with validation results and recommendations
# MAGIC     """
# MAGIC     try:
# MAGIC         files = dbutils.fs.ls(checkpoint_path)
# MAGIC         
# MAGIC         # Check for required checkpoint components
# MAGIC         has_offsets = any("offsets" in f.path for f in files)
# MAGIC         has_commits = any("commits" in f.path for f in files)
# MAGIC         has_metadata = any("metadata" in f.path for f in files)
# MAGIC         
# MAGIC         file_count = len(files)
# MAGIC         
# MAGIC         # Determine health status
# MAGIC         is_valid = has_offsets and has_commits
# MAGIC         is_healthy = is_valid and has_metadata
# MAGIC         
# MAGIC         return {
# MAGIC             "valid": is_valid,
# MAGIC             "healthy": is_healthy,
# MAGIC             "file_count": file_count,
# MAGIC             "has_offsets": has_offsets,
# MAGIC             "has_commits": has_commits,
# MAGIC             "has_metadata": has_metadata,
# MAGIC             "recommendation": "Ready to use" if is_healthy else "Consider fresh checkpoint"
# MAGIC         }
# MAGIC     except Exception as e:
# MAGIC         return {
# MAGIC             "valid": False,
# MAGIC             "healthy": False,
# MAGIC             "file_count": 0,
# MAGIC             "error": str(e),
# MAGIC             "recommendation": "Checkpoint does not exist or is inaccessible"
# MAGIC         }
# MAGIC ```
# MAGIC
# MAGIC **Usage**:
# MAGIC ```python
# MAGIC checkpoint_status = validate_checkpoint("/Volumes/workspace/default/streaming_data/checkpoints/my_stream")
# MAGIC print(f"Valid: {checkpoint_status['valid']}")
# MAGIC print(f"Recommendation: {checkpoint_status['recommendation']}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Applied 2: Auto Loader Configuration
# MAGIC
# MAGIC **Purpose**: Generate optimal Auto Loader configuration based on file format
# MAGIC
# MAGIC ```python
# MAGIC def configure_auto_loader(file_format, source_path, schema_path=None):
# MAGIC     """
# MAGIC     Configure Auto Loader with best practices for given file format.
# MAGIC     
# MAGIC     Args:
# MAGIC         file_format: File format (csv, json, parquet, avro, etc.)
# MAGIC         source_path: Source directory in Unity Catalog Volume
# MAGIC         schema_path: Optional schema location (defaults to source_path/_schema)
# MAGIC     
# MAGIC     Returns:
# MAGIC         Configured streaming DataFrame
# MAGIC     """
# MAGIC     if schema_path is None:
# MAGIC         schema_path = f"{source_path}/_schema"
# MAGIC     
# MAGIC     # Base configuration
# MAGIC     options = {
# MAGIC         "cloudFiles.format": file_format,
# MAGIC         "cloudFiles.schemaLocation": schema_path,
# MAGIC         "cloudFiles.inferColumnTypes": "true"
# MAGIC     }
# MAGIC     
# MAGIC     # Format-specific optimizations
# MAGIC     if file_format == "csv":
# MAGIC         options.update({
# MAGIC             "header": "true",
# MAGIC             "inferSchema": "false",  # Use cloudFiles schema inference instead
# MAGIC             "cloudFiles.schemaHints": ""  # Can add hints like "amount DECIMAL(10,2)"
# MAGIC         })
# MAGIC     elif file_format == "json":
# MAGIC         options.update({
# MAGIC             "multiLine": "false",  # Set to true for multi-line JSON
# MAGIC             "cloudFiles.allowOverwrites": "false"  # Prevent reprocessing modified files
# MAGIC         })
# MAGIC     elif file_format == "parquet":
# MAGIC         options.update({
# MAGIC             "mergeSchema": "false",  # Set true if schema evolves
# MAGIC             "cloudFiles.validateOptions": "true"
# MAGIC         })
# MAGIC     
# MAGIC     # Create streaming DataFrame
# MAGIC     return spark.readStream \
# MAGIC         .format("cloudFiles") \
# MAGIC         .options(**options) \
# MAGIC         .load(source_path)
# MAGIC ```
# MAGIC
# MAGIC **Usage**:
# MAGIC ```python
# MAGIC # CSV example
# MAGIC csv_stream = configure_auto_loader(
# MAGIC     file_format="csv",
# MAGIC     source_path="/Volumes/workspace/default/streaming_data/csv_source"
# MAGIC )
# MAGIC
# MAGIC # JSON example with custom schema location
# MAGIC json_stream = configure_auto_loader(
# MAGIC     file_format="json",
# MAGIC     source_path="/Volumes/workspace/default/streaming_data/json_source",
# MAGIC     schema_path="/Volumes/workspace/default/streaming_data/schemas/json"
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Best Practices**:
# MAGIC * Always use `cloudFiles.inferColumnTypes` for type inference
# MAGIC * Set `cloudFiles.schemaLocation` to persist schema across restarts
# MAGIC * Use `cloudFiles.schemaHints` for critical columns (amounts, dates)
# MAGIC * For JSON, set `multiLine=true` only if needed (performance impact)
# MAGIC * Enable `cloudFiles.useNotifications` for large-scale cloud storage
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC **Auto Loader (cloudFiles)**:
# MAGIC * Requires `cloudFiles.format` and `cloudFiles.schemaLocation`
# MAGIC * Use `cloudFiles.inferColumnTypes` for automatic type inference
# MAGIC * Schema evolution handled automatically
# MAGIC * Directory listing (default) vs file notification mode (for scale)
# MAGIC
# MAGIC **Checkpoints**:
# MAGIC * Mandatory for production streaming
# MAGIC * Stores offsets, metadata, and state
# MAGIC * Each stream needs unique checkpoint directory
# MAGIC * Use Unity Catalog Volumes for checkpoints on serverless
# MAGIC
# MAGIC **Triggers**:
# MAGIC * No trigger: Continuous micro-batch processing (default)
# MAGIC * `trigger(once=True)`: Single micro-batch, then stop
# MAGIC * `trigger(availableNow=True)`: Multiple micro-batches until caught up, then stop
# MAGIC * `trigger(processingTime="10 seconds")`: Micro-batch every 10 seconds
# MAGIC
# MAGIC **Watermarks**:
# MAGIC * Define how long to wait for late data
# MAGIC * Required for append mode with windowed aggregations
# MAGIC * Format: `.withWatermark("timestamp_column", "duration")`
# MAGIC * Data older than watermark is dropped
# MAGIC
# MAGIC **Output Modes**:
# MAGIC * `append`: Only new rows (most efficient, default for Delta)
# MAGIC * `complete`: Entire result re-emitted (only for aggregations)
# MAGIC * `update`: Only changed rows (not supported by all sinks)
# MAGIC
# MAGIC **Sinks**:
# MAGIC * Unity Catalog tables: Use `.toTable("catalog.schema.table")`
# MAGIC * File paths: Use `.start(path)` (avoid on serverless)
# MAGIC * Custom logic: Use `.foreachBatch(function)`
# MAGIC
# MAGIC **Serverless Best Practices**:
# MAGIC * Use Unity Catalog Volumes for source files and checkpoints
# MAGIC * Use Unity Catalog managed tables for sinks
# MAGIC * Avoid DBFS (`/dbfs/`) and `/tmp/` paths
# MAGIC * Format: `/Volumes/catalog/schema/volume/path`

# COMMAND ----------

# DBTITLE 1,Solutions 6-10
# Exercise 6: Full Auto Loader pipeline with schema evolution
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

# Exercise 7: Parquet with directory listing mode
ex7 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "false")  # Explicit directory listing
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex7")
    .load("/Volumes/workspace/default/streaming_data/parquet_source")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/ex7")
    .trigger(availableNow=True)
    .start("/Volumes/workspace/default/streaming_data/output_parquet")
)

# Exercise 8: File notification mode decision
# Use file notification when:
# - Processing millions of files
# - Need better scalability than directory listing
# - Files arrive frequently in large volumes
ex8_option = "cloudFiles.useNotifications"
ex8_value = "true"

# Exercise 9: Column type inference
ex9 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex9")
    .option("cloudFiles.inferColumnTypes", "true")  # Infer int, double, etc.
    .load("/Volumes/workspace/default/streaming_data/csv_source")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/ex9")
    .trigger(once=True)
    .toTable("workspace.default.streaming_ex9")
)

# Exercise 10: Data transformation pipeline
from pyspark.sql.functions import current_timestamp, col

ex10 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex10")
    .load("/Volumes/workspace/default/streaming_data/json_source")
    .filter(col("id") > 1)
    .withColumn("processed_at", current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/ex10")
    .trigger(availableNow=True)
    .toTable("workspace.default.streaming_ex10")
)

# COMMAND ----------

# DBTITLE 1,Solutions 11-15
# Exercise 11: COPY INTO for incremental load
# CRITICAL: COPY INTO does NOT auto-create tables - you must create the table first

# APPROACH A: Empty table + mergeSchema (recommended for unknown schema)
# Create empty table, let COPY INTO infer schema on first load
spark.sql("CREATE TABLE IF NOT EXISTS workspace.default.copy_target")

ex11_approach_a = spark.sql("""
COPY INTO workspace.default.copy_target
FROM '/Volumes/workspace/default/streaming_data/copy_source/'
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true')
""")

# APPROACH B: Explicit schema (when schema is known upfront)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.default.copy_target (
  id BIGINT,
  timestamp STRING,
  event_type STRING
)
""")

ex11_approach_b = spark.sql("""
COPY INTO workspace.default.copy_target
FROM '/Volumes/workspace/default/streaming_data/copy_source/'
FILEFORMAT = JSON
""")

# APPROACH C: CTAS with LIMIT 0 (alternative schema inference)
spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.default.copy_target AS 
SELECT * FROM read_files(
  '/Volumes/workspace/default/streaming_data/copy_source/',
  format => 'json'
) LIMIT 0
""")

ex11_approach_c = spark.sql("""
COPY INTO workspace.default.copy_target
FROM '/Volumes/workspace/default/streaming_data/copy_source/'
FILEFORMAT = JSON
""")

# For the exam: Know that COPY INTO requires pre-existing table
# Common exam answer (incomplete, assumes table exists):
ex11_sql = """
COPY INTO workspace.default.copy_target
FROM '/Volumes/workspace/default/streaming_data/copy_source/'
FILEFORMAT = JSON
"""

# Exercise 11: Key Takeaways
# 1. COPY INTO does NOT auto-create tables (unlike some documentation suggests)
# 2. You will get error: "Table doesn't exist. Create an empty Delta table first"
# 3. mergeSchema option allows schema inference after table creation
# 4. Exam likely expects simplified syntax (assumes table exists)
# 5. Production requires one of the three approaches above

# When to use each approach:
# - Approach A: Unknown schema, flexible, most practical for real-world use
# - Approach B: Known schema, explicit control, best performance, clear intent
# - Approach C: Alternative inference method using read_files LIMIT 0

# Exercise 12: Schema enforcement vs evolution
# Schema enforcement: explicitly disable evolution, rescue bad/new data
ex12a = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex12a")
    .option("cloudFiles.schemaEvolutionMode", "none")  # CRITICAL: Disable evolution for enforcement
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")  # Capture bad records and new columns
    .load("/Volumes/workspace/default/streaming_data/csv_source")
)

# Schema evolution: adapt to new columns automatically
ex12b = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex12b")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Auto-add new columns to table
    .load("/Volumes/workspace/default/streaming_data/csv_source")
)

# Key Distinction:
# ex12a with mode "none": New columns -> rejected/rescued, malformed data -> rescued
# ex12b with mode "addNewColumns": New columns -> added to table, malformed data -> fails stream
# Note: Default when using schemaLocation is "addNewColumns", so "none" must be explicit

# Exercise 13: Mixed file types - Auto Loader with format inference
ex13 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")  # Primary format
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex13")
    .load("/Volumes/workspace/default/streaming_data/mixed_source")
)
# Note: For true mixed types, process separately or use format-specific paths
# Approach 2 would be: separate Auto Loader pipelines per format

# Exercise 14: Serverless requires UC Volumes for checkpoints and schema locations
# Reason: Serverless cannot access DBFS root (/mnt/, /dbfs/) - only UC Volumes
ex14 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    # CORRECT for serverless: UC Volume path
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex14")
    .load("/Volumes/workspace/default/streaming_data/json_source")
)
# WRONG for serverless: .option("cloudFiles.schemaLocation", "/mnt/schemas/ex14")

# Exercise 15: Production-ready pipeline with all best practices
ex15 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/ex15")
    .option("cloudFiles.inferColumnTypes", "true")  # Infer proper types
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # Handle schema changes
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")  # Capture malformed data
    .load("/Volumes/workspace/default/streaming_data/csv_source")
    .filter(col("id").isNotNull())  # Data quality check
    .withColumn("ingestion_timestamp", current_timestamp())  # Audit column
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/ex15")
    .trigger(availableNow=True)  # Micro-batch processing
    .toTable("workspace.default.streaming_ex15")
)

# COMMAND ----------

# DBTITLE 1,MCQ Solutions
# MCQ Solutions (Exercises 6-15 focus)

mcq1_answer = 'B'  # Directory listing is default
mcq2_answer = 'B'  # Checkpoints track offsets/progress
mcq3_answer = 'C'  # once = single micro-batch
mcq4_answer = 'C'  # Serverless requires UC Volumes
mcq5_answer = 'B'  # Infers proper types instead of all strings
mcq6_answer = 'C'  # Automatically adapts to new columns
mcq7_answer = 'B'  # File notification scales better for high volume
mcq8_answer = 'B'  # COPY INTO is simpler for smaller volumes
mcq9_answer = 'B'  # availableNow can use multiple micro-batches
mcq10_answer = 'B'  # Captures bad data instead of failing

# COMMAND ----------

# DBTITLE 1,Challenge Solutions
# Challenge 1: Ingestion method selection

# Scenario A: COPY INTO
# Reason: One-time daily load, stable schema, moderate volume - COPY INTO is simpler
challenge1_a = """
COPY INTO workspace.default.target_table
FROM '/Volumes/workspace/default/streaming_data/parquet_source/'
FILEFORMAT = PARQUET
"""

# Scenario B: Auto Loader
# Reason: Continuous ingestion, schema evolution, high file count - Auto Loader with file notification
challenge1_b = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/scenario_b")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.useNotifications", "true")  # For millions of files
    .option("cloudFiles.inferColumnTypes", "true")
    .load("/Volumes/workspace/default/streaming_data/json_source")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/scenario_b")
    .trigger(availableNow=True)
    .toTable("workspace.default.scenario_b_target")
)

# Scenario C: Lakeflow Connect
# Reason: SaaS source (Salesforce), managed connector available, no custom logic
challenge1_c = "Use Lakeflow Connect managed connector for Salesforce"

# Challenge 2: Schema evolution scenario

challenge2 = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/evolving")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # KEY: Adapt to new columns
    .option("cloudFiles.inferColumnTypes", "true")
    .load("/Volumes/workspace/default/streaming_data/csv_evolving")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/evolving")
    .option("mergeSchema", "true")  # Allow Delta table schema to evolve
    .trigger(availableNow=True)
    .toTable("workspace.default.evolving_table")
)

# Comparison: rescuedDataColumn vs schema evolution
challenge2_rescued = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/rescued")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")  # Captures data that doesn't match schema
    .load("/Volumes/workspace/default/streaming_data/csv_evolving")
)
# Result: New columns would be captured as JSON in _rescued_data column (not ideal)
# Schema evolution is better for legitimate schema changes

# COMMAND ----------

# DBTITLE 1,Challenge 3 Solution: Medallion Architecture
from pyspark.sql.functions import col, current_timestamp, to_date, sum as _sum, count, avg, expr
from pyspark.sql.window import Window

# Challenge 3: Multi-Hop Medallion Architecture

# Bronze Layer: Raw ingestion with Auto Loader
bronze_layer = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/workspace/default/streaming_data/schemas/medallion_bronze")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")  # Capture malformed records
    .load("/Volumes/workspace/default/streaming_data/medallion_source")
    .withColumn("ingestion_timestamp", current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/medallion_bronze")
    .trigger(availableNow=True)
    .toTable("workspace.default.medallion_bronze")
)

# Silver Layer: Cleansed and validated
silver_layer = (
    spark.readStream
    .table("workspace.default.medallion_bronze")
    .filter(
        (col("order_id").isNotNull()) &
        (col("customer_id").isNotNull()) &
        (col("quantity") > 0) &
        (col("price") > 0)
    )
    .withColumn("date", to_date(col("date")))
    # Deduplication: keep latest record per order_id based on ingestion_timestamp
    .dropDuplicates(["order_id"])
    .withColumn("processing_timestamp", current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/medallion_silver")
    .trigger(availableNow=True)
    .toTable("workspace.default.medallion_silver")
)

# Gold Layer: Business aggregations
gold_layer = (
    spark.readStream
    .table("workspace.default.medallion_silver")
    .withColumn("revenue", col("quantity") * col("price"))
    .groupBy("date", "category")
    .agg(
        _sum("revenue").alias("total_revenue"),
        _sum("quantity").alias("total_quantity"),
        count("order_id").alias("order_count"),
        ((_sum("revenue") / count("order_id"))).alias("avg_order_value")
    )
    .writeStream
    .format("delta")
    .outputMode("complete")  # Required for aggregations without watermark
    .option("checkpointLocation", "/Volumes/workspace/default/streaming_data/checkpoints/medallion_gold")
    .trigger(availableNow=True)
    .toTable("workspace.default.medallion_gold")
)

print("Challenge 3: Multi-hop medallion architecture deployed")
print("Bronze → Silver → Gold pipeline created")

# COMMAND ----------

# DBTITLE 1,Applied Solutions
def create_auto_loader_reader(source_path, file_format, schema_location, 
                               enable_schema_evolution=True, 
                               enable_type_inference=True,
                               use_file_notification=False):
    """
    Create Auto Loader reader with best practices.
    
    Args:
        source_path: Volume path to source files
        file_format: File format (csv, json, parquet, etc.)
        schema_location: Volume path for schema storage
        enable_schema_evolution: Allow new columns
        enable_type_inference: Infer column types (not just strings)
        use_file_notification: Use cloud notifications (for millions of files)
    
    Returns:
        Configured streaming DataFrame reader
    """
    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_location)
    )
    
    if enable_schema_evolution:
        reader = reader.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    
    if enable_type_inference:
        reader = reader.option("cloudFiles.inferColumnTypes", "true")
    
    if use_file_notification:
        reader = reader.option("cloudFiles.useNotifications", "true")
    
    return reader.load(source_path)


def recommend_ingestion_method(source_type, file_count, frequency, schema_stability):
    """
    Recommend ingestion method based on requirements.
    
    Args:
        source_type: 'files', 'database', or 'saas'
        file_count: 'low' (<1000), 'medium' (1000-100k), 'high' (>100k)
        frequency: 'once', 'daily', 'hourly', 'continuous'
        schema_stability: 'stable' or 'evolving'
    
    Returns:
        Recommended method and reasoning
    """
    if source_type == 'saas':
        return "Lakeflow Connect", "Managed connectors for SaaS sources"
    
    if source_type == 'database':
        if frequency == 'once':
            return "JDBC in notebook", "One-time database extract"
        else:
            return "Lakeflow Connect or JDBC scheduled", "Recurring database ingestion"
    
    if source_type == 'files':
        if frequency == 'once' and file_count == 'low' and schema_stability == 'stable':
            return "COPY INTO", "Simple one-time load with stable schema"
        
        if file_count == 'high' or frequency == 'continuous' or schema_stability == 'evolving':
            return "Auto Loader", "High volume, continuous, or evolving schema"
        
        return "Auto Loader or COPY INTO", "Either works for medium complexity"
    
    return "Unknown", "Cannot determine from inputs"

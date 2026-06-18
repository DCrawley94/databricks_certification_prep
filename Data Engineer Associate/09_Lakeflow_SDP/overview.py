# Databricks notebook source
# DBTITLE 1,Topic 9: Lakeflow Spark Declarative Pipelines
# MAGIC %md
# MAGIC # Topic 9: Lakeflow Spark Declarative Pipelines (SDP)
# MAGIC
# MAGIC **Exam Weight: Section 2 (Data Ingestion and Loading) + Section 3 (Transformation) - 15-20% combined**
# MAGIC
# MAGIC Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables) is Databricks' framework for building reliable, maintainable data pipelines using declarative Python or SQL. This is a critical exam topic with NO existing module coverage.
# MAGIC
# MAGIC ## Product Name Change (EXAM-CRITICAL)
# MAGIC
# MAGIC **DO NOT call this "Delta Live Tables" or "DLT" on the exam.** The product was renamed in May 2026:
# MAGIC * Old name: Delta Live Tables (DLT)
# MAGIC * New name: Lakeflow Spark Declarative Pipelines (SDP)
# MAGIC * Shorthand: SDP
# MAGIC
# MAGIC **Exam impact**: Questions will use "Lakeflow Spark Declarative Pipelines" or "SDP". If you see answer choices with "Delta Live Tables", they may be outdated distractors.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Why This Topic Matters for the Exam
# MAGIC
# MAGIC SDP questions test your ability to:
# MAGIC 1. Choose between streaming tables and materialized views
# MAGIC 2. Configure expectations for data quality enforcement
# MAGIC 3. Use AUTO CDC for change data capture
# MAGIC 4. Understand incremental processing semantics
# MAGIC 5. Distinguish SDP from Spark Structured Streaming and Lakeflow Connect
# MAGIC 6. Configure and troubleshoot pipeline deployments
# MAGIC
# MAGIC **Critical exam trap**: SDP is NOT the same as Structured Streaming or Lakeflow Connect. Each has different use cases and capabilities.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exam Objectives Covered
# MAGIC
# MAGIC From the official exam guide:
# MAGIC
# MAGIC * Create and configure Lakeflow Spark Declarative Pipelines for incremental and streaming data transformations
# MAGIC * Apply expectations and data quality constraints in SDP pipelines
# MAGIC * Use AUTO CDC for change data capture patterns
# MAGIC * Distinguish between streaming tables and materialized views
# MAGIC * Configure pipeline compute, storage, and retry policies
# MAGIC * Monitor pipeline execution and diagnose failures
# MAGIC * Integrate SDP pipelines with Unity Catalog governance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Topic Coverage
# MAGIC
# MAGIC 1. **SDP Fundamentals**: Declarative vs imperative, pipeline graph, live tables
# MAGIC 2. **Streaming Tables**: Continuous processing, append-only semantics, checkpointing
# MAGIC 3. **Materialized Views**: Batch processing, complete refresh, dependencies
# MAGIC 4. **Expectations**: Data quality constraints (warn, drop, fail)
# MAGIC 5. **AUTO CDC**: Apply changes (upserts, deletes) from CDC sources
# MAGIC 6. **Pipeline Configuration**: Compute, storage, development vs production modes
# MAGIC 7. **Incremental Processing**: How SDP tracks and processes changes
# MAGIC 8. **Monitoring and Troubleshooting**: Event logs, lineage, error diagnosis
# MAGIC 9. **Decision Scenarios**: When to use SDP vs alternatives

# COMMAND ----------

# DBTITLE 1,SDP Fundamentals
# MAGIC %md
# MAGIC ## SDP Fundamentals
# MAGIC
# MAGIC ### What Is Lakeflow Spark Declarative Pipelines?
# MAGIC
# MAGIC Lakeflow SDP is a framework for building and managing data pipelines where you declare WHAT you want (the desired state), not HOW to compute it (the execution plan). Databricks manages orchestration, retries, and incremental processing.
# MAGIC
# MAGIC **Key characteristic**: You define tables (streaming or materialized), and SDP figures out:
# MAGIC * Execution order (dependency graph)
# MAGIC * Incremental updates (what's changed since last run)
# MAGIC * Error recovery (automatic retries)
# MAGIC * Data quality enforcement (expectations)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Declarative vs Imperative
# MAGIC
# MAGIC **Imperative approach** (standard Spark):
# MAGIC ```python
# MAGIC # You specify HOW to process
# MAGIC df = spark.readStream.table("raw_events")
# MAGIC filtered = df.filter(col("event_type") == "purchase")
# MAGIC filtered.writeStream.table("purchases")
# MAGIC ```
# MAGIC
# MAGIC **Declarative approach** (SDP):
# MAGIC ```python
# MAGIC # You specify WHAT you want
# MAGIC @dlt.table
# MAGIC def purchases():
# MAGIC     return dlt.read_stream("raw_events").filter(col("event_type") == "purchase")
# MAGIC ```
# MAGIC
# MAGIC SDP handles:
# MAGIC * When to run this table
# MAGIC * Dependencies (run after raw_events is ready)
# MAGIC * Incremental processing (only new rows)
# MAGIC * Retries on failure
# MAGIC * Checkpointing and recovery
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Core Concepts
# MAGIC
# MAGIC #### 1. Live Tables
# MAGIC
# MAGIC A "live table" is any table defined in an SDP pipeline. It can be:
# MAGIC * **Streaming table**: Processes data continuously as it arrives
# MAGIC * **Materialized view**: Recomputes from scratch on schedule
# MAGIC
# MAGIC **Critical distinction**: All SDP tables are "live" in the sense that SDP manages them. But "streaming table" has a specific technical meaning (continuous incremental processing).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Pipeline Graph
# MAGIC
# MAGIC SDP builds a directed acyclic graph (DAG) of dependencies:
# MAGIC ```
# MAGIC raw_events --> purchases --> daily_summary
# MAGIC            --> returns
# MAGIC ```
# MAGIC
# MAGIC Execution rules:
# MAGIC * Tables run in dependency order (parents before children)
# MAGIC * Independent tables can run in parallel
# MAGIC * Failures stop downstream tables but don't affect independent branches
# MAGIC
# MAGIC **Exam scenario**: "Table C depends on tables A and B. Table B fails. What happens to C?"
# MAGIC * **Answer**: C doesn't run (dependency failed)
# MAGIC * **What about table D, which depends only on A?** D runs normally (independent of B's failure)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3. Incremental Processing
# MAGIC
# MAGIC SDP tracks which data has been processed and only processes new/changed rows:
# MAGIC * **Streaming tables**: Use Structured Streaming checkpoints
# MAGIC * **Materialized views**: Use table versions and watermarks
# MAGIC
# MAGIC **How it works**:
# MAGIC 1. First run: Process all source data
# MAGIC 2. Subsequent runs: Process only new data since last checkpoint
# MAGIC 3. On failure: Resume from last successful checkpoint
# MAGIC
# MAGIC **Exam trap**: Materialized views do NOT incrementally process - they recompute from scratch each run. Only streaming tables are truly incremental.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### SDP vs Alternatives
# MAGIC
# MAGIC #### SDP vs Structured Streaming
# MAGIC
# MAGIC | Aspect | SDP | Structured Streaming |
# MAGIC |--------|-----|----------------------|
# MAGIC | **Definition** | Declarative (WHAT) | Imperative (HOW) |
# MAGIC | **Orchestration** | Automatic | Manual |
# MAGIC | **Dependencies** | Automatic graph | Manual ordering |
# MAGIC | **Retries** | Built-in | You implement |
# MAGIC | **Data quality** | Expectations framework | Custom logic |
# MAGIC | **Use case** | Multi-stage pipelines | Single-stage streaming |
# MAGIC
# MAGIC **Exam scenario**: "You have a complex pipeline with 10 interdependent tables. Should you use SDP or Structured Streaming?"
# MAGIC * **Answer**: SDP (manages dependencies and orchestration automatically)
# MAGIC * **When to use Structured Streaming**: Single-stage transformations with custom logic not supported by SDP
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### SDP vs Lakeflow Connect
# MAGIC
# MAGIC | Aspect | SDP | Lakeflow Connect |
# MAGIC |--------|-----|------------------|
# MAGIC | **Purpose** | Transform data within Databricks | Ingest data FROM external sources |
# MAGIC | **Source** | Tables/files in Databricks | Databases, SaaS apps, external files |
# MAGIC | **Language** | Python or SQL | Configuration (no code) |
# MAGIC | **Transformations** | Complex multi-stage | None (ingestion only) |
# MAGIC | **Use case** | Build data pipelines | Load data into Databricks |
# MAGIC
# MAGIC **Exam scenario**: "You need to ingest MySQL data and transform it through bronze/silver/gold layers. What do you use?"
# MAGIC * **Answer**: Lakeflow Connect (ingest) + SDP (transform)
# MAGIC * **Wrong answer**: SDP alone (can't ingest from MySQL) or Lakeflow Connect alone (doesn't transform)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pipeline Anatomy
# MAGIC
# MAGIC An SDP pipeline consists of:
# MAGIC
# MAGIC 1. **Source code**: Python or SQL files defining tables
# MAGIC 2. **Configuration**: Compute, storage locations, mode (dev/prod)
# MAGIC 3. **Target catalog/schema**: Where tables are written
# MAGIC 4. **Cluster settings**: Compute resources for execution
# MAGIC
# MAGIC **File structure**:
# MAGIC ```
# MAGIC pipeline/
# MAGIC   bronze.py          # Raw data ingestion
# MAGIC   silver.py          # Cleaned/standardized
# MAGIC   gold.py            # Business-level aggregates
# MAGIC   expectations.py    # Data quality rules
# MAGIC ```
# MAGIC
# MAGIC **Pipeline configuration** (pseudo-YAML):
# MAGIC ```yaml
# MAGIC name: sales_pipeline
# MAGIC target: production.sales
# MAGIC libraries:
# MAGIC   - file: /pipelines/bronze.py
# MAGIC   - file: /pipelines/silver.py
# MAGIC   - file: /pipelines/gold.py
# MAGIC mode: production
# MAGIC compute: serverless
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Execution Modes
# MAGIC
# MAGIC #### Development Mode
# MAGIC
# MAGIC **Purpose**: Fast iteration during development
# MAGIC
# MAGIC **Behavior**:
# MAGIC * Reuses cluster across runs (faster startup)
# MAGIC * Verbose logging and debugging info
# MAGIC * Stops on first error (fail fast)
# MAGIC * Can run with partial pipelines
# MAGIC
# MAGIC **When to use**: Building and testing pipelines
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Production Mode
# MAGIC
# MAGIC **Purpose**: Reliable, repeatable execution
# MAGIC
# MAGIC **Behavior**:
# MAGIC * New cluster per run (isolation)
# MAGIC * Retries on transient failures
# MAGIC * Continues past recoverable errors
# MAGIC * Full table validation and lineage tracking
# MAGIC
# MAGIC **When to use**: Scheduled production workloads
# MAGIC
# MAGIC **Exam trap**: Development mode is NOT less reliable - it's optimized for speed. Production mode adds overhead for isolation and retry guarantees.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Table Naming and Storage
# MAGIC
# MAGIC **Table names**:
# MAGIC * Defined by function name: `@dlt.table def purchases()` creates table `purchases`
# MAGIC * Can override: `@dlt.table(name="fact_purchases")`
# MAGIC * Stored in target catalog.schema: `production.sales.purchases`
# MAGIC
# MAGIC **Storage locations**:
# MAGIC * **Managed tables**: SDP manages storage in Unity Catalog
# MAGIC * **External tables**: You specify path (rare, not recommended)
# MAGIC
# MAGIC **Exam scenario**: "You define @dlt.table def customer_orders() in a pipeline with target production.sales. What is the full table name?"
# MAGIC * **Answer**: `production.sales.customer_orders`

# COMMAND ----------

# DBTITLE 1,Streaming Tables
# MAGIC %md
# MAGIC ## Streaming Tables
# MAGIC
# MAGIC ### What Are Streaming Tables?
# MAGIC
# MAGIC Streaming tables process data continuously and incrementally. They read from streaming sources and maintain checkpoints to track progress.
# MAGIC
# MAGIC **Key characteristics**:
# MAGIC * **Append-only**: Rows are added, never updated or deleted
# MAGIC * **Continuous processing**: Runs until stopped (or until source exhausted)
# MAGIC * **Checkpointed**: Tracks processed data, survives failures
# MAGIC * **Exactly-once semantics**: Each source row processed exactly once
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Syntax
# MAGIC
# MAGIC **Python**:
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Streaming purchases from raw events",
# MAGIC     table_properties={"quality": "bronze"}
# MAGIC )
# MAGIC def streaming_purchases():
# MAGIC     return (
# MAGIC         dlt.read_stream("raw_events")
# MAGIC         .filter(col("event_type") == "purchase")
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **SQL**:
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE streaming_purchases
# MAGIC COMMENT "Streaming purchases from raw events"
# MAGIC AS SELECT *
# MAGIC FROM STREAM(raw_events)
# MAGIC WHERE event_type = 'purchase'
# MAGIC ```
# MAGIC
# MAGIC **Key elements**:
# MAGIC * `dlt.read_stream()` or `STREAM()`: Declares streaming source
# MAGIC * `@dlt.table` or `CREATE STREAMING TABLE`: Declares streaming table
# MAGIC * No `writeStream` call - SDP handles writes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Reading Sources
# MAGIC
# MAGIC #### From SDP Tables
# MAGIC
# MAGIC ```python
# MAGIC # Read another streaming table in same pipeline
# MAGIC dlt.read_stream("upstream_table")
# MAGIC ```
# MAGIC
# MAGIC #### From Unity Catalog Tables
# MAGIC
# MAGIC ```python
# MAGIC # Read external Delta table as stream
# MAGIC dlt.read_stream("catalog.schema.source_table")
# MAGIC ```
# MAGIC
# MAGIC #### From Cloud Files (Auto Loader)
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def raw_events():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .load("s3://bucket/events/")
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Exam trap**: You can use Auto Loader (cloudFiles) within SDP streaming tables. SDP and Auto Loader are complementary, not alternatives.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Streaming Semantics
# MAGIC
# MAGIC #### Append-Only
# MAGIC
# MAGIC Streaming tables cannot UPDATE or DELETE rows. All operations are INSERT.
# MAGIC
# MAGIC **What this means**:
# MAGIC * Filters: OK (rows that don't match filter are never written)
# MAGIC * Aggregations: OK (GROUP BY with append semantics)
# MAGIC * Joins: OK (stream-static and stream-stream joins supported)
# MAGIC * Updates: NOT OK (use materialized views or AUTO CDC for upserts)
# MAGIC
# MAGIC **Exam scenario**: "You need to update customer status in a streaming pipeline. Should you use a streaming table?"
# MAGIC * **Answer**: No - streaming tables are append-only. Use materialized view or AUTO CDC pattern.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Checkpointing
# MAGIC
# MAGIC SDP automatically maintains checkpoints for streaming tables:
# MAGIC * Tracks processed offsets/file positions
# MAGIC * Enables exactly-once processing
# MAGIC * Allows pipeline restart without reprocessing
# MAGIC
# MAGIC **Checkpoint location**: Managed by SDP in pipeline storage
# MAGIC
# MAGIC **Exam trap**: You do NOT manually manage checkpoints in SDP (unlike raw Structured Streaming). SDP handles checkpoint storage and recovery.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Aggregations in Streaming Tables
# MAGIC
# MAGIC Supported:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def hourly_sales():
# MAGIC     return (
# MAGIC         dlt.read_stream("purchases")
# MAGIC         .groupBy("store_id", window("purchase_time", "1 hour"))
# MAGIC         .agg(sum("amount").alias("total_sales"))
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Requirements for streaming aggregations**:
# MAGIC * Must use time-based windowing (for watermarking)
# MAGIC * Cannot use arbitrary GROUP BY without windows (unbounded state)
# MAGIC
# MAGIC **Exam scenario**: "Can you GROUP BY customer_id in a streaming table without windowing?"
# MAGIC * **Answer**: No - requires materialized view (batch processing) for arbitrary GROUP BY
# MAGIC * **Why**: Streaming aggregations need bounded state (windows provide bounds)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Joins in Streaming Tables
# MAGIC
# MAGIC #### Stream-Static Join
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def enriched_events():
# MAGIC     events = dlt.read_stream("raw_events")
# MAGIC     customers = dlt.read("customer_dim")  # Static table
# MAGIC     return events.join(customers, "customer_id")
# MAGIC ```
# MAGIC
# MAGIC **Behavior**: Joins streaming events with latest static table snapshot
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Stream-Stream Join
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def matched_events():
# MAGIC     stream_a = dlt.read_stream("events_a")
# MAGIC     stream_b = dlt.read_stream("events_b")
# MAGIC     return stream_a.join(
# MAGIC         stream_b,
# MAGIC         on="event_id",
# MAGIC         how="inner"
# MAGIC     ).withWatermark("event_time", "10 minutes")
# MAGIC ```
# MAGIC
# MAGIC **Behavior**: Joins two streams within time window (watermark bounds state)
# MAGIC
# MAGIC **Exam trap**: Stream-stream joins require watermarks to bound state. Without watermarks, state grows infinitely (OOM).

# COMMAND ----------

# DBTITLE 1,Materialized Views
# MAGIC %md
# MAGIC ## Materialized Views
# MAGIC
# MAGIC ### What Are Materialized Views?
# MAGIC
# MAGIC Materialized views process data in batch. On each pipeline run, they recompute the entire result from scratch (or incrementally based on upstream changes).
# MAGIC
# MAGIC **Key characteristics**:
# MAGIC * **Complete refresh**: Full recomputation (or incremental based on upstream)
# MAGIC * **Batch processing**: Runs on schedule or trigger, not continuously
# MAGIC * **Updates allowed**: Can overwrite, update, or delete rows
# MAGIC * **No checkpoints**: No streaming state to maintain
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Syntax
# MAGIC
# MAGIC **Python**:
# MAGIC ```python
# MAGIC @dlt.view
# MAGIC def customer_summary():
# MAGIC     return (
# MAGIC         dlt.read("purchases")
# MAGIC         .groupBy("customer_id")
# MAGIC         .agg(
# MAGIC             sum("amount").alias("total_spent"),
# MAGIC             count("*").alias("purchase_count")
# MAGIC         )
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **SQL**:
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW customer_summary
# MAGIC AS SELECT 
# MAGIC     customer_id,
# MAGIC     SUM(amount) AS total_spent,
# MAGIC     COUNT(*) AS purchase_count
# MAGIC FROM purchases
# MAGIC GROUP BY customer_id
# MAGIC ```
# MAGIC
# MAGIC **Key differences from streaming table**:
# MAGIC * Uses `@dlt.view` (not `@dlt.table`)
# MAGIC * Uses `dlt.read()` (not `dlt.read_stream()`)
# MAGIC * Uses `MATERIALIZED VIEW` (not `STREAMING TABLE`)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Streaming Table vs Materialized View
# MAGIC
# MAGIC | Aspect | Streaming Table | Materialized View |
# MAGIC |--------|-----------------|-------------------|
# MAGIC | **Processing** | Continuous | Batch |
# MAGIC | **Semantics** | Append-only | Full CRUD |
# MAGIC | **State** | Checkpoints | None |
# MAGIC | **Source** | Stream (STREAM()) | Batch (read()) |
# MAGIC | **Aggregation** | Windowed only | Arbitrary GROUP BY |
# MAGIC | **Updates** | No | Yes |
# MAGIC | **Performance** | Incremental (fast) | Full recompute (slower) |
# MAGIC
# MAGIC **Decision criteria**:
# MAGIC * Need continuous processing? → Streaming table
# MAGIC * Need to update/delete rows? → Materialized view
# MAGIC * Need arbitrary GROUP BY without windows? → Materialized view
# MAGIC * Append-only with incremental updates? → Streaming table
# MAGIC
# MAGIC **Exam trap**: You CAN read streaming tables in materialized views (batch read of accumulated data). But you CANNOT read materialized views as streams.

# COMMAND ----------

# DBTITLE 1,Expectations (Data Quality)
# MAGIC %md
# MAGIC ## Expectations (Data Quality)
# MAGIC
# MAGIC ### What Are Expectations?
# MAGIC
# MAGIC Expectations are data quality constraints that SDP enforces on your tables. They define rules that incoming data must satisfy.
# MAGIC
# MAGIC **Three enforcement modes**:
# MAGIC 1. **warn**: Log violations but keep rows
# MAGIC 2. **drop**: Drop violating rows silently
# MAGIC 3. **fail**: Fail the pipeline on any violation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Syntax
# MAGIC
# MAGIC **Python - warn mode**:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC @dlt.expect("valid_amount", "amount > 0")
# MAGIC def purchases():
# MAGIC     return dlt.read_stream("raw_events")
# MAGIC ```
# MAGIC
# MAGIC **Python - drop mode**:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC @dlt.expect_or_drop("valid_email", "email IS NOT NULL")
# MAGIC def customers():
# MAGIC     return dlt.read_stream("raw_customers")
# MAGIC ```
# MAGIC
# MAGIC **Python - fail mode**:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC @dlt.expect_or_fail("unique_id", "id IS NOT NULL")
# MAGIC def orders():
# MAGIC     return dlt.read_stream("raw_orders")
# MAGIC ```
# MAGIC
# MAGIC **SQL**:
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE purchases
# MAGIC (
# MAGIC   CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT valid_date EXPECT (purchase_date IS NOT NULL) ON VIOLATION FAIL,
# MAGIC   CONSTRAINT valid_customer EXPECT (customer_id IS NOT NULL)
# MAGIC )
# MAGIC AS SELECT * FROM STREAM(raw_events)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Enforcement Modes
# MAGIC
# MAGIC #### warn (default)
# MAGIC
# MAGIC ```python
# MAGIC @dlt.expect("positive_amount", "amount > 0")
# MAGIC ```
# MAGIC
# MAGIC **Behavior**:
# MAGIC * Violating rows are kept in table
# MAGIC * Violations logged to metrics
# MAGIC * Pipeline continues
# MAGIC
# MAGIC **When to use**: Exploratory analysis, understanding data quality issues
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### drop
# MAGIC
# MAGIC ```python
# MAGIC @dlt.expect_or_drop("valid_email", "email LIKE '%@%'")
# MAGIC ```
# MAGIC
# MAGIC **Behavior**:
# MAGIC * Violating rows are dropped
# MAGIC * Dropped row count tracked in metrics
# MAGIC * Pipeline continues
# MAGIC
# MAGIC **When to use**: Production pipelines where invalid data is expected but should be filtered
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### fail
# MAGIC
# MAGIC ```python
# MAGIC @dlt.expect_or_fail("non_null_id", "id IS NOT NULL")
# MAGIC ```
# MAGIC
# MAGIC **Behavior**:
# MAGIC * ANY violation stops the pipeline
# MAGIC * Pipeline marked as failed
# MAGIC * No data written if constraint violated
# MAGIC
# MAGIC **When to use**: Critical constraints that should never fail (schema validation, primary keys)
# MAGIC
# MAGIC **Exam trap**: fail mode is immediate - a single violating row stops the entire pipeline. Use cautiously.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Multiple Expectations
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC @dlt.expect("valid_amount", "amount > 0")
# MAGIC @dlt.expect_or_drop("valid_date", "order_date IS NOT NULL")
# MAGIC @dlt.expect_or_fail("non_null_id", "order_id IS NOT NULL")
# MAGIC def orders():
# MAGIC     return dlt.read_stream("raw_orders")
# MAGIC ```
# MAGIC
# MAGIC **Evaluation order**:
# MAGIC 1. All expectations evaluated for each row
# MAGIC 2. Most restrictive action taken (fail > drop > warn)
# MAGIC 3. If any expectation uses fail and fails, pipeline stops
# MAGIC
# MAGIC **Exam scenario**: "You have expect (warn), expect_or_drop, and expect_or_fail on same table. One row violates the drop constraint and the fail constraint. What happens?"
# MAGIC * **Answer**: Pipeline fails (fail is most restrictive)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Expectation Metrics
# MAGIC
# MAGIC SDP tracks expectation results:
# MAGIC * Total rows processed
# MAGIC * Rows passed/failed per expectation
# MAGIC * Percentage of violations
# MAGIC
# MAGIC **Access via**: Pipeline event logs and metrics UI
# MAGIC
# MAGIC **Exam relevance**: Questions about monitoring data quality or diagnosing pipeline issues often involve expectation metrics.

# COMMAND ----------

# DBTITLE 1,AUTO CDC
# MAGIC %md
# MAGIC ## AUTO CDC (Apply Changes)
# MAGIC
# MAGIC ### What Is AUTO CDC?
# MAGIC
# MAGIC AUTO CDC is SDP's built-in pattern for applying Change Data Capture (CDC) operations. It processes INSERT, UPDATE, and DELETE operations from CDC sources.
# MAGIC
# MAGIC **Use case**: Maintain current state of a table by applying a stream of changes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Syntax
# MAGIC
# MAGIC **Python**:
# MAGIC ```python
# MAGIC import dlt
# MAGIC
# MAGIC dlt.create_streaming_table("customers_current_state")
# MAGIC
# MAGIC dlt.apply_changes(
# MAGIC     target="customers_current_state",
# MAGIC     source="customers_cdc_stream",
# MAGIC     keys=["customer_id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type=1
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **SQL**:
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE customers_current_state;
# MAGIC
# MAGIC APPLY CHANGES INTO customers_current_state
# MAGIC FROM STREAM(customers_cdc_stream)
# MAGIC KEYS (customer_id)
# MAGIC SEQUENCE BY updated_at
# MAGIC STORED AS SCD TYPE 1
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Parameters
# MAGIC
# MAGIC #### target
# MAGIC
# MAGIC The table receiving changes. Must be created first:
# MAGIC ```python
# MAGIC dlt.create_streaming_table("target_table")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### source
# MAGIC
# MAGIC The CDC stream containing changes:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def cdc_stream():
# MAGIC     return dlt.read_stream("raw_cdc_events")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### keys
# MAGIC
# MAGIC Primary key columns for matching rows:
# MAGIC ```python
# MAGIC keys=["customer_id"]  # Single key
# MAGIC keys=["order_id", "line_item_id"]  # Composite key
# MAGIC ```
# MAGIC
# MAGIC **Behavior**: Rows with matching keys are updated; new keys are inserted
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### sequence_by
# MAGIC
# MAGIC Column that orders changes (timestamp or sequence number):
# MAGIC ```python
# MAGIC sequence_by="updated_at"  # Use timestamp
# MAGIC sequence_by="sequence_num"  # Use monotonic counter
# MAGIC ```
# MAGIC
# MAGIC **Purpose**: Handle out-of-order events (apply later changes, ignore earlier ones)
# MAGIC
# MAGIC **Exam trap**: If two changes have same key but different sequence_by values, only the LATEST (highest sequence_by) is applied. Earlier changes are ignored.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### stored_as_scd_type
# MAGIC
# MAGIC Slowly Changing Dimension type:
# MAGIC
# MAGIC **SCD Type 1** (current state only):
# MAGIC ```python
# MAGIC stored_as_scd_type=1
# MAGIC ```
# MAGIC * Only current values kept
# MAGIC * Updates overwrite previous values
# MAGIC * No history tracking
# MAGIC
# MAGIC **SCD Type 2** (full history):
# MAGIC ```python
# MAGIC stored_as_scd_type=2
# MAGIC ```
# MAGIC * All historical values kept
# MAGIC * Updates create new rows with validity dates
# MAGIC * Adds columns: `__START_AT`, `__END_AT`, `__CURRENT`
# MAGIC
# MAGIC **Exam scenario**: "You need to track customer address changes over time. Which SCD type?"
# MAGIC * **Answer**: SCD Type 2 (preserves history)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### CDC Event Format
# MAGIC
# MAGIC Source stream must include operation type:
# MAGIC
# MAGIC **Option 1: Explicit operation column**:
# MAGIC ```python
# MAGIC # Source has column: operation = 'INSERT', 'UPDATE', 'DELETE'
# MAGIC apply_changes(..., column_list=["customer_id", "name", "email"])
# MAGIC ```
# MAGIC
# MAGIC **Option 2: Debezium format**:
# MAGIC ```python
# MAGIC # Debezium CDC events with 'op' field
# MAGIC apply_changes(..., apply_as_deletes="op = 'd'", apply_as_truncates="op = 't'")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Delete Handling
# MAGIC
# MAGIC **With SCD Type 1**:
# MAGIC ```python
# MAGIC apply_changes(
# MAGIC     ...,
# MAGIC     apply_as_deletes="operation = 'DELETE'",
# MAGIC     stored_as_scd_type=1
# MAGIC )
# MAGIC ```
# MAGIC * Matching rows are deleted from target
# MAGIC
# MAGIC **With SCD Type 2**:
# MAGIC ```python
# MAGIC apply_changes(
# MAGIC     ...,
# MAGIC     apply_as_deletes="operation = 'DELETE'",
# MAGIC     stored_as_scd_type=2
# MAGIC )
# MAGIC ```
# MAGIC * `__END_AT` set to delete timestamp
# MAGIC * `__CURRENT` set to false
# MAGIC * Row remains in table (soft delete)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exam-Critical Points
# MAGIC
# MAGIC 1. **AUTO CDC requires streaming source**: Cannot use with materialized views
# MAGIC 2. **Target must be pre-created**: Use `create_streaming_table()` first
# MAGIC 3. **sequence_by handles out-of-order**: Later changes always win
# MAGIC 4. **SCD Type 2 adds columns**: `__START_AT`, `__END_AT`, `__CURRENT`
# MAGIC 5. **Deletes behave differently by SCD type**: Hard delete (Type 1) vs soft delete (Type 2)
# MAGIC
# MAGIC **Common trap**: Forgetting to create target table before apply_changes. Pipeline will fail.

# COMMAND ----------

# DBTITLE 1,Pipeline Configuration and Deployment
# MAGIC %md
# MAGIC ## Pipeline Configuration and Deployment
# MAGIC
# MAGIC ### Pipeline Settings
# MAGIC
# MAGIC When creating an SDP pipeline, configure:
# MAGIC
# MAGIC #### Target Catalog and Schema
# MAGIC
# MAGIC ```yaml
# MAGIC target: production.sales
# MAGIC ```
# MAGIC
# MAGIC All tables created in this pipeline write to `production.sales.*`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Libraries (Source Files)
# MAGIC
# MAGIC ```yaml
# MAGIC libraries:
# MAGIC   - file: /pipelines/bronze.py
# MAGIC   - file: /pipelines/silver.py
# MAGIC   - notebook: /Users/user@example.com/gold_aggregations
# MAGIC ```
# MAGIC
# MAGIC Pipeline loads table definitions from these files/notebooks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Compute Configuration
# MAGIC
# MAGIC **Serverless** (recommended):
# MAGIC ```yaml
# MAGIC compute: serverless
# MAGIC ```
# MAGIC * Managed compute, fast startup
# MAGIC * Auto-scaling
# MAGIC * Optimized for SDP workloads
# MAGIC
# MAGIC **Classic compute** (legacy):
# MAGIC ```yaml
# MAGIC cluster_config:
# MAGIC   node_type_id: i3.xlarge
# MAGIC   num_workers: 2
# MAGIC ```
# MAGIC * You manage cluster configuration
# MAGIC * Slower startup
# MAGIC * More control over resources
# MAGIC
# MAGIC **Exam preference**: Questions favor serverless unless specific cluster requirements mentioned.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Execution Mode
# MAGIC
# MAGIC **Development**:
# MAGIC ```yaml
# MAGIC mode: development
# MAGIC ```
# MAGIC * Fast iteration
# MAGIC * Reused cluster
# MAGIC * Fail fast on errors
# MAGIC
# MAGIC **Production**:
# MAGIC ```yaml
# MAGIC mode: production
# MAGIC ```
# MAGIC * Isolated runs
# MAGIC * Automatic retries
# MAGIC * Full validation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Storage Location
# MAGIC
# MAGIC ```yaml
# MAGIC storage: s3://bucket/pipeline-storage/
# MAGIC ```
# MAGIC
# MAGIC Where SDP stores:
# MAGIC * Checkpoints
# MAGIC * Metadata
# MAGIC * Event logs
# MAGIC
# MAGIC **Default**: Managed by Databricks in Unity Catalog location
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Triggering Pipelines
# MAGIC
# MAGIC #### Triggered (on-demand)
# MAGIC
# MAGIC ```yaml
# MAGIC trigger: none
# MAGIC ```
# MAGIC
# MAGIC Pipeline runs when manually started or triggered by API/job.
# MAGIC
# MAGIC **When to use**: Development, testing, or event-driven workflows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scheduled
# MAGIC
# MAGIC ```yaml
# MAGIC schedule:
# MAGIC   quartz_cron: "0 0 * * * ?"  # Hourly
# MAGIC ```
# MAGIC
# MAGIC Pipeline runs on cron schedule.
# MAGIC
# MAGIC **When to use**: Regular batch processing (hourly, daily)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Continuous
# MAGIC
# MAGIC ```yaml
# MAGIC continuous: true
# MAGIC ```
# MAGIC
# MAGIC Pipeline runs continuously, processing new data as it arrives.
# MAGIC
# MAGIC **When to use**: Real-time streaming pipelines
# MAGIC
# MAGIC **Exam trap**: Continuous mode only makes sense with streaming tables. Materialized views in continuous mode will recompute repeatedly (wasteful).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Unity Catalog Integration
# MAGIC
# MAGIC SDP integrates with Unity Catalog:
# MAGIC
# MAGIC **Table registration**:
# MAGIC * All tables automatically registered in target catalog/schema
# MAGIC * Inherits catalog-level governance (tags, permissions, lineage)
# MAGIC
# MAGIC **Permissions required**:
# MAGIC * `USE CATALOG` on target catalog
# MAGIC * `USE SCHEMA` on target schema
# MAGIC * `CREATE TABLE` on target schema
# MAGIC
# MAGIC **Lineage tracking**:
# MAGIC * SDP automatically tracks table dependencies
# MAGIC * Visible in Unity Catalog lineage UI
# MAGIC
# MAGIC **Exam scenario**: "A pipeline with target production.sales fails with 'permission denied'. What permission is missing?"
# MAGIC * **Answer**: `CREATE TABLE` on `production.sales` schema (or `USE SCHEMA`/`USE CATALOG` on parent objects)

# COMMAND ----------

# DBTITLE 1,Common Exam Traps and Decision Patterns
# MAGIC %md
# MAGIC ## Common Exam Traps and Decision Patterns
# MAGIC
# MAGIC ### Decision Tree: Streaming Table vs Materialized View
# MAGIC
# MAGIC ```
# MAGIC START
# MAGIC   |
# MAGIC   v
# MAGIC [Do you need continuous/incremental processing?]
# MAGIC   |
# MAGIC   +-- Yes --> [Is data append-only?]
# MAGIC   |             |
# MAGIC   |             +-- Yes --> STREAMING TABLE
# MAGIC   |             +-- No (updates/deletes) --> [Need history?]
# MAGIC   |                                             |
# MAGIC   |                                             +-- No --> AUTO CDC (SCD Type 1)
# MAGIC   |                                             +-- Yes --> AUTO CDC (SCD Type 2)
# MAGIC   |
# MAGIC   +-- No (batch is fine) --> [Need arbitrary GROUP BY?]
# MAGIC                                 |
# MAGIC                                 +-- Yes --> MATERIALIZED VIEW
# MAGIC                                 +-- No --> STREAMING TABLE (if source is streaming)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### High-Frequency Exam Traps
# MAGIC
# MAGIC #### Trap 1: Product Name Confusion
# MAGIC
# MAGIC **Wrong**: "Use Delta Live Tables for this pipeline"
# MAGIC **Correct**: "Use Lakeflow Spark Declarative Pipelines (SDP)"
# MAGIC
# MAGIC The product was renamed. Exam questions use the new name.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Trap 2: Streaming Tables Can Update
# MAGIC
# MAGIC **Wrong**: "Use streaming table with UPDATE operations"
# MAGIC **Reality**: Streaming tables are append-only. Updates require materialized views or AUTO CDC.
# MAGIC
# MAGIC **Exam signal**: Any mention of "update", "delete", "current state" → NOT a streaming table
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Trap 3: Materialized Views Are Not Incremental
# MAGIC
# MAGIC **Wrong**: "Materialized views incrementally process only new rows"
# MAGIC **Reality**: Materialized views recompute from scratch (unless you manually implement incremental logic)
# MAGIC
# MAGIC **Exam signal**: "Only process new data" → Streaming table, NOT materialized view
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Trap 4: Expectations with fail Stop Pipeline
# MAGIC
# MAGIC **Wrong**: "Use expect_or_fail to log critical violations while keeping pipeline running"
# MAGIC **Reality**: expect_or_fail stops the entire pipeline on first violation
# MAGIC
# MAGIC **Decision**:
# MAGIC * Need to keep running? → expect or expect_or_drop
# MAGIC * Must never violate? → expect_or_fail
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Trap 5: AUTO CDC Requires Pre-Created Target
# MAGIC
# MAGIC **Wrong**:
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="customers",  # Target doesn't exist yet
# MAGIC     source="cdc_stream",
# MAGIC     keys=["id"]
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Correct**:
# MAGIC ```python
# MAGIC dlt.create_streaming_table("customers")  # Create first
# MAGIC
# MAGIC dlt.apply_changes(
# MAGIC     target="customers",
# MAGIC     source="cdc_stream",
# MAGIC     keys=["id"]
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Trap 6: SCD Type 2 Adds Columns
# MAGIC
# MAGIC **Wrong**: "SCD Type 2 just keeps old rows"
# MAGIC **Reality**: SCD Type 2 adds `__START_AT`, `__END_AT`, `__CURRENT` columns
# MAGIC
# MAGIC **Exam question**: "What columns are added by SCD Type 2?" → Answer must include these three
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Trap 7: Continuous Mode with Materialized Views
# MAGIC
# MAGIC **Wrong**: "Run pipeline continuously with only materialized views"
# MAGIC **Reality**: Continuous mode with materialized views causes repeated full recomputes (wasteful)
# MAGIC
# MAGIC **When continuous makes sense**: Pipeline has streaming tables that process data as it arrives
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Trap 8: Reading Materialized Views as Streams
# MAGIC
# MAGIC **Wrong**:
# MAGIC ```python
# MAGIC dlt.read_stream("materialized_view_table")  # Can't stream from materialized view
# MAGIC ```
# MAGIC
# MAGIC **Reality**: Materialized views are batch tables. Can't read as stream.
# MAGIC
# MAGIC **What works**:
# MAGIC * Read streaming table in materialized view: `dlt.read("streaming_table")` ✓
# MAGIC * Read materialized view as batch: `dlt.read("materialized_view")` ✓
# MAGIC * Read materialized view as stream: NOT SUPPORTED ✗
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Trap 9: SDP vs Structured Streaming Confusion
# MAGIC
# MAGIC **Exam question**: "When should you use SDP vs raw Structured Streaming?"
# MAGIC
# MAGIC **Answer**:
# MAGIC * **Use SDP**: Multi-stage pipelines with dependencies, need automatic orchestration, data quality constraints
# MAGIC * **Use Structured Streaming**: Single-stage custom transformations, need low-level control, custom state management
# MAGIC
# MAGIC **Signal phrases**:
# MAGIC * "Multiple dependent tables" → SDP
# MAGIC * "Data quality constraints" → SDP (expectations)
# MAGIC * "Custom stateful aggregation" → Structured Streaming
# MAGIC * "Need full control over checkpoints" → Structured Streaming
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Trap 10: SDP Cannot Ingest External Data
# MAGIC
# MAGIC **Wrong**: "Use SDP to ingest from MySQL database"
# MAGIC **Reality**: SDP transforms data already in Databricks. Use Lakeflow Connect to ingest.
# MAGIC
# MAGIC **Correct workflow**: Lakeflow Connect (ingest MySQL) → SDP (transform through bronze/silver/gold)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exam Study Checklist
# MAGIC
# MAGIC Before the exam, confirm you can:
# MAGIC
# MAGIC - [ ] Explain streaming table vs materialized view (3+ differences)
# MAGIC - [ ] Write syntax for both Python and SQL streaming tables
# MAGIC - [ ] List all three expectation modes (warn, drop, fail) and when to use each
# MAGIC - [ ] Explain AUTO CDC parameters (target, source, keys, sequence_by, stored_as_scd_type)
# MAGIC - [ ] Describe SCD Type 1 vs Type 2 differences
# MAGIC - [ ] Identify when SDP is appropriate vs alternatives (Structured Streaming, Lakeflow Connect)
# MAGIC - [ ] List columns added by SCD Type 2 (`__START_AT`, `__END_AT`, `__CURRENT`)
# MAGIC - [ ] Explain development vs production mode differences
# MAGIC - [ ] Know required Unity Catalog permissions for SDP pipelines
# MAGIC - [ ] Recognize common mistakes (streaming tables can't update, materialized views aren't incremental)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Quick Reference Table
# MAGIC
# MAGIC | Feature | Streaming Table | Materialized View | AUTO CDC |
# MAGIC |---------|----------------|-------------------|----------|
# MAGIC | **Processing** | Continuous | Batch | Continuous |
# MAGIC | **Syntax** | @dlt.table + read_stream | @dlt.view + read | apply_changes |
# MAGIC | **Updates** | No (append-only) | Yes | Yes |
# MAGIC | **State** | Checkpoint | None | Checkpoint |
# MAGIC | **GROUP BY** | Windowed only | Arbitrary | N/A |
# MAGIC | **Use case** | Incremental ETL | Aggregations | CDC ingestion |
# MAGIC | **History** | Append history | No history | SCD Type 1 or 2 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Memory Aids
# MAGIC
# MAGIC **Streaming = Stream, append-only, checkpoint**
# MAGIC **Materialized = Batch, any operation, recompute**
# MAGIC **AUTO CDC = Changes, SCD, sequence_by**
# MAGIC
# MAGIC **Expectations**:
# MAGIC * **warn** = Watch (log only)
# MAGIC * **drop** = Delete (remove bad rows)
# MAGIC * **fail** = Fatal (stop everything)
# MAGIC
# MAGIC **SCD Types**:
# MAGIC * **Type 1** = Current state (one row per key)
# MAGIC * **Type 2** = Full history (multiple rows per key with dates)

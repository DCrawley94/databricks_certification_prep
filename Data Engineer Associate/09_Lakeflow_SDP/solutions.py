# Databricks notebook source
# DBTITLE 1,Topic 9: Lakeflow SDP - Solutions
# MAGIC %md
# MAGIC # Topic 9: Lakeflow Spark Declarative Pipelines - Solutions
# MAGIC
# MAGIC ## How to Use This Notebook
# MAGIC
# MAGIC This notebook provides complete solutions to all practice tasks. Each solution includes:
# MAGIC * **Complete answer**: The correct response with code where applicable
# MAGIC * **Explanation**: Why this answer is correct and how it works
# MAGIC * **Exam tip**: Common mistakes and traps to avoid
# MAGIC * **Memory aid**: Mnemonics or patterns to remember for the exam
# MAGIC
# MAGIC ## Structure
# MAGIC
# MAGIC * **Exercises 1-15**: Detailed solutions with explanations
# MAGIC * **MCQs 1-5**: Correct answers with justification
# MAGIC * **Challenges 1-2**: Complete solutions for complex scenarios
# MAGIC * **Applieds 1-2**: Decision tree walkthroughs
# MAGIC * **Study Guide**: High-frequency topics and exam strategy
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Study Approach
# MAGIC
# MAGIC 1. **Attempt first**: Try each exercise before viewing the solution
# MAGIC 2. **Compare thoroughly**: Even if your answer is correct, read the exam tips
# MAGIC 3. **Focus on traps**: Common mistakes are what separate exam success from failure
# MAGIC 4. **Practice syntax**: Write code by hand to internalize Python and SQL patterns
# MAGIC 5. **Review dependencies**: Understand table type selection, reading patterns, and execution flow

# COMMAND ----------

# DBTITLE 1,Exercise 1 Solution: Streaming Table vs Materialized View Selection
# MAGIC %md
# MAGIC ## Exercise 1 Solution: Streaming Table vs Materialized View Selection
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. Requirement 1 (continuously ingest JSON from S3):
# MAGIC    - Table type: Streaming table
# MAGIC    - Justification: Continuous ingestion from files requires streaming. Auto Loader 
# MAGIC      provides incremental processing. Events never updated (append-only). Need real-time
# MAGIC      processing as files arrive.
# MAGIC    - Key syntax: dlt.read_stream() with cloudFiles format
# MAGIC
# MAGIC 2. Requirement 2 (daily sales totals):
# MAGIC    - Table type: Materialized view
# MAGIC    - Justification: Aggregation (GROUP BY store) recomputed once per day. Source is 
# MAGIC      streaming but transformation is batch. No need for continuous processing. Batch 
# MAGIC      aggregation over streaming source.
# MAGIC    - Key syntax: dlt.read() (batch read) with @dlt.view decorator
# MAGIC
# MAGIC 3. Requirement 3 (filter clickstream to purchase events):
# MAGIC    - Table type: Streaming table
# MAGIC    - Justification: Need real-time filtering as events arrive. Source is streaming, 
# MAGIC      output must be streaming for downstream real-time processing. Filtering doesn't 
# MAGIC      change streaming nature.
# MAGIC    - Key syntax: dlt.read_stream() with filter(), @dlt.table decorator
# MAGIC
# MAGIC 4. Requirement 4 (maintain current customer balances from CDC):
# MAGIC    - Table type: Streaming table with AUTO CDC
# MAGIC    - Justification: CDC stream requires streaming ingestion. AUTO CDC (apply_changes) 
# MAGIC      maintains current state from update stream. Need to handle INSERT, UPDATE, DELETE 
# MAGIC      as they arrive.
# MAGIC    - Key syntax: dlt.create_streaming_table() + dlt.apply_changes() with SCD Type 1
# MAGIC ```
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Streaming table criteria**:
# MAGIC * Source data arrives continuously (files, Kafka, CDC feeds)
# MAGIC * Need to process data as it arrives (low latency requirement)
# MAGIC * Append-only operations OR streaming CDC processing
# MAGIC * Downstream consumers need streaming source
# MAGIC
# MAGIC **Materialized view criteria**:
# MAGIC * Batch aggregations or transformations
# MAGIC * Results recomputed periodically (not continuously)
# MAGIC * Can read streaming sources in batch mode
# MAGIC * Query pattern favors pre-computed results
# MAGIC
# MAGIC **Key insight for Requirement 2**:
# MAGIC Even though the SOURCE is streaming (transactions), the TRANSFORMATION is batch 
# MAGIC (daily aggregation). Materialized views can batch-read streaming tables using dlt.read().
# MAGIC
# MAGIC **AUTO CDC considerations**:
# MAGIC When CDC is involved, you MUST use streaming table + apply_changes. Materialized views 
# MAGIC cannot handle CDC operations (INSERT/UPDATE/DELETE logic).
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Using materialized view for filtering streaming data.
# MAGIC
# MAGIC **Wrong reasoning**: "Filtering is simple, so use materialized view."
# MAGIC
# MAGIC **Why wrong**: If downstream tables need to process results in real-time, they need a 
# MAGIC streaming source. Materialized views are batch-only.
# MAGIC
# MAGIC **Exam signal phrases**:
# MAGIC * "As data arrives" / "continuously" / "real-time" → Streaming table
# MAGIC * "Once per day" / "periodic aggregation" / "batch computation" → Materialized view
# MAGIC * "CDC" / "maintain current state" / "updates and deletes" → AUTO CDC (streaming table)
# MAGIC
# MAGIC **Another trap**: "Calculate totals from streaming source" could be EITHER streaming or 
# MAGIC materialized view. The differentiator is:
# MAGIC * Continuous recalculation needed? → Streaming table with windowing
# MAGIC * Periodic batch aggregation? → Materialized view
# MAGIC
# MAGIC ### Memory Aid
# MAGIC
# MAGIC **STREAM acronym**:
# MAGIC * **S**ource: Continuous arrival
# MAGIC * **T**ime: Need low latency
# MAGIC * **R**eal-time: Downstream needs streaming
# MAGIC * **E**vent-driven: Process on arrival
# MAGIC * **A**ppend: Data is append-only OR CDC
# MAGIC * **M**onitoring: State accumulation acceptable (with windows)
# MAGIC
# MAGIC **BATCH acronym** (materialized views):
# MAGIC * **B**atch reads: dlt.read() not read_stream()
# MAGIC * **A**ggregations: Arbitrary GROUP BY allowed
# MAGIC * **T**imed: Periodic recomputation
# MAGIC * **C**omplete: Full re-scan of source
# MAGIC * **H**istorical: No need for real-time processing
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Reference overview:**
# MAGIC See Topic 9, sections on Streaming table fundamentals, Materialized View patterns, and table type selection decision trees.

# COMMAND ----------

# DBTITLE 1,Exercise 2 Solution: Expectation Configuration
# MAGIC %md
# MAGIC ## Exercise 2 Solution: Expectation Configuration
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC **Rule 1: Order amount must be positive (drop invalid)**
# MAGIC ```python
# MAGIC @dlt.expect_or_drop("positive_amount", "amount > 0")
# MAGIC ```
# MAGIC * Decorator: @dlt.expect_or_drop
# MAGIC * Name: "positive_amount"
# MAGIC * Expression: "amount > 0"
# MAGIC * Behavior: Rows with amount <= 0 are silently dropped, not written to target table
# MAGIC
# MAGIC **Rule 2: Customer ID must not be null (fail pipeline)**
# MAGIC ```python
# MAGIC @dlt.expect_or_fail("customer_id_required", "customer_id IS NOT NULL")
# MAGIC ```
# MAGIC * Decorator: @dlt.expect_or_fail
# MAGIC * Name: "customer_id_required"
# MAGIC * Expression: "customer_id IS NOT NULL"
# MAGIC * Behavior: Any row with null customer_id causes entire pipeline to fail immediately
# MAGIC
# MAGIC **Rule 3: Order date within last 2 years (log but keep)**
# MAGIC ```python
# MAGIC @dlt.expect("recent_order", "order_date >= current_date() - INTERVAL 2 YEARS")
# MAGIC ```
# MAGIC * Decorator: @dlt.expect
# MAGIC * Name: "recent_order"
# MAGIC * Expression: "order_date >= current_date() - INTERVAL 2 YEARS"
# MAGIC * Behavior: Violations logged to event log, but all rows (valid and invalid) written to target
# MAGIC
# MAGIC **Bonus question answer**: If a row violates both Rule 2 (fail) and Rule 1 (drop):
# MAGIC * Rule 2 takes precedence because @dlt.expect_or_fail stops the pipeline immediately
# MAGIC * The pipeline fails before the drop logic is evaluated
# MAGIC * Order of decorators does not matter - fail always takes precedence over drop
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Three expectation types**:
# MAGIC
# MAGIC 1. **@dlt.expect**: Monitor only
# MAGIC    * Logs violations to event log
# MAGIC    * All rows written (valid + invalid)
# MAGIC    * Use for: Non-critical data quality issues, monitoring trends
# MAGIC    * Metrics available in event log for alerting
# MAGIC
# MAGIC 2. **@dlt.expect_or_drop**: Filter silently
# MAGIC    * Drops violating rows
# MAGIC    * Valid rows written to target
# MAGIC    * Violations logged
# MAGIC    * Use for: Filters, data cleanup, known data quality issues
# MAGIC    * Does NOT fail pipeline
# MAGIC
# MAGIC 3. **@dlt.expect_or_fail**: Hard requirement
# MAGIC    * Any violation fails entire pipeline immediately
# MAGIC    * No data written if constraint violated
# MAGIC    * Use for: Critical business rules, schema requirements
# MAGIC    * Pipeline must be fixed before continuing
# MAGIC
# MAGIC **Multiple expectations interaction**:
# MAGIC * All expectations evaluated independently
# MAGIC * expect_or_fail takes precedence (fails immediately)
# MAGIC * expect_or_drop filters rows before downstream processing
# MAGIC * expect logs but doesn't affect data flow
# MAGIC
# MAGIC **Constraint expression**:
# MAGIC * Must be valid Spark SQL boolean expression
# MAGIC * Can use any Spark SQL function
# MAGIC * Cannot reference other tables (use joins in query instead)
# MAGIC * Expression evaluated per row
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Confusing decorator order priority
# MAGIC
# MAGIC **Wrong assumption**: "Decorators stack in order, so if I put @expect_or_drop first, it runs before @expect_or_fail"
# MAGIC
# MAGIC **Why wrong**: expect_or_fail ALWAYS takes precedence regardless of decorator order. If any row violates a fail constraint, pipeline stops immediately.
# MAGIC
# MAGIC **Exam signal phrases**:
# MAGIC * "Must be valid" / "Required" / "Cannot be null" → expect_or_fail
# MAGIC * "Filter out" / "Remove invalid" / "Silently drop" → expect_or_drop  
# MAGIC * "Monitor" / "Track violations" / "Log but keep" → expect
# MAGIC
# MAGIC **Tricky scenario**: "Invalid rows should be dropped, but if more than 10% of rows are invalid, fail the pipeline."
# MAGIC
# MAGIC This requires BOTH:
# MAGIC ```python
# MAGIC @dlt.expect_or_drop("valid_data", "amount > 0")
# MAGIC @dlt.expect_or_fail("acceptable_quality", "_rescued_data IS NULL OR size(_rescued_data) < 0.1 * count(*)")
# MAGIC ```
# MAGIC But this is ADVANCED - not testable at Associate level.
# MAGIC
# MAGIC ### Memory Aid
# MAGIC
# MAGIC **Expectation severity levels** (least to most strict):
# MAGIC 1. **expect** = "FYI" (For Your Information) - just log it
# MAGIC 2. **expect_or_drop** = "FILTER" - remove bad rows
# MAGIC 3. **expect_or_fail** = "STOP" - pipeline cannot continue
# MAGIC
# MAGIC **Decorator naming pattern**:
# MAGIC * No suffix (_expect_) = Passive monitoring
# MAGIC * "or_drop" = Active filtering
# MAGIC * "or_fail" = Hard requirement
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Reference overview:**
# MAGIC See Topic 9 sections on Data Quality Expectations and Constraint Enforcement

# COMMAND ----------

# DBTITLE 1,Exercise 3 Solution: AUTO CDC Configuration (SCD Type 1)
# MAGIC %md
# MAGIC ## Exercise 3 Solution: AUTO CDC Configuration (SCD Type 1)
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC
# MAGIC # Step 1: Create target streaming table
# MAGIC dlt.create_streaming_table("customers")
# MAGIC
# MAGIC # Step 2: Apply CDC changes
# MAGIC dlt.apply_changes(
# MAGIC     target="customers",                    # Target table name
# MAGIC     source="raw_customer_cdc",            # Source CDC stream
# MAGIC     keys=["customer_id"],                 # Primary key(s)
# MAGIC     sequence_by="updated_at",             # Ordering column
# MAGIC     stored_as_scd_type=1                  # SCD Type 1: current state only
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Parameter explanations**:
# MAGIC
# MAGIC 1. **target**: Name of the target table where current customer state is maintained
# MAGIC    * Must be created first with dlt.create_streaming_table()
# MAGIC    * Cannot be a materialized view
# MAGIC
# MAGIC 2. **source**: Name of the source CDC stream
# MAGIC    * Must be a streaming table or table name readable by dlt
# MAGIC    * Contains CDC events (INSERT, UPDATE, DELETE operations)
# MAGIC
# MAGIC 3. **keys**: List of columns forming the primary key
# MAGIC    * Used to identify which row to update
# MAGIC    * Multiple columns allowed: keys=["customer_id", "region"]
# MAGIC    * All key columns must be non-null
# MAGIC
# MAGIC 4. **sequence_by**: Column used to order CDC events
# MAGIC    * Typically a timestamp (updated_at, event_time)
# MAGIC    * Later events (higher values) overwrite earlier events
# MAGIC    * Critical for correct ordering when events arrive out of order
# MAGIC
# MAGIC 5. **stored_as_scd_type=1**: Maintain current state only
# MAGIC    * Type 1: Single row per key, always current state
# MAGIC    * Type 2: Multiple rows per key with validity periods (history tracking)
# MAGIC
# MAGIC **Question 3 answer**: When two CDC events have the same customer_id but different updated_at:
# MAGIC * Events ordered by sequence_by (updated_at)
# MAGIC * Latest event wins (highest updated_at)
# MAGIC * Older event applied first, then newer event overwrites
# MAGIC * Final table state reflects the most recent updated_at value
# MAGIC
# MAGIC **Question 4 answer**: DELETE operations in SCD Type 1:
# MAGIC * Row is physically deleted from target table
# MAGIC * No history retained
# MAGIC * If you need to track deletions, use SCD Type 2 (adds validity flags)
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **AUTO CDC workflow**:
# MAGIC
# MAGIC 1. CDC stream contains events:
# MAGIC    ```
# MAGIC    {customer_id: 1, name: "Alice", updated_at: "2024-01-01T10:00:00", _change_type: "insert"}
# MAGIC    {customer_id: 1, name: "Alice Smith", updated_at: "2024-01-02T15:00:00", _change_type: "update"}
# MAGIC    {customer_id: 2, name: "Bob", updated_at: "2024-01-01T11:00:00", _change_type: "insert"}
# MAGIC    {customer_id: 1, updated_at: "2024-01-03T09:00:00", _change_type: "delete"}
# MAGIC    ```
# MAGIC
# MAGIC 2. apply_changes processes events in sequence_by order:
# MAGIC    * Event 1 (customer_id=1, insert) → Row added
# MAGIC    * Event 3 (customer_id=2, insert) → Row added
# MAGIC    * Event 2 (customer_id=1, update) → Row updated (name changed)
# MAGIC    * Event 4 (customer_id=1, delete) → Row deleted
# MAGIC
# MAGIC 3. Final table state:
# MAGIC    ```
# MAGIC    customer_id | name | updated_at
# MAGIC    2           | Bob  | 2024-01-01T11:00:00
# MAGIC    ```
# MAGIC    * customer_id=1 deleted entirely (SCD Type 1 doesn't retain history)
# MAGIC
# MAGIC **SCD Type 1 vs Type 2**:
# MAGIC
# MAGIC | Feature | SCD Type 1 | SCD Type 2 |
# MAGIC |---------|-----------|------------|
# MAGIC | History | No (current state only) | Yes (full history) |
# MAGIC | Rows per key | 1 | Multiple (one per change) |
# MAGIC | DELETE behavior | Physical delete | Soft delete (end_date set) |
# MAGIC | Columns added | None | __START_AT, __END_AT, __DELETED |
# MAGIC | Use case | Current state, no audit | Historical tracking, compliance |
# MAGIC
# MAGIC **Key requirements for AUTO CDC**:
# MAGIC * Source MUST be streaming (cannot be materialized view)
# MAGIC * Target MUST be created with create_streaming_table
# MAGIC * keys must uniquely identify rows
# MAGIC * sequence_by must provide total ordering
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Using AUTO CDC when simple streaming table suffices
# MAGIC
# MAGIC **Wrong reasoning**: "I have a stream of events, so I should use apply_changes"
# MAGIC
# MAGIC **Why wrong**: apply_changes is for CDC (INSERT/UPDATE/DELETE), not append-only event streams.
# MAGIC
# MAGIC Use AUTO CDC only when:
# MAGIC * Source has UPDATE or DELETE operations
# MAGIC * Need to maintain "current state" or "history" of entities
# MAGIC * Events can arrive out of order (sequence_by handles this)
# MAGIC
# MAGIC Do NOT use AUTO CDC when:
# MAGIC * Append-only events (use regular streaming table)
# MAGIC * No updates or deletes in source
# MAGIC * Simple filtering or transformation (use dlt.read_stream)
# MAGIC
# MAGIC **Exam signal phrases**:
# MAGIC * "CDC stream" / "database changes" / "updates and deletes" → AUTO CDC
# MAGIC * "Current state" / "latest value" → SCD Type 1
# MAGIC * "Historical changes" / "audit trail" → SCD Type 2
# MAGIC * "Event stream" / "append-only" → Streaming table (not AUTO CDC)
# MAGIC
# MAGIC ### Memory Aid
# MAGIC
# MAGIC **AUTO CDC = APPLY acronym**:
# MAGIC * **A**lways use create_streaming_table first
# MAGIC * **P**rimary key specified in keys parameter
# MAGIC * **P**rocesses INSERT, UPDATE, DELETE
# MAGIC * **L**atest event wins (sequence_by)
# MAGIC * **Y**ields current state (Type 1) or history (Type 2)
# MAGIC
# MAGIC **SCD Type memory**:
# MAGIC * Type **1** = **1** row per key (current only)
# MAGIC * Type **2** = **2+** rows per key (history)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Reference overview:**
# MAGIC See Topic 9 sections on Change Data Capture and AUTO CDC

# COMMAND ----------

# DBTITLE 1,Exercise 4 Solution: Python to SQL Syntax Conversion
# MAGIC %md
# MAGIC ## Exercise 4 Solution: Python to SQL Syntax Conversion
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_purchases (
# MAGIC   CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT valid_date EXPECT (event_date IS NOT NULL) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC COMMENT 'Filtered purchase events'
# MAGIC TBLPROPERTIES ('quality' = 'silver')
# MAGIC AS
# MAGIC SELECT 
# MAGIC   event_id,
# MAGIC   customer_id,
# MAGIC   amount,
# MAGIC   event_date
# MAGIC FROM STREAM(bronze_events)
# MAGIC WHERE event_type = 'purchase'
# MAGIC ```
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Python to SQL mappings**:
# MAGIC
# MAGIC 1. **Table decorator**:
# MAGIC    * Python: `@dlt.table(comment="...", table_properties={"key": "value"})`
# MAGIC    * SQL: `CREATE OR REFRESH STREAMING TABLE ... COMMENT '...' TBLPROPERTIES ('key' = 'value')`
# MAGIC
# MAGIC 2. **Expectations**:
# MAGIC    * Python: `@dlt.expect_or_drop("name", "expression")`
# MAGIC    * SQL: `CONSTRAINT name EXPECT (expression) ON VIOLATION DROP ROW`
# MAGIC    * Note: SQL constraints defined INSIDE the CREATE TABLE statement, before AS
# MAGIC
# MAGIC 3. **Streaming read**:
# MAGIC    * Python: `dlt.read_stream("table_name")`
# MAGIC    * SQL: `FROM STREAM(table_name)`
# MAGIC    * Note: STREAM() function wraps the table name in SQL
# MAGIC
# MAGIC 4. **Filter**:
# MAGIC    * Python: `.filter(col("event_type") == "purchase")`
# MAGIC    * SQL: `WHERE event_type = 'purchase'`
# MAGIC
# MAGIC 5. **Select columns**:
# MAGIC    * Python: `.select("col1", "col2", "col3")`
# MAGIC    * SQL: `SELECT col1, col2, col3`
# MAGIC
# MAGIC **Expectation syntax in SQL**:
# MAGIC * Format: `CONSTRAINT <name> EXPECT (<expression>) ON VIOLATION <action>`
# MAGIC * Actions:
# MAGIC    * `ON VIOLATION DROP ROW` = @dlt.expect_or_drop
# MAGIC    * `ON VIOLATION FAIL UPDATE` = @dlt.expect_or_fail
# MAGIC    * No action clause = @dlt.expect (monitor only)
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC
# MAGIC 1. **Wrong**: Putting constraints after AS:
# MAGIC    ```sql
# MAGIC    CREATE OR REFRESH STREAMING TABLE silver_purchases
# MAGIC    AS SELECT ...
# MAGIC    CONSTRAINT valid_amount EXPECT (amount > 0)  -- WRONG POSITION
# MAGIC    ```
# MAGIC    **Correct**: Constraints go BEFORE AS
# MAGIC
# MAGIC 2. **Wrong**: Using Python-style function names in SQL:
# MAGIC    ```sql
# MAGIC    FROM dlt.read_stream('bronze_events')  -- WRONG
# MAGIC    ```
# MAGIC    **Correct**: `FROM STREAM(bronze_events)`
# MAGIC
# MAGIC 3. **Wrong**: Forgetting REFRESH keyword:
# MAGIC    ```sql
# MAGIC    CREATE STREAMING TABLE ...  -- WRONG (missing REFRESH)
# MAGIC    ```
# MAGIC    **Correct**: `CREATE OR REFRESH STREAMING TABLE`
# MAGIC    * REFRESH allows idempotent re-execution
# MAGIC
# MAGIC **Exam signal**: If question asks "convert Python to SQL", check these elements:
# MAGIC * Streaming read: STREAM() not read_stream
# MAGIC * Constraints: Before AS clause
# MAGIC * CREATE OR REFRESH: Include both keywords
# MAGIC * Table properties: TBLPROPERTIES ('key' = 'value')
# MAGIC
# MAGIC ### Memory Aid
# MAGIC
# MAGIC **SQL SDP structure** (top to bottom):
# MAGIC 1. CREATE OR REFRESH STREAMING TABLE name
# MAGIC 2. (CONSTRAINTS go here)
# MAGIC 3. COMMENT 'description'
# MAGIC 4. TBLPROPERTIES ('key' = 'value')
# MAGIC 5. AS
# MAGIC 6. SELECT ...
# MAGIC 7. FROM STREAM(source)
# MAGIC 8. WHERE ...
# MAGIC
# MAGIC Mnemonic: **CCTASFW** = "Create, Constraints, Then AS, From, Where"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Reference overview:**
# MAGIC See Topic 9 sections on Python vs SQL Syntax

# COMMAND ----------

# DBTITLE 1,Exercise 5 Solution: Pipeline Dependency Graph
# MAGIC %md
# MAGIC ## Exercise 5 Solution: Pipeline Dependency Graph and Execution Order
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC **Question 1: Dependency graph**
# MAGIC
# MAGIC ```
# MAGIC bronze_events (root)
# MAGIC     │
# MAGIC     ├── silver_purchases
# MAGIC     │       │
# MAGIC     │       ├── gold_daily_summary
# MAGIC     │       └── gold_customer_metrics
# MAGIC     │
# MAGIC     └── silver_returns
# MAGIC             └── gold_customer_metrics
# MAGIC ```
# MAGIC
# MAGIC Dependencies:
# MAGIC * bronze_events: No dependencies (root)
# MAGIC * silver_purchases: Depends on bronze_events
# MAGIC * silver_returns: Depends on bronze_events
# MAGIC * gold_daily_summary: Depends on silver_purchases
# MAGIC * gold_customer_metrics: Depends on BOTH silver_purchases AND silver_returns
# MAGIC
# MAGIC **Question 2: If bronze_events fails**
# MAGIC ALL downstream tables cannot run:
# MAGIC * silver_purchases (blocked)
# MAGIC * silver_returns (blocked)
# MAGIC * gold_daily_summary (blocked by silver_purchases failure)
# MAGIC * gold_customer_metrics (blocked by both silver table failures)
# MAGIC
# MAGIC Result: Entire pipeline blocked.
# MAGIC
# MAGIC **Question 3: If silver_purchases fails**
# MAGIC Affected tables:
# MAGIC * gold_daily_summary (cannot run - direct dependency)
# MAGIC * gold_customer_metrics (cannot run - needs BOTH silver tables)
# MAGIC
# MAGIC NOT affected:
# MAGIC * bronze_events (independent)
# MAGIC * silver_returns (independent - different branch from bronze_events)
# MAGIC
# MAGIC **Question 4: Can silver_purchases and silver_returns run in parallel?**
# MAGIC YES, they can run in parallel because:
# MAGIC * Both depend only on bronze_events
# MAGIC * No dependency between them
# MAGIC * Independent transformations
# MAGIC * SDP automatically parallelizes independent branches
# MAGIC
# MAGIC **Question 5: Table type selection**
# MAGIC
# MAGIC | Table | Type | Justification |
# MAGIC |-------|------|---------------|
# MAGIC | bronze_events | Streaming table | Source is S3 files (Auto Loader), continuous ingestion, append-only |
# MAGIC | silver_purchases | Streaming table | Real-time filtering from streaming source, downstream needs streaming |
# MAGIC | silver_returns | Streaming table | Real-time filtering from streaming source, downstream needs streaming |
# MAGIC | gold_daily_summary | Materialized view | Batch aggregation (daily), periodic recomputation, batch read of streaming source |
# MAGIC | gold_customer_metrics | Materialized view | Batch aggregation across multiple sources, periodic recomputation |
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Dependency resolution**:
# MAGIC * SDP analyzes table definitions to build dependency graph
# MAGIC * Tables with no dependencies (roots) run first
# MAGIC * Dependent tables wait for upstream completion
# MAGIC * Independent branches run in parallel
# MAGIC
# MAGIC **Failure propagation**:
# MAGIC * If table fails, all downstream dependents blocked
# MAGIC * Independent branches continue running
# MAGIC * In production mode: retries attempted automatically
# MAGIC * In development mode: pipeline stops, manual fix required
# MAGIC
# MAGIC **Parallel execution**:
# MAGIC * SDP automatically parallelizes independent branches
# MAGIC * No explicit parallelism configuration needed
# MAGIC * Limited by cluster resources
# MAGIC * silver_purchases and silver_returns process simultaneously if resources available
# MAGIC
# MAGIC **Mixed table types in pipeline**:
# MAGIC * Streaming tables process continuously
# MAGIC * Materialized views recompute periodically (or on-demand)
# MAGIC * Materialized views CAN batch-read streaming tables using dlt.read()
# MAGIC * This pattern (streaming → batch aggregation) is common for gold layer
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Assuming all tables in a pipeline must be the same type
# MAGIC
# MAGIC **Wrong reasoning**: "Pipeline has streaming tables, so everything must be streaming"
# MAGIC
# MAGIC **Why wrong**: Mixing streaming tables and materialized views is a standard pattern:
# MAGIC * Bronze/Silver: Streaming tables (real-time ingestion and filtering)
# MAGIC * Gold: Materialized views (batch aggregations for analytics)
# MAGIC
# MAGIC **Exam scenario**: "A pipeline has 5 streaming tables and 2 materialized views. Is this valid?"
# MAGIC Answer: YES. Common layered architecture.
# MAGIC
# MAGIC **Dependency confusion**: "If Table A reads Table B, does Table B depend on Table A?"
# MAGIC Answer: NO. B → A dependency (A depends on B). Arrows point toward dependent.
# MAGIC
# MAGIC ### Memory Aid
# MAGIC
# MAGIC **Dependency direction**:
# MAGIC * "A reads B" = "A depends on B" = "B must run first"
# MAGIC * Arrow: B → A (from source to consumer)
# MAGIC
# MAGIC **Failure impact**:
# MAGIC * Downstream: Blocked (cannot run without upstream data)
# MAGIC * Upstream: Unaffected (independent)
# MAGIC * Siblings: Unaffected (independent branches)
# MAGIC
# MAGIC **Parallelism rule**:
# MAGIC * Same level + no cross-dependencies = Parallel
# MAGIC * Different levels = Sequential
# MAGIC * Cross-dependencies = Sequential
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Reference overview:**
# MAGIC See Topic 9 sections on Pipeline Dependencies and Execution Order

# COMMAND ----------

# DBTITLE 1,Exercise 6 Solution: Development vs Production Mode
# MAGIC %md
# MAGIC ## Exercise 6 Solution: Development vs Production Mode
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC **Question 1: Three key differences**
# MAGIC
# MAGIC | Feature | Development Mode | Production Mode |
# MAGIC |---------|-----------------|----------------|
# MAGIC | Failure handling | Stops on first error | Automatic retries (up to 3x) |
# MAGIC | Checkpoint handling | Full refresh on each run | Incremental processing |
# MAGIC | Schema evolution | Allows schema changes freely | Schema changes require explicit migration |
# MAGIC
# MAGIC **Question 2: During development, table fails due to data quality issue**
# MAGIC
# MAGIC * **Failed table**: Stops processing, marked as failed, error visible in UI
# MAGIC * **Downstream dependent tables**: Cannot run (blocked by upstream failure), marked as "not run"
# MAGIC * **Independent tables** (other branches): Continue running normally (failure doesn't propagate to independent branches)
# MAGIC
# MAGIC Development mode = "fail fast" - see problems immediately
# MAGIC
# MAGIC **Question 3: Configuration changes for production deployment**
# MAGIC
# MAGIC ```python
# MAGIC # Development
# MAGIC pipeline_config = {
# MAGIC     "development": True,     # Enable development mode
# MAGIC     "continuous": False      # Manual trigger
# MAGIC }
# MAGIC
# MAGIC # Production
# MAGIC pipeline_config = {
# MAGIC     "development": False,    # Enable production mode
# MAGIC     "continuous": True       # Continuous processing
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Additional production considerations:
# MAGIC * Set target schema to production catalog (not dev)
# MAGIC * Configure alerts for failures
# MAGIC * Set appropriate cluster size
# MAGIC * Enable auto-scaling
# MAGIC * Configure retention policies
# MAGIC
# MAGIC **Question 4: Transient network error in production**
# MAGIC
# MAGIC Production mode:
# MAGIC * Automatic retry (up to 3 attempts)
# MAGIC * If retries succeed: Pipeline continues normally
# MAGIC * If all retries fail: Pipeline fails, alert triggered
# MAGIC * Checkpoint preserved (incremental processing resumes from last success)
# MAGIC
# MAGIC Development mode:
# MAGIC * Immediate failure (no retries)
# MAGIC * Pipeline stops
# MAGIC * Manual intervention required
# MAGIC * Next run starts fresh (full refresh)
# MAGIC
# MAGIC **Question 5: Should you run continuous=true in development?**
# MAGIC
# MAGIC NO. Reasons:
# MAGIC * Development is for testing and iteration
# MAGIC * Continuous mode keeps pipeline running indefinitely
# MAGIC * Hard to test changes with continuous updates
# MAGIC * Wastes compute resources during development
# MAGIC * Manual trigger (continuous=false) allows controlled testing
# MAGIC
# MAGIC Use continuous=true only in production for:
# MAGIC * Real-time data pipelines
# MAGIC * Streaming workloads requiring low latency
# MAGIC * Production deployments with stable code
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Development mode characteristics**:
# MAGIC * Fast feedback loop
# MAGIC * Full refresh on each run (no checkpoints)
# MAGIC * Immediate failure visibility
# MAGIC * Schema flexibility
# MAGIC * Manual triggering preferred
# MAGIC * Lower compute costs (shorter runs)
# MAGIC
# MAGIC **Production mode characteristics**:
# MAGIC * Resilient to transient errors
# MAGIC * Incremental processing (checkpoints)
# MAGIC * Automatic retries
# MAGIC * Schema stability enforced
# MAGIC * Continuous operation
# MAGIC * Higher availability
# MAGIC
# MAGIC **Mode selection strategy**:
# MAGIC * Start in development for all new pipelines
# MAGIC * Test thoroughly with representative data
# MAGIC * Switch to production only when stable
# MAGIC * Keep development pipelines for testing changes
# MAGIC * Use separate catalogs/schemas for dev vs prod
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Not understanding checkpoint behavior difference
# MAGIC
# MAGIC **Wrong assumption**: "Development and production both use checkpoints, just with different retry logic"
# MAGIC
# MAGIC **Why wrong**: Development mode does FULL REFRESH on each run (ignores checkpoints). Production mode uses INCREMENTAL PROCESSING (checkpoints track progress).
# MAGIC
# MAGIC This difference is critical:
# MAGIC * Development: Fast iteration, see all changes
# MAGIC * Production: Efficient processing, only new data
# MAGIC
# MAGIC **Exam scenario**: "A pipeline in development mode processes 1M rows. You fix a bug and rerun. How many rows processed?"
# MAGIC
# MAGIC Answer: 1M rows again (full refresh). Not incremental.
# MAGIC
# MAGIC **Signal phrases**:
# MAGIC * "Testing new code" / "Iterative development" → Development mode
# MAGIC * "Production deployment" / "Continuous operation" → Production mode
# MAGIC * "Transient errors" / "Automatic retry" → Production mode feature
# MAGIC * "Full refresh" / "See all data" → Development mode behavior
# MAGIC
# MAGIC ### Memory Aid
# MAGIC
# MAGIC **DEV vs PROD acronym**:
# MAGIC
# MAGIC **DEV** mode:
# MAGIC * **D**irect failure (no retries)
# MAGIC * **E**very run is full refresh
# MAGIC * **V**isibility of all issues immediately
# MAGIC
# MAGIC **PROD** mode:
# MAGIC * **P**ersistent checkpoints (incremental)
# MAGIC * **R**etries automatic (up to 3x)
# MAGIC * **O**ngoing continuous operation
# MAGIC * **D**urable against transient errors
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Reference overview:**
# MAGIC See Topic 9 sections on Pipeline Modes and Development Workflow

# COMMAND ----------

# DBTITLE 1,Exercise 7 Solution: Unity Catalog Permissions
# MAGIC %md
# MAGIC ## Exercise 7 Solution: Unity Catalog Permissions for Pipelines
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC **Question 1: Required permissions for service principal**
# MAGIC
# MAGIC The service principal needs:
# MAGIC
# MAGIC 1. **USE CATALOG** on analytics catalog
# MAGIC 2. **USE SCHEMA** on analytics.sales schema  
# MAGIC 3. **CREATE TABLE** on analytics.sales schema
# MAGIC 4. **MODIFY** on tables (for ongoing updates)
# MAGIC 5. **SELECT** on tables (pipelines read their own tables for dependencies)
# MAGIC
# MAGIC **Question 2: GRANT statements**
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant catalog access
# MAGIC GRANT USE CATALOG ON CATALOG analytics TO `pipeline_sp`;
# MAGIC
# MAGIC -- Grant schema access and table creation
# MAGIC GRANT USE SCHEMA ON SCHEMA analytics.sales TO `pipeline_sp`;
# MAGIC GRANT CREATE TABLE ON SCHEMA analytics.sales TO `pipeline_sp`;
# MAGIC
# MAGIC -- Grant modify and select on future tables
# MAGIC GRANT MODIFY, SELECT ON SCHEMA analytics.sales TO `pipeline_sp`;
# MAGIC ```
# MAGIC
# MAGIC Note: Granting at SCHEMA level applies to all tables in that schema (current and future).
# MAGIC
# MAGIC **Question 3: Reading from external table**
# MAGIC
# MAGIC Additional permission required:
# MAGIC ```sql
# MAGIC -- Need SELECT on source table
# MAGIC GRANT SELECT ON TABLE landing.raw.events TO `pipeline_sp`;
# MAGIC
# MAGIC -- Also need USE CATALOG and USE SCHEMA for source
# MAGIC GRANT USE CATALOG ON CATALOG landing TO `pipeline_sp`;
# MAGIC GRANT USE SCHEMA ON SCHEMA landing.raw TO `pipeline_sp`;
# MAGIC ```
# MAGIC
# MAGIC **Question 4: Ownership and access**
# MAGIC
# MAGIC * **Owner**: The service principal (pipeline_sp) owns the tables it creates
# MAGIC * **Default access**: Other users CANNOT query these tables by default
# MAGIC * **To enable access**: Grant SELECT to users/groups:
# MAGIC   ```sql
# MAGIC   GRANT SELECT ON SCHEMA analytics.sales TO `data_analysts`;
# MAGIC   ```
# MAGIC
# MAGIC Ownership does NOT automatically grant read access to other users. Explicit grants required.
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Unity Catalog permission hierarchy**:
# MAGIC
# MAGIC ```
# MAGIC CATALOG
# MAGIC   │
# MAGIC   USE CATALOG (required to access anything in catalog)
# MAGIC   │
# MAGIC   └── SCHEMA
# MAGIC        │
# MAGIC        USE SCHEMA (required to access tables in schema)
# MAGIC        │
# MAGIC        ├── CREATE TABLE (create new tables)
# MAGIC        ├── CREATE VIEW (create views)
# MAGIC        ├── CREATE FUNCTION (create functions)
# MAGIC        │
# MAGIC        └── TABLE
# MAGIC             ├── SELECT (read data)
# MAGIC             ├── MODIFY (insert/update/delete)
# MAGIC             └── ALL PRIVILEGES (full access)
# MAGIC ```
# MAGIC
# MAGIC **Permission inheritance**:
# MAGIC * Granting at SCHEMA level applies to all tables in schema
# MAGIC * Granting at CATALOG level applies to all schemas
# MAGIC * Service principals need explicit grants (no implicit permissions)
# MAGIC
# MAGIC **Common pipeline permission patterns**:
# MAGIC
# MAGIC 1. **Read-only pipeline** (just transforms existing data):
# MAGIC    * USE CATALOG, USE SCHEMA on source
# MAGIC    * SELECT on source tables
# MAGIC    * CREATE TABLE on target schema
# MAGIC
# MAGIC 2. **Full pipeline** (creates and updates tables):
# MAGIC    * All above +
# MAGIC    * MODIFY on target tables
# MAGIC
# MAGIC 3. **Cross-catalog pipeline** (reads from landing, writes to analytics):
# MAGIC    * Permissions on BOTH catalogs
# MAGIC    * USE CATALOG on landing (source)
# MAGIC    * USE CATALOG + CREATE TABLE on analytics (target)
# MAGIC
# MAGIC **Service principal vs user permissions**:
# MAGIC * Service principals: Explicit grants only
# MAGIC * Users: May have implicit grants via groups
# MAGIC * Pipeline service principals should have minimal required permissions
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Forgetting hierarchical USE permissions
# MAGIC
# MAGIC **Wrong grants**:
# MAGIC ```sql
# MAGIC GRANT CREATE TABLE ON SCHEMA analytics.sales TO `pipeline_sp`;
# MAGIC -- Missing USE CATALOG and USE SCHEMA
# MAGIC ```
# MAGIC
# MAGIC **Why wrong**: Even with CREATE TABLE, service principal cannot access schema without USE CATALOG and USE SCHEMA.
# MAGIC
# MAGIC **Correct sequence** (top to bottom):
# MAGIC 1. USE CATALOG on catalog
# MAGIC 2. USE SCHEMA on schema
# MAGIC 3. Specific permissions (CREATE TABLE, SELECT, MODIFY)
# MAGIC
# MAGIC **Exam scenario**: "Pipeline fails with 'Permission denied' when reading source table. What grants are needed?"
# MAGIC
# MAGIC Check ALL levels:
# MAGIC * Source: USE CATALOG, USE SCHEMA, SELECT
# MAGIC * Target: USE CATALOG, USE SCHEMA, CREATE TABLE, MODIFY
# MAGIC
# MAGIC Missing ANY of these causes permission errors.
# MAGIC
# MAGIC **Signal phrases**:
# MAGIC * "Service principal" / "Pipeline user" → Need explicit grants
# MAGIC * "Permission denied" → Check hierarchical USE permissions
# MAGIC * "Cross-catalog" / "Multiple schemas" → Grants on EACH catalog/schema
# MAGIC
# MAGIC ### Memory Aid
# MAGIC
# MAGIC **UC Permission levels** (top to bottom):
# MAGIC 1. **CATALOG**: "Can I enter the building?"
# MAGIC 2. **SCHEMA**: "Can I enter this floor?"
# MAGIC 3. **TABLE**: "Can I use this desk?"
# MAGIC
# MAGIC Must pass each level to access the next.
# MAGIC
# MAGIC **Pipeline permission checklist**:
# MAGIC * ☑ USE CATALOG (source and target)
# MAGIC * ☑ USE SCHEMA (source and target)
# MAGIC * ☑ SELECT (source tables)
# MAGIC * ☑ CREATE TABLE (target schema)
# MAGIC * ☑ MODIFY (target tables)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Reference overview:**
# MAGIC See Topic 9 sections on Unity Catalog Integration and Permissions

# COMMAND ----------

# DBTITLE 1,Exercise 8 Solution: Reading Patterns (Batch vs Stream)
# MAGIC %md
# MAGIC ## Exercise 8 Solution: Reading Patterns (Batch vs Stream)
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC **Question 1: Can gold_summary use dlt.read("silver_clean")?**
# MAGIC
# MAGIC YES. This performs a batch read of the streaming table.
# MAGIC * Reads ALL data in silver_clean as a batch DataFrame
# MAGIC * On each pipeline run, re-reads the entire table
# MAGIC * No incremental processing
# MAGIC * Useful for full recomputation aggregations
# MAGIC
# MAGIC **Question 2: Can gold_summary use dlt.read_stream("silver_clean")?**
# MAGIC
# MAGIC NO. Materialized views (@dlt.view) CANNOT use read_stream.
# MAGIC * dlt.read_stream only works in streaming tables (@dlt.table)
# MAGIC * Error: "Streaming sources are not supported in materialized views"
# MAGIC * Must use dlt.read for batch reads
# MAGIC
# MAGIC **Question 3: Can silver_clean use dlt.read("bronze_raw")?**
# MAGIC
# MAGIC YES, but it changes the semantics:
# MAGIC * Batch reads the entire bronze_raw table on each run
# MAGIC * No incremental processing (inefficient)
# MAGIC * Loses streaming benefits
# MAGIC * silver_clean would no longer be a true streaming table
# MAGIC
# MAGIC Technically valid but defeats the purpose of streaming architecture.
# MAGIC
# MAGIC **Question 4: Incremental processing for gold_summary**
# MAGIC
# MAGIC Two options:
# MAGIC
# MAGIC **Option A**: Change to streaming table
# MAGIC ```python
# MAGIC @dlt.table  # Changed from @dlt.view
# MAGIC def gold_summary():
# MAGIC     return (
# MAGIC         dlt.read_stream("silver_clean")
# MAGIC         .groupBy("date")
# MAGIC         .agg(sum("amount"))
# MAGIC     )
# MAGIC ```
# MAGIC Incrementally processes new rows, maintains running totals.
# MAGIC
# MAGIC **Option B**: Keep as materialized view, optimize with Liquid Clustering
# MAGIC ```python
# MAGIC @dlt.view
# MAGIC def gold_summary():
# MAGIC     return (
# MAGIC         dlt.read("silver_clean")
# MAGIC         .where(col("date") >= current_date() - 7)  # Only recent data
# MAGIC         .groupBy("date")
# MAGIC         .agg(sum("amount"))
# MAGIC     )
# MAGIC ```
# MAGIC Still batch, but filters to reduce data scanned.
# MAGIC
# MAGIC **Question 5: Difference between approaches**
# MAGIC
# MAGIC **Approach A (materialized view + batch read)**:
# MAGIC ```python
# MAGIC @dlt.view
# MAGIC def summary():
# MAGIC     return dlt.read("silver_clean").groupBy("date").agg(sum("amount"))
# MAGIC ```
# MAGIC * Type: Materialized view
# MAGIC * Processing: Batch (full re-scan on each run)
# MAGIC * Trigger: On-demand or scheduled
# MAGIC * State: No checkpoint (recomputes from scratch)
# MAGIC * Use case: Periodic aggregations, dashboards
# MAGIC
# MAGIC **Approach B (streaming table + streaming read)**:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def summary():
# MAGIC     return dlt.read_stream("silver_clean").groupBy("date").agg(sum("amount"))
# MAGIC ```
# MAGIC * Type: Streaming table
# MAGIC * Processing: Incremental (only new rows)
# MAGIC * Trigger: Continuous or triggered
# MAGIC * State: Checkpoint tracks processed rows
# MAGIC * Use case: Real-time aggregations, windowed metrics
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Reading patterns matrix**:
# MAGIC
# MAGIC | Consumer Type | dlt.read() | dlt.read_stream() |
# MAGIC |---------------|-----------|------------------|
# MAGIC | @dlt.view (materialized view) | ✔ YES (batch read) | ❌ NO (not supported) |
# MAGIC | @dlt.table (streaming table) | ✔ YES (batch read) | ✔ YES (streaming read) |
# MAGIC
# MAGIC **Key rules**:
# MAGIC
# MAGIC 1. **Materialized views MUST use dlt.read()** (batch only)
# MAGIC 2. **Streaming tables CAN use either**:
# MAGIC    * dlt.read_stream() for incremental processing
# MAGIC    * dlt.read() for batch reads (but defeats streaming purpose)
# MAGIC 3. **Streaming table can batch-read another streaming table**
# MAGIC 4. **Materialized view can batch-read streaming table**
# MAGIC
# MAGIC **When to use each pattern**:
# MAGIC
# MAGIC **Batch read (dlt.read)**:
# MAGIC * Periodic full recomputation needed
# MAGIC * Aggregations that require complete dataset
# MAGIC * Downstream is a dashboard (batch queries)
# MAGIC * Data size manageable for full scans
# MAGIC
# MAGIC **Streaming read (dlt.read_stream)**:
# MAGIC * Need incremental processing
# MAGIC * Real-time or low-latency requirements
# MAGIC * Large datasets (avoid full scans)
# MAGIC * Downstream needs streaming input
# MAGIC
# MAGIC **Mixed patterns** (common in layered architecture):
# MAGIC * Bronze: Streaming tables with read_stream (ingestion)
# MAGIC * Silver: Streaming tables with read_stream (filtering, enrichment)
# MAGIC * Gold: Materialized views with read (batch aggregations)
# MAGIC
# MAGIC This pattern is valid and common.
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Thinking materialized views can use read_stream
# MAGIC
# MAGIC **Wrong code**:
# MAGIC ```python
# MAGIC @dlt.view  # Materialized view
# MAGIC def summary():
# MAGIC     return dlt.read_stream("source")  # ERROR
# MAGIC ```
# MAGIC
# MAGIC **Why wrong**: Materialized views are inherently batch. They compute results by running a batch query, not by processing streams incrementally.
# MAGIC
# MAGIC **Correct patterns**:
# MAGIC ```python
# MAGIC # Pattern 1: Batch aggregation
# MAGIC @dlt.view
# MAGIC def summary():
# MAGIC     return dlt.read("streaming_source").groupBy("key").count()
# MAGIC
# MAGIC # Pattern 2: Streaming aggregation
# MAGIC @dlt.table
# MAGIC def summary():
# MAGIC     return dlt.read_stream("streaming_source").groupBy("key").count()
# MAGIC ```
# MAGIC
# MAGIC Both valid, different semantics.
# MAGIC
# MAGIC **Exam scenario**: "A materialized view needs to read from a streaming table. What syntax?"
# MAGIC
# MAGIC Answer: `dlt.read("streaming_table")` - batch read of streaming source.
# MAGIC
# MAGIC **Signal phrases**:
# MAGIC * "Materialized view" + "streaming source" → Use dlt.read() (batch read)
# MAGIC * "Incremental processing" / "Only new rows" → Use dlt.read_stream() in @dlt.table
# MAGIC * "Full recomputation" / "Complete refresh" → Use dlt.read() (batch)
# MAGIC
# MAGIC ### Memory Aid
# MAGIC
# MAGIC **READ acronym**:
# MAGIC * **R**ead() = **R**ecompute everything (batch)
# MAGIC * **R**ead_stream() = **R**eal-time incremental processing
# MAGIC
# MAGIC **Materialized view limitations**:
# MAGIC * **M**ust use batch reads only
# MAGIC * **V**iews cannot maintain streaming state
# MAGIC
# MAGIC If you see @dlt.view + read_stream = WRONG
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Reference overview:**
# MAGIC See Topic 9 sections on Reading Patterns and Table Types

# COMMAND ----------

# DBTITLE 1,MCQ Solutions 1-3
# MAGIC %md
# MAGIC ## MCQ Solutions
# MAGIC
# MAGIC ### MCQ 1 Solution: Streaming Table vs Materialized View
# MAGIC
# MAGIC **Correct Answer: C**
# MAGIC
# MAGIC **Explanation**:
# MAGIC
# MAGIC The requirements are:
# MAGIC * Calculate total spending per customer (aggregation)
# MAGIC * Data is append-only
# MAGIC * Query pattern: "What is customer X's total lifetime spending?" (point queries)
# MAGIC * 10M transactions/day
# MAGIC
# MAGIC **Why C is correct**:
# MAGIC Materialized view with batch aggregation is the right choice because:
# MAGIC 1. Query pattern favors pre-computed aggregates (not real-time updates)
# MAGIC 2. Append-only data means no CDC complexity
# MAGIC 3. Periodic recomputation is acceptable for "lifetime" totals
# MAGIC 4. More efficient than maintaining streaming aggregations
# MAGIC
# MAGIC **Why others are wrong**:
# MAGIC
# MAGIC **A (Streaming with windowed aggregation)**: Wrong because:
# MAGIC * Windows require time bounds ("1 day")
# MAGIC * Question asks for "lifetime" total, not time-bound windows
# MAGIC * Windowed aggregations expire old data
# MAGIC
# MAGIC **B (Streaming with non-windowed aggregation)**: Wrong because:
# MAGIC * Streaming aggregations without windows are not supported in standard SDP
# MAGIC * Would require stateful stream processing (complex)
# MAGIC * No real-time requirement in scenario
# MAGIC
# MAGIC **D (AUTO CDC)**: Wrong because:
# MAGIC * AUTO CDC is for change data capture (INSERT/UPDATE/DELETE)
# MAGIC * Source is append-only transactions, not CDC stream
# MAGIC * Overcomplicates a simple aggregation
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 2 Solution: Expectation Enforcement
# MAGIC
# MAGIC **Correct Answer: D**
# MAGIC
# MAGIC **Explanation**:
# MAGIC
# MAGIC Expectation priority:
# MAGIC 1. **@dlt.expect_or_fail** (recent_date): HIGHEST priority - fails immediately
# MAGIC 2. **@dlt.expect_or_drop** (valid_customer): Drops rows silently
# MAGIC 3. **@dlt.expect** (positive_amount): Logs violations, keeps rows
# MAGIC
# MAGIC **Execution**:
# MAGIC 1. Pipeline processes batch of 1000 rows
# MAGIC 2. Encounters 5 rows with order_date < '2020-01-01'
# MAGIC 3. expect_or_fail constraint violated
# MAGIC 4. Pipeline FAILS IMMEDIATELY on first violation
# MAGIC 5. NO ROWS WRITTEN (entire batch rejected)
# MAGIC 6. Other violations (30 + 15 rows) never evaluated
# MAGIC
# MAGIC **Why others are wrong**:
# MAGIC
# MAGIC **A**: Correct that pipeline fails, correct that no rows written
# MAGIC **B**: Wrong - pipeline doesn't continue, it fails on expect_or_fail
# MAGIC **C**: Wrong - pipeline doesn't write any rows when expect_or_fail violated
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 3 Solution: AUTO CDC SCD Types
# MAGIC
# MAGIC **Correct Answer: B**
# MAGIC
# MAGIC **Explanation**:
# MAGIC
# MAGIC Requirements:
# MAGIC * Need historical prices ("What was price on 2023-05-15?")
# MAGIC * 7-year history retention
# MAGIC * Track product deletions
# MAGIC * Point-in-time queries
# MAGIC
# MAGIC This requires **SCD Type 2** (historical tracking).
# MAGIC
# MAGIC **Why B is correct**:
# MAGIC ```python
# MAGIC stored_as_scd_type=2
# MAGIC ```
# MAGIC * Maintains full history of changes
# MAGIC * Each price change creates new row
# MAGIC * Rows have __START_AT and __END_AT timestamps
# MAGIC * Can query historical state: WHERE '2023-05-15' BETWEEN __START_AT AND __END_AT
# MAGIC * Deleted products marked with __DELETED flag (not physically deleted)
# MAGIC
# MAGIC **Why others are wrong**:
# MAGIC
# MAGIC **A (SCD Type 1)**: Wrong because:
# MAGIC * Type 1 maintains only CURRENT state
# MAGIC * Historical prices overwritten on updates
# MAGIC * Cannot answer "What was price on 2023-05-15?"
# MAGIC * Deleted products physically removed (no history)
# MAGIC
# MAGIC **C (Streaming table without CDC)**: Wrong because:
# MAGIC * Would just append all events
# MAGIC * No automatic handling of updates/deletes
# MAGIC * No current state maintenance
# MAGIC * Manual point-in-time query logic required
# MAGIC
# MAGIC **D (Materialized view with aggregation)**: Wrong because:
# MAGIC * Only gives last update timestamp per product
# MAGIC * Doesn't maintain price history
# MAGIC * Cannot reconstruct historical state
# MAGIC * Inappropriate for CDC use case

# COMMAND ----------

# DBTITLE 1,MCQ Solutions 4-5
# MAGIC %md
# MAGIC ### MCQ 4 Solution: Development vs Production Mode
# MAGIC
# MAGIC **Correct Answer: B**
# MAGIC
# MAGIC **Explanation**:
# MAGIC
# MAGIC **Production mode**:
# MAGIC * Automatically retries failed tables up to 3 times
# MAGIC * Retries apply to transient errors (network, temporary resource issues)
# MAGIC * If retry succeeds, pipeline continues normally
# MAGIC * If all retries fail, pipeline fails
# MAGIC
# MAGIC **Development mode**:
# MAGIC * Fails immediately on first error
# MAGIC * No automatic retries
# MAGIC * "Fail fast" for quick debugging
# MAGIC * Manual rerun required
# MAGIC
# MAGIC **Why others are wrong**:
# MAGIC
# MAGIC **A**: Wrong - production mode has automatic retries, doesn't stop immediately
# MAGIC **C**: Wrong - both modes continue processing independent tables, but production retries the failed table
# MAGIC **D**: Wrong - neither mode "rolls back" changes. Streaming tables checkpoint progress.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 5 Solution: Reading Materialized Views as Streams
# MAGIC
# MAGIC **Correct Answer: B**
# MAGIC
# MAGIC **Explanation**:
# MAGIC
# MAGIC **The code**:
# MAGIC ```python
# MAGIC @dlt.view  # Materialized view
# MAGIC def daily_summary():
# MAGIC     return dlt.read("transactions").groupBy("date").agg(sum("amount"))
# MAGIC
# MAGIC @dlt.table  # Streaming table
# MAGIC def enriched_summary():
# MAGIC     return dlt.read_stream("daily_summary")  # ERROR HERE
# MAGIC ```
# MAGIC
# MAGIC **Why B is correct**:
# MAGIC * Materialized views CANNOT be read as streams
# MAGIC * read_stream("daily_summary") will fail
# MAGIC * Error: "Streaming sources are not supported for this table type"
# MAGIC * Materialized views are inherently batch - no streaming state to read
# MAGIC
# MAGIC **Fix options**:
# MAGIC
# MAGIC **Option 1**: Read daily_summary as batch:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def enriched_summary():
# MAGIC     return dlt.read("daily_summary").join(lookup_table, "date")
# MAGIC ```
# MAGIC
# MAGIC **Option 2**: Change daily_summary to streaming table:
# MAGIC ```python
# MAGIC @dlt.table  # Changed from @dlt.view
# MAGIC def daily_summary():
# MAGIC     return dlt.read_stream("transactions").groupBy("date").agg(sum("amount"))
# MAGIC ```
# MAGIC
# MAGIC **Why others are wrong**:
# MAGIC
# MAGIC **A**: Wrong - pipeline fails, cannot read materialized views as streams
# MAGIC **C**: Wrong - pipeline doesn't succeed, it fails immediately
# MAGIC **D**: Wrong - materialized views CAN be joined, but must use batch reads

# COMMAND ----------

# DBTITLE 1,Challenge Solution: Multi-Layer Pipeline Design
# MAGIC %md
# MAGIC ## Challenge Solution: Multi-Layer Pipeline Design
# MAGIC
# MAGIC ### Part 1: Table Type Selection
# MAGIC
# MAGIC | Layer | Table | Type | Justification |
# MAGIC |-------|-------|------|---------------|
# MAGIC | Bronze | bronze_orders | Streaming table | Continuous ingestion from S3 using Auto Loader, append-only events, need real-time processing |
# MAGIC | Bronze | bronze_customers | Streaming table | CDC stream requires streaming ingestion |
# MAGIC | Bronze | bronze_products | Materialized view | Daily batch file, complete refresh, no incremental processing needed |
# MAGIC | Silver | silver_orders_validated | Streaming table | Real-time filtering with expectations, downstream needs streaming |
# MAGIC | Silver | silver_customers_current | Streaming table + AUTO CDC | CDC stream with current state maintenance (SCD Type 1) |
# MAGIC | Silver | silver_products_active | Materialized view | Batch filter of batch source, updated daily |
# MAGIC | Gold | gold_revenue_by_category | Streaming table | Continuous aggregation as orders arrive, real-time dashboard requirement |
# MAGIC | Gold | gold_customer_lifetime_value | Materialized view | Daily batch aggregation, periodic recomputation acceptable |
# MAGIC
# MAGIC ### Part 2: Implementation Code
# MAGIC
# MAGIC #### Bronze: Order Ingestion
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import col, current_timestamp
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Raw orders from S3 using Auto Loader",
# MAGIC     table_properties={"quality": "bronze"}
# MAGIC )
# MAGIC def bronze_orders():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .option("cloudFiles.schemaLocation", "/tmp/schema/orders")
# MAGIC         .load("s3://orders-raw/")
# MAGIC         .withColumn("ingestion_time", current_timestamp())
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC #### Silver: Validated Orders with Expectations
# MAGIC ```python
# MAGIC @dlt.table(
# MAGIC     comment="Validated orders with data quality checks",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("positive_amount", "amount > 0")
# MAGIC @dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
# MAGIC @dlt.expect_or_drop("recent_order", "order_time >= current_date() - INTERVAL 1 YEAR")
# MAGIC def silver_orders_validated():
# MAGIC     return (
# MAGIC         dlt.read_stream("bronze_orders")
# MAGIC         .select(
# MAGIC             "order_id",
# MAGIC             "customer_id",
# MAGIC             "product_id",
# MAGIC             "amount",
# MAGIC             "order_time",
# MAGIC             "status"
# MAGIC         )
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC #### Silver: Customer Dimension with AUTO CDC (SCD Type 1)
# MAGIC ```python
# MAGIC # Step 1: Create target table
# MAGIC dlt.create_streaming_table(
# MAGIC     name="silver_customers_current",
# MAGIC     comment="Current customer state from CDC stream",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC
# MAGIC # Step 2: Apply CDC changes
# MAGIC dlt.apply_changes(
# MAGIC     target="silver_customers_current",
# MAGIC     source="bronze_customers",
# MAGIC     keys=["customer_id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type=1  # Current state only
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### Gold: Daily Revenue by Category (Continuous)
# MAGIC ```python
# MAGIC from pyspark.sql.functions import sum as _sum, col
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Real-time revenue by product category",
# MAGIC     table_properties={"quality": "gold"}
# MAGIC )
# MAGIC def gold_revenue_by_category():
# MAGIC     orders = dlt.read_stream("silver_orders_validated")
# MAGIC     products = dlt.read("silver_products_active")  # Batch read of MV
# MAGIC     
# MAGIC     return (
# MAGIC         orders
# MAGIC         .join(products, "product_id")
# MAGIC         .groupBy(
# MAGIC             col("category"),
# MAGIC             window(col("order_time"), "1 day")
# MAGIC         )
# MAGIC         .agg(
# MAGIC             _sum("amount").alias("total_revenue"),
# MAGIC             count("*").alias("order_count")
# MAGIC         )
# MAGIC         .select(
# MAGIC             col("window.start").alias("date"),
# MAGIC             "category",
# MAGIC             "total_revenue",
# MAGIC             "order_count"
# MAGIC         )
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ### Part 3: Dependency Graph
# MAGIC
# MAGIC ```
# MAGIC Layer Structure:
# MAGIC
# MAGIC BRONZE (3 tables):
# MAGIC   - bronze_orders (S3 Auto Loader)
# MAGIC   - bronze_customers (CDC stream)
# MAGIC   - bronze_products (Daily CSV)
# MAGIC        │
# MAGIC        │
# MAGIC        v
# MAGIC SILVER (3 tables):
# MAGIC   - silver_orders_validated ← bronze_orders
# MAGIC   - silver_customers_current ← bronze_customers
# MAGIC   - silver_products_active ← bronze_products
# MAGIC        │
# MAGIC        │
# MAGIC        v
# MAGIC GOLD (2 tables):
# MAGIC   - gold_revenue_by_category ← silver_orders_validated + silver_products_active
# MAGIC   - gold_customer_lifetime_value ← silver_orders_validated + silver_customers_current
# MAGIC
# MAGIC Parallel Processing:
# MAGIC - All 3 bronze tables can run in parallel (independent sources)
# MAGIC - All 3 silver tables can run in parallel (depend only on their bronze sources)
# MAGIC - Both gold tables can run in parallel (independent aggregations)
# MAGIC ```
# MAGIC
# MAGIC ### Part 4: Configuration Recommendations
# MAGIC
# MAGIC #### Initial Development
# MAGIC ```python
# MAGIC pipeline_config = {
# MAGIC     "development": True,
# MAGIC     "continuous": False,
# MAGIC     "target": "dev_analytics.sales",
# MAGIC     "clusters": {
# MAGIC         "default": {
# MAGIC             "num_workers": 2,
# MAGIC             "node_type_id": "i3.xlarge"
# MAGIC         }
# MAGIC     }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Rationale**:
# MAGIC * Development mode for fast iteration
# MAGIC * Manual trigger (continuous=False) for controlled testing
# MAGIC * Small cluster (2 workers) for cost efficiency
# MAGIC * Separate dev catalog for isolation
# MAGIC
# MAGIC #### Production Deployment
# MAGIC ```python
# MAGIC pipeline_config = {
# MAGIC     "development": False,
# MAGIC     "continuous": True,
# MAGIC     "target": "prod_analytics.sales",
# MAGIC     "clusters": {
# MAGIC         "default": {
# MAGIC             "autoscale": {
# MAGIC                 "min_workers": 2,
# MAGIC                 "max_workers": 8
# MAGIC             },
# MAGIC             "node_type_id": "i3.2xlarge"
# MAGIC         }
# MAGIC     },
# MAGIC     "notifications": {
# MAGIC         "on_failure": ["data-eng-team@company.com"],
# MAGIC         "on_success": []
# MAGIC     }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Rationale**:
# MAGIC * Production mode for automatic retries and resilience
# MAGIC * Continuous processing for real-time pipelines
# MAGIC * Auto-scaling for variable load (50K events/hour)
# MAGIC * Larger node types for production performance
# MAGIC * Failure notifications for monitoring
# MAGIC * Production catalog for data consumers
# MAGIC
# MAGIC ### Key Design Decisions
# MAGIC
# MAGIC **1. Mixed table types**:
# MAGIC * Streaming tables for real-time paths (orders, customers)
# MAGIC * Materialized views for batch paths (products, lifetime value)
# MAGIC * This is a standard pattern for layered architecture
# MAGIC
# MAGIC **2. Stream-static join**:
# MAGIC * gold_revenue_by_category joins streaming orders with batch products
# MAGIC * Valid pattern: streaming fact table + batch dimension
# MAGIC * Product catalog changes infrequently (daily), no need for streaming
# MAGIC
# MAGIC **3. AUTO CDC for customers only**:
# MAGIC * Customer data has updates/deletes (CDC stream)
# MAGIC * Products are complete refresh (no CDC needed)
# MAGIC * Orders are append-only (no CDC needed)
# MAGIC
# MAGIC **4. Windowed aggregation for revenue**:
# MAGIC * 1-day tumbling windows for daily revenue
# MAGIC * Allows continuous updates as orders arrive
# MAGIC * Downstream can query by date range
# MAGIC
# MAGIC **5. SCD Type 1 for customers**:
# MAGIC * Scenario doesn't require customer history
# MAGIC * Current state sufficient for lifetime value calculation
# MAGIC * If audit trail needed, switch to SCD Type 2

# COMMAND ----------

# DBTITLE 1,Applied Solution: Table Type Selection Decision Tree
# MAGIC %md
# MAGIC ## Applied Solution: Table Type Selection Decision Tree
# MAGIC
# MAGIC ### Complete Decision Tree
# MAGIC
# MAGIC ```
# MAGIC START: Table Type Selection
# MAGIC     |
# MAGIC     v
# MAGIC [Q1] Does source data arrive continuously?
# MAGIC     |
# MAGIC     +-- NO (Batch arrival) --> [Q2a] Need periodic recomputation?
# MAGIC     |                              |
# MAGIC     |                              +-- YES --> MATERIALIZED VIEW with dlt.read()
# MAGIC     |                              +-- NO --> STREAMING TABLE (if downstream needs streaming)
# MAGIC     |
# MAGIC     +-- YES (Continuous) --> [Q3] Does source have UPDATE/DELETE operations?
# MAGIC                                 |
# MAGIC                                 +-- YES (CDC stream) --> [Q4] Need historical changes?
# MAGIC                                 |                           |
# MAGIC                                 |                           +-- YES --> AUTO CDC (SCD Type 2)
# MAGIC                                 |                           +-- NO --> AUTO CDC (SCD Type 1)
# MAGIC                                 |
# MAGIC                                 +-- NO (Append-only) --> [Q5] Need real-time processing?
# MAGIC                                                             |
# MAGIC                                                             +-- YES --> STREAMING TABLE with read_stream()
# MAGIC                                                             +-- NO --> MATERIALIZED VIEW with read()
# MAGIC ```
# MAGIC
# MAGIC ### Decision Points Explained
# MAGIC
# MAGIC **Q1: Does source data arrive continuously?**
# MAGIC * YES: Streaming sources (Kafka, Auto Loader, CDC feeds, cloud storage with continuous arrival)
# MAGIC * NO: Batch sources (daily files, scheduled extracts, periodic uploads)
# MAGIC
# MAGIC **Q2a: Need periodic recomputation? (Batch sources)**
# MAGIC * YES: Aggregations, summaries, dashboards (use materialized view)
# MAGIC * NO: Downstream streaming consumers need this data (use streaming table with scheduled ingestion)
# MAGIC
# MAGIC **Q3: Does source have UPDATE/DELETE operations?**
# MAGIC * YES: CDC streams from databases, change feeds
# MAGIC * NO: Append-only event streams (logs, clickstreams, IoT)
# MAGIC
# MAGIC **Q4: Need historical changes?**
# MAGIC * YES: Audit trail, compliance, point-in-time queries (SCD Type 2)
# MAGIC * NO: Current state only, latest values (SCD Type 1)
# MAGIC
# MAGIC **Q5: Need real-time processing?**
# MAGIC * YES: Low-latency requirements, downstream real-time consumers
# MAGIC * NO: Batch analytics acceptable, periodic queries
# MAGIC
# MAGIC ### Test Scenarios
# MAGIC
# MAGIC **Scenario A: Clickstream events from Kafka, filter to purchase events, downstream needs real-time**
# MAGIC
# MAGIC 1. Q1: Continuous arrival? YES (Kafka)
# MAGIC 2. Q3: UPDATE/DELETE? NO (append-only events)
# MAGIC 3. Q5: Real-time? YES (downstream needs real-time)
# MAGIC
# MAGIC **Answer: STREAMING TABLE with read_stream()**
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def purchase_events():
# MAGIC     return (
# MAGIC         dlt.read_stream("kafka_clickstream")
# MAGIC         .filter(col("event_type") == "purchase")
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Scenario B: Daily aggregation of sales by region, source is streaming table**
# MAGIC
# MAGIC 1. Q1: Source is streaming but transformation is batch aggregation
# MAGIC 2. Q2a: Periodic recomputation? YES (daily)
# MAGIC
# MAGIC **Answer: MATERIALIZED VIEW with read()**
# MAGIC
# MAGIC ```python
# MAGIC @dlt.view
# MAGIC def daily_sales():
# MAGIC     return (
# MAGIC         dlt.read("sales_stream")
# MAGIC         .groupBy("region", "date")
# MAGIC         .agg(sum("amount"))
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Scenario C: Employee record CDC stream, need full history of all changes**
# MAGIC
# MAGIC 1. Q1: Continuous? YES (CDC)
# MAGIC 2. Q3: UPDATE/DELETE? YES (CDC stream)
# MAGIC 3. Q4: Historical changes? YES (full history)
# MAGIC
# MAGIC **Answer: AUTO CDC (SCD Type 2)**
# MAGIC
# MAGIC ```python
# MAGIC dlt.create_streaming_table("employees_history")
# MAGIC
# MAGIC dlt.apply_changes(
# MAGIC     target="employees_history",
# MAGIC     source="employee_cdc",
# MAGIC     keys=["employee_id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type=2  # Full history
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Scenario D: Product catalog CDC stream, need current state only**
# MAGIC
# MAGIC 1. Q1: Continuous? YES (CDC)
# MAGIC 2. Q3: UPDATE/DELETE? YES (CDC stream)
# MAGIC 3. Q4: Historical changes? NO (current state only)
# MAGIC
# MAGIC **Answer: AUTO CDC (SCD Type 1)**
# MAGIC
# MAGIC ```python
# MAGIC dlt.create_streaming_table("products_current")
# MAGIC
# MAGIC dlt.apply_changes(
# MAGIC     target="products_current",
# MAGIC     source="product_cdc",
# MAGIC     keys=["product_id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type=1  # Current state only
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Quick Reference Table
# MAGIC
# MAGIC | Source Type | Operations | Latency Need | History Need | Table Type | Key Function |
# MAGIC |-------------|-----------|--------------|--------------|------------|-------------|
# MAGIC | Continuous | Append-only | Real-time | N/A | Streaming table | read_stream() |
# MAGIC | Continuous | Append-only | Batch OK | N/A | Materialized view | read() |
# MAGIC | Continuous CDC | UPDATE/DELETE | Real-time | Yes | Streaming + AUTO CDC Type 2 | apply_changes() |
# MAGIC | Continuous CDC | UPDATE/DELETE | Real-time | No | Streaming + AUTO CDC Type 1 | apply_changes() |
# MAGIC | Batch | Any | Periodic | N/A | Materialized view | read() |
# MAGIC | Batch | Any | Real-time | N/A | Streaming table (scheduled) | read_stream() |
# MAGIC
# MAGIC ### Common Pitfalls
# MAGIC
# MAGIC **Pitfall 1**: Using streaming table when materialized view would be simpler
# MAGIC * Signal: "Calculate daily totals" + "Query results once per day"
# MAGIC * Wrong: Streaming table with continuous aggregation
# MAGIC * Right: Materialized view with batch aggregation
# MAGIC
# MAGIC **Pitfall 2**: Using AUTO CDC for append-only streams
# MAGIC * Signal: "Event stream" + "No updates or deletes"
# MAGIC * Wrong: apply_changes() with SCD Type 1
# MAGIC * Right: Streaming table with read_stream()
# MAGIC
# MAGIC **Pitfall 3**: Using materialized view when downstream needs streaming
# MAGIC * Signal: "Downstream table uses read_stream()"
# MAGIC * Wrong: Materialized view (cannot be read as stream)
# MAGIC * Right: Streaming table
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Reference overview:**
# MAGIC See Topic 9 sections on Table Type Selection and Architecture Patterns

# COMMAND ----------

# DBTITLE 1,Study Guide: High-Frequency Topics and Exam Strategy
# MAGIC %md
# MAGIC ## Study Guide: High-Frequency Topics and Exam Strategy
# MAGIC
# MAGIC ### Core Concepts You Must Know Cold
# MAGIC
# MAGIC #### 1. Table Type Selection (HIGH FREQUENCY)
# MAGIC
# MAGIC **Streaming Table vs Materialized View**:
# MAGIC * Streaming: Continuous data, real-time processing, downstream streaming consumers
# MAGIC * Materialized view: Batch aggregations, periodic recomputation, batch reads
# MAGIC * Key trap: Materialized views CAN read streaming tables (batch read with dlt.read)
# MAGIC
# MAGIC **When questioned, ask yourself**:
# MAGIC 1. Does downstream need streaming? (If yes → streaming table)
# MAGIC 2. Is it periodic aggregation? (If yes → likely materialized view)
# MAGIC 3. Is source CDC with updates/deletes? (If yes → AUTO CDC)
# MAGIC
# MAGIC #### 2. Expectations (HIGH FREQUENCY)
# MAGIC
# MAGIC **Three types - know the behavior**:
# MAGIC * **@dlt.expect**: Log violations, keep all rows
# MAGIC * **@dlt.expect_or_drop**: Drop violating rows, log violations
# MAGIC * **@dlt.expect_or_fail**: Fail pipeline immediately on any violation
# MAGIC
# MAGIC **Critical rule**: expect_or_fail ALWAYS takes precedence (decorator order doesn't matter)
# MAGIC
# MAGIC **SQL syntax trap**: Constraints go BEFORE AS clause, not after
# MAGIC
# MAGIC #### 3. AUTO CDC (MEDIUM-HIGH FREQUENCY)
# MAGIC
# MAGIC **SCD Type 1 vs Type 2**:
# MAGIC * Type 1: Current state only, 1 row per key, physical deletes
# MAGIC * Type 2: Full history, multiple rows per key, soft deletes, adds __START_AT/__END_AT/__DELETED
# MAGIC
# MAGIC **Key parameters**:
# MAGIC * **keys**: Primary key columns
# MAGIC * **sequence_by**: Timestamp for ordering (critical for out-of-order events)
# MAGIC * **stored_as_scd_type**: 1 or 2
# MAGIC
# MAGIC **When NOT to use AUTO CDC**: Append-only streams (use regular streaming table)
# MAGIC
# MAGIC #### 4. Reading Patterns (MEDIUM FREQUENCY)
# MAGIC
# MAGIC **Rules**:
# MAGIC * Materialized views: MUST use dlt.read() (batch only)
# MAGIC * Streaming tables: CAN use read() OR read_stream()
# MAGIC * Materialized views CANNOT be read as streams
# MAGIC
# MAGIC **Pattern**: Streaming bronze/silver → Batch gold (common architecture)
# MAGIC
# MAGIC #### 5. Development vs Production Mode (MEDIUM FREQUENCY)
# MAGIC
# MAGIC **Key differences**:
# MAGIC * Development: Full refresh, fail fast, no retries
# MAGIC * Production: Incremental (checkpoints), automatic retries (3x), resilient
# MAGIC
# MAGIC **Continuous mode**:
# MAGIC * Development: Use continuous=false (manual trigger for testing)
# MAGIC * Production: Use continuous=true (real-time pipelines)
# MAGIC
# MAGIC #### 6. Unity Catalog Permissions (LOW-MEDIUM FREQUENCY)
# MAGIC
# MAGIC **Hierarchical requirements** (all needed):
# MAGIC 1. USE CATALOG on catalog
# MAGIC 2. USE SCHEMA on schema
# MAGIC 3. CREATE TABLE on schema (to create tables)
# MAGIC 4. MODIFY + SELECT on tables (for updates and dependencies)
# MAGIC
# MAGIC **Cross-catalog**: Need permissions on BOTH source and target catalogs
# MAGIC
# MAGIC #### 7. Python ↔ SQL Syntax (MEDIUM FREQUENCY)
# MAGIC
# MAGIC **Key conversions**:
# MAGIC * Python: `dlt.read_stream("table")` → SQL: `FROM STREAM(table)`
# MAGIC * Python: `@dlt.expect_or_drop("name", "expr")` → SQL: `CONSTRAINT name EXPECT (expr) ON VIOLATION DROP ROW`
# MAGIC * SQL: Always use `CREATE OR REFRESH STREAMING TABLE`
# MAGIC * SQL: Constraints BEFORE AS clause
# MAGIC
# MAGIC #### 8. Pipeline Dependencies (LOW-MEDIUM FREQUENCY)
# MAGIC
# MAGIC **Failure propagation**:
# MAGIC * Upstream fails → All downstream blocked
# MAGIC * Downstream fails → Upstream and siblings unaffected
# MAGIC
# MAGIC **Parallel execution**: SDP automatically parallelizes independent branches
# MAGIC
# MAGIC ### Exam Strategy
# MAGIC
# MAGIC **Time Management**:
# MAGIC * 45 questions in 90 minutes = 2 minutes per question
# MAGIC * SDP questions are ~15-20% of exam (~7-9 questions)
# MAGIC * Don't spend more than 3 minutes on any single question
# MAGIC
# MAGIC **Signal Phrases to Watch**:
# MAGIC * "As data arrives" / "Continuously" → Streaming table
# MAGIC * "Once per day" / "Periodic" → Materialized view
# MAGIC * "CDC" / "Updates and deletes" → AUTO CDC
# MAGIC * "Current state" → SCD Type 1
# MAGIC * "Historical changes" / "Audit trail" → SCD Type 2
# MAGIC * "Must be valid" / "Required" → expect_or_fail
# MAGIC * "Filter out" / "Drop invalid" → expect_or_drop
# MAGIC * "Monitor" / "Log violations" → expect
# MAGIC
# MAGIC **Common Traps**:
# MAGIC 1. Assuming materialized views can use read_stream (they cannot)
# MAGIC 2. Forgetting hierarchical UC permissions (need USE CATALOG + USE SCHEMA + specific permission)
# MAGIC 3. Thinking decorator order matters for expectations (expect_or_fail always wins)
# MAGIC 4. Using AUTO CDC for append-only streams (use regular streaming table)
# MAGIC 5. Putting SQL constraints after AS clause (they go before)
# MAGIC
# MAGIC ### Last-Minute Review Checklist
# MAGIC
# MAGIC **The Night Before**:
# MAGIC
# MAGIC □ Table type selection decision tree (draw it from memory)
# MAGIC □ Three expectation types and behaviors
# MAGIC □ SCD Type 1 vs Type 2 differences
# MAGIC □ Reading pattern rules (what can read what, and how)
# MAGIC □ Development vs production mode differences
# MAGIC □ UC permission hierarchy (catalog → schema → table)
# MAGIC □ Python ↔ SQL key syntax conversions
# MAGIC
# MAGIC **Day Of Exam**:
# MAGIC
# MAGIC □ expect_or_fail takes precedence (always)
# MAGIC □ Materialized views = batch only (cannot read_stream)
# MAGIC □ AUTO CDC = CDC only (not for append-only)
# MAGIC □ CREATE OR REFRESH (not just CREATE)
# MAGIC □ Constraints before AS in SQL
# MAGIC □ Type 1 = 1 row per key, Type 2 = 2+ rows per key
# MAGIC
# MAGIC ### Practice Question Patterns
# MAGIC
# MAGIC **Pattern 1**: Given requirements, choose table type
# MAGIC * Look for: Source type, latency needs, update patterns
# MAGIC * Answer: Apply decision tree
# MAGIC
# MAGIC **Pattern 2**: Given code with expectations, what happens to data?
# MAGIC * Look for: Which expectations violated, which type (expect/drop/fail)
# MAGIC * Answer: expect_or_fail stops immediately, expect_or_drop filters, expect logs
# MAGIC
# MAGIC **Pattern 3**: Convert Python to SQL or vice versa
# MAGIC * Look for: Streaming reads, expectations, table properties
# MAGIC * Answer: read_stream → STREAM(), constraints before AS, CREATE OR REFRESH
# MAGIC
# MAGIC **Pattern 4**: Given pipeline structure, what fails if X fails?
# MAGIC * Look for: Dependency graph
# MAGIC * Answer: All downstream blocked, upstream/siblings continue
# MAGIC
# MAGIC **Pattern 5**: Permission denied error, what grants needed?
# MAGIC * Look for: Source and target locations
# MAGIC * Answer: USE CATALOG, USE SCHEMA, CREATE TABLE, MODIFY, SELECT (all levels)
# MAGIC
# MAGIC ### Final Confidence Builders
# MAGIC
# MAGIC **You're ready when you can**:
# MAGIC 1. Draw the table type decision tree in under 2 minutes
# MAGIC 2. State all three expectation types and behaviors without hesitation
# MAGIC 3. Explain SCD Type 1 vs 2 in one sentence each
# MAGIC 4. List UC permission hierarchy from memory
# MAGIC 5. Convert basic Python SDP to SQL (or vice versa) in under 3 minutes
# MAGIC
# MAGIC **Trust your preparation**. SDP basics are straightforward once you internalize the decision patterns. Focus on the high-frequency topics above, avoid overthinking, and move quickly through questions you're confident about.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **End of Solutions - Good luck on the exam!**

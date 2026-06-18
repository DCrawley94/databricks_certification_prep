# Databricks notebook source
# DBTITLE 1,Topic 9: Lakeflow SDP - Practice Tasks
# MAGIC %md
# MAGIC # Topic 9: Lakeflow Spark Declarative Pipelines - Practice Tasks
# MAGIC
# MAGIC ## How to Use This Notebook
# MAGIC
# MAGIC This notebook contains exercises, multiple-choice questions, and a challenge to test your understanding of Lakeflow SDP **basics** for the Data Engineer Associate exam.
# MAGIC
# MAGIC **Approach**:
# MAGIC 1. Attempt each exercise without looking at solutions
# MAGIC 2. Write actual code or detailed explanations
# MAGIC 3. Check your answers in solutions.py after attempting
# MAGIC 4. Review exam tips even if you got the answer right
# MAGIC
# MAGIC ## Exercise Categories
# MAGIC
# MAGIC * **Exercises 1-8**: Focused practice on Associate-level SDP fundamentals
# MAGIC * **MCQs 1-5**: Exam-style multiple choice questions
# MAGIC * **Challenge**: Comprehensive multi-layer pipeline design
# MAGIC * **Applied**: Table type selection decision tree
# MAGIC
# MAGIC ## Topics Covered (Associate Level Basics)
# MAGIC
# MAGIC 1. Streaming table vs materialized view selection
# MAGIC 2. Expectation configuration (data quality)
# MAGIC 3. AUTO CDC implementation (SCD Type 1 basics)
# MAGIC 4. Python and SQL syntax conversion
# MAGIC 5. Pipeline dependencies and execution order
# MAGIC 6. Development vs production mode
# MAGIC 7. Unity Catalog permissions
# MAGIC 8. Reading patterns (batch vs stream)

# COMMAND ----------

# DBTITLE 1,Exercise 1: Streaming Table vs Materialized View Selection
# MAGIC %md
# MAGIC ## Exercise 1: Streaming Table vs Materialized View Selection
# MAGIC
# MAGIC **Scenario**: You have four data processing requirements. For each, determine whether to use a streaming table or materialized view, and justify your answer.
# MAGIC
# MAGIC 1. **Requirement 1**: Continuously ingest JSON events from S3 using Auto Loader. Events are never updated. Need to process data as files arrive.
# MAGIC
# MAGIC 2. **Requirement 2**: Calculate daily sales totals by store. Source is a streaming table with individual transactions. Totals recomputed once per day.
# MAGIC
# MAGIC 3. **Requirement 3**: Filter clickstream events to identify only purchase events. Source is a streaming table. Need real-time filtering as events arrive.
# MAGIC
# MAGIC 4. **Requirement 4**: Maintain current customer account balances. Source is a CDC stream with account updates. Need latest balance per customer.
# MAGIC
# MAGIC **For each requirement, provide**:
# MAGIC * Streaming table or materialized view?
# MAGIC * Justification (why this choice?)
# MAGIC * Key syntax element (read_stream vs read, @dlt.table vs @dlt.view)

# COMMAND ----------

# DBTITLE 1,Exercise 2: Expectation Configuration
# MAGIC %md
# MAGIC ## Exercise 2: Expectation Configuration
# MAGIC
# MAGIC **Scenario**: You're building a pipeline to process order data. Configure expectations for the following data quality rules:
# MAGIC
# MAGIC 1. **Rule 1**: Order amount must be positive. Invalid orders should be dropped silently.
# MAGIC
# MAGIC 2. **Rule 2**: Customer ID must not be null. Any null customer_id should fail the pipeline immediately.
# MAGIC
# MAGIC 3. **Rule 3**: Order date should be within the last 2 years. Violations should be logged but rows kept.
# MAGIC
# MAGIC **For each rule, provide**:
# MAGIC * Expectation decorator (@dlt.expect, @dlt.expect_or_drop, or @dlt.expect_or_fail)
# MAGIC * Constraint name
# MAGIC * SQL expression
# MAGIC * Expected behavior on violation
# MAGIC
# MAGIC **Additional question**: If a single row violates both Rule 2 (fail) and Rule 1 (drop), what happens to the pipeline?

# COMMAND ----------

# DBTITLE 1,Exercise 3: AUTO CDC Configuration (SCD Type 1)
# MAGIC %md
# MAGIC ## Exercise 3: AUTO CDC Configuration (SCD Type 1)
# MAGIC
# MAGIC **Scenario**: You have a CDC stream from a MySQL database containing customer data changes. Configure AUTO CDC to maintain **current state only** (SCD Type 1):
# MAGIC
# MAGIC **Requirements**:
# MAGIC * Track only current customer state (no history)
# MAGIC * Handle INSERT, UPDATE, and DELETE operations
# MAGIC * Use customer_id as the primary key
# MAGIC * Order changes by updated_at timestamp
# MAGIC
# MAGIC **Provide**:
# MAGIC 1. Complete Python code using dlt.create_streaming_table and dlt.apply_changes
# MAGIC 2. Explanation of each parameter
# MAGIC 3. What happens when two CDC events have the same customer_id but different updated_at values?
# MAGIC 4. How are DELETE operations handled in SCD Type 1?

# COMMAND ----------

# DBTITLE 1,Exercise 4: Python to SQL Syntax Conversion
# MAGIC %md
# MAGIC ## Exercise 4: Python to SQL Syntax Conversion
# MAGIC
# MAGIC **Scenario**: Convert the following Python SDP code to equivalent SQL:
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC @dlt.table(
# MAGIC     comment="Filtered purchase events",
# MAGIC     table_properties={"quality": "silver"}
# MAGIC )
# MAGIC @dlt.expect_or_drop("valid_amount", "amount > 0")
# MAGIC @dlt.expect_or_drop("valid_date", "event_date IS NOT NULL")
# MAGIC def silver_purchases():
# MAGIC     return (
# MAGIC         dlt.read_stream("bronze_events")
# MAGIC         .filter(col("event_type") == "purchase")
# MAGIC         .select("event_id", "customer_id", "amount", "event_date")
# MAGIC     )
# MAGIC ```
# MAGIC
# MAGIC **Provide**:
# MAGIC * Complete SQL CREATE STREAMING TABLE statement
# MAGIC * Include comment, expectations with correct violation handling, and column selection
# MAGIC * Ensure syntax is valid Databricks SQL

# COMMAND ----------

# DBTITLE 1,Exercise 5: Pipeline Dependency Graph
# MAGIC %md
# MAGIC ## Exercise 5: Pipeline Dependency Graph and Execution Order
# MAGIC
# MAGIC **Scenario**: A pipeline has the following tables:
# MAGIC
# MAGIC * **bronze_events**: Reads from S3 files using Auto Loader
# MAGIC * **silver_purchases**: Reads bronze_events (streaming), filters for event_type = 'purchase'
# MAGIC * **silver_returns**: Reads bronze_events (streaming), filters for event_type = 'return'
# MAGIC * **gold_daily_summary**: Reads silver_purchases (batch), aggregates by day
# MAGIC * **gold_customer_metrics**: Reads both silver_purchases and silver_returns (batch)
# MAGIC
# MAGIC **Questions**:
# MAGIC 1. Draw the dependency graph (which tables depend on which)
# MAGIC 2. If bronze_events fails, which tables cannot run?
# MAGIC 3. If silver_purchases fails, which tables are affected?
# MAGIC 4. Can silver_purchases and silver_returns run in parallel? Why or why not?
# MAGIC 5. Which tables should use streaming tables vs materialized views? Justify each.

# COMMAND ----------

# DBTITLE 1,Exercise 6: Development vs Production Mode
# MAGIC %md
# MAGIC ## Exercise 6: Development vs Production Mode
# MAGIC
# MAGIC **Scenario**: You're developing a new SDP pipeline with 8 tables. Currently in development mode.
# MAGIC
# MAGIC **Questions**:
# MAGIC
# MAGIC 1. List 3 key differences between development and production mode
# MAGIC
# MAGIC 2. During development, a table fails due to a data quality issue. What happens to:
# MAGIC    * The failed table
# MAGIC    * Downstream dependent tables
# MAGIC    * Independent tables in other branches
# MAGIC
# MAGIC 3. You're ready to deploy to production. What configuration changes are needed?
# MAGIC
# MAGIC 4. In production mode, a transient network error causes a table to fail. What happens differently than in development mode?
# MAGIC
# MAGIC 5. Should you run pipelines with continuous=true in development mode? Why or why not?

# COMMAND ----------

# DBTITLE 1,Exercise 7: Unity Catalog Permissions for Pipelines
# MAGIC %md
# MAGIC ## Exercise 7: Unity Catalog Permissions for Pipelines
# MAGIC
# MAGIC **Scenario**: A data engineer tries to create an SDP pipeline with:
# MAGIC * Target: analytics.sales
# MAGIC * Service principal: pipeline_sp
# MAGIC
# MAGIC The pipeline fails with "Permission denied" when trying to create tables.
# MAGIC
# MAGIC **Tasks**:
# MAGIC
# MAGIC 1. List ALL Unity Catalog permissions required for the service principal to run this pipeline successfully
# MAGIC
# MAGIC 2. Write the GRANT statements to provide these permissions
# MAGIC
# MAGIC 3. The pipeline also needs to read source data from landing.raw.events (external table). What additional permission is needed?
# MAGIC
# MAGIC 4. After the pipeline creates tables, who owns them? Can other users query these tables by default?

# COMMAND ----------

# DBTITLE 1,Exercise 8: Reading Patterns (Batch vs Stream)
# MAGIC %md
# MAGIC ## Exercise 8: Reading Patterns (Batch vs Stream)
# MAGIC
# MAGIC **Scenario**: A pipeline has the following tables:
# MAGIC
# MAGIC * **bronze_raw** (streaming table): Raw data from S3
# MAGIC * **silver_clean** (streaming table): Cleaned data from bronze_raw
# MAGIC * **gold_summary** (materialized view): Daily aggregates from silver_clean
# MAGIC
# MAGIC **Questions**:
# MAGIC
# MAGIC 1. Can gold_summary use `dlt.read("silver_clean")`? What does this do?
# MAGIC
# MAGIC 2. Can gold_summary use `dlt.read_stream("silver_clean")`? Why or why not?
# MAGIC
# MAGIC 3. Can silver_clean use `dlt.read("bronze_raw")`? What would happen?
# MAGIC
# MAGIC 4. If you want gold_summary to incrementally process only new rows from silver_clean, what should you do?
# MAGIC
# MAGIC 5. What's the difference between these two approaches:
# MAGIC    ```python
# MAGIC    # Approach A
# MAGIC    @dlt.view
# MAGIC    def summary():
# MAGIC        return dlt.read("silver_clean").groupBy("date").agg(sum("amount"))
# MAGIC    
# MAGIC    # Approach B  
# MAGIC    @dlt.table
# MAGIC    def summary():
# MAGIC        return dlt.read_stream("silver_clean").groupBy("date").agg(sum("amount"))
# MAGIC    ```

# COMMAND ----------

# DBTITLE 1,MCQs 1-3
# MAGIC %md
# MAGIC ## Multiple Choice Questions
# MAGIC
# MAGIC ### MCQ 1: Streaming Table vs Materialized View
# MAGIC
# MAGIC **Question**: You need to process customer transaction data with the following requirements:
# MAGIC * Source: Streaming table with 10M transactions/day
# MAGIC * Need to calculate total spending per customer
# MAGIC * Query pattern: Users frequently ask "What is customer X's total lifetime spending?"
# MAGIC * Data is append-only (transactions never updated)
# MAGIC
# MAGIC Which approach is correct?
# MAGIC
# MAGIC **A.** Use a streaming table with windowed aggregation:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def customer_spending():
# MAGIC     return dlt.read_stream("transactions") \
# MAGIC         .groupBy("customer_id", window("transaction_time", "1 day")) \
# MAGIC         .agg(sum("amount"))
# MAGIC ```
# MAGIC
# MAGIC **B.** Use a streaming table with non-windowed aggregation:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def customer_spending():
# MAGIC     return dlt.read_stream("transactions") \
# MAGIC         .groupBy("customer_id") \
# MAGIC         .agg(sum("amount"))
# MAGIC ```
# MAGIC
# MAGIC **C.** Use a materialized view:
# MAGIC ```python
# MAGIC @dlt.view
# MAGIC def customer_spending():
# MAGIC     return dlt.read("transactions") \
# MAGIC         .groupBy("customer_id") \
# MAGIC         .agg(sum("amount"))
# MAGIC ```
# MAGIC
# MAGIC **D.** Use AUTO CDC:
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="customer_spending",
# MAGIC     source="transactions",
# MAGIC     keys=["customer_id"],
# MAGIC     sequence_by="transaction_time",
# MAGIC     stored_as_scd_type=1
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 2: Expectation Enforcement
# MAGIC
# MAGIC **Question**: A pipeline has the following expectations on an orders table:
# MAGIC
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC @dlt.expect("positive_amount", "amount > 0")
# MAGIC @dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
# MAGIC @dlt.expect_or_fail("recent_date", "order_date >= '2020-01-01'")
# MAGIC def orders():
# MAGIC     return dlt.read_stream("raw_orders")
# MAGIC ```
# MAGIC
# MAGIC A batch of 1000 rows arrives:
# MAGIC * 950 rows: All expectations pass
# MAGIC * 30 rows: amount <= 0 (violate positive_amount)
# MAGIC * 15 rows: customer_id IS NULL (violate valid_customer)
# MAGIC * 5 rows: order_date < '2020-01-01' (violate recent_date)
# MAGIC
# MAGIC What happens?
# MAGIC
# MAGIC **A.** Pipeline fails immediately. No rows written.
# MAGIC
# MAGIC **B.** Pipeline continues. 950 rows written. 50 rows dropped. Violations logged.
# MAGIC
# MAGIC **C.** Pipeline continues. 980 rows written (950 + 30 with negative amounts). 15 rows dropped. 5 violations logged.
# MAGIC
# MAGIC **D.** Pipeline fails on first violation (row with order_date < 2020). No rows written.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 3: AUTO CDC SCD Types
# MAGIC
# MAGIC **Question**: You're implementing AUTO CDC to track product price changes. Requirements:
# MAGIC * Need to know the price of any product at any historical date
# MAGIC * Compliance requires 7-year price history
# MAGIC * Products can be deleted (discontinued)
# MAGIC * Need to query "What was product X's price on 2023-05-15?"
# MAGIC
# MAGIC Which configuration is correct?
# MAGIC
# MAGIC **A.**
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="products",
# MAGIC     source="product_cdc",
# MAGIC     keys=["product_id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type=1
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **B.**
# MAGIC ```python
# MAGIC dlt.apply_changes(
# MAGIC     target="products",
# MAGIC     source="product_cdc",
# MAGIC     keys=["product_id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type=2
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **C.**
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def products():
# MAGIC     return dlt.read_stream("product_cdc")
# MAGIC ```
# MAGIC
# MAGIC **D.**
# MAGIC ```python
# MAGIC @dlt.view
# MAGIC def products():
# MAGIC     return dlt.read("product_cdc") \
# MAGIC         .groupBy("product_id") \
# MAGIC         .agg(max("updated_at").alias("last_update"))
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,MCQs 4-5
# MAGIC %md
# MAGIC ### MCQ 4: Development vs Production Mode
# MAGIC
# MAGIC **Question**: A pipeline with 6 tables is deployed to production. During a run, table 3 fails due to a transient network error. What happens in production mode vs development mode?
# MAGIC
# MAGIC **A.** Both modes: Pipeline stops immediately. All tables marked failed.
# MAGIC
# MAGIC **B.** Production: Automatic retry up to 3 times. Development: Fails immediately, no retry.
# MAGIC
# MAGIC **C.** Production: Continues processing other tables. Development: Stops entire pipeline.
# MAGIC
# MAGIC **D.** Production: Rolls back all changes. Development: Keeps partial results.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 5: Reading Materialized Views as Streams
# MAGIC
# MAGIC **Question**: A pipeline has this code:
# MAGIC
# MAGIC ```python
# MAGIC @dlt.view
# MAGIC def daily_summary():
# MAGIC     return dlt.read("transactions").groupBy("date").agg(sum("amount"))
# MAGIC
# MAGIC @dlt.table
# MAGIC def enriched_summary():
# MAGIC     return dlt.read_stream("daily_summary").join(lookup_table, "date")
# MAGIC ```
# MAGIC
# MAGIC What happens when the pipeline runs?
# MAGIC
# MAGIC **A.** Pipeline succeeds. enriched_summary processes daily_summary as a stream.
# MAGIC
# MAGIC **B.** Pipeline fails. Cannot read materialized views as streams.
# MAGIC
# MAGIC **C.** Pipeline succeeds but enriched_summary recomputes entirely on each run.
# MAGIC
# MAGIC **D.** Pipeline fails. Materialized views cannot be joined with other tables.

# COMMAND ----------

# DBTITLE 1,Challenge: Multi-Layer Pipeline Design
# MAGIC %md
# MAGIC ## Challenge: Multi-Layer Pipeline Design
# MAGIC
# MAGIC **Scenario**: Design a complete SDP pipeline for an e-commerce platform with the following requirements:
# MAGIC
# MAGIC ### Data Sources
# MAGIC
# MAGIC **Source 1: Order events (S3)**
# MAGIC * Files land in s3://orders-raw/ continuously
# MAGIC * JSON format: {order_id, customer_id, product_id, amount, order_time, status}
# MAGIC * ~50K events/hour
# MAGIC * Need real-time processing
# MAGIC
# MAGIC **Source 2: Customer CDC (MySQL via Debezium)**
# MAGIC * CDC stream with customer updates (INSERT, UPDATE, DELETE)
# MAGIC * Format: {customer_id, name, email, segment, address, updated_at, operation}
# MAGIC * Need to maintain current customer state only (no history)
# MAGIC
# MAGIC **Source 3: Product catalog (daily batch)**
# MAGIC * CSV file uploaded daily to s3://products/
# MAGIC * Format: {product_id, name, category, price, active_flag}
# MAGIC * Complete refresh each day
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC 1. **Bronze layer**: Ingest all three sources with minimal transformation
# MAGIC
# MAGIC 2. **Silver layer**:
# MAGIC    * Validated orders (amount > 0, customer_id NOT NULL, order_date within last year)
# MAGIC    * Drop invalid orders silently
# MAGIC    * Current customer dimension (from CDC)
# MAGIC    * Active products only (where active_flag = true)
# MAGIC
# MAGIC 3. **Gold layer**:
# MAGIC    * Daily revenue by product category (updated continuously as orders arrive)
# MAGIC    * Customer lifetime value (total spending per customer, recomputed daily)
# MAGIC
# MAGIC ### Deliverables
# MAGIC
# MAGIC Provide complete Python code for:
# MAGIC
# MAGIC 1. **Table type selection**: For each of the 8 tables (3 bronze, 3 silver, 2 gold), specify:
# MAGIC    * Streaming table or materialized view
# MAGIC    * Justification
# MAGIC
# MAGIC 2. **Implementation**: Write complete code for at least:
# MAGIC    * One bronze table (orders)
# MAGIC    * One silver table (validated orders with expectations)
# MAGIC    * One silver table (customer dimension with AUTO CDC)
# MAGIC    * One gold table (daily revenue by category)
# MAGIC
# MAGIC 3. **Dependency graph**: Draw or describe the dependencies between all 8 tables
# MAGIC
# MAGIC 4. **Configuration**: Recommend development vs production mode settings for:
# MAGIC    * Initial development
# MAGIC    * Production deployment

# COMMAND ----------

# DBTITLE 1,Applied: Table Type Selection Decision Tree
# MAGIC %md
# MAGIC ## Applied: Table Type Selection Decision Tree
# MAGIC
# MAGIC **Task**: Create a decision tree to determine table type for any transformation requirement.
# MAGIC
# MAGIC Your decision tree should guide users through these questions:
# MAGIC
# MAGIC 1. **Source characteristics**
# MAGIC    * Does data arrive continuously or in batches?
# MAGIC    * Is the source append-only or does it include updates/deletes?
# MAGIC
# MAGIC 2. **Processing requirements**
# MAGIC    * Need real-time/low-latency processing?
# MAGIC    * Need to maintain historical changes (SCD Type 2)?
# MAGIC    * Just need current state?
# MAGIC
# MAGIC 3. **Downstream consumers**
# MAGIC    * Do downstream tables need streaming input?
# MAGIC    * Is periodic batch processing sufficient?
# MAGIC
# MAGIC 4. **Transformation complexity**
# MAGIC    * Simple filtering/projection?
# MAGIC    * Aggregation with GROUP BY?
# MAGIC    * Joins (stream-static vs stream-stream)?
# MAGIC
# MAGIC **Deliverable**: Create a decision tree that leads to one of these outcomes:
# MAGIC * **Streaming table** with dlt.read_stream()
# MAGIC * **Streaming table with AUTO CDC** (SCD Type 1)
# MAGIC * **Streaming table with AUTO CDC** (SCD Type 2)
# MAGIC * **Materialized view** with dlt.read()
# MAGIC
# MAGIC **Test your decision tree** with these scenarios:
# MAGIC
# MAGIC Scenario A: Clickstream events from Kafka, filter to purchase events, downstream needs real-time → ?
# MAGIC
# MAGIC Scenario B: Daily aggregation of sales by region, source is streaming table → ?
# MAGIC
# MAGIC Scenario C: Employee record CDC stream, need full history of all changes → ?
# MAGIC
# MAGIC Scenario D: Product catalog CDC stream, need current state only → ?

# Databricks notebook source
# DBTITLE 1,Topic 3: Delta Lake
# MAGIC %md
# MAGIC # Topic 3: Delta Lake
# MAGIC
# MAGIC ## Introduction
# MAGIC
# MAGIC Delta Lake is the foundation of the Lakehouse. This topic has the highest exam density of technical commands you must memorize. Since you identified Delta as a weakness, this section requires focused study.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC * CREATE TABLE syntax and options
# MAGIC * INSERT, UPDATE, DELETE, MERGE operations
# MAGIC * VACUUM and time travel
# MAGIC * OPTIMIZE and Z-ordering
# MAGIC * Table constraints and metadata
# MAGIC * Transaction log fundamentals
# MAGIC
# MAGIC ### Why This Matters for the Exam
# MAGIC * 20% of exam questions (9 questions)
# MAGIC * Heavily tested: MERGE, VACUUM retention, OPTIMIZE
# MAGIC * Many syntax-specific questions
# MAGIC * Common mistake: confusing VACUUM behavior
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 1: CREATE TABLE
# MAGIC %md
# MAGIC ## Concept 1: CREATE TABLE (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### Basic Syntax
# MAGIC
# MAGIC ```sql
# MAGIC -- Managed table (Unity Catalog stores data)
# MAGIC CREATE TABLE catalog.schema.table_name (
# MAGIC     customer_id INT NOT NULL,
# MAGIC     name STRING,
# MAGIC     amount DOUBLE,
# MAGIC     date DATE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC ```
# MAGIC
# MAGIC ### CREATE OR REPLACE TABLE
# MAGIC
# MAGIC ```sql
# MAGIC -- Atomically replace existing table
# MAGIC CREATE OR REPLACE TABLE catalog.schema.table_name (
# MAGIC     customer_id INT,
# MAGIC     amount DOUBLE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC ```
# MAGIC
# MAGIC ### CREATE TABLE AS SELECT (CTAS)
# MAGIC
# MAGIC ```sql
# MAGIC -- Create from query results
# MAGIC CREATE TABLE catalog.schema.new_table
# MAGIC AS SELECT * FROM catalog.schema.source_table
# MAGIC WHERE date >= '2024-01-01';
# MAGIC ```
# MAGIC
# MAGIC ### CREATE TABLE IF NOT EXISTS
# MAGIC
# MAGIC ```sql
# MAGIC -- Only create if doesn't exist
# MAGIC CREATE TABLE IF NOT EXISTS catalog.schema.table_name (
# MAGIC     id INT,
# MAGIC     value STRING
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Table Options
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE catalog.schema.table_name (
# MAGIC     id INT,
# MAGIC     data STRING,
# MAGIC     created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (created_at)
# MAGIC COMMENT 'Customer transaction data'
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "What happens if you CREATE TABLE on an existing table?"
# MAGIC * Answer: Fails with error (use CREATE OR REPLACE or CREATE IF NOT EXISTS)
# MAGIC
# MAGIC **Common trap**: Forgetting USING DELTA (defaults to Parquet in some contexts)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 2: INSERT, UPDATE, DELETE
# MAGIC %md
# MAGIC ## Concept 2: INSERT, UPDATE, DELETE
# MAGIC
# MAGIC ### INSERT INTO
# MAGIC
# MAGIC ```sql
# MAGIC -- Insert specific values
# MAGIC INSERT INTO catalog.schema.table_name
# MAGIC VALUES (1, 'Alice', 100.0, '2024-01-01');
# MAGIC
# MAGIC -- Insert from query
# MAGIC INSERT INTO catalog.schema.target
# MAGIC SELECT * FROM catalog.schema.source
# MAGIC WHERE date = '2024-01-01';
# MAGIC
# MAGIC -- Insert specific columns
# MAGIC INSERT INTO catalog.schema.table_name (id, name)
# MAGIC VALUES (1, 'Alice');
# MAGIC ```
# MAGIC
# MAGIC ### INSERT OVERWRITE
# MAGIC
# MAGIC ```sql
# MAGIC -- Replace all data
# MAGIC INSERT OVERWRITE catalog.schema.table_name
# MAGIC SELECT * FROM catalog.schema.source;
# MAGIC
# MAGIC -- Replace specific partition (if partitioned)
# MAGIC INSERT OVERWRITE catalog.schema.table_name
# MAGIC PARTITION (year='2024', month='01')
# MAGIC SELECT * FROM catalog.schema.source;
# MAGIC ```
# MAGIC
# MAGIC ### UPDATE
# MAGIC
# MAGIC ```sql
# MAGIC -- Update all rows
# MAGIC UPDATE catalog.schema.table_name
# MAGIC SET amount = amount * 1.1;
# MAGIC
# MAGIC -- Update with WHERE clause
# MAGIC UPDATE catalog.schema.table_name
# MAGIC SET status = 'inactive'
# MAGIC WHERE last_login < '2023-01-01';
# MAGIC
# MAGIC # Update multiple columns
# MAGIC UPDATE catalog.schema.table_name
# MAGIC SET amount = 0, status = 'cancelled'
# MAGIC WHERE customer_id = 123;
# MAGIC ```
# MAGIC
# MAGIC ### DELETE
# MAGIC
# MAGIC ```python
# MAGIC # Delete with condition
# MAGIC DELETE FROM catalog.schema.table_name
# MAGIC WHERE date < '2023-01-01';
# MAGIC
# MAGIC # Delete all (dangerous!)
# MAGIC DELETE FROM catalog.schema.table_name;
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **INSERT INTO vs INSERT OVERWRITE**:
# MAGIC * INSERT INTO: Appends data
# MAGIC * INSERT OVERWRITE: Replaces data (atomic operation)
# MAGIC
# MAGIC **Common mistake**: UPDATE and DELETE create new versions but do not reclaim space until VACUUM
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 3: MERGE (VERY HIGH EXAM FREQUENCY)
# MAGIC %md
# MAGIC ## Concept 3: MERGE (VERY HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### What is MERGE?
# MAGIC
# MAGIC MERGE (upsert) combines INSERT and UPDATE in a single atomic operation. This is one of the most tested Delta Lake features.
# MAGIC
# MAGIC ### Minimal MERGE Syntax (What's Actually Required)
# MAGIC
# MAGIC ```sql
# MAGIC -- Absolute minimum - no aliases, single operation
# MAGIC MERGE INTO catalog.schema.target
# MAGIC USING catalog.schema.source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET amount = source.amount;
# MAGIC ```
# MAGIC
# MAGIC **Required components**:
# MAGIC * `MERGE INTO` with target table name
# MAGIC * `USING` with source table name
# MAGIC * `ON` condition (how to match rows)
# MAGIC * At least one `WHEN` clause (MATCHED or NOT MATCHED)
# MAGIC
# MAGIC **Optional components**:
# MAGIC * Aliases (AS t, AS s)
# MAGIC * Both MATCHED and NOT MATCHED clauses
# MAGIC * Multiple conditions
# MAGIC * DELETE operations
# MAGIC
# MAGIC ### Basic MERGE Syntax
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO catalog.schema.target AS t
# MAGIC USING catalog.schema.source AS s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET t.amount = s.amount, t.updated_at = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (customer_id, amount, updated_at)
# MAGIC     VALUES (s.customer_id, s.amount, current_timestamp());
# MAGIC ```
# MAGIC
# MAGIC ### MERGE with Multiple Conditions
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO catalog.schema.target AS t
# MAGIC USING catalog.schema.source AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED AND s.delete_flag = true THEN
# MAGIC     DELETE
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT *;
# MAGIC ```
# MAGIC
# MAGIC ### MERGE with UPDATE *
# MAGIC
# MAGIC ```sql
# MAGIC -- Update all columns from source
# MAGIC MERGE INTO target AS t
# MAGIC USING source AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC ```
# MAGIC
# MAGIC ### MERGE with Deduplicated Source (CRITICAL)
# MAGIC
# MAGIC When source contains duplicates, deduplicate BEFORE merging:
# MAGIC
# MAGIC ```sql
# MAGIC -- Deduplicate source using window functions
# MAGIC MERGE INTO catalog.schema.target AS t
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         customer_id,
# MAGIC         amount,
# MAGIC         updated_at
# MAGIC     FROM catalog.schema.source
# MAGIC     QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
# MAGIC ) AS s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC ```
# MAGIC
# MAGIC ### MERGE with Aggregated Source
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO catalog.schema.daily_summary AS t
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         customer_id,
# MAGIC         SUM(amount) as total_amount,
# MAGIC         COUNT(*) as txn_count
# MAGIC     FROM catalog.schema.transactions
# MAGIC     WHERE date = current_date()
# MAGIC     GROUP BY customer_id
# MAGIC ) AS s
# MAGIC ON t.customer_id = s.customer_id AND t.date = current_date()
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET 
# MAGIC         t.total_amount = s.total_amount,
# MAGIC         t.txn_count = s.txn_count
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (customer_id, date, total_amount, txn_count)
# MAGIC     VALUES (s.customer_id, current_date(), s.total_amount, s.txn_count);
# MAGIC ```
# MAGIC
# MAGIC ### MERGE Behavior
# MAGIC
# MAGIC **Key points**:
# MAGIC * MERGE is atomic (all or nothing)
# MAGIC * Source MUST have unique keys - duplicates will cause the operation to fail with a cardinality violation error
# MAGIC * To handle duplicates, deduplicate source first using window functions (ROW_NUMBER, QUALIFY)
# MAGIC * Can combine UPDATE, INSERT, DELETE in one operation
# MAGIC * Creates a new table version
# MAGIC
# MAGIC ### PySpark MERGE (DeltaTable API)
# MAGIC
# MAGIC ```python
# MAGIC from delta.tables import DeltaTable
# MAGIC
# MAGIC # Load target as DeltaTable
# MAGIC target = DeltaTable.forName(spark, "catalog.schema.target")
# MAGIC source = spark.table("catalog.schema.source")
# MAGIC
# MAGIC # Perform merge
# MAGIC target.alias("t").merge(
# MAGIC     source.alias("s"),
# MAGIC     "t.customer_id = s.customer_id"
# MAGIC ).whenMatchedUpdate(
# MAGIC     set = {
# MAGIC         "amount": "s.amount",
# MAGIC         "updated_at": "current_timestamp()"
# MAGIC     }
# MAGIC ).whenNotMatchedInsert(
# MAGIC     values = {
# MAGIC         "customer_id": "s.customer_id",
# MAGIC         "amount": "s.amount",
# MAGIC         "updated_at": "current_timestamp()"
# MAGIC     }
# MAGIC ).execute()
# MAGIC ```
# MAGIC
# MAGIC ### Exam Question Patterns
# MAGIC
# MAGIC **Pattern 1**: "How do you upsert data into a Delta table?"
# MAGIC * Answer: Use MERGE operation
# MAGIC
# MAGIC **Pattern 2**: "What happens if source has multiple rows matching the same target row?"
# MAGIC * Answer: Operation fails with cardinality violation error. Must deduplicate source before MERGE.
# MAGIC
# MAGIC **Pattern 3**: "Can you delete rows using MERGE?"
# MAGIC * Answer: Yes, using WHEN MATCHED AND condition THEN DELETE
# MAGIC
# MAGIC ### Common Mistakes
# MAGIC
# MAGIC * Not deduplicating source data (MERGE fails on duplicate keys)
# MAGIC * Not handling both MATCHED and NOT MATCHED cases when needed
# MAGIC * Assuming aliases are required (they're optional but improve readability)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 4: Time Travel & VACUUM
# MAGIC %md
# MAGIC ## Concept 4: Time Travel & VACUUM (VERY HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### Time Travel
# MAGIC
# MAGIC Delta Lake maintains versioned history, allowing queries of previous table states.
# MAGIC
# MAGIC #### Query by Version
# MAGIC
# MAGIC ```sql
# MAGIC -- Query specific version
# MAGIC SELECT * FROM catalog.schema.table_name VERSION AS OF 5;
# MAGIC
# MAGIC -- Query by timestamp
# MAGIC SELECT * FROM catalog.schema.table_name TIMESTAMP AS OF '2024-01-15';
# MAGIC ```
# MAGIC
# MAGIC #### Query by Date String
# MAGIC
# MAGIC ```sql
# MAGIC -- Various timestamp formats
# MAGIC SELECT * FROM table_name TIMESTAMP AS OF '2024-01-15 10:30:00';
# MAGIC SELECT * FROM table_name TIMESTAMP AS OF '2024-01-15';
# MAGIC ```
# MAGIC
# MAGIC #### PySpark Time Travel
# MAGIC
# MAGIC ```python
# MAGIC # Version
# MAGIC df = spark.read.format("delta").option("versionAsOf", 5).load("/path")
# MAGIC
# MAGIC # Timestamp
# MAGIC df = spark.read.format("delta") \
# MAGIC     .option("timestampAsOf", "2024-01-15") \
# MAGIC     .load("/path")
# MAGIC ```
# MAGIC
# MAGIC ### DESCRIBE HISTORY
# MAGIC
# MAGIC ```sql
# MAGIC -- View table history
# MAGIC DESCRIBE HISTORY catalog.schema.table_name;
# MAGIC
# MAGIC -- Limit results
# MAGIC DESCRIBE HISTORY catalog.schema.table_name LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC ### VACUUM (CRITICAL FOR EXAM)
# MAGIC
# MAGIC **What VACUUM does**:
# MAGIC * Removes old data files not referenced by the current table version
# MAGIC * Reclaims storage space
# MAGIC * Makes old versions inaccessible
# MAGIC
# MAGIC #### VACUUM Syntax
# MAGIC
# MAGIC ```sql
# MAGIC -- Remove files older than retention period (default 7 days)
# MAGIC VACUUM catalog.schema.table_name;
# MAGIC
# MAGIC -- Specify custom retention (in hours)
# MAGIC VACUUM catalog.schema.table_name RETAIN 168 HOURS;  -- 7 days
# MAGIC VACUUM catalog.schema.table_name RETAIN 0 HOURS;    -- Danger!
# MAGIC
# MAGIC -- Dry run (show what would be deleted)
# MAGIC VACUUM catalog.schema.table_name DRY RUN;
# MAGIC ```
# MAGIC
# MAGIC #### VACUUM Retention Rules
# MAGIC
# MAGIC | Retention | Behavior | Use Case |
# MAGIC |-----------|----------|----------|
# MAGIC | Default (7 days) | Keep 7 days of history | Standard production |
# MAGIC | 168 hours (7 days) | Same as default | Explicit retention |
# MAGIC | 0 hours | Delete all old files | **Dangerous!** Requires override |
# MAGIC | 30+ days | Extended history | Compliance requirements |
# MAGIC
# MAGIC #### Enabling 0-Hour VACUUM
# MAGIC
# MAGIC ```python
# MAGIC # Required to VACUUM with 0 retention
# MAGIC spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
# MAGIC
# MAGIC # Then VACUUM
# MAGIC spark.sql("VACUUM catalog.schema.table_name RETAIN 0 HOURS")
# MAGIC ```
# MAGIC
# MAGIC ### Time Travel Limitations
# MAGIC
# MAGIC **After VACUUM**:
# MAGIC * Cannot query versions older than retention period
# MAGIC * Attempting to query old version raises error
# MAGIC
# MAGIC **Example**:
# MAGIC ```sql
# MAGIC -- This will fail if version 3 was vacuumed
# MAGIC SELECT * FROM table VERSION AS OF 3;
# MAGIC -- Error: "The transaction log has been truncated"
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips (CRITICAL)
# MAGIC
# MAGIC **Most common question**: "What is the default VACUUM retention period?"
# MAGIC * Answer: 7 days (168 hours)
# MAGIC
# MAGIC **Second most common**: "What happens to time travel after VACUUM?"
# MAGIC * Answer: Cannot query versions older than retention period
# MAGIC
# MAGIC **Common trap**: "Can you VACUUM with 0 hours retention?"
# MAGIC * Answer: Yes, but requires setting `spark.databricks.delta.retentionDurationCheck.enabled=false`
# MAGIC
# MAGIC **Remember**: VACUUM removes files, OPTIMIZE rearranges files
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 5: OPTIMIZE & Z-ORDERING
# MAGIC %md
# MAGIC ## Concept 5: OPTIMIZE & Z-ORDERING (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### OPTIMIZE
# MAGIC
# MAGIC **What OPTIMIZE does**:
# MAGIC * Compacts small files into larger files
# MAGIC * Improves read performance
# MAGIC * Does NOT delete old files (use VACUUM after)
# MAGIC
# MAGIC #### Basic OPTIMIZE
# MAGIC
# MAGIC ```sql
# MAGIC -- Optimize entire table
# MAGIC OPTIMIZE catalog.schema.table_name;
# MAGIC
# MAGIC -- Optimize specific partition
# MAGIC OPTIMIZE catalog.schema.table_name
# MAGIC WHERE year = '2024' AND month = '01';
# MAGIC ```
# MAGIC
# MAGIC ### Z-ORDERING
# MAGIC
# MAGIC **What Z-ORDERING does**:
# MAGIC * Co-locates related data in same files
# MAGIC * Improves performance for filters on specified columns
# MAGIC * Works best with high-cardinality columns
# MAGIC
# MAGIC #### Z-ORDER Syntax
# MAGIC
# MAGIC ```sql
# MAGIC -- Z-order by single column
# MAGIC OPTIMIZE catalog.schema.table_name
# MAGIC ZORDER BY (customer_id);
# MAGIC
# MAGIC -- Z-order by multiple columns
# MAGIC OPTIMIZE catalog.schema.table_name
# MAGIC ZORDER BY (customer_id, product_id);
# MAGIC ```
# MAGIC
# MAGIC ### When to Use Z-ORDERING
# MAGIC
# MAGIC **Good candidates**:
# MAGIC * High-cardinality columns (customer_id, product_id)
# MAGIC * Columns frequently used in WHERE clauses
# MAGIC * Columns used in JOINs
# MAGIC
# MAGIC **Avoid Z-ordering on**:
# MAGIC * Low-cardinality columns (status, category with few values)
# MAGIC * Partition columns (already organized)
# MAGIC * Columns rarely filtered
# MAGIC
# MAGIC ### OPTIMIZE vs VACUUM
# MAGIC
# MAGIC | Operation | Purpose | Deletes Files | When to Use |
# MAGIC |-----------|---------|---------------|-------------|
# MAGIC | **OPTIMIZE** | Compact small files | No | Improve read performance |
# MAGIC | **VACUUM** | Remove old files | Yes | Reclaim storage space |
# MAGIC
# MAGIC **Typical workflow**:
# MAGIC ```sql
# MAGIC -- Step 1: Compact files
# MAGIC OPTIMIZE catalog.schema.table_name ZORDER BY (customer_id);
# MAGIC
# MAGIC -- Step 2: Clean up old files (after retention period)
# MAGIC VACUUM catalog.schema.table_name;
# MAGIC ```
# MAGIC
# MAGIC ### Auto-Optimize
# MAGIC
# MAGIC ```sql
# MAGIC -- Enable auto-optimize on table
# MAGIC ALTER TABLE catalog.schema.table_name
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Auto-Optimize properties**:
# MAGIC * `optimizeWrite`: Optimize file sizes during writes
# MAGIC * `autoCompact`: Automatically compact small files after write
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Common question**: "What's the difference between OPTIMIZE and VACUUM?"
# MAGIC * OPTIMIZE: Compacts files, improves reads
# MAGIC * VACUUM: Deletes old files, reclaims storage
# MAGIC
# MAGIC **Z-ORDER tip**: Use on high-cardinality columns frequently in WHERE clauses
# MAGIC
# MAGIC **Remember**: OPTIMIZE does NOT reclaim space; VACUUM does
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 6: Table Constraints
# MAGIC %md
# MAGIC ## Concept 6: Table Constraints
# MAGIC
# MAGIC ### NOT NULL Constraints
# MAGIC
# MAGIC ```sql
# MAGIC -- Define at creation
# MAGIC CREATE TABLE catalog.schema.table_name (
# MAGIC     id INT NOT NULL,
# MAGIC     name STRING NOT NULL,
# MAGIC     amount DOUBLE
# MAGIC );
# MAGIC
# MAGIC -- Add after creation
# MAGIC ALTER TABLE catalog.schema.table_name
# MAGIC ALTER COLUMN name SET NOT NULL;
# MAGIC ```
# MAGIC
# MAGIC ### CHECK Constraints
# MAGIC
# MAGIC ```sql
# MAGIC -- Add CHECK constraint
# MAGIC ALTER TABLE catalog.schema.table_name
# MAGIC ADD CONSTRAINT amount_positive CHECK (amount >= 0);
# MAGIC
# MAGIC -- Multiple conditions
# MAGIC ALTER TABLE catalog.schema.table_name
# MAGIC ADD CONSTRAINT valid_status CHECK (status IN ('active', 'inactive', 'pending'));
# MAGIC ```
# MAGIC
# MAGIC ### Generated Columns
# MAGIC
# MAGIC ```sql
# MAGIC -- Computed column
# MAGIC CREATE TABLE catalog.schema.table_name (
# MAGIC     price DOUBLE,
# MAGIC     quantity INT,
# MAGIC     total DOUBLE GENERATED ALWAYS AS (price * quantity)
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Identity Columns
# MAGIC
# MAGIC ```sql
# MAGIC -- Auto-incrementing ID
# MAGIC CREATE TABLE catalog.schema.table_name (
# MAGIC     id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     name STRING
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Question**: "How do you enforce that a column only contains positive values?"
# MAGIC * Answer: Use CHECK constraint
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Key Takeaways & Exam Focus
# MAGIC %md
# MAGIC ## Key Takeaways & Exam Focus
# MAGIC
# MAGIC ### Most Testable Concepts (Priority Order)
# MAGIC
# MAGIC 1. **MERGE operations** (Very High Frequency)
# MAGIC    * Syntax: ON condition, WHEN MATCHED, WHEN NOT MATCHED
# MAGIC    * Behavior with duplicates
# MAGIC    * Can combine INSERT, UPDATE, DELETE
# MAGIC
# MAGIC 2. **VACUUM retention** (Very High Frequency)
# MAGIC    * Default: 7 days (168 hours)
# MAGIC    * Effect on time travel
# MAGIC    * 0-hour VACUUM requires config override
# MAGIC
# MAGIC 3. **Time travel syntax** (High Frequency)
# MAGIC    * VERSION AS OF
# MAGIC    * TIMESTAMP AS OF
# MAGIC    * DESCRIBE HISTORY
# MAGIC
# MAGIC 4. **OPTIMIZE vs VACUUM** (High Frequency)
# MAGIC    * OPTIMIZE: compact files, no deletion
# MAGIC    * VACUUM: delete old files, reclaim space
# MAGIC    * Z-ORDERING: co-locate data
# MAGIC
# MAGIC 5. **CREATE TABLE options** (Medium Frequency)
# MAGIC    * CREATE OR REPLACE
# MAGIC    * CREATE IF NOT EXISTS
# MAGIC    * PARTITIONED BY
# MAGIC    * TBLPROPERTIES
# MAGIC
# MAGIC ### Commands to Memorize
# MAGIC
# MAGIC **MERGE template**:
# MAGIC ```sql
# MAGIC MERGE INTO target AS t
# MAGIC USING source AS s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC ```
# MAGIC
# MAGIC **VACUUM template**:
# MAGIC ```sql
# MAGIC VACUUM table_name RETAIN 168 HOURS;
# MAGIC ```
# MAGIC
# MAGIC **OPTIMIZE template**:
# MAGIC ```sql
# MAGIC OPTIMIZE table_name ZORDER BY (column1, column2);
# MAGIC ```
# MAGIC
# MAGIC **Time travel template**:
# MAGIC ```sql
# MAGIC SELECT * FROM table VERSION AS OF 5;
# MAGIC SELECT * FROM table TIMESTAMP AS OF '2024-01-15';
# MAGIC ```
# MAGIC
# MAGIC ### Critical Facts to Remember
# MAGIC
# MAGIC * Default VACUUM retention: **7 days**
# MAGIC * MERGE is **atomic** (all or nothing)
# MAGIC * OPTIMIZE does **NOT** delete files
# MAGIC * VACUUM makes old versions **inaccessible**
# MAGIC * Z-ORDER works best on **high-cardinality** columns
# MAGIC * UPDATE/DELETE create new versions but don't reclaim space until VACUUM
# MAGIC
# MAGIC ### Common Exam Traps
# MAGIC
# MAGIC **Trap 1**: "OPTIMIZE reclaims storage"
# MAGIC * False! VACUUM reclaims storage
# MAGIC
# MAGIC **Trap 2**: "MERGE handles duplicate keys in source automatically"
# MAGIC * False! MERGE fails on duplicates. Must deduplicate source first using window functions.
# MAGIC
# MAGIC **Trap 3**: "After VACUUM, can still time travel to any version"
# MAGIC * False! Only versions within retention period
# MAGIC
# MAGIC **Trap 4**: "Z-ORDER on partition columns improves performance"
# MAGIC * False! Partition columns are already organized
# MAGIC
# MAGIC ### Exam Question Patterns
# MAGIC
# MAGIC **Pattern 1**: "How do you upsert data?"
# MAGIC * Answer: MERGE INTO... WHEN MATCHED... WHEN NOT MATCHED
# MAGIC
# MAGIC **Pattern 2**: "What's the default VACUUM retention?"
# MAGIC * Answer: 7 days (168 hours)
# MAGIC
# MAGIC **Pattern 3**: "How do you compact small files?"
# MAGIC * Answer: OPTIMIZE (not VACUUM)
# MAGIC
# MAGIC **Pattern 4**: "Can you query old versions after VACUUM?"
# MAGIC * Answer: Only if within retention period
# MAGIC
# MAGIC ### Delta Lake Operation Comparison
# MAGIC
# MAGIC | Operation | Creates Version | Reclaims Space | Affects Time Travel |
# MAGIC |-----------|----------------|----------------|---------------------|
# MAGIC | INSERT | Yes | No | No |
# MAGIC | UPDATE | Yes | No | No |
# MAGIC | DELETE | Yes | No | No |
# MAGIC | MERGE | Yes | No | No |
# MAGIC | OPTIMIZE | Yes | No | No |
# MAGIC | VACUUM | No | Yes | Yes (limits history) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Additional Resources
# MAGIC
# MAGIC * [Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
# MAGIC * [Delta Lake Best Practices](https://docs.databricks.com/delta/best-practices.html)
# MAGIC * [MERGE Command Reference](https://docs.databricks.com/sql/language-manual/delta-merge-into.html)
# MAGIC
# MAGIC **Next**: Practice with `practice_tasks.py` focusing on MERGE and VACUUM scenarios.

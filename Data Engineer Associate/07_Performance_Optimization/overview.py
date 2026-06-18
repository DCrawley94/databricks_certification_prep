# Databricks notebook source
# DBTITLE 1,Topic 7: Performance Optimization
# MAGIC %md
# MAGIC # Topic 7: Performance Optimization
# MAGIC
# MAGIC ## Introduction
# MAGIC
# MAGIC Performance optimization covers query tuning, data layout strategies, and automated optimization features in Databricks.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC * Liquid Clustering (NEW - replaces Z-Order)
# MAGIC * Predictive Optimization (NEW - automated maintenance)
# MAGIC * Partition strategies
# MAGIC * File sizing and compaction
# MAGIC * Join optimization
# MAGIC * Caching strategies
# MAGIC
# MAGIC ### Why This Matters for the Exam
# MAGIC * Section 6 (Troubleshooting, Monitoring, Optimization): 10% (~5 questions)
# MAGIC * Liquid Clustering and Predictive Optimization newly added (May 2026 guide)
# MAGIC * Data layout optimization heavily tested
# MAGIC * Performance troubleshooting scenarios
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 1: Liquid Clustering (NEW - HIGH EXAM FREQUENCY)
# MAGIC %md
# MAGIC ## Concept 1: Liquid Clustering (NEW - HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### What is Liquid Clustering?
# MAGIC
# MAGIC Liquid Clustering is Databricks' **automatic data layout optimization** that replaces manual partitioning and Z-ordering. Introduced in DBR 13.3+.
# MAGIC
# MAGIC ### Key Characteristics
# MAGIC
# MAGIC **Automatic**:
# MAGIC * No manual OPTIMIZE required
# MAGIC * Adapts to query patterns over time
# MAGIC * Self-tuning based on workload
# MAGIC
# MAGIC **Flexible**:
# MAGIC * Can change clustering keys without rewriting data
# MAGIC * Supports up to 4 clustering columns
# MAGIC * Works with streaming and batch
# MAGIC
# MAGIC **Efficient**:
# MAGIC * Better performance than static partitioning
# MAGIC * Incremental reorganization
# MAGIC * No small file problem
# MAGIC
# MAGIC ### Creating Liquid Clustered Table
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE catalog.schema.orders (
# MAGIC     order_id BIGINT,
# MAGIC     customer_id BIGINT,
# MAGIC     order_date DATE,
# MAGIC     region STRING
# MAGIC )
# MAGIC CLUSTER BY (order_date, region);
# MAGIC ```
# MAGIC
# MAGIC **Key points**:
# MAGIC * `CLUSTER BY` clause defines clustering columns
# MAGIC * Order matters (most selective first)
# MAGIC * Up to 4 columns supported
# MAGIC
# MAGIC ### Converting Existing Table
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE catalog.schema.orders
# MAGIC CLUSTER BY (order_date, region);
# MAGIC ```
# MAGIC
# MAGIC **Note**: Requires rewriting data (runs OPTIMIZE automatically)
# MAGIC
# MAGIC ### Changing Clustering Keys
# MAGIC
# MAGIC ```sql
# MAGIC -- Change clustering columns
# MAGIC ALTER TABLE catalog.schema.orders
# MAGIC CLUSTER BY (customer_id, order_date);
# MAGIC
# MAGIC -- Remove clustering
# MAGIC ALTER TABLE catalog.schema.orders
# MAGIC CLUSTER BY NONE;
# MAGIC ```
# MAGIC
# MAGIC ### Liquid Clustering vs Traditional Approaches
# MAGIC
# MAGIC | Feature | Liquid Clustering | Z-Order | Partitioning |
# MAGIC |---------|------------------|---------|---------------|
# MAGIC | **Maintenance** | Automatic | Manual OPTIMIZE | Manual OPTIMIZE |
# MAGIC | **Flexibility** | Change keys easily | Fixed at creation | Fixed at creation |
# MAGIC | **Query patterns** | Adapts automatically | Static | Static |
# MAGIC | **Small files** | Prevented | Can occur | Can occur |
# MAGIC | **Max columns** | 4 | Unlimited | Unlimited |
# MAGIC | **Exam focus** | High (NEW) | Medium | Medium |
# MAGIC
# MAGIC ### When to Use Liquid Clustering
# MAGIC
# MAGIC **Use Liquid Clustering when**:
# MAGIC * High-cardinality columns (customer_id, product_id)
# MAGIC * Multiple query patterns (don't know which columns to partition)
# MAGIC * Want automatic optimization
# MAGIC * DBR 13.3+ available
# MAGIC
# MAGIC **Avoid when**:
# MAGIC * Need more than 4 clustering columns
# MAGIC * Low-cardinality categorical columns (use partitioning)
# MAGIC * Legacy DBR versions (< 13.3)
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "What is the main advantage of Liquid Clustering over Z-Order?"
# MAGIC * Answer: Automatic maintenance and ability to change clustering keys
# MAGIC
# MAGIC **Question**: "How many clustering columns does Liquid Clustering support?"
# MAGIC * Answer: Up to 4 columns
# MAGIC
# MAGIC **Question**: "How do you enable Liquid Clustering on existing table?"
# MAGIC * Answer: `ALTER TABLE table_name CLUSTER BY (col1, col2)`
# MAGIC
# MAGIC **Common trap**: Thinking Liquid Clustering is the same as partitioning (it's not - it's a replacement for Z-Order with automatic tuning)
# MAGIC
# MAGIC **Memory aid**: "Liquid = Flexible and flowing (adapts automatically)"
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 2: Predictive Optimization (NEW - HIGH EXAM FREQUENCY)
# MAGIC %md
# MAGIC ## Concept 2: Predictive Optimization (NEW - HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### What is Predictive Optimization?
# MAGIC
# MAGIC Predictive Optimization is Databricks' **automated background maintenance** for Delta tables, including compaction, vacuum, and statistics collection.
# MAGIC
# MAGIC ### Key Features
# MAGIC
# MAGIC **Automatic Operations**:
# MAGIC * Small file compaction (replaces manual OPTIMIZE)
# MAGIC * Vacuum (removes old file versions)
# MAGIC * Statistics collection (for CBO)
# MAGIC * Table property updates
# MAGIC
# MAGIC **Intelligent Scheduling**:
# MAGIC * Runs during low-usage periods
# MAGIC * Prioritizes high-value tables
# MAGIC * Avoids impacting active workloads
# MAGIC
# MAGIC **No Manual Intervention**:
# MAGIC * Set once, runs forever
# MAGIC * Adapts to table usage patterns
# MAGIC * No cron jobs or manual scripts
# MAGIC
# MAGIC ### Enabling Predictive Optimization
# MAGIC
# MAGIC **On new table**:
# MAGIC ```sql
# MAGIC CREATE TABLE catalog.schema.orders (
# MAGIC     order_id BIGINT,
# MAGIC     amount DECIMAL(10,2)
# MAGIC )
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.enablePredictiveOptimization' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **On existing table**:
# MAGIC ```sql
# MAGIC ALTER TABLE catalog.schema.orders
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.enablePredictiveOptimization' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **On entire schema** (all tables):
# MAGIC ```sql
# MAGIC ALTER SCHEMA catalog.schema
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.enablePredictiveOptimization' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### What Gets Optimized
# MAGIC
# MAGIC **1. Compaction (Small File Problem)**:
# MAGIC * Automatically runs OPTIMIZE
# MAGIC * Combines small files into optimal sizes (128 MB - 1 GB)
# MAGIC * No manual scheduling needed
# MAGIC
# MAGIC **2. Vacuum**:
# MAGIC * Removes old file versions (respects retention period)
# MAGIC * Frees up storage
# MAGIC * Maintains time travel capability
# MAGIC
# MAGIC **3. Statistics Collection**:
# MAGIC * Updates table and column statistics
# MAGIC * Improves query planner (Cost-Based Optimizer)
# MAGIC * No manual ANALYZE TABLE required
# MAGIC
# MAGIC ### Checking Status
# MAGIC
# MAGIC ```sql
# MAGIC -- View table properties
# MAGIC DESCRIBE EXTENDED catalog.schema.orders;
# MAGIC
# MAGIC -- Check if enabled
# MAGIC SHOW TBLPROPERTIES catalog.schema.orders;
# MAGIC ```
# MAGIC
# MAGIC ### Disabling Predictive Optimization
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE catalog.schema.orders
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.enablePredictiveOptimization' = 'false'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Predictive Optimization vs Manual Maintenance
# MAGIC
# MAGIC | Aspect | Predictive Optimization | Manual Maintenance |
# MAGIC |--------|------------------------|--------------------|
# MAGIC | **Scheduling** | Automatic | Manual cron/jobs |
# MAGIC | **Timing** | Smart (off-peak) | Fixed schedule |
# MAGIC | **Operations** | All (optimize, vacuum, stats) | One at a time |
# MAGIC | **Cost** | Serverless compute (free tier) | Cluster costs |
# MAGIC | **Maintenance** | Zero | Ongoing |
# MAGIC | **Exam focus** | High (NEW) | Medium |
# MAGIC
# MAGIC ### When to Use Predictive Optimization
# MAGIC
# MAGIC **Use when**:
# MAGIC * Table has frequent writes (small files accumulate)
# MAGIC * Want zero-maintenance optimization
# MAGIC * Serverless available
# MAGIC * Unity Catalog managed tables
# MAGIC
# MAGIC **Not needed when**:
# MAGIC * Append-only tables with large files already
# MAGIC * Infrequent writes
# MAGIC * Manual control required (specific timing)
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "What operations does Predictive Optimization perform?"
# MAGIC * Answer: Compaction (OPTIMIZE), vacuum, and statistics collection
# MAGIC
# MAGIC **Question**: "How do you enable Predictive Optimization?"
# MAGIC * Answer: Set table property `delta.enablePredictiveOptimization = true`
# MAGIC
# MAGIC **Question**: "Can Predictive Optimization be enabled at schema level?"
# MAGIC * Answer: Yes - applies to all tables in schema
# MAGIC
# MAGIC **Common trap**: Thinking you still need manual OPTIMIZE when Predictive Optimization is enabled (you don't)
# MAGIC
# MAGIC **Memory aid**: "Predictive = Proactive automated care"
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 3: Traditional Optimization Techniques
# MAGIC %md
# MAGIC ## Concept 3: Traditional Optimization Techniques (MEDIUM EXAM FREQUENCY)
# MAGIC
# MAGIC ### File Sizing & Compaction
# MAGIC
# MAGIC **Problem**: Small files cause slow queries
# MAGIC
# MAGIC **Solution**: OPTIMIZE command
# MAGIC ```sql
# MAGIC OPTIMIZE catalog.schema.orders;
# MAGIC ```
# MAGIC
# MAGIC **With Z-Order** (pre-Liquid Clustering):
# MAGIC ```sql
# MAGIC OPTIMIZE catalog.schema.orders
# MAGIC ZORDER BY (customer_id);
# MAGIC ```
# MAGIC
# MAGIC **Target file size**: 128 MB - 1 GB
# MAGIC
# MAGIC ### Partitioning
# MAGIC
# MAGIC **Good for**: Low/medium cardinality columns (date, region)
# MAGIC
# MAGIC **Ideal range**: 100-10,000 partitions
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE orders (
# MAGIC     order_id BIGINT,
# MAGIC     amount DECIMAL
# MAGIC )
# MAGIC PARTITIONED BY (order_date DATE);
# MAGIC ```
# MAGIC
# MAGIC **Partition pruning** (efficient):
# MAGIC ```sql
# MAGIC SELECT * FROM orders
# MAGIC WHERE order_date = '2026-06-10';  -- Reads 1 partition
# MAGIC ```
# MAGIC
# MAGIC ### Caching
# MAGIC
# MAGIC **When to cache**: DataFrame used multiple times
# MAGIC
# MAGIC ```python
# MAGIC df.cache()  # In-memory + disk
# MAGIC df.persist(StorageLevel.MEMORY_ONLY)  # Memory only
# MAGIC ```
# MAGIC
# MAGIC **Don't cache when**: Used only once
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Key Takeaways & Exam Focus
# MAGIC %md
# MAGIC ## Key Takeaways & Exam Focus
# MAGIC
# MAGIC ### Most Testable Concepts
# MAGIC
# MAGIC 1. **Liquid Clustering** (High Frequency - NEW)
# MAGIC    * Automatic data layout optimization
# MAGIC    * `CLUSTER BY` clause (up to 4 columns)
# MAGIC    * Can change clustering keys
# MAGIC    * Replaces Z-Order for most use cases
# MAGIC
# MAGIC 2. **Predictive Optimization** (High Frequency - NEW)
# MAGIC    * Automated OPTIMIZE, VACUUM, statistics
# MAGIC    * Enable with `delta.enablePredictiveOptimization = true`
# MAGIC    * Can enable at table or schema level
# MAGIC    * Runs during low-usage periods
# MAGIC
# MAGIC 3. **File Sizing** (Medium Frequency)
# MAGIC    * Target: 128 MB - 1 GB per file
# MAGIC    * Small file problem solved by OPTIMIZE or Predictive Optimization
# MAGIC    * Liquid Clustering prevents small files automatically
# MAGIC
# MAGIC 4. **Partitioning** (Medium Frequency)
# MAGIC    * Good for low/medium cardinality
# MAGIC    * Ideal range: 100-10,000 partitions
# MAGIC    * Partition pruning for performance
# MAGIC
# MAGIC ### Decision Matrix
# MAGIC
# MAGIC **Data Layout Strategy**:
# MAGIC * High-cardinality columns + DBR 13.3+ → Liquid Clustering
# MAGIC * Low-cardinality columns (date, region) → Partitioning
# MAGIC * Multiple query patterns → Liquid Clustering
# MAGIC * Legacy DBR → Z-Order
# MAGIC
# MAGIC **Maintenance Strategy**:
# MAGIC * Frequent writes + UC managed table → Predictive Optimization
# MAGIC * Want zero maintenance → Predictive Optimization
# MAGIC * Need manual control → Manual OPTIMIZE
# MAGIC * Specific timing requirements → Manual OPTIMIZE
# MAGIC
# MAGIC ### Common Exam Traps
# MAGIC
# MAGIC **Trap 1**: "Liquid Clustering is the same as partitioning"
# MAGIC * False! Liquid Clustering is automatic and flexible, partitioning is static
# MAGIC
# MAGIC **Trap 2**: "Need to run OPTIMIZE when Predictive Optimization enabled"
# MAGIC * False! Predictive Optimization handles it automatically
# MAGIC
# MAGIC **Trap 3**: "Liquid Clustering supports unlimited columns"
# MAGIC * False! Maximum 4 clustering columns
# MAGIC
# MAGIC **Trap 4**: "Predictive Optimization only does compaction"
# MAGIC * False! It also does vacuum and statistics collection
# MAGIC
# MAGIC ### Syntax Quick Reference
# MAGIC
# MAGIC **Liquid Clustering**:
# MAGIC ```sql
# MAGIC -- Create with clustering
# MAGIC CREATE TABLE t CLUSTER BY (col1, col2);
# MAGIC
# MAGIC -- Add clustering to existing
# MAGIC ALTER TABLE t CLUSTER BY (col1, col2);
# MAGIC
# MAGIC -- Change clustering keys
# MAGIC ALTER TABLE t CLUSTER BY (col3, col4);
# MAGIC ```
# MAGIC
# MAGIC **Predictive Optimization**:
# MAGIC ```sql
# MAGIC -- Enable on table
# MAGIC ALTER TABLE t
# MAGIC SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'true');
# MAGIC
# MAGIC -- Enable on schema (all tables)
# MAGIC ALTER SCHEMA s
# MAGIC SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'true');
# MAGIC ```
# MAGIC
# MAGIC **Manual OPTIMIZE**:
# MAGIC ```sql
# MAGIC OPTIMIZE t;
# MAGIC OPTIMIZE t ZORDER BY (col);
# MAGIC ```
# MAGIC
# MAGIC ### Study Priorities
# MAGIC
# MAGIC **High Priority**:
# MAGIC 1. Liquid Clustering features and syntax
# MAGIC 2. Predictive Optimization capabilities
# MAGIC 3. When to use each optimization approach
# MAGIC 4. Liquid Clustering column limit (4)
# MAGIC 5. Predictive Optimization table property name
# MAGIC
# MAGIC **Medium Priority**:
# MAGIC 1. File sizing targets
# MAGIC 2. Partition cardinality guidelines
# MAGIC 3. Z-Order syntax (legacy)
# MAGIC 4. Manual OPTIMIZE workflow
# MAGIC 5. Caching use cases
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **End of Topic 7 Overview**

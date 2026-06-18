# Databricks notebook source
# DBTITLE 1,Topic 7 Solutions - All Exercises
# MAGIC %md
# MAGIC # Topic 7: Performance Optimization - Complete Solutions
# MAGIC
# MAGIC ## Exercises 1-15, MCQs, Challenge, Applied
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Liquid Clustering (Exercises 1-5)
# MAGIC
# MAGIC ### Exercise 1: Create Liquid Clustered Table
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC CREATE TABLE sales.orders (
# MAGIC     order_id BIGINT,
# MAGIC     customer_id BIGINT,
# MAGIC     order_date DATE,
# MAGIC     amount DECIMAL(10,2)
# MAGIC )
# MAGIC CLUSTER BY (customer_id, order_date);
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: Use `CLUSTER BY` clause with up to 4 columns. Order matters - most selective first.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 2: Enable on Existing Table
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC ALTER TABLE catalog.schema.transactions
# MAGIC CLUSTER BY (region, product_id);
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: ALTER TABLE with CLUSTER BY enables Liquid Clustering. Automatically runs OPTIMIZE to reorganize data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 3: Column Limit
# MAGIC **Answer**: Maximum **4 clustering columns**
# MAGIC
# MAGIC **Explanation**: Liquid Clustering supports up to 4 columns. Beyond that, use partitioning or Z-Order.
# MAGIC
# MAGIC **Exam trap**: High-frequency question - memorize the number 4.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 4: Change Clustering Keys
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC ALTER TABLE orders
# MAGIC CLUSTER BY (customer_id, product_category);
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: Simply ALTER TABLE with new columns. This is a key advantage over Z-Order (which can't be changed without recreating).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 5: Comparison
# MAGIC **Answer**:
# MAGIC
# MAGIC **Liquid Clustering**:
# MAGIC - Automatic maintenance
# MAGIC - Flexible (change keys easily)
# MAGIC - Max 4 columns
# MAGIC - Best for: High-cardinality columns, unknown query patterns
# MAGIC
# MAGIC **Z-Order**:
# MAGIC - Manual OPTIMIZE required
# MAGIC - Fixed at creation
# MAGIC - Unlimited columns
# MAGIC - Best for: Legacy DBR, need >4 columns
# MAGIC
# MAGIC **Partitioning**:
# MAGIC - Manual OPTIMIZE required
# MAGIC - Fixed at creation
# MAGIC - Best for: Low-cardinality (date, region), <10K partitions
# MAGIC
# MAGIC **Exam focus**: Liquid Clustering replaces Z-Order for most use cases on DBR 13.3+.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Predictive Optimization (Exercises 6-10)
# MAGIC
# MAGIC ### Exercise 6: Enable on Table
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC ALTER TABLE catalog.schema.events
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.enablePredictiveOptimization' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 7: Enable at Schema Level
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC ALTER SCHEMA production.sales
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.enablePredictiveOptimization' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: Applies to all tables in schema. New tables inherit the setting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 8: Operations Performed
# MAGIC **Answer**: Three operations:
# MAGIC 1. **Compaction** (OPTIMIZE for small files)
# MAGIC 2. **Vacuum** (remove old file versions)
# MAGIC 3. **Statistics collection** (for Cost-Based Optimizer)
# MAGIC
# MAGIC **Exam trap**: Very high-frequency question - know all three.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 9: Check if Enabled
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC SHOW TBLPROPERTIES catalog.schema.table_name;
# MAGIC ```
# MAGIC
# MAGIC Look for `delta.enablePredictiveOptimization = true`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 10: Advantages
# MAGIC **Answer**:
# MAGIC 1. **Automatic**: No manual scheduling or cron jobs
# MAGIC 2. **Smart timing**: Runs during low-usage periods
# MAGIC 3. **Zero maintenance**: Set once, works forever
# MAGIC 4. **Free compute**: Uses serverless (free tier)
# MAGIC 5. **Comprehensive**: All operations (optimize, vacuum, stats) together
# MAGIC
# MAGIC **Manual OPTIMIZE**: Requires scheduling, cluster costs, one operation at a time.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Traditional Optimization (Exercises 11-15)
# MAGIC
# MAGIC ### Exercise 11: Small File Problem
# MAGIC **Answer**:
# MAGIC
# MAGIC **Problem**: 50,000 files × 2 MB = 100 GB total, but excessive metadata overhead causes slow queries.
# MAGIC
# MAGIC **Solutions**:
# MAGIC
# MAGIC **Option 1 - Predictive Optimization** (recommended):
# MAGIC ```sql
# MAGIC ALTER TABLE table_name
# MAGIC SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'true');
# MAGIC ```
# MAGIC
# MAGIC **Option 2 - Manual OPTIMIZE**:
# MAGIC ```sql
# MAGIC OPTIMIZE table_name;
# MAGIC ```
# MAGIC
# MAGIC **Option 3 - Auto-Optimize**:
# MAGIC ```sql
# MAGIC ALTER TABLE table_name
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 12: Ideal File Size
# MAGIC **Answer**: **128 MB - 1 GB**
# MAGIC
# MAGIC **Explanation**: Matches Parquet block size (128 MB) and allows efficient parallel reading without excessive metadata.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 13: Partition on High Cardinality
# MAGIC **Answer**: **No, should NOT partition on customer_id.**
# MAGIC
# MAGIC **Reasoning**:
# MAGIC - 2 million partitions creates massive metadata overhead
# MAGIC - Listing operations become slow
# MAGIC - Ideal partition count: 100-10,000
# MAGIC - Better approach: Liquid Clustering or Z-Order on customer_id
# MAGIC
# MAGIC **Exam trap**: Common question about high-cardinality partitioning.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 14: OPTIMIZE with Z-Order
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC OPTIMIZE table_name
# MAGIC ZORDER BY (event_type, user_id);
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: Compacts files AND co-locates data for filtered columns.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 15: Broadcast Threshold
# MAGIC **Answer**: **30 MB** (with Databricks AQE enabled)
# MAGIC
# MAGIC **Explanation**: Databricks default is 30 MB, higher than open-source Spark (10 MB). Tables below threshold automatically broadcast.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## MCQ Answers
# MAGIC
# MAGIC **MCQ 1**: B) 4 columns
# MAGIC
# MAGIC **MCQ 2**: C) Compaction, vacuum, and statistics collection
# MAGIC
# MAGIC **MCQ 3**: B) 100-10,000
# MAGIC
# MAGIC **MCQ 4**: B) 30 MB
# MAGIC
# MAGIC **MCQ 5**: C) 128 MB - 1 GB
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Challenge Solution
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1: Enable Liquid Clustering (high-cardinality columns)
# MAGIC -- Use event_type (200 values) and user_region (50 values)
# MAGIC ALTER TABLE events
# MAGIC CLUSTER BY (event_date, event_type);
# MAGIC
# MAGIC -- Step 2: Enable Predictive Optimization for automated maintenance
# MAGIC ALTER TABLE events
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.enablePredictiveOptimization' = 'true'
# MAGIC );
# MAGIC
# MAGIC -- Step 3: One-time manual OPTIMIZE to fix current small files
# MAGIC OPTIMIZE events;
# MAGIC
# MAGIC -- Optional: Partition by date if strong temporal access pattern
# MAGIC -- (Alternative to Liquid Clustering on event_date)
# MAGIC -- Note: Can't use both partitioning and Liquid Clustering together
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC
# MAGIC **Why Liquid Clustering**:
# MAGIC - `event_date` and `event_type` are commonly filtered
# MAGIC - `user_region` has only 50 values (could partition, but Liquid Clustering more flexible)
# MAGIC - Automatic maintenance prevents future small files
# MAGIC - Can change clustering keys if query patterns evolve
# MAGIC
# MAGIC **Why Predictive Optimization**:
# MAGIC - Continuous streaming writes will create small files
# MAGIC - Automatic compaction prevents problem from recurring
# MAGIC - Also handles vacuum and statistics
# MAGIC - Zero maintenance overhead
# MAGIC
# MAGIC **Initial OPTIMIZE**:
# MAGIC - Fixes current 100K small files immediately
# MAGIC - Predictive Optimization handles future writes
# MAGIC
# MAGIC **Alternative approach** (if date-based queries dominate):
# MAGIC ```sql
# MAGIC -- Partition by date instead
# MAGIC CREATE TABLE events_partitioned (
# MAGIC     event_type STRING,
# MAGIC     user_region STRING,
# MAGIC     ...
# MAGIC )
# MAGIC PARTITIONED BY (event_date DATE)
# MAGIC CLUSTER BY (event_type);  -- Can't cluster by event_date if partitioning on it
# MAGIC
# MAGIC ALTER TABLE events_partitioned
# MAGIC SET TBLPROPERTIES ('delta.enablePredictiveOptimization' = 'true');
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Applied: Optimization Decision Framework
# MAGIC
# MAGIC ### 1. Data Layout Strategy
# MAGIC
# MAGIC ```python
# MAGIC def select_layout_strategy(columns, cardinality, dbr_version):
# MAGIC     """
# MAGIC     Decide between Liquid Clustering, partitioning, or Z-Order.
# MAGIC     """
# MAGIC     # Check DBR version
# MAGIC     if dbr_version < 13.3:
# MAGIC         return "Z-Order (Liquid Clustering not available)"
# MAGIC     
# MAGIC     # High-cardinality columns
# MAGIC     if any(card > 10000 for card in cardinality.values()):
# MAGIC         return "Liquid Clustering (high cardinality)"
# MAGIC     
# MAGIC     # Low-cardinality, temporal pattern
# MAGIC     if 'date' in columns and cardinality.get('date', 0) < 10000:
# MAGIC         return "Partition by date + Liquid Clustering on others"
# MAGIC     
# MAGIC     # Multiple query patterns
# MAGIC     if len(columns) <= 4:
# MAGIC         return "Liquid Clustering (flexible)"
# MAGIC     
# MAGIC     # Need > 4 columns
# MAGIC     return "Z-Order (Liquid Clustering limited to 4 columns)"
# MAGIC ```
# MAGIC
# MAGIC ### 2. Maintenance Strategy
# MAGIC
# MAGIC ```python
# MAGIC def select_maintenance_strategy(write_pattern, table_type, control_needed):
# MAGIC     """
# MAGIC     Decide between Predictive Optimization and manual maintenance.
# MAGIC     """
# MAGIC     # Unity Catalog managed table
# MAGIC     if table_type != "UC_managed":
# MAGIC         return "Manual OPTIMIZE (Predictive Optimization requires UC)"
# MAGIC     
# MAGIC     # Need specific timing
# MAGIC     if control_needed:
# MAGIC         return "Manual OPTIMIZE (specific timing required)"
# MAGIC     
# MAGIC     # Frequent writes
# MAGIC     if write_pattern in ["streaming", "frequent_batch"]:
# MAGIC         return "Predictive Optimization (prevents small files)"
# MAGIC     
# MAGIC     # Infrequent writes
# MAGIC     if write_pattern == "infrequent":
# MAGIC         return "Manual OPTIMIZE as needed (low overhead)"
# MAGIC     
# MAGIC     return "Predictive Optimization (default recommendation)"
# MAGIC ```
# MAGIC
# MAGIC ### 3. Column Selection Priority
# MAGIC
# MAGIC **For Liquid Clustering** (max 4 columns):
# MAGIC 1. Highest selectivity filters first
# MAGIC 2. Columns used in WHERE clauses
# MAGIC 3. High-cardinality over low-cardinality
# MAGIC 4. Columns used in JOINs
# MAGIC
# MAGIC **For Partitioning**:
# MAGIC 1. Date/time columns (if temporal access pattern)
# MAGIC 2. Low-cardinality categorical (region, status)
# MAGIC 3. Columns with predictable distribution
# MAGIC 4. Target: 100-10,000 partitions
# MAGIC
# MAGIC ### 4. Complete Decision Tree
# MAGIC
# MAGIC ```
# MAGIC Start: Need to optimize table layout
# MAGIC   |
# MAGIC   ├─ DBR version?
# MAGIC   |    ├─ < 13.3 → Use Z-Order
# MAGIC   |    └─ >= 13.3 → Continue
# MAGIC   |
# MAGIC   ├─ Cardinality of filter columns?
# MAGIC   |    ├─ High (>10K values) → Liquid Clustering
# MAGIC   |    ├─ Low (<100 values) → Consider partitioning
# MAGIC   |    └─ Medium (100-10K) → Liquid Clustering or partition
# MAGIC   |
# MAGIC   ├─ Number of important columns?
# MAGIC   |    ├─ <= 4 → Liquid Clustering
# MAGIC   |    └─ > 4 → Z-Order or partition + cluster
# MAGIC   |
# MAGIC   ├─ Query pattern predictable?
# MAGIC   |    ├─ Yes (e.g., always filter by date) → Partition
# MAGIC   |    └─ No (multiple patterns) → Liquid Clustering
# MAGIC   |
# MAGIC   └─ Maintenance needs?
# MAGIC        ├─ Automated → Predictive Optimization
# MAGIC        └─ Manual control → Manual OPTIMIZE
# MAGIC ```
# MAGIC
# MAGIC ### Real-World Examples
# MAGIC
# MAGIC **Example 1**: Clickstream events table
# MAGIC - Columns: user_id (millions), event_timestamp, event_type (100s)
# MAGIC - Access: Filter by user_id and event_type
# MAGIC - Writes: Continuous streaming
# MAGIC
# MAGIC **Decision**: Liquid Clustering on (event_type, user_id) + Predictive Optimization
# MAGIC
# MAGIC **Example 2**: Daily sales data
# MAGIC - Columns: sale_date, store_id (1000s), product_id (10K+)
# MAGIC - Access: Always filter by date, then store
# MAGIC - Writes: Daily batch
# MAGIC
# MAGIC **Decision**: Partition by sale_date + Liquid Clustering on (store_id, product_id) + Predictive Optimization
# MAGIC
# MAGIC **Example 3**: Reference table
# MAGIC - Columns: product_id, category, price
# MAGIC - Size: 500 MB
# MAGIC - Access: Mostly broadcast joins
# MAGIC - Writes: Weekly update
# MAGIC
# MAGIC **Decision**: No clustering needed (small table), manual OPTIMIZE weekly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **End of Topic 7 Solutions**

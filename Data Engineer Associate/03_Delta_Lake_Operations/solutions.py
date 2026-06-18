# Databricks notebook source
# DBTITLE 1,Topic 3: Delta Lake - Solutions
# MAGIC %md
# MAGIC # Topic 3: Delta Lake Operations - Solutions
# MAGIC
# MAGIC ## Complete solutions for all exercises, MCQs, challenges covering MERGE, VACUUM, time travel, OPTIMIZE, ZORDER, and table properties.
# MAGIC
# MAGIC **Serverless Note**: All solutions use Unity Catalog managed tables (`workspace.default.*`) for serverless compatibility.

# COMMAND ----------

# DBTITLE 1,Setup
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit

sample_data = [(1, "Alice", "active", 100), (2, "Bob", "active", 200), (3, "Charlie", "inactive", 150)]
df = spark.createDataFrame(sample_data, ["id", "name", "status", "amount"])
df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.delta_practice")
print("Setup complete! Delta table created: workspace.default.delta_practice")

# COMMAND ----------

# DBTITLE 1,Exercises 1-5: Solutions
# MAGIC %md
# MAGIC ## Exercise 1: Read Delta Table
# MAGIC
# MAGIC **SQL approach**:
# MAGIC ```sql
# MAGIC SELECT * FROM workspace.default.delta_practice
# MAGIC ```
# MAGIC
# MAGIC **PySpark approach**:
# MAGIC ```python
# MAGIC ex1_result = spark.table("workspace.default.delta_practice")
# MAGIC display(ex1_result)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 2: Time Travel by Version
# MAGIC
# MAGIC **PySpark approach**:
# MAGIC ```python
# MAGIC ex2_result = spark.read.format("delta").option("versionAsOf", 0).table("workspace.default.delta_practice")
# MAGIC display(ex2_result)
# MAGIC ```
# MAGIC
# MAGIC **SQL approach**:
# MAGIC ```sql
# MAGIC SELECT * FROM workspace.default.delta_practice VERSION AS OF 0
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 3: UPDATE
# MAGIC
# MAGIC **SQL approach (recommended)**:
# MAGIC ```sql
# MAGIC UPDATE workspace.default.delta_practice
# MAGIC SET status = 'inactive'
# MAGIC WHERE amount > 150
# MAGIC ```
# MAGIC
# MAGIC **PySpark DeltaTable API**:
# MAGIC ```python
# MAGIC dt = DeltaTable.forName(spark, "workspace.default.delta_practice")
# MAGIC dt.update(
# MAGIC     condition="amount > 150",
# MAGIC     set={"status": "'inactive'"}
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Expected Result**: Bob's status changes from 'active' to 'inactive' (amount=200)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 4: DELETE
# MAGIC
# MAGIC **SQL approach (recommended)**:
# MAGIC ```sql
# MAGIC DELETE FROM workspace.default.delta_practice WHERE status = 'inactive'
# MAGIC ```
# MAGIC
# MAGIC **PySpark DeltaTable API**:
# MAGIC ```python
# MAGIC dt = DeltaTable.forName(spark, "workspace.default.delta_practice")
# MAGIC dt.delete("status = 'inactive'")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 5: MERGE - Upsert
# MAGIC
# MAGIC **SQL approach (recommended)**:
# MAGIC ```sql
# MAGIC MERGE INTO workspace.default.delta_practice AS target
# MAGIC USING (
# MAGIC     VALUES 
# MAGIC         (1, 'Alice', 'active', 150),
# MAGIC         (4, 'David', 'active', 300)
# MAGIC ) AS source(id, name, status, amount)
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET 
# MAGIC         target.name = source.name,
# MAGIC         target.status = source.status,
# MAGIC         target.amount = source.amount
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (id, name, status, amount)
# MAGIC     VALUES (source.id, source.name, source.status, source.amount)
# MAGIC ```
# MAGIC
# MAGIC **PySpark DeltaTable API**:
# MAGIC ```python
# MAGIC updates = spark.createDataFrame(
# MAGIC     [(1, "Alice", "active", 150), (4, "David", "active", 300)],
# MAGIC     ["id", "name", "status", "amount"]
# MAGIC )
# MAGIC
# MAGIC dt = DeltaTable.forName(spark, "workspace.default.delta_practice")
# MAGIC dt.alias("target").merge(
# MAGIC     updates.alias("source"),
# MAGIC     "target.id = source.id"
# MAGIC ).whenMatchedUpdateAll(
# MAGIC ).whenNotMatchedInsertAll(
# MAGIC ).execute()
# MAGIC ```
# MAGIC
# MAGIC **Key Points**:
# MAGIC * MERGE requires ON clause
# MAGIC * Both WHEN MATCHED and WHEN NOT MATCHED are optional
# MAGIC * UPDATE SET * or updateAll() updates all columns from source
# MAGIC * INSERT * or insertAll() inserts all columns from source

# COMMAND ----------

# DBTITLE 1,Exercises 6-10 & MCQs: Solutions
# MAGIC %md
# MAGIC ## Exercise 6: OPTIMIZE
# MAGIC
# MAGIC **SQL approach**:
# MAGIC ```sql
# MAGIC OPTIMIZE workspace.default.delta_practice
# MAGIC ```
# MAGIC
# MAGIC **PySpark DeltaTable API**:
# MAGIC ```python
# MAGIC dt = DeltaTable.forName(spark, "workspace.default.delta_practice")
# MAGIC dt.optimize().executeCompaction()
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 7: ZORDER
# MAGIC
# MAGIC **SQL approach (recommended)**:
# MAGIC ```sql
# MAGIC OPTIMIZE workspace.default.delta_practice ZORDER BY (status)
# MAGIC ```
# MAGIC
# MAGIC **PySpark DeltaTable API**:
# MAGIC ```python
# MAGIC dt = DeltaTable.forName(spark, "workspace.default.delta_practice")
# MAGIC dt.optimize().executeZOrderBy("status")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 8: VACUUM
# MAGIC
# MAGIC **SQL approach**:
# MAGIC ```sql
# MAGIC VACUUM workspace.default.delta_practice RETAIN 168 HOURS
# MAGIC ```
# MAGIC
# MAGIC **Note**: 168 hours = 7 days (default retention period)
# MAGIC
# MAGIC **PySpark DeltaTable API**:
# MAGIC ```python
# MAGIC dt = DeltaTable.forName(spark, "workspace.default.delta_practice")
# MAGIC dt.vacuum(168)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 9: Describe History
# MAGIC
# MAGIC **SQL approach**:
# MAGIC ```sql
# MAGIC DESCRIBE HISTORY workspace.default.delta_practice
# MAGIC ```
# MAGIC
# MAGIC **PySpark approach**:
# MAGIC ```python
# MAGIC ex9_result = DeltaTable.forName(spark, "workspace.default.delta_practice").history()
# MAGIC display(ex9_result)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 10: Set Table Properties
# MAGIC
# MAGIC **SQL approach**:
# MAGIC ```sql
# MAGIC ALTER TABLE workspace.default.delta_practice
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true',
# MAGIC     'delta.deletedFileRetentionDuration' = 'interval 30 days'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Note**: These properties enable automatic optimization during writes and extend deleted file retention to 30 days.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 11: Time Travel by Timestamp
# MAGIC
# MAGIC **PySpark approach**:
# MAGIC ```python
# MAGIC from datetime import datetime, timedelta
# MAGIC
# MAGIC one_hour_ago = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
# MAGIC ex11_result = spark.read.format("delta").option("timestampAsOf", one_hour_ago).table("workspace.default.delta_practice")
# MAGIC display(ex11_result)
# MAGIC ```
# MAGIC
# MAGIC **SQL approach**:
# MAGIC ```sql
# MAGIC SELECT * FROM workspace.default.delta_practice 
# MAGIC TIMESTAMP AS OF current_timestamp() - INTERVAL 1 HOUR
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 12: MERGE with DELETE
# MAGIC
# MAGIC **SQL approach**:
# MAGIC ```sql
# MAGIC MERGE INTO workspace.default.delta_practice AS target
# MAGIC USING (
# MAGIC     VALUES (1, 'Alice', 'active', 50), (2, 'Bob', 'active', 250)
# MAGIC ) AS source(id, name, status, amount)
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED AND source.amount < 100 THEN DELETE
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC **PySpark DeltaTable API**:
# MAGIC ```python
# MAGIC updates = spark.createDataFrame(
# MAGIC     [(1, "Alice", "active", 50), (2, "Bob", "active", 250)],
# MAGIC     ["id", "name", "status", "amount"]
# MAGIC )
# MAGIC
# MAGIC dt = DeltaTable.forName(spark, "workspace.default.delta_practice")
# MAGIC dt.alias("target").merge(
# MAGIC     updates.alias("source"),
# MAGIC     "target.id = source.id"
# MAGIC ).whenMatchedDelete(
# MAGIC     condition="source.amount < 100"
# MAGIC ).whenMatchedUpdateAll(
# MAGIC ).whenNotMatchedInsertAll(
# MAGIC ).execute()
# MAGIC ```
# MAGIC
# MAGIC **Key Point**: Order matters. DELETE condition should come before UPDATE to properly handle the conditional logic.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 13: RESTORE Version
# MAGIC
# MAGIC **SQL approach**:
# MAGIC ```sql
# MAGIC RESTORE TABLE workspace.default.delta_practice TO VERSION AS OF 0
# MAGIC ```
# MAGIC
# MAGIC This atomically restores the table to version 0, undoing all subsequent changes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 14: Check Small Files
# MAGIC
# MAGIC **SQL approach**:
# MAGIC ```sql
# MAGIC DESCRIBE DETAIL workspace.default.delta_practice
# MAGIC ```
# MAGIC
# MAGIC Then look at the `numFiles` column.
# MAGIC
# MAGIC **PySpark approach**:
# MAGIC ```python
# MAGIC detail = spark.sql("DESCRIBE DETAIL workspace.default.delta_practice")
# MAGIC ex14_result = detail.select("numFiles").collect()[0][0]
# MAGIC print(f"Number of data files: {ex14_result}")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 15: Clone Table
# MAGIC
# MAGIC **SQL approach**:
# MAGIC ```sql
# MAGIC CREATE TABLE workspace.default.delta_practice_clone 
# MAGIC SHALLOW CLONE workspace.default.delta_practice
# MAGIC ```
# MAGIC
# MAGIC **Shallow clone**: References the same data files (fast, space-efficient)
# MAGIC **Deep clone** (alternative): `DEEP CLONE` copies all data files
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## MCQ Answers
# MAGIC
# MAGIC **MCQ 1**: Default VACUUM retention?
# MAGIC * **Answer: B) 7 days (168 hours)**
# MAGIC
# MAGIC **MCQ 2**: What does OPTIMIZE do?
# MAGIC * **Answer: B) Compacts small files**
# MAGIC * Note: OPTIMIZE rearranges data but does NOT delete old files (VACUUM does)
# MAGIC
# MAGIC **MCQ 3**: ZORDER improves performance for?
# MAGIC * **Answer: B) Reads with filters**
# MAGIC * ZORDER co-locates related data, improving filter and JOIN performance
# MAGIC
# MAGIC **MCQ 4**: MERGE requires which clause?
# MAGIC * **Answer: C) ON**
# MAGIC * The ON clause specifies the join condition between source and target
# MAGIC
# MAGIC **MCQ 5**: Time travel syntax?
# MAGIC * **Answer: C) Both**
# MAGIC * Both VERSION AS OF and TIMESTAMP AS OF are valid time travel syntax

# COMMAND ----------

# DBTITLE 1,Challenges & Applieds: Solutions
# MAGIC %md
# MAGIC ## Challenge 1: SCD Type 2 Implementation
# MAGIC
# MAGIC **Scenario**: Implement Slowly Changing Dimension Type 2 to track historical changes.
# MAGIC
# MAGIC **Solution**:
# MAGIC ```python
# MAGIC from pyspark.sql.functions import lit, current_timestamp
# MAGIC
# MAGIC # Step 1: Close out existing current records that are being updated
# MAGIC updates = spark.createDataFrame(
# MAGIC     [(1, "Alice Updated", "active", 200)],
# MAGIC     ["id", "name", "status", "amount"]
# MAGIC )
# MAGIC
# MAGIC dt = DeltaTable.forName(spark, "workspace.default.delta_practice")
# MAGIC
# MAGIC # First, expire existing current records that have updates
# MAGIC dt.alias("target").merge(
# MAGIC     updates.alias("source"),
# MAGIC     "target.id = source.id AND target.is_current = true"
# MAGIC ).whenMatchedUpdate(
# MAGIC     set={
# MAGIC         "is_current": "false",
# MAGIC         "end_date": "current_timestamp()"
# MAGIC     }
# MAGIC ).execute()
# MAGIC
# MAGIC # Step 2: Insert new versions as current
# MAGIC updates_with_metadata = updates.withColumn("is_current", lit(True)) \
# MAGIC     .withColumn("start_date", current_timestamp()) \
# MAGIC     .withColumn("end_date", lit(None).cast("timestamp"))
# MAGIC
# MAGIC updates_with_metadata.write.format("delta").mode("append") \
# MAGIC     .saveAsTable("workspace.default.delta_practice")
# MAGIC ```
# MAGIC
# MAGIC **Key Concept**: SCD Type 2 maintains full history by closing old records and inserting new versions.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Challenge 2: Optimization Strategy
# MAGIC
# MAGIC **Scenario**: A table has 10,000 small files and slow query performance.
# MAGIC
# MAGIC **Strategy**:
# MAGIC 1. **Assess current state**:
# MAGIC    ```sql
# MAGIC    DESCRIBE DETAIL workspace.default.delta_practice
# MAGIC    ```
# MAGIC    Look at `numFiles`, `sizeInBytes`, and avg file size.
# MAGIC
# MAGIC 2. **If file count > 1000**: Run OPTIMIZE
# MAGIC    ```sql
# MAGIC    OPTIMIZE workspace.default.delta_practice
# MAGIC    ```
# MAGIC
# MAGIC 3. **If queries use specific filters**: Add ZORDER on those columns
# MAGIC    ```sql
# MAGIC    OPTIMIZE workspace.default.delta_practice ZORDER BY (customer_id, date)
# MAGIC    ```
# MAGIC
# MAGIC 4. **Enable auto-optimization for future writes**:
# MAGIC    ```sql
# MAGIC    ALTER TABLE workspace.default.delta_practice
# MAGIC    SET TBLPROPERTIES (
# MAGIC        'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC        'delta.autoOptimize.autoCompact' = 'true'
# MAGIC    )
# MAGIC    ```
# MAGIC
# MAGIC 5. **Schedule weekly VACUUM** to reclaim space:
# MAGIC    ```sql
# MAGIC    VACUUM workspace.default.delta_practice RETAIN 168 HOURS
# MAGIC    ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ETL Applied 1: Delta Health Check
# MAGIC
# MAGIC ```python
# MAGIC def delta_health_check(table_name):
# MAGIC     """
# MAGIC     Check Delta table health metrics.
# MAGIC     
# MAGIC     Args:
# MAGIC         table_name: Fully qualified table name (e.g., 'workspace.default.my_table')
# MAGIC     
# MAGIC     Returns:
# MAGIC         Dictionary with health metrics
# MAGIC     """
# MAGIC     dt = DeltaTable.forName(spark, table_name)
# MAGIC     detail = spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0]
# MAGIC     history_count = dt.history().count()
# MAGIC     
# MAGIC     num_files = detail["numFiles"]
# MAGIC     size_bytes = detail["sizeInBytes"]
# MAGIC     avg_file_size = size_bytes / num_files if num_files > 0 else 0
# MAGIC     
# MAGIC     return {
# MAGIC         "num_files": num_files,
# MAGIC         "size_bytes": size_bytes,
# MAGIC         "avg_file_size_mb": avg_file_size / (1024 * 1024),
# MAGIC         "version_count": history_count,
# MAGIC         "needs_optimization": num_files > 1000 or avg_file_size < 1024 * 1024,
# MAGIC         "needs_vacuum": history_count > 10
# MAGIC     }
# MAGIC ```
# MAGIC
# MAGIC **Usage**:
# MAGIC ```python
# MAGIC health = delta_health_check("workspace.default.delta_practice")
# MAGIC print(f"Files: {health['num_files']}, Avg size: {health['avg_file_size_mb']:.2f} MB")
# MAGIC if health['needs_optimization']:
# MAGIC     print("Recommendation: Run OPTIMIZE")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ETL Applied 2: Smart VACUUM
# MAGIC
# MAGIC ```python
# MAGIC def smart_vacuum(table_name, days_retention=7, dry_run=True):
# MAGIC     """
# MAGIC     Intelligently VACUUM table only when needed.
# MAGIC     
# MAGIC     Args:
# MAGIC         table_name: Fully qualified table name
# MAGIC         days_retention: Retention period in days (default: 7)
# MAGIC         dry_run: If True, show what would be deleted without actually deleting
# MAGIC     
# MAGIC     Returns:
# MAGIC         None
# MAGIC     """
# MAGIC     health = delta_health_check(table_name)
# MAGIC     
# MAGIC     if health["version_count"] <= 5:
# MAGIC         print(f"Skipped VACUUM - only {health['version_count']} versions exist")
# MAGIC         return
# MAGIC     
# MAGIC     hours_retention = days_retention * 24
# MAGIC     
# MAGIC     if dry_run:
# MAGIC         print(f"DRY RUN: Would vacuum {table_name} with {hours_retention} hours retention")
# MAGIC         spark.sql(f"VACUUM {table_name} RETAIN {hours_retention} HOURS DRY RUN").show()
# MAGIC     else:
# MAGIC         print(f"Executing VACUUM on {table_name}...")
# MAGIC         dt = DeltaTable.forName(spark, table_name)
# MAGIC         dt.vacuum(hours_retention)
# MAGIC         print(f"VACUUM completed")
# MAGIC ```
# MAGIC
# MAGIC **Usage**:
# MAGIC ```python
# MAGIC # Safe dry run first
# MAGIC smart_vacuum("workspace.default.delta_practice", days_retention=7, dry_run=True)
# MAGIC
# MAGIC # Execute after reviewing
# MAGIC smart_vacuum("workspace.default.delta_practice", days_retention=7, dry_run=False)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC * **Default VACUUM retention**: 7 days (168 hours)
# MAGIC * **MERGE syntax**: Requires ON clause, then WHEN MATCHED/NOT MATCHED clauses
# MAGIC * **OPTIMIZE**: Compacts small files, does NOT delete old files
# MAGIC * **VACUUM**: Deletes old data files, reclaims storage, limits time travel
# MAGIC * **ZORDER**: Co-locates data for better filter performance on high-cardinality columns
# MAGIC * **Time travel**: Use VERSION AS OF or TIMESTAMP AS OF
# MAGIC * **autoOptimize properties**: `optimizeWrite` + `autoCompact`
# MAGIC * **RESTORE**: Atomically reverts table to previous version
# MAGIC * **CLONE types**: SHALLOW (references files), DEEP (copies files)
# MAGIC * **DeltaTable API**: Use `forName()` for UC tables, not `forPath()`

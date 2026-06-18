# Databricks notebook source
# DBTITLE 1,Topic 3: Delta Lake Operations - Practice Tasks
# MAGIC %md
# MAGIC # Topic 3: Delta Lake Operations - Practice Tasks
# MAGIC
# MAGIC ## Overview
# MAGIC 25% of exam questions. Critical topics: MERGE, VACUUM, time travel, OPTIMIZE, ZORDER, table properties.
# MAGIC
# MAGIC ## Focus Areas
# MAGIC * MERGE syntax and UPDATE/DELETE
# MAGIC * VACUUM retention and behavior
# MAGIC * Time travel queries
# MAGIC * OPTIMIZE and ZORDER
# MAGIC * Table properties (autoOptimize, retention)
# MAGIC * Transaction log operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Serverless Note**: All exercises use Unity Catalog managed tables (`workspace.default.*`) instead of file paths. This is the recommended pattern for serverless compute and matches production best practices.

# COMMAND ----------

# DBTITLE 1,Setup
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp

# Create sample Delta table
sample_data = [
    (1, "Alice", "active", 100),
    (2, "Bob", "active", 200),
    (3, "Charlie", "inactive", 150)
]

df = spark.createDataFrame(sample_data, ["id", "name", "status", "amount"])

# Write as Unity Catalog managed table (serverless-compatible)
df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.delta_practice")

print("Setup complete! Delta table created: workspace.default.delta_practice")

# COMMAND ----------

# DBTITLE 1,Exercises 1-5: Basic Operations
# MAGIC %md
# MAGIC ## Exercise 1: Read Delta Table (Easy)
# MAGIC Read the Delta table `workspace.default.delta_practice`.
# MAGIC
# MAGIC ## Exercise 2: Time Travel by Version (Easy)
# MAGIC Read version 0 of the Delta table.
# MAGIC
# MAGIC ## Exercise 3: UPDATE (Medium)
# MAGIC Update status to 'inactive' for all records where amount > 150.
# MAGIC
# MAGIC ## Exercise 4: DELETE (Medium)
# MAGIC Delete all records where status = 'inactive'.
# MAGIC
# MAGIC ## Exercise 5: MERGE - Upsert (Hard)
# MAGIC Merge updates: (id=1, name="Alice", status="active", amount=150), (id=4, name="David", status="active", amount=300).

# COMMAND ----------

# DBTITLE 1,Exercises 1-5: Your Solutions
# Exercise 1
ex1_result = spark.sql("SELECT * FROM workspace.default.delta_practice")
display(ex1_result)

# Exercise 2
ex2_result = spark.sql("SELECT * FROM workspace.default.delta_practice VERSION AS OF 0")
display(ex2_result)

# Exercise 3
spark.sql("UPDATE workspace.default.delta_practice SET status = 'inactive' WHERE amount > 150")
spark.sql("SELECT * FROM workspace.default.delta_practice").display()


# Exercise 4
# Write SQL or DataFrame code
spark.sql("DELETE FROM workspace.default.delta_practice WHERE status = 'inactive'")
spark.sql("SELECT * FROM workspace.default.delta_practice").display()

# Exercise 5
# Write MERGE code
spark.createDataFrame(
    [
        (1, "Alice", "active", 150),
        (4, "David", "active", 300)
    ],
    ["id", "name", "status", "amount"]
).createOrReplaceTempView("source_vw")

spark.sql("""
MERGE INTO workspace.default.delta_practice t
USING source_vw s
ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET *
WHEN NOT MATCHED THEN 
    INSERT *        
""")

spark.sql("SELECT * FROM workspace.default.delta_practice").display()





# Reset
sample_data = [
    (1, "Alice", "active", 100),
    (2, "Bob", "active", 200),
    (3, "Charlie", "inactive", 150)
]
df = spark.createDataFrame(sample_data, ["id", "name", "status", "amount"])
df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.delta_practice")

# COMMAND ----------

# DBTITLE 1,Exercises 6-10: OPTIMIZE, VACUUM, Properties
# MAGIC %md
# MAGIC ## Exercise 6: OPTIMIZE (Medium)
# MAGIC Run OPTIMIZE on `workspace.default.delta_practice`.
# MAGIC
# MAGIC ## Exercise 7: ZORDER (Medium)
# MAGIC Optimize table with ZORDER by `status` column.
# MAGIC
# MAGIC ## Exercise 8: VACUUM (Medium)
# MAGIC Run VACUUM to remove files older than 7 days (168 hours).
# MAGIC
# MAGIC ## Exercise 9: Describe History (Easy)
# MAGIC Show the transaction history for the Delta table.
# MAGIC
# MAGIC ## Exercise 10: Set Table Properties (Medium)
# MAGIC Enable autoOptimize and set deleted file retention to 30 days.

# COMMAND ----------

# DBTITLE 1,Exercises 6-10: Your Solutions
# Exercise 6
# Write OPTIMIZE command
spark.sql("OPTIMIZE workspace.default.delta_practice")

# Exercise 7
# Write ZORDER command
spark.sql("OPTIMIZE workspace.default.delta_practice ZORDER BY (status)")

# Exercise 8
# Write VACUUM command
spark.sql("VACUUM workspace.default.delta_practice")

# Exercise 9
ex9_result = spark.sql("DESCRIBE HISTORY workspace.default.delta_practice")
display(ex9_result)

# Exercise 10
# Write ALTER TABLE commands
spark.sql("""
ALTER TABLE workspace.default.delta_practice SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 30 days'
)
""")

spark.sql("DESCRIBE DETAIL workspace.default.delta_practice").select("name", "properties").display()

# COMMAND ----------

# DBTITLE 1,Exercises 11-15 & MCQs
# MAGIC %md
# MAGIC ## Exercise 11: Time Travel by Timestamp (Medium)
# MAGIC Read Delta table as it existed 1 hour ago (use current timestamp minus 1 hour).
# MAGIC
# MAGIC ## Exercise 12: MERGE with DELETE (Hard)
# MAGIC Create a MERGE that updates matched rows with amount >= 100, and deletes matched rows where amount < 100.
# MAGIC
# MAGIC ## Exercise 13: Restore Version (Medium)
# MAGIC Restore table to version 0.
# MAGIC
# MAGIC ## Exercise 14: Check Small Files (Medium)
# MAGIC Use DESCRIBE DETAIL to find the number of data files.
# MAGIC
# MAGIC ## Exercise 15: Clone Table (Medium)
# MAGIC Create a shallow clone of the Delta table as `workspace.default.delta_practice_clone`.
# MAGIC
# MAGIC ## MCQ 1
# MAGIC Default VACUUM retention?
# MAGIC A) 1 day  B) 7 days  C) 30 days  D) 90 days
# MAGIC
# MAGIC ## MCQ 2
# MAGIC What does OPTIMIZE do?
# MAGIC A) Deletes files  B) Compacts small files  C) Creates index  D) Validates data
# MAGIC
# MAGIC ## MCQ 3
# MAGIC ZORDER improves performance for?
# MAGIC A) Writes  B) Reads with filters  C) Deletes  D) Updates
# MAGIC
# MAGIC ## MCQ 4
# MAGIC MERGE requires which clause?
# MAGIC A) MATCHED  B) NOT MATCHED  C) ON  D) WHERE
# MAGIC
# MAGIC ## MCQ 5
# MAGIC Time travel syntax?
# MAGIC A) VERSION AS OF  B) TIMESTAMP AS OF  C) Both  D) Neither

# COMMAND ----------

# DBTITLE 1,Exercises 11-15, MCQs, Challenges: Your Solutions
# Exercise 11
ex11_result = spark.sql(
    "SELECT * FROM workspace.default.delta_practice TIMESTAMP AS OF (current_timestamp() - INTERVAL 1 HOUR)"
)
display(ex11_result)

# Exercise 12
# Write MERGE code
spark.createDataFrame(
    [
        (1, "Alice", "active", 150),
        (4, "David", "active", 300)
    ],
    ["id", "name", "status", "amount"]
).createOrReplaceTempView("source_vw")

spark.sql("""
    MERGE INTO workspace.default.delta_practice t
    USING source_vw s
    ON t.id = s.id
    WHEN MATCHED AND s.amount < 100 THEN
        DELETE
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *       
""")

# # Reset table to original state
original_data = [
    (1, "Alice", "active", 100),
    (2, "Bob", "active", 200),
    (3, "Charlie", "inactive", 150)
]
df_original = spark.createDataFrame(
    original_data, ["id", "name", "status", "amount"]
)
df_original.write.format("delta").mode("overwrite").saveAsTable(
    "workspace.default.delta_practice"
)

# Exercise 13
# Write RESTORE command
spark.sql("RESTORE TABLE workspace.default.delta_practice TO VERSION AS OF 0")

# Exercise 14
# ex14_result = spark.sql("DESCRIBE DETAIL workspace.default.delta_practice")['numFiles']
ex14_num_files = spark.sql("DESCRIBE DETAIL workspace.default.delta_practice").collect()[0]['numFiles']
print(ex14_num_files)

# Exercise 15
# Write CLONE command
spark.sql("""
CREATE TABLE workspace.default.delta_practice_clone
AS SELECT * FROM workspace.default.delta_practice
""")
spark.sql("select * from workspace.default.delta_practice_clone").display()

# MCQ Answers
mcq_answers = {1: 'B', 2: 'B', 3: 'B', 4: 'C', 5: 'C'}

# Challenge 1: CDC Pattern
# Implement SCD Type 2 using MERGE

# Challenge 2: Optimization Strategy
# Given table with 10K small files, propose optimization plan

# ETL Applied 1: Delta Health Check
def delta_health_check(table_path):
    """
    Return health metrics: file count, avg file size, version count.
    """
    pass

# ETL Applied 2: Intelligent VACUUM
def smart_vacuum(table_path, days_retention):
    """
    VACUUM only if needed based on file count.
    """
    pass

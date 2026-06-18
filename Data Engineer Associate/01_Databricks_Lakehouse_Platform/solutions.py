# Databricks notebook source
# DBTITLE 1,Solutions - Overview
# MAGIC %md
# MAGIC # Topic 1: Databricks Lakehouse Platform - Solutions
# MAGIC
# MAGIC This notebook contains complete solutions, explanations, and validation functions for all practice exercises.
# MAGIC
# MAGIC ## How to Use
# MAGIC 1. Attempt exercises in practice_tasks.py first
# MAGIC 2. Check your work against these solutions
# MAGIC 3. Read explanations to understand WHY
# MAGIC 4. Review common mistakes
# MAGIC 5. Note exam tips

# COMMAND ----------

# DBTITLE 1,Setup & Validation Functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import time

# Recreate sample data for validation
transactions_data = [
    (1, "2024-06-01", "ProductA", 100, 50.0),
    (2, "2024-06-01", "ProductB", 200, 75.0),
    (3, "2024-06-02", "ProductA", 150, 50.0),
    (4, "2024-06-02", "ProductC", 300, 120.0),
    (5, "2024-06-03", "ProductB", 250, 75.0),
] * 2000

transactions_df = spark.createDataFrame(
    transactions_data,
    ["transaction_id", "date", "product", "quantity", "price"]
)

def validate_config_value(student_value, expected_value, config_name):
    """
    Validate configuration retrieval.
    
    Args:
        student_value: Value retrieved by student
        expected_value: Correct value
        config_name: Configuration name for messaging
    
    Returns:
        bool: True if correct
    """
    if str(student_value) == str(expected_value):
        print(f"✓ Correct! {config_name} = {expected_value}")
        return True
    else:
        print(f"✗ Incorrect. Got {student_value}, expected {expected_value}")
        return False

print("Setup complete")

# COMMAND ----------

# DBTITLE 1,Exercise 1: Solution
# MAGIC %md
# MAGIC ## Exercise 1: Check Current Configuration
# MAGIC
# MAGIC ### Solution

# COMMAND ----------

# DBTITLE 1,Exercise 1: Code
shuffle_partitions = int(spark.conf.get('spark.sql.shuffle.partitions'))
print(f"Current shuffle partitions: {shuffle_partitions}")

# COMMAND ----------

# DBTITLE 1,Exercise 1: Explanation
# MAGIC %md
# MAGIC ### Explanation
# MAGIC The `spark.conf.get()` method retrieves configuration values. The return value is a string, so convert to int for numeric configs.
# MAGIC
# MAGIC ### Common Mistakes
# MAGIC * Forgetting to convert string to int
# MAGIC * Using `spark.conf.set()` instead of `get()`
# MAGIC * Incorrect config name
# MAGIC
# MAGIC ### Exam Tip
# MAGIC Know how to both GET and SET configurations. Default is 200.

# COMMAND ----------

# DBTITLE 1,Exercise 2: Solution
# MAGIC %md
# MAGIC ## Exercise 2: Modify Configuration
# MAGIC
# MAGIC ### Solution

# COMMAND ----------

# DBTITLE 1,Exercise 2: Code
spark.conf.set('spark.sql.shuffle.partitions', '50')
verify = int(spark.conf.get('spark.sql.shuffle.partitions'))
print(f"Updated to: {verify}")
assert verify == 50, "Configuration not updated correctly"

# COMMAND ----------

# DBTITLE 1,Exercise 2: Explanation
# MAGIC %md
# MAGIC ### Explanation
# MAGIC Use `spark.conf.set(key, value)` to modify configurations. Value must be a string. Verify changes with `get()`.
# MAGIC
# MAGIC ### Common Mistakes
# MAGIC * Passing int instead of string to set()
# MAGIC * Not verifying the change took effect
# MAGIC * Modifying wrong configuration
# MAGIC
# MAGIC ### Exam Tip
# MAGIC Configuration changes apply immediately to the current session. Jobs clusters start fresh each time.

# COMMAND ----------

# DBTITLE 1,Exercise 3-5: Solutions
# MAGIC %md
# MAGIC ## Exercise 3: Compare Partition Counts
# MAGIC
# MAGIC ### Solution
# MAGIC ```python
# MAGIC result = transactions_df.groupBy('product').count()
# MAGIC result_partitions = result.rdd.getNumPartitions()
# MAGIC ```
# MAGIC
# MAGIC Partition count matches current shuffle.partitions setting.
# MAGIC
# MAGIC ## Exercise 4: Compute Selection
# MAGIC
# MAGIC ### Answer: B (Jobs Cluster)
# MAGIC
# MAGIC **Reasoning**: Production ETL, scheduled, auto-terminate, cost-effective.
# MAGIC
# MAGIC ## Exercise 5: Configuration Trade-offs
# MAGIC
# MAGIC ### Solution
# MAGIC ```python
# MAGIC optimal_partitions = 2048 / 128  # = 16
# MAGIC ```
# MAGIC
# MAGIC 16 partitions for 2 GB dataset with 128 MB target.

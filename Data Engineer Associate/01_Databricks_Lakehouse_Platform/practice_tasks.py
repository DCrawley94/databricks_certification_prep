# Databricks notebook source
# DBTITLE 1,Setup - Run This First
# Databricks Lakehouse Platform - Practice Tasks
# Run this cell first to set up sample data and helper functions

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import time

# Create sample transaction data for exercises
transactions_data = [
    (1, "2024-06-01", "ProductA", 100, 50.0),
    (2, "2024-06-01", "ProductB", 200, 75.0),
    (3, "2024-06-02", "ProductA", 150, 50.0),
    (4, "2024-06-02", "ProductC", 300, 120.0),
    (5, "2024-06-03", "ProductB", 250, 75.0),
] * 2000  # Multiply for more realistic dataset

transactions_df = spark.createDataFrame(
    transactions_data,
    ["transaction_id", "date", "product", "quantity", "price"]
)

print(f"Setup complete! Created sample dataset with {transactions_df.count()} rows")
print(f"Current shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# COMMAND ----------

# DBTITLE 1,Exercise 1: Check Current Configuration (Easy)
# MAGIC %md
# MAGIC ## Exercise 1: Check Current Configuration (Easy)
# MAGIC
# MAGIC ### Objective
# MAGIC Retrieve and display the current value of `spark.sql.shuffle.partitions`.
# MAGIC
# MAGIC ### Instructions
# MAGIC Write code to get the current configuration value and store it in a variable called `shuffle_partitions`.
# MAGIC
# MAGIC ### Expected Output
# MAGIC A variable containing the integer value of the configuration.
# MAGIC
# MAGIC ### Your Solution
# MAGIC Write your code in the cell below.

# COMMAND ----------

# DBTITLE 1,Exercise 1: Your Solution
# Write your solution here
shuffle_partitions = spark.conf.get('spark.sql.shuffle.partitions')
print(shuffle_partitions)

# COMMAND ----------

# DBTITLE 1,Exercise 2: Modify Configuration (Easy)
# MAGIC %md
# MAGIC ## Exercise 2: Modify Configuration (Easy)
# MAGIC
# MAGIC ### Objective
# MAGIC Change the `spark.sql.shuffle.partitions` configuration to 50.
# MAGIC
# MAGIC ### Instructions
# MAGIC Write code to set the configuration value to 50, then verify it was changed correctly.
# MAGIC
# MAGIC ### Expected Behavior
# MAGIC Configuration should be updated to 50.
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,Exercise 2: Your Solution
# Write your solution here
spark.conf.set("spark.sql.shuffle.partitions", 50)

print(spark.conf.get("spark.sql.shuffle.partitions"))


# COMMAND ----------

# DBTITLE 1,Exercise 3: Compare Partition Counts (Easy)
# MAGIC %md
# MAGIC ## Exercise 3: Compare Partition Counts (Easy)
# MAGIC
# MAGIC ### Objective
# MAGIC Perform a groupBy aggregation and check how many partitions the result has.
# MAGIC
# MAGIC ### Instructions
# MAGIC 1. Use the `transactions_df` DataFrame
# MAGIC 2. Group by `product` and count the number of transactions
# MAGIC 3. Get the number of partitions in the result
# MAGIC 4. Store the partition count in a variable called `result_partitions`
# MAGIC
# MAGIC ### Expected Output
# MAGIC An integer representing the number of partitions.
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,Exercise 3: Your Solution
# Write your solution here
result_partitions = None

# Step 1: Complete the groupBy with an aggregation
# groupBy() returns GroupedData, not a DataFrame
# You must add an aggregation like .count(), .sum(), .avg(), etc.
test_df = transactions_df.groupBy("product").count()

# Step 2: Get partition count (NOT available on serverless)
# On standard clusters, you would use:
# result_partitions = test_df.rdd.getNumPartitions()
#
# This accesses the RDD API which is blocked on Spark Connect (serverless)
# The result would equal spark.conf.get('spark.sql.shuffle.partitions')
# because groupBy triggers a shuffle operation

print(f"Completed groupBy aggregation: {test_df.count()} groups")
print(f"Current shuffle.partitions config: {spark.conf.get('spark.sql.shuffle.partitions')}")
print("Note: On serverless, cannot directly inspect partition count via RDD API")

# COMMAND ----------

# DBTITLE 1,Exercise 4: Compute Selection Scenario (Medium)
# MAGIC %md
# MAGIC ## Exercise 4: Compute Selection Scenario (Medium)
# MAGIC
# MAGIC ### Scenario
# MAGIC You need to run a production ETL job that processes 500 GB of data daily at 3 AM. The job takes about 2 hours to complete.
# MAGIC
# MAGIC ### Question
# MAGIC Which compute type should you use?
# MAGIC
# MAGIC A) All-Purpose Cluster
# MAGIC B) Jobs Cluster
# MAGIC C) SQL Warehouse
# MAGIC D) Serverless
# MAGIC
# MAGIC ### Instructions
# MAGIC Store your answer (A, B, C, or D) in a variable called `answer_4`.
# MAGIC
# MAGIC ### Reasoning
# MAGIC Explain your choice in a comment.
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,Exercise 4: Your Solution
# Write your solution here
answer_4 = "B"  # Replace with 'A', 'B', 'C', or 'D'

# Explain your reasoning:
# Jobs clusters are more cost effective than the other options. They terminate automatically and are purpose built for scheduled workloads. SQL warehouse is best for interactive queries. Serverless is best for adhoc queries without waiting for a cluster to start up. And an all purpose cluster is useful for dev work.

# COMMAND ----------

# DBTITLE 1,Exercise 5: Configuration Trade-offs (Medium)
# MAGIC %md
# MAGIC ## Exercise 5: Configuration Trade-offs (Medium)
# MAGIC
# MAGIC ### Scenario
# MAGIC You have a small dataset (2 GB) and you're running a groupBy aggregation. The default configuration creates 200 tasks, each processing about 10 MB of data.
# MAGIC
# MAGIC ### Objective
# MAGIC Calculate the optimal number of shuffle partitions for this dataset.
# MAGIC
# MAGIC ### Instructions
# MAGIC 1. Assume target partition size of 128 MB
# MAGIC 2. Calculate: `partitions = data_size_mb / target_partition_size_mb`
# MAGIC 3. Store the result in `optimal_partitions`
# MAGIC
# MAGIC ### Hints
# MAGIC - 2 GB = 2048 MB
# MAGIC - Target size = 128 MB per partition
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,Exercise 5: Your Solution
# Write your solution here
data_size_mb = 2048
target_partition_size_mb = 128
optimal_partitions = data_size_mb/target_partition_size_mb

print(optimal_partitions)

# COMMAND ----------

# DBTITLE 1,Exercise 6: Adaptive Query Execution (Medium)
# MAGIC %md
# MAGIC ## Exercise 6: Adaptive Query Execution (Medium)
# MAGIC
# MAGIC ### Objective
# MAGIC Check if Adaptive Query Execution (AQE) is enabled.
# MAGIC
# MAGIC ### Instructions
# MAGIC 1. Get the value of `spark.sql.adaptive.enabled`
# MAGIC 2. Store it in `aqe_enabled` as a boolean
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,Exercise 6: Your Solution
# Write your solution here
# Note: spark.sql.adaptive.enabled is not accessible on Spark Connect (serverless)
# On standard clusters, you would use:
# aqe_enabled = spark.conf.get("spark.sql.adaptive.enabled")
#
# AQE is enabled by default on Databricks, so for exam purposes,
# the answer would typically be True on modern Databricks runtimes

aqe_enabled = None  # Cannot retrieve on serverless

print("AQE config not accessible on serverless compute")
print("On standard Databricks clusters, AQE is enabled by default (True)")

# COMMAND ----------

# DBTITLE 1,Exercise 7: Broadcast Join Threshold (Medium)
# MAGIC %md
# MAGIC ## Exercise 7: Broadcast Join Threshold (Medium)
# MAGIC
# MAGIC ### Objective
# MAGIC Retrieve the broadcast join threshold.
# MAGIC
# MAGIC ### Instructions
# MAGIC Get `spark.databricks.adaptive.autoBroadcastJoinThreshold` or `spark.sql.autoBroadcastJoinThreshold` and convert to MB.
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,Exercise 7: Your Solution
# Write your solution here
# Note: On serverless, many Spark configs are not accessible
# On standard clusters, you would use:
# broadcast_threshold_bytes = spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
# broadcast_threshold_mb = int(broadcast_threshold_bytes) / (1024 * 1024)

broadcast_threshold_mb = None

print("Broadcast threshold config not accessible on serverless compute")
print("On standard Databricks clusters, default is 10 MB")

# COMMAND ----------

# DBTITLE 1,Exercise 8: Compute Decision Matrix (Hard)
# MAGIC %md
# MAGIC ## Exercise 8: Compute Decision Matrix (Hard)
# MAGIC
# MAGIC ### Match workloads to compute types
# MAGIC 1. Interactive data exploration with multiple analysts
# MAGIC 2. Scheduled ETL pipeline running nightly
# MAGIC 3. Ad-hoc SQL queries from dashboards
# MAGIC
# MAGIC A) All-Purpose, B) Jobs Cluster, C) SQL Warehouse
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,Exercise 8: Your Solution
workload_compute_mapping = {1: None, 2: None, 3: None}

# COMMAND ----------

# DBTITLE 1,Exercise 9: Performance Measurement (Hard)
# MAGIC %md
# MAGIC ## Exercise 9: Performance Measurement (Hard)
# MAGIC
# MAGIC ### Objective
# MAGIC Measure execution time with different shuffle partition settings (200 vs 20).
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,Exercise 9: Your Solution
import time
time_config_a = None
time_config_b = None

# COMMAND ----------

# DBTITLE 1,Exercise 10-15: Quick Scenarios
# MAGIC %md
# MAGIC ## Exercises 10-15: Quick Scenarios
# MAGIC
# MAGIC **Ex 10**: Which access mode provides highest security? (A: No Isolation, B: Shared, C: Single User)
# MAGIC
# MAGIC **Ex 11**: Default shuffle.partitions value?
# MAGIC
# MAGIC **Ex 12**: When should you use DBR ML instead of standard DBR?
# MAGIC
# MAGIC **Ex 13**: What happens when executor memory is too low?
# MAGIC
# MAGIC **Ex 14**: Best practice for production ETL compute?
# MAGIC
# MAGIC **Ex 15**: How to disable broadcast joins?

# COMMAND ----------

# DBTITLE 1,Exercises 10-15: Your Solutions
answer_10 = 'C'
answer_11 = '200'
answer_12 = 'When you want to run ML workloads - this runtime comes prepackaged with ML libraries'
answer_13 = 'OOM errors'
answer_14 = 'Using Databricks jobs/job compute - see reasoning in related question above'
answer_15 = 'Set config - not sure what the exact config name and value is'

# COMMAND ----------

# DBTITLE 1,Multiple Choice Questions
# MAGIC %md
# MAGIC ## Multiple Choice Questions
# MAGIC
# MAGIC ### MCQ 1
# MAGIC What is the default value of `spark.sql.shuffle.partitions`?
# MAGIC
# MAGIC A) 50
# MAGIC B) 100
# MAGIC C) 200
# MAGIC D) 500
# MAGIC
# MAGIC ### MCQ 2
# MAGIC Which compute type automatically terminates after job completion?
# MAGIC
# MAGIC A) All-Purpose Cluster
# MAGIC B) Jobs Cluster
# MAGIC C) SQL Warehouse
# MAGIC D) Interactive Cluster
# MAGIC
# MAGIC ### MCQ 3
# MAGIC What is the Databricks default broadcast join threshold with AQE enabled?
# MAGIC
# MAGIC A) 10 MB
# MAGIC B) 20 MB
# MAGIC C) 30 MB
# MAGIC D) 50 MB
# MAGIC
# MAGIC ### MCQ 4
# MAGIC Which access mode provides the strongest isolation?
# MAGIC
# MAGIC A) No Isolation Shared
# MAGIC B) Shared
# MAGIC C) Single User
# MAGIC D) All provide equal isolation
# MAGIC
# MAGIC ### MCQ 5
# MAGIC When should you increase `spark.executor.memory`?
# MAGIC
# MAGIC A) When tasks are slow
# MAGIC B) When getting OOM errors
# MAGIC C) Always, more memory is better
# MAGIC D) Never, it's auto-tuned

# COMMAND ----------

# DBTITLE 1,MCQ Answers
mcq_answers = {
    1: 'C',  # A, B, C, or D
    2: 'B',
    3: 'A',
    4: 'C',
    5: 'B'
}

# COMMAND ----------

# DBTITLE 1,Challenge 1: Production ETL Design
# MAGIC %md
# MAGIC ## Challenge 1: Production ETL Design
# MAGIC
# MAGIC ### Scenario
# MAGIC You're designing a production ETL pipeline that:
# MAGIC * Processes 2 TB of data daily
# MAGIC * Runs at 2 AM every day
# MAGIC * Takes 3-4 hours to complete
# MAGIC * Involves complex joins and aggregations
# MAGIC * Must be cost-effective
# MAGIC
# MAGIC ### Tasks
# MAGIC 1. Select the appropriate compute type
# MAGIC 2. Recommend shuffle.partitions value
# MAGIC 3. Should AQE be enabled?
# MAGIC 4. What cluster access mode?
# MAGIC 5. Justify each decision
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,Challenge 1: Your Solution
challenge_1_design = {
    'compute_type': None,
    'shuffle_partitions': None,
    'aqe_enabled': None,
    'access_mode': None,
    'justification': ''
}

# COMMAND ----------

# DBTITLE 1,Challenge 2: Configuration Optimization
# MAGIC %md
# MAGIC ## Challenge 2: Configuration Optimization
# MAGIC
# MAGIC ### Scenario
# MAGIC A data engineer reports:
# MAGIC * Query processes 50 GB of data
# MAGIC * Using default configurations
# MAGIC * Takes 45 minutes
# MAGIC * Spark UI shows 200 tasks, most complete in < 5 seconds
# MAGIC * Significant time spent in scheduling overhead
# MAGIC
# MAGIC ### Tasks
# MAGIC 1. Diagnose the problem
# MAGIC 2. Recommend specific configuration changes
# MAGIC 3. Estimate expected improvement
# MAGIC 4. Explain the root cause
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,Challenge 2: Your Solution
challenge_2_diagnosis = {
    'problem': '',
    'config_changes': {},
    'expected_improvement': '',
    'root_cause': ''
}

# COMMAND ----------

# DBTITLE 1,ETL Applied 1: Data Volume Assessment
# MAGIC %md
# MAGIC ## ETL Applied 1: Data Volume Assessment
# MAGIC
# MAGIC ### Task
# MAGIC Given a dataset, determine optimal shuffle.partitions.
# MAGIC
# MAGIC ### Instructions
# MAGIC 1. Calculate the size of `transactions_df` in MB
# MAGIC 2. Apply the rule: target 128 MB per partition
# MAGIC 3. Set the optimal configuration
# MAGIC 4. Validate the change improves performance
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,ETL Applied 1: Your Solution
# Calculate dataset size and optimize
dataset_size_mb = None
recommended_partitions = None

# COMMAND ----------

# DBTITLE 1,ETL Applied 2: Compute Selection Decision Tree
# MAGIC %md
# MAGIC ## ETL Applied 2: Compute Selection Decision Tree
# MAGIC
# MAGIC ### Task
# MAGIC Implement a function that recommends compute type based on workload characteristics.
# MAGIC
# MAGIC ### Inputs
# MAGIC * is_production: bool
# MAGIC * is_scheduled: bool
# MAGIC * requires_ml_libraries: bool
# MAGIC * is_sql_only: bool
# MAGIC * multiple_users: bool
# MAGIC
# MAGIC ### Output
# MAGIC Recommended compute type
# MAGIC
# MAGIC ### Your Solution

# COMMAND ----------

# DBTITLE 1,ETL Applied 2: Your Solution
def recommend_compute(is_production, is_scheduled, requires_ml_libraries, is_sql_only, multiple_users):
    """
    Recommend compute type based on workload characteristics.
    
    Args:
        is_production: Production workload
        is_scheduled: Runs on schedule
        requires_ml_libraries: Needs ML libraries
        is_sql_only: Only SQL queries
        multiple_users: Multiple concurrent users
    
    Returns:
        str: Recommended compute type
    """
    # Implement your logic here
    return None

# Databricks notebook source
# DBTITLE 1,Instructions
# MAGIC %md
# MAGIC # Topic 7: Performance Optimization - Practice Tasks
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC Attempt each exercise before checking solutions.py
# MAGIC
# MAGIC ## Exercise Categories
# MAGIC
# MAGIC * **Exercises 1-5**: Liquid Clustering (NEW)
# MAGIC * **Exercises 6-10**: Predictive Optimization (NEW)
# MAGIC * **Exercises 11-15**: Traditional optimization (partitioning, file sizing, joins)
# MAGIC * **MCQs 1-5**: Exam-style questions
# MAGIC * **Challenge**: Complete optimization scenario
# MAGIC * **Applied**: Optimization strategy framework

# COMMAND ----------

# DBTITLE 1,Exercise 1: Create Liquid Clustered Table
# MAGIC %md
# MAGIC ## Exercise 1: Create Liquid Clustered Table
# MAGIC **Question**: Write SQL to create a table `sales.orders` with Liquid Clustering on columns `customer_id` and `order_date`.

# COMMAND ----------

# DBTITLE 1,Solution 1
# Your solution for exercise 1

ex1 = None

# COMMAND ----------

# DBTITLE 1,Exercise 2: Enable Liquid Clustering on Existing Table
# MAGIC %md
# MAGIC ## Exercise 2: Enable Liquid Clustering on Existing Table
# MAGIC **Question**: Write SQL to add Liquid Clustering to existing table `catalog.schema.transactions` on columns `region` and `product_id`.

# COMMAND ----------

# DBTITLE 1,Solution 2
# Your solution for exercise 2

ex2 = None

# COMMAND ----------

# DBTITLE 1,Exercise 3: Liquid Clustering Column Limit
# MAGIC %md
# MAGIC ## Exercise 3: Liquid Clustering Column Limit
# MAGIC **Question**: What is the maximum number of clustering columns supported by Liquid Clustering?

# COMMAND ----------

# DBTITLE 1,Solution 3
# Your solution for exercise 3

ex3 = None

# COMMAND ----------

# DBTITLE 1,Exercise 4: Change Clustering Keys
# MAGIC %md
# MAGIC ## Exercise 4: Change Clustering Keys
# MAGIC **Question**: Table `orders` is currently clustered by `(order_date, region)`. Write SQL to change clustering to `(customer_id, product_category)`.

# COMMAND ----------

# DBTITLE 1,Solution 4
# Your solution for exercise 4

ex4 = None

# COMMAND ----------

# DBTITLE 1,Exercise 5: Liquid vs Z-Order vs Partitioning
# MAGIC %md
# MAGIC ## Exercise 5: Liquid vs Z-Order vs Partitioning
# MAGIC **Question**: Compare these three approaches. When would you choose each?

# COMMAND ----------

# DBTITLE 1,Solution 5
# Your solution for exercise 5

ex5 = None

# COMMAND ----------

# DBTITLE 1,Exercise 6: Enable Predictive Optimization on Table
# MAGIC %md
# MAGIC ## Exercise 6: Enable Predictive Optimization on Table
# MAGIC **Question**: Write SQL to enable Predictive Optimization on table `catalog.schema.events`.

# COMMAND ----------

# DBTITLE 1,Solution 6
# Your solution for exercise 6

ex6 = None

# COMMAND ----------

# DBTITLE 1,Exercise 7: Enable at Schema Level
# MAGIC %md
# MAGIC ## Exercise 7: Enable at Schema Level
# MAGIC **Question**: Write SQL to enable Predictive Optimization for ALL tables in schema `production.sales`.

# COMMAND ----------

# DBTITLE 1,Solution 7
# Your solution for exercise 7

ex7 = None

# COMMAND ----------

# DBTITLE 1,Exercise 8: Predictive Optimization Operations
# MAGIC %md
# MAGIC ## Exercise 8: Predictive Optimization Operations
# MAGIC **Question**: What three operations does Predictive Optimization perform automatically?

# COMMAND ----------

# DBTITLE 1,Solution 8
# Your solution for exercise 8

ex8 = None

# COMMAND ----------

# DBTITLE 1,Exercise 9: Check if Enabled
# MAGIC %md
# MAGIC ## Exercise 9: Check if Enabled
# MAGIC **Question**: Write SQL to check if Predictive Optimization is enabled on a table.

# COMMAND ----------

# DBTITLE 1,Solution 9
# Your solution for exercise 9

ex9 = None

# COMMAND ----------

# DBTITLE 1,Exercise 10: Predictive Optimization vs Manual OPTIMIZE
# MAGIC %md
# MAGIC ## Exercise 10: Predictive Optimization vs Manual OPTIMIZE
# MAGIC **Question**: What are the advantages of Predictive Optimization over manual OPTIMIZE commands?

# COMMAND ----------

# DBTITLE 1,Solution 10
# Your solution for exercise 10

ex10 = None

# COMMAND ----------

# DBTITLE 1,Exercise 11: Small File Problem
# MAGIC %md
# MAGIC ## Exercise 11: Small File Problem
# MAGIC **Question**: Table has 50,000 files, each 2 MB. What's the problem and how do you fix it?

# COMMAND ----------

# DBTITLE 1,Solution 11
# Your solution for exercise 11

ex11 = None

# COMMAND ----------

# DBTITLE 1,Exercise 12: Ideal File Size
# MAGIC %md
# MAGIC ## Exercise 12: Ideal File Size
# MAGIC **Question**: What is the ideal target file size for Parquet files in Delta tables?

# COMMAND ----------

# DBTITLE 1,Solution 12
# Your solution for exercise 12

ex12 = None

# COMMAND ----------

# DBTITLE 1,Exercise 13: Partition Cardinality
# MAGIC %md
# MAGIC ## Exercise 13: Partition Cardinality
# MAGIC **Question**: Table has 2 million unique `customer_id` values. Should you partition on this column? Why or why not?

# COMMAND ----------

# DBTITLE 1,Solution 13
# Your solution for exercise 13

ex13 = None

# COMMAND ----------

# DBTITLE 1,Exercise 14: Manual OPTIMIZE with Z-Order
# MAGIC %md
# MAGIC ## Exercise 14: Manual OPTIMIZE with Z-Order
# MAGIC **Question**: Write SQL to compact files AND optimize for queries filtering on `event_type` and `user_id`.

# COMMAND ----------

# DBTITLE 1,Solution 14
# Your solution for exercise 14

ex14 = None

# COMMAND ----------

# DBTITLE 1,Exercise 15: Broadcast Join Threshold
# MAGIC %md
# MAGIC ## Exercise 15: Broadcast Join Threshold
# MAGIC **Question**: What is the default broadcast join threshold in Databricks (with AQE enabled)?

# COMMAND ----------

# DBTITLE 1,Solution 15
# Your solution for exercise 15

ex15 = None

# COMMAND ----------

# DBTITLE 1,MCQs 1-5
# MAGIC %md
# MAGIC ## Multiple Choice Questions
# MAGIC
# MAGIC ### MCQ 1: Liquid Clustering Maximum Columns
# MAGIC How many clustering columns does Liquid Clustering support?
# MAGIC
# MAGIC A) 2 columns  
# MAGIC B) 4 columns  
# MAGIC C) 8 columns  
# MAGIC D) Unlimited
# MAGIC
# MAGIC ### MCQ 2: Predictive Optimization Operations
# MAGIC Which operations does Predictive Optimization perform automatically?
# MAGIC
# MAGIC A) Only compaction  
# MAGIC B) Compaction and vacuum  
# MAGIC C) Compaction, vacuum, and statistics collection  
# MAGIC D) Only statistics collection
# MAGIC
# MAGIC ### MCQ 3: Ideal Partition Count
# MAGIC What is the ideal range for number of partitions?
# MAGIC
# MAGIC A) 10-100  
# MAGIC B) 100-10,000  
# MAGIC C) 10,000-100,000  
# MAGIC D) No limit
# MAGIC
# MAGIC ### MCQ 4: Broadcast Join Threshold
# MAGIC What is the default broadcast join threshold in Databricks with AQE?
# MAGIC
# MAGIC A) 10 MB  
# MAGIC B) 30 MB  
# MAGIC C) 50 MB  
# MAGIC D) 100 MB
# MAGIC
# MAGIC ### MCQ 5: Small File Target
# MAGIC What is the recommended target file size for Delta tables?
# MAGIC
# MAGIC A) 10-50 MB  
# MAGIC B) 50-100 MB  
# MAGIC C) 128 MB - 1 GB  
# MAGIC D) 1-5 GB

# COMMAND ----------

# DBTITLE 1,MCQ Solutions
# Your answers for MCQs 1-5

mcq1 = None
mcq2 = None
mcq3 = None
mcq4 = None
mcq5 = None

# COMMAND ----------

# DBTITLE 1,Challenge: Complete Optimization Strategy
# MAGIC %md
# MAGIC ## Challenge: Complete Optimization Strategy
# MAGIC
# MAGIC **Scenario**: You have a 5 TB `events` table with these characteristics:
# MAGIC - Written to continuously (streaming)
# MAGIC - Queries filter by `event_date` (date) and `user_region` (string, ~50 values)
# MAGIC - Also commonly filtered by `event_type` (string, ~200 values)
# MAGIC - Currently has 100,000 small files (avg 50 MB each)
# MAGIC - No optimization configured
# MAGIC - Running on DBR 14.2 (supports Liquid Clustering)
# MAGIC - Unity Catalog managed table
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Optimize data layout for common query patterns
# MAGIC 2. Prevent future small file accumulation
# MAGIC 3. Minimize manual maintenance
# MAGIC 4. Improve query performance on filtered columns
# MAGIC
# MAGIC **Task**: Design complete optimization strategy with SQL commands.

# COMMAND ----------

# DBTITLE 1,Challenge Solution
# Your solution for the Challenge

challenge = None

# COMMAND ----------

# DBTITLE 1,Applied: Optimization Decision Framework
# MAGIC %md
# MAGIC ## Applied: Optimization Decision Framework
# MAGIC
# MAGIC Develop a decision tree for:
# MAGIC 1. When to use Liquid Clustering vs partitioning vs Z-Order
# MAGIC 2. When to enable Predictive Optimization
# MAGIC 3. How to choose clustering/partition columns
# MAGIC 4. Manual vs automated maintenance strategy

# COMMAND ----------

# DBTITLE 1,Applied Solution
# Your solution for the Applied

def optimization_strategy():
    pass

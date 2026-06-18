# Databricks notebook source
# DBTITLE 1,Instructions
# MAGIC %md
# MAGIC # Topic 6: Unity Catalog & Governance - Practice Tasks
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC Attempt each exercise before checking solutions.py
# MAGIC
# MAGIC ## Exercise Categories
# MAGIC
# MAGIC * **Exercises 1-10**: Unity Catalog hierarchy, permissions, and grants
# MAGIC * **Exercises 11-15**: ABAC policies (NEW - critical for exam)
# MAGIC * **Exercises 16-20**: Managed vs external tables, information schema
# MAGIC * **MCQs 1-5**: Exam-style questions
# MAGIC * **Challenge**: Complete governance setup
# MAGIC * **Applied**: Permission strategy framework

# COMMAND ----------

# DBTITLE 1,Exercise 1: Three-Level Namespace
# MAGIC %md
# MAGIC ## Exercise 1: Three-Level Namespace
# MAGIC **Question**: What are the three levels in the Unity Catalog namespace, and what is their hierarchy?

# COMMAND ----------

# DBTITLE 1,Solution 1
# Your solution for exercise 1

ex1 = None

# COMMAND ----------

# DBTITLE 1,Exercise 2: Create Catalog and Schema
# MAGIC %md
# MAGIC ## Exercise 2: Create Catalog and Schema
# MAGIC **Question**: Write SQL to create a catalog named `analytics` and a schema named `sales` within it.

# COMMAND ----------

# DBTITLE 1,Solution 2
# Your solution for exercise 2

ex2 = None

# COMMAND ----------

# DBTITLE 1,Exercise 3: USAGE Permission
# MAGIC %md
# MAGIC ## Exercise 3: USAGE Permission
# MAGIC **Question**: A user has `SELECT` on `production.sales.orders` but cannot query it. What is likely missing?

# COMMAND ----------

# DBTITLE 1,Solution 3
# Your solution for exercise 3

ex3 = None

# COMMAND ----------

# DBTITLE 1,Exercise 4: Grant Complete Access
# MAGIC %md
# MAGIC ## Exercise 4: Grant Complete Access
# MAGIC **Question**: Write SQL to grant complete read access to `analysts` group for table `production.sales.orders`, including all required permissions.

# COMMAND ----------

# DBTITLE 1,Solution 4
# Your solution for exercise 4

ex4 = None

# COMMAND ----------

# DBTITLE 1,Exercise 5: MODIFY Permission
# MAGIC %md
# MAGIC ## Exercise 5: MODIFY Permission
# MAGIC **Question**: What operations does `MODIFY` permission allow on a table?

# COMMAND ----------

# DBTITLE 1,Solution 5
# Your solution for exercise 5

ex5 = None

# COMMAND ----------

# DBTITLE 1,Exercise 6: DENY vs GRANT
# MAGIC %md
# MAGIC ## Exercise 6: DENY vs GRANT
# MAGIC **Question**: User has `SELECT` granted on schema but `DENY SELECT` on specific table. Can they read the table? Why?

# COMMAND ----------

# DBTITLE 1,Solution 6
# Your solution for exercise 6

ex6 = None

# COMMAND ----------

# DBTITLE 1,Exercise 7: Show Grants
# MAGIC %md
# MAGIC ## Exercise 7: Show Grants
# MAGIC **Question**: Write SQL to view all permissions on table `production.sales.orders`.

# COMMAND ----------

# DBTITLE 1,Solution 7
# Your solution for exercise 7

ex7 = None

# COMMAND ----------

# DBTITLE 1,Exercise 8: Revoke Permission
# MAGIC %md
# MAGIC ## Exercise 8: Revoke Permission
# MAGIC **Question**: Write SQL to revoke `INSERT` permission from `data_engineers` group on `production.sales.orders`.

# COMMAND ----------

# DBTITLE 1,Solution 8
# Your solution for exercise 8

ex8 = None

# COMMAND ----------

# DBTITLE 1,Exercise 9: Transfer Ownership
# MAGIC %md
# MAGIC ## Exercise 9: Transfer Ownership
# MAGIC **Question**: Write SQL to transfer ownership of table `production.sales.orders` to user `admin@example.com`.

# COMMAND ----------

# DBTITLE 1,Solution 9
# Your solution for exercise 9

ex9 = None

# COMMAND ----------

# DBTITLE 1,Exercise 10: Permission Hierarchy
# MAGIC %md
# MAGIC ## Exercise 10: Permission Hierarchy
# MAGIC **Question**: List the minimum permissions a user needs to SELECT from `catalog_a.schema_b.table_c`.

# COMMAND ----------

# DBTITLE 1,Solution 10
# Your solution for exercise 10

ex10 = None

# COMMAND ----------

# DBTITLE 1,Exercise 11: ABAC vs Traditional
# MAGIC %md
# MAGIC ## Exercise 11: ABAC vs Traditional
# MAGIC **Question**: What is the main advantage of ABAC policies over traditional per-table column masks and row filters?

# COMMAND ----------

# DBTITLE 1,Solution 11
# Your solution for exercise 11

ex11 = None

# COMMAND ----------

# DBTITLE 1,Exercise 12: Create Row Filter Policy
# MAGIC %md
# MAGIC ## Exercise 12: Create Row Filter Policy
# MAGIC **Question**: Create an ABAC row filter policy named `region_access` that filters rows based on a `region` column, allowing users to only see rows where the region matches their authorized regions (stored in table `user_regions`).

# COMMAND ----------

# DBTITLE 1,Solution 12
# Your solution for exercise 12

ex12 = None

# COMMAND ----------

# DBTITLE 1,Exercise 13: Apply Row Filter
# MAGIC %md
# MAGIC ## Exercise 13: Apply Row Filter
# MAGIC **Question**: Apply the `region_access` policy from Exercise 12 to table `production.sales.orders` on the `sales_region` column.

# COMMAND ----------

# DBTITLE 1,Solution 13
# Your solution for exercise 13

ex13 = None

# COMMAND ----------

# DBTITLE 1,Exercise 14: Create Column Mask Policy
# MAGIC %md
# MAGIC ## Exercise 14: Create Column Mask Policy
# MAGIC **Question**: Create an ABAC column mask policy named `ssn_mask` that:
# MAGIC - Shows full SSN to `compliance` group
# MAGIC - Shows last 4 digits only to `hr_managers` group
# MAGIC - Shows 'REDACTED' to everyone else

# COMMAND ----------

# DBTITLE 1,Solution 14
# Your solution for exercise 14

ex14 = None

# COMMAND ----------

# DBTITLE 1,Exercise 15: Remove ABAC Policy
# MAGIC %md
# MAGIC ## Exercise 15: Remove ABAC Policy
# MAGIC **Question**: Write SQL to:
# MAGIC a) Remove the row filter from `production.sales.orders`
# MAGIC b) Delete the `region_access` policy definition

# COMMAND ----------

# DBTITLE 1,Solution 15
# Your solution for exercise 15

ex15a = None  # Remove row filter
ex15b = None  # Delete policy

# COMMAND ----------

# DBTITLE 1,Exercise 16: Managed vs External
# MAGIC %md
# MAGIC ## Exercise 16: Managed vs External
# MAGIC **Question**: What happens to the data when you `DROP` a managed table vs an external table?

# COMMAND ----------

# DBTITLE 1,Solution 16
# Your solution for exercise 16

ex16 = None

# COMMAND ----------

# DBTITLE 1,Exercise 17: Create External Table
# MAGIC %md
# MAGIC ## Exercise 17: Create External Table
# MAGIC **Question**: Write SQL to create an external table `raw.source_data` pointing to S3 location `s3://mybucket/data/`.

# COMMAND ----------

# DBTITLE 1,Solution 17
# Your solution for exercise 17

ex17 = None

# COMMAND ----------

# DBTITLE 1,Exercise 18: Convert Table Type
# MAGIC %md
# MAGIC ## Exercise 18: Convert Table Type
# MAGIC **Question**: Write SQL to convert a managed table to an external table.

# COMMAND ----------

# DBTITLE 1,Solution 18
# Your solution for exercise 18

ex18 = None

# COMMAND ----------

# DBTITLE 1,Exercise 19: External Location
# MAGIC %md
# MAGIC ## Exercise 19: External Location
# MAGIC **Question**: What are the two components required to create an external table in Unity Catalog?

# COMMAND ----------

# DBTITLE 1,Solution 19
# Your solution for exercise 19

ex19 = None

# COMMAND ----------

# DBTITLE 1,Exercise 20: Information Schema
# MAGIC %md
# MAGIC ## Exercise 20: Information Schema
# MAGIC **Question**: Write SQL to query all tables in schema `production.sales` using the information schema.

# COMMAND ----------

# DBTITLE 1,Solution 20
# Your solution for exercise 20

ex20 = None

# COMMAND ----------

# DBTITLE 1,MCQs 1-5
# MAGIC %md
# MAGIC ## Multiple Choice Questions
# MAGIC
# MAGIC ### MCQ 1: Hierarchy
# MAGIC How many levels are in the Unity Catalog namespace?
# MAGIC A) 2 levels
# MAGIC B) 3 levels
# MAGIC C) 4 levels
# MAGIC D) 5 levels
# MAGIC
# MAGIC ### MCQ 2: Metastore Scope
# MAGIC What is the scope of a Unity Catalog metastore?
# MAGIC A) Workspace-level
# MAGIC B) Account-level
# MAGIC C) Cluster-level
# MAGIC D) Catalog-level
# MAGIC
# MAGIC ### MCQ 3: External Table DROP
# MAGIC What happens to data when you DROP an external table?
# MAGIC A) Both metadata and data are deleted
# MAGIC B) Only metadata is deleted, data remains
# MAGIC C) Only data is deleted, metadata remains
# MAGIC D) Nothing happens
# MAGIC
# MAGIC ### MCQ 4: ABAC Reusability
# MAGIC Which statement is true about ABAC policies?
# MAGIC A) Must be recreated for each table
# MAGIC B) Can be applied to multiple tables
# MAGIC C) Only work with managed tables
# MAGIC D) Require table-specific functions
# MAGIC
# MAGIC ### MCQ 5: USAGE Permission
# MAGIC What does USAGE permission allow?
# MAGIC A) Read data from tables
# MAGIC B) Write data to tables
# MAGIC C) Navigate to catalog/schema
# MAGIC D) Delete tables

# COMMAND ----------

# DBTITLE 1,MCQ Solutions
# Your answers for MCQs 1-5

mcq1 = None
mcq2 = None
mcq3 = None
mcq4 = None
mcq5 = None

# COMMAND ----------

# DBTITLE 1,Challenge: Complete Governance Setup
# MAGIC %md
# MAGIC ## Challenge: Complete Governance Setup
# MAGIC
# MAGIC **Scenario**: Set up governance for a multi-region sales organization:
# MAGIC
# MAGIC **Requirements**:
# MAGIC 1. Catalog: `global_sales`
# MAGIC 2. Schemas: `us_region`, `eu_region`, `apac_region`
# MAGIC 3. Table: `orders` in each schema with columns: `order_id`, `customer_ssn`, `amount`, `region`
# MAGIC 4. Groups: `us_analysts`, `eu_analysts`, `apac_analysts`, `global_compliance`
# MAGIC 5. Access rules:
# MAGIC    - Each regional analyst group sees only their region's data (row-level)
# MAGIC    - SSN visible to `global_compliance` only, masked for analysts
# MAGIC    - All analysts can read (SELECT), only engineers can write (MODIFY)
# MAGIC    - Use ABAC policies for reusability
# MAGIC
# MAGIC **Task**: Write complete SQL to:
# MAGIC a) Create catalog and schemas
# MAGIC b) Create ABAC row filter policy for regional access
# MAGIC c) Create ABAC column mask policy for SSN
# MAGIC d) Apply policies to all tables
# MAGIC e) Grant appropriate permissions

# COMMAND ----------

# DBTITLE 1,Challenge Solution
# Your solution for the Challenge

challenge = None

# COMMAND ----------

# DBTITLE 1,Applied: Permission Strategy Framework
# MAGIC %md
# MAGIC ## Applied: Permission Strategy Framework
# MAGIC
# MAGIC Develop a decision framework for:
# MAGIC 1. When to use USAGE vs SELECT vs MODIFY
# MAGIC 2. When to use GRANT vs DENY
# MAGIC 3. When to use ABAC policies vs traditional masks
# MAGIC 4. Managed vs external table selection

# COMMAND ----------

# DBTITLE 1,Applied Solution
# Your solution for the Applied

def permission_strategy():
    pass

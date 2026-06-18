# Databricks notebook source
# DBTITLE 1,Topic 2: ELT Spark SQL/Python - Solutions
# MAGIC %md
# MAGIC # Topic 2: ELT with Spark SQL and Python - Solutions
# MAGIC
# MAGIC ## Complete solutions with explanations for all exercises, MCQs, challenges, and applieds.
# MAGIC
# MAGIC ### How to Use
# MAGIC 1. Attempt exercises in practice_tasks.py first
# MAGIC 2. Compare your solutions
# MAGIC 3. Read explanations for WHY
# MAGIC 4. Note common mistakes

# COMMAND ----------

# DBTITLE 1,Setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, row_number, rank, lag, when, lit, udf
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import re

sales_data = [(1, "2024-01-01", "ProductA", "Electronics", 100, 50.0), (2, "2024-01-02", "ProductB", "Electronics", 150, 75.0), (3, "2024-01-03", "ProductA", "Electronics", 120, 50.0), (4, "2024-01-04", "ProductC", "Clothing", 200, 25.0), (5, "2024-01-05", "ProductB", "Electronics", 180, 75.0)] * 500

sales_df = spark.createDataFrame(sales_data, ["order_id", "date", "product", "category", "quantity", "price"])

print("Setup complete")

# COMMAND ----------

# DBTITLE 1,Exercises 1-5: Solutions
# MAGIC %md
# MAGIC ## Exercise 1: Select Columns
# MAGIC ```python
# MAGIC ex1_result = sales_df.select("product", "price")
# MAGIC ```
# MAGIC
# MAGIC ## Exercise 2: Filter Rows
# MAGIC ```python
# MAGIC ex2_result = sales_df.filter(col("quantity") > 150)
# MAGIC ```
# MAGIC Alternative: `.where(col("quantity") > 150)`
# MAGIC
# MAGIC ## Exercise 3: Add Computed Column
# MAGIC ```python
# MAGIC ex3_result = sales_df.withColumn("total", col("quantity") * col("price"))
# MAGIC ```
# MAGIC
# MAGIC ## Exercise 4: Multiple Transformations
# MAGIC ```python
# MAGIC ex4_result = (sales_df
# MAGIC     .filter(col("quantity") > 100)
# MAGIC     .withColumn("total", col("quantity") * col("price"))
# MAGIC     .select("product", "total"))
# MAGIC ```
# MAGIC
# MAGIC ## Exercise 5: Aggregation
# MAGIC ```python
# MAGIC ex5_result = sales_df.groupBy("category").agg(_sum("quantity").alias("total_quantity"))
# MAGIC ```
# MAGIC
# MAGIC **Common Mistakes**: Forgetting `.alias()`, using `sum` instead of `_sum` (import conflict).

# COMMAND ----------

# DBTITLE 1,Exercises 6-10: Solutions
# MAGIC %md
# MAGIC ## Exercise 6: Row Number
# MAGIC ```python
# MAGIC window = Window.partitionBy("category").orderBy(col("price").desc())
# MAGIC ex6_result = sales_df.withColumn("row_num", row_number().over(window))
# MAGIC ```
# MAGIC
# MAGIC ## Exercise 7: Lag Function
# MAGIC ```python
# MAGIC window = Window.partitionBy("product").orderBy("date")
# MAGIC ex7_result = sales_df.withColumn("prev_quantity", lag("quantity", 1).over(window))
# MAGIC ```
# MAGIC
# MAGIC ## Exercise 8: Inner Join
# MAGIC ```python
# MAGIC # Assuming customers_df has order_id
# MAGIC ex8_result = sales_df.join(customers_df, "customer_id", "inner")
# MAGIC ```
# MAGIC
# MAGIC ## Exercise 9: Left Join
# MAGIC ```python
# MAGIC ex9_result = sales_df.join(customers_df, "customer_id", "left")
# MAGIC ```
# MAGIC
# MAGIC ## Exercise 10: Running Total
# MAGIC ```python
# MAGIC window = Window.partitionBy("category").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
# MAGIC ex10_result = sales_df.withColumn("running_total", _sum("quantity").over(window))
# MAGIC ```
# MAGIC
# MAGIC **Key**: `.rowsBetween()` defines frame for aggregation.

# COMMAND ----------

# DBTITLE 1,Exercises 11-15 & MCQs: Solutions
# MAGIC %md
# MAGIC ## Exercise 11: Schema
# MAGIC ```python
# MAGIC schema = StructType([
# MAGIC     StructField("id", IntegerType(), False),
# MAGIC     StructField("name", StringType(), True),
# MAGIC     StructField("amount", DoubleType(), True)
# MAGIC ])
# MAGIC ```
# MAGIC Or: `"id INT, name STRING, amount DOUBLE"`
# MAGIC
# MAGIC ## Exercise 12: UDF
# MAGIC ```python
# MAGIC @udf(returnType=StringType())
# MAGIC def categorize_price(price):
# MAGIC     if price > 60: return "high"
# MAGIC     elif price >= 25: return "medium"
# MAGIC     else: return "low"
# MAGIC ```
# MAGIC
# MAGIC ## Exercise 13: Write Parquet
# MAGIC ```python
# MAGIC sales_df.write.mode("overwrite").parquet("/tmp/sales_output")
# MAGIC ```
# MAGIC
# MAGIC ## Exercise 14: Partitioned Write
# MAGIC ```python
# MAGIC sales_df.write.mode("overwrite").partitionBy("category").parquet("/tmp/sales_partitioned")
# MAGIC ```
# MAGIC
# MAGIC ## Exercise 15: SQL Translation
# MAGIC ```python
# MAGIC ex15_result = (sales_df
# MAGIC     .groupBy("category")
# MAGIC     .agg(avg("price").alias("avg_price"))
# MAGIC     .filter(col("avg_price") > 50))
# MAGIC ```
# MAGIC
# MAGIC ## MCQ Answers
# MAGIC 1: B (overwrite)
# MAGIC 2: B (No difference - aliases)
# MAGIC 3: B (left)
# MAGIC 4: C (Auto-detects types)
# MAGIC 5: A (Broadcast join for small tables)

# COMMAND ----------

# DBTITLE 1,Challenges & Applieds: Solutions
# MAGIC %md
# MAGIC ## Challenge 1: Top 3 per Category
# MAGIC ```python
# MAGIC window = Window.partitionBy("category").orderBy(col("revenue").desc())
# MAGIC challenge1 = (sales_df
# MAGIC     .withColumn("revenue", col("quantity") * col("price"))
# MAGIC     .withColumn("rank", row_number().over(window))
# MAGIC     .filter(col("rank") <= 3))
# MAGIC ```
# MAGIC
# MAGIC ## Challenge 2: Schema Evolution
# MAGIC ```python
# MAGIC existing_df = spark.read.parquet("/path/existing")
# MAGIC new_df = spark.read.parquet("/path/new")
# MAGIC
# MAGIC # Merge with schema evolution
# MAGIC merged = existing_df.unionByName(new_df, allowMissingColumns=True)
# MAGIC merged.write.mode("overwrite").option("mergeSchema", "true").parquet("/path/output")
# MAGIC ```
# MAGIC
# MAGIC ## ETL Applied 1
# MAGIC ```python
# MAGIC def dataframe_summary(df):
# MAGIC     summary = {
# MAGIC         "row_count": df.count(),
# MAGIC         "column_count": len(df.columns),
# MAGIC         "null_counts": {c: df.filter(col(c).isNull()).count() for c in df.columns}
# MAGIC     }
# MAGIC     return summary
# MAGIC ```
# MAGIC
# MAGIC ## ETL Applied 2
# MAGIC ```python
# MAGIC def standardize_column_names(df):
# MAGIC     for col_name in df.columns:
# MAGIC         new_name = col_name.lower().replace(" ", "_")
# MAGIC         new_name = re.sub(r'[^a-z0-9_]', '', new_name)
# MAGIC         df = df.withColumnRenamed(col_name, new_name)
# MAGIC     return df
# MAGIC ```
# MAGIC
# MAGIC ## Key Takeaways
# MAGIC * filter() and where() are identical
# MAGIC * Use broadcast() for small dimension tables
# MAGIC * rowsBetween() critical for window aggregations
# MAGIC * unionByName() handles schema differences
# MAGIC * mergeSchema option for Parquet schema evolution

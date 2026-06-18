# Databricks notebook source
# DBTITLE 1,Topic 2: ELT Spark SQL/Python - Practice Tasks
# MAGIC %md
# MAGIC # Topic 2: ELT with Spark SQL and Python - Practice Tasks
# MAGIC
# MAGIC ## Overview
# MAGIC This notebook contains 15 exercises, 5 MCQs, 2 challenges, and 2 ETL applieds covering DataFrame API, SQL transformations, window functions, reading/writing data, and schema management.
# MAGIC
# MAGIC ## Instructions
# MAGIC 1. Run setup cell first
# MAGIC 2. Attempt each exercise
# MAGIC 3. Check solutions.py for answers
# MAGIC 4. Focus on DataFrame API syntax (exam priority)

# COMMAND ----------

# DBTITLE 1,Serverless Compute Limitations - Important Notes
# MAGIC %md
# MAGIC ## Serverless Compute Limitations
# MAGIC
# MAGIC **Issues encountered during practice:**
# MAGIC
# MAGIC ### 1. File System Restrictions
# MAGIC - **Problem**: Cannot write to `/tmp/` or `dbfs:/` paths on serverless compute
# MAGIC - **Error**: `UnsupportedOperationException` or `AnalysisException` about path not being writable
# MAGIC - **Solution**: Use Unity Catalog tables (`.saveAsTable()`) or Volume paths (`/Volumes/catalog/schema/volume/...`)
# MAGIC - **Affected exercises**: 13, 14
# MAGIC
# MAGIC ### 2. Managed Table Format Restrictions  
# MAGIC - **Problem**: Unity Catalog managed tables only support Delta format by default
# MAGIC - **Error**: `INVALID_PARAMETER_VALUE.MANAGED_TABLE_FORMAT` - "Only Delta is supported for managed tables"
# MAGIC - **Solution**: Remove `.format('parquet')` when using `.saveAsTable()` (Delta is default)
# MAGIC - **Affected exercises**: 13, 14
# MAGIC
# MAGIC ### 3. RDD API Not Available (from Topic 1)
# MAGIC - **Problem**: RDD API methods like `.rdd.getNumPartitions()` don't work on serverless
# MAGIC - **Error**: `AttributeError: 'DataFrame' object has no attribute 'rdd'`
# MAGIC - **Solution**: Use DataFrame API equivalents or acknowledge limitation
# MAGIC - **Affected**: Topic 1 partition inspection exercises
# MAGIC
# MAGIC ### 4. Config Access Restrictions (from Topic 1)
# MAGIC - **Problem**: Some Spark configs not accessible via `spark.conf.get()` on serverless
# MAGIC - **Error**: `NoSuchElementException` for certain config keys
# MAGIC - **Solution**: Use try-except blocks or acknowledge configs may not be inspectable
# MAGIC - **Affected**: Topic 1 config inspection exercises
# MAGIC
# MAGIC **General Pattern**: Serverless compute has a more restricted API surface than classic clusters. When exercises reference classic cluster features, expect to either:
# MAGIC 1. Use alternative APIs
# MAGIC 2. Acknowledge the limitation
# MAGIC 3. Update exercise to fit serverless constraints

# COMMAND ----------

# DBTITLE 1,Setup - Run First
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, row_number, rank, lag, when, lit, udf
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# Sample sales data - generate realistic transactions
import random
from datetime import datetime, timedelta

products = [
    ("ProductA", "Electronics", 50.0),
    ("ProductB", "Electronics", 75.0),
    ("ProductC", "Clothing", 25.0),
    ("ProductD", "Clothing", 40.0),
    ("ProductE", "Home", 60.0),
]

customer_ids = [1, 2, 3]
start_date = datetime(2024, 1, 1)

sales_data = []
for i in range(1, 501):
    product, category, price = random.choice(products)
    customer_id = random.choice(customer_ids)
    order_date = (start_date + timedelta(days=random.randint(0, 90))).strftime("%Y-%m-%d")
    quantity = random.randint(50, 250)
    sales_data.append((i, order_date, product, category, quantity, price, customer_id))

sales_df = spark.createDataFrame(sales_data, ["order_id", "date", "product", "category", "quantity", "price", "customer_id"])

# Sample customer data
customers_data = [
    (1, "Alice", "US", "2023-01-01"),
    (2, "Bob", "UK", "2023-02-01"),
    (3, "Charlie", "US", "2023-03-01"),
]

customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "country", "join_date"])

print("Setup complete!")
print(f"Sales records: {sales_df.count()}")
print(f"Customer records: {customers_df.count()}")

# COMMAND ----------

# DBTITLE 1,Exercises 1-5: DataFrame Basics
# MAGIC %md
# MAGIC ## Exercise 1: Select Columns (Easy)
# MAGIC Select only `product` and `price` columns from `sales_df`.
# MAGIC
# MAGIC ## Exercise 2: Filter Rows (Easy)
# MAGIC Filter `sales_df` for orders where `quantity > 150`.
# MAGIC
# MAGIC ## Exercise 3: Add Computed Column (Easy)
# MAGIC Add a column `total` that is `quantity * price`.
# MAGIC
# MAGIC ## Exercise 4: Multiple Transformations (Medium)
# MAGIC From `sales_df`: filter quantity > 100, add total column, select product and total.
# MAGIC
# MAGIC ## Exercise 5: Aggregation (Medium)
# MAGIC Group by `category` and calculate total quantity sold per category.

# COMMAND ----------

# DBTITLE 1,Exercises 1-5: Your Solutions
# Exercise 1
ex1_result = sales_df.select('product', 'price')
display(ex1_result)

# Exercise 2
ex2_result = sales_df.filter('quantity > 150')
display(ex2_result)

# Exercise 3
ex3_result = sales_df.withColumn('total', col('quantity') * col('price'))
display(ex3_result)


# Exercise 4
ex4_result = sales_df.filter('quantity > 100').withColumn('total', col('quantity') * col('price')).select('product', 'total')
display(ex4_result)

# Exercise 5
ex5_result = sales_df.groupBy('category').agg(_sum('quantity'))
display(ex5_result)


# COMMAND ----------

# DBTITLE 1,Exercises 6-10: Window Functions & Joins
# MAGIC %md
# MAGIC ## Exercise 6: Row Number (Medium)
# MAGIC Add row number partitioned by `category`, ordered by `price` DESC.
# MAGIC
# MAGIC ## Exercise 7: Lag Function (Medium)
# MAGIC Add a column showing the previous order's quantity for each product.
# MAGIC
# MAGIC ## Exercise 8: Inner Join (Medium)
# MAGIC Join `sales_df` with `customers_df` on `customer_id`.
# MAGIC
# MAGIC ## Exercise 9: Left Join (Medium)
# MAGIC Perform left join keeping all sales records.
# MAGIC
# MAGIC ## Exercise 10: Window Aggregation (Hard)
# MAGIC Calculate running total of quantity per category ordered by date.

# COMMAND ----------

# DBTITLE 1,Exercises 6-10: Your Solutions
# Exercise 6
window = Window.partitionBy('category').orderBy(col('price').desc())
ex6_result = sales_df.withColumn('rn', row_number().over(window))
display(ex6_result)

# Exercise 7
window = Window.partitionBy('product').orderBy(col('date').asc())
ex7_result = sales_df.withColumn('prev_order_quantity', lag(col('quantity')).over(window))
display(ex7_result)

# Exercise 8
ex8_result = sales_df.join(customers_df, on='customer_id')
display(ex8_result)

# Exercise 9
ex9_result = sales_df.join(customers_df, on='customer_id', how='left')
display(ex9_result)

# Exercise 10
window = Window.partitionBy('category').orderBy(col('date').asc()).rowsBetween(Window.unboundedPreceding, Window.currentRow)
ex10_result = sales_df.withColumn('running_total', _sum('quantity').over(window))
display(ex10_result)


# COMMAND ----------

# DBTITLE 1,Exercises 11-15: Schema, UDFs, Read/Write
# MAGIC %md
# MAGIC ## Exercise 11: Define Schema (Medium)
# MAGIC Create a schema for: id (int), name (string), amount (double).
# MAGIC
# MAGIC ## Exercise 12: Create UDF (Medium)
# MAGIC Create UDF that categorizes prices: high (>60), medium (25-60), low (<25).
# MAGIC
# MAGIC ## Exercise 13: Write to Unity Catalog Table (Easy)
# MAGIC Write `sales_df` as a managed table to `workspace.default.sales_output`.
# MAGIC **Note**: On serverless compute, use `.saveAsTable()` instead of `.save()`. Unity Catalog managed tables use Delta format by default.
# MAGIC
# MAGIC ## Exercise 14: Write with Partitioning (Medium)
# MAGIC Write `sales_df` as a managed table to `workspace.default.sales_output_partitioned`, partitioned by `category`.
# MAGIC **Note**: Use `.partitionBy('category')` before `.saveAsTable()`.
# MAGIC
# MAGIC ## Exercise 15: SQL Translation (Hard)
# MAGIC Translate to DataFrame API: `SELECT category, AVG(price) as avg_price FROM sales GROUP BY category HAVING avg_price > 50`

# COMMAND ----------

# DBTITLE 1,Exercises 11-15: Your Solutions
# Exercise 11
ex11_schema = StructType([
    StructField('id', IntegerType()),
    StructField('name', StringType()),
    StructField('amount', DoubleType()),
])

# Exercise 12
def categorise_price(price):
    if price < 25:
        return 'low'
    elif price <= 60:
        return 'medium'
    else:
        return 'high'

ex12_udf = udf(categorise_price, StringType())

# Exercise 13
(
    sales_df
    .write
    .mode('overwrite')
    .saveAsTable('workspace.default.sales_output')
)

# Exercise 14
(
    sales_df
    .write
    .mode('overwrite')
    .partitionBy('category')
    .saveAsTable('workspace.default.sales_output_partitioned')
)

# Exercise 15
ex15_result = (
    sales_df
    # .select("category", "price")
    .groupBy("category")
    .agg(avg("price").alias("avg_price"))
    .filter(col("avg_price") > 50)
)
display(ex15_result)

# COMMAND ----------

# DBTITLE 1,MCQs & Challenges
# MAGIC %md
# MAGIC ## MCQ 1
# MAGIC Which save mode overwrites existing data?
# MAGIC A) append  B) overwrite  C) error  D) ignore
# MAGIC
# MAGIC ## MCQ 2
# MAGIC What's the difference between `filter()` and `where()`?
# MAGIC A) filter is faster  B) No difference  C) where supports SQL  D) filter for DataFrames only
# MAGIC
# MAGIC ## MCQ 3
# MAGIC Which join type keeps all rows from left DataFrame?
# MAGIC A) inner  B) left  C) right  D) cross
# MAGIC
# MAGIC ## MCQ 4
# MAGIC What does `inferSchema` do?
# MAGIC A) Creates schema  B) Validates schema  C) Auto-detects types  D) Enforces schema
# MAGIC
# MAGIC ## MCQ 5
# MAGIC Which is most efficient for large joins?
# MAGIC A) Broadcast join  B) Sort-merge  C) Shuffle hash  D) Nested loop
# MAGIC
# MAGIC ## Challenge 1: Complex Transformation
# MAGIC From sales_df: Calculate each product's revenue, rank products by revenue within each category, keep only top 3 per category.
# MAGIC
# MAGIC ## Challenge 2: Schema Evolution
# MAGIC You have existing parquet data. New data has additional columns. Write code to merge both with schema evolution.
# MAGIC
# MAGIC ## ETL Applied 1
# MAGIC Create function that takes DataFrame and returns summary: row count, column count, null counts per column.
# MAGIC
# MAGIC ## ETL Applied 2
# MAGIC Create function that standardizes column names: lowercase, replace spaces with underscores, remove special chars.

# COMMAND ----------

# DBTITLE 1,MCQs, Challenges, Applieds: Your Solutions
# MCQ Answers
mcq_answers = {1: 'B', 2: 'C', 3: 'B', 4: 'C', 5: 'C'}

# Challenge 1
challenge1_result = None

# Challenge 2
# Write your code

# ETL Applied 1
def dataframe_summary(df):
    """
    Return summary statistics for DataFrame.
    """
    pass

# ETL Applied 2
def standardize_column_names(df):
    """
    Standardize all column names.
    """
    pass

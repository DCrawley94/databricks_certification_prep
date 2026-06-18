# Databricks notebook source
# DBTITLE 1,Topic 2: ELT with Spark SQL and Python
# MAGIC %md
# MAGIC # Topic 2: ELT with Spark SQL and Python
# MAGIC
# MAGIC ## Introduction
# MAGIC
# MAGIC This topic covers data transformation using both Spark SQL and PySpark DataFrame API. Understanding both approaches and knowing when to use each is critical for the exam.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC * SQL transformations: CTEs, window functions, aggregations
# MAGIC * PySpark DataFrame API (focus area for your background)
# MAGIC * Reading and writing various data formats
# MAGIC * User-defined functions (UDFs)
# MAGIC * Schema enforcement and evolution
# MAGIC
# MAGIC ### Why This Matters for the Exam
# MAGIC * 29% of exam questions (approximately 13 questions)
# MAGIC * Highest weighted domain
# MAGIC * Heavy focus on DataFrame API syntax (your weak area)
# MAGIC * SQL to PySpark translation questions are common
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 1: SQL vs DataFrame API
# MAGIC %md
# MAGIC ## Concept 1: SQL vs DataFrame API
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC **Spark SQL**:
# MAGIC * Familiar syntax for SQL users
# MAGIC * Better for complex analytical queries
# MAGIC * Easier to read for non-programmers
# MAGIC * Can reference temp views
# MAGIC
# MAGIC **DataFrame API**:
# MAGIC * Programmatic control flow
# MAGIC * Type safety (with PySpark typing)
# MAGIC * Dynamic column operations
# MAGIC * Easier to unit test
# MAGIC
# MAGIC ### Equivalent Operations
# MAGIC
# MAGIC | Operation | SQL | DataFrame API |
# MAGIC |-----------|-----|---------------|
# MAGIC | Filter | `WHERE amount > 100` | `.filter(col("amount") > 100)` |
# MAGIC | Select | `SELECT id, name` | `.select("id", "name")` |
# MAGIC | Aggregate | `GROUP BY category` | `.groupBy("category")` |
# MAGIC | Join | `JOIN other ON id` | `.join(other, "id")` |
# MAGIC | Order | `ORDER BY date DESC` | `.orderBy(col("date").desc())` |
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC Questions often ask you to translate SQL to DataFrame API or vice versa. Know both syntaxes well.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 2: DataFrame Transformations
# MAGIC %md
# MAGIC ## Concept 2: DataFrame Transformations (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### Core Methods
# MAGIC
# MAGIC #### select() - Choose Columns
# MAGIC ```python
# MAGIC # Select specific columns
# MAGIC df.select("customer_id", "amount")
# MAGIC
# MAGIC # Select with expressions
# MAGIC df.select(col("amount") * 1.1)
# MAGIC
# MAGIC # Select with alias
# MAGIC df.select(col("amount").alias("total"))
# MAGIC
# MAGIC # Select all plus computed column
# MAGIC df.select("*", (col("price") * col("quantity")).alias("total"))
# MAGIC ```
# MAGIC
# MAGIC #### filter() / where() - Filter Rows
# MAGIC ```python
# MAGIC # Simple filter
# MAGIC df.filter(col("amount") > 100)
# MAGIC df.where(col("amount") > 100)  # Same as filter
# MAGIC
# MAGIC # Multiple conditions
# MAGIC df.filter((col("amount") > 100) & (col("status") == "active"))
# MAGIC
# MAGIC # String matching
# MAGIC df.filter(col("name").like("A%"))
# MAGIC df.filter(col("name").contains("test"))
# MAGIC df.filter(col("name").isin(["Alice", "Bob"]))
# MAGIC ```
# MAGIC
# MAGIC #### withColumn() - Add/Transform Columns
# MAGIC ```python
# MAGIC # Add new column
# MAGIC df.withColumn("total", col("price") * col("quantity"))
# MAGIC
# MAGIC # Transform existing column
# MAGIC df.withColumn("amount", col("amount") * 1.1)
# MAGIC
# MAGIC # Conditional column
# MAGIC df.withColumn("category", 
# MAGIC     when(col("amount") > 1000, "high")
# MAGIC     .when(col("amount") > 100, "medium")
# MAGIC     .otherwise("low")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### groupBy() + agg() - Aggregations
# MAGIC ```python
# MAGIC from pyspark.sql.functions import sum, avg, count, max, min
# MAGIC
# MAGIC # Single aggregation
# MAGIC df.groupBy("category").agg(sum("amount"))
# MAGIC
# MAGIC # Multiple aggregations
# MAGIC df.groupBy("category").agg(
# MAGIC     sum("amount").alias("total_amount"),
# MAGIC     avg("amount").alias("avg_amount"),
# MAGIC     count("*").alias("count")
# MAGIC )
# MAGIC
# MAGIC # Group by multiple columns
# MAGIC df.groupBy("category", "region").agg(sum("amount"))
# MAGIC ```
# MAGIC
# MAGIC #### join() - Combining DataFrames
# MAGIC ```python
# MAGIC # Inner join (default)
# MAGIC df1.join(df2, "customer_id")
# MAGIC df1.join(df2, on="customer_id", how="inner")
# MAGIC
# MAGIC # Other join types
# MAGIC df1.join(df2, "customer_id", "left")
# MAGIC df1.join(df2, "customer_id", "right")
# MAGIC df1.join(df2, "customer_id", "outer")
# MAGIC df1.join(df2, "customer_id", "left_anti")  # Not in df2
# MAGIC
# MAGIC # Join on multiple columns
# MAGIC df1.join(df2, ["customer_id", "date"])
# MAGIC
# MAGIC # Join with different column names
# MAGIC df1.join(df2, df1.id == df2.customer_id)
# MAGIC ```
# MAGIC
# MAGIC ### Exam Pattern: SQL to DataFrame Translation
# MAGIC
# MAGIC **Question**: "Convert this SQL to DataFrame API:"
# MAGIC ```sql
# MAGIC SELECT category, SUM(amount) as total
# MAGIC FROM sales
# MAGIC WHERE date >= '2024-01-01'
# MAGIC GROUP BY category
# MAGIC HAVING SUM(amount) > 1000
# MAGIC ORDER BY total DESC
# MAGIC ```
# MAGIC
# MAGIC **Answer**:
# MAGIC ```python
# MAGIC df.filter(col("date") >= "2024-01-01") \
# MAGIC   .groupBy("category") \
# MAGIC   .agg(sum("amount").alias("total")) \
# MAGIC   .filter(col("total") > 1000) \
# MAGIC   .orderBy(col("total").desc())
# MAGIC ```
# MAGIC
# MAGIC Note: HAVING clause translates to filter() after aggregation.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 3: Window Functions
# MAGIC %md
# MAGIC ## Concept 3: Window Functions (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### What Are Window Functions?
# MAGIC
# MAGIC Window functions perform calculations across a set of rows related to the current row without collapsing the result set (unlike GROUP BY).
# MAGIC
# MAGIC ### Window Specification
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.window import Window
# MAGIC
# MAGIC # Define window
# MAGIC window = Window.partitionBy("category").orderBy("date")
# MAGIC
# MAGIC # Use with ranking function
# MAGIC df.withColumn("rank", rank().over(window))
# MAGIC ```
# MAGIC
# MAGIC ### Common Window Functions
# MAGIC
# MAGIC #### Ranking Functions
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import row_number, rank, dense_rank
# MAGIC
# MAGIC window = Window.partitionBy("category").orderBy(col("amount").desc())
# MAGIC
# MAGIC # ROW_NUMBER: 1, 2, 3, 4 (unique)
# MAGIC df.withColumn("row_num", row_number().over(window))
# MAGIC
# MAGIC # RANK: 1, 2, 2, 4 (gaps after ties)
# MAGIC df.withColumn("rank", rank().over(window))
# MAGIC
# MAGIC # DENSE_RANK: 1, 2, 2, 3 (no gaps)
# MAGIC df.withColumn("dense_rank", dense_rank().over(window))
# MAGIC ```
# MAGIC
# MAGIC #### Analytic Functions
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import lag, lead, first, last
# MAGIC
# MAGIC window = Window.partitionBy("customer_id").orderBy("date")
# MAGIC
# MAGIC # LAG: Previous row value
# MAGIC df.withColumn("prev_amount", lag("amount", 1).over(window))
# MAGIC
# MAGIC # LEAD: Next row value
# MAGIC df.withColumn("next_amount", lead("amount", 1).over(window))
# MAGIC
# MAGIC # FIRST: First value in window
# MAGIC df.withColumn("first_amount", first("amount").over(window))
# MAGIC
# MAGIC # LAST: Last value in window
# MAGIC df.withColumn("last_amount", last("amount").over(window))
# MAGIC ```
# MAGIC
# MAGIC #### Aggregate Functions Over Windows
# MAGIC
# MAGIC ```python
# MAGIC window = Window.partitionBy("category").orderBy("date") \
# MAGIC                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
# MAGIC
# MAGIC # Running total
# MAGIC df.withColumn("running_total", sum("amount").over(window))
# MAGIC
# MAGIC # Running average
# MAGIC df.withColumn("running_avg", avg("amount").over(window))
# MAGIC
# MAGIC # Cumulative count
# MAGIC df.withColumn("cumulative_count", count("*").over(window))
# MAGIC ```
# MAGIC
# MAGIC ### Window Frame Specifications
# MAGIC
# MAGIC ```python
# MAGIC # Rows between
# MAGIC Window.partitionBy("id").orderBy("date") \
# MAGIC       .rowsBetween(-2, 0)  # Current + 2 previous rows
# MAGIC
# MAGIC Window.partitionBy("id").orderBy("date") \
# MAGIC       .rowsBetween(Window.unboundedPreceding, Window.currentRow)  # Start to current
# MAGIC
# MAGIC Window.partitionBy("id").orderBy("date") \
# MAGIC       .rowsBetween(Window.currentRow, Window.unboundedFollowing)  # Current to end
# MAGIC
# MAGIC # Range between (value-based, not row-based)
# MAGIC Window.partitionBy("id").orderBy("timestamp") \
# MAGIC       .rangeBetween(-86400, 0)  # Last 24 hours (if timestamp is in seconds)
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Common trap**: Forgetting `.over(window)` in window functions
# MAGIC
# MAGIC **Pattern**: "Find the top N rows per group" requires `row_number()` or `rank()` with filter
# MAGIC
# MAGIC ```python
# MAGIC # Top 3 products by sales per category
# MAGIC window = Window.partitionBy("category").orderBy(col("sales").desc())
# MAGIC df.withColumn("rank", row_number().over(window)) \
# MAGIC   .filter(col("rank") <= 3)
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 4: Reading and Writing Data
# MAGIC %md
# MAGIC ## Concept 4: Reading and Writing Data (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### Reading Data
# MAGIC
# MAGIC #### CSV Files
# MAGIC ```python
# MAGIC # Basic read
# MAGIC df = spark.read.csv("/path/to/file.csv")
# MAGIC
# MAGIC # With options
# MAGIC df = spark.read.format("csv") \
# MAGIC     .option("header", "true") \
# MAGIC     .option("inferSchema", "true") \
# MAGIC     .option("delimiter", ",") \
# MAGIC     .load("/path/to/file.csv")
# MAGIC
# MAGIC # Alternative syntax
# MAGIC df = spark.read.csv("/path/to/file.csv", header=True, inferSchema=True)
# MAGIC ```
# MAGIC
# MAGIC #### JSON Files
# MAGIC ```python
# MAGIC # Basic read
# MAGIC df = spark.read.json("/path/to/file.json")
# MAGIC
# MAGIC # Multi-line JSON
# MAGIC df = spark.read.option("multiLine", "true").json("/path/to/file.json")
# MAGIC ```
# MAGIC
# MAGIC #### Parquet Files
# MAGIC ```python
# MAGIC # Parquet (columnar format, most efficient)
# MAGIC df = spark.read.parquet("/path/to/file.parquet")
# MAGIC ```
# MAGIC
# MAGIC #### Delta Tables
# MAGIC ```python
# MAGIC # Read Delta table
# MAGIC df = spark.read.format("delta").load("/path/to/delta")
# MAGIC
# MAGIC # Read from catalog
# MAGIC df = spark.table("catalog.schema.table_name")
# MAGIC ```
# MAGIC
# MAGIC ### Common Read Options
# MAGIC
# MAGIC | Option | Purpose | Values | When to Use |
# MAGIC |--------|---------|--------|-------------|
# MAGIC | `header` | First row is header | true/false | CSV with headers |
# MAGIC | `inferSchema` | Auto-detect types | true/false | When schema unknown |
# MAGIC | `delimiter` | Column separator | ",", "\t", "|" | Non-comma CSVs |
# MAGIC | `multiLine` | JSON spans lines | true/false | Pretty-printed JSON |
# MAGIC | `mergeSchema` | Combine schemas | true/false | Schema evolution |
# MAGIC
# MAGIC ### Writing Data
# MAGIC
# MAGIC #### Save Modes
# MAGIC
# MAGIC ```python
# MAGIC # error (default): Fail if path exists
# MAGIC df.write.mode("error").parquet("/path")
# MAGIC
# MAGIC # append: Add to existing data
# MAGIC df.write.mode("append").parquet("/path")
# MAGIC
# MAGIC # overwrite: Replace existing data
# MAGIC df.write.mode("overwrite").parquet("/path")
# MAGIC
# MAGIC # ignore: Do nothing if path exists
# MAGIC df.write.mode("ignore").parquet("/path")
# MAGIC ```
# MAGIC
# MAGIC #### Write Formats
# MAGIC
# MAGIC ```python
# MAGIC # Parquet
# MAGIC df.write.parquet("/path/to/output")
# MAGIC
# MAGIC # CSV
# MAGIC df.write.option("header", "true").csv("/path/to/output")
# MAGIC
# MAGIC # JSON
# MAGIC df.write.json("/path/to/output")
# MAGIC
# MAGIC # Delta
# MAGIC df.write.format("delta").save("/path/to/delta")
# MAGIC ```
# MAGIC
# MAGIC #### Partitioned Writes
# MAGIC
# MAGIC ```python
# MAGIC # Partition by column
# MAGIC df.write.partitionBy("year", "month").parquet("/path")
# MAGIC
# MAGIC # Creates structure: /path/year=2024/month=01/
# MAGIC #                     /path/year=2024/month=02/
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Common question**: "Which option is needed to read a CSV with headers?"
# MAGIC * Answer: `option("header", "true")`
# MAGIC
# MAGIC **Common trap**: Forgetting `inferSchema=true` results in all columns as strings
# MAGIC
# MAGIC **Save mode confusion**: 
# MAGIC * `append` adds rows (does NOT merge schemas by default)
# MAGIC * `overwrite` replaces entire dataset
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 5: Schema Management
# MAGIC %md
# MAGIC ## Concept 5: Schema Management
# MAGIC
# MAGIC ### Defining Schema
# MAGIC
# MAGIC #### Using StructType
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# MAGIC
# MAGIC schema = StructType([
# MAGIC     StructField("customer_id", IntegerType(), nullable=False),
# MAGIC     StructField("name", StringType(), nullable=True),
# MAGIC     StructField("amount", DoubleType(), nullable=True)
# MAGIC ])
# MAGIC
# MAGIC # Use with read
# MAGIC df = spark.read.schema(schema).csv("/path/to/file.csv")
# MAGIC ```
# MAGIC
# MAGIC #### DDL String (Simpler Syntax)
# MAGIC
# MAGIC ```python
# MAGIC schema = "customer_id INT, name STRING, amount DOUBLE"
# MAGIC
# MAGIC df = spark.read.schema(schema).csv("/path/to/file.csv")
# MAGIC ```
# MAGIC
# MAGIC ### Common Data Types
# MAGIC
# MAGIC | Type | PySpark | SQL Equivalent |
# MAGIC |------|---------|----------------|
# MAGIC | Integer | `IntegerType()` | INT |
# MAGIC | Long | `LongType()` | BIGINT |
# MAGIC | Double | `DoubleType()` | DOUBLE |
# MAGIC | String | `StringType()` | STRING |
# MAGIC | Boolean | `BooleanType()` | BOOLEAN |
# MAGIC | Date | `DateType()` | DATE |
# MAGIC | Timestamp | `TimestampType()` | TIMESTAMP |
# MAGIC | Decimal | `DecimalType(10,2)` | DECIMAL(10,2) |
# MAGIC | Array | `ArrayType(StringType())` | ARRAY<STRING> |
# MAGIC | Struct | `StructType([...])` | STRUCT<...> |
# MAGIC
# MAGIC ### Schema Enforcement vs Evolution
# MAGIC
# MAGIC #### Schema Enforcement (Default)
# MAGIC
# MAGIC ```python
# MAGIC # Writing with schema enforcement
# MAGIC df.write.format("delta").save("/path")
# MAGIC
# MAGIC # Later write with different schema fails
# MAGIC df_new.write.format("delta").mode("append").save("/path")  # Error if schema differs
# MAGIC ```
# MAGIC
# MAGIC #### Schema Evolution
# MAGIC
# MAGIC ```python
# MAGIC # Allow schema changes
# MAGIC df_new.write.format("delta") \
# MAGIC     .mode("append") \
# MAGIC     .option("mergeSchema", "true") \
# MAGIC     .save("/path")
# MAGIC ```
# MAGIC
# MAGIC **When to use schema evolution**:
# MAGIC * Adding new columns to existing table
# MAGIC * Changing nullable constraints
# MAGIC
# MAGIC **When NOT to use**:
# MAGIC * Production tables (maintain strict schema)
# MAGIC * When data quality is critical
# MAGIC
# MAGIC ### Schema Operations
# MAGIC
# MAGIC ```python
# MAGIC # Print schema
# MAGIC df.printSchema()
# MAGIC
# MAGIC # Get schema as StructType
# MAGIC schema = df.schema
# MAGIC
# MAGIC # Get column names
# MAGIC columns = df.columns
# MAGIC
# MAGIC # Get data types
# MAGIC dtypes = df.dtypes  # List of (name, type) tuples
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question pattern**: "How do you add a new column to an existing Delta table?"
# MAGIC * Answer: Write with `mergeSchema=true`
# MAGIC
# MAGIC **Common mistake**: Forgetting `nullable` parameter in StructField
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 6: User-Defined Functions
# MAGIC %md
# MAGIC ## Concept 6: User-Defined Functions (UDFs)
# MAGIC
# MAGIC ### Python UDFs
# MAGIC
# MAGIC #### Basic UDF
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import udf
# MAGIC from pyspark.sql.types import StringType
# MAGIC
# MAGIC # Define Python function
# MAGIC def categorize_amount(amount):
# MAGIC     if amount > 1000:
# MAGIC         return "high"
# MAGIC     elif amount > 100:
# MAGIC         return "medium"
# MAGIC     else:
# MAGIC         return "low"
# MAGIC
# MAGIC # Register as UDF
# MAGIC categorize_udf = udf(categorize_amount, StringType())
# MAGIC
# MAGIC # Use in DataFrame
# MAGIC df.withColumn("category", categorize_udf(col("amount")))
# MAGIC ```
# MAGIC
# MAGIC #### UDF with Decorator
# MAGIC
# MAGIC ```python
# MAGIC @udf(returnType=StringType())
# MAGIC def categorize_amount(amount):
# MAGIC     if amount > 1000:
# MAGIC         return "high"
# MAGIC     elif amount > 100:
# MAGIC         return "medium"
# MAGIC     else:
# MAGIC         return "low"
# MAGIC
# MAGIC df.withColumn("category", categorize_amount(col("amount")))
# MAGIC ```
# MAGIC
# MAGIC ### SQL UDFs
# MAGIC
# MAGIC ```python
# MAGIC # Register for SQL use
# MAGIC spark.udf.register("categorize", categorize_amount, StringType())
# MAGIC
# MAGIC # Use in SQL
# MAGIC spark.sql("SELECT *, categorize(amount) as category FROM table")
# MAGIC ```
# MAGIC
# MAGIC ### Performance Considerations
# MAGIC
# MAGIC **Python UDFs are slow** because:
# MAGIC * Data serialization between JVM and Python
# MAGIC * Row-by-row processing
# MAGIC * No Catalyst optimization
# MAGIC
# MAGIC **Alternatives to UDFs**:
# MAGIC
# MAGIC ```python
# MAGIC # Instead of UDF
# MAGIC @udf(returnType=StringType())
# MAGIC def categorize(amount):
# MAGIC     return "high" if amount > 1000 else "low"
# MAGIC
# MAGIC df.withColumn("category", categorize(col("amount")))
# MAGIC
# MAGIC # Use built-in when()
# MAGIC df.withColumn("category",
# MAGIC     when(col("amount") > 1000, "high")
# MAGIC     .when(col("amount") > 100, "medium")
# MAGIC     .otherwise("low")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Pandas UDFs (Vectorized UDFs)
# MAGIC
# MAGIC **Much faster than regular UDFs** because they process batches:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import pandas_udf
# MAGIC import pandas as pd
# MAGIC
# MAGIC @pandas_udf("double")
# MAGIC def calculate_tax(amount: pd.Series) -> pd.Series:
# MAGIC     return amount * 0.1
# MAGIC
# MAGIC df.withColumn("tax", calculate_tax(col("amount")))
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "Why are Python UDFs slower than built-in functions?"
# MAGIC * Answer: Serialization overhead and no Catalyst optimization
# MAGIC
# MAGIC **Question**: "What's a faster alternative to Python UDFs?"
# MAGIC * Answer: Pandas UDFs (vectorized UDFs) or built-in functions
# MAGIC
# MAGIC **Best practice**: Avoid UDFs when possible; use built-in functions
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Key Takeaways & Exam Focus
# MAGIC %md
# MAGIC ## Key Takeaways & Exam Focus
# MAGIC
# MAGIC ### Most Testable Concepts
# MAGIC
# MAGIC 1. **DataFrame API transformations** (Very High Frequency)
# MAGIC    * filter(), select(), withColumn(), groupBy(), join()
# MAGIC    * Know all join types
# MAGIC    * Translation from SQL to DataFrame API
# MAGIC
# MAGIC 2. **Window functions** (High Frequency)
# MAGIC    * row_number(), rank(), dense_rank()
# MAGIC    * lag(), lead()
# MAGIC    * Window specification syntax
# MAGIC
# MAGIC 3. **Read/Write options** (High Frequency)
# MAGIC    * CSV: header, inferSchema, delimiter
# MAGIC    * Save modes: append, overwrite, error, ignore
# MAGIC    * Partitioned writes
# MAGIC
# MAGIC 4. **Schema management** (Medium Frequency)
# MAGIC    * StructType definition
# MAGIC    * Schema enforcement vs evolution
# MAGIC    * mergeSchema option
# MAGIC
# MAGIC 5. **UDFs** (Medium Frequency)
# MAGIC    * Performance implications
# MAGIC    * When to avoid UDFs
# MAGIC    * Pandas UDFs as alternative
# MAGIC
# MAGIC ### SQL to DataFrame API Quick Reference
# MAGIC
# MAGIC | SQL | DataFrame API |
# MAGIC |-----|---------------|
# MAGIC | `SELECT col1, col2` | `.select("col1", "col2")` |
# MAGIC | `WHERE amount > 100` | `.filter(col("amount") > 100)` |
# MAGIC | `GROUP BY category` | `.groupBy("category")` |
# MAGIC | `HAVING SUM(amount) > 1000` | `.agg(...).filter(col("sum") > 1000)` |
# MAGIC | `ORDER BY date DESC` | `.orderBy(col("date").desc())` |
# MAGIC | `JOIN other ON id` | `.join(other, "id")` |
# MAGIC | `LEFT JOIN` | `.join(other, "id", "left")` |
# MAGIC | `UNION ALL` | `.union(other)` |
# MAGIC | `DISTINCT` | `.distinct()` |
# MAGIC | `LIMIT 10` | `.limit(10)` |
# MAGIC
# MAGIC ### What to Memorize
# MAGIC
# MAGIC **Must know by heart**:
# MAGIC * All join types: inner, left, right, outer, left_anti, left_semi
# MAGIC * Save modes: error (default), append, overwrite, ignore
# MAGIC * Common read options: header, inferSchema, delimiter, multiLine
# MAGIC * Window function differences: row_number vs rank vs dense_rank
# MAGIC * When HAVING becomes filter() after aggregation
# MAGIC
# MAGIC ### Exam Question Patterns
# MAGIC
# MAGIC **Pattern 1**: "Convert this SQL query to DataFrame API"
# MAGIC * Practice translating complex SQL with joins, aggregations, window functions
# MAGIC
# MAGIC **Pattern 2**: "Which option is required to read a CSV file with headers?"
# MAGIC * Know all read/write options
# MAGIC
# MAGIC **Pattern 3**: "What's the performance issue with Python UDFs?"
# MAGIC * Understand serialization overhead
# MAGIC
# MAGIC **Pattern 4**: "How do you find the top 3 items per category?"
# MAGIC * Use row_number() with window partitioned by category
# MAGIC
# MAGIC ### Common Mistakes to Avoid
# MAGIC
# MAGIC * Forgetting `.over(window)` in window functions
# MAGIC * Using `&` instead of `and` in filter conditions (must use `&` and `|` for DataFrame boolean logic)
# MAGIC * Confusing `append` mode (adds rows) with `mergeSchema` (allows schema changes)
# MAGIC * Not specifying `inferSchema=true` when reading CSVs (results in all strings)
# MAGIC * Using Python UDFs when built-in functions would work
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Additional Resources
# MAGIC
# MAGIC * [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
# MAGIC * [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
# MAGIC * [DataFrame Operations](https://spark.apache.org/docs/latest/sql-ref.html)
# MAGIC
# MAGIC **Next**: Open `practice_tasks.py` to test your DataFrame API skills.

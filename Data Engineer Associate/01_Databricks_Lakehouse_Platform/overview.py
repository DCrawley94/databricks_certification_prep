# Databricks notebook source
# DBTITLE 1,Topic 1: Databricks Lakehouse Platform - Overview
# MAGIC %md
# MAGIC # Topic 1: Databricks Lakehouse Platform
# MAGIC
# MAGIC ## Introduction
# MAGIC
# MAGIC The Databricks Lakehouse Platform combines the best of data lakes and data warehouses, providing a unified platform for data engineering, analytics, and machine learning. Understanding the platform architecture and compute options is foundational for the Data Engineer Associate exam.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC * Databricks workspace architecture and navigation
# MAGIC * Compute resources: clusters, SQL warehouses, serverless
# MAGIC * Critical Spark configurations
# MAGIC * Cluster policies and access modes
# MAGIC * Storage options (DBFS, external storage)
# MAGIC
# MAGIC ### Why This Matters for the Exam
# MAGIC * ~22% of exam questions (approximately 10 questions)
# MAGIC * Heavy focus on Spark configurations and cluster setup
# MAGIC * Understanding compute selection is key for performance questions
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 1: Databricks Architecture
# MAGIC %md
# MAGIC ## Concept 1: Databricks Architecture
# MAGIC
# MAGIC ### The Lakehouse Platform
# MAGIC
# MAGIC Databricks implements a **Lakehouse architecture** that unifies:
# MAGIC * **Data Lake**: Scalable, low-cost storage for all data types
# MAGIC * **Data Warehouse**: ACID transactions, schema enforcement, query performance
# MAGIC * **ML/AI Platform**: Integrated ML workflows, model serving
# MAGIC
# MAGIC ### Key Components
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │              Databricks Workspace (Control Plane)        │
# MAGIC │  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
# MAGIC │  │ Notebooks│  │ Jobs     │  │ Clusters │             │
# MAGIC │  └──────────┘  └──────────┘  └──────────┘             │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                         ↕
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │              Compute Plane (Data Plane)                  │
# MAGIC │  ┌──────────────┐  ┌──────────────┐                    │
# MAGIC │  │ Spark Cluster│  │ SQL Warehouse│                    │
# MAGIC │  └──────────────┘  └──────────────┘                    │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC                         ↕
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │              Storage (Delta Lake)                        │
# MAGIC │  ┌──────────────┐  ┌──────────────┐                    │
# MAGIC │  │ Cloud Storage│  │ Unity Catalog│                    │
# MAGIC │  │ (S3/ADLS/GCS)│  │ Metastore    │                    │
# MAGIC │  └──────────────┘  └──────────────┘                    │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Workspace Components
# MAGIC
# MAGIC | Component | Purpose | Exam Relevance |
# MAGIC |-----------|---------|----------------|
# MAGIC | **Notebooks** | Interactive code development | Medium - understand magic commands |
# MAGIC | **Jobs** | Scheduled/triggered workflows | High - know task types, scheduling |
# MAGIC | **Clusters** | Compute resources | High - configuration is heavily tested |
# MAGIC | **SQL Warehouses** | SQL analytics compute | Medium - understand sizing |
# MAGIC | **Repos** | Git integration | Low - basic understanding |
# MAGIC | **DBFS** | Databricks File System | Medium - understand mount points |
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Common Question Pattern**: "Which component would you use for scheduled ETL pipelines?"
# MAGIC * Answer: Jobs (not notebooks running manually)
# MAGIC
# MAGIC **Common Trap**: Confusing DBFS with external storage
# MAGIC * DBFS is backed by cloud storage but has the `/dbfs/` path convention
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 2: Compute Resources
# MAGIC %md
# MAGIC ## Concept 2: Compute Resources
# MAGIC
# MAGIC ### Types of Compute
# MAGIC
# MAGIC Databricks offers three main compute options:
# MAGIC
# MAGIC #### 1. All-Purpose Clusters
# MAGIC **Used for**: Interactive development, notebooks, ad-hoc analysis
# MAGIC
# MAGIC **Characteristics**:
# MAGIC * Can be started/stopped manually or auto-terminate
# MAGIC * Support multiple users (with access modes)
# MAGIC * More expensive than jobs clusters
# MAGIC * Retain cached data between runs
# MAGIC
# MAGIC **When to use**:
# MAGIC * Developing and testing code
# MAGIC * Exploratory data analysis
# MAGIC * Interactive ML model development
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Jobs Clusters
# MAGIC **Used for**: Scheduled jobs, automated workflows
# MAGIC
# MAGIC **Characteristics**:
# MAGIC * Created automatically when job runs
# MAGIC * Terminated when job completes
# MAGIC * More cost-effective than all-purpose
# MAGIC * Isolated (one job per cluster)
# MAGIC
# MAGIC **When to use**:
# MAGIC * Production ETL pipelines
# MAGIC * Scheduled data processing
# MAGIC * Automated reporting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3. SQL Warehouses (formerly SQL Endpoints)
# MAGIC **Used for**: SQL queries, BI dashboards, analytics
# MAGIC
# MAGIC **Characteristics**:
# MAGIC * Optimized for SQL workloads
# MAGIC * Auto-scaling and serverless options
# MAGIC * Support query result caching
# MAGIC * Multiple concurrent users
# MAGIC
# MAGIC **Warehouse Types**:
# MAGIC
# MAGIC | Type | Use Case | Auto-scaling | Cost |
# MAGIC |------|----------|--------------|------|
# MAGIC | **Serverless** | Production SQL, dashboards | Yes | Pay-per-query |
# MAGIC | **Pro** | General SQL analytics | Yes | Mid-range |
# MAGIC | **Classic** | Legacy workloads | Limited | Lower |
# MAGIC
# MAGIC **When to use**:
# MAGIC * Running SQL queries
# MAGIC * Dashboarding (Lakeview, BI tools)
# MAGIC * Ad-hoc analytics by analysts
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Cluster Access Modes
# MAGIC
# MAGIC | Mode | Languages | Use Case | Isolation |
# MAGIC |------|-----------|----------|----------|
# MAGIC | **No Isolation Shared** | All (Python, SQL, Scala, R) | Data engineering, full control | Low |
# MAGIC | **Shared** | SQL, Python (with restrictions) | Multi-user analytics, Unity Catalog | High |
# MAGIC | **Single User** | All | Individual developer | High |
# MAGIC
# MAGIC ### Exam Comparison Table
# MAGIC
# MAGIC | Feature | All-Purpose | Jobs | SQL Warehouse |
# MAGIC |---------|-------------|------|---------------|
# MAGIC | Cost | $$$ | $ | $$ |
# MAGIC | Auto-terminate | Optional | Always | Optional |
# MAGIC | Multi-user | Yes | No | Yes |
# MAGIC | Best for | Development | Production | SQL/BI |
# MAGIC | Exam frequency | High | High | Medium |
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Cost optimization question**: "What's the most cost-effective compute for production ETL?"
# MAGIC * Answer: Jobs clusters (not all-purpose)
# MAGIC
# MAGIC **SQL workload question**: "Which compute should you use for interactive SQL queries?"
# MAGIC * Answer: SQL Warehouse (not Spark cluster)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 3: Critical Spark Configurations
# MAGIC %md
# MAGIC ## Concept 3: Critical Spark Configurations (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### Why Configurations Matter
# MAGIC
# MAGIC Spark configurations control:
# MAGIC * Memory allocation
# MAGIC * Parallelism (how many tasks run simultaneously)
# MAGIC * Performance optimizations
# MAGIC * Delta Lake behavior
# MAGIC
# MAGIC **Exam Impact**: Config questions appear in ~20-25% of the exam!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Executor Configurations
# MAGIC
# MAGIC #### spark.executor.memory
# MAGIC **What it controls**: Memory available to each executor
# MAGIC
# MAGIC **Default**: Varies by cluster size (typically 4-16 GB)
# MAGIC
# MAGIC **Format**: `"8g"`, `"16g"`, `"32g"`
# MAGIC
# MAGIC **When to increase**:
# MAGIC * Large data shuffles
# MAGIC * Wide transformations
# MAGIC * Caching large datasets
# MAGIC * Getting OOM (Out of Memory) errors
# MAGIC
# MAGIC **Exam tip**: More memory isn't always better - too much can reduce parallelism
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### spark.executor.cores
# MAGIC **What it controls**: CPU cores per executor
# MAGIC
# MAGIC **Default**: Varies by cluster (typically 4-8)
# MAGIC
# MAGIC **Range**: Usually 2-8 cores
# MAGIC
# MAGIC **When to adjust**:
# MAGIC * CPU-intensive workloads → increase
# MAGIC * Memory-intensive workloads → decrease (more memory per core)
# MAGIC
# MAGIC **Formula**:
# MAGIC ```
# MAGIC Total parallelism = num_executors × executor_cores
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Parallelism Configurations
# MAGIC
# MAGIC #### spark.sql.shuffle.partitions (VERY HIGH EXAM FREQUENCY)
# MAGIC **What it controls**: Number of partitions after shuffle operations
# MAGIC
# MAGIC **Default**: 200
# MAGIC
# MAGIC **When to change**:
# MAGIC
# MAGIC | Data Size | Recommended Value | Reasoning |
# MAGIC |-----------|-------------------|----------|
# MAGIC | < 1 GB | 10-50 | Avoid too many small tasks |
# MAGIC | 1-10 GB | 100-200 | Default works well |
# MAGIC | 10-100 GB | 200-500 | Increase parallelism |
# MAGIC | > 100 GB | 500-2000 | Maximize cluster utilization |
# MAGIC
# MAGIC **Exam trap**: Default 200 is often too high for small datasets!
# MAGIC
# MAGIC **Example**:
# MAGIC ```python
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", "100")
# MAGIC
# MAGIC # Now operations like groupBy, join will use 100 partitions
# MAGIC df.groupBy("customer_id").agg(sum("revenue"))
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### spark.default.parallelism
# MAGIC **What it controls**: Default partitions for RDD operations
# MAGIC
# MAGIC **Default**: Number of cores in cluster
# MAGIC
# MAGIC **When it applies**: RDD transformations (not DataFrame operations)
# MAGIC
# MAGIC **Exam note**: Less commonly tested than `shuffle.partitions`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Adaptive Query Execution (AQE) Configs
# MAGIC
# MAGIC #### spark.sql.adaptive.enabled (HIGH EXAM FREQUENCY)
# MAGIC **What it does**: Enables runtime query optimization
# MAGIC
# MAGIC **Default**: `true` (on by default in newer DBR versions)
# MAGIC
# MAGIC **Benefits**:
# MAGIC * Dynamically coalesces shuffle partitions
# MAGIC * Converts sort-merge joins to broadcast joins
# MAGIC * Handles skewed joins automatically
# MAGIC
# MAGIC **When to disable**: Rarely! Only if you need predictable explain plans
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### spark.sql.adaptive.coalescePartitions.enabled
# MAGIC **What it does**: Reduces number of partitions after shuffle if data is small
# MAGIC
# MAGIC **Default**: `true`
# MAGIC
# MAGIC **Example**: If shuffle.partitions = 200 but data is only 50 MB, AQE may coalesce to 10 partitions
# MAGIC
# MAGIC **Benefit**: Prevents many small tasks, improves performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Broadcast Join Configurations
# MAGIC
# MAGIC #### spark.sql.autoBroadcastJoinThreshold
# MAGIC **What it controls**: Maximum size (bytes) for broadcasting a table
# MAGIC
# MAGIC **Default**: 10 MB (10485760 bytes)
# MAGIC
# MAGIC **When to increase**: If you have larger dimension tables that fit in memory
# MAGIC
# MAGIC **Example**:
# MAGIC ```python
# MAGIC # Increase threshold to 50 MB
# MAGIC spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")
# MAGIC
# MAGIC # Now tables up to 50 MB will be broadcast
# MAGIC df_large.join(df_medium, "customer_id")  # df_medium broadcast if < 50 MB
# MAGIC ```
# MAGIC
# MAGIC **Exam tip**: Broadcast joins are much faster than shuffle joins for small tables!
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Configuration Reference Table (MEMORIZE THIS)
# MAGIC
# MAGIC | Configuration | Default | Purpose | When to Change | Exam Frequency |
# MAGIC |---------------|---------|---------|----------------|----------------|
# MAGIC | `spark.executor.memory` | 4-16g | Executor RAM | Large shuffles, OOM errors | Medium |
# MAGIC | `spark.executor.cores` | 4-8 | Cores per executor | Balance CPU vs memory | Medium |
# MAGIC | `spark.sql.shuffle.partitions` | 200 | Shuffle parallelism | Match data size | **VERY HIGH** |
# MAGIC | `spark.sql.adaptive.enabled` | true | Runtime optimization | Rarely disable | High |
# MAGIC | `spark.sql.adaptive.coalescePartitions.enabled` | true | Reduce small partitions | Keep enabled | Medium |
# MAGIC | `spark.sql.autoBroadcastJoinThreshold` | 10MB | Broadcast join size | Larger dimension tables | High |
# MAGIC | `spark.sql.files.maxPartitionBytes` | 128MB | File partition size | Control scan parallelism | Medium |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### How to Set Configurations
# MAGIC
# MAGIC #### Method 1: In Notebook
# MAGIC ```python
# MAGIC # Set for current session
# MAGIC spark.conf.set("spark.sql.shuffle.partitions", "100")
# MAGIC
# MAGIC # Verify
# MAGIC print(spark.conf.get("spark.sql.shuffle.partitions"))
# MAGIC ```
# MAGIC
# MAGIC #### Method 2: Cluster Configuration (Spark tab)
# MAGIC ```
# MAGIC spark.sql.shuffle.partitions 100
# MAGIC spark.sql.adaptive.enabled true
# MAGIC ```
# MAGIC
# MAGIC #### Method 3: Jobs Configuration
# MAGIC ```json
# MAGIC {
# MAGIC   "spark_conf": {
# MAGIC     "spark.sql.shuffle.partitions": "100"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exam Question Patterns
# MAGIC
# MAGIC **Pattern 1**: "Your job processes 500 GB of data but runs slowly with many small tasks. Which config should you adjust?"
# MAGIC * Answer: Increase `spark.sql.shuffle.partitions` (from 200 to 500-1000)
# MAGIC
# MAGIC **Pattern 2**: "You're joining a 10 TB fact table with a 100 MB dimension table. How can you optimize?"
# MAGIC * Answer: Increase `spark.sql.autoBroadcastJoinThreshold` to 100+ MB
# MAGIC
# MAGIC **Pattern 3**: "What does `spark.sql.adaptive.enabled=true` provide?"
# MAGIC * Answer: Runtime query optimization (coalescing partitions, broadcast optimization)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Example: Configuration Impact
# Example: See the impact of shuffle.partitions

# Create sample data (10,000 rows with 10,000 unique categories)
data = [(i, f"value_{i}", i * 100) for i in range(10000)]
df = spark.createDataFrame(data, ["id", "category", "amount"])

# Check default configuration
default_config = spark.conf.get("spark.sql.shuffle.partitions")

# Perform groupBy with default config
result_default = df.groupBy("category").agg({"amount": "sum"})

# Now adjust configuration for small dataset
spark.conf.set("spark.sql.shuffle.partitions", "10")
optimized_config = spark.conf.get("spark.sql.shuffle.partitions")

# Perform groupBy with optimized config
result_optimized = df.groupBy("category").agg({"amount": "sum"})

# Show the configuration comparison
config_comparison = spark.createDataFrame([
    ("Default", default_config, "Too many partitions for small data"),
    ("Optimized", optimized_config, "Right-sized for 10K rows")
], ["Config", "shuffle.partitions", "Note"])

display(config_comparison)

# COMMAND ----------

# DBTITLE 1,Concept 4: Cluster Policies
# MAGIC %md
# MAGIC ## Concept 4: Cluster Policies
# MAGIC
# MAGIC ### What Are Cluster Policies?
# MAGIC
# MAGIC Cluster policies are **templates** that:
# MAGIC * Enforce standards across an organization
# MAGIC * Limit configuration options
# MAGIC * Control costs
# MAGIC * Ensure security and compliance
# MAGIC
# MAGIC ### Types of Policies
# MAGIC
# MAGIC #### 1. Unrestricted Policies
# MAGIC * Allow users to configure any cluster settings
# MAGIC * Used for admin/power users
# MAGIC * No restrictions
# MAGIC
# MAGIC #### 2. Limited Policies
# MAGIC * Fix certain configurations
# MAGIC * Allow users to choose others
# MAGIC * Example: Fix instance type, allow users to choose cluster size
# MAGIC
# MAGIC #### 3. Restrictive Policies
# MAGIC * Lock down most configurations
# MAGIC * Users can only start/stop
# MAGIC * Example: Production clusters with fixed configurations
# MAGIC
# MAGIC ### Policy Components
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "cluster_type": {
# MAGIC     "type": "fixed",
# MAGIC     "value": "all-purpose"
# MAGIC   },
# MAGIC   "node_type_id": {
# MAGIC     "type": "allowlist",
# MAGIC     "values": ["i3.xlarge", "i3.2xlarge"],
# MAGIC     "defaultValue": "i3.xlarge"
# MAGIC   },
# MAGIC   "autotermination_minutes": {
# MAGIC     "type": "fixed",
# MAGIC     "value": 60
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Common Policy Use Cases
# MAGIC
# MAGIC | Use Case | Configuration | Benefit |
# MAGIC |----------|---------------|----------|
# MAGIC | **Cost control** | Fix max workers, auto-termination | Prevent runaway costs |
# MAGIC | **Security** | Fix instance types, network settings | Compliance |
# MAGIC | **Standardization** | Fix Spark configs, DBR version | Consistent performance |
# MAGIC | **Resource governance** | Limit cluster sizes | Fair resource sharing |
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question Pattern**: "How do you prevent users from creating large expensive clusters?"
# MAGIC * Answer: Use a cluster policy with fixed or limited max workers
# MAGIC
# MAGIC **Remember**: Policies can fix, limit (allowlist), or forbid (denylist) settings
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 5: Databricks Runtime (DBR)
# MAGIC %md
# MAGIC ## Concept 5: Databricks Runtime (DBR)
# MAGIC
# MAGIC ### What is DBR?
# MAGIC
# MAGIC Databricks Runtime is the **execution environment** running on clusters. It includes:
# MAGIC * Apache Spark
# MAGIC * Delta Lake
# MAGIC * Pre-installed libraries
# MAGIC * Databricks optimizations
# MAGIC
# MAGIC ### Runtime Types
# MAGIC
# MAGIC #### 1. Databricks Runtime (Standard)
# MAGIC **Includes**: Spark, Delta Lake, common libraries
# MAGIC
# MAGIC **Use for**: Data engineering, ETL
# MAGIC
# MAGIC **Example versions**: 11.3 LTS, 12.2, 13.3 LTS
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Databricks Runtime ML
# MAGIC **Includes**: Everything in standard + ML libraries
# MAGIC
# MAGIC **Added libraries**:
# MAGIC * TensorFlow, PyTorch, Scikit-learn
# MAGIC * MLflow
# MAGIC * XGBoost, LightGBM
# MAGIC * Distributed training frameworks
# MAGIC
# MAGIC **Use for**: Machine learning, model training
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3. Photon Runtime
# MAGIC **What it is**: Native vectorized query engine
# MAGIC
# MAGIC **Benefits**:
# MAGIC * 2-3x faster SQL queries
# MAGIC * Better with Delta Lake
# MAGIC * Lower costs for SQL workloads
# MAGIC
# MAGIC **Use for**: SQL-heavy workloads, BI dashboards
# MAGIC
# MAGIC **Exam note**: Photon is enabled by default on newer clusters
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### LTS (Long Term Support) Versions
# MAGIC
# MAGIC **What it means**: Supported for 2 years (vs 6 months for non-LTS)
# MAGIC
# MAGIC **Example**: 11.3 LTS, 13.3 LTS
# MAGIC
# MAGIC **When to use**: Production workloads needing stability
# MAGIC
# MAGIC **Exam tip**: LTS is recommended for production clusters
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Version Selection
# MAGIC
# MAGIC | Workload Type | Recommended Runtime | Why |
# MAGIC |---------------|---------------------|-----|
# MAGIC | SQL analytics | Latest with Photon | Best SQL performance |
# MAGIC | Data engineering | Latest LTS | Stability |
# MAGIC | ML training | ML Runtime (latest LTS) | Pre-installed ML libraries |
# MAGIC | Legacy code | Specific older version | Compatibility |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "Which runtime includes TensorFlow and MLflow pre-installed?"
# MAGIC * Answer: Databricks Runtime ML (not standard runtime)
# MAGIC
# MAGIC **Question**: "What provides 2-3x faster SQL performance?"
# MAGIC * Answer: Photon runtime
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Best Practices
# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC ### 1. Compute Selection
# MAGIC **Do**: Use jobs clusters for production ETL
# MAGIC * More cost-effective
# MAGIC * Isolated execution
# MAGIC * Auto-termination
# MAGIC
# MAGIC **Avoid**: Using all-purpose clusters for scheduled jobs
# MAGIC * More expensive
# MAGIC * Resource contention
# MAGIC * Requires manual management
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. Configuration Management
# MAGIC **Do**: Set `spark.sql.shuffle.partitions` based on data size
# MAGIC * Small data (< 10 GB): 50-100
# MAGIC * Medium data (10-100 GB): 100-200
# MAGIC * Large data (> 100 GB): 200-1000
# MAGIC
# MAGIC **Avoid**: Always using default 200
# MAGIC * Creates too many small tasks for small data
# MAGIC * Not enough parallelism for large data
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. Cluster Sizing
# MAGIC **Do**: Start small, scale as needed
# MAGIC * Monitor resource utilization
# MAGIC * Use auto-scaling for variable workloads
# MAGIC * Right-size based on actual usage
# MAGIC
# MAGIC **Avoid**: Over-provisioning "just in case"
# MAGIC * Wastes money
# MAGIC * May actually slow down small jobs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. Auto-termination
# MAGIC **Do**: Set auto-termination on all-purpose clusters
# MAGIC * Prevents idle cost
# MAGIC * Typically 60-120 minutes
# MAGIC
# MAGIC **Avoid**: Leaving clusters running indefinitely
# MAGIC * Wastes money
# MAGIC * Ties up resources
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. Cluster Policies
# MAGIC **Do**: Use policies to enforce standards
# MAGIC * Fix auto-termination
# MAGIC * Limit instance types
# MAGIC * Set Spark configs
# MAGIC
# MAGIC **Avoid**: Giving unrestricted access to all users
# MAGIC * Cost risk
# MAGIC * Configuration drift
# MAGIC * Security issues
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Common Pitfalls
# MAGIC %md
# MAGIC ## Common Pitfalls
# MAGIC
# MAGIC ### Pitfall 1: Not Adjusting shuffle.partitions
# MAGIC **Mistake**: Using default 200 partitions for a 1 GB dataset
# MAGIC
# MAGIC **Solution**: Set to 20-50 for small datasets
# MAGIC
# MAGIC **Why it matters**: 200 tiny tasks have more overhead than 20 reasonable-sized tasks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pitfall 2: Confusing Compute Types
# MAGIC **Mistake**: Using SQL Warehouse for Spark ML code
# MAGIC
# MAGIC **Solution**: Use ML Runtime cluster for ML workloads
# MAGIC
# MAGIC **Why it matters**: SQL Warehouses don't support full Spark/ML APIs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pitfall 3: Over-provisioning Memory
# MAGIC **Mistake**: Setting executor.memory to 64g "to be safe"
# MAGIC
# MAGIC **Solution**: Use 8-16g and scale out with more executors instead
# MAGIC
# MAGIC **Why it matters**: More executors = more parallelism, often faster
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pitfall 4: Ignoring AQE
# MAGIC **Mistake**: Disabling Adaptive Query Execution
# MAGIC
# MAGIC **Solution**: Keep it enabled (default)
# MAGIC
# MAGIC **Why it matters**: AQE provides free runtime optimizations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pitfall 5: Wrong Cluster for Jobs
# MAGIC **Mistake**: Attaching scheduled job to all-purpose cluster
# MAGIC
# MAGIC **Solution**: Use jobs cluster (ephemeral)
# MAGIC
# MAGIC **Why it matters**: Jobs clusters are cheaper and more reliable
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Exam Tips & Key Takeaways
# MAGIC %md
# MAGIC ## Exam Tips & Key Takeaways
# MAGIC
# MAGIC ### Most Testable Concepts
# MAGIC
# MAGIC 1. **spark.sql.shuffle.partitions** (Very High Frequency)
# MAGIC    - Default: 200
# MAGIC    - Adjust based on data size
# MAGIC    - Appears in 15-20% of questions
# MAGIC
# MAGIC 2. **Compute selection** (Very High Frequency)
# MAGIC    - Jobs clusters for production
# MAGIC    - SQL Warehouse for SQL/BI
# MAGIC    - All-purpose for development
# MAGIC
# MAGIC 3. **spark.sql.adaptive.enabled** (High Frequency)
# MAGIC    - Default: true
# MAGIC    - Provides runtime optimizations
# MAGIC
# MAGIC 4. **Cluster policies** (High Frequency)
# MAGIC    - Control costs and enforce standards
# MAGIC
# MAGIC 5. **Runtime selection** (Medium Frequency)
# MAGIC    - ML Runtime for ML workloads
# MAGIC    - Photon for SQL performance
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Configuration Quick Reference (Memorize!)
# MAGIC
# MAGIC | Config | Default | Common Values | Exam Tip |
# MAGIC |--------|---------|---------------|----------|
# MAGIC | `shuffle.partitions` | 200 | 50 (small), 200 (medium), 500+ (large) | **Most tested config!** |
# MAGIC | `autoBroadcastJoinThreshold` | 10 MB | 10 MB - 100 MB | Know when to increase |
# MAGIC | `adaptive.enabled` | true | true | Understand benefits |
# MAGIC | `executor.memory` | Varies | 4g - 32g | Balance with cores |
# MAGIC | `executor.cores` | Varies | 2 - 8 | Affects parallelism |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Comparison Tables to Know
# MAGIC
# MAGIC **Compute Types**:
# MAGIC
# MAGIC | Feature | All-Purpose | Jobs | SQL Warehouse |
# MAGIC |---------|-------------|------|---------------|
# MAGIC | Cost | High | Low | Medium |
# MAGIC | Use | Development | Production | SQL/BI |
# MAGIC | Exam questions | Many | Many | Some |
# MAGIC
# MAGIC **Cluster Access Modes**:
# MAGIC
# MAGIC | Mode | Languages | Isolation | Use Case |
# MAGIC |------|-----------|-----------|----------|
# MAGIC | No Isolation | All | Low | Data engineering |
# MAGIC | Shared | SQL, Python | High | Analytics |
# MAGIC | Single User | All | High | Individual dev |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exam Question Types
# MAGIC
# MAGIC #### Type 1: Configuration Questions
# MAGIC "Which configuration controls the number of partitions after a shuffle?"
# MAGIC * Answer: `spark.sql.shuffle.partitions`
# MAGIC
# MAGIC #### Type 2: Optimization Questions
# MAGIC "How can you optimize a job that's creating many small tasks?"
# MAGIC * Answer: Reduce `shuffle.partitions` for small datasets
# MAGIC
# MAGIC #### Type 3: Compute Selection
# MAGIC "Which compute should you use for production ETL?"
# MAGIC * Answer: Jobs cluster (not all-purpose)
# MAGIC
# MAGIC #### Type 4: Cost Questions
# MAGIC "What's the most cost-effective option for scheduled workflows?"
# MAGIC * Answer: Jobs cluster with auto-termination
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### What to Memorize
# MAGIC
# MAGIC **Must know by heart**:
# MAGIC * `spark.sql.shuffle.partitions` default = 200
# MAGIC * `spark.sql.autoBroadcastJoinThreshold` default = 10 MB
# MAGIC * Jobs clusters are cheaper than all-purpose
# MAGIC * SQL Warehouses are for SQL workloads
# MAGIC * AQE is enabled by default
# MAGIC * Photon provides 2-3x SQL performance
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Additional Resources
# MAGIC %md
# MAGIC ## Additional Resources
# MAGIC
# MAGIC ### Official Databricks Documentation
# MAGIC
# MAGIC * [Databricks Lakehouse Platform](https://docs.databricks.com/lakehouse/index.html)
# MAGIC * [Compute Configuration](https://docs.databricks.com/clusters/configure.html)
# MAGIC * [Spark Configuration](https://docs.databricks.com/clusters/configure.html#spark-configuration)
# MAGIC * [Cluster Policies](https://docs.databricks.com/administration-guide/clusters/policies.html)
# MAGIC * [Databricks Runtime](https://docs.databricks.com/release-notes/runtime/index.html)
# MAGIC
# MAGIC ### Apache Spark Documentation
# MAGIC
# MAGIC * [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
# MAGIC * [Spark Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC * Review this overview until concepts are clear
# MAGIC * Move to **practice_tasks.py** to test your understanding
# MAGIC * Focus extra time on Spark configurations (highly tested)
# MAGIC * Create flashcards for configuration defaults
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next**: Open `practice_tasks.py` and start with Exercise 1.

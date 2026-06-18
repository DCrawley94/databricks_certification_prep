# Databricks notebook source
# DBTITLE 1,Databricks Data Engineer Associate Certification - Master Study Plan
# MAGIC %md
# MAGIC # Databricks Data Engineer Associate - Study Plan
# MAGIC
# MAGIC ## Overview
# MAGIC This comprehensive study plan prepares you for the **Databricks Certified Data Engineer Associate** exam. The exam validates your ability to use Databricks and Apache Spark to perform core data engineering tasks.
# MAGIC
# MAGIC ### Exam Details
# MAGIC - **Duration**: 90 minutes
# MAGIC - **Questions**: 45 multiple choice questions
# MAGIC - **Passing Score**: 70%
# MAGIC - **Cost**: $200 USD
# MAGIC - **Format**: Online proctored
# MAGIC - **Validity**: 2 years
# MAGIC
# MAGIC ### Your Background Profile
# MAGIC - ~1 year Databricks experience
# MAGIC - Strong Python & SQL skills
# MAGIC - Less familiar with Spark DataFrame API (focus area)
# MAGIC - Primarily batch ingestion experience (streaming needs attention)
# MAGIC - Emphasis needed: Spark configurations, Delta operations syntax
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Sharing and Cleanup Instructions
# MAGIC %md
# MAGIC ## Sharing This Material
# MAGIC
# MAGIC ### Purpose
# MAGIC This study material can be shared with colleagues preparing for the same certification. Follow these steps to remove personal progress and answers.
# MAGIC
# MAGIC ### Cleanup Process
# MAGIC
# MAGIC #### Study Plan Notebook (this file)
# MAGIC 1. **Progress Tracker**: Replace all `[x]` with `[ ]` in Progress Tracker sections
# MAGIC 2. **Progress Metrics**: Reset all counters to `0 / X` format
# MAGIC 3. **Session Log**: Delete all individual session cells (keep "Session Log" header)
# MAGIC 4. **Weak Areas Log**: Replace numbered items with blank lines
# MAGIC 5. **Questions to Research**: Clear all entries
# MAGIC 6. **Weekly Checklist**: Reset all `[x]` to `[ ]`
# MAGIC
# MAGIC #### Practice Notebooks (each topic folder)
# MAGIC 1. **Your Solution Cells**: Delete all cells titled "Exercise X: Your Solution"
# MAGIC 2. **Answer Variables**: Reset to `None` or empty values (e.g., `answer_1 = None`)
# MAGIC 3. **Personal Comments**: Remove any custom comments or notes
# MAGIC 4. **Validation Cells**: Keep intact - these verify correctness
# MAGIC 5. **MCQ Answers**: Clear answer dictionary values
# MAGIC
# MAGIC #### Overview Notebooks
# MAGIC No cleanup needed - these contain reference material only.
# MAGIC
# MAGIC #### What to Keep
# MAGIC - All reference material and explanations
# MAGIC - Exercise descriptions and prompts
# MAGIC - Validation functions
# MAGIC - Solution notebooks (colleagues can review after attempting)
# MAGIC - Overall structure and progress tracking framework
# MAGIC
# MAGIC ### Quick Reset Checklist
# MAGIC ```
# MAGIC [ ] Study plan: Reset all checkboxes and counters
# MAGIC [ ] Study plan: Delete session log entries
# MAGIC [ ] Study plan: Clear weak areas and questions logs
# MAGIC [ ] Practice notebooks: Delete solution cells (1-9 topics)
# MAGIC [ ] Practice notebooks: Reset answer variables
# MAGIC [ ] Practice notebooks: Remove personal comments
# MAGIC ```
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Exam Domains Breakdown
# MAGIC %md
# MAGIC ## Exam Domains and Weighting
# MAGIC
# MAGIC | Domain | Weight | Est. Questions | Priority |
# MAGIC |--------|--------|----------------|----------|
# MAGIC | 1. Databricks Lakehouse Platform | 22% | 10 | Medium |
# MAGIC | 2. ELT with Spark SQL and Python | 29% | 13 | High |
# MAGIC | 3. Incremental Data Processing | 22% | 10 | High |
# MAGIC | 4. Production Pipelines | 16% | 7 | Medium |
# MAGIC | 5. Data Governance | 11% | 5 | Low |
# MAGIC
# MAGIC **Note**: Delta Lake operations and Performance Optimization are embedded across domains 2-4.
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Complete Study Roadmap
# MAGIC %md
# MAGIC ## Complete Study Roadmap
# MAGIC
# MAGIC ### Week 1-2: Foundation and Platform (12-15 hours)
# MAGIC
# MAGIC #### Topic 1: Databricks Lakehouse Platform (6-8 hours)
# MAGIC **Location**: `01_Databricks_Lakehouse_Platform/`
# MAGIC
# MAGIC **What You'll Learn**:
# MAGIC - Databricks workspace architecture and navigation
# MAGIC - Compute resources: clusters, SQL warehouses, serverless compute
# MAGIC - Critical Spark configurations (executor memory, cores, shuffle partitions)
# MAGIC - Cluster policies and runtime selection
# MAGIC - Databricks file system (DBFS) and workspace storage
# MAGIC
# MAGIC **Study Approach**:
# MAGIC 1. Read `overview.py` - focus on Spark config tables
# MAGIC 2. Complete `practice_tasks.py` - 15 exercises + 5 MCQs
# MAGIC 3. Review `solutions.py` - understand alternative approaches
# MAGIC
# MAGIC **Key Exam Topics**:
# MAGIC - `spark.executor.memory`, `spark.executor.cores`
# MAGIC - `spark.sql.shuffle.partitions`
# MAGIC - Cluster modes (Standard, High Concurrency, Single Node)
# MAGIC - Runtime versions and ML runtime
# MAGIC
# MAGIC **Progress Tracker**:
# MAGIC - [x] Overview completed (partial - Spark configs, compute types, shuffles, broadcast joins)
# MAGIC - [x] Practice tasks completed (13/15 exercises - skipped Ex 9, 10)
# MAGIC - [x] MCQs completed (5/5 correct)
# MAGIC - [ ] Challenge scenarios completed (0/2)
# MAGIC - [ ] ETL applieds completed (0/2)
# MAGIC - [ ] Ready for domain questions
# MAGIC
# MAGIC **Session Date**: June 10, 2026
# MAGIC **Time Spent**: ~2.5 hours
# MAGIC
# MAGIC **Strengths Demonstrated**:
# MAGIC - Config retrieval and modification syntax
# MAGIC - Compute selection for scheduled workloads
# MAGIC - Broadcast join mechanics understanding
# MAGIC - Shuffle partition calculations
# MAGIC
# MAGIC **Knowledge Gaps Identified**:
# MAGIC - Exercise 13: Incomplete symptoms of memory pressure (only mentioned OOM, missing spill-to-disk and task failures)
# MAGIC - Exercise 15: Did not know how to disable broadcast joins (`spark.sql.autoBroadcastJoinThreshold = -1`)
# MAGIC - MCQ 3: Need to reinforce that AQE doesn't change threshold value, only makes selection dynamic
# MAGIC
# MAGIC **Next Session Action Items**:
# MAGIC 1. Complete Challenge 1 and 2 (production ETL design, config optimization)
# MAGIC 2. Complete ETL Applieds 1 and 2
# MAGIC 3. Review Exercise 13 and 15 gaps - create flashcards
# MAGIC 4. Practice writing config names from memory
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Topic 2: ELT with Spark SQL and Python (6-7 hours)
# MAGIC **Location**: `02_ELT_Spark_SQL_Python/`
# MAGIC
# MAGIC **What You'll Learn**:
# MAGIC - SQL transformations: CTEs, window functions, complex aggregations
# MAGIC - PySpark DataFrame API (your focus area!)
# MAGIC - Reading/writing data formats (Parquet, JSON, CSV, Delta)
# MAGIC - UDFs and built-in functions
# MAGIC - Schema enforcement and evolution
# MAGIC
# MAGIC **Study Approach**:
# MAGIC 1. Read `overview.py` - compare SQL vs DataFrame API syntax
# MAGIC 2. Practice `practice_tasks.py` - emphasize DataFrame API exercises
# MAGIC 3. Review `solutions.py` - note SQL ↔ PySpark equivalents
# MAGIC
# MAGIC **Key Exam Topics**:
# MAGIC - DataFrame transformations: `select()`, `filter()`, `groupBy()`, `agg()`
# MAGIC - Window functions: `RANK()`, `ROW_NUMBER()`, `LAG()`, `LEAD()`
# MAGIC - Reading options: `header`, `inferSchema`, `delimiter`
# MAGIC - Schema definition with StructType
# MAGIC
# MAGIC **Progress Tracker**:
# MAGIC - [x] Overview completed (via exercises)
# MAGIC - [x] Practice tasks completed (15/15 exercises)
# MAGIC - [x] MCQs completed (5/5 correct)
# MAGIC - [x] DataFrame API fluency achieved
# MAGIC - [ ] ETL tasks completed (0/2)
# MAGIC
# MAGIC **Session Date**: June 11, 2026
# MAGIC **Time Spent**: ~1.5 hours
# MAGIC
# MAGIC **Strengths Demonstrated**:
# MAGIC - DataFrame transformations and method chaining
# MAGIC - Window functions and partitioning
# MAGIC - Schema definition with StructType
# MAGIC - Understanding of aggregation vs window operations
# MAGIC
# MAGIC **Knowledge Gaps Identified**:
# MAGIC - Exercise 12: Missed udf import requirement initially
# MAGIC - Exercise 13-14: Serverless compute file writing limitations
# MAGIC - Exercise 15: Unnecessary .select() before .groupBy()
# MAGIC - Join strategies (broadcast, sort-merge, shuffle hash) are Professional-level, not Associate scope
# MAGIC
# MAGIC **Next Session Action Items**:
# MAGIC 1. Complete ETL Tasks 1 and 2 for Topic 2
# MAGIC 2. Create DataFrame API transformation flashcards
# MAGIC 3. Begin Topic 3: Delta Lake Operations
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Week 3: Delta Lake Deep Dive (8-10 hours) [PRIORITY]
# MAGIC
# MAGIC #### Topic 3: Delta Lake Operations (8-10 hours)
# MAGIC **Location**: `03_Delta_Lake_Operations/`
# MAGIC
# MAGIC **What You'll Learn**:
# MAGIC - CREATE TABLE syntax (managed vs external, partitioned)
# MAGIC - MERGE INTO for upserts and SCD Type 2
# MAGIC - VACUUM: retention periods, safety checks
# MAGIC - OPTIMIZE: bin-packing, Z-ORDER indexing
# MAGIC - Time travel and versioning
# MAGIC - Table properties and Delta configurations
# MAGIC - DESCRIBE HISTORY, DESCRIBE DETAIL
# MAGIC - Liquid clustering
# MAGIC
# MAGIC **Study Approach**:
# MAGIC 1. Read `overview.py` - memorize syntax patterns
# MAGIC 2. Practice `practice_tasks.py` - focus on MERGE and OPTIMIZE
# MAGIC 3. Review `solutions.py` - understand retention and safety
# MAGIC 4. **Extra practice**: Repeat MERGE scenarios until automatic
# MAGIC
# MAGIC **Critical Syntax to Memorize**:
# MAGIC ```sql
# MAGIC -- MERGE INTO pattern
# MAGIC MERGE INTO target t
# MAGIC USING source s
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC
# MAGIC -- OPTIMIZE with Z-ORDER
# MAGIC OPTIMIZE table_name ZORDER BY (col1, col2)
# MAGIC
# MAGIC -- VACUUM with retention
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM table_name RETAIN 0 HOURS;
# MAGIC ```
# MAGIC
# MAGIC **Progress Tracker**:
# MAGIC - [ ] Overview completed
# MAGIC - [ ] Practice tasks completed (15/15 exercises)
# MAGIC - [ ] MCQs completed (5/5 correct)
# MAGIC - [ ] MERGE syntax mastered
# MAGIC - [ ] OPTIMIZE + Z-ORDER practiced
# MAGIC - [ ] VACUUM safety understood
# MAGIC - [ ] Time travel queries practiced
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Week 4: Streaming and Incremental Processing (8-10 hours)
# MAGIC
# MAGIC #### Topic 4: Incremental Data Processing (8-10 hours)
# MAGIC **Location**: `04_Incremental_Data_Processing/`
# MAGIC
# MAGIC **What You'll Learn**:
# MAGIC - Auto Loader (cloudFiles): schema inference, evolution
# MAGIC - Structured Streaming fundamentals
# MAGIC - Trigger types: `availableNow`, `processingTime`, `once`, `continuous`
# MAGIC - Watermarking and late data handling
# MAGIC - Checkpointing and fault tolerance
# MAGIC - Change Data Capture (CDC) patterns
# MAGIC
# MAGIC **Study Approach**:
# MAGIC 1. Read `overview.py` - understand streaming concepts (new for you!)
# MAGIC 2. Practice `practice_tasks.py` - focus on trigger differences
# MAGIC 3. Review `solutions.py` - understand checkpoint recovery
# MAGIC 4. **Extra**: Compare batch vs streaming approaches
# MAGIC
# MAGIC **Key Exam Topics**:
# MAGIC - Auto Loader: `.format("cloudFiles").option("cloudFiles.format", "json")`
# MAGIC - Trigger types and use cases
# MAGIC - Checkpoint location management
# MAGIC - `spark.readStream` vs `spark.read`
# MAGIC - Output modes: `append`, `complete`, `update`
# MAGIC
# MAGIC **Progress Tracker**:
# MAGIC - [ ] Overview completed
# MAGIC - [ ] Practice tasks completed (15/15 exercises)
# MAGIC - [ ] MCQs completed (5/5 correct)
# MAGIC - [ ] Streaming concepts understood
# MAGIC - [ ] Trigger types differentiated
# MAGIC - [ ] CDC patterns practiced
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Week 5: Pipelines and Governance (7-9 hours)
# MAGIC
# MAGIC #### Topic 5: Production Pipelines (4-5 hours)
# MAGIC **Location**: `05_Production_Pipelines/`
# MAGIC
# MAGIC **What You'll Learn**:
# MAGIC - Lakeflow Spark Declarative Pipelines syntax (formerly DLT)
# MAGIC - Streaming live tables vs materialized views
# MAGIC - Pipeline configuration and development modes
# MAGIC - Expectations for data quality
# MAGIC - Error handling and monitoring
# MAGIC - Jobs and workflow orchestration
# MAGIC
# MAGIC **Study Approach**:
# MAGIC 1. Read `overview.py` - understand pipeline declarations
# MAGIC 2. Practice `practice_tasks.py` - write pipeline code
# MAGIC 3. Review `solutions.py` - compare approaches
# MAGIC
# MAGIC **Key Exam Topics**:
# MAGIC - `@dlt.table` vs `@dlt.view`
# MAGIC - `dlt.read()` and `dlt.read_stream()`
# MAGIC - Expectations: `@dlt.expect()`, `@dlt.expect_or_drop()`, `@dlt.expect_or_fail()`
# MAGIC - Pipeline modes: `triggered`, `continuous`
# MAGIC
# MAGIC **Progress Tracker**:
# MAGIC - [ ] Overview completed
# MAGIC - [ ] Practice tasks completed (15/15 exercises)
# MAGIC - [ ] MCQs completed (5/5 correct)
# MAGIC - [ ] Pipeline syntax mastered
# MAGIC - [ ] Expectations understood
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Topic 6: Unity Catalog and Data Governance (3-4 hours)
# MAGIC **Location**: `06_Unity_Catalog_Governance/`
# MAGIC
# MAGIC **What You'll Learn**:
# MAGIC - Three-level namespace: `catalog.schema.table`
# MAGIC - Permissions: `USE CATALOG`, `USE SCHEMA`, `SELECT`, `MODIFY`, `CREATE`
# MAGIC - Managed vs external tables
# MAGIC - Volumes for file storage
# MAGIC - Data lineage and audit logs
# MAGIC - Secure views and row/column-level security
# MAGIC
# MAGIC **Study Approach**:
# MAGIC 1. Read `overview.py` - understand permission hierarchy
# MAGIC 2. Practice `practice_tasks.py` - focus on GRANT syntax
# MAGIC 3. Review `solutions.py`
# MAGIC
# MAGIC **Key Exam Topics**:
# MAGIC - Permission requirements for common operations
# MAGIC - Difference between managed and external tables
# MAGIC - Volume paths: `/Volumes/catalog/schema/volume/`
# MAGIC - Lineage tracking
# MAGIC
# MAGIC **Progress Tracker**:
# MAGIC - [ ] Overview completed
# MAGIC - [ ] Practice tasks completed (15/15 exercises)
# MAGIC - [ ] MCQs completed (5/5 correct)
# MAGIC - [ ] Permission model understood
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Week 6: Performance and Optimization (6-8 hours) [PRIORITY]
# MAGIC
# MAGIC #### Topic 7: Performance Optimization (6-8 hours)
# MAGIC **Location**: `07_Performance_Optimization/`
# MAGIC
# MAGIC **What You'll Learn**:
# MAGIC - Partitioning strategies and best practices
# MAGIC - File sizing: small file problem, optimal sizes
# MAGIC - Broadcast joins vs shuffle joins
# MAGIC - Caching strategies: `CACHE TABLE`, `df.cache()`
# MAGIC - Adaptive Query Execution (AQE)
# MAGIC - Critical Spark configurations
# MAGIC
# MAGIC **Study Approach**:
# MAGIC 1. Read `overview.py` - memorize config table
# MAGIC 2. Practice `practice_tasks.py` - identify performance issues
# MAGIC 3. Review `solutions.py` - understand optimization reasoning
# MAGIC 4. **Extra**: Create flashcards for configs
# MAGIC
# MAGIC **Critical Configurations to Memorize**:
# MAGIC | Configuration | Purpose | Default | Exam Tip |
# MAGIC |---------------|---------|---------|----------|
# MAGIC | `spark.sql.adaptive.enabled` | Enable AQE | true | Know when to disable |
# MAGIC | `spark.sql.adaptive.coalescePartitions.enabled` | Reduce partition count | true | Helps with small files |
# MAGIC | `spark.databricks.delta.optimize.maxFileSize` | Target file size | 1GB | Balance with query patterns |
# MAGIC | `spark.sql.shuffle.partitions` | Shuffle partition count | 200 | Tune based on data size |
# MAGIC | `spark.databricks.delta.retentionDurationCheck.enabled` | VACUUM safety | true | Disable carefully |
# MAGIC | `spark.sql.files.maxPartitionBytes` | Partition size in bytes | 128MB | Affects scan parallelism |
# MAGIC | `spark.sql.autoBroadcastJoinThreshold` | Broadcast join threshold | 10MB | Increase for better joins |
# MAGIC
# MAGIC **Progress Tracker**:
# MAGIC - [ ] Overview completed
# MAGIC - [ ] Practice tasks completed (15/15 exercises)
# MAGIC - [ ] MCQs completed (5/5 correct)
# MAGIC - [ ] Config table memorized
# MAGIC - [ ] Performance patterns recognized
# MAGIC - [ ] Join optimization understood
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Exam Strategy & Tips
# MAGIC %md
# MAGIC ## Exam Strategy and Tips
# MAGIC
# MAGIC ### Time Management
# MAGIC - **90 minutes / 45 questions = 2 minutes per question**
# MAGIC - First pass: Answer questions you know immediately (aim for 60 min)
# MAGIC - Second pass: Return to flagged questions (30 min)
# MAGIC - Save scenario-based questions for second pass
# MAGIC
# MAGIC ### Question Types
# MAGIC
# MAGIC #### 1. Syntax Questions (35-40%)
# MAGIC **Example**: "Which command optimizes a Delta table and creates a Z-ORDER index on customer_id?"
# MAGIC - **Strategy**: Memorize exact syntax for MERGE, OPTIMIZE, VACUUM
# MAGIC - **Your Focus**: Delta operations, Spark configs
# MAGIC
# MAGIC #### 2. Configuration Questions (20-25%)
# MAGIC **Example**: "Which configuration controls the maximum size of files created by OPTIMIZE?"
# MAGIC - **Strategy**: Memorize the config table in Topic 7
# MAGIC - **Your Focus**: All configs with `delta`, `adaptive`, `shuffle`
# MAGIC
# MAGIC #### 3. Conceptual Questions (20-25%)
# MAGIC **Example**: "What happens to data when you run VACUUM with a retention period shorter than the default?"
# MAGIC - **Strategy**: Understand WHY, not just HOW
# MAGIC - **Your Focus**: Delta Lake internals, streaming concepts
# MAGIC
# MAGIC #### 4. Scenario Questions (15-20%)
# MAGIC **Example**: "A streaming query is failing with late data errors. What should you configure?"
# MAGIC - **Strategy**: Practice end-to-end scenarios in practice tasks
# MAGIC - **Your Focus**: Streaming (new for you), performance troubleshooting
# MAGIC
# MAGIC ### Common Traps
# MAGIC
# MAGIC #### Trap 1: Syntax Variations
# MAGIC - Incorrect: `OPTIMIZE table_name BY (col)` 
# MAGIC - Correct: `OPTIMIZE table_name ZORDER BY (col)`
# MAGIC - **Note**: Pay attention to exact keywords
# MAGIC
# MAGIC #### Trap 2: Default Values
# MAGIC - Question: "What is the default retention period for VACUUM?"
# MAGIC - Incorrect: 7 hours
# MAGIC - Correct: 7 days (168 hours)
# MAGIC - **Note**: Memorize defaults for VACUUM, OPTIMIZE, checkpoints
# MAGIC
# MAGIC #### Trap 3: Spark vs Databricks Configs
# MAGIC - `spark.sql.shuffle.partitions` (Spark config)
# MAGIC - `spark.databricks.delta.retentionDurationCheck.enabled` (Databricks config)
# MAGIC - **Note**: Databricks configs have `databricks` in the name
# MAGIC
# MAGIC #### Trap 4: Streaming Terminology
# MAGIC - Output mode vs Trigger mode (different concepts)
# MAGIC - Checkpoint vs State store (different purposes)
# MAGIC - **Note**: Review streaming glossary in Topic 4
# MAGIC
# MAGIC ### Your Specific Focus Areas
# MAGIC
# MAGIC #### High Priority (Study Extra)
# MAGIC 1. **PySpark DataFrame API** (less familiar)
# MAGIC    - Practice translating SQL to DataFrame syntax
# MAGIC    - Memorize common transformations
# MAGIC 2. **Streaming concepts** (limited experience)
# MAGIC    - Understand trigger types thoroughly
# MAGIC    - Practice Auto Loader scenarios
# MAGIC 3. **Spark configurations** (exam heavy)
# MAGIC    - Memorize the config table
# MAGIC    - Understand when to tune each config
# MAGIC
# MAGIC #### Medium Priority (Review)
# MAGIC 1. Delta Lake operations (likely comfortable but exam-critical)
# MAGIC 2. Pipeline syntax (may be new syntax style)
# MAGIC 3. Performance optimization (build on existing knowledge)
# MAGIC
# MAGIC #### Low Priority (Quick Review)
# MAGIC 1. SQL transformations (already strong)
# MAGIC 2. Unity Catalog permissions (straightforward)
# MAGIC
# MAGIC ### The Night Before
# MAGIC - [ ] Review config table (Topic 7)
# MAGIC - [ ] Review MERGE syntax variations (Topic 3)
# MAGIC - [ ] Review streaming trigger types (Topic 4)
# MAGIC - [ ] Review pipeline expectations (Topic 5)
# MAGIC - [ ] Get 8 hours of sleep!
# MAGIC
# MAGIC ### During the Exam
# MAGIC 1. **Read carefully**: "Which is NOT correct" vs "Which is correct"
# MAGIC 2. **Flag and move on**: Don't get stuck on one question
# MAGIC 3. **Eliminate wrong answers**: Usually 2 are obviously wrong
# MAGIC 4. **Trust your practice**: You've seen similar questions
# MAGIC 5. **Check your flags**: Use remaining time to revisit
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Quick Reference Links
# MAGIC %md
# MAGIC ## Quick Reference Links
# MAGIC
# MAGIC ### Official Databricks Documentation
# MAGIC - [Data Engineer Associate Exam Guide](https://www.databricks.com/learn/certification/data-engineer-associate)
# MAGIC - [Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
# MAGIC - [Structured Streaming Guide](https://docs.databricks.com/structured-streaming/index.html)
# MAGIC - [Spark SQL Reference](https://docs.databricks.com/sql/language-manual/index.html)
# MAGIC - [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
# MAGIC - [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/workflows/delta-live-tables/index.html)
# MAGIC
# MAGIC ### Databricks Academy
# MAGIC - [Free Training: Data Engineering with Databricks](https://www.databricks.com/learn/training/home)
# MAGIC - [Exam Practice Questions](https://www.databricks.com/learn/certification/practice-exams)
# MAGIC
# MAGIC ### Your Study Materials
# MAGIC - Topic 1: `/databricks_certification_prep/Data Engineer Associate/01_Databricks_Lakehouse_Platform/`
# MAGIC - Topic 2: `/databricks_certification_prep/Data Engineer Associate/02_ELT_Spark_SQL_Python/`
# MAGIC - Topic 3: `/databricks_certification_prep/Data Engineer Associate/03_Delta_Lake_Operations/` [PRIORITY]
# MAGIC - Topic 4: `/databricks_certification_prep/Data Engineer Associate/04_Incremental_Data_Processing/`
# MAGIC - Topic 5: `/databricks_certification_prep/Data Engineer Associate/05_Production_Pipelines/`
# MAGIC - Topic 6: `/databricks_certification_prep/Data Engineer Associate/06_Unity_Catalog_Governance/`
# MAGIC - Topic 7: `/databricks_certification_prep/Data Engineer Associate/07_Performance_Optimization/` [PRIORITY]
# MAGIC - Topic 8: `/databricks_certification_prep/Data Engineer Associate/08_Lakeflow_Connect/`
# MAGIC - Topic 9: `/databricks_certification_prep/Data Engineer Associate/09_Lakeflow_SDP/`
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Progress Tracking Dashboard
# MAGIC %md
# MAGIC ## Progress Tracking Dashboard
# MAGIC
# MAGIC ### Overall Progress
# MAGIC **Target Study Time**: 45-55 hours over 6 weeks
# MAGIC **Actual Study Time**: 11 hours (Week 1-4)
# MAGIC **Topics Completed**: 1 / 9 (Topic 3 complete; Topics 1-2, 4 partial)
# MAGIC **Practice Exercises**: 60 / 135 (15 per topic)
# MAGIC **MCQs Correct**: 15 / 45 (5 per topic)
# MAGIC **Challenge Scenarios**: 0 / 18 (2 per topic)
# MAGIC **ETL Tasks**: 0 / 18 (2 per topic)
# MAGIC
# MAGIC **Last Updated**: June 17, 2026
# MAGIC
# MAGIC ### Weekly Checklist
# MAGIC
# MAGIC #### Week 1: Foundation
# MAGIC - [~] Topic 1: Databricks Lakehouse Platform (2.5 hrs / 6-8 hrs target) - 70% COMPLETE
# MAGIC - [~] Topic 2: ELT with Spark SQL and Python (1.5 hrs) - EXERCISES COMPLETE, ETL TASKS PENDING
# MAGIC - [~] **Milestone**: Complete 30 exercises (28/30 done), understand DataFrame API (achieved)
# MAGIC
# MAGIC #### Week 2: Consolidation
# MAGIC - [ ] Review Week 1 materials
# MAGIC - [ ] Practice weak areas from Week 1
# MAGIC - [ ] Begin Topic 3 overview
# MAGIC
# MAGIC #### Week 3: Delta Lake Deep Dive [PRIORITY]
# MAGIC - [x] Topic 3: Delta Lake Operations (2.5 hrs / 8-10 hrs target) - COMPLETE
# MAGIC - [x] **Milestone**: MERGE, OPTIMIZE, VACUUM syntax memorized
# MAGIC - [x] Extra practice: Conditional MERGE scenarios completed
# MAGIC
# MAGIC #### Week 4: Streaming
# MAGIC - [~] Topic 4: Incremental Data Processing (4.5 hrs / 8-10 hrs target) - EXERCISES 1-13 COMPLETE
# MAGIC - [x] **Milestone**: Streaming triggers, Auto Loader, serverless constraints understood
# MAGIC - [ ] Complete remaining exercises (14-15), ETL tasks, challenges
# MAGIC
# MAGIC #### Week 5: Pipelines & Governance
# MAGIC - [ ] Topic 5: Production Pipelines (4-5 hrs)
# MAGIC - [ ] Topic 6: Unity Catalog Governance (3-4 hrs)
# MAGIC - [ ] **Milestone**: Complete Topics 1-6
# MAGIC
# MAGIC #### Week 6: Performance and Review [PRIORITY]
# MAGIC - [ ] Topic 7: Performance Optimization (6-8 hrs)
# MAGIC - [ ] Topic 8: Lakeflow Connect (2-3 hrs)
# MAGIC - [ ] Topic 9: Lakeflow SDP (3-4 hrs)
# MAGIC - [ ] Create Spark config flashcards
# MAGIC - [ ] Full review of all topics
# MAGIC - [ ] Practice weak areas
# MAGIC - [ ] Take official practice exam
# MAGIC - [ ] **Milestone**: Ready for exam
# MAGIC
# MAGIC ### Weak Areas Log
# MAGIC **Track topics that need extra review:**
# MAGIC
# MAGIC 1. **Memory pressure symptoms** (Topic 1) - Only identified OOM errors, missed spill-to-disk and task failures
# MAGIC 2. **Disabling broadcast joins** (Topic 1) - Config: spark.sql.autoBroadcastJoinThreshold = -1
# MAGIC 3. **AQE relationship to broadcast threshold** (Topic 1) - AQE doesn't change threshold value, makes selection dynamic
# MAGIC 4. **Join strategies vs join types** (Topic 1/2) - Associate covers join types (inner, left, outer, anti) but NOT join strategies (broadcast, sort-merge, shuffle hash) which are Professional-level
# MAGIC 5. **GroupBy implicit column selection** (Topic 2) - .groupBy().agg() implicitly defines output columns; no .select() needed when translating SQL GROUP BY
# MAGIC 6. **MERGE duplicate handling** (Topic 3) - MERGE fails on duplicate keys in source; must deduplicate using ROW_NUMBER/QUALIFY before merge
# MAGIC 7. **Time travel boundaries** (Topic 3) - Cannot query timestamps before table creation; Delta throws error (does not fall back to version 0)
# MAGIC 8. **CLONE syntax** (Topic 3 bonus) - SHALLOW CLONE references files, DEEP CLONE copies files, CTAS transforms during copy
# MAGIC 9. **COPY INTO table creation** (Topic 4) - COPY INTO does NOT auto-create tables; requires explicit CREATE TABLE first with mergeSchema option for schema inference
# MAGIC 10. **Schema evolution defaults** (Topic 4) - When using schemaLocation, default is addNewColumns mode; must explicitly set "none" for enforcement
# MAGIC 11. **rescue vs none mode** (Topic 4) - "rescue" mode is hybrid (evolution + rescued data); "none" mode is true enforcement (rejects new columns)
# MAGIC 12. **Auto Loader format inference** (Topic 4) - Does NOT exist; cloudFiles.format is always required and expects single value; mixed formats need separate readers
# MAGIC
# MAGIC ### Questions to Research
# MAGIC **Track questions you encounter during practice:**
# MAGIC
# MAGIC 1. **MCQ 2**: Are filter() and where() truly aliases in PySpark? Need to verify in docs
# MAGIC 2. **MCQ 5**: If join strategies are Professional-level, why is this MCQ in Associate practice?
# MAGIC 3. **Serverless limitations**: Document all compute restrictions for exercises (no /tmp/, no DBFS root, Delta required for managed tables)
# MAGIC 4. **VACUUM retention edge cases**: What happens if you query a timestamp exactly at retention boundary? Does it include or exclude that moment?
# MAGIC 5. **Table property verification**: Best practices for verifying ALTER TABLE SET TBLPROPERTIES actually applied (DESCRIBE DETAIL vs SHOW TBLPROPERTIES)
# MAGIC 6. **COPY INTO mergeSchema behavior**: When using empty CREATE TABLE + mergeSchema, does it infer from first file batch only or continuously adapt?
# MAGIC 7. **Schema evolution mode "rescue"**: Is rescue mode documented in official exam guide or just in extended docs? Need to verify exam scope
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Final Checklist
# MAGIC %md
# MAGIC ## Final Pre-Exam Checklist
# MAGIC
# MAGIC ### Knowledge Verification
# MAGIC - [ ] Can write MERGE INTO syntax from memory
# MAGIC - [ ] Can write OPTIMIZE with Z-ORDER from memory
# MAGIC - [ ] Can explain VACUUM retention and safety
# MAGIC - [ ] Can list 7+ critical Spark configurations
# MAGIC - [ ] Can differentiate streaming trigger types
# MAGIC - [ ] Can write Auto Loader code from memory
# MAGIC - [ ] Can explain broadcast vs shuffle joins
# MAGIC - [ ] Understand Unity Catalog permission hierarchy
# MAGIC - [ ] Can write Lakeflow pipeline expectations
# MAGIC - [ ] Can translate SQL to PySpark DataFrame API
# MAGIC
# MAGIC ### Practice Completion
# MAGIC - [ ] All 135 exercises completed
# MAGIC - [ ] All 45 MCQs answered correctly
# MAGIC - [ ] All 18 challenge scenarios completed
# MAGIC - [ ] All 18 ETL tasks completed
# MAGIC - [ ] Weak areas identified and reviewed
# MAGIC - [ ] Official practice exam taken
# MAGIC - [ ] Score 75%+ on practice exam
# MAGIC
# MAGIC ### Exam Logistics
# MAGIC - [ ] Exam scheduled
# MAGIC - [ ] Testing environment prepared (quiet room, stable internet)
# MAGIC - [ ] ID ready (government-issued photo ID)
# MAGIC - [ ] Desk cleared (only water allowed)
# MAGIC - [ ] Webcam and microphone tested
# MAGIC - [ ] Databricks certification account confirmed
# MAGIC
# MAGIC ### Mental Preparation
# MAGIC - [ ] Reviewed exam strategy section
# MAGIC - [ ] Identified time management approach
# MAGIC - [ ] Prepared for common traps
# MAGIC - [ ] Confident in strong areas
# MAGIC - [ ] Aware of weak areas but not anxious
# MAGIC - [ ] Well-rested before exam day
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC This plan provides the structure and materials needed to pass the exam. Follow the roadmap, focus on priority areas (Delta Lake, Spark configs, streaming concepts), and practice consistently. The exam validates existing knowledge applied to Databricks context.
# MAGIC
# MAGIC **Note**: Passing score is 70%. Focus effort on strong areas while addressing weak areas through targeted practice.

# COMMAND ----------

# DBTITLE 1,Session Log
# MAGIC %md
# MAGIC ## Session Log
# MAGIC
# MAGIC Each study session is documented below in reverse chronological order (newest first).
# MAGIC
# MAGIC **Format**: Date - Topic - Duration - Key activities and outcomes
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,2026-06-17: Topic 4 Progress - COPY INTO, Schema Evolution, Mixed Files
# MAGIC %md
# MAGIC ### June 17, 2026 - Topic 4: Incremental Data Processing (Continued)
# MAGIC **Duration**: 2.5 hours  
# MAGIC **Notebook**: `04_Incremental_Data_Processing/practice_tasks.py`
# MAGIC
# MAGIC **Completed**:
# MAGIC - Exercises 11-13: COPY INTO, schema enforcement vs evolution, mixed file types
# MAGIC - Deep dive into COPY INTO table creation requirements (does NOT auto-create)
# MAGIC - Clarified schema evolution defaults (addNewColumns is default with schemaLocation)
# MAGIC - Discovered "rescue" mode as hybrid evolution + error handling approach
# MAGIC - Corrected exercise instructions for Exercise 13 (format inference doesn't exist)
# MAGIC - Updated solutions.py with multiple COPY INTO approaches (empty table + mergeSchema, explicit schema, CTAS)
# MAGIC
# MAGIC **Key Learnings**:
# MAGIC - COPY INTO requires pre-existing table; must use mergeSchema option or explicit schema
# MAGIC - Schema evolution mode "none" must be explicit when using schemaLocation (default is addNewColumns)
# MAGIC - Auto Loader has no format inference; mixed file types need separate readers with path wildcards
# MAGIC - "rescue" mode combines schema evolution with rescued data column (hybrid approach)
# MAGIC
# MAGIC **Next Session**:
# MAGIC - Complete exercises 14-15
# MAGIC - Tackle challenge scenarios (medallion architecture, schema evolution simulation)
# MAGIC - Review MCQs
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,2026-06-16: Topic 4 Started - Incremental Data Processing
# MAGIC %md
# MAGIC ### June 16, 2026 - Topic 4: Incremental Data Processing (Started)
# MAGIC **Duration**: 2 hours  
# MAGIC **Notebook**: `04_Incremental_Data_Processing/practice_tasks.py`
# MAGIC
# MAGIC **Completed**:
# MAGIC - Exercises 2-5: Auto Loader, streaming writes, trigger types (once, availableNow, processingTime)
# MAGIC - Restructured exercises to address serverless constraints (explicit triggers, Volume paths, schema locations)
# MAGIC - Diagnosed and resolved DBFS access errors (serverless blocks DBFS root writes)
# MAGIC - Updated exercise format: Exercise 2 as reusable reader, Exercises 3-5 as independent write operations
# MAGIC
# MAGIC **Key Learnings**:
# MAGIC - Serverless requires explicit trigger (once/availableNow) - continuous streaming not supported
# MAGIC - Auto Loader requires cloudFiles.schemaLocation on serverless
# MAGIC - Must write to UC Volumes (/Volumes/...) not DBFS paths or UC table names without pre-creation
# MAGIC - Trigger types: once (single micro-batch), availableNow (catch-up micro-batches), processingTime (exam syntax only)
# MAGIC
# MAGIC **Next**: Complete exercises 6-15, MCQs, challenges, ETL tasks
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,2026-06-15: Topic 3 Complete - Delta Lake Operations
# MAGIC %md
# MAGIC ### June 15, 2026 - Topic 3: Delta Lake Operations
# MAGIC **Duration**: 2.5 hours  
# MAGIC **Notebook**: `03_Delta_Lake_Operations`
# MAGIC
# MAGIC **Completed**:
# MAGIC - Practice exercises 1-15 (all exercises completed)
# MAGIC - All 5 MCQs completed (100% correct)
# MAGIC - Exercises focused on CREATE TABLE, INSERT/UPDATE/DELETE, MERGE, time travel, VACUUM, OPTIMIZE, ZORDER, table properties
# MAGIC - Fixed Exercise 3 instruction (changed task to SET status = 'inactive' WHERE amount > 150)
# MAGIC - Corrected overview.py MERGE documentation (clarified duplicate key handling and deduplication requirement)
# MAGIC
# MAGIC **Key Learnings**:
# MAGIC - MERGE requires unique keys in source - operation fails with cardinality violation if source has duplicates
# MAGIC - Must deduplicate source before MERGE using window functions (ROW_NUMBER with QUALIFY)
# MAGIC - Time travel has strict boundaries: cannot query timestamps before table creation (errors, doesn't fall back to version 0)
# MAGIC - VACUUM default retention: 7 days (168 hours) - this is heavily tested
# MAGIC - OPTIMIZE compacts files but does NOT reclaim space; VACUUM reclaims space
# MAGIC - ZORDER syntax must be part of OPTIMIZE command, cannot run independently
# MAGIC - Table property syntax requires quotes and interval keyword: `'delta.deletedFileRetentionDuration' = 'interval 30 days'`
# MAGIC - Multiple WHEN MATCHED clauses allowed in MERGE; order matters (DELETE before UPDATE)
# MAGIC
# MAGIC **Gaps Requiring Review**:
# MAGIC 1. Exercise 11: Time travel approach works but would fail on fresh table created < 1 hour ago (VERSION AS OF safer than TIMESTAMP AS OF with relative times)
# MAGIC 2. Exercise 12: Initially misread requirement (swapped DELETE/UPDATE conditions)
# MAGIC 3. Exercise 15: Used CTAS instead of SHALLOW CLONE (kept as bonus - CLONE not exam-required per gap analysis)
# MAGIC 4. Exam content claims: Added rule to local instructions - only make claims about exam frequency when documented in study materials
# MAGIC
# MAGIC **Material Corrections Made**:
# MAGIC 1. Fixed practice_tasks.py Exercise 3 instruction (cell nuid: 60c85f1b-4516-4680-98c5-8464526df9cf)
# MAGIC 2. Updated solutions.py Exercise 3 solution (cell nuid: 8ff574b3-8cbc-45e0-b881-7b65d34e5f18)
# MAGIC 3. Corrected overview.py MERGE section (cell nuid: 174174c3-3586-496b-8c4a-d4d77922e189) - added deduplication requirement and minimal syntax example
# MAGIC 4. Updated local .assistant_instructions.md with exam content claim policy
# MAGIC
# MAGIC **Next Session Action Items**:
# MAGIC 1. Verify VACUUM retention boundary behavior (inclusive vs exclusive at exact retention moment)
# MAGIC 2. Practice MERGE deduplication patterns from memory
# MAGIC 3. Review DESCRIBE DETAIL vs SHOW TBLPROPERTIES for property verification
# MAGIC 4. Begin Topic 4: Incremental Data Processing (streaming focus)
# MAGIC
# MAGIC **Status**: Topic 3 complete (exercises + MCQs)

# COMMAND ----------

# DBTITLE 1,2026-06-11: Practice Tasks Review & Exam Scope Verification
# MAGIC %md
# MAGIC ### June 11, 2026 - Topic 2: ELT with Spark SQL and Python
# MAGIC **Duration**: 1.5 hours  
# MAGIC **Notebook**: `02_ELT_Spark_SQL_Python`
# MAGIC
# MAGIC **Completed**:
# MAGIC - Practice exercises 1-15 (all exercises completed)
# MAGIC - All 5 MCQs completed
# MAGIC - Exercises focused on DataFrame API, schema definition, UDFs, window functions, and data writing
# MAGIC - Corrected serverless compute limitations in exercise instructions (13-14)
# MAGIC - Verified exam scope for join strategies via exam guide analysis
# MAGIC
# MAGIC **Key Learnings**:
# MAGIC - DataFrame `.groupBy().agg()` implicitly defines output columns (grouping + aggregated columns only)
# MAGIC - No explicit `.select()` needed when translating SQL GROUP BY to DataFrame API
# MAGIC - Aggregation collapses rows; contrast with window functions which preserve all columns
# MAGIC - `.partitionBy()` must come before `.saveAsTable()` in write method chain
# MAGIC - On serverless compute: Unity Catalog managed tables use Delta format by default
# MAGIC - Serverless restrictions: no `/tmp/` or `dbfs:/` writes, must use `.saveAsTable()` for managed tables
# MAGIC
# MAGIC **Gaps Requiring Review**:
# MAGIC 1. Exercise 12: Initially missed `udf` import in setup cell (fixed during session)
# MAGIC 2. Exercise 13-14: Attempted `/tmp/` writes which fail on serverless (instructions now corrected)
# MAGIC 3. Exercise 15: Original code had `.select()` before `.groupBy()` - operation order matters
# MAGIC 4. Join strategies (broadcast, sort-merge, shuffle hash) are Professional-level, NOT Associate exam scope
# MAGIC 5. Associate scope covers join **types** (inner, left, outer, anti) and basic syntax only
# MAGIC
# MAGIC **Next Session Action Items**:
# MAGIC 1. Verify `filter()` vs `where()` are true aliases (MCQ 2 research item)
# MAGIC 2. Review why join strategy MCQ appears in Associate practice if out of scope
# MAGIC 3. Begin Topic 3: Delta Lake Operations (MERGE, OPTIMIZE, VACUUM priority)
# MAGIC 4. Create flashcards for DataFrame API transformations
# MAGIC 5. Review Topic 1 and 2 weak areas before moving forward
# MAGIC
# MAGIC **Status**: Topic 2 complete (exercises + MCQs)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,2026-06-10: Topic 1 Started
# MAGIC %md
# MAGIC ### June 10, 2026 - Topic 1 Started
# MAGIC **Duration**: 2.5 hours  
# MAGIC **Notebook**: `01_Databricks_Lakehouse_Platform/`
# MAGIC
# MAGIC **Completed**:
# MAGIC - Overview notebook (Spark configs, compute types, shuffle mechanics, broadcast joins)
# MAGIC - Exercises 1-8, 11-15 (13/15 total)
# MAGIC - All 5 MCQs (100% correct)
# MAGIC
# MAGIC **Skipped**:
# MAGIC - Exercise 9 (performance measurement)
# MAGIC - Exercise 10 (cluster policies scenario)
# MAGIC - Challenge 1 and 2
# MAGIC - ETL Applieds 1 and 2
# MAGIC
# MAGIC **Key Learnings**:
# MAGIC - Shuffles occur during wide transformations (groupBy, join, orderBy) when data must be redistributed across executors
# MAGIC - Broadcast joins copy small tables to all executors, avoiding shuffle for large-small table joins
# MAGIC - Default shuffle.partitions (200) is often suboptimal - tune based on data size
# MAGIC - Serverless compute restricts RDD API access and many config reads
# MAGIC
# MAGIC **Gaps Requiring Review**:
# MAGIC 1. Memory pressure manifests as: OOM errors, spill-to-disk (performance degradation), task failures
# MAGIC 2. Disable broadcast joins: `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`
# MAGIC 3. AQE doesn't change broadcast threshold value, it makes join strategy selection dynamic at runtime
# MAGIC
# MAGIC **Next Session Action Items**:
# MAGIC 1. Complete Topic 1 Challenge scenarios (2)
# MAGIC 2. Complete Topic 1 ETL Applieds (2)
# MAGIC 3. Create flashcards for critical configs
# MAGIC 4. Review solutions notebook for alternative approaches
# MAGIC 5. Move to Topic 2 overview
# MAGIC
# MAGIC **Status**: Topic 1 ~70% complete, needs 1-2 more hours to finish
# MAGIC
# MAGIC ---

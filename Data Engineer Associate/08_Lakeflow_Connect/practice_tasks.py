# Databricks notebook source
# DBTITLE 1,Topic 8: Lakeflow Connect - Practice Tasks
# MAGIC %md
# MAGIC # Topic 8: Lakeflow Connect - Practice Tasks
# MAGIC
# MAGIC ## Instructions
# MAGIC
# MAGIC Complete the exercises below to test your understanding of Lakeflow Connect concepts. Each exercise includes:
# MAGIC * **Task description**: What you need to accomplish
# MAGIC * **Requirements**: Specific criteria your solution must meet
# MAGIC * **Exam focus**: Which exam objectives this tests
# MAGIC
# MAGIC Work through exercises in order. Later exercises build on concepts from earlier ones.
# MAGIC
# MAGIC ## Coverage
# MAGIC
# MAGIC * **Setup**: Test data preparation (if needed)
# MAGIC * **Exercises 1-15**: Core concepts and configuration patterns
# MAGIC * **MCQs 1-5**: Multiple choice scenarios (exam-style)
# MAGIC * **Challenges 1-2**: Complex multi-step problems
# MAGIC * **Applieds 1-2**: Decision-making scenarios
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Practice Data
# MAGIC
# MAGIC Most exercises are conceptual (no code execution required). For exercises requiring configuration examples, you'll write pseudo-configuration YAML or explain approaches.

# COMMAND ----------

# DBTITLE 1,Exercise 1: Connector Type Selection
# MAGIC %md
# MAGIC ## Exercise 1: Connector Type Selection
# MAGIC
# MAGIC **Exam Focus**: Distinguish between standard and managed connectors
# MAGIC
# MAGIC **Scenario**: You have the following ingestion requirements:
# MAGIC
# MAGIC 1. Ingest customer data from MySQL database
# MAGIC 2. Ingest CRM data from Salesforce
# MAGIC 3. Ingest HR data from Workday
# MAGIC 4. Ingest transactional data from PostgreSQL
# MAGIC 5. Ingest support tickets from ServiceNow
# MAGIC 6. Ingest sales data from a custom internal database
# MAGIC
# MAGIC **Task**: For each source, specify:
# MAGIC * Connector type (standard or managed)
# MAGIC * Justification (why that connector type)
# MAGIC * Authentication method
# MAGIC
# MAGIC **Requirements**:
# MAGIC * Correctly identify when standard vs managed connectors apply
# MAGIC * Explain the decision criteria
# MAGIC * Identify authentication patterns
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC 1. MySQL:
# MAGIC    - Connector type: ?
# MAGIC    - Justification: ?
# MAGIC    - Authentication: ?
# MAGIC
# MAGIC 2. Salesforce:
# MAGIC    - Connector type: ?
# MAGIC    - Justification: ?
# MAGIC    - Authentication: ?
# MAGIC
# MAGIC 3. Workday:
# MAGIC    - Connector type: ?
# MAGIC    - Justification: ?
# MAGIC    - Authentication: ?
# MAGIC
# MAGIC 4. PostgreSQL:
# MAGIC    - Connector type: ?
# MAGIC    - Justification: ?
# MAGIC    - Authentication: ?
# MAGIC
# MAGIC 5. ServiceNow:
# MAGIC    - Connector type: ?
# MAGIC    - Justification: ?
# MAGIC    - Authentication: ?
# MAGIC
# MAGIC 6. Custom internal database:
# MAGIC    - Connector type: ?
# MAGIC    - Justification: ?
# MAGIC    - Authentication: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 2: JDBC Connection Strings
# MAGIC %md
# MAGIC ## Exercise 2: JDBC Connection Strings
# MAGIC
# MAGIC **Exam Focus**: Construct correct JDBC connection strings for different databases
# MAGIC
# MAGIC **Scenario**: You need to configure Lakeflow Connect to ingest from the following databases:
# MAGIC
# MAGIC 1. MySQL server at `mysql-prod.company.com`, port `3306`, database `sales_db`, with SSL enabled
# MAGIC 2. PostgreSQL server at `10.0.5.100`, port `5432`, database `analytics`, default schema `public`
# MAGIC 3. SQL Server at `sqlserver.internal.net`, port `1433`, database `crm`, with encryption
# MAGIC 4. Oracle database at `oracle-prod.company.com`, port `1521`, SID `ORCL`
# MAGIC
# MAGIC **Task**: Write the correct JDBC connection string for each database.
# MAGIC
# MAGIC **Requirements**:
# MAGIC * Use correct JDBC URL format for each database type
# MAGIC * Include necessary parameters (SSL, encryption, schema)
# MAGIC * Follow database-specific syntax (semicolons for SQL Server, etc.)
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC 1. MySQL:
# MAGIC jdbc:???
# MAGIC
# MAGIC 2. PostgreSQL:
# MAGIC jdbc:???
# MAGIC
# MAGIC 3. SQL Server:
# MAGIC jdbc:???
# MAGIC
# MAGIC 4. Oracle:
# MAGIC jdbc:???
# MAGIC ```
# MAGIC
# MAGIC **Additional Question**: Which database uses semicolons instead of question marks for parameters?

# COMMAND ----------

# DBTITLE 1,Exercise 3: Incremental Loading Strategy
# MAGIC %md
# MAGIC ## Exercise 3: Incremental Loading Strategy
# MAGIC
# MAGIC **Exam Focus**: Choose correct incremental loading pattern based on source characteristics
# MAGIC
# MAGIC **Scenario**: Analyze the following tables and determine the appropriate incremental loading strategy:
# MAGIC
# MAGIC ### Table 1: `orders`
# MAGIC ```
# MAGIC Columns:
# MAGIC - order_id (INT, PRIMARY KEY, auto-increment)
# MAGIC - customer_id (INT)
# MAGIC - order_date (TIMESTAMP)
# MAGIC - total_amount (DECIMAL)
# MAGIC - status (VARCHAR)
# MAGIC - created_at (TIMESTAMP, immutable, default CURRENT_TIMESTAMP)
# MAGIC - updated_at (TIMESTAMP, updates on every change)
# MAGIC
# MAGIC Behavior:
# MAGIC - New orders inserted daily
# MAGIC - Existing orders NEVER updated (immutable)
# MAGIC - 100,000 rows, growing by 1,000/day
# MAGIC ```
# MAGIC
# MAGIC ### Table 2: `customer_accounts`
# MAGIC ```
# MAGIC Columns:
# MAGIC - account_id (INT, PRIMARY KEY)
# MAGIC - customer_name (VARCHAR)
# MAGIC - email (VARCHAR)
# MAGIC - account_balance (DECIMAL)
# MAGIC - last_login (TIMESTAMP)
# MAGIC - updated_at (TIMESTAMP, updates on every change)
# MAGIC
# MAGIC Behavior:
# MAGIC - New accounts added daily (50 new accounts/day)
# MAGIC - Existing accounts updated frequently (account_balance, last_login change)
# MAGIC - 500,000 total rows
# MAGIC - Need current state, not history
# MAGIC ```
# MAGIC
# MAGIC ### Table 3: `product_catalog`
# MAGIC ```
# MAGIC Columns:
# MAGIC - product_id (INT, PRIMARY KEY)
# MAGIC - product_name (VARCHAR)
# MAGIC - category (VARCHAR)
# MAGIC - price (DECIMAL)
# MAGIC
# MAGIC Behavior:
# MAGIC - Small reference table (5,000 rows)
# MAGIC - Updates infrequent (weekly)
# MAGIC - No timestamp or watermark column
# MAGIC ```
# MAGIC
# MAGIC **Task**: For each table, specify:
# MAGIC 1. Loading pattern (full load, incremental append, incremental upsert)
# MAGIC 2. Watermark column (if applicable)
# MAGIC 3. Primary key (if applicable)
# MAGIC 4. Justification
# MAGIC
# MAGIC **Requirements**:
# MAGIC * Choose the MOST EFFICIENT pattern for each scenario
# MAGIC * Explain why alternatives are suboptimal
# MAGIC * Consider table size, update frequency, and column availability
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC Table 1 (orders):
# MAGIC - Loading pattern: ?
# MAGIC - Watermark column: ?
# MAGIC - Primary key: ?
# MAGIC - Justification: ?
# MAGIC
# MAGIC Table 2 (customer_accounts):
# MAGIC - Loading pattern: ?
# MAGIC - Watermark column: ?
# MAGIC - Primary key: ?
# MAGIC - Justification: ?
# MAGIC
# MAGIC Table 3 (product_catalog):
# MAGIC - Loading pattern: ?
# MAGIC - Watermark column: ?
# MAGIC - Primary key: ?
# MAGIC - Justification: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 4: Credential Management
# MAGIC %md
# MAGIC ## Exercise 4: Credential Management
# MAGIC
# MAGIC **Exam Focus**: Secure credential handling for Lakeflow Connect
# MAGIC
# MAGIC **Scenario**: Your team is configuring Lakeflow Connect for a production MySQL database. The database requires username and password authentication.
# MAGIC
# MAGIC **Task**: Answer the following questions:
# MAGIC
# MAGIC 1. **Where should you store the database credentials?**
# MAGIC    - Option A: Hardcode in JDBC connection string
# MAGIC    - Option B: Store in notebook variables
# MAGIC    - Option C: Pass as job parameters
# MAGIC    - Option D: Store in Databricks Secrets
# MAGIC
# MAGIC 2. **What is the correct process for storing credentials?**
# MAGIC    - List the steps in order
# MAGIC
# MAGIC 3. **How do you reference secrets in Lakeflow Connect configuration?**
# MAGIC    - Provide example code/configuration
# MAGIC
# MAGIC 4. **Why are the other options (A, B, C) incorrect?**
# MAGIC    - Explain the security risk for each
# MAGIC
# MAGIC 5. **A team member suggests storing the password in a Unity Catalog table with restricted access. Is this acceptable?**
# MAGIC    - Yes or No
# MAGIC    - Justification
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC 1. Correct answer: ?
# MAGIC
# MAGIC 2. Steps to store credentials:
# MAGIC    Step 1: ?
# MAGIC    Step 2: ?
# MAGIC    Step 3: ?
# MAGIC
# MAGIC 3. Secret reference example:
# MAGIC    ???
# MAGIC
# MAGIC 4. Why other options are wrong:
# MAGIC    - Option A: ?
# MAGIC    - Option B: ?
# MAGIC    - Option C: ?
# MAGIC
# MAGIC 5. Storing in Unity Catalog table:
# MAGIC    - Answer: ?
# MAGIC    - Justification: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 5: Parallelization Configuration
# MAGIC %md
# MAGIC ## Exercise 5: Parallelization Configuration
# MAGIC
# MAGIC **Exam Focus**: Configure parallelization for JDBC ingestion performance
# MAGIC
# MAGIC **Scenario**: You're ingesting a large MySQL table:
# MAGIC
# MAGIC ```
# MAGIC Table: transactions
# MAGIC Rows: 50,000,000
# MAGIC Size: 80 GB
# MAGIC Columns:
# MAGIC - transaction_id (BIGINT, PRIMARY KEY, auto-increment, range 1 to 50000000)
# MAGIC - customer_id (INT)
# MAGIC - amount (DECIMAL)
# MAGIC - transaction_date (TIMESTAMP)
# MAGIC - category (VARCHAR)
# MAGIC
# MAGIC Source database:
# MAGIC - Max concurrent connections: 16
# MAGIC - Current ingestion time: 6 hours (single connection)
# MAGIC ```
# MAGIC
# MAGIC **Task**: Configure parallelization to reduce ingestion time.
# MAGIC
# MAGIC **Questions**:
# MAGIC
# MAGIC 1. **Which column should you use for partitioning?**
# MAGIC    - Explain why
# MAGIC
# MAGIC 2. **How many partitions should you configure?**
# MAGIC    - Consider connection limit
# MAGIC
# MAGIC 3. **If you configure 16 partitions with lower_bound=1, upper_bound=50000000, what queries will Lakeflow Connect generate?**
# MAGIC    - Provide the WHERE clause for partitions 1, 2, and 16
# MAGIC
# MAGIC 4. **Why can't you partition on `transaction_date` or `category`?**
# MAGIC
# MAGIC 5. **The DBA says the database can only handle 8 concurrent connections, not 16. What do you change?**
# MAGIC
# MAGIC 6. **After enabling parallelization, ingestion is still slow. What else might you tune?**
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC 1. Partition column: ?
# MAGIC    Reason: ?
# MAGIC
# MAGIC 2. Number of partitions: ?
# MAGIC    Justification: ?
# MAGIC
# MAGIC 3. Generated queries:
# MAGIC    Partition 1: WHERE ?
# MAGIC    Partition 2: WHERE ?
# MAGIC    Partition 16: WHERE ?
# MAGIC
# MAGIC 4. Why not partition on transaction_date or category:
# MAGIC    - transaction_date: ?
# MAGIC    - category: ?
# MAGIC
# MAGIC 5. With 8 connection limit:
# MAGIC    Change: ?
# MAGIC
# MAGIC 6. Additional tuning:
# MAGIC    Parameter: ?
# MAGIC    Recommendation: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 6: Decision Matrix - Ingestion Method Selection
# MAGIC %md
# MAGIC ## Exercise 6: Decision Matrix - Ingestion Method Selection
# MAGIC
# MAGIC **Exam Focus**: Choose correct ingestion method (Lakeflow Connect vs Auto Loader vs COPY INTO)
# MAGIC
# MAGIC **Scenario**: You have five ingestion requirements. Choose the appropriate method for each.
# MAGIC
# MAGIC ### Requirement 1
# MAGIC ```
# MAGIC Source: MySQL database (on-premises)
# MAGIC Data: Customer orders table
# MAGIC Frequency: Daily batch (midnight)
# MAGIC Size: 10 GB/day
# MAGIC Latency: Hourly updates acceptable
# MAGIC Incremental: Yes (watermark column available)
# MAGIC ```
# MAGIC
# MAGIC ### Requirement 2
# MAGIC ```
# MAGIC Source: JSON files landing in S3
# MAGIC Data: IoT sensor readings
# MAGIC Frequency: Continuous (files arrive every 30 seconds)
# MAGIC Size: 1000 files/hour, 100 MB each
# MAGIC Latency: Sub-minute required
# MAGIC Incremental: Process each file exactly once
# MAGIC ```
# MAGIC
# MAGIC ### Requirement 3
# MAGIC ```
# MAGIC Source: CSV files in ADLS
# MAGIC Data: Partner data feed
# MAGIC Frequency: Weekly (every Monday)
# MAGIC Size: Single 5 GB CSV file
# MAGIC Latency: Process within 1 hour of arrival
# MAGIC Incremental: No duplicates, simple batch load
# MAGIC ```
# MAGIC
# MAGIC ### Requirement 4
# MAGIC ```
# MAGIC Source: Salesforce
# MAGIC Data: Opportunity and Account objects
# MAGIC Frequency: Hourly sync
# MAGIC Size: 100K records, growing
# MAGIC Latency: 1-hour delay acceptable
# MAGIC Incremental: Capture updates and new records
# MAGIC ```
# MAGIC
# MAGIC ### Requirement 5
# MAGIC ```
# MAGIC Source: PostgreSQL database
# MAGIC Data: Product catalog
# MAGIC Frequency: Real-time (sub-minute latency required)
# MAGIC Size: Small (10K rows)
# MAGIC Latency: 30 seconds maximum
# MAGIC Incremental: Capture inserts/updates/deletes
# MAGIC ```
# MAGIC
# MAGIC **Task**: For each requirement:
# MAGIC 1. Choose the ingestion method (Lakeflow Connect, Auto Loader, COPY INTO, or other)
# MAGIC 2. Justify your choice
# MAGIC 3. Explain why alternatives are suboptimal
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC Requirement 1:
# MAGIC - Method: ?
# MAGIC - Justification: ?
# MAGIC - Why not alternatives: ?
# MAGIC
# MAGIC Requirement 2:
# MAGIC - Method: ?
# MAGIC - Justification: ?
# MAGIC - Why not alternatives: ?
# MAGIC
# MAGIC Requirement 3:
# MAGIC - Method: ?
# MAGIC - Justification: ?
# MAGIC - Why not alternatives: ?
# MAGIC
# MAGIC Requirement 4:
# MAGIC - Method: ?
# MAGIC - Justification: ?
# MAGIC - Why not alternatives: ?
# MAGIC
# MAGIC Requirement 5:
# MAGIC - Method: ?
# MAGIC - Justification: ?
# MAGIC - Why not alternatives: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 7: Managed Connector Configuration
# MAGIC %md
# MAGIC ## Exercise 7: Managed Connector Configuration
# MAGIC
# MAGIC **Exam Focus**: Configure managed connectors for SaaS applications
# MAGIC
# MAGIC **Scenario**: You need to ingest Salesforce data for sales analytics.
# MAGIC
# MAGIC **Requirements**:
# MAGIC * Objects needed: Account, Opportunity, Contact
# MAGIC * Include all fields (standard and custom)
# MAGIC * Capture updates hourly
# MAGIC * Handle schema changes automatically
# MAGIC
# MAGIC **Questions**:
# MAGIC
# MAGIC 1. **Which connector type do you use?**
# MAGIC    - Standard JDBC or Managed Salesforce connector
# MAGIC    - Justification
# MAGIC
# MAGIC 2. **What authentication method does the managed connector use?**
# MAGIC
# MAGIC 3. **Can you filter Opportunities to only those with Amount > $50,000 at the connector level?**
# MAGIC    - Yes or No
# MAGIC    - If No, how do you achieve this filtering?
# MAGIC
# MAGIC 4. **Your sales team adds a custom field `ExpectedCloseQuarter__c` to the Opportunity object. What happens on the next sync?**
# MAGIC
# MAGIC 5. **The Salesforce API has a rate limit of 5,000 requests/hour. How does the managed connector handle this?**
# MAGIC
# MAGIC 6. **You notice the `Account` object has a nested `BillingAddress` field with street, city, state, zip. How is this represented in the Delta table?**
# MAGIC
# MAGIC 7. **Can you run the connector every 5 minutes for near-real-time sync?**
# MAGIC    - Yes or No
# MAGIC    - Explain limitations
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC 1. Connector type: ?
# MAGIC    Justification: ?
# MAGIC
# MAGIC 2. Authentication: ?
# MAGIC
# MAGIC 3. Filter at connector level: ?
# MAGIC    How to filter: ?
# MAGIC
# MAGIC 4. Custom field handling: ?
# MAGIC
# MAGIC 5. Rate limit handling: ?
# MAGIC
# MAGIC 6. Nested address representation: ?
# MAGIC
# MAGIC 7. 5-minute sync: ?
# MAGIC    Limitations: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 8: Schema Evolution Scenarios
# MAGIC %md
# MAGIC ## Exercise 8: Schema Evolution Scenarios
# MAGIC
# MAGIC **Exam Focus**: Understand schema evolution behavior in Lakeflow Connect
# MAGIC
# MAGIC **Scenario**: You have a Lakeflow Connect pipeline ingesting from ServiceNow. The target Delta table has the following schema:
# MAGIC
# MAGIC ```
# MAGIC incident_table:
# MAGIC - incident_id (STRING)
# MAGIC - short_description (STRING)
# MAGIC - priority (INT)
# MAGIC - created_at (TIMESTAMP)
# MAGIC - assigned_to (STRING)
# MAGIC ```
# MAGIC
# MAGIC The following changes occur in ServiceNow:
# MAGIC
# MAGIC **Change 1**: ServiceNow admin adds a new field `escalation_level` (STRING)
# MAGIC **Change 2**: ServiceNow admin deletes the field `assigned_to`
# MAGIC **Change 3**: ServiceNow changes `priority` from INT to STRING (e.g., "High", "Medium", "Low")
# MAGIC **Change 4**: ServiceNow renames `short_description` to `summary`
# MAGIC
# MAGIC **Questions**:
# MAGIC
# MAGIC 1. **For Change 1 (new field added), what happens on the next sync?**
# MAGIC    - Assume schema evolution is enabled
# MAGIC    - What appears in existing rows?
# MAGIC
# MAGIC 2. **For Change 2 (field deleted), what happens?**
# MAGIC    - Does the column disappear from Delta table?
# MAGIC    - What appears in new rows?
# MAGIC
# MAGIC 3. **For Change 3 (type change INT to STRING), what happens?**
# MAGIC    - Does schema evolution handle this automatically?
# MAGIC    - If not, how do you fix it?
# MAGIC
# MAGIC 4. **For Change 4 (field renamed), what happens?**
# MAGIC    - Does the column get renamed in Delta table?
# MAGIC    - What does the connector see?
# MAGIC
# MAGIC 5. **If schema evolution is DISABLED, what happens for Change 1?**
# MAGIC
# MAGIC 6. **How would you intentionally disable schema evolution if needed?**
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC 1. New field added:
# MAGIC    - What happens: ?
# MAGIC    - Existing rows: ?
# MAGIC
# MAGIC 2. Field deleted:
# MAGIC    - Column disappears: ?
# MAGIC    - New rows: ?
# MAGIC
# MAGIC 3. Type change:
# MAGIC    - Automatic handling: ?
# MAGIC    - Fix required: ?
# MAGIC
# MAGIC 4. Field renamed:
# MAGIC    - Column renamed: ?
# MAGIC    - Connector behavior: ?
# MAGIC
# MAGIC 5. Schema evolution disabled:
# MAGIC    - What happens: ?
# MAGIC
# MAGIC 6. Disable schema evolution:
# MAGIC    - How: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 9: Custom Query at Source
# MAGIC %md
# MAGIC ## Exercise 9: Custom Query at Source
# MAGIC
# MAGIC **Exam Focus**: Use custom queries in standard connectors for filtering and transformation
# MAGIC
# MAGIC **Scenario**: You're ingesting from a PostgreSQL database. The source table has:
# MAGIC
# MAGIC ```
# MAGIC transactions table:
# MAGIC - transaction_id (BIGINT)
# MAGIC - customer_id (INT)
# MAGIC - product_id (INT)
# MAGIC - transaction_date (DATE)
# MAGIC - amount (DECIMAL)
# MAGIC - status (VARCHAR)  -- values: 'completed', 'pending', 'cancelled', 'refunded'
# MAGIC - created_at (TIMESTAMP)
# MAGIC - updated_at (TIMESTAMP)
# MAGIC ```
# MAGIC
# MAGIC The table has 100 million rows. You only need:
# MAGIC * Completed transactions (status = 'completed')
# MAGIC * From 2024 onwards (transaction_date >= '2024-01-01')
# MAGIC * Columns: transaction_id, customer_id, amount, transaction_date
# MAGIC * No cancelled or refunded transactions
# MAGIC
# MAGIC **Questions**:
# MAGIC
# MAGIC 1. **Should you ingest the full table and filter in Spark, or filter at source?**
# MAGIC    - Which approach is correct
# MAGIC    - Why
# MAGIC
# MAGIC 2. **Write the custom SQL query to use in Lakeflow Connect configuration.**
# MAGIC    - Use PostgreSQL syntax
# MAGIC    - Include only required columns and filters
# MAGIC
# MAGIC 3. **Your colleague suggests using Databricks SQL syntax in the custom query. Is this correct?**
# MAGIC    - Yes or No
# MAGIC    - Explanation
# MAGIC
# MAGIC 4. **You want to cast `amount` to DOUBLE during ingestion. Can you do this in the custom query?**
# MAGIC    - Yes or No
# MAGIC    - If yes, show the modified query
# MAGIC
# MAGIC 5. **The custom query reduces data transfer from 80 GB to 15 GB. What SQL optimization concept is this?**
# MAGIC
# MAGIC 6. **Can you join multiple source tables in the custom query?**
# MAGIC    - Yes or No
# MAGIC    - Any limitations?
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC 1. Approach: ?
# MAGIC    Why: ?
# MAGIC
# MAGIC 2. Custom query:
# MAGIC ```sql
# MAGIC ???
# MAGIC ```
# MAGIC
# MAGIC 3. Databricks SQL syntax: ?
# MAGIC    Explanation: ?
# MAGIC
# MAGIC 4. Cast amount to DOUBLE: ?
# MAGIC    Modified query:
# MAGIC ```sql
# MAGIC ???
# MAGIC ```
# MAGIC
# MAGIC 5. Optimization concept: ?
# MAGIC
# MAGIC 6. Join multiple tables: ?
# MAGIC    Limitations: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 10: Upsert vs Append Decision
# MAGIC %md
# MAGIC ## Exercise 10: Upsert vs Append Decision
# MAGIC
# MAGIC **Exam Focus**: Choose between incremental append and incremental upsert patterns
# MAGIC
# MAGIC **Scenario**: Analyze the following use cases and choose the correct incremental pattern.
# MAGIC
# MAGIC ### Use Case 1: Event Logs
# MAGIC ```
# MAGIC Table: application_logs
# MAGIC Columns:
# MAGIC - log_id (BIGINT, auto-increment)
# MAGIC - timestamp (TIMESTAMP)
# MAGIC - user_id (INT)
# MAGIC - event_type (VARCHAR)
# MAGIC - message (TEXT)
# MAGIC
# MAGIC Behavior:
# MAGIC - Append-only (no updates or deletes)
# MAGIC - 10 million new logs/day
# MAGIC - Historical logs never change
# MAGIC - You need all events for analysis
# MAGIC ```
# MAGIC
# MAGIC ### Use Case 2: Inventory Levels
# MAGIC ```
# MAGIC Table: product_inventory
# MAGIC Columns:
# MAGIC - product_id (INT, PRIMARY KEY)
# MAGIC - warehouse_id (INT)
# MAGIC - quantity_on_hand (INT)
# MAGIC - last_updated (TIMESTAMP)
# MAGIC
# MAGIC Behavior:
# MAGIC - Updates frequently (quantity changes)
# MAGIC - 50,000 products
# MAGIC - Need CURRENT inventory levels only
# MAGIC - Don't need historical changes
# MAGIC ```
# MAGIC
# MAGIC ### Use Case 3: Order Status
# MAGIC ```
# MAGIC Table: orders
# MAGIC Columns:
# MAGIC - order_id (BIGINT, PRIMARY KEY)
# MAGIC - customer_id (INT)
# MAGIC - order_date (DATE)
# MAGIC - status (VARCHAR)  -- 'pending' -> 'shipped' -> 'delivered'
# MAGIC - updated_at (TIMESTAMP)
# MAGIC
# MAGIC Behavior:
# MAGIC - Status updates as orders progress
# MAGIC - Need to track status changes over time
# MAGIC - Build history of status transitions
# MAGIC ```
# MAGIC
# MAGIC **Task**: For each use case:
# MAGIC 1. Choose pattern (incremental append or incremental upsert)
# MAGIC 2. Specify watermark column
# MAGIC 3. Specify primary key (if needed)
# MAGIC 4. Justify the choice
# MAGIC 5. Explain what would go wrong with the other pattern
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC Use Case 1 (Event Logs):
# MAGIC - Pattern: ?
# MAGIC - Watermark: ?
# MAGIC - Primary key: ?
# MAGIC - Justification: ?
# MAGIC - Why not the other pattern: ?
# MAGIC
# MAGIC Use Case 2 (Inventory Levels):
# MAGIC - Pattern: ?
# MAGIC - Watermark: ?
# MAGIC - Primary key: ?
# MAGIC - Justification: ?
# MAGIC - Why not the other pattern: ?
# MAGIC
# MAGIC Use Case 3 (Order Status):
# MAGIC - Pattern: ?
# MAGIC - Watermark: ?
# MAGIC - Primary key: ?
# MAGIC - Justification: ?
# MAGIC - Why not the other pattern: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 11: Performance Troubleshooting
# MAGIC %md
# MAGIC ## Exercise 11: Performance Troubleshooting
# MAGIC
# MAGIC **Exam Focus**: Diagnose and fix performance issues in Lakeflow Connect pipelines
# MAGIC
# MAGIC **Scenario 1**: MySQL ingestion taking 8 hours for 50 GB table
# MAGIC ```
# MAGIC Current config:
# MAGIC - No parallelization
# MAGIC - Fetch size: default (100 rows)
# MAGIC - Network: 1 Gbps connection
# MAGIC - Source allows 20 concurrent connections
# MAGIC ```
# MAGIC
# MAGIC **Problem**: What are THREE changes you'd make to improve performance?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Scenario 2**: Salesforce managed connector fails with "Rate limit exceeded"
# MAGIC ```
# MAGIC Current config:
# MAGIC - Sync frequency: Every 15 minutes
# MAGIC - Objects: 10 large objects (Account, Opportunity, Contact, etc.)
# MAGIC - Records: 500K total
# MAGIC ```
# MAGIC
# MAGIC **Problem**: What's the issue and how do you fix it?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Scenario 3**: PostgreSQL ingestion uses 10 partitions but still slow
# MAGIC ```
# MAGIC Current config:
# MAGIC - Partitions: 10
# MAGIC - Partition column: transaction_id (BIGINT)
# MAGIC - Bounds: lower=1, upper=1000000
# MAGIC - Fetch size: default
# MAGIC ```
# MAGIC
# MAGIC **Metrics**:
# MAGIC - Partition 1-9: Complete in 10 minutes
# MAGIC - Partition 10: Takes 3 hours
# MAGIC
# MAGIC **Problem**: What's causing this and how do you fix it?
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC Scenario 1 - Three improvements:
# MAGIC 1. ?
# MAGIC 2. ?
# MAGIC 3. ?
# MAGIC
# MAGIC Scenario 2:
# MAGIC - Issue: ?
# MAGIC - Fix: ?
# MAGIC
# MAGIC Scenario 3:
# MAGIC - Root cause: ?
# MAGIC - Fix: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 12: Unity Catalog Integration
# MAGIC %md
# MAGIC ## Exercise 12: Unity Catalog Integration
# MAGIC
# MAGIC **Exam Focus**: Understand how Lakeflow Connect integrates with Unity Catalog governance
# MAGIC
# MAGIC **Scenario**: Your organization has the following Unity Catalog structure:
# MAGIC
# MAGIC ```
# MAGIC Catalogs:
# MAGIC - dev (development)
# MAGIC - staging (pre-production)
# MAGIC - prod (production)
# MAGIC
# MAGIC Schemas in prod catalog:
# MAGIC - raw (landing zone for ingested data)
# MAGIC - clean (transformed data)
# MAGIC - analytics (gold layer)
# MAGIC ```
# MAGIC
# MAGIC **Questions**:
# MAGIC
# MAGIC 1. **You want Lakeflow Connect to land MySQL data into the `prod.raw` schema. How do you specify the target location?**
# MAGIC    - Full table path format
# MAGIC
# MAGIC 2. **Your user account has SELECT on `prod.raw` but not CREATE TABLE. What happens when you run the pipeline?**
# MAGIC
# MAGIC 3. **Data accidentally landed in `hive_metastore.default` instead of `prod.raw`. What configuration was missing?**
# MAGIC
# MAGIC 4. **Can Lakeflow Connect bypass Unity Catalog permissions?**
# MAGIC    - Yes or No
# MAGIC    - Explanation
# MAGIC
# MAGIC 5. **You want to ingest Salesforce data into `prod.raw.salesforce_accounts`. The pipeline needs to run as a service principal. What permissions does the service principal need?**
# MAGIC
# MAGIC 6. **A colleague suggests ingesting into the legacy Hive metastore for better performance. Is this a good practice?**
# MAGIC    - Yes or No
# MAGIC    - Why or why not
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC 1. Target table path: ?
# MAGIC
# MAGIC 2. Without CREATE TABLE permission: ?
# MAGIC
# MAGIC 3. Missing configuration: ?
# MAGIC
# MAGIC 4. Bypass UC permissions: ?
# MAGIC    Explanation: ?
# MAGIC
# MAGIC 5. Service principal permissions:
# MAGIC    - ?
# MAGIC
# MAGIC 6. Use Hive metastore: ?
# MAGIC    Reasoning: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 13: Scheduling and Orchestration
# MAGIC %md
# MAGIC ## Exercise 13: Scheduling and Orchestration
# MAGIC
# MAGIC **Exam Focus**: Configure scheduling and integrate Lakeflow Connect with Lakeflow Jobs
# MAGIC
# MAGIC **Scenario**: You have a multi-stage data pipeline:
# MAGIC
# MAGIC 1. Ingest orders from MySQL (Lakeflow Connect)
# MAGIC 2. Ingest customer data from Salesforce (Lakeflow Connect)
# MAGIC 3. Join and clean data (Spark notebook)
# MAGIC 4. Write to gold layer (Spark notebook)
# MAGIC
# MAGIC **Questions**:
# MAGIC
# MAGIC 1. **Can Lakeflow Connect pipelines trigger automatically when source data changes?**
# MAGIC    - Yes or No
# MAGIC    - If No, what are the trigger options?
# MAGIC
# MAGIC 2. **You want the pipeline to run daily at 2 AM UTC. How do you configure this?**
# MAGIC
# MAGIC 3. **How do you orchestrate all four stages to run in sequence using Lakeflow Jobs?**
# MAGIC    - List the task types and dependencies
# MAGIC
# MAGIC 4. **Stage 1 (MySQL ingestion) must complete before Stage 3 (Spark notebook) runs. How do you enforce this dependency?**
# MAGIC
# MAGIC 5. **Can you trigger the Lakeflow Connect pipeline on S3 file arrival?**
# MAGIC    - Yes or No
# MAGIC    - Explanation
# MAGIC
# MAGIC 6. **What's the minimum sync frequency supported by managed connectors?**
# MAGIC
# MAGIC 7. **You need the pipeline to run twice daily (8 AM and 6 PM). How do you configure this?**
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC 1. Auto-trigger on data change: ?
# MAGIC    Available triggers: ?
# MAGIC
# MAGIC 2. Daily at 2 AM UTC:
# MAGIC    Configuration: ?
# MAGIC
# MAGIC 3. Orchestration with Lakeflow Jobs:
# MAGIC    Task 1: Type = ?, Name = ?
# MAGIC    Task 2: Type = ?, Name = ?
# MAGIC    Task 3: Type = ?, Name = ?, Depends on = ?
# MAGIC    Task 4: Type = ?, Name = ?, Depends on = ?
# MAGIC
# MAGIC 4. Enforce dependency:
# MAGIC    How: ?
# MAGIC
# MAGIC 5. Trigger on file arrival: ?
# MAGIC    Explanation: ?
# MAGIC
# MAGIC 6. Minimum frequency: ?
# MAGIC
# MAGIC 7. Twice daily:
# MAGIC    Configuration: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 14: Semi-Structured Data Handling
# MAGIC %md
# MAGIC ## Exercise 14: Semi-Structured Data Handling
# MAGIC
# MAGIC **Exam Focus**: Handle JSON and nested data from SaaS connectors
# MAGIC
# MAGIC **Scenario**: You're ingesting Salesforce Opportunity data. The managed connector returns:
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "Id": "006ABC123",
# MAGIC   "Name": "Acme Corp - 1000 Licenses",
# MAGIC   "Amount": 50000,
# MAGIC   "Owner": {
# MAGIC     "Id": "005XYZ789",
# MAGIC     "Name": "John Smith",
# MAGIC     "Email": "john@example.com",
# MAGIC     "Title": "Sales Director"
# MAGIC   },
# MAGIC   "Account": {
# MAGIC     "Id": "001DEF456",
# MAGIC     "Name": "Acme Corp",
# MAGIC     "Industry": "Technology"
# MAGIC   },
# MAGIC   "ContactRoles": [
# MAGIC     {"ContactId": "003AAA111", "Role": "Decision Maker"},
# MAGIC     {"ContactId": "003BBB222", "Role": "Influencer"}
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Questions**:
# MAGIC
# MAGIC 1. **How does the managed connector flatten the nested `Owner` object?**
# MAGIC    - What column names are created?
# MAGIC
# MAGIC 2. **How do you access the owner's email in Spark SQL after ingestion?**
# MAGIC    - Write the column reference
# MAGIC
# MAGIC 3. **The `ContactRoles` field is an array. How is this handled?**
# MAGIC    - Storage format in Delta table
# MAGIC
# MAGIC 4. **You need one row per contact role (two rows for this opportunity). How do you achieve this?**
# MAGIC    - Can the connector do this during ingestion?
# MAGIC    - If not, what Spark function do you use?
# MAGIC
# MAGIC 5. **Salesforce adds a new nested field `CloseDate.FiscalYear`. What happens on next sync?**
# MAGIC
# MAGIC 6. **Do you need to write JSON parsing code (e.g., `from_json()`) for the `Owner` object?**
# MAGIC    - Yes or No
# MAGIC    - Why or why not?
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC 1. Flattened Owner columns:
# MAGIC    - ?
# MAGIC
# MAGIC 2. Access owner email:
# MAGIC    SELECT ??? FROM opportunities
# MAGIC
# MAGIC 3. ContactRoles storage:
# MAGIC    Format: ?
# MAGIC
# MAGIC 4. One row per contact role:
# MAGIC    - Connector capability: ?
# MAGIC    - Spark function: ?
# MAGIC
# MAGIC 5. New nested field:
# MAGIC    What happens: ?
# MAGIC
# MAGIC 6. Need from_json(): ?
# MAGIC    Reasoning: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercise 15: Error Handling and Troubleshooting
# MAGIC %md
# MAGIC ## Exercise 15: Error Handling and Troubleshooting
# MAGIC
# MAGIC **Exam Focus**: Diagnose and fix common Lakeflow Connect errors
# MAGIC
# MAGIC **Error 1**:
# MAGIC ```
# MAGIC Error: java.sql.SQLException: Access denied for user 'dbuser'@'databricks-host' 
# MAGIC (using password: YES)
# MAGIC ```
# MAGIC **Question**: What are three possible causes?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Error 2**:
# MAGIC ```
# MAGIC Error: Schema mismatch. Column 'new_field' exists in source but not in target table.
# MAGIC ```
# MAGIC **Question**: What's the issue and how do you fix it?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Error 3**:
# MAGIC ```
# MAGIC Error: Target table 'dev.sales.orders' not found. 
# MAGIC Table will be created in catalog 'hive_metastore'.
# MAGIC ```
# MAGIC **Question**: What configuration is missing?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Error 4**:
# MAGIC ```
# MAGIC Error: JDBC partition column 'order_date' must be numeric (INT, BIGINT). 
# MAGIC Found: TIMESTAMP
# MAGIC ```
# MAGIC **Question**: What's wrong and what are two solutions?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Error 5**:
# MAGIC ```
# MAGIC Salesforce connector: Rate limit exceeded (5000 requests/hour)
# MAGIC Next retry in 3600 seconds
# MAGIC ```
# MAGIC **Question**: Should you implement custom rate limiting code? Why or why not?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Error 6**:
# MAGIC ```
# MAGIC Error: Connection timeout after 30 seconds connecting to mysql-prod.company.com:3306
# MAGIC ```
# MAGIC **Question**: What are three possible causes?
# MAGIC
# MAGIC **Your Answer**:
# MAGIC
# MAGIC ```
# MAGIC Error 1 - Three causes:
# MAGIC 1. ?
# MAGIC 2. ?
# MAGIC 3. ?
# MAGIC
# MAGIC Error 2:
# MAGIC - Issue: ?
# MAGIC - Fix: ?
# MAGIC
# MAGIC Error 3:
# MAGIC - Missing config: ?
# MAGIC
# MAGIC Error 4:
# MAGIC - Problem: ?
# MAGIC - Solution 1: ?
# MAGIC - Solution 2: ?
# MAGIC
# MAGIC Error 5:
# MAGIC - Implement custom rate limiting: ?
# MAGIC - Reasoning: ?
# MAGIC
# MAGIC Error 6 - Three causes:
# MAGIC 1. ?
# MAGIC 2. ?
# MAGIC 3. ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,MCQ 1: Connector Selection
# MAGIC %md
# MAGIC ## MCQ 1: Connector Selection
# MAGIC
# MAGIC **Question**: Your team needs to ingest data from the following sources into Unity Catalog. Which ingestion method should you use for each?
# MAGIC
# MAGIC Sources:
# MAGIC 1. Oracle database with 500 GB transactions table
# MAGIC 2. JSON files landing in S3 bucket (1000 files/hour)
# MAGIC 3. Salesforce CRM data (Accounts and Opportunities)
# MAGIC 4. CSV file uploaded to ADLS weekly
# MAGIC 5. ServiceNow incident tickets
# MAGIC
# MAGIC Options for each source:
# MAGIC * A. Lakeflow Connect (standard connector)
# MAGIC * B. Lakeflow Connect (managed connector)
# MAGIC * C. Auto Loader
# MAGIC * D. COPY INTO
# MAGIC * E. Partner connector required
# MAGIC
# MAGIC **Your Answer**:
# MAGIC ```
# MAGIC 1. Oracle: ?
# MAGIC 2. JSON in S3: ?
# MAGIC 3. Salesforce: ?
# MAGIC 4. Weekly CSV: ?
# MAGIC 5. ServiceNow: ?
# MAGIC ```
# MAGIC
# MAGIC **Justification for each**:
# MAGIC ```
# MAGIC 1. Why: ?
# MAGIC 2. Why: ?
# MAGIC 3. Why: ?
# MAGIC 4. Why: ?
# MAGIC 5. Why: ?
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,MCQ 2: Incremental Loading Configuration
# MAGIC %md
# MAGIC ## MCQ 2: Incremental Loading Configuration
# MAGIC
# MAGIC **Question**: A MySQL table has the following structure:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE customer_accounts (
# MAGIC     account_id INT PRIMARY KEY AUTO_INCREMENT,
# MAGIC     customer_name VARCHAR(255),
# MAGIC     email VARCHAR(255),
# MAGIC     balance DECIMAL(10,2),
# MAGIC     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
# MAGIC     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Behavior**:
# MAGIC * New accounts added daily
# MAGIC * Existing accounts frequently updated (balance changes)
# MAGIC * You need CURRENT state only (not history)
# MAGIC * Table has 5 million rows
# MAGIC
# MAGIC Which configuration is correct?
# MAGIC
# MAGIC **A.**
# MAGIC ```yaml
# MAGIC loading_pattern: full_load
# MAGIC ```
# MAGIC
# MAGIC **B.**
# MAGIC ```yaml
# MAGIC loading_pattern: incremental_append
# MAGIC watermark_column: created_at
# MAGIC ```
# MAGIC
# MAGIC **C.**
# MAGIC ```yaml
# MAGIC loading_pattern: incremental_append
# MAGIC watermark_column: updated_at
# MAGIC ```
# MAGIC
# MAGIC **D.**
# MAGIC ```yaml
# MAGIC loading_pattern: incremental_upsert
# MAGIC watermark_column: updated_at
# MAGIC primary_key: account_id
# MAGIC ```
# MAGIC
# MAGIC **E.**
# MAGIC ```yaml
# MAGIC loading_pattern: incremental_upsert
# MAGIC watermark_column: created_at
# MAGIC primary_key: account_id
# MAGIC ```
# MAGIC
# MAGIC **Your Answer**: ?
# MAGIC
# MAGIC **Justification**: ?
# MAGIC
# MAGIC **Why other options are wrong**:
# MAGIC * A: ?
# MAGIC * B: ?
# MAGIC * C: ?
# MAGIC * E: ?

# COMMAND ----------

# DBTITLE 1,MCQ 3: Performance Optimization
# MAGIC %md
# MAGIC ## MCQ 3: Performance Optimization
# MAGIC
# MAGIC **Question**: A JDBC ingestion from PostgreSQL is taking 6 hours for a 100 GB table. Current configuration:
# MAGIC
# MAGIC ```yaml
# MAGIC source_table: transactions
# MAGIC partitions: 1
# MAGIC fetch_size: default (100 rows)
# MAGIC partition_column: null
# MAGIC ```
# MAGIC
# MAGIC The table structure:
# MAGIC ```sql
# MAGIC transaction_id BIGINT PRIMARY KEY (range 1 to 50000000)
# MAGIC transaction_date DATE
# MAGIC customer_id INT
# MAGIC amount DECIMAL
# MAGIC ```
# MAGIC
# MAGIC Source database allows 16 concurrent connections.
# MAGIC
# MAGIC Which change will provide the MOST performance improvement?
# MAGIC
# MAGIC **A.** Increase fetch size to 50,000 rows
# MAGIC
# MAGIC **B.** Enable parallelization with 16 partitions on `transaction_id`
# MAGIC
# MAGIC **C.** Enable parallelization with 16 partitions on `transaction_date`
# MAGIC
# MAGIC **D.** Use custom query with WHERE clause to reduce data transfer
# MAGIC
# MAGIC **E.** Switch to managed connector instead of standard JDBC
# MAGIC
# MAGIC **Your Answer**: ?
# MAGIC
# MAGIC **Justification**: ?
# MAGIC
# MAGIC **Why other options are less effective**:
# MAGIC * Remaining options: ?

# COMMAND ----------

# DBTITLE 1,MCQ 4: Schema Evolution
# MAGIC %md
# MAGIC ## MCQ 4: Schema Evolution
# MAGIC
# MAGIC **Question**: You have a Lakeflow Connect pipeline ingesting from Salesforce to `prod.raw.opportunities`. The pipeline has been running for 6 months.
# MAGIC
# MAGIC Current Delta table schema:
# MAGIC ```
# MAGIC Id STRING
# MAGIC Name STRING
# MAGIC Amount DECIMAL
# MAGIC CloseDate DATE
# MAGIC Owner.Name STRING
# MAGIC Owner.Email STRING
# MAGIC ```
# MAGIC
# MAGIC Salesforce admin makes the following change:
# MAGIC * Adds custom field `ExpectedRevenue__c` (DECIMAL)
# MAGIC * Deletes standard field `CloseDate`
# MAGIC * Changes `Amount` from DECIMAL to STRING
# MAGIC
# MAGIC Schema evolution is ENABLED.
# MAGIC
# MAGIC What happens on the next sync?
# MAGIC
# MAGIC **A.** All changes applied automatically. New column added, CloseDate removed, Amount converted to STRING
# MAGIC
# MAGIC **B.** New column `ExpectedRevenue__c` added. CloseDate remains (with NULLs for new rows). Amount type mismatch causes failure
# MAGIC
# MAGIC **C.** New column `ExpectedRevenue__c` added. All other changes ignored (CloseDate and Amount unchanged)
# MAGIC
# MAGIC **D.** Pipeline fails immediately with schema mismatch error. Manual intervention required for all changes
# MAGIC
# MAGIC **E.** New column added automatically. CloseDate and Amount require manual ALTER TABLE statements
# MAGIC
# MAGIC **Your Answer**: ?
# MAGIC
# MAGIC **Justification**: ?
# MAGIC
# MAGIC **What happens to existing data**: ?

# COMMAND ----------

# DBTITLE 1,MCQ 5: Ingestion Method Decision
# MAGIC %md
# MAGIC ## MCQ 5: Ingestion Method Decision
# MAGIC
# MAGIC **Question**: Which scenario-method pairing is INCORRECT?
# MAGIC
# MAGIC **A.** Scenario: CSV files land in S3 every 5 minutes, need sub-minute latency  
# MAGIC Method: Auto Loader with streaming mode
# MAGIC
# MAGIC **B.** Scenario: Weekly Parquet file upload to ADLS, simple batch load  
# MAGIC Method: COPY INTO
# MAGIC
# MAGIC **C.** Scenario: Ingest from Salesforce with hourly sync  
# MAGIC Method: Lakeflow Connect managed connector
# MAGIC
# MAGIC **D.** Scenario: MySQL database, need real-time CDC (sub-second latency)  
# MAGIC Method: Lakeflow Connect standard connector with incremental upsert
# MAGIC
# MAGIC **E.** Scenario: PostgreSQL database, daily full table refresh  
# MAGIC Method: Lakeflow Connect standard connector with full load mode
# MAGIC
# MAGIC **Your Answer**: ?
# MAGIC
# MAGIC **Why this pairing is incorrect**: ?
# MAGIC
# MAGIC **What method should be used instead**: ?

# COMMAND ----------

# DBTITLE 1,Challenge 1: Multi-Source Integration Pipeline
# MAGIC %md
# MAGIC ## Challenge 1: Multi-Source Integration Pipeline
# MAGIC
# MAGIC **Scenario**: You're building a customer analytics platform that ingests data from multiple sources:
# MAGIC
# MAGIC ### Source 1: MySQL Database (on-premises)
# MAGIC ```
# MAGIC Table: customer_transactions
# MAGIC Rows: 50 million
# MAGIC Columns: transaction_id, customer_id, product_id, amount, transaction_date, created_at, updated_at
# MAGIC Behavior: 
# MAGIC - New transactions inserted continuously
# MAGIC - Existing transactions NEVER updated
# MAGIC - Growing by 100K rows/day
# MAGIC Connection limit: 8 concurrent connections
# MAGIC ```
# MAGIC
# MAGIC ### Source 2: Salesforce
# MAGIC ```
# MAGIC Objects needed: Account, Contact, Opportunity
# MAGIC Records: 200K accounts, 500K contacts, 100K opportunities
# MAGIC Custom fields: Multiple custom fields per object
# MAGIC Update frequency: Data changes hourly
# MAGIC ```
# MAGIC
# MAGIC ### Source 3: JSON Files in S3
# MAGIC ```
# MAGIC Path: s3://company-data/events/
# MAGIC File pattern: YYYY/MM/DD/HH/events-*.json
# MAGIC Frequency: 1000 files/hour
# MAGIC Size: 50 MB per file
# MAGIC Schema: Semi-structured with nested fields
# MAGIC Requirement: Process within 5 minutes of arrival
# MAGIC ```
# MAGIC
# MAGIC ### Target Unity Catalog Structure
# MAGIC ```
# MAGIC prod.raw.mysql_transactions
# MAGIC prod.raw.salesforce_accounts
# MAGIC prod.raw.salesforce_contacts  
# MAGIC prod.raw.salesforce_opportunities
# MAGIC prod.raw.event_logs
# MAGIC ```
# MAGIC
# MAGIC **Tasks**:
# MAGIC
# MAGIC 1. **For each source, design the complete Lakeflow Connect/Auto Loader/COPY INTO configuration**:
# MAGIC    * Ingestion method
# MAGIC    * Loading pattern (full/append/upsert)
# MAGIC    * Watermark columns (if applicable)
# MAGIC    * Parallelization settings (if applicable)
# MAGIC    * Schedule/trigger
# MAGIC
# MAGIC 2. **Design the Lakeflow Job orchestration**:
# MAGIC    * List all tasks
# MAGIC    * Define dependencies
# MAGIC    * Configure schedule
# MAGIC    * Handle failure scenarios
# MAGIC
# MAGIC 3. **Identify potential issues and mitigation strategies**:
# MAGIC    * Performance bottlenecks
# MAGIC    * Schema evolution challenges
# MAGIC    * Authentication management
# MAGIC    * Cost optimization
# MAGIC
# MAGIC 4. **Estimate data processing times** based on:
# MAGIC    * MySQL: 50M rows, 80 GB
# MAGIC    * Salesforce: 800K records total
# MAGIC    * S3: 1000 files/hour, 50 GB/hour
# MAGIC
# MAGIC **Your Complete Solution**:
# MAGIC
# MAGIC ```
# MAGIC === SOURCE CONFIGURATIONS ===
# MAGIC
# MAGIC [Document your complete configuration for each source]
# MAGIC
# MAGIC === ORCHESTRATION ===
# MAGIC
# MAGIC [Design the Lakeflow Job DAG]
# MAGIC
# MAGIC === ISSUES & MITIGATIONS ===
# MAGIC
# MAGIC [Identify and address potential problems]
# MAGIC
# MAGIC === PERFORMANCE ESTIMATES ===
# MAGIC
# MAGIC [Estimate processing times and resource requirements]
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Challenge 2: Migration and Optimization
# MAGIC %md
# MAGIC ## Challenge 2: Migration and Optimization
# MAGIC
# MAGIC **Scenario**: Your company currently ingests data using custom Python scripts with JDBC. You've been asked to migrate to Lakeflow Connect and optimize the pipeline.
# MAGIC
# MAGIC ### Current Implementation (Custom Python)
# MAGIC ```python
# MAGIC # Runs in notebook, scheduled via Lakeflow Jobs
# MAGIC import jaydebeapi
# MAGIC
# MAGIC conn = jaydebeapi.connect(
# MAGIC     "org.postgresql.Driver",
# MAGIC     f"jdbc:postgresql://{host}:5432/{database}",
# MAGIC     [username, password],  # Credentials hardcoded
# MAGIC     "/path/to/postgresql-driver.jar"
# MAGIC )
# MAGIC
# MAGIC cursor = conn.cursor()
# MAGIC cursor.execute("SELECT * FROM orders WHERE created_at > '2024-01-01'")
# MAGIC rows = cursor.fetchall()
# MAGIC
# MAGIC # Manual DataFrame creation
# MAGIC df = spark.createDataFrame(rows, schema)
# MAGIC
# MAGIC # Write to Delta (overwrites existing data)
# MAGIC df.write.mode("overwrite").saveAsTable("dev.raw.orders")
# MAGIC ```
# MAGIC
# MAGIC ### Problems with Current Implementation
# MAGIC * Hardcoded credentials
# MAGIC * No incremental loading (full refresh daily)
# MAGIC * Single connection (no parallelization)
# MAGIC * Overwrites entire table daily
# MAGIC * No schema evolution handling
# MAGIC * Manual error handling
# MAGIC * Target table in wrong catalog (dev instead of prod)
# MAGIC
# MAGIC ### Source Table Details
# MAGIC ```
# MAGIC Table: orders
# MAGIC Rows: 10 million
# MAGIC Growth: 50K new rows/day
# MAGIC Updates: Order status changes (10% of rows updated daily)
# MAGIC Columns:
# MAGIC - order_id (BIGINT, PRIMARY KEY)
# MAGIC - customer_id (INT)
# MAGIC - order_date (DATE)
# MAGIC - total_amount (DECIMAL)
# MAGIC - status (VARCHAR)
# MAGIC - created_at (TIMESTAMP)
# MAGIC - updated_at (TIMESTAMP)
# MAGIC Database: PostgreSQL 14
# MAGIC Connection limit: 12 concurrent
# MAGIC ```
# MAGIC
# MAGIC **Tasks**:
# MAGIC
# MAGIC 1. **Design the Lakeflow Connect configuration**:
# MAGIC    * Connector type
# MAGIC    * Incremental loading strategy
# MAGIC    * Watermark and primary key selection
# MAGIC    * Parallelization settings
# MAGIC    * Target table location
# MAGIC    * Credential management
# MAGIC
# MAGIC 2. **Calculate the improvement**:
# MAGIC    * Current: Full refresh 10M rows daily, takes 4 hours
# MAGIC    * Proposed: Incremental with your configuration
# MAGIC    * Estimate new processing time
# MAGIC    * Estimate data transfer reduction
# MAGIC
# MAGIC 3. **Handle the migration**:
# MAGIC    * How do you migrate without data loss?
# MAGIC    * How do you validate the new pipeline?
# MAGIC    * What's your rollback plan?
# MAGIC
# MAGIC 4. **Document the benefits**:
# MAGIC    * Performance improvement
# MAGIC    * Cost reduction
# MAGIC    * Operational simplicity
# MAGIC    * Security improvements
# MAGIC
# MAGIC 5. **Address edge cases**:
# MAGIC    * What if orders are deleted in source?
# MAGIC    * What if updated_at timestamp is unreliable?
# MAGIC    * What if schema changes during migration?
# MAGIC
# MAGIC **Your Complete Solution**:
# MAGIC
# MAGIC ```
# MAGIC === LAKEFLOW CONNECT CONFIGURATION ===
# MAGIC
# MAGIC [Complete configuration details]
# MAGIC
# MAGIC === PERFORMANCE ANALYSIS ===
# MAGIC
# MAGIC Current:
# MAGIC - Processing time: 4 hours
# MAGIC - Data transfer: 80 GB
# MAGIC - Cost: $X
# MAGIC
# MAGIC Proposed:
# MAGIC - Processing time: ?
# MAGIC - Data transfer: ?
# MAGIC - Cost: ?
# MAGIC - Improvement: ?
# MAGIC
# MAGIC === MIGRATION PLAN ===
# MAGIC
# MAGIC [Step-by-step migration approach]
# MAGIC
# MAGIC === BENEFITS ===
# MAGIC
# MAGIC [Quantified improvements]
# MAGIC
# MAGIC === EDGE CASES ===
# MAGIC
# MAGIC [How you handle special scenarios]
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Applied 1: Decision Tree Exercise
# MAGIC %md
# MAGIC ## Applied 1: Decision Tree Exercise
# MAGIC
# MAGIC **Exam Focus**: Apply decision logic to choose the correct ingestion method
# MAGIC
# MAGIC **Instructions**: For each scenario, work through the decision tree to arrive at the correct answer.
# MAGIC
# MAGIC ### Decision Tree
# MAGIC ```
# MAGIC 1. Is source a DATABASE or SaaS API?
# MAGIC    YES -> Is it a well-known SaaS app (Salesforce, Workday, ServiceNow)?
# MAGIC           YES -> Use Lakeflow Connect (managed connector)
# MAGIC           NO  -> Use Lakeflow Connect (standard JDBC connector)
# MAGIC    NO  -> Continue to 2
# MAGIC
# MAGIC 2. Is source FILES in cloud storage?
# MAGIC    YES -> Continue to 3
# MAGIC    NO  -> Use custom code or partner connector
# MAGIC
# MAGIC 3. Do you need STREAMING ingestion (< 5 minute latency)?
# MAGIC    YES -> Use Auto Loader
# MAGIC    NO  -> Continue to 4
# MAGIC
# MAGIC 4. Do you need SCHEMA EVOLUTION or handle high file volumes (> 100 files/hour)?
# MAGIC    YES -> Use Auto Loader
# MAGIC    NO  -> Use COPY INTO (simpler for infrequent batch)
# MAGIC ```
# MAGIC
# MAGIC ### Apply the Tree to These Scenarios
# MAGIC
# MAGIC **Scenario A**: Ingest from Microsoft Dynamics CRM (not a pre-built managed connector available)
# MAGIC
# MAGIC **Scenario B**: CSV files land in GCS twice daily, simple schema, low volume
# MAGIC
# MAGIC **Scenario C**: Parquet files arrive in S3 every 30 seconds, need 1-minute latency
# MAGIC
# MAGIC **Scenario D**: Oracle database, 500 GB table, daily refresh
# MAGIC
# MAGIC **Scenario E**: JSON files in ADLS, schema changes weekly, 500 files/hour
# MAGIC
# MAGIC **Scenario F**: HubSpot marketing data, need hourly sync
# MAGIC
# MAGIC **Your Answers**:
# MAGIC
# MAGIC For each scenario, document your decision tree path:
# MAGIC
# MAGIC ```
# MAGIC Scenario A:
# MAGIC Step 1: ?
# MAGIC Step 2 (if applicable): ?
# MAGIC Step 3 (if applicable): ?
# MAGIC Final Answer: ?
# MAGIC
# MAGIC Scenario B:
# MAGIC Step 1: ?
# MAGIC Step 2: ?
# MAGIC Step 3: ?
# MAGIC Step 4: ?
# MAGIC Final Answer: ?
# MAGIC
# MAGIC [Continue for C, D, E, F]
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Applied 2: Troubleshooting Decision Matrix
# MAGIC %md
# MAGIC ## Applied 2: Troubleshooting Decision Matrix
# MAGIC
# MAGIC **Exam Focus**: Systematically diagnose Lakeflow Connect issues
# MAGIC
# MAGIC **Instructions**: Use the troubleshooting matrix to diagnose each problem.
# MAGIC
# MAGIC ### Troubleshooting Matrix
# MAGIC
# MAGIC | Symptom | Possible Causes | Diagnostic Steps | Solutions |
# MAGIC |---------|----------------|------------------|------------|
# MAGIC | **Slow ingestion** | 1. No parallelization<br>2. Low fetch size<br>3. Network bottleneck<br>4. Source DB overloaded | Check: partition config, fetch size, network bandwidth, DB metrics | Enable parallelization, increase fetch size, optimize network, schedule off-peak |
# MAGIC | **Connection errors** | 1. Wrong credentials<br>2. Network/firewall issue<br>3. Host unreachable<br>4. SSL/TLS mismatch | Check: credentials in Secrets, network connectivity, host resolution, SSL config | Fix credentials, open firewall, verify host, configure SSL |
# MAGIC | **Schema errors** | 1. Schema evolution disabled<br>2. Type mismatch<br>3. Wrong target table | Check: schema evolution setting, source/target types, table location | Enable evolution, fix type mismatch, correct target |
# MAGIC | **Rate limits** | 1. Too frequent sync<br>2. Too many objects<br>3. API quota exceeded | Check: sync frequency, object count, API logs | Reduce frequency, select fewer objects, contact provider |
# MAGIC
# MAGIC ### Problems to Diagnose
# MAGIC
# MAGIC **Problem 1**:
# MAGIC ```
# MAGIC Pipeline: MySQL -> Unity Catalog
# MAGIC Symptom: Takes 10 hours for 20 GB table
# MAGIC Config: Single connection, default settings
# MAGIC Source: Allows 16 concurrent connections
# MAGIC ```
# MAGIC
# MAGIC **Problem 2**:
# MAGIC ```
# MAGIC Pipeline: Salesforce -> Unity Catalog
# MAGIC Symptom: Fails with "Invalid token" after 2 hours
# MAGIC Config: Uses OAuth token from 6 months ago
# MAGIC Frequency: Runs daily
# MAGIC ```
# MAGIC
# MAGIC **Problem 3**:
# MAGIC ```
# MAGIC Pipeline: PostgreSQL -> Unity Catalog
# MAGIC Symptom: Error "Column new_status not found"
# MAGIC Config: Schema evolution disabled
# MAGIC Source: DBA added column yesterday
# MAGIC ```
# MAGIC
# MAGIC **Problem 4**:
# MAGIC ```
# MAGIC Pipeline: ServiceNow -> Unity Catalog  
# MAGIC Symptom: Rate limit exceeded, sync fails
# MAGIC Config: Syncs every 10 minutes, 15 objects
# MAGIC Records: 1M total
# MAGIC ```
# MAGIC
# MAGIC **Your Diagnosis**:
# MAGIC
# MAGIC For each problem, use the matrix:
# MAGIC
# MAGIC ```
# MAGIC Problem 1:
# MAGIC Symptom category: ?
# MAGIC Most likely cause: ?
# MAGIC Diagnostic steps to confirm: ?
# MAGIC Recommended solution: ?
# MAGIC Expected improvement: ?
# MAGIC
# MAGIC Problem 2:
# MAGIC Symptom category: ?
# MAGIC Most likely cause: ?
# MAGIC Diagnostic steps: ?
# MAGIC Solution: ?
# MAGIC Why it happens: ?
# MAGIC
# MAGIC [Continue for Problems 3 and 4]
# MAGIC ```
# MAGIC
# MAGIC ### Bonus Question
# MAGIC
# MAGIC Rank these optimizations by impact (1 = highest impact):
# MAGIC * A. Enable parallelization (1 -> 8 partitions)
# MAGIC * B. Increase fetch size (100 -> 10,000 rows)
# MAGIC * C. Use custom query to filter 50% of rows at source
# MAGIC * D. Change from full load to incremental append
# MAGIC * E. Upgrade network bandwidth (100 Mbps -> 1 Gbps)
# MAGIC
# MAGIC **Your ranking**: ?
# MAGIC
# MAGIC **Justification**: ?

# Databricks notebook source
# DBTITLE 1,Topic 8: Lakeflow Connect
# MAGIC %md
# MAGIC # Topic 8: Lakeflow Connect
# MAGIC
# MAGIC **Exam Weight: Section 2 (Data Ingestion and Loading) - 21%**
# MAGIC
# MAGIC Lakeflow Connect is Databricks' managed ingestion service for reliably loading data from diverse enterprise sources into Unity Catalog tables. This is the highest-weighted section on the exam, and Lakeflow Connect is explicitly called out in multiple exam objectives.
# MAGIC
# MAGIC ## Why This Topic Matters for the Exam
# MAGIC
# MAGIC Lakeflow Connect questions test your ability to:
# MAGIC 1. Choose the correct ingestion method for a given source and requirement
# MAGIC 2. Configure standard connectors for relational databases
# MAGIC 3. Understand managed connector capabilities for SaaS applications
# MAGIC 4. Handle semi-structured and nested data during ingestion
# MAGIC 5. Distinguish when Lakeflow Connect is appropriate vs Auto Loader or COPY INTO
# MAGIC
# MAGIC **Critical exam trap**: The exam will present scenarios where multiple ingestion methods could work, but only one is optimal. You must know the decision criteria.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exam Objectives Covered
# MAGIC
# MAGIC From the official exam guide:
# MAGIC
# MAGIC * Enable and detail data ingestion patterns, including batch, streaming, and incremental loading
# MAGIC * Import data from sources such as local files, Lakeflow Connect standard connectors, and Lakeflow Connect managed connectors
# MAGIC * Configure Lakeflow Connect to reliably ingest data from diverse enterprise sources into Unity Catalog-governed tables
# MAGIC * Prioritize between Auto Loader, Lakeflow Connect (standard and managed connectors), partner connectors, and other ingestion methods based on technical requirements
# MAGIC * Ingest semi-structured and unstructured data (JSON, nested data) via Lakeflow Connect into Unity Catalog Delta tables
# MAGIC * Use JDBC/ODBC or REST clients in notebooks to land data into cloud storage or directly into Unity Catalog tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Topic Coverage
# MAGIC
# MAGIC 1. **Lakeflow Connect Architecture**: Standard vs managed connectors, data landing zones, ingestion patterns
# MAGIC 2. **Standard Connectors**: JDBC/ODBC configuration for relational databases (MySQL, PostgreSQL, SQL Server, Oracle)
# MAGIC 3. **Managed Connectors**: SaaS application ingestion (Salesforce, Workday, ServiceNow, HubSpot, NetSuite)
# MAGIC 4. **Decision Matrix**: When to use Lakeflow Connect vs Auto Loader vs COPY INTO vs partner connectors
# MAGIC 5. **Semi-Structured Data**: Handling JSON, nested schemas, schema evolution
# MAGIC 6. **Configuration Patterns**: Connection secrets, incremental loading strategies, scheduling
# MAGIC 7. **Exam Traps**: Common mistakes in connector selection and configuration

# COMMAND ----------

# DBTITLE 1,Lakeflow Connect Architecture
# MAGIC %md
# MAGIC ## Lakeflow Connect Architecture
# MAGIC
# MAGIC ### What Is Lakeflow Connect?
# MAGIC
# MAGIC Lakeflow Connect is Databricks' fully managed data ingestion service that connects to external data sources and loads data into Unity Catalog tables. It handles authentication, incremental loading, schema evolution, and fault tolerance without requiring you to write ingestion code.
# MAGIC
# MAGIC **Key characteristic**: Lakeflow Connect is a **managed service**, not a library or API. You configure connections through the UI or API, and Databricks runs the ingestion infrastructure.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Two Types of Connectors
# MAGIC
# MAGIC #### 1. Standard Connectors
# MAGIC
# MAGIC **What they are**: Generic JDBC/ODBC connectors that work with any relational database or JDBC-compliant source.
# MAGIC
# MAGIC **Common sources**:
# MAGIC * MySQL, PostgreSQL, SQL Server, Oracle, DB2
# MAGIC * Snowflake, Redshift, BigQuery (via JDBC)
# MAGIC * Any database with a JDBC driver
# MAGIC
# MAGIC **How they work**:
# MAGIC 1. You provide connection details (host, port, credentials, database/schema)
# MAGIC 2. You specify which tables or SQL queries to ingest
# MAGIC 3. Lakeflow Connect uses JDBC to extract data
# MAGIC 4. Data lands in Unity Catalog Delta tables
# MAGIC
# MAGIC **Configuration requirements**:
# MAGIC * JDBC connection string
# MAGIC * Credentials (stored in Databricks Secrets)
# MAGIC * Source table/query specification
# MAGIC * Incremental loading column (optional, for CDC)
# MAGIC
# MAGIC **Exam trap**: Standard connectors require you to know the source schema. They don't auto-discover tables (you must specify which tables to ingest).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Managed Connectors
# MAGIC
# MAGIC **What they are**: Pre-built, application-specific connectors for popular SaaS platforms. Databricks maintains these connectors and handles API changes.
# MAGIC
# MAGIC **Common sources**:
# MAGIC * **Salesforce**: CRM data (accounts, opportunities, leads, custom objects)
# MAGIC * **Workday**: HR and financial data
# MAGIC * **ServiceNow**: ITSM tickets and incident data
# MAGIC * **HubSpot**: Marketing and sales data
# MAGIC * **NetSuite**: ERP data
# MAGIC * **Google Ads, Meta Ads**: Advertising campaign data
# MAGIC * **Jira, Confluence**: Project management and documentation
# MAGIC
# MAGIC **How they work**:
# MAGIC 1. You authenticate with the SaaS application (OAuth or API token)
# MAGIC 2. You select which objects/entities to sync (e.g., Salesforce Accounts, Opportunities)
# MAGIC 3. Lakeflow Connect uses the application's API to extract data
# MAGIC 4. Data lands in Unity Catalog Delta tables with normalized schemas
# MAGIC
# MAGIC **Key advantages over standard connectors**:
# MAGIC * **No schema knowledge required**: Connector knows the application's data model
# MAGIC * **API rate limit handling**: Automatically throttles requests
# MAGIC * **Incremental sync**: Uses application's change tracking (e.g., LastModifiedDate)
# MAGIC * **Nested object handling**: Automatically flattens complex API responses
# MAGIC
# MAGIC **Exam trap**: Managed connectors are **not real-time**. They run on a schedule (typically hourly or daily). For near-real-time ingestion from SaaS apps, you'd use partner connectors (Fivetran, Airbyte) or custom code.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Data Flow Architecture
# MAGIC
# MAGIC ```
# MAGIC Source System                  Lakeflow Connect              Unity Catalog
# MAGIC
# MAGIC [Database/SaaS] -----> [Connector Config] -----> [Landing Zone] -----> [Delta Table]
# MAGIC      ^                        ^                        ^
# MAGIC      |                        |                        |
# MAGIC   JDBC/API            Secrets/Credentials        Schema Evolution
# MAGIC ```
# MAGIC
# MAGIC **Landing zone**: Lakeflow Connect may use temporary cloud storage (S3/ADLS/GCS) as an intermediate staging area before writing to Delta tables. This is transparent to you.
# MAGIC
# MAGIC **Schema evolution**: Lakeflow Connect can automatically handle schema changes in the source:
# MAGIC * New columns appear in source → added to Delta table
# MAGIC * Columns removed in source → existing columns remain (null values for new rows)
# MAGIC * Data type changes → may require manual intervention
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Ingestion Patterns
# MAGIC
# MAGIC Lakeflow Connect supports three ingestion patterns:
# MAGIC
# MAGIC #### 1. Full Load (Snapshot)
# MAGIC
# MAGIC **What it does**: Copies the entire table on every run.
# MAGIC
# MAGIC **When to use**:
# MAGIC * Small reference tables (< 1GB)
# MAGIC * Source doesn't support incremental tracking
# MAGIC * Data freshness requirements allow periodic full refreshes
# MAGIC
# MAGIC **Exam trap**: Full load is NOT incremental. If a table has 10M rows, every sync reads 10M rows, even if only 100 changed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Incremental Load (Append)
# MAGIC
# MAGIC **What it does**: Copies only new rows since the last sync, using a watermark column (typically a timestamp or auto-increment ID).
# MAGIC
# MAGIC **How it works**:
# MAGIC 1. First run: Copy all rows, record max watermark value
# MAGIC 2. Subsequent runs: Copy rows where watermark > last recorded value
# MAGIC
# MAGIC **Watermark column requirements**:
# MAGIC * Monotonically increasing (timestamp, auto-increment integer)
# MAGIC * Never updated (immutable)
# MAGIC * Never null
# MAGIC
# MAGIC **When to use**:
# MAGIC * Append-only sources (logs, events, transactions)
# MAGIC * Source has a reliable watermark column
# MAGIC
# MAGIC **Exam scenario**: "A table has created_at (immutable) and updated_at (changes on updates). Which column should you use for incremental ingestion?"
# MAGIC * **Answer**: `created_at` - captures new rows only
# MAGIC * **Why not updated_at**: Changes on updates, so you'd miss updated rows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3. Incremental Load (Merge/Upsert)
# MAGIC
# MAGIC **What it does**: Captures inserts AND updates using a watermark column. On each sync, performs a MERGE operation (upsert) into the target Delta table.
# MAGIC
# MAGIC **How it works**:
# MAGIC 1. Extract rows where watermark > last sync
# MAGIC 2. Perform MERGE on target table using primary key
# MAGIC 3. Update existing rows, insert new rows
# MAGIC
# MAGIC **Requirements**:
# MAGIC * Watermark column (tracks changes)
# MAGIC * Primary key column(s) (for matching rows)
# MAGIC
# MAGIC **When to use**:
# MAGIC * Source tables with updates (not just inserts)
# MAGIC * You need current state, not full history
# MAGIC
# MAGIC **Exam trap**: Upsert mode requires BOTH a watermark column AND a primary key. If source doesn't have a reliable watermark, you can't use incremental upsert.
# MAGIC
# MAGIC **Alternative for CDC**: Some databases support Change Data Capture (CDC) logs. Lakeflow Connect can consume CDC streams for true log-based replication. This is more reliable than watermark-based incremental loading.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Comparison: Standard vs Managed Connectors
# MAGIC
# MAGIC | Aspect | Standard Connectors | Managed Connectors |
# MAGIC |--------|---------------------|--------------------|
# MAGIC | **Source types** | Any JDBC database | Specific SaaS apps |
# MAGIC | **Schema knowledge** | You must specify tables/queries | Auto-discovered |
# MAGIC | **Authentication** | JDBC connection string + credentials | OAuth or API token |
# MAGIC | **API rate limits** | Your responsibility to handle | Handled automatically |
# MAGIC | **Incremental loading** | You specify watermark column | Uses app's change tracking |
# MAGIC | **Nested data** | You handle flattening | Auto-flattened |
# MAGIC | **Maintenance** | You update JDBC drivers | Databricks maintains |
# MAGIC | **Customization** | Full control via SQL queries | Limited to connector capabilities |
# MAGIC
# MAGIC **Exam scenario**: "You need to ingest data from a custom internal database (not a SaaS app). Which connector type?"
# MAGIC * **Answer**: Standard connector (JDBC)
# MAGIC * **Why**: Managed connectors only work with specific SaaS applications
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Scheduling and Orchestration
# MAGIC
# MAGIC Lakeflow Connect pipelines can be triggered:
# MAGIC
# MAGIC 1. **Scheduled (time-based)**: Run every N hours/days (e.g., daily at 2 AM)
# MAGIC 2. **On-demand**: Manually triggered via UI or API
# MAGIC 3. **Orchestrated via Lakeflow Jobs**: Include as a task in a job workflow
# MAGIC
# MAGIC **Exam trap**: Lakeflow Connect pipelines are NOT event-driven. They don't trigger automatically when source data changes. For event-driven ingestion, use Auto Loader with file notifications or partner streaming connectors.

# COMMAND ----------

# DBTITLE 1,Standard Connectors - JDBC/ODBC Configuration
# MAGIC %md
# MAGIC ## Standard Connectors: JDBC/ODBC Configuration
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Standard connectors use JDBC to extract data from relational databases. You configure the connection, specify tables or queries, and Lakeflow Connect handles the extraction and loading.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Configuration Components
# MAGIC
# MAGIC #### 1. Connection Details
# MAGIC
# MAGIC **Required parameters**:
# MAGIC * **Host**: Database server hostname or IP
# MAGIC * **Port**: Database port (e.g., 3306 for MySQL, 5432 for PostgreSQL)
# MAGIC * **Database/Schema**: Target database or schema name
# MAGIC * **Username/Password**: Stored in Databricks Secrets
# MAGIC
# MAGIC **JDBC connection string format**:
# MAGIC ```
# MAGIC jdbc:<database_type>://<host>:<port>/<database>
# MAGIC ```
# MAGIC
# MAGIC **Examples**:
# MAGIC * MySQL: `jdbc:mysql://db.example.com:3306/sales_db`
# MAGIC * PostgreSQL: `jdbc:postgresql://pg.example.com:5432/analytics`
# MAGIC * SQL Server: `jdbc:sqlserver://sqlserver.example.com:1433;databaseName=crm`
# MAGIC * Oracle: `jdbc:oracle:thin:@oracle.example.com:1521:ORCL`
# MAGIC
# MAGIC **Exam trap**: Different databases use different connection string formats. SQL Server uses semicolons, Oracle uses colons. Know the common formats.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Credential Management
# MAGIC
# MAGIC **Best practice**: Store credentials in Databricks Secrets, never hardcode.
# MAGIC
# MAGIC **Setup process**:
# MAGIC 1. Create a secret scope (via CLI or UI)
# MAGIC 2. Store username and password as separate secrets
# MAGIC 3. Reference secrets in connector configuration
# MAGIC
# MAGIC **Example secret references**:
# MAGIC ```python
# MAGIC username = dbutils.secrets.get(scope="jdbc_prod", key="mysql_username")
# MAGIC password = dbutils.secrets.get(scope="jdbc_prod", key="mysql_password")
# MAGIC ```
# MAGIC
# MAGIC **Exam scenario**: "How should you provide database credentials to Lakeflow Connect?"
# MAGIC * **Correct**: Store in Databricks Secrets
# MAGIC * **Incorrect**: Hardcode in connection string, store in notebook variables, pass as job parameters
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3. Table/Query Specification
# MAGIC
# MAGIC **Two approaches**:
# MAGIC
# MAGIC ##### Option A: Table Name
# MAGIC
# MAGIC Specify a table to ingest in full:
# MAGIC ```sql
# MAGIC SELECT * FROM sales.orders
# MAGIC ```
# MAGIC
# MAGIC **When to use**: Ingesting entire tables without transformation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ##### Option B: Custom Query
# MAGIC
# MAGIC Provide a SQL query that Lakeflow Connect will execute:
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_date,
# MAGIC     total_amount
# MAGIC FROM sales.orders
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC     AND status != 'cancelled'
# MAGIC ```
# MAGIC
# MAGIC **When to use**:
# MAGIC * Filter rows at source (reduce data transfer)
# MAGIC * Select specific columns
# MAGIC * Perform lightweight transformations (CAST, string operations)
# MAGIC * Join multiple source tables
# MAGIC
# MAGIC **Exam trap**: Custom queries must be valid SQL for the SOURCE database, not Spark SQL. If the source is SQL Server, use SQL Server syntax (not Databricks SQL syntax).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4. Incremental Loading Configuration
# MAGIC
# MAGIC **For append-only incremental loading**:
# MAGIC
# MAGIC 1. **Identify watermark column**: Timestamp or auto-increment ID that tracks new rows
# MAGIC 2. **Specify column in configuration**: Lakeflow Connect tracks the maximum value
# MAGIC 3. **Subsequent runs**: Only extract rows where `watermark_column > last_max_value`
# MAGIC
# MAGIC **Example configuration** (pseudo-syntax):
# MAGIC ```yaml
# MAGIC source_table: sales.orders
# MAGIC watermark_column: created_at
# MAGIC watermark_type: timestamp
# MAGIC ```
# MAGIC
# MAGIC **First run**:
# MAGIC ```sql
# MAGIC SELECT * FROM sales.orders
# MAGIC -- Records max(created_at) = '2024-06-08 15:30:00'
# MAGIC ```
# MAGIC
# MAGIC **Second run**:
# MAGIC ```sql
# MAGIC SELECT * FROM sales.orders
# MAGIC WHERE created_at > '2024-06-08 15:30:00'
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **For upsert (merge) incremental loading**:
# MAGIC
# MAGIC 1. **Identify watermark column**: Tracks changes (e.g., `updated_at`)
# MAGIC 2. **Identify primary key**: Unique identifier for matching rows (e.g., `order_id`)
# MAGIC 3. **Lakeflow Connect performs MERGE**: Updates existing rows, inserts new rows
# MAGIC
# MAGIC **Example configuration**:
# MAGIC ```yaml
# MAGIC source_table: sales.orders
# MAGIC watermark_column: updated_at
# MAGIC primary_key: order_id
# MAGIC merge_mode: upsert
# MAGIC ```
# MAGIC
# MAGIC **Merge logic** (behind the scenes):
# MAGIC ```sql
# MAGIC MERGE INTO target_table AS t
# MAGIC USING source_table AS s
# MAGIC ON t.order_id = s.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC **Exam trap**: Upsert mode is slower than append mode because it requires reading both source incremental data AND target table for matching. Use upsert only when you need to capture updates.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Performance Considerations
# MAGIC
# MAGIC #### 1. Parallelization
# MAGIC
# MAGIC Lakeflow Connect can parallelize JDBC reads by partitioning the source table:
# MAGIC
# MAGIC **Partition configuration**:
# MAGIC * **Partition column**: Numeric column (integer, bigint) used to split reads
# MAGIC * **Number of partitions**: How many parallel reads
# MAGIC * **Lower/upper bounds**: Min and max values of partition column
# MAGIC
# MAGIC **How it works**:
# MAGIC * If `num_partitions = 4`, `partition_column = id`, `lower_bound = 1`, `upper_bound = 1000`
# MAGIC * Lakeflow Connect generates 4 queries:
# MAGIC   * Partition 1: `WHERE id >= 1 AND id < 250`
# MAGIC   * Partition 2: `WHERE id >= 250 AND id < 500`
# MAGIC   * Partition 3: `WHERE id >= 500 AND id < 750`
# MAGIC   * Partition 4: `WHERE id >= 750 AND id <= 1000`
# MAGIC
# MAGIC **When to use**: Large tables (> 1GB) with a numeric partitioning column.
# MAGIC
# MAGIC **Exam trap**: Partitioning requires a NUMERIC column. You can't partition on strings or timestamps.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Fetch Size
# MAGIC
# MAGIC **What it is**: Number of rows fetched per JDBC round trip.
# MAGIC
# MAGIC **Default**: Varies by database (typically 10-100 rows)
# MAGIC
# MAGIC **Tuning**:
# MAGIC * Increase fetch size for large tables (e.g., 10,000 rows) to reduce round trips
# MAGIC * Decrease fetch size if running out of memory
# MAGIC
# MAGIC **Configuration**:
# MAGIC ```python
# MAGIC fetch_size = 10000
# MAGIC ```
# MAGIC
# MAGIC **Exam scenario**: "A JDBC ingestion is slow despite parallelization. What might improve performance?"
# MAGIC * **Answer**: Increase fetch size to reduce JDBC round trips
# MAGIC * **Alternative**: Increase number of partitions (if not already maxed)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3. Predicate Pushdown
# MAGIC
# MAGIC When you specify a custom query with a WHERE clause, the filtering happens at the SOURCE database (not in Spark). This reduces data transfer.
# MAGIC
# MAGIC **Example**:
# MAGIC ```sql
# MAGIC SELECT * FROM sales.orders
# MAGIC WHERE order_date >= '2024-01-01'
# MAGIC ```
# MAGIC
# MAGIC **What happens**: Source database filters rows, only matching rows are sent to Databricks.
# MAGIC
# MAGIC **Exam trap**: If you ingest the full table and THEN filter in Spark, you've transferred unnecessary data. Always filter at source when possible.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Common Database-Specific Configurations
# MAGIC
# MAGIC #### MySQL
# MAGIC
# MAGIC **Connection string**:
# MAGIC ```
# MAGIC jdbc:mysql://host:3306/database?useSSL=true&serverTimezone=UTC
# MAGIC ```
# MAGIC
# MAGIC **Common parameters**:
# MAGIC * `useSSL=true`: Encrypt connection
# MAGIC * `serverTimezone=UTC`: Set timezone for TIMESTAMP columns
# MAGIC * `zeroDateTimeBehavior=convertToNull`: Handle invalid '0000-00-00' dates
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### PostgreSQL
# MAGIC
# MAGIC **Connection string**:
# MAGIC ```
# MAGIC jdbc:postgresql://host:5432/database?ssl=true
# MAGIC ```
# MAGIC
# MAGIC **Common parameters**:
# MAGIC * `ssl=true`: Encrypt connection
# MAGIC * `currentSchema=myschema`: Default schema for queries
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### SQL Server
# MAGIC
# MAGIC **Connection string**:
# MAGIC ```
# MAGIC jdbc:sqlserver://host:1433;databaseName=mydb;encrypt=true
# MAGIC ```
# MAGIC
# MAGIC **Common parameters**:
# MAGIC * `encrypt=true`: Encrypt connection
# MAGIC * `trustServerCertificate=true`: Skip certificate validation (dev only)
# MAGIC
# MAGIC **Exam trap**: SQL Server uses semicolons in connection strings, not question marks.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Oracle
# MAGIC
# MAGIC **Connection string**:
# MAGIC ```
# MAGIC jdbc:oracle:thin:@host:1521:SID
# MAGIC ```
# MAGIC
# MAGIC OR for service name:
# MAGIC ```
# MAGIC jdbc:oracle:thin:@//host:1521/servicename
# MAGIC ```
# MAGIC
# MAGIC **Exam trap**: Oracle uses SID or service name, not database name. Connection string format is different.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exam Decision Scenarios
# MAGIC
# MAGIC **Scenario 1**: "You need to ingest 100GB of historical order data from MySQL. Ingestion is taking 8 hours. How do you speed it up?"
# MAGIC
# MAGIC **Answer**: Enable parallelization by specifying:
# MAGIC * Partition column (e.g., `order_id`)
# MAGIC * Number of partitions (e.g., 16 or 32)
# MAGIC * Increase fetch size to 10,000-50,000 rows
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Scenario 2**: "Source database allows only 5 concurrent connections. You've configured 10 partitions. What happens?"
# MAGIC
# MAGIC **Answer**: Ingestion will fail or be throttled. Reduce partitions to 5 or fewer.
# MAGIC
# MAGIC **Exam lesson**: Respect source database connection limits. Lakeflow Connect parallelization uses multiple connections.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Scenario 3**: "You're ingesting from PostgreSQL. The source table has a `last_modified` timestamp that updates on every row change. You need to capture updates. How do you configure incremental loading?"
# MAGIC
# MAGIC **Answer**: 
# MAGIC * Watermark column: `last_modified`
# MAGIC * Primary key: Table's primary key (e.g., `order_id`)
# MAGIC * Merge mode: Upsert
# MAGIC
# MAGIC **Why**: `last_modified` tracks changes, primary key enables matching for MERGE.

# COMMAND ----------

# DBTITLE 1,Managed Connectors - SaaS Integration
# MAGIC %md
# MAGIC ## Managed Connectors: SaaS Integration
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Managed connectors are pre-built integrations for popular SaaS applications. Databricks maintains these connectors, handles API changes, and provides normalized schemas.
# MAGIC
# MAGIC **Key advantage**: You don't need to understand the SaaS application's API or data model. The connector does the heavy lifting.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Supported SaaS Applications
# MAGIC
# MAGIC #### 1. Salesforce
# MAGIC
# MAGIC **What it ingests**: CRM data (accounts, contacts, opportunities, leads, custom objects)
# MAGIC
# MAGIC **Authentication**: OAuth 2.0 or username/password + security token
# MAGIC
# MAGIC **Incremental sync**: Uses `LastModifiedDate` or `SystemModstamp` fields
# MAGIC
# MAGIC **Common objects**:
# MAGIC * `Account`: Customer accounts
# MAGIC * `Contact`: Individual contacts
# MAGIC * `Opportunity`: Sales opportunities
# MAGIC * `Lead`: Sales leads
# MAGIC * `Case`: Support cases
# MAGIC * Custom objects (e.g., `CustomProduct__c`)
# MAGIC
# MAGIC **Schema handling**: Salesforce has dynamic schemas (custom fields). Lakeflow Connect auto-discovers fields and handles schema evolution.
# MAGIC
# MAGIC **Exam scenario**: "You need to ingest Salesforce Opportunity data, including custom fields created by your sales team. How do you ensure custom fields are captured?"
# MAGIC * **Answer**: Managed Salesforce connector auto-discovers all fields, including custom
# MAGIC * **Wrong answer**: Use standard JDBC connector (Salesforce API requires specialized authentication)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 2. Workday
# MAGIC
# MAGIC **What it ingests**: HR and financial data (employees, payroll, expenses, budgets)
# MAGIC
# MAGIC **Authentication**: OAuth 2.0 or username/password
# MAGIC
# MAGIC **Common objects**:
# MAGIC * `Worker`: Employee records
# MAGIC * `Organization`: Department/team structure
# MAGIC * `Position`: Job positions
# MAGIC * `Compensation`: Salary and bonus data
# MAGIC
# MAGIC **Exam trap**: Workday data is heavily nested (XML-like structure). The managed connector flattens nested objects into columnar Delta tables.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 3. ServiceNow
# MAGIC
# MAGIC **What it ingests**: ITSM data (incidents, requests, changes, assets)
# MAGIC
# MAGIC **Authentication**: Basic auth or OAuth 2.0
# MAGIC
# MAGIC **Common tables**:
# MAGIC * `incident`: Support tickets
# MAGIC * `change_request`: Change management records
# MAGIC * `cmdb_ci`: Configuration items (IT assets)
# MAGIC * `sys_user`: User accounts
# MAGIC
# MAGIC **Incremental sync**: Uses `sys_updated_on` field
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 4. HubSpot
# MAGIC
# MAGIC **What it ingests**: Marketing and sales data (contacts, companies, deals, emails)
# MAGIC
# MAGIC **Authentication**: API key or OAuth 2.0
# MAGIC
# MAGIC **Common objects**:
# MAGIC * `contacts`: Marketing contacts
# MAGIC * `companies`: Organizations
# MAGIC * `deals`: Sales deals
# MAGIC * `engagements`: Email interactions
# MAGIC
# MAGIC **Exam scenario**: "You're analyzing email campaign effectiveness. Which HubSpot object contains email interaction data?"
# MAGIC * **Answer**: `engagements` object (tracks emails, calls, meetings)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 5. NetSuite
# MAGIC
# MAGIC **What it ingests**: ERP data (transactions, customers, inventory, financials)
# MAGIC
# MAGIC **Authentication**: Token-based authentication (TBA)
# MAGIC
# MAGIC **Common records**:
# MAGIC * `Customer`: Customer master data
# MAGIC * `SalesOrder`: Sales transactions
# MAGIC * `Invoice`: Invoices
# MAGIC * `Item`: Product catalog
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### 6. Google Ads / Meta Ads
# MAGIC
# MAGIC **What they ingest**: Advertising campaign data (impressions, clicks, conversions, costs)
# MAGIC
# MAGIC **Authentication**: OAuth 2.0
# MAGIC
# MAGIC **Common metrics**:
# MAGIC * Campaign performance (impressions, clicks, CTR)
# MAGIC * Ad group metrics
# MAGIC * Keyword performance
# MAGIC * Conversion tracking
# MAGIC
# MAGIC **Exam trap**: Ad platform APIs have rate limits and attribution windows. Managed connectors handle these automatically.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Managed Connector Workflow
# MAGIC
# MAGIC #### Step 1: Authenticate
# MAGIC
# MAGIC **OAuth flow** (most common):
# MAGIC 1. Click "Connect" in Databricks UI
# MAGIC 2. Redirect to SaaS application login
# MAGIC 3. Grant permissions
# MAGIC 4. Databricks stores OAuth token securely
# MAGIC
# MAGIC **API key flow** (alternative):
# MAGIC 1. Generate API key in SaaS application
# MAGIC 2. Paste into Databricks connector configuration
# MAGIC 3. Databricks stores key in Secrets
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Step 2: Select Objects
# MAGIC
# MAGIC **Example (Salesforce)**:
# MAGIC * Select: `Account`, `Opportunity`, `Contact`
# MAGIC * Deselect: `Lead`, `Case` (not needed for this pipeline)
# MAGIC
# MAGIC **Exam trap**: You can't SELECT specific fields with managed connectors. You get ALL fields for the objects you select. For field-level filtering, use a downstream transformation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Step 3: Configure Incremental Sync
# MAGIC
# MAGIC **Automatic for most connectors**: Uses application's built-in change tracking.
# MAGIC
# MAGIC **Examples**:
# MAGIC * Salesforce: `LastModifiedDate` or `SystemModstamp`
# MAGIC * ServiceNow: `sys_updated_on`
# MAGIC * HubSpot: `hs_lastmodifieddate`
# MAGIC
# MAGIC **Full refresh option**: Some connectors support full snapshot mode (re-ingest all data). Use for small tables or when incremental sync is unreliable.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Step 4: Set Schedule
# MAGIC
# MAGIC **Common schedules**:
# MAGIC * Hourly: Near-real-time ingestion
# MAGIC * Daily: Overnight batch loads
# MAGIC * Weekly: Low-volume sources
# MAGIC
# MAGIC **Exam trap**: Managed connectors are NOT real-time. Minimum frequency is typically hourly. For sub-minute latency, you need partner streaming connectors or custom code.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Schema Handling
# MAGIC
# MAGIC #### Automatic Flattening
# MAGIC
# MAGIC Managed connectors flatten nested API responses into columnar format.
# MAGIC
# MAGIC **Example (Salesforce Opportunity)**:
# MAGIC
# MAGIC **API response** (JSON):
# MAGIC ```json
# MAGIC {
# MAGIC   "Id": "006...",
# MAGIC   "Name": "Acme Corp Deal",
# MAGIC   "Amount": 100000,
# MAGIC   "Owner": {
# MAGIC     "Id": "005...",
# MAGIC     "Name": "John Smith",
# MAGIC     "Email": "john@example.com"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Flattened Delta table**:
# MAGIC ```
# MAGIC Id         | Name            | Amount  | Owner.Id | Owner.Name  | Owner.Email
# MAGIC 006...     | Acme Corp Deal  | 100000  | 005...   | John Smith  | john@example.com
# MAGIC ```
# MAGIC
# MAGIC **Exam lesson**: Nested objects become dot-notation columns (`Owner.Name`). You don't need to write JSON parsing code.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Schema Evolution
# MAGIC
# MAGIC When SaaS applications add new fields:
# MAGIC
# MAGIC 1. **Managed connector detects new field** (on next sync)
# MAGIC 2. **Adds column to Delta table** (schema evolution)
# MAGIC 3. **Existing rows have NULL** for new column
# MAGIC 4. **New rows populate the field**
# MAGIC
# MAGIC **Configuration**: Schema evolution is typically enabled by default. You can disable it if you want strict schema enforcement.
# MAGIC
# MAGIC **Exam scenario**: "Your Salesforce admin adds a custom field `ExpectedCloseDate__c` to Opportunity. How do you ensure it's captured in Databricks?"
# MAGIC * **Answer**: Schema evolution is automatic. Next sync will add the column.
# MAGIC * **Wrong answer**: Reconfigure connector, recreate table, run manual ALTER TABLE
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exam Decision Scenarios
# MAGIC
# MAGIC **Scenario 1**: "You need to ingest Salesforce data. You're considering: (A) Managed Salesforce connector, (B) Standard JDBC connector, (C) Custom Python API calls. Which is correct?"
# MAGIC
# MAGIC **Answer**: (A) Managed Salesforce connector
# MAGIC
# MAGIC **Why**:
# MAGIC * JDBC doesn't work with Salesforce (requires REST API)
# MAGIC * Custom API calls require handling authentication, pagination, rate limits, schema changes
# MAGIC * Managed connector handles all of this
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Scenario 2**: "You need to ingest ServiceNow incident data updated within the last hour. How frequently should you schedule the pipeline?"
# MAGIC
# MAGIC **Answer**: Hourly
# MAGIC
# MAGIC **Why**: Managed connectors support hourly as the minimum frequency. For sub-hour latency, you'd use streaming partner connectors.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Scenario 3**: "You're ingesting HubSpot contact data. The HubSpot API returns nested properties (e.g., `properties.email`, `properties.firstname`). How do you handle this in Databricks?"
# MAGIC
# MAGIC **Answer**: The managed connector automatically flattens nested properties into columns
# MAGIC
# MAGIC **Wrong answer**: Use `from_json()` or `explode()` to parse JSON (not needed, connector does this)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Scenario 4**: "Your managed connector pipeline is failing with 'Rate limit exceeded' errors. What should you do?"
# MAGIC
# MAGIC **Answer**: Reduce sync frequency or contact Databricks support
# MAGIC
# MAGIC **Why**: Managed connectors should handle rate limits automatically, but if the source imposes stricter limits, you may need to reduce frequency. Standard connectors require YOU to handle rate limits (sleep, retry logic).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Limitations of Managed Connectors
# MAGIC
# MAGIC 1. **No custom transformations during ingestion**: You get raw data. Transform downstream.
# MAGIC 2. **Limited object filtering**: You select entire objects, not specific fields or rows.
# MAGIC 3. **Fixed sync frequency**: Can't trigger on source data changes.
# MAGIC 4. **No cross-object joins during ingestion**: Ingest objects separately, join in Spark.
# MAGIC 5. **Dependent on Databricks release cycle**: If SaaS app changes API, you wait for Databricks to update the connector.
# MAGIC
# MAGIC **Exam trap**: For advanced use cases (complex filtering, real-time ingestion, custom transformations), you may need partner connectors (Fivetran, Airbyte) or custom code. Managed connectors are for common patterns.

# COMMAND ----------

# DBTITLE 1,Decision Matrix - Choosing the Right Ingestion Method
# MAGIC %md
# MAGIC ## Decision Matrix: Choosing the Right Ingestion Method
# MAGIC
# MAGIC ### The Core Exam Question
# MAGIC
# MAGIC The exam will present scenarios and ask you to choose between:
# MAGIC 1. **Lakeflow Connect** (standard or managed)
# MAGIC 2. **Auto Loader**
# MAGIC 3. **COPY INTO**
# MAGIC 4. **Partner connectors** (Fivetran, Airbyte)
# MAGIC 5. **Custom code** (JDBC in notebooks, REST API calls)
# MAGIC
# MAGIC You must know the decision criteria for each.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Lakeflow Connect vs Auto Loader vs COPY INTO
# MAGIC
# MAGIC | Criteria | Lakeflow Connect | Auto Loader | COPY INTO |
# MAGIC |----------|------------------|-------------|------------|
# MAGIC | **Source type** | Databases, SaaS APIs | Cloud object storage files | Cloud object storage files |
# MAGIC | **Primary use case** | Pulling data from external systems | Ingesting files as they arrive | One-time or scheduled file loads |
# MAGIC | **Incremental loading** | Watermark-based or API change tracking | File-level tracking (checkpoints) | Manual tracking (file listing) |
# MAGIC | **Schema evolution** | Automatic (configurable) | Automatic (schema inference) | Manual (ALTER TABLE) |
# MAGIC | **Streaming support** | No (scheduled batch) | Yes (continuous streaming) | No (batch only) |
# MAGIC | **Fault tolerance** | Retry logic built-in | Exactly-once processing | Idempotent (reruns safe) |
# MAGIC | **Authentication** | Credentials/OAuth managed | IAM roles (cloud-native) | IAM roles (cloud-native) |
# MAGIC | **File format support** | N/A (reads from APIs/JDBC) | JSON, CSV, Parquet, Avro, ORC | JSON, CSV, Parquet, Avro, ORC, text |
# MAGIC | **Latency** | Hourly to daily | Near real-time (seconds) | Minutes to hours (scheduled) |
# MAGIC | **Complexity** | Low (UI configuration) | Medium (requires understanding of checkpoints) | Low (simple SQL command) |
# MAGIC | **Cost** | Databricks serverless compute | Databricks compute (cluster or serverless) | Databricks compute (cluster or serverless) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use Lakeflow Connect
# MAGIC
# MAGIC **Choose Lakeflow Connect when**:
# MAGIC
# MAGIC 1. **Source is a database or SaaS application**
# MAGIC    * MySQL, PostgreSQL, SQL Server, Oracle, Salesforce, Workday, ServiceNow
# MAGIC    * NOT files in S3/ADLS/GCS
# MAGIC
# MAGIC 2. **You want managed authentication and credential handling**
# MAGIC    * Lakeflow Connect stores credentials securely
# MAGIC    * Handles OAuth refresh tokens automatically
# MAGIC
# MAGIC 3. **You need automatic schema discovery** (for SaaS apps)
# MAGIC    * Managed connectors auto-discover objects and fields
# MAGIC    * No need to define schemas manually
# MAGIC
# MAGIC 4. **You want incremental loading without writing code**
# MAGIC    * Configure watermark column, Lakeflow Connect handles the rest
# MAGIC    * Automatic MERGE for upsert scenarios
# MAGIC
# MAGIC 5. **Hourly or daily latency is acceptable**
# MAGIC    * Not for real-time or sub-minute latency requirements
# MAGIC
# MAGIC **Exam scenario**: "You need to ingest data from an on-premises Oracle database into Unity Catalog. Which method?"
# MAGIC * **Answer**: Lakeflow Connect (standard JDBC connector)
# MAGIC * **Why**: Source is a database, not files. Auto Loader and COPY INTO only work with files.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use Auto Loader
# MAGIC
# MAGIC **Choose Auto Loader when**:
# MAGIC
# MAGIC 1. **Source is files landing in cloud storage**
# MAGIC    * S3, ADLS Gen2, GCS
# MAGIC    * Files arrive continuously or in batches
# MAGIC
# MAGIC 2. **You need streaming ingestion**
# MAGIC    * Process files as they arrive (seconds to minutes latency)
# MAGIC    * Continuous pipeline, not scheduled
# MAGIC
# MAGIC 3. **You need exactly-once processing guarantees**
# MAGIC    * Auto Loader tracks which files have been processed (checkpoint)
# MAGIC    * Reruns don't duplicate data
# MAGIC
# MAGIC 4. **Schema evolution is required**
# MAGIC    * Auto Loader infers schema from files
# MAGIC    * Handles new columns automatically (with `mergeSchema` or `rescuedDataColumn`)
# MAGIC
# MAGIC 5. **Source produces high file volumes**
# MAGIC    * Auto Loader is optimized for thousands of small files
# MAGIC    * Uses file notifications (queue-based) for efficiency
# MAGIC
# MAGIC **Exam scenario**: "JSON files land in S3 every 5 minutes. You need near-real-time ingestion with exactly-once guarantees. Which method?"
# MAGIC * **Answer**: Auto Loader
# MAGIC * **Why**: Files in S3, streaming requirement, exactly-once semantics. Lakeflow Connect doesn't work with files. COPY INTO is batch, not streaming.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use COPY INTO
# MAGIC
# MAGIC **Choose COPY INTO when**:
# MAGIC
# MAGIC 1. **Source is files in cloud storage**
# MAGIC    * S3, ADLS Gen2, GCS
# MAGIC    * NOT databases or APIs
# MAGIC
# MAGIC 2. **Files arrive infrequently or in large batches**
# MAGIC    * Daily, weekly, or monthly file drops
# MAGIC    * Not continuous streaming
# MAGIC
# MAGIC 3. **You want simple SQL-based ingestion**
# MAGIC    * Single SQL command, no checkpoint management
# MAGIC    * Easy to understand and debug
# MAGIC
# MAGIC 4. **Incremental loading is simple** (based on file names or paths)
# MAGIC    * COPY INTO tracks which files have been processed
# MAGIC    * Idempotent: rerunning is safe (doesn't duplicate)
# MAGIC
# MAGIC 5. **You don't need schema evolution** (or can handle manually)
# MAGIC    * Schema must match target table
# MAGIC    * New columns require ALTER TABLE
# MAGIC
# MAGIC **Exam scenario**: "A partner uploads a CSV file to S3 once per day. You need to load it into a Delta table. Which method?"
# MAGIC * **Answer**: COPY INTO
# MAGIC * **Why**: Daily batch load, simple use case. Auto Loader is overkill for daily files. Lakeflow Connect doesn't work with files.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use Partner Connectors (Fivetran, Airbyte)
# MAGIC
# MAGIC **Choose partner connectors when**:
# MAGIC
# MAGIC 1. **You need connectors Databricks doesn't provide**
# MAGIC    * Niche SaaS applications
# MAGIC    * Legacy systems
# MAGIC
# MAGIC 2. **You need real-time or sub-minute latency**
# MAGIC    * Partner connectors often support CDC streaming
# MAGIC    * Lakeflow Connect is hourly at best
# MAGIC
# MAGIC 3. **You need pre-built transformations during ingestion**
# MAGIC    * Fivetran normalizes nested JSON automatically
# MAGIC    * Airbyte supports custom transformations
# MAGIC
# MAGIC 4. **You want vendor-managed infrastructure**
# MAGIC    * Partner handles all connector maintenance
# MAGIC    * You pay for the service
# MAGIC
# MAGIC **Exam trap**: Partner connectors are NOT part of Databricks. They're third-party tools. For the exam, assume you're using Databricks-native tools unless explicitly stated.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### When to Use Custom Code (JDBC in Notebooks, REST APIs)
# MAGIC
# MAGIC **Choose custom code when**:
# MAGIC
# MAGIC 1. **You need complex transformation DURING ingestion**
# MAGIC    * Filter, aggregate, or join at source
# MAGIC    * Lakeflow Connect and Auto Loader don't transform during ingestion
# MAGIC
# MAGIC 2. **You need full control over ingestion logic**
# MAGIC    * Retry logic, error handling, logging
# MAGIC    * Custom authentication flows
# MAGIC
# MAGIC 3. **Source has unique requirements**
# MAGIC    * Non-standard API
# MAGIC    * Complex pagination or rate limiting
# MAGIC
# MAGIC 4. **You're prototyping or experimenting**
# MAGIC    * Quick one-off ingestion
# MAGIC    * Not production-grade
# MAGIC
# MAGIC **Exam trap**: Custom code is harder to maintain than managed solutions. For production pipelines, prefer Lakeflow Connect or Auto Loader unless you have specific requirements they can't meet.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exam Decision Tree
# MAGIC
# MAGIC Follow this logic for exam questions:
# MAGIC
# MAGIC ```
# MAGIC Start
# MAGIC   |
# MAGIC   +-- Is source a DATABASE or SaaS API?
# MAGIC   |     YES -> Use Lakeflow Connect
# MAGIC   |     NO  -> Continue
# MAGIC   |
# MAGIC   +-- Is source FILES in cloud storage?
# MAGIC   |     YES -> Continue
# MAGIC   |     NO  -> Use custom code or partner connector
# MAGIC   |
# MAGIC   +-- Do you need STREAMING ingestion (near real-time)?
# MAGIC   |     YES -> Use Auto Loader
# MAGIC   |     NO  -> Continue
# MAGIC   |
# MAGIC   +-- Do you need SCHEMA EVOLUTION or high file volumes?
# MAGIC   |     YES -> Use Auto Loader
# MAGIC   |     NO  -> Use COPY INTO (simpler for batch)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exam Scenarios - Practice
# MAGIC
# MAGIC #### Scenario 1
# MAGIC "You need to ingest Salesforce Opportunity data into Unity Catalog. The data updates hourly. Which method?"
# MAGIC
# MAGIC **Answer**: Lakeflow Connect (managed Salesforce connector)
# MAGIC
# MAGIC **Why**:
# MAGIC * Source is SaaS API (not files)
# MAGIC * Managed connector handles Salesforce authentication and schema
# MAGIC * Hourly sync is supported
# MAGIC
# MAGIC **Not Auto Loader**: Salesforce is not files in cloud storage
# MAGIC
# MAGIC **Not COPY INTO**: COPY INTO only works with files
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario 2
# MAGIC "JSON files land in S3 every 10 seconds. You need sub-minute ingestion latency with exactly-once processing. Which method?"
# MAGIC
# MAGIC **Answer**: Auto Loader
# MAGIC
# MAGIC **Why**:
# MAGIC * Source is files (S3)
# MAGIC * Streaming requirement (sub-minute latency)
# MAGIC * Exactly-once guarantee (Auto Loader checkpoints)
# MAGIC
# MAGIC **Not COPY INTO**: Batch only, not streaming
# MAGIC
# MAGIC **Not Lakeflow Connect**: Doesn't work with files in S3
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario 3
# MAGIC "A partner uploads a 50GB Parquet file to ADLS once per week. You need to load it into a Delta table. Which method?"
# MAGIC
# MAGIC **Answer**: COPY INTO
# MAGIC
# MAGIC **Why**:
# MAGIC * Source is files (ADLS)
# MAGIC * Infrequent batch load (weekly)
# MAGIC * Simple use case (full file ingestion)
# MAGIC
# MAGIC **Not Auto Loader**: Overkill for weekly batch. Auto Loader is for continuous/frequent ingestion.
# MAGIC
# MAGIC **Not Lakeflow Connect**: Doesn't work with files
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario 4
# MAGIC "You need to ingest data from a PostgreSQL database. The source table has 10M rows. Only 1000 rows change per day (tracked by `updated_at` timestamp). You need to capture updates. Which method?"
# MAGIC
# MAGIC **Answer**: Lakeflow Connect (standard JDBC connector with upsert mode)
# MAGIC
# MAGIC **Why**:
# MAGIC * Source is database (PostgreSQL)
# MAGIC * Incremental upsert requirement (updates, not just inserts)
# MAGIC * Watermark column available (`updated_at`)
# MAGIC
# MAGIC **Configuration**:
# MAGIC * Watermark column: `updated_at`
# MAGIC * Primary key: Table's primary key
# MAGIC * Merge mode: Upsert
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario 5
# MAGIC "CSV files land in GCS. Files have inconsistent schemas (some files have extra columns). You need to ingest all files without failing on schema mismatches. Which method?"
# MAGIC
# MAGIC **Answer**: Auto Loader (with schema evolution enabled)
# MAGIC
# MAGIC **Why**:
# MAGIC * Source is files (GCS)
# MAGIC * Schema evolution requirement (handle extra columns)
# MAGIC * Auto Loader supports `mergeSchema` or `rescuedDataColumn`
# MAGIC
# MAGIC **Not COPY INTO**: Requires schema to match exactly. New columns cause failure.
# MAGIC
# MAGIC **Not Lakeflow Connect**: Doesn't work with files
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario 6
# MAGIC "You need to ingest data from a MySQL database. The source table is append-only (no updates or deletes). New rows are identified by an auto-increment `id` column. Which method?"
# MAGIC
# MAGIC **Answer**: Lakeflow Connect (standard JDBC connector with incremental append mode)
# MAGIC
# MAGIC **Why**:
# MAGIC * Source is database (MySQL)
# MAGIC * Append-only (no upsert needed)
# MAGIC * Watermark column available (`id`)
# MAGIC
# MAGIC **Configuration**:
# MAGIC * Watermark column: `id`
# MAGIC * Merge mode: Append (not upsert)
# MAGIC
# MAGIC **Not full load**: Full load re-reads all 10M rows every sync. Incremental append only reads new rows.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Key Exam Takeaways
# MAGIC
# MAGIC 1. **Lakeflow Connect = Databases + SaaS APIs**
# MAGIC    * NOT for files in S3/ADLS/GCS
# MAGIC
# MAGIC 2. **Auto Loader = Files + Streaming + Schema Evolution**
# MAGIC    * Best for continuous file ingestion
# MAGIC
# MAGIC 3. **COPY INTO = Files + Simple Batch Loads**
# MAGIC    * Best for infrequent file drops
# MAGIC
# MAGIC 4. **Don't mix categories**: Lakeflow Connect doesn't work with files. Auto Loader and COPY INTO don't work with databases.
# MAGIC
# MAGIC 5. **Match latency requirements**: Lakeflow Connect is hourly+, Auto Loader is seconds, COPY INTO is minutes to hours.
# MAGIC
# MAGIC 6. **Schema evolution**: Auto Loader handles automatically. COPY INTO requires manual changes. Lakeflow Connect handles for SaaS apps.

# COMMAND ----------

# DBTITLE 1,Semi-Structured Data Handling
# MAGIC %md
# MAGIC ## Semi-Structured Data Handling
# MAGIC
# MAGIC ### Overview
# MAGIC
# MAGIC Semi-structured data (JSON, XML, nested objects) is common in APIs and SaaS applications. Lakeflow Connect handles this automatically for managed connectors, but you need to understand the behavior.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### JSON and Nested Objects
# MAGIC
# MAGIC #### Automatic Flattening (Managed Connectors)
# MAGIC
# MAGIC Managed connectors flatten nested JSON into columnar format.
# MAGIC
# MAGIC **Example**: Salesforce Account with nested BillingAddress
# MAGIC
# MAGIC **API response**:
# MAGIC ```json
# MAGIC {
# MAGIC   "Id": "001...",
# MAGIC   "Name": "Acme Corp",
# MAGIC   "BillingAddress": {
# MAGIC     "street": "123 Main St",
# MAGIC     "city": "San Francisco",
# MAGIC     "state": "CA",
# MAGIC     "postalCode": "94105"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Flattened Delta table**:
# MAGIC ```
# MAGIC Id      | Name       | BillingAddress.street | BillingAddress.city | BillingAddress.state | BillingAddress.postalCode
# MAGIC 001...  | Acme Corp  | 123 Main St           | San Francisco       | CA                   | 94105
# MAGIC ```
# MAGIC
# MAGIC **Exam lesson**: Nested objects become dot-notation columns. No JSON parsing needed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Arrays in API Responses
# MAGIC
# MAGIC Some APIs return arrays (e.g., list of phone numbers, list of tags).
# MAGIC
# MAGIC **Example**: Contact with multiple phone numbers
# MAGIC
# MAGIC **API response**:
# MAGIC ```json
# MAGIC {
# MAGIC   "Id": "003...",
# MAGIC   "Name": "John Smith",
# MAGIC   "PhoneNumbers": [
# MAGIC     {"type": "mobile", "number": "555-1234"},
# MAGIC     {"type": "work", "number": "555-5678"}
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Handling**:
# MAGIC * **Option 1**: Store array as JSON string in Delta table
# MAGIC * **Option 2**: Flatten array into separate rows (one row per phone number)
# MAGIC * **Option 3**: Store array as Spark array type
# MAGIC
# MAGIC **Managed connectors**: Typically store as JSON string or array type. You handle downstream with Spark functions (`explode()`, `from_json()`).
# MAGIC
# MAGIC **Exam trap**: If you need each array element as a separate row, you must use Spark transformations AFTER ingestion. Lakeflow Connect doesn't explode arrays during ingestion.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Schema Inference and Evolution
# MAGIC
# MAGIC #### Schema Inference (Standard Connectors)
# MAGIC
# MAGIC For JDBC sources, Lakeflow Connect queries the database metadata to infer schema:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM information_schema.columns
# MAGIC WHERE table_name = 'orders'
# MAGIC ```
# MAGIC
# MAGIC **Inferred types**:
# MAGIC * `INT`, `BIGINT` -> Spark `IntegerType`, `LongType`
# MAGIC * `VARCHAR`, `TEXT` -> Spark `StringType`
# MAGIC * `TIMESTAMP` -> Spark `TimestampType`
# MAGIC * `DECIMAL` -> Spark `DecimalType`
# MAGIC
# MAGIC **Exam trap**: Type mismatches can cause failures. Example: Source has `DECIMAL(10,2)`, but Delta table expects `DOUBLE`. Lakeflow Connect uses source types by default.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Schema Evolution (Managed Connectors)
# MAGIC
# MAGIC When SaaS applications add new fields:
# MAGIC
# MAGIC 1. **Connector detects new field** (next sync)
# MAGIC 2. **Adds column to Delta table** (schema merge)
# MAGIC 3. **Existing rows have NULL**
# MAGIC 4. **New rows populate the field**
# MAGIC
# MAGIC **Configuration option**: `mergeSchema` (enabled by default)
# MAGIC
# MAGIC **Exam scenario**: "Your Workday admin adds a custom field `CostCenter`. How do you ensure it's captured?"
# MAGIC * **Answer**: Schema evolution is automatic with managed connectors
# MAGIC * **Wrong**: Manually ALTER TABLE, recreate pipeline
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Handling Data Type Mismatches
# MAGIC
# MAGIC #### Problem: Source vs Target Type Conflicts
# MAGIC
# MAGIC **Example**: Source has `VARCHAR(50)`, but Delta table expects `INT`.
# MAGIC
# MAGIC **What happens**: Ingestion fails with type mismatch error.
# MAGIC
# MAGIC **Solutions**:
# MAGIC 1. **Custom query at source**: Cast to correct type
# MAGIC    ```sql
# MAGIC    SELECT CAST(column_name AS INT) AS column_name
# MAGIC    FROM source_table
# MAGIC    ```
# MAGIC
# MAGIC 2. **Recreate target table**: ALTER TABLE to match source type
# MAGIC
# MAGIC 3. **Use STRING in Delta**: Store as string, cast downstream in Spark
# MAGIC
# MAGIC **Exam lesson**: Type mismatches are a common failure mode. Always validate schema before production.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Null Handling
# MAGIC
# MAGIC #### NULL Values in Source Data
# MAGIC
# MAGIC Lakeflow Connect preserves NULLs from source:
# MAGIC
# MAGIC **Source**:
# MAGIC ```
# MAGIC order_id | customer_id | order_date
# MAGIC 1        | 100         | 2024-06-01
# MAGIC 2        | NULL        | 2024-06-02
# MAGIC ```
# MAGIC
# MAGIC **Delta table**:
# MAGIC ```
# MAGIC order_id | customer_id | order_date
# MAGIC 1        | 100         | 2024-06-01
# MAGIC 2        | null        | 2024-06-02
# MAGIC ```
# MAGIC
# MAGIC **Exam trap**: If your downstream logic assumes no NULLs, you need to handle them (filter, coalesce, default values).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Missing Fields in API Responses
# MAGIC
# MAGIC Some APIs omit fields if they're empty (instead of returning NULL).
# MAGIC
# MAGIC **Example**: Salesforce may omit `BillingAddress` if not set.
# MAGIC
# MAGIC **Managed connector behavior**: Creates column with NULL value.
# MAGIC
# MAGIC **Exam lesson**: Missing fields become NULLs in Delta. Check for both `NULL` and missing fields.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Character Encoding and Special Characters
# MAGIC
# MAGIC #### UTF-8 Encoding
# MAGIC
# MAGIC Lakeflow Connect uses UTF-8 encoding by default.
# MAGIC
# MAGIC **Handles**:
# MAGIC * Emoji: "Great product! 😊"
# MAGIC * Special characters: "Café", "naïve"
# MAGIC * International characters: "北京", "Москва"
# MAGIC
# MAGIC **Exam trap**: If source database uses a different encoding (e.g., Latin-1), you may need to specify encoding in connection parameters.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Escape Characters in JSON
# MAGIC
# MAGIC JSON strings may contain escape sequences:
# MAGIC
# MAGIC **Example**:
# MAGIC ```json
# MAGIC {"description": "Line 1\nLine 2\tTabbed"}
# MAGIC ```
# MAGIC
# MAGIC **Delta table**:
# MAGIC ```
# MAGIC description
# MAGIC "Line 1
# MAGIC Line 2    Tabbed"
# MAGIC ```
# MAGIC
# MAGIC **Managed connectors**: Handle escape sequences automatically. You don't need to unescape manually.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Large Objects (LOBs)
# MAGIC
# MAGIC #### BLOB and CLOB Handling
# MAGIC
# MAGIC Some databases store large objects:
# MAGIC * **BLOB**: Binary data (images, PDFs)
# MAGIC * **CLOB**: Large text (documents, XML)
# MAGIC
# MAGIC **Lakeflow Connect behavior**:
# MAGIC * **Small LOBs** (< 1MB): Ingested as binary or string columns
# MAGIC * **Large LOBs** (> 1MB): May fail or truncate
# MAGIC
# MAGIC **Recommendation**: Store large files in cloud storage (S3/ADLS/GCS), store file paths in database.
# MAGIC
# MAGIC **Exam scenario**: "A database table has a `document` column (CLOB, avg 5MB). Ingestion is slow. What should you do?"
# MAGIC * **Answer**: Store documents in cloud storage, store paths in database
# MAGIC * **Why**: Databases are not optimized for large binary data. Use object storage.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exam Scenarios - Semi-Structured Data
# MAGIC
# MAGIC #### Scenario 1
# MAGIC "Salesforce returns nested JSON objects. How do you access nested fields in Databricks?"
# MAGIC
# MAGIC **Answer**: Managed connector flattens nested objects into dot-notation columns (e.g., `BillingAddress.city`)
# MAGIC
# MAGIC **Not correct**: Use `from_json()` (not needed, already flattened)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario 2
# MAGIC "A managed connector pipeline fails with 'Schema mismatch' after the SaaS app added a new field. How do you fix it?"
# MAGIC
# MAGIC **Answer**: Enable schema evolution (should be default). Pipeline will auto-add the column.
# MAGIC
# MAGIC **Not correct**: Recreate pipeline, manually ALTER TABLE (evolution handles this)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario 3
# MAGIC "You're ingesting ServiceNow incident data. The API returns an array of comments (multiple comments per incident). You need one row per comment. How do you achieve this?"
# MAGIC
# MAGIC **Answer**: 
# MAGIC 1. Lakeflow Connect ingests incidents with comments as array column
# MAGIC 2. Use Spark `explode()` downstream to create one row per comment
# MAGIC
# MAGIC **Not during ingestion**: Lakeflow Connect doesn't explode arrays. Handle in transformation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario 4
# MAGIC "A JDBC source has a `price` column (DECIMAL(10,2)). Delta table expects DOUBLE. Ingestion fails. How do you fix it?"
# MAGIC
# MAGIC **Answer**: Use custom query to cast at source:
# MAGIC ```sql
# MAGIC SELECT CAST(price AS DOUBLE) AS price
# MAGIC FROM source_table
# MAGIC ```
# MAGIC
# MAGIC **Or**: Recreate Delta table with DECIMAL type to match source.

# COMMAND ----------

# DBTITLE 1,Key Exam Traps and Common Mistakes
# MAGIC %md
# MAGIC ## Key Exam Traps and Common Mistakes
# MAGIC
# MAGIC ### Trap 1: Choosing the Wrong Ingestion Method
# MAGIC
# MAGIC **Exam will test**: Can you distinguish when to use Lakeflow Connect vs Auto Loader vs COPY INTO?
# MAGIC
# MAGIC **Common mistakes**:
# MAGIC * Using Lakeflow Connect for files in S3 (wrong - use Auto Loader or COPY INTO)
# MAGIC * Using Auto Loader for database ingestion (wrong - use Lakeflow Connect)
# MAGIC * Using COPY INTO for streaming (wrong - COPY INTO is batch only)
# MAGIC
# MAGIC **Remember**:
# MAGIC * **Lakeflow Connect** = Databases + SaaS APIs
# MAGIC * **Auto Loader** = Files + Streaming + Schema Evolution
# MAGIC * **COPY INTO** = Files + Simple Batch
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Trap 2: Incremental Loading Misconceptions
# MAGIC
# MAGIC **Mistake 1**: Using `updated_at` for append-only incremental loading
# MAGIC
# MAGIC **Problem**: If you want to capture NEW rows only (not updates), use `created_at` (immutable). Using `updated_at` will re-capture updated rows.
# MAGIC
# MAGIC **Correct approach**:
# MAGIC * **Append-only**: Use `created_at` or `id` (immutable)
# MAGIC * **Upsert (capture updates)**: Use `updated_at` with primary key
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Mistake 2**: Thinking incremental upsert is always faster
# MAGIC
# MAGIC **Problem**: Upsert (MERGE) requires reading the target table for matching. For small incremental batches and large target tables, upsert can be SLOWER than append.
# MAGIC
# MAGIC **When upsert is justified**: When you need current state (updates matter).
# MAGIC
# MAGIC **When append is better**: When you're building an immutable history (append-only log).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Mistake 3**: Not configuring watermark column
# MAGIC
# MAGIC **Problem**: Without a watermark, Lakeflow Connect performs full load every sync (reads entire table).
# MAGIC
# MAGIC **Exam scenario**: "Ingestion is slow despite enabling incremental loading. What's wrong?"
# MAGIC * **Answer**: Watermark column not configured. Connector is doing full load.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Trap 3: Standard vs Managed Connector Confusion
# MAGIC
# MAGIC **Mistake**: Trying to use standard JDBC connector for Salesforce
# MAGIC
# MAGIC **Problem**: Salesforce uses REST API, not JDBC. Standard connector won't work.
# MAGIC
# MAGIC **Correct**: Use managed Salesforce connector.
# MAGIC
# MAGIC **Exam rule**: If the source is a well-known SaaS app (Salesforce, Workday, ServiceNow, HubSpot), use the MANAGED connector, not standard JDBC.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Mistake**: Thinking managed connectors support custom queries
# MAGIC
# MAGIC **Problem**: Managed connectors ingest entire objects (e.g., all Salesforce Accounts). You can't filter at source.
# MAGIC
# MAGIC **Solution**: Filter downstream in Spark (after ingestion).
# MAGIC
# MAGIC **Exam scenario**: "You need only Salesforce Opportunities with Amount > $10,000. How do you configure the connector?"
# MAGIC * **Answer**: Ingest all Opportunities, filter in Spark:
# MAGIC   ```python
# MAGIC   df = spark.table("opportunities").filter("Amount > 10000")
# MAGIC   ```
# MAGIC * **Not correct**: Configure connector to filter (managed connectors don't support WHERE clauses)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Trap 4: Authentication and Credentials
# MAGIC
# MAGIC **Mistake**: Hardcoding credentials in connection strings
# MAGIC
# MAGIC **Problem**: Security risk. Credentials visible in logs, notebooks, job configurations.
# MAGIC
# MAGIC **Correct**: Store in Databricks Secrets:
# MAGIC ```python
# MAGIC username = dbutils.secrets.get(scope="prod", key="db_username")
# MAGIC password = dbutils.secrets.get(scope="prod", key="db_password")
# MAGIC ```
# MAGIC
# MAGIC **Exam question**: "How should you provide database credentials to Lakeflow Connect?"
# MAGIC * **Answer**: Databricks Secrets
# MAGIC * **Wrong**: Notebook variables, job parameters, hardcoded strings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Mistake**: Not refreshing OAuth tokens
# MAGIC
# MAGIC **Problem**: Managed connectors use OAuth. Tokens expire (typically after 1-2 hours).
# MAGIC
# MAGIC **Lakeflow Connect behavior**: Automatically refreshes tokens. You don't handle this manually.
# MAGIC
# MAGIC **Exam lesson**: Lakeflow Connect manages OAuth refresh. If using custom code, YOU must handle token refresh.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Trap 5: Schema Evolution Misunderstandings
# MAGIC
# MAGIC **Mistake**: Thinking schema evolution is always enabled
# MAGIC
# MAGIC **Problem**: Schema evolution is configurable. If disabled, new columns cause ingestion failure.
# MAGIC
# MAGIC **Exam scenario**: "SaaS app adds a new field. Connector fails with 'Schema mismatch'. What's wrong?"
# MAGIC * **Answer**: Schema evolution is disabled. Enable it.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Mistake**: Expecting schema evolution to handle type changes
# MAGIC
# MAGIC **Problem**: Schema evolution adds NEW columns. It doesn't handle type CHANGES (e.g., INT -> STRING).
# MAGIC
# MAGIC **Example**: Source changes `age` from INT to VARCHAR. Schema evolution can't fix this automatically.
# MAGIC
# MAGIC **Solution**: Manual intervention (ALTER TABLE, recreate pipeline).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Trap 6: Parallelization and Performance
# MAGIC
# MAGIC **Mistake**: Over-parallelizing JDBC reads
# MAGIC
# MAGIC **Problem**: Source database has connection limit (e.g., 10 concurrent connections). You configure 50 partitions. Ingestion fails or throttles.
# MAGIC
# MAGIC **Solution**: Match partitions to source connection limit.
# MAGIC
# MAGIC **Exam scenario**: "Database allows 8 concurrent connections. You've configured 16 partitions. What happens?"
# MAGIC * **Answer**: Ingestion fails or is throttled. Reduce partitions to 8 or fewer.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Mistake**: Not using parallelization for large tables
# MAGIC
# MAGIC **Problem**: Ingesting 100GB table with 1 partition (single connection). Takes hours.
# MAGIC
# MAGIC **Solution**: Enable parallelization:
# MAGIC * Partition column (numeric)
# MAGIC * Number of partitions (e.g., 16 or 32)
# MAGIC
# MAGIC **Exam lesson**: Parallelization speeds up large ingestions but requires a numeric partition column.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Trap 7: API Rate Limits (Managed Connectors)
# MAGIC
# MAGIC **Mistake**: Thinking you need to handle rate limits manually
# MAGIC
# MAGIC **Problem**: Managed connectors handle rate limits automatically (throttle requests, retry).
# MAGIC
# MAGIC **Exam question**: "A managed connector is slow. Should you implement custom rate limiting?"
# MAGIC * **Answer**: No. Managed connector handles this. If too slow, reduce sync frequency.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Mistake**: Expecting real-time sync from managed connectors
# MAGIC
# MAGIC **Problem**: Managed connectors run on schedule (hourly, daily). They're NOT real-time.
# MAGIC
# MAGIC **Exam scenario**: "You need sub-minute latency from Salesforce. Should you use the managed connector?"
# MAGIC * **Answer**: No. Use partner streaming connector (Fivetran, Airbyte) or custom CDC solution.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Trap 8: File vs Database Confusion
# MAGIC
# MAGIC **Mistake**: Using Auto Loader for database ingestion
# MAGIC
# MAGIC **Problem**: Auto Loader ingests FILES from cloud storage, not databases.
# MAGIC
# MAGIC **Exam scenario**: "You need to ingest from MySQL. Should you use Auto Loader?"
# MAGIC * **Answer**: No. Use Lakeflow Connect (standard JDBC connector).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Mistake**: Using Lakeflow Connect for files in S3
# MAGIC
# MAGIC **Problem**: Lakeflow Connect doesn't ingest files. It connects to databases and APIs.
# MAGIC
# MAGIC **Exam scenario**: "CSV files land in S3. Should you use Lakeflow Connect?"
# MAGIC * **Answer**: No. Use Auto Loader or COPY INTO.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Trap 9: Unity Catalog Governance
# MAGIC
# MAGIC **Mistake**: Thinking Lakeflow Connect bypasses Unity Catalog
# MAGIC
# MAGIC **Problem**: All Lakeflow Connect pipelines land data in Unity Catalog tables. Governance applies.
# MAGIC
# MAGIC **Exam lesson**: Lakeflow Connect respects Unity Catalog permissions. If you don't have CREATE TABLE permission in target schema, ingestion fails.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Mistake**: Not specifying target catalog/schema
# MAGIC
# MAGIC **Problem**: Data lands in default schema, which may not be governed.
# MAGIC
# MAGIC **Best practice**: Always specify full three-level namespace:
# MAGIC ```
# MAGIC catalog.schema.table
# MAGIC ```
# MAGIC
# MAGIC **Exam scenario**: "Data landed in `hive_metastore.default` instead of `prod.sales`. What went wrong?"
# MAGIC * **Answer**: Target catalog/schema not specified. Defaulted to legacy Hive metastore.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Trap 10: Scheduling and Triggers
# MAGIC
# MAGIC **Mistake**: Expecting event-driven triggers
# MAGIC
# MAGIC **Problem**: Lakeflow Connect uses time-based schedules (hourly, daily), not event-driven.
# MAGIC
# MAGIC **Exam scenario**: "You need ingestion to trigger when source database commits a transaction. How?"
# MAGIC * **Answer**: Lakeflow Connect doesn't support event triggers. Use custom CDC solution or partner connector.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Mistake**: Running managed connectors too frequently
# MAGIC
# MAGIC **Problem**: SaaS APIs have rate limits. Syncing every 5 minutes may hit limits and fail.
# MAGIC
# MAGIC **Solution**: Match sync frequency to API rate limits and data freshness requirements.
# MAGIC
# MAGIC **Exam lesson**: More frequent isn't always better. Respect API rate limits.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Summary: Critical Exam Concepts
# MAGIC
# MAGIC 1. **Lakeflow Connect works with databases and SaaS APIs, NOT files**
# MAGIC 2. **Auto Loader works with files, NOT databases**
# MAGIC 3. **Incremental loading requires a watermark column**
# MAGIC 4. **Upsert requires BOTH watermark and primary key**
# MAGIC 5. **Managed connectors handle authentication, rate limits, schema evolution**
# MAGIC 6. **Standard connectors require you to specify tables/queries**
# MAGIC 7. **Schema evolution adds columns, doesn't handle type changes**
# MAGIC 8. **Parallelization requires numeric partition column and respect for source limits**
# MAGIC 9. **Store credentials in Databricks Secrets, never hardcode**
# MAGIC 10. **Lakeflow Connect is scheduled (not real-time), minimum hourly**
# MAGIC
# MAGIC **Exam strategy**: Read the scenario carefully. Identify:
# MAGIC 1. Source type (database, SaaS API, files)
# MAGIC 2. Latency requirement (real-time, hourly, daily)
# MAGIC 3. Schema evolution needs (dynamic schemas, nested data)
# MAGIC 4. Incremental loading pattern (append-only, upsert)
# MAGIC
# MAGIC Match these to the decision matrix.

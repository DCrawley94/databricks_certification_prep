# Databricks notebook source
# DBTITLE 1,Topic 8: Lakeflow Connect - Solutions
# MAGIC %md
# MAGIC # Topic 8: Lakeflow Connect - Solutions
# MAGIC
# MAGIC ## How to Use This Notebook
# MAGIC
# MAGIC This notebook provides complete solutions to all practice tasks. Each solution includes:
# MAGIC * **Complete answer**: The correct response
# MAGIC * **Explanation**: Why this answer is correct
# MAGIC * **Exam tip**: Common mistakes and traps to avoid
# MAGIC
# MAGIC ## Structure
# MAGIC
# MAGIC * **Exercises 1-15**: Detailed solutions with explanations
# MAGIC * **MCQs 1-5**: Correct answers with justification
# MAGIC * **Challenges 1-2**: Complete solutions for complex scenarios
# MAGIC * **Applieds 1-2**: Decision tree walkthroughs
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Study Approach
# MAGIC
# MAGIC 1. Attempt the practice task first (don't peek at solutions)
# MAGIC 2. Compare your answer to the solution
# MAGIC 3. Read the explanation even if you got it right
# MAGIC 4. Note the exam tips - these highlight high-frequency traps
# MAGIC 5. Rework any exercises you missed

# COMMAND ----------

# DBTITLE 1,Exercise 1 Solution: Connector Type Selection
# MAGIC %md
# MAGIC ## Exercise 1 Solution: Connector Type Selection
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. MySQL:
# MAGIC    - Connector type: Standard JDBC connector
# MAGIC    - Justification: MySQL is a relational database requiring JDBC connectivity
# MAGIC    - Authentication: JDBC connection string with credentials stored in Databricks Secrets
# MAGIC
# MAGIC 2. Salesforce:
# MAGIC    - Connector type: Managed Salesforce connector
# MAGIC    - Justification: Salesforce is a well-known SaaS application with pre-built managed connector
# MAGIC    - Authentication: OAuth 2.0 or username/password + security token
# MAGIC
# MAGIC 3. Workday:
# MAGIC    - Connector type: Managed Workday connector
# MAGIC    - Justification: Workday is a supported SaaS application with managed connector
# MAGIC    - Authentication: OAuth 2.0
# MAGIC
# MAGIC 4. PostgreSQL:
# MAGIC    - Connector type: Standard JDBC connector
# MAGIC    - Justification: PostgreSQL is a relational database requiring JDBC connectivity
# MAGIC    - Authentication: JDBC connection string with credentials in Secrets
# MAGIC
# MAGIC 5. ServiceNow:
# MAGIC    - Connector type: Managed ServiceNow connector
# MAGIC    - Justification: ServiceNow is a supported SaaS application with managed connector
# MAGIC    - Authentication: Basic auth or OAuth 2.0
# MAGIC
# MAGIC 6. Custom internal database:
# MAGIC    - Connector type: Standard JDBC connector
# MAGIC    - Justification: Custom databases don't have managed connectors; use generic JDBC
# MAGIC    - Authentication: JDBC connection string with credentials in Secrets
# MAGIC ```
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Rule 1: Relational databases (MySQL, PostgreSQL, Oracle, SQL Server) = Standard JDBC connector**
# MAGIC * These use JDBC protocol for connectivity
# MAGIC * You must specify connection details (host, port, database)
# MAGIC * You configure which tables or queries to ingest
# MAGIC
# MAGIC **Rule 2: Well-known SaaS applications (Salesforce, Workday, ServiceNow) = Managed connector**
# MAGIC * Databricks maintains pre-built integrations
# MAGIC * Auto-discover schemas and objects
# MAGIC * Handle API authentication and rate limits automatically
# MAGIC
# MAGIC **Rule 3: Custom or unsupported applications = Standard JDBC connector (if JDBC-compliant) or custom code**
# MAGIC * Managed connectors only exist for specific SaaS apps
# MAGIC * If your application has a JDBC driver, use standard connector
# MAGIC * If not JDBC-compliant, you'll need custom code or partner connectors
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Trying to use standard JDBC connector for Salesforce or other SaaS apps.
# MAGIC
# MAGIC **Why it fails**: Salesforce uses REST API, not JDBC. Authentication requires OAuth, not simple username/password. API responses are JSON with nested structures. Standard JDBC connector can't handle this.
# MAGIC
# MAGIC **Exam signal**: If the question mentions a recognizable SaaS brand (Salesforce, Workday, ServiceNow, HubSpot, NetSuite), the answer is ALWAYS managed connector, not standard JDBC.
# MAGIC
# MAGIC **Memory aid**: 
# MAGIC * Database + port number in scenario = Standard JDBC
# MAGIC * SaaS brand name in scenario = Managed connector

# COMMAND ----------

# DBTITLE 1,Exercise 2 Solution: JDBC Connection Strings
# MAGIC %md
# MAGIC ## Exercise 2 Solution: JDBC Connection Strings
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. MySQL:
# MAGIC jdbc:mysql://mysql-prod.company.com:3306/sales_db?useSSL=true&serverTimezone=UTC
# MAGIC
# MAGIC 2. PostgreSQL:
# MAGIC jdbc:postgresql://10.0.5.100:5432/analytics?ssl=true&currentSchema=public
# MAGIC
# MAGIC 3. SQL Server:
# MAGIC jdbc:sqlserver://sqlserver.internal.net:1433;databaseName=crm;encrypt=true
# MAGIC
# MAGIC 4. Oracle:
# MAGIC jdbc:oracle:thin:@oracle-prod.company.com:1521:ORCL
# MAGIC ```
# MAGIC
# MAGIC **Additional Question Answer**: SQL Server uses semicolons instead of question marks for parameters.
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **MySQL format**:
# MAGIC * Pattern: `jdbc:mysql://host:port/database?param1=value1&param2=value2`
# MAGIC * Uses `?` before first parameter, `&` between parameters
# MAGIC * Common params: `useSSL=true`, `serverTimezone=UTC`
# MAGIC
# MAGIC **PostgreSQL format**:
# MAGIC * Pattern: `jdbc:postgresql://host:port/database?param1=value1&param2=value2`
# MAGIC * Similar to MySQL (question mark and ampersand)
# MAGIC * Common params: `ssl=true`, `currentSchema=schema_name`
# MAGIC
# MAGIC **SQL Server format**:
# MAGIC * Pattern: `jdbc:sqlserver://host:port;param1=value1;param2=value2`
# MAGIC * Uses `;` (semicolon) to separate parameters - NOT question mark
# MAGIC * `databaseName=` is used instead of appending to URL
# MAGIC * Common params: `encrypt=true`, `trustServerCertificate=true`
# MAGIC
# MAGIC **Oracle format**:
# MAGIC * Pattern: `jdbc:oracle:thin:@host:port:SID` or `jdbc:oracle:thin:@//host:port/servicename`
# MAGIC * Uses `:` (colon) separators throughout
# MAGIC * SID vs service name: two different connection methods
# MAGIC * No parameter suffix for basic connections
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **High-frequency trap**: Mixing up SQL Server and MySQL/PostgreSQL syntax.
# MAGIC
# MAGIC **Wrong answer**:
# MAGIC ```
# MAGIC jdbc:sqlserver://host:1433?databaseName=crm&encrypt=true  // WRONG - uses ? and &
# MAGIC ```
# MAGIC
# MAGIC **Correct answer**:
# MAGIC ```
# MAGIC jdbc:sqlserver://host:1433;databaseName=crm;encrypt=true  // Correct - uses semicolons
# MAGIC ```
# MAGIC
# MAGIC **Memory aid for the exam**:
# MAGIC * MySQL/PostgreSQL: Think "question mark" (?) for parameters
# MAGIC * SQL Server: Think "semicolon separation" (;)
# MAGIC * Oracle: Think "colon connection" (:)
# MAGIC
# MAGIC **Another exam trap**: Oracle SID vs service name format.
# MAGIC * `jdbc:oracle:thin:@host:port:SID` - single colon before SID
# MAGIC * `jdbc:oracle:thin:@//host:port/servicename` - double slash, forward slash before service
# MAGIC
# MAGIC Mixing these up causes connection failures.

# COMMAND ----------

# DBTITLE 1,Exercise 3 Solution: Incremental Loading Strategy
# MAGIC %md
# MAGIC ## Exercise 3 Solution: Incremental Loading Strategy
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC Table 1 (orders):
# MAGIC - Loading pattern: Incremental Append
# MAGIC - Watermark column: created_at
# MAGIC - Primary key: Not needed for append
# MAGIC - Justification: Orders are immutable (never updated). Use created_at because it's immutable 
# MAGIC   and tracks when rows were created. order_id could also work but created_at is more reliable 
# MAGIC   for time-based incremental loading. updated_at is wrong because it changes on updates 
# MAGIC   (but this table has no updates).
# MAGIC
# MAGIC Table 2 (customer_accounts):
# MAGIC - Loading pattern: Incremental Upsert (MERGE)
# MAGIC - Watermark column: updated_at
# MAGIC - Primary key: account_id
# MAGIC - Justification: Accounts are frequently updated (balance, last_login change). Need current 
# MAGIC   state, not history. updated_at tracks changes, account_id enables MERGE matching. 
# MAGIC   Incremental append would create duplicate account_id rows. Full load would re-read 
# MAGIC   500K rows every sync (inefficient).
# MAGIC
# MAGIC Table 3 (product_catalog):
# MAGIC - Loading pattern: Full Load
# MAGIC - Watermark column: Not applicable
# MAGIC - Primary key: Not applicable
# MAGIC - Justification: Small table (5K rows), no watermark column available, infrequent updates. 
# MAGIC   Full load is simplest and efficient for small reference tables. Could use upsert if you 
# MAGIC   added an updated_at column, but full load is acceptable here given size.
# MAGIC ```
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **When to use Incremental Append**:
# MAGIC * Source data is **append-only** (no updates or deletes)
# MAGIC * Reliable watermark column exists (immutable timestamp or auto-increment ID)
# MAGIC * Examples: logs, events, transactions, immutable fact tables
# MAGIC * Result: New rows added to target, existing rows never touched
# MAGIC
# MAGIC **When to use Incremental Upsert**:
# MAGIC * Source data has **updates** (not just inserts)
# MAGIC * Need **current state** (not full history)
# MAGIC * Watermark column tracks changes (e.g., `updated_at`)
# MAGIC * Primary key available for matching
# MAGIC * Examples: customer accounts, inventory, order status
# MAGIC * Result: MERGE operation (update existing rows, insert new rows)
# MAGIC
# MAGIC **When to use Full Load**:
# MAGIC * Table is **small** (< 1 GB)
# MAGIC * No watermark column available
# MAGIC * Updates are infrequent
# MAGIC * Simplicity preferred over efficiency
# MAGIC * Examples: reference tables, lookup tables, dimension tables
# MAGIC * Result: Complete table replacement every sync
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Critical mistake**: Using `updated_at` as watermark for append-only loading.
# MAGIC
# MAGIC **Scenario**: "Orders table has created_at and updated_at. Orders are append-only. Which column for incremental loading?"
# MAGIC
# MAGIC **Wrong answer**: `updated_at` - "It's the most recent timestamp"
# MAGIC
# MAGIC **Why wrong**: Even though orders aren't updated in this table, if you DID update an order, updated_at would change. This means subsequent syncs would re-capture that row. Also, if updated_at is nullable or has default behavior, you might miss rows.
# MAGIC
# MAGIC **Correct answer**: `created_at` - Immutable, tracks insertion time, never changes.
# MAGIC
# MAGIC **Exam signal phrases**:
# MAGIC * "Append-only" → Incremental append with immutable watermark
# MAGIC * "Frequently updated" or "need current state" → Incremental upsert
# MAGIC * "Small reference table" → Full load
# MAGIC * "No timestamp column" → Full load (or add one)
# MAGIC
# MAGIC **Performance consideration (exam-relevant)**:
# MAGIC * Upsert is SLOWER than append (requires reading target table for matching)
# MAGIC * Don't use upsert if you don't need to capture updates
# MAGIC * For building history: Use append with SCD Type 2 pattern downstream, not upsert

# COMMAND ----------

# DBTITLE 1,Exercise 4 Solution: Credential Management
# MAGIC %md
# MAGIC ## Exercise 4 Solution: Credential Management
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. Correct answer: D. Store in Databricks Secrets
# MAGIC
# MAGIC 2. Steps to store credentials:
# MAGIC    Step 1: Create a secret scope (via Databricks CLI or UI)
# MAGIC    Step 2: Add secrets to the scope (username and password as separate keys)
# MAGIC    Step 3: Reference secrets in Lakeflow Connect configuration using dbutils.secrets.get()
# MAGIC
# MAGIC 3. Secret reference example:
# MAGIC    username = dbutils.secrets.get(scope="jdbc_prod", key="mysql_username")
# MAGIC    password = dbutils.secrets.get(scope="jdbc_prod", key="mysql_password")
# MAGIC    
# MAGIC    connection_string = f"jdbc:mysql://host:3306/db"
# MAGIC    # Pass username/password separately to connector config
# MAGIC
# MAGIC 4. Why other options are wrong:
# MAGIC    - Option A (Hardcode): Credentials visible in plain text in configuration, logs, 
# MAGIC      job history. Anyone with view access sees credentials. Security violation.
# MAGIC    - Option B (Notebook variables): Variables visible in notebook code. Notebooks are 
# MAGIC      often shared or exported. Credentials leaked in version control if committed.
# MAGIC    - Option C (Job parameters): Parameters visible in job configuration UI and logs. 
# MAGIC      Anyone with view job permissions sees credentials.
# MAGIC
# MAGIC 5. Storing in Unity Catalog table:
# MAGIC    - Answer: No
# MAGIC    - Justification: Unity Catalog tables are for data, not secrets. Tables can be 
# MAGIC      queried, exported, shared. Databricks Secrets is purpose-built for credential 
# MAGIC      management with encryption at rest, access control, and audit logging. Never 
# MAGIC      store credentials in tables, even with restricted access.
# MAGIC ```
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Why Databricks Secrets is correct**:
# MAGIC 1. **Encryption**: Secrets encrypted at rest and in transit
# MAGIC 2. **Access control**: Fine-grained permissions on secret scopes
# MAGIC 3. **Audit logging**: All secret access is logged
# MAGIC 4. **No exposure**: Secrets never appear in logs, notebooks, or UI
# MAGIC 5. **Integration**: Native integration with Lakeflow Connect, Jobs, notebooks
# MAGIC
# MAGIC **How Databricks Secrets works**:
# MAGIC * Secrets stored in backend key management service (AWS Secrets Manager, Azure Key Vault, or Databricks-managed)
# MAGIC * Retrieved at runtime via `dbutils.secrets.get(scope, key)`
# MAGIC * Returns `[REDACTED]` in logs and notebook output
# MAGIC * Access requires READ permission on the secret scope
# MAGIC
# MAGIC **Secret scope types**:
# MAGIC * **Databricks-managed**: Stored in Databricks key management
# MAGIC * **Azure Key Vault-backed**: Syncs with Azure Key Vault
# MAGIC * **AWS Secrets Manager-backed**: Syncs with AWS Secrets Manager
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Thinking job parameters are secure because they're "configuration, not code."
# MAGIC
# MAGIC **Why wrong**: Job parameters are visible in:
# MAGIC * Job configuration UI (anyone with VIEW permission)
# MAGIC * Job run logs
# MAGIC * API responses when querying job details
# MAGIC * Audit logs
# MAGIC
# MAGIC **Correct approach**: Store credentials in Secrets, reference the secret name in job parameters.
# MAGIC
# MAGIC **Example**:
# MAGIC ```yaml
# MAGIC # WRONG - password visible
# MAGIC Job parameter: db_password = "mySecretPassword123"
# MAGIC
# MAGIC # CORRECT - only secret reference visible
# MAGIC Job parameter: db_password_secret = "jdbc_prod/mysql_password"
# MAGIC Notebook code: password = dbutils.secrets.get(scope="jdbc_prod", key="mysql_password")
# MAGIC ```
# MAGIC
# MAGIC **Exam signal**: Any question about credentials or sensitive data → answer is ALWAYS Databricks Secrets, never hardcode, never variables, never job parameters, never tables.

# COMMAND ----------

# DBTITLE 1,Exercise 5 Solution: Parallelization Configuration
# MAGIC %md
# MAGIC ## Exercise 5 Solution: Parallelization Configuration
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. Partition column: transaction_id
# MAGIC    Reason: It's a BIGINT (numeric), auto-increment (evenly distributed), covers full 
# MAGIC    range 1-50000000. Numeric column is REQUIRED for partitioning.
# MAGIC
# MAGIC 2. Number of partitions: 16
# MAGIC    Justification: Source allows 16 concurrent connections. Each partition uses one 
# MAGIC    connection. More partitions = more parallelism = faster ingestion, up to connection limit.
# MAGIC
# MAGIC 3. Generated queries:
# MAGIC    Partition 1: WHERE transaction_id >= 1 AND transaction_id < 3125001
# MAGIC    Partition 2: WHERE transaction_id >= 3125001 AND transaction_id < 6250001
# MAGIC    Partition 16: WHERE transaction_id >= 46875001 AND transaction_id <= 50000000
# MAGIC    
# MAGIC    Calculation: Range = (upper - lower) / partitions = 50000000 / 16 = 3,125,000 per partition
# MAGIC
# MAGIC 4. Why not partition on transaction_date or category:
# MAGIC    - transaction_date: Not numeric (TIMESTAMP). Partitioning requires INT or BIGINT. 
# MAGIC      JDBC partition logic only works with numeric columns for range-based splitting.
# MAGIC    - category: Not numeric (VARCHAR). Also likely has skewed distribution (some 
# MAGIC      categories much larger than others), leading to unbalanced partitions.
# MAGIC
# MAGIC 5. With 8 connection limit:
# MAGIC    Change: Reduce partitions from 16 to 8 (or fewer)
# MAGIC    If you configure 16 partitions but DB allows only 8 connections, excess partitions 
# MAGIC    will queue or fail. Respect source database limits.
# MAGIC
# MAGIC 6. Additional tuning:
# MAGIC    Parameter: fetch_size
# MAGIC    Recommendation: Increase from default (100) to 10,000-50,000 rows per fetch. 
# MAGIC    Reduces JDBC round trips. With 50M rows and fetch_size=100, you need 500K 
# MAGIC    round trips. With fetch_size=10,000, only 5K round trips. Dramatic improvement.
# MAGIC ```
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **How JDBC parallelization works**:
# MAGIC 1. Lakeflow Connect generates N parallel queries (N = num_partitions)
# MAGIC 2. Each query has a WHERE clause filtering on partition_column
# MAGIC 3. Range is split evenly: `(upper_bound - lower_bound) / num_partitions`
# MAGIC 4. Each partition executes on a separate connection
# MAGIC
# MAGIC **Why numeric column required**:
# MAGIC * JDBC partition logic: `WHERE col >= lower AND col < upper`
# MAGIC * Requires numeric comparison (<, >=)
# MAGIC * Strings and timestamps don't support this range-based splitting
# MAGIC
# MAGIC **Connection limit considerations**:
# MAGIC * Each partition = one concurrent connection
# MAGIC * If you exceed source DB connection limit:
# MAGIC   * Ingestion fails with "too many connections" error
# MAGIC   * Or connections queue (serialized, no performance gain)
# MAGIC * Always check source DB's `max_connections` setting
# MAGIC
# MAGIC **Fetch size impact**:
# MAGIC * Default fetch size: 10-100 rows (database-dependent)
# MAGIC * Each fetch = one JDBC round trip (network overhead)
# MAGIC * Larger fetch size: Fewer round trips, faster ingestion
# MAGIC * But: Larger memory usage per executor
# MAGIC * Sweet spot: 10,000-50,000 rows for large tables
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **High-frequency trap**: Trying to partition on timestamp or date columns.
# MAGIC
# MAGIC **Scenario**: "You want to parallelize ingestion. Should you partition on transaction_date (TIMESTAMP)?"
# MAGIC
# MAGIC **Wrong answer**: Yes, because it evenly distributes data by date.
# MAGIC
# MAGIC **Why wrong**: JDBC partitioning requires numeric column (INT, BIGINT). Timestamp is not supported. Ingestion will fail.
# MAGIC
# MAGIC **Workaround**: 
# MAGIC * Use a numeric column (ID) for partitioning
# MAGIC * Filter by date in a custom query (applied AFTER partitioning)
# MAGIC * Example: Partition on ID, custom query includes `WHERE transaction_date >= '2024-01-01'`
# MAGIC
# MAGIC **Another trap**: Over-parallelization.
# MAGIC
# MAGIC **Scenario**: "Source DB allows 8 connections. You configure 32 partitions. What happens?"
# MAGIC
# MAGIC **Answer**: Ingestion fails or is throttled. 32 partitions try to open 32 connections, exceeding limit.
# MAGIC
# MAGIC **Memory aid**: Partitions = connections. Match to source limit or go slightly under for safety.

# COMMAND ----------

# DBTITLE 1,Exercise 6 Solution: Decision Matrix
# MAGIC %md
# MAGIC ## Exercise 6 Solution: Decision Matrix - Ingestion Method Selection
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC Requirement 1:
# MAGIC - Method: Lakeflow Connect (standard JDBC connector)
# MAGIC - Justification: Source is MySQL database (not files). Daily batch with hourly latency 
# MAGIC   acceptable. Incremental loading with watermark column. Standard JDBC connector handles 
# MAGIC   all relational databases.
# MAGIC - Why not alternatives: Auto Loader and COPY INTO only work with files in cloud storage, 
# MAGIC   not databases. Managed connector only for SaaS apps, not MySQL.
# MAGIC
# MAGIC Requirement 2:
# MAGIC - Method: Auto Loader
# MAGIC - Justification: Source is files in S3 (not database). Continuous arrival (every 30 seconds). 
# MAGIC   Sub-minute latency required = streaming. Auto Loader provides exactly-once processing 
# MAGIC   with checkpoints. High file volume (1000 files/hour) is Auto Loader's strength.
# MAGIC - Why not alternatives: COPY INTO is batch only (not streaming). Lakeflow Connect doesn't 
# MAGIC   work with files. COPY INTO suitable for infrequent batch, not continuous streaming.
# MAGIC
# MAGIC Requirement 3:
# MAGIC - Method: COPY INTO
# MAGIC - Justification: Source is files in ADLS (not database). Weekly batch (infrequent). 
# MAGIC   Single large file. Simple use case without schema evolution needs. COPY INTO is simplest 
# MAGIC   for infrequent batch loads.
# MAGIC - Why not alternatives: Auto Loader is overkill for weekly batch (adds checkpoint overhead). 
# MAGIC   Lakeflow Connect doesn't work with files. Auto Loader better for high-frequency ingestion.
# MAGIC
# MAGIC Requirement 4:
# MAGIC - Method: Lakeflow Connect (managed Salesforce connector)
# MAGIC - Justification: Source is Salesforce (well-known SaaS app with pre-built connector). 
# MAGIC   Hourly sync supported. Need to capture updates and new records (upsert pattern). 
# MAGIC   Managed connector auto-discovers objects/fields, handles OAuth, rate limits, schema evolution.
# MAGIC - Why not alternatives: Standard JDBC won't work (Salesforce uses REST API). Auto Loader 
# MAGIC   and COPY INTO only for files. Custom code adds maintenance burden.
# MAGIC
# MAGIC Requirement 5:
# MAGIC - Method: Partner connector (Fivetran, Airbyte) OR custom CDC solution
# MAGIC - Justification: Real-time requirement (30 seconds) exceeds Lakeflow Connect minimum 
# MAGIC   frequency (hourly). Need true CDC streaming. Lakeflow Connect is batch-oriented 
# MAGIC   (scheduled), not event-driven.
# MAGIC - Why not alternatives: Lakeflow Connect minimum frequency is hourly (too slow). 
# MAGIC   Auto Loader is for files, not databases. Custom CDC solution requires significant 
# MAGIC   development (use partner if available).
# MAGIC ```
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Decision logic**:
# MAGIC 1. **Source type**: Database/API vs Files
# MAGIC    * Database/SaaS API → Lakeflow Connect
# MAGIC    * Files in cloud storage → Auto Loader or COPY INTO
# MAGIC
# MAGIC 2. **Latency requirement**:
# MAGIC    * Real-time (< 1 minute) → Auto Loader (files) or partner connector (databases)
# MAGIC    * Near real-time (minutes) → Auto Loader (files) or Lakeflow Connect with frequent sync
# MAGIC    * Hourly+ → Lakeflow Connect or COPY INTO
# MAGIC
# MAGIC 3. **File characteristics** (if source is files):
# MAGIC    * Continuous/high volume + streaming → Auto Loader
# MAGIC    * Infrequent batch → COPY INTO
# MAGIC    * Schema evolution needs → Auto Loader
# MAGIC
# MAGIC 4. **SaaS vs Database** (if source is database/API):
# MAGIC    * Well-known SaaS (Salesforce, Workday, ServiceNow) → Managed connector
# MAGIC    * Generic database (MySQL, PostgreSQL, Oracle) → Standard connector
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Using Lakeflow Connect for files or Auto Loader for databases.
# MAGIC
# MAGIC **Key distinction**:
# MAGIC * **Lakeflow Connect** = Connects TO external systems (pulls data)
# MAGIC * **Auto Loader / COPY INTO** = Reads FROM cloud storage (files already in S3/ADLS/GCS)
# MAGIC
# MAGIC **Exam signal phrases**:
# MAGIC * "Files land in S3/ADLS/GCS" → Auto Loader or COPY INTO (NOT Lakeflow Connect)
# MAGIC * "MySQL/PostgreSQL/Oracle database" → Lakeflow Connect standard connector
# MAGIC * "Salesforce/Workday/ServiceNow" → Lakeflow Connect managed connector
# MAGIC * "Sub-minute latency" with files → Auto Loader streaming
# MAGIC * "Sub-minute latency" with database → Partner connector or custom CDC
# MAGIC * "Weekly batch" with files → COPY INTO
# MAGIC
# MAGIC **Another trap**: Thinking Lakeflow Connect supports real-time.
# MAGIC
# MAGIC **Fact**: Lakeflow Connect minimum frequency is hourly. For sub-hour latency, use:
# MAGIC * Auto Loader (files with streaming)
# MAGIC * Partner connectors (Fivetran, Airbyte for databases)
# MAGIC * Custom CDC streaming solutions

# COMMAND ----------

# DBTITLE 1,Exercise 7 Solution: Managed Connector Configuration
# MAGIC %md
# MAGIC ## Exercise 7 Solution: Managed Connector Configuration
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. Connector type: Managed Salesforce connector
# MAGIC    Justification: Salesforce is a well-known SaaS application with pre-built managed 
# MAGIC    connector. Managed connector handles Salesforce REST API authentication, schema 
# MAGIC    auto-discovery, rate limits, and OAuth token refresh. Standard JDBC won't work 
# MAGIC    (Salesforce doesn't support JDBC protocol).
# MAGIC
# MAGIC 2. Authentication: OAuth 2.0 (or username/password + security token)
# MAGIC    OAuth is preferred. Connector handles token refresh automatically.
# MAGIC
# MAGIC 3. Filter at connector level: No
# MAGIC    How to filter: Ingest all Opportunities, then filter in Spark transformation:
# MAGIC    df = spark.table("salesforce_opportunities").filter("Amount > 50000")
# MAGIC    
# MAGIC    Managed connectors ingest entire objects. No WHERE clause support at connector level.
# MAGIC
# MAGIC 4. Custom field handling: On next sync, schema evolution automatically adds new column 
# MAGIC    ExpectedCloseQuarter__c to the Delta table. Existing rows have NULL for this column. 
# MAGIC    New rows populate the field. No manual intervention required.
# MAGIC
# MAGIC 5. Rate limit handling: Managed connector automatically throttles requests to stay within 
# MAGIC    API limits. Uses backoff and retry logic. If rate limit still exceeded (too many 
# MAGIC    objects or too frequent sync), connector waits and retries. You don't implement 
# MAGIC    custom rate limiting.
# MAGIC
# MAGIC 6. Nested address representation: Flattened into dot-notation columns:
# MAGIC    BillingAddress.street
# MAGIC    BillingAddress.city
# MAGIC    BillingAddress.state
# MAGIC    BillingAddress.zip
# MAGIC    
# MAGIC    Each nested field becomes a separate column. No JSON parsing needed in Spark.
# MAGIC
# MAGIC 7. 5-minute sync: No
# MAGIC    Limitations: Managed connectors support hourly minimum frequency. 5-minute sync 
# MAGIC    not supported. For near-real-time Salesforce ingestion, use partner streaming 
# MAGIC    connectors (Fivetran, Airbyte) or Salesforce Platform Events with custom code.
# MAGIC ```
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Managed connector key characteristics**:
# MAGIC 1. **Full object ingestion**: No field-level or row-level filtering at connector
# MAGIC 2. **Auto schema discovery**: Connector knows all fields (standard + custom)
# MAGIC 3. **Automatic flattening**: Nested objects become columnar (dot notation)
# MAGIC 4. **Schema evolution built-in**: New fields added automatically
# MAGIC 5. **Rate limit handling**: Managed by Databricks, not you
# MAGIC 6. **Hourly minimum**: Not real-time, scheduled batch ingestion
# MAGIC
# MAGIC **Why no filtering at connector**:
# MAGIC * Managed connectors are designed for simplicity, not customization
# MAGIC * You select OBJECTS (Account, Opportunity), not fields or rows
# MAGIC * Filtering happens downstream in Spark (after ingestion)
# MAGIC * Trade-off: Simpler configuration, less control
# MAGIC
# MAGIC **Schema evolution behavior**:
# MAGIC * New columns: Added automatically to Delta table
# MAGIC * Deleted columns: Remain in Delta table (with NULLs for new rows)
# MAGIC * Type changes: May require manual intervention (ALTER TABLE)
# MAGIC * Renamed columns: Appear as new column (old column remains)
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Expecting managed connectors to support custom queries or filtering.
# MAGIC
# MAGIC **Scenario**: "You need only Salesforce Accounts with Industry = 'Technology'. How do you configure the connector?"
# MAGIC
# MAGIC **Wrong answer**: Configure WHERE clause in connector settings.
# MAGIC
# MAGIC **Why wrong**: Managed connectors don't support WHERE clauses. You get entire objects.
# MAGIC
# MAGIC **Correct answer**: 
# MAGIC 1. Configure connector to ingest Account object
# MAGIC 2. Filter in Spark: `df.filter("Industry = 'Technology'")`
# MAGIC
# MAGIC **Another trap**: Thinking managed connectors are real-time.
# MAGIC
# MAGIC **Fact**: Minimum frequency is hourly. Managed connectors are batch-oriented, pulling data on schedule. Not event-driven.
# MAGIC
# MAGIC **For real-time Salesforce**:
# MAGIC * Use Salesforce Platform Events (event-driven streaming)
# MAGIC * Use partner connectors with CDC support
# MAGIC * Use custom code with Streaming API
# MAGIC
# MAGIC **Nested data handling (exam-critical)**:
# MAGIC
# MAGIC Managed connectors automatically flatten:
# MAGIC ```
# MAGIC API: { "Owner": { "Name": "John", "Email": "john@example.com" } }
# MAGIC Delta: Owner.Name = "John", Owner.Email = "john@example.com"
# MAGIC ```
# MAGIC
# MAGIC You do NOT need:
# MAGIC * `from_json()` to parse JSON
# MAGIC * `explode()` to flatten structures
# MAGIC * Custom parsing logic
# MAGIC
# MAGIC The flattening happens during ingestion.

# COMMAND ----------

# DBTITLE 1,Exercise 8 Solution: Schema Evolution
# MAGIC %md
# MAGIC ## Exercise 8 Solution: Schema Evolution
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. Week 2 outcome: New column loyalty_tier automatically added to target Delta table. 
# MAGIC    Existing rows (Week 1 data) have loyalty_tier = NULL. New rows (Week 2 data) have 
# MAGIC    populated loyalty_tier values. No manual intervention required.
# MAGIC
# MAGIC 2. Week 3 outcome: Column phone_number remains in Delta table but contains NULL for 
# MAGIC    all new rows (Week 3 onward). Existing rows (Week 1-2 data) retain their 
# MAGIC    phone_number values. Column is NOT dropped automatically.
# MAGIC
# MAGIC 3. Week 4 outcome: Cannot automatically handle. Data type change from STRING to DECIMAL 
# MAGIC    requires manual intervention:
# MAGIC    - Ingestion may fail with type mismatch error
# MAGIC    - OR new string values fail to write to DECIMAL column
# MAGIC    - Fix: ALTER TABLE to add new DECIMAL column, migrate data, drop old column
# MAGIC
# MAGIC 4. Expected behavior:
# MAGIC    loyalty_points (Week 2): 0, 150, 300 (existing customers have 0, new customers populated)
# MAGIC    loyalty_tier (Week 2): NULL for existing, 'Gold'/'Silver'/etc for new customers
# MAGIC    email (Week 3): 'cust1@ex.com' (from Week 1-2), NULL for Week 3+ (source removed it)
# MAGIC    phone_number (Week 3): '555-0100' (from Week 1-2), NULL for Week 3+ (source removed)
# MAGIC    discount_rate (Week 4): Requires manual fix - type change not auto-handled
# MAGIC
# MAGIC 5. Alternative handling: Use schema enforcement modes:
# MAGIC    - schema("mode") = "rescue": Store incompatible data in _rescued_data column
# MAGIC    - Alter table to DECIMAL before Week 4 ingestion
# MAGIC    - Build separate pipeline stage for type coercion
# MAGIC ```
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Schema evolution handling**:
# MAGIC
# MAGIC **New columns (additive changes)**:
# MAGIC * Automatically added to Delta table
# MAGIC * Existing rows: NULL for new columns
# MAGIC * New rows: Populated values
# MAGIC * Behavior: Seamless, no action needed
# MAGIC
# MAGIC **Removed columns (deletive changes)**:
# MAGIC * Columns NOT automatically dropped
# MAGIC * Existing data preserved
# MAGIC * New rows: NULL values
# MAGIC * Rationale: Dropping columns risks data loss; keep for safety
# MAGIC * Manual cleanup: `ALTER TABLE DROP COLUMN` if desired
# MAGIC
# MAGIC **Type changes (incompatible changes)**:
# MAGIC * NOT automatically handled
# MAGIC * May cause ingestion failure
# MAGIC * Requires manual intervention
# MAGIC * Options:
# MAGIC   1. Alter table to compatible type
# MAGIC   2. Add new typed column, deprecate old
# MAGIC   3. Use rescue column for incompatible data
# MAGIC
# MAGIC **Schema evolution modes** (Databricks Delta):
# MAGIC ```python
# MAGIC .option("mergeSchema", "true")  # Enable schema evolution
# MAGIC .option("rescuedDataColumn", "_rescued_data")  # Handle unparseable data
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Thinking removed source columns are automatically dropped from target.
# MAGIC
# MAGIC **Scenario**: "Source removes column address. What happens in target Delta table?"
# MAGIC
# MAGIC **Wrong answer**: Column address is automatically dropped from Delta table.
# MAGIC
# MAGIC **Why wrong**: Delta tables never auto-drop columns (data loss risk). Column remains, new rows have NULL.
# MAGIC
# MAGIC **Correct behavior**:
# MAGIC * Column persists in Delta schema
# MAGIC * Historical data (existing rows) retains address values
# MAGIC * New data (after column removed) has address = NULL
# MAGIC * Manual cleanup required if you want to drop column
# MAGIC
# MAGIC **Another trap**: Type change assumptions.
# MAGIC
# MAGIC **Scenario**: "Source changes column from INT to STRING. Lakeflow Connect handles this automatically, true or false?"
# MAGIC
# MAGIC **Answer**: False
# MAGIC
# MAGIC **Why**: Type widening (INT to STRING) might work in some contexts, but type changes generally require manual handling. Delta Lake is strict about schema. Ingestion may fail or require rescue column.
# MAGIC
# MAGIC **Handling approach** (exam-relevant):
# MAGIC 1. Detect type change (monitoring, schema tracking)
# MAGIC 2. Add new column with new type: `discount_rate_decimal`
# MAGIC 3. Migrate data: `UPDATE SET discount_rate_decimal = CAST(discount_rate AS DECIMAL)`
# MAGIC 4. Deprecate old column (or drop after migration)
# MAGIC 5. Update downstream queries
# MAGIC
# MAGIC **Memory aid**:
# MAGIC * Additive (new columns) = AUTO-YES
# MAGIC * Deletive (removed columns) = NO-DROP (columns stay)
# MAGIC * Mutative (type changes) = MANUAL-FIX

# COMMAND ----------

# DBTITLE 1,Exercise 9 Solution: Custom Query at Source
# MAGIC %md
# MAGIC ## Exercise 9 Solution: Custom Query at Source
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. Custom query:
# MAGIC SELECT 
# MAGIC     o.order_id,
# MAGIC     o.order_date,
# MAGIC     o.customer_id,
# MAGIC     c.customer_name,
# MAGIC     c.customer_email,
# MAGIC     o.order_total,
# MAGIC     o.status
# MAGIC FROM orders o
# MAGIC INNER JOIN customers c ON o.customer_id = c.customer_id
# MAGIC WHERE o.order_date >= '2024-01-01'
# MAGIC     AND o.status IN ('completed', 'shipped')
# MAGIC
# MAGIC 2. Benefits of custom query:
# MAGIC    - Reduced data transfer: Filter 90% of rows at source (only 2024 data, exclude 
# MAGIC      pending/cancelled). Transfer 500K rows instead of 5M.
# MAGIC    - Single query: Join at source eliminates need for separate customer table ingestion 
# MAGIC      and Spark join. Simpler pipeline.
# MAGIC    - Predicate pushdown: Filtering happens in source MySQL (uses MySQL indexes if available). 
# MAGIC      Much faster than transferring 5M rows then filtering in Spark.
# MAGIC    - Bandwidth savings: Fewer rows = less network traffic between source and Databricks.
# MAGIC
# MAGIC 3. Trade-offs:
# MAGIC    - Source database load: JOIN and WHERE processing happens on source MySQL server. 
# MAGIC      Adds CPU/IO load. May impact production workloads if source is heavily used.
# MAGIC    - Less flexibility: Query is static. To change date filter or status list, must 
# MAGIC      reconfigure connector. Separate table ingestion allows dynamic Spark filtering.
# MAGIC    - No incremental loading: Custom queries make incremental loading harder to configure. 
# MAGIC      Watermark column must be in SELECT, and JOIN may complicate watermark tracking.
# MAGIC
# MAGIC 4. Query must be valid for: MySQL (source database)
# MAGIC    Why: Lakeflow Connect sends query directly to source database. Source executes it.
# MAGIC    MySQL-specific considerations: Use MySQL date format, functions (DATE_ADD, etc.), 
# MAGIC    no Spark SQL functions (no date_format, no array functions). Use MySQL JOIN syntax.
# MAGIC
# MAGIC 5. Incremental loading configuration:
# MAGIC    - Watermark column: order_date (must be in SELECT list)
# MAGIC    - Subsequent queries will be: SELECT ... WHERE order_date > 'last_watermark' AND ...
# MAGIC    - Lakeflow Connect appends watermark condition to existing WHERE clause
# MAGIC    - Challenge: If JOIN returns rows with different order_date than orders table 
# MAGIC      (shouldn't happen with INNER JOIN on single order_date), tracking gets complex
# MAGIC ```
# MAGIC
# MAGIC ### Explanation
# MAGIC
# MAGIC **Why custom queries improve performance**:
# MAGIC 1. **Predicate pushdown**: Filtering at source uses source database indexes
# MAGIC 2. **Reduced data transfer**: Only relevant rows sent over network
# MAGIC 3. **Join at source**: Avoids separate ingestion + Spark join
# MAGIC 4. **Source optimization**: Source query optimizer can use statistics, indexes
# MAGIC
# MAGIC **When to use custom queries**:
# MAGIC * Need to filter majority of rows (reduce transfer)
# MAGIC * Need to join multiple source tables
# MAGIC * Need to cast/transform data types at source
# MAGIC * Source has good indexes on filter columns
# MAGIC
# MAGIC **When NOT to use custom queries**:
# MAGIC * Need flexibility (changing filters frequently)
# MAGIC * Source database under heavy load (avoid extra processing)
# MAGIC * Complex incremental loading logic
# MAGIC * Need to preserve raw source data (custom query is transformative)
# MAGIC
# MAGIC **SQL dialect considerations** (exam-critical):
# MAGIC * Custom query must be valid for SOURCE database, not Spark SQL
# MAGIC * MySQL syntax != PostgreSQL syntax != SQL Server syntax
# MAGIC * Example differences:
# MAGIC   * String concatenation: MySQL uses `CONCAT()`, PostgreSQL uses `||`, SQL Server uses `+`
# MAGIC   * Date functions: `DATE_ADD(date, INTERVAL 1 DAY)` in MySQL, `date + INTERVAL '1 day'` in PostgreSQL
# MAGIC   * LIMIT clause: `LIMIT 100` in MySQL/PostgreSQL, `TOP 100` in SQL Server
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Common mistake**: Writing custom query in Spark SQL syntax.
# MAGIC
# MAGIC **Scenario**: "You configure a custom query for PostgreSQL source:
# MAGIC ```sql
# MAGIC SELECT *, date_format(order_date, 'yyyy-MM-dd') as formatted_date
# MAGIC FROM orders
# MAGIC ```
# MAGIC What happens?"
# MAGIC
# MAGIC **Answer**: Query fails. `date_format()` is a Spark SQL function, not PostgreSQL. PostgreSQL uses `to_char(order_date, 'YYYY-MM-DD')`.
# MAGIC
# MAGIC **Why**: Lakeflow Connect sends query directly to source. Source database executes it. Must use source's SQL dialect.
# MAGIC
# MAGIC **Another trap**: Forgetting watermark column in SELECT.
# MAGIC
# MAGIC **Scenario**: "You configure incremental loading with watermark_column = 'updated_at'. Custom query:
# MAGIC ```sql
# MAGIC SELECT order_id, customer_id, total FROM orders WHERE status = 'completed'
# MAGIC ```
# MAGIC What happens?"
# MAGIC
# MAGIC **Answer**: Incremental loading fails. Watermark column `updated_at` not in SELECT list. Lakeflow Connect can't track maximum value.
# MAGIC
# MAGIC **Fix**: Add watermark column:
# MAGIC ```sql
# MAGIC SELECT order_id, customer_id, total, updated_at FROM orders WHERE status = 'completed'
# MAGIC ```
# MAGIC
# MAGIC **Memory aid**: Custom query = source's SQL dialect, not Spark. Test query directly in source database before using in connector.

# COMMAND ----------

# DBTITLE 1,Exercises 10-12 Solutions
# MAGIC %md
# MAGIC ## Exercise 10 Solution: Upsert vs Append Decision
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC Table: user_sessions
# MAGIC Decision: Incremental Append
# MAGIC Watermark: session_start_time (immutable)
# MAGIC Justification: Sessions are immutable after creation. session_end_time and duration_seconds 
# MAGIC are calculated once when session ends, then never change. No updates occur. Upsert adds 
# MAGIC MERGE overhead (reading target table) with no benefit. Append is faster and simpler for 
# MAGIC immutable data.
# MAGIC
# MAGIC Table: product_inventory  
# MAGIC Decision: Incremental Upsert
# MAGIC Watermark: last_updated (tracks changes)
# MAGIC Primary key: product_id  
# MAGIC Justification: Inventory changes constantly (quantity_available, reorder_status). Need 
# MAGIC current state, not history of every change. Upsert ensures each product_id appears once 
# MAGIC with latest values. Append would create duplicate product_id rows (unusable). Full load 
# MAGIC would re-read 50K products every sync (wasteful).
# MAGIC
# MAGIC Table: clickstream_events
# MAGIC Decision: Incremental Append
# MAGIC Watermark: event_timestamp (immutable)
# MAGIC Justification: Clickstream events are immutable logs. Each event recorded once, never 
# MAGIC modified. user_id and session_id don't change for an event. High volume (millions/day) 
# MAGIC requires fastest ingestion - append with no MERGE overhead.
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 11 Solution: Performance Troubleshooting
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. Root causes:
# MAGIC    - Single-threaded ingestion: No parallelization configured. Reading 20M rows 
# MAGIC      sequentially in one connection.
# MAGIC    - Default fetch size: Likely 100 rows per JDBC round trip = 200K round trips for 
# MAGIC      20M rows. Massive network overhead.
# MAGIC    - No predicate pushdown: Reading entire table even if only recent data needed.
# MAGIC
# MAGIC 2. Recommendations:
# MAGIC    Priority 1 - Enable parallelization:
# MAGIC    - partition_column: user_id (BIGINT, auto-increment)
# MAGIC    - num_partitions: 16 (source allows 20 connections, use 16 for safety)
# MAGIC    - This alone reduces time from 4 hours to ~15 minutes (16x parallelism)
# MAGIC    
# MAGIC    Priority 2 - Increase fetch size:
# MAGIC    - fetch_size: 50,000 rows per fetch
# MAGIC    - Reduces JDBC round trips from 200K to 400
# MAGIC    - Cuts network overhead by 99.5%
# MAGIC    
# MAGIC    Priority 3 - Incremental loading:
# MAGIC    - If only recent data needed, use watermark (created_at or updated_at)
# MAGIC    - Avoids reading 20M rows every sync
# MAGIC    - Reads only changed/new rows
# MAGIC
# MAGIC 3. Verification:
# MAGIC    - Monitor connector metrics: Check parallelism level (should show 16 tasks)
# MAGIC    - Check source database: Monitor active connections (should see 16 concurrent)
# MAGIC    - Measure ingestion time: Should drop to 10-20 minutes
# MAGIC    - Check data completeness: Row count should match source
# MAGIC
# MAGIC 4. Additional considerations:
# MAGIC    - Source database load: 16 concurrent queries add CPU/IO load. Monitor source 
# MAGIC      during ingestion. Scale back partitions if source struggles.
# MAGIC    - Network bandwidth: More parallelism = more throughput. Ensure network can handle.
# MAGIC    - Memory: Higher fetch size = more memory per executor. Monitor for OOM errors.
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 12 Solution: Unity Catalog Integration
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. Full table path: analytics_prod.sales.salesforce_opportunities
# MAGIC    - Catalog: analytics_prod
# MAGIC    - Schema: sales  
# MAGIC    - Table: salesforce_opportunities
# MAGIC
# MAGIC 2. Required permissions:
# MAGIC    - Catalog level: USE CATALOG on analytics_prod
# MAGIC    - Schema level: USE SCHEMA and CREATE TABLE on sales schema
# MAGIC    - Rationale: Lakeflow Connect needs to create table in schema. USE grants are required 
# MAGIC      to access catalog and schema.
# MAGIC
# MAGIC 3. Grant statements:
# MAGIC    GRANT USE CATALOG ON CATALOG analytics_prod TO `connector_service_principal`;
# MAGIC    GRANT USE SCHEMA ON SCHEMA analytics_prod.sales TO `connector_service_principal`;
# MAGIC    GRANT CREATE TABLE ON SCHEMA analytics_prod.sales TO `connector_service_principal`;
# MAGIC    
# MAGIC    Note: Replace connector_service_principal with actual identity running the connector.
# MAGIC
# MAGIC 4. Tags and comments:
# MAGIC    - Table comment: Add during/after ingestion with ALTER TABLE:
# MAGIC      ALTER TABLE analytics_prod.sales.salesforce_opportunities 
# MAGIC      SET TBLPROPERTIES ('comment' = 'Salesforce Opportunity data, synced hourly');
# MAGIC    
# MAGIC    - PII tag: Apply to email columns:
# MAGIC      ALTER TABLE analytics_prod.sales.salesforce_opportunities 
# MAGIC      ALTER COLUMN contact_email SET TAGS ('PII' = 'email');
# MAGIC    
# MAGIC    These operations require MODIFY permission on the table.
# MAGIC
# MAGIC 5. Managed vs external tables:
# MAGIC    - Lakeflow Connect creates: Managed tables (by default)
# MAGIC    - Behavior: Data stored in Unity Catalog managed location 
# MAGIC      (e.g., s3://catalog-bucket/analytics_prod/sales/salesforce_opportunities/)
# MAGIC    - DROP TABLE: Deletes both metadata and data files
# MAGIC    - Advantage: UC manages lifecycle, simpler governance
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Exercises 13-15 Solutions
# MAGIC %md
# MAGIC ## Exercise 13 Solution: Scheduling and Orchestration
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. Schedule configuration:
# MAGIC    - Lakeflow Connect schedule: Every 4 hours (or hourly if 4-hour not available)
# MAGIC    - Alternatives: Use Lakeflow Jobs to trigger connector on a cron schedule: 
# MAGIC      0 */4 * * * (every 4 hours)
# MAGIC
# MAGIC 2. Lakeflow Jobs workflow:
# MAGIC    Task 1: Run Lakeflow Connect ingestion
# MAGIC    - Type: Lakeflow Connect pipeline task
# MAGIC    - Connection: MySQL source configuration
# MAGIC    - Target: raw_layer.sales.orders
# MAGIC    
# MAGIC    Task 2: Aggregate data
# MAGIC    - Type: Notebook task
# MAGIC    - Depends on: Task 1 (wait for ingestion)
# MAGIC    - Notebook: /path/to/aggregation_notebook
# MAGIC    - Logic:
# MAGIC      spark.sql("""
# MAGIC      CREATE OR REPLACE TABLE aggregated_layer.sales.daily_orders AS
# MAGIC      SELECT 
# MAGIC        DATE(order_date) as order_day,
# MAGIC        COUNT(*) as total_orders,
# MAGIC        SUM(order_total) as total_revenue
# MAGIC      FROM raw_layer.sales.orders
# MAGIC      GROUP BY DATE(order_date)
# MAGIC      """)
# MAGIC    
# MAGIC    Task 3: Generate report
# MAGIC    - Type: Notebook task  
# MAGIC    - Depends on: Task 2 (wait for aggregation)
# MAGIC    - Notebook: /path/to/reporting_notebook
# MAGIC    - Delivers: Dashboard update or exported file
# MAGIC
# MAGIC 3. Task dependencies:
# MAGIC    Task 1 → Task 2 → Task 3 (linear dependency chain)
# MAGIC    Ensures: Ingestion completes before aggregation, aggregation completes before reporting
# MAGIC
# MAGIC 4. Handling ingestion failures:
# MAGIC    - Retry configuration: Set max_retries = 3 for Task 1
# MAGIC    - Alert: Configure email/Slack alert on Task 1 failure
# MAGIC    - Downstream behavior: Tasks 2 and 3 will NOT run if Task 1 fails (dependency blocks)
# MAGIC    - Recovery: Fix source issue, manually re-run job
# MAGIC
# MAGIC 5. Alternative: Event-driven orchestration
# MAGIC    - If Lakeflow Connect supported event triggers (currently doesn't), could trigger 
# MAGIC      downstream tasks immediately after ingestion completes
# MAGIC    - Current limitation: Schedule-based only, not event-driven
# MAGIC    - Workaround: Use hourly schedule + data freshness checks in aggregation task
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 14 Solution: Semi-Structured Data Handling
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. Target schema:
# MAGIC    CREATE TABLE raw_layer.events.api_events (
# MAGIC      event_id STRING,
# MAGIC      event_type STRING,
# MAGIC      timestamp TIMESTAMP,
# MAGIC      user_id STRING,
# MAGIC      user_name STRING,
# MAGIC      user_email STRING,
# MAGIC      user_country STRING,
# MAGIC      properties_page_url STRING,
# MAGIC      properties_referrer STRING,
# MAGIC      properties_duration_seconds INT,
# MAGIC      tags ARRAY<STRING>
# MAGIC    )
# MAGIC    
# MAGIC    Note: Managed connectors flatten nested objects using dot notation.
# MAGIC
# MAGIC 2. Managed connector behavior:
# MAGIC    - user object: Flattened to user_id, user_name, user_email, user_country
# MAGIC    - properties object: Flattened to properties_page_url, properties_referrer, 
# MAGIC      properties_duration_seconds
# MAGIC    - tags array: Preserved as ARRAY<STRING> (arrays not flattened)
# MAGIC    - No manual from_json() or explode() needed
# MAGIC
# MAGIC 3. Schema evolution: New field properties.session_id
# MAGIC    - Outcome: New column properties_session_id added automatically to Delta table
# MAGIC    - Existing rows: Have properties_session_id = NULL
# MAGIC    - New rows: Have populated properties_session_id values
# MAGIC    - No manual schema migration needed
# MAGIC
# MAGIC 4. Query to extract user email:
# MAGIC    SELECT user_email FROM raw_layer.events.api_events;
# MAGIC    
# MAGIC    (Not user.email - already flattened by connector)
# MAGIC
# MAGIC 5. If using Auto Loader instead:
# MAGIC    - Need manual parsing:
# MAGIC      df = spark.read.format("cloudFiles") \
# MAGIC        .option("cloudFiles.format", "json") \
# MAGIC        .load("s3://bucket/events/")
# MAGIC      
# MAGIC      from pyspark.sql.functions import col
# MAGIC      df_flattened = df.select(
# MAGIC        col("event_id"),
# MAGIC        col("event_type"),
# MAGIC        col("timestamp"),
# MAGIC        col("user.id").alias("user_id"),
# MAGIC        col("user.name").alias("user_name"),
# MAGIC        col("user.email").alias("user_email"),
# MAGIC        col("properties.page_url").alias("properties_page_url"),
# MAGIC        col("tags")
# MAGIC      )
# MAGIC    
# MAGIC    - More control but more code. Managed connector simpler for standard flattening.
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Exercise 15 Solution: Error Handling and Troubleshooting
# MAGIC
# MAGIC ### Complete Answer
# MAGIC
# MAGIC ```
# MAGIC 1. Authentication failure:
# MAGIC    - Error: "Access denied for user 'dbuser'@'<databricks_ip>'"
# MAGIC    - Diagnosis steps:
# MAGIC      a) Verify credentials in Databricks Secrets (check scope/key names)
# MAGIC      b) Test credentials directly against MySQL from other client
# MAGIC      c) Check MySQL user grants: GRANT SELECT ON sales_db.* TO 'dbuser'@'%';
# MAGIC      d) Verify network connectivity (can Databricks reach MySQL host?)
# MAGIC    - Fix: Update password in Secrets, or fix MySQL user permissions
# MAGIC
# MAGIC 2. Connection timeout:
# MAGIC    - Error: "Connection timed out after 30000ms"
# MAGIC    - Diagnosis:
# MAGIC      a) Check network connectivity: Can Databricks VPC reach MySQL?
# MAGIC      b) Verify security groups/firewall rules allow inbound on port 3306
# MAGIC      c) Check if MySQL configured to accept remote connections (bind-address setting)
# MAGIC      d) Test from Databricks notebook: %sh nc -zv mysql-host 3306
# MAGIC    - Fix: Update firewall rules, or use VPC peering/private link for connectivity
# MAGIC
# MAGIC 3. Incremental loading not working:
# MAGIC    - Symptoms: Every sync reads all 10M rows (full load behavior)
# MAGIC    - Diagnosis:
# MAGIC      a) Verify watermark column is correctly configured
# MAGIC      b) Check if watermark column has NULL values (breaks incremental logic)
# MAGIC      c) Verify watermark column is monotonically increasing
# MAGIC      d) Check connector state storage (watermark value being persisted?)
# MAGIC    - Likely cause: Watermark column has NULLs or gaps, causing fallback to full load
# MAGIC    - Fix: Use non-nullable watermark column, or fill NULLs with default value
# MAGIC
# MAGIC 4. Too many connections error:
# MAGIC    - Error: "Too many connections (max: 8)"
# MAGIC    - Diagnosis: Parallelization set to num_partitions=16, exceeds max_connections=8
# MAGIC    - Fix: Reduce partitions to 8 or fewer:
# MAGIC      num_partitions = 6  # Stay under limit for safety
# MAGIC
# MAGIC 5. Schema mismatch:
# MAGIC    - Error: "Column 'discount_rate' is type STRING in source but DECIMAL in target"
# MAGIC    - Diagnosis: Source changed data type, Delta table expects original type
# MAGIC    - Fix:
# MAGIC      a) Short-term: Disable connector, fix source schema, re-enable
# MAGIC      b) Long-term: Alter table to compatible type, or add new typed column:
# MAGIC         ALTER TABLE ADD COLUMN discount_rate_new DECIMAL(5,2);
# MAGIC         Migrate data, deprecate old column
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,MCQ 1-3 Solutions
# MAGIC %md
# MAGIC ## MCQ 1 Solution: Connector Selection for Multiple Sources
# MAGIC
# MAGIC ### Correct Answer: C
# MAGIC
# MAGIC **Complete breakdown**:
# MAGIC * MySQL: Standard JDBC connector (relational database)
# MAGIC * Salesforce: Managed Salesforce connector (well-known SaaS with pre-built integration)
# MAGIC * Workday: Managed Workday connector (well-known SaaS with pre-built integration)
# MAGIC
# MAGIC ### Why Other Options Are Wrong
# MAGIC
# MAGIC **Option A: Standard connectors for all**
# MAGIC * Wrong because: Salesforce and Workday don't support JDBC. They use REST APIs with OAuth authentication. Standard JDBC connector cannot authenticate with OAuth or parse their API responses.
# MAGIC
# MAGIC **Option B: Managed connectors for all**
# MAGIC * Wrong because: MySQL doesn't have a managed connector. Managed connectors only exist for specific SaaS applications. MySQL is a generic relational database.
# MAGIC
# MAGIC **Option D: Managed for MySQL and Workday, standard for Salesforce**
# MAGIC * Wrong because: Exactly backwards. MySQL needs standard JDBC, Salesforce needs managed connector.
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC Remember the distinction:
# MAGIC * **Standard JDBC = Relational databases** (MySQL, PostgreSQL, Oracle, SQL Server, etc.)
# MAGIC * **Managed = Well-known SaaS brands** (Salesforce, Workday, ServiceNow, HubSpot, NetSuite, etc.)
# MAGIC
# MAGIC If you see a recognizable SaaS brand name, it's managed. If you see a database name or "custom internal application," it's standard JDBC (if JDBC-compatible).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## MCQ 2 Solution: Incremental Loading Configuration
# MAGIC
# MAGIC ### Correct Answer: C
# MAGIC
# MAGIC **Complete breakdown**:
# MAGIC * Pattern: Incremental Upsert (MERGE)
# MAGIC * Watermark column: `updated_at` (tracks changes including updates)
# MAGIC * Primary key: `product_id` (unique identifier for matching)
# MAGIC
# MAGIC ### Why Other Options Are Wrong
# MAGIC
# MAGIC **Option A: Full load**
# MAGIC * Wrong because: Product catalog is 2M rows (large). Full load would re-read 2M rows every sync even if only 1000 products changed. Extremely inefficient. Takes hours instead of minutes.
# MAGIC
# MAGIC **Option B: Incremental append with `product_id`**
# MAGIC * Wrong because: 
# MAGIC   1. `product_id` is not a watermark (doesn't track time/changes)
# MAGIC   2. Append creates duplicate `product_id` rows when products are updated
# MAGIC   3. You'd have multiple rows per product instead of current state
# MAGIC
# MAGIC **Option D: Incremental upsert with `created_at`**
# MAGIC * Wrong because: `created_at` only tracks NEW products, not updates. If a product's price or description changes, `created_at` doesn't change. You'd miss all updates, only capturing new products.
# MAGIC
# MAGIC ### Why C Is Correct
# MAGIC
# MAGIC 1. **Upsert captures updates**: Inventory and pricing change frequently, need current state
# MAGIC 2. **`updated_at` tracks changes**: Any product modification updates this timestamp
# MAGIC 3. **`product_id` enables MERGE**: Unique key for matching existing rows
# MAGIC 4. **Efficient**: Only reads changed products (1000/day), not full 2M rows
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Watermark column decision tree**:
# MAGIC * Source is append-only (no updates) → Use immutable timestamp (`created_at`)
# MAGIC * Source has updates, need current state → Use change-tracking timestamp (`updated_at`, `last_modified`)
# MAGIC * Source has no timestamps → Full load (or add timestamp column)
# MAGIC
# MAGIC **Common trap**: Using `created_at` for upsert scenarios. This only works for append-only data.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## MCQ 3 Solution: Performance Optimization
# MAGIC
# MAGIC ### Correct Answer: D
# MAGIC
# MAGIC **Complete breakdown**:
# MAGIC All three actions improve performance:
# MAGIC 1. **Parallelization**: 20 partitions = 20 concurrent reads = 20x faster (assuming sufficient cores and connections)
# MAGIC 2. **Increased fetch size**: Reduces JDBC round trips from ~1M (10M rows / 10 default fetch size) to ~200 (10M / 50K fetch size) = 5000x fewer network calls
# MAGIC 3. **Custom query with WHERE clause**: Filters at source, reduces data transfer from 10M rows to subset (only relevant date range)
# MAGIC
# MAGIC ### Why Other Options Are Wrong
# MAGIC
# MAGIC **Option A: Only parallelization**
# MAGIC * Incomplete: Parallelization helps, but still has excessive JDBC round trips and unnecessary data transfer
# MAGIC
# MAGIC **Option B: Only fetch size**
# MAGIC * Incomplete: Reduces round trips, but still single-threaded and transfers unnecessary data
# MAGIC
# MAGIC **Option C: Only custom query**
# MAGIC * Incomplete: Reduces data transfer, but still single-threaded with excessive round trips
# MAGIC
# MAGIC ### Why D Is Correct
# MAGIC
# MAGIC Each optimization addresses a different bottleneck:
# MAGIC * **Parallelization** → Fixes single-threaded bottleneck (uses multiple cores)
# MAGIC * **Fetch size** → Fixes network round-trip overhead (batch communication)
# MAGIC * **Custom query** → Fixes unnecessary data transfer (filter at source)
# MAGIC
# MAGIC Combining all three provides multiplicative performance gains.
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Performance optimization hierarchy** for JDBC ingestion:
# MAGIC
# MAGIC 1. **First: Incremental loading** (biggest impact - read only changed data)
# MAGIC 2. **Second: Parallelization** (use multiple connections)
# MAGIC 3. **Third: Increase fetch size** (reduce round trips)
# MAGIC 4. **Fourth: Predicate pushdown** (custom query with WHERE)
# MAGIC
# MAGIC Don't stop at one optimization when multiple bottlenecks exist. The question phrase "still taking 2 hours after parallelization" signals you need additional optimizations.
# MAGIC
# MAGIC **Red flag phrases** (signals multiple fixes needed):
# MAGIC * "Still slow after..."
# MAGIC * "Despite parallelization..."
# MAGIC * "Even with..."
# MAGIC
# MAGIC These indicate one fix isn't enough - look for combination answers like "all of the above."

# COMMAND ----------

# DBTITLE 1,MCQ 4-5 Solutions
# MAGIC %md
# MAGIC ## MCQ 4 Solution: Schema Evolution
# MAGIC
# MAGIC ### Correct Answer: B
# MAGIC
# MAGIC **Complete breakdown**:
# MAGIC * New column `loyalty_tier` is automatically added to target table
# MAGIC * Existing rows (from previous syncs) have `loyalty_tier = NULL`
# MAGIC * New rows (current sync onward) have populated `loyalty_tier` values
# MAGIC * No manual intervention required
# MAGIC
# MAGIC ### Why Other Options Are Wrong
# MAGIC
# MAGIC **Option A: Ingestion fails, schema mismatch**
# MAGIC * Wrong because: Lakeflow Connect supports automatic schema evolution (additive changes). New columns are added automatically, not blocked.
# MAGIC
# MAGIC **Option C: New column ignored, not added**
# MAGIC * Wrong because: Schema evolution is enabled by default. New columns aren't silently dropped - they're added to maintain schema consistency.
# MAGIC
# MAGIC **Option D: Manual ALTER TABLE required**
# MAGIC * Wrong because: Automatic schema evolution means Lakeflow Connect handles the ALTER TABLE internally. You don't need manual intervention for additive changes.
# MAGIC
# MAGIC ### Why B Is Correct
# MAGIC
# MAGIC **Schema evolution behavior for new columns**:
# MAGIC 1. Connector detects new column in source
# MAGIC 2. Automatically adds column to Delta table schema
# MAGIC 3. Existing rows retain their values (don't have new column, so NULL)
# MAGIC 4. New rows populate the column
# MAGIC 5. Result: Backward-compatible schema change
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Schema evolution outcomes by change type**:
# MAGIC
# MAGIC | Change Type | Outcome | Manual Action |
# MAGIC |-------------|---------|---------------|
# MAGIC | New column (additive) | Auto-added, existing rows = NULL | None |
# MAGIC | Removed column (deletive) | Column remains, new rows = NULL | Optional cleanup |
# MAGIC | Type change (mutative) | May fail or require rescue | Required |
# MAGIC | Renamed column | Appears as new column + old remains | Manual migration |
# MAGIC
# MAGIC **Memory aid**: Additive = AUTO-ADD, Deletive = KEEP-COLUMN, Mutative = MANUAL-FIX
# MAGIC
# MAGIC **Common trap**: Thinking schema evolution requires manual intervention for new columns. It doesn't - it's automatic for additive changes.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## MCQ 5 Solution: Ingestion Method Decision
# MAGIC
# MAGIC ### Correct Answer: D
# MAGIC
# MAGIC **Complete breakdown**:
# MAGIC * **Scenario 1**: CSV files in S3 + hourly batch → COPY INTO
# MAGIC * **Scenario 2**: PostgreSQL + daily batch → Lakeflow Connect (standard JDBC)
# MAGIC * **Scenario 3**: Salesforce + hourly sync → Lakeflow Connect (managed connector)
# MAGIC
# MAGIC ### Why Other Options Are Wrong
# MAGIC
# MAGIC **Option A: Lakeflow Connect for all**
# MAGIC * Wrong because: Scenario 1 is files in S3. Lakeflow Connect doesn't read files - it connects to databases/APIs. For files, use Auto Loader or COPY INTO.
# MAGIC
# MAGIC **Option B: Auto Loader for Scenario 1, Lakeflow Connect for 2 and 3**
# MAGIC * Wrong because: Auto Loader is for streaming or high-frequency file ingestion. Scenario 1 is infrequent batch (hourly). COPY INTO is simpler and sufficient. Auto Loader adds unnecessary streaming overhead.
# MAGIC
# MAGIC **Option C: COPY INTO for 1, Lakeflow Connect for 2, Auto Loader for 3**
# MAGIC * Wrong because: Auto Loader doesn't work with Salesforce. Salesforce is an API-based SaaS application, not files. Need Lakeflow Connect managed connector.
# MAGIC
# MAGIC ### Why D Is Correct
# MAGIC
# MAGIC **Scenario 1 decision**: CSV files + hourly batch
# MAGIC * Files in cloud storage → Auto Loader or COPY INTO
# MAGIC * Infrequent batch → COPY INTO (simpler, no streaming overhead)
# MAGIC * No schema evolution mentioned → COPY INTO sufficient
# MAGIC
# MAGIC **Scenario 2 decision**: PostgreSQL + daily batch
# MAGIC * Database source → Lakeflow Connect (not files)
# MAGIC * Standard database → Standard JDBC connector
# MAGIC * Daily batch → Lakeflow Connect handles scheduling
# MAGIC
# MAGIC **Scenario 3 decision**: Salesforce + hourly sync
# MAGIC * SaaS application → Lakeflow Connect (not files)
# MAGIC * Well-known SaaS → Managed connector exists
# MAGIC * Hourly sync → Within Lakeflow Connect capabilities (minimum frequency)
# MAGIC
# MAGIC ### Exam Tip
# MAGIC
# MAGIC **Decision matrix quick reference**:
# MAGIC
# MAGIC | Source Type | Frequency | Method |
# MAGIC |-------------|-----------|--------|
# MAGIC | Files in cloud storage | Continuous/high | Auto Loader |
# MAGIC | Files in cloud storage | Infrequent batch | COPY INTO |
# MAGIC | Relational database | Any batch schedule | Lakeflow Connect (standard) |
# MAGIC | SaaS app (Salesforce, etc.) | Hourly+ | Lakeflow Connect (managed) |
# MAGIC | Any source | Real-time (< 1 min) | Partner connector or custom |
# MAGIC
# MAGIC **Key distinctions**:
# MAGIC * Lakeflow Connect = Connects TO external systems (pulls data via JDBC/API)
# MAGIC * Auto Loader / COPY INTO = Reads files ALREADY IN cloud storage (S3/ADLS/GCS)
# MAGIC * Never use Lakeflow Connect for files, never use Auto Loader/COPY INTO for databases
# MAGIC
# MAGIC **Exam signal phrases**:
# MAGIC * "Files land in S3/ADLS" → NOT Lakeflow Connect
# MAGIC * "Database/API" → NOT Auto Loader or COPY INTO
# MAGIC * "Well-known SaaS" (Salesforce, Workday) → Managed connector
# MAGIC * "Hourly or less frequent" + files → COPY INTO over Auto Loader

# COMMAND ----------

# DBTITLE 1,Challenge 1 Solution
# MAGIC %md
# MAGIC ## Challenge 1 Solution: Multi-Source Integration Pipeline
# MAGIC
# MAGIC ### Complete Architecture Design
# MAGIC
# MAGIC ```yaml
# MAGIC Pipeline: Multi-Source Sales Integration
# MAGIC
# MAGIC Sources:
# MAGIC   1. MySQL (transactional_db.orders_raw)
# MAGIC   2. Salesforce (Opportunity, Account objects)
# MAGIC   3. S3 (s3://bucket/customer-uploads/)
# MAGIC
# MAGIC Target Schema: raw_layer (one table per source)
# MAGIC              + integrated_layer (unified view)
# MAGIC
# MAGIC Components:
# MAGIC   - 3 Lakeflow Connect pipelines (MySQL, Salesforce, S3 ingestion)
# MAGIC   - 1 Integration notebook (joins and standardization)
# MAGIC   - 1 Lakeflow Job (orchestration)
# MAGIC ```
# MAGIC
# MAGIC ### Step-by-Step Implementation
# MAGIC
# MAGIC #### 1. MySQL Ingestion Configuration
# MAGIC
# MAGIC ```yaml
# MAGIC Connector: Lakeflow Connect (standard JDBC)
# MAGIC Source: MySQL transactional_db.orders_raw
# MAGIC Target: raw_layer.mysql.orders
# MAGIC
# MAGIC Configuration:
# MAGIC   connection_string: jdbc:mysql://mysql-prod.company.com:3306/transactional_db
# MAGIC   username: {{secrets.get("jdbc_prod", "mysql_user")}}
# MAGIC   password: {{secrets.get("jdbc_prod", "mysql_password")}}
# MAGIC   
# MAGIC Ingestion Pattern: Incremental Upsert
# MAGIC   watermark_column: updated_at
# MAGIC   primary_key: order_id
# MAGIC   reason: Orders can be updated (status changes). Need current state.
# MAGIC
# MAGIC Parallelization:
# MAGIC   partition_column: order_id
# MAGIC   num_partitions: 12
# MAGIC   fetch_size: 25000
# MAGIC   reason: 5M orders, parallelize for performance
# MAGIC
# MAGIC Custom Query:
# MAGIC   SELECT 
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     order_date,
# MAGIC     order_total,
# MAGIC     status,
# MAGIC     updated_at
# MAGIC   FROM orders_raw
# MAGIC   WHERE status != 'cancelled'  -- Filter at source
# MAGIC
# MAGIC Schedule: Every 4 hours
# MAGIC ```
# MAGIC
# MAGIC #### 2. Salesforce Ingestion Configuration
# MAGIC
# MAGIC ```yaml
# MAGIC Connector: Lakeflow Connect (managed Salesforce)
# MAGIC Source: Salesforce (Opportunity, Account)
# MAGIC Target: 
# MAGIC   - raw_layer.salesforce.opportunities
# MAGIC   - raw_layer.salesforce.accounts
# MAGIC
# MAGIC Configuration:
# MAGIC   authentication: OAuth 2.0
# MAGIC   objects:
# MAGIC     - Opportunity (Id, AccountId, Amount, StageName, CloseDate, LastModifiedDate)
# MAGIC     - Account (Id, Name, Industry, BillingCountry, LastModifiedDate)
# MAGIC   
# MAGIC Ingestion Pattern: Managed connector auto-upsert
# MAGIC   incremental_sync: Uses LastModifiedDate automatically
# MAGIC   
# MAGIC Schema Evolution: Enabled (auto-add custom fields)
# MAGIC
# MAGIC Schedule: Every 4 hours (aligned with MySQL)
# MAGIC ```
# MAGIC
# MAGIC #### 3. S3 File Ingestion Configuration
# MAGIC
# MAGIC ```yaml
# MAGIC Method: COPY INTO (not Lakeflow Connect - files in cloud storage)
# MAGIC Source: s3://company-data/customer-uploads/*.csv
# MAGIC Target: raw_layer.s3.customer_data
# MAGIC
# MAGIC Configuration:
# MAGIC   format: CSV
# MAGIC   header: true
# MAGIC   schema_inference: true
# MAGIC   
# MAGIC Ingestion Pattern: Full load
# MAGIC   reason: Files uploaded monthly (infrequent), not incremental
# MAGIC
# MAGIC Schedule: Daily (checks for new files, processes if found)
# MAGIC
# MAGIC Notebook code:
# MAGIC spark.sql("""
# MAGIC   COPY INTO raw_layer.s3.customer_data
# MAGIC   FROM 's3://company-data/customer-uploads/'
# MAGIC   FILEFORMAT = CSV
# MAGIC   FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
# MAGIC   COPY_OPTIONS ('mergeSchema' = 'true')
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC #### 4. Integration Notebook
# MAGIC
# MAGIC ```python
# MAGIC # Notebook: /pipelines/integrate_sources
# MAGIC
# MAGIC # Load raw data from all sources
# MAGIC orders = spark.table("raw_layer.mysql.orders")
# MAGIC opportunities = spark.table("raw_layer.salesforce.opportunities")
# MAGIC accounts = spark.table("raw_layer.salesforce.accounts")
# MAGIC customers = spark.table("raw_layer.s3.customer_data")
# MAGIC
# MAGIC # Standardize column names (different naming conventions)
# MAGIC from pyspark.sql.functions import col, coalesce
# MAGIC
# MAGIC orders_std = orders.select(
# MAGIC     col("order_id"),
# MAGIC     col("customer_id"),
# MAGIC     col("order_date"),
# MAGIC     col("order_total").alias("revenue"),
# MAGIC     col("status").alias("order_status")
# MAGIC )
# MAGIC
# MAGIC opportunities_std = opportunities.select(
# MAGIC     col("Id").alias("opportunity_id"),
# MAGIC     col("AccountId").alias("customer_id"),
# MAGIC     col("CloseDate").alias("order_date"),
# MAGIC     col("Amount").alias("revenue"),
# MAGIC     col("StageName").alias("order_status")
# MAGIC )
# MAGIC
# MAGIC # Union MySQL and Salesforce orders
# MAGIC all_orders = orders_std.unionByName(opportunities_std, allowMissingColumns=True)
# MAGIC
# MAGIC # Join with S3 customer data
# MAGIC integrated = all_orders.join(
# MAGIC     customers.select("customer_id", "customer_name", "industry", "region"),
# MAGIC     on="customer_id",
# MAGIC     how="left"
# MAGIC )
# MAGIC
# MAGIC # Write to integrated layer
# MAGIC integrated.write.format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .option("overwriteSchema", "true") \
# MAGIC     .saveAsTable("integrated_layer.sales.unified_orders")
# MAGIC ```
# MAGIC
# MAGIC #### 5. Orchestration with Lakeflow Jobs
# MAGIC
# MAGIC ```yaml
# MAGIC Job: Sales Integration Pipeline
# MAGIC
# MAGIC Tasks:
# MAGIC   Task 1: Ingest MySQL
# MAGIC     type: Lakeflow Connect
# MAGIC     connection: mysql_orders_connection
# MAGIC     depends_on: []
# MAGIC   
# MAGIC   Task 2: Ingest Salesforce
# MAGIC     type: Lakeflow Connect  
# MAGIC     connection: salesforce_connection
# MAGIC     depends_on: []
# MAGIC   
# MAGIC   Task 3: Ingest S3
# MAGIC     type: Notebook
# MAGIC     notebook: /pipelines/ingest_s3_files
# MAGIC     depends_on: []
# MAGIC   
# MAGIC   Task 4: Integrate Data
# MAGIC     type: Notebook
# MAGIC     notebook: /pipelines/integrate_sources
# MAGIC     depends_on: [Task 1, Task 2, Task 3]  # Wait for all ingestions
# MAGIC
# MAGIC Schedule: Cron: 0 */4 * * * (every 4 hours)
# MAGIC
# MAGIC Retry: max_retries = 2, delay = 5 minutes
# MAGIC
# MAGIC Alerts:
# MAGIC   on_failure: email to data-eng-team@company.com
# MAGIC   on_success: (none - only alert on failures)
# MAGIC ```
# MAGIC
# MAGIC ### Data Quality and Consistency
# MAGIC
# MAGIC ```python
# MAGIC # Add to integration notebook
# MAGIC
# MAGIC # 1. Check for missing customer_id joins
# MAGIC missing_customers = integrated.filter(col("customer_name").isNull())
# MAGIC if missing_customers.count() > 0:
# MAGIC     print(f"WARNING: {missing_customers.count()} orders missing customer data")
# MAGIC     missing_customers.write.mode("overwrite").saveAsTable("monitoring.missing_customer_joins")
# MAGIC
# MAGIC # 2. Check for duplicate order_id
# MAGIC from pyspark.sql.functions import count
# MAGIC duplicates = integrated.groupBy("order_id").agg(count("*").alias("cnt")) \
# MAGIC     .filter(col("cnt") > 1)
# MAGIC if duplicates.count() > 0:
# MAGIC     raise Exception(f"Data quality failure: {duplicates.count()} duplicate order_ids")
# MAGIC
# MAGIC # 3. Revenue validation
# MAGIC total_revenue = integrated.agg({"revenue": "sum"}).collect()[0][0]
# MAGIC if total_revenue < 0:
# MAGIC     raise Exception("Data quality failure: Negative total revenue")
# MAGIC ```
# MAGIC
# MAGIC ### Exam Relevance
# MAGIC
# MAGIC This solution demonstrates:
# MAGIC 1. **Multi-connector usage**: Standard JDBC, managed SaaS, file ingestion
# MAGIC 2. **Ingestion pattern selection**: Upsert for MySQL, auto-sync for Salesforce, batch for S3
# MAGIC 3. **Performance optimization**: Parallelization, fetch size for JDBC
# MAGIC 4. **Orchestration**: Task dependencies, failure handling
# MAGIC 5. **Data quality**: Validation, monitoring, error handling
# MAGIC 6. **Unity Catalog**: Three-level namespace (catalog.schema.table)

# COMMAND ----------

# DBTITLE 1,Challenge 2 Solution
# MAGIC %md
# MAGIC ## Challenge 2 Solution: Migration and Optimization
# MAGIC
# MAGIC ### Migration Analysis
# MAGIC
# MAGIC #### Current State Problems
# MAGIC
# MAGIC ```python
# MAGIC # Existing custom code (simplified)
# MAGIC import pymysql
# MAGIC import boto3
# MAGIC
# MAGIC # 1. Manual connection management
# MAGIC connection = pymysql.connect(
# MAGIC     host='mysql-prod',
# MAGIC     user='dbuser',
# MAGIC     password='hardcoded_password',  # SECURITY ISSUE
# MAGIC     database='sales'
# MAGIC )
# MAGIC
# MAGIC # 2. Manual incremental loading logic
# MAGIC last_sync_time = get_last_sync_from_metadata_table()
# MAGIC cursor = connection.cursor()
# MAGIC cursor.execute(f"""
# MAGIC     SELECT * FROM orders 
# MAGIC     WHERE updated_at > '{last_sync_time}'
# MAGIC """)  # SQL INJECTION RISK
# MAGIC
# MAGIC rows = cursor.fetchall()  # MEMORY ISSUE for large result sets
# MAGIC
# MAGIC # 3. Manual write to S3
# MAGIC for row in rows:
# MAGIC     json_data = convert_to_json(row)
# MAGIC     s3.put_object(
# MAGIC         Bucket='landing-zone',
# MAGIC         Key=f'orders/{row[0]}.json',
# MAGIC         Body=json_data
# MAGIC     )  # SLOW: one API call per row
# MAGIC
# MAGIC # 4. Separate Spark job to read S3 and write Delta
# MAGIC spark.read.json('s3://landing-zone/orders/') \
# MAGIC     .write.format('delta').mode('append') \
# MAGIC     .save('/mnt/delta/orders')  # NO UPSERT, creates duplicates
# MAGIC ```
# MAGIC
# MAGIC **Issues**:
# MAGIC 1. **Security**: Hardcoded credentials, SQL injection vulnerability
# MAGIC 2. **Reliability**: No retry logic, connection failures unhandled
# MAGIC 3. **Performance**: Single-threaded, fetchall() loads all to memory, slow S3 writes
# MAGIC 4. **Maintenance**: Custom code requires updates for schema changes
# MAGIC 5. **Data quality**: No upsert logic (duplicates on re-runs), no schema validation
# MAGIC 6. **Monitoring**: No built-in observability, manual logging
# MAGIC
# MAGIC ### Lakeflow Connect Migration
# MAGIC
# MAGIC #### Step 1: Configure Lakeflow Connect
# MAGIC
# MAGIC ```yaml
# MAGIC Connector Type: Standard JDBC (MySQL)
# MAGIC
# MAGIC Connection:
# MAGIC   connection_string: jdbc:mysql://mysql-prod.company.com:3306/sales?useSSL=true
# MAGIC   username: {{secrets.get("jdbc_prod", "mysql_user")}}
# MAGIC   password: {{secrets.get("jdbc_prod", "mysql_password")}}
# MAGIC   # Credentials moved to Databricks Secrets
# MAGIC
# MAGIC Source:
# MAGIC   table: orders
# MAGIC   
# MAGIC Incremental Loading:
# MAGIC   pattern: Incremental Upsert
# MAGIC   watermark_column: updated_at
# MAGIC   primary_key: order_id
# MAGIC   # Native incremental logic replaces custom code
# MAGIC
# MAGIC Performance:
# MAGIC   partition_column: order_id
# MAGIC   num_partitions: 16
# MAGIC   fetch_size: 50000
# MAGIC   # Parallelization replaces single-threaded ingestion
# MAGIC
# MAGIC Target:
# MAGIC   catalog: production
# MAGIC   schema: sales
# MAGIC   table: orders
# MAGIC   # Direct-to-Delta replaces S3 staging
# MAGIC
# MAGIC Schedule: Every 30 minutes
# MAGIC ```
# MAGIC
# MAGIC #### Step 2: Migration Process
# MAGIC
# MAGIC ```python
# MAGIC # Notebook: /migrations/validate_lakeflow_connect
# MAGIC
# MAGIC from pyspark.sql.functions import col, count, sum as spark_sum
# MAGIC
# MAGIC # 1. Run both systems in parallel (validation period)
# MAGIC legacy_table = spark.table("legacy.sales.orders")
# MAGIC lakeflow_table = spark.table("production.sales.orders")
# MAGIC
# MAGIC # 2. Compare row counts
# MAGIC legacy_count = legacy_table.count()
# MAGIC lakeflow_count = lakeflow_table.count()
# MAGIC print(f"Legacy: {legacy_count}, Lakeflow: {lakeflow_count}")
# MAGIC
# MAGIC assert abs(legacy_count - lakeflow_count) < 100, "Row count mismatch exceeds tolerance"
# MAGIC
# MAGIC # 3. Compare revenue totals
# MAGIC legacy_revenue = legacy_table.agg(spark_sum("order_total")).collect()[0][0]
# MAGIC lakeflow_revenue = lakeflow_table.agg(spark_sum("order_total")).collect()[0][0]
# MAGIC revenue_diff_pct = abs(legacy_revenue - lakeflow_revenue) / legacy_revenue * 100
# MAGIC
# MAGIC assert revenue_diff_pct < 1.0, f"Revenue mismatch: {revenue_diff_pct}%"
# MAGIC
# MAGIC # 4. Check for duplicates (should be 0 with upsert)
# MAGIC duplicates = lakeflow_table.groupBy("order_id").agg(count("*").alias("cnt")) \
# MAGIC     .filter(col("cnt") > 1)
# MAGIC     
# MAGIC assert duplicates.count() == 0, "Duplicates found in Lakeflow table"
# MAGIC
# MAGIC # 5. Compare latest records (data freshness)
# MAGIC legacy_max_date = legacy_table.agg({"updated_at": "max"}).collect()[0][0]
# MAGIC lakeflow_max_date = lakeflow_table.agg({"updated_at": "max"}).collect()[0][0]
# MAGIC
# MAGIC freshness_lag_minutes = (legacy_max_date - lakeflow_max_date).total_seconds() / 60
# MAGIC assert freshness_lag_minutes < 60, f"Lakeflow data is {freshness_lag_minutes} min behind"
# MAGIC
# MAGIC print("✓ All validation checks passed")
# MAGIC ```
# MAGIC
# MAGIC #### Step 3: Decommission Legacy
# MAGIC
# MAGIC ```python
# MAGIC # After 2 weeks of parallel operation:
# MAGIC # 1. Update downstream dependencies to point to production.sales.orders
# MAGIC # 2. Disable legacy Python script
# MAGIC # 3. Clean up S3 landing zone
# MAGIC # 4. Drop legacy.sales.orders table (after final backup)
# MAGIC
# MAGIC from pyspark.sql.utils import AnalysisException
# MAGIC
# MAGIC # Check for dependencies
# MAGIC dependencies = spark.sql("""
# MAGIC     SELECT table_name, last_accessed 
# MAGIC     FROM system.information_schema.tables
# MAGIC     WHERE table_catalog = 'legacy' 
# MAGIC       AND table_schema = 'sales'
# MAGIC       AND table_name = 'orders'
# MAGIC """)
# MAGIC
# MAGIC if dependencies.count() == 0:
# MAGIC     print("No active dependencies on legacy table")
# MAGIC     
# MAGIC     # Final backup
# MAGIC     spark.table("legacy.sales.orders").write \
# MAGIC         .format("delta") \
# MAGIC         .mode("overwrite") \
# MAGIC         .saveAsTable("archive.sales.orders_legacy_backup")
# MAGIC     
# MAGIC     # Drop legacy table
# MAGIC     spark.sql("DROP TABLE IF EXISTS legacy.sales.orders")
# MAGIC     print("✓ Legacy table decommissioned")
# MAGIC else:
# MAGIC     print("WARNING: Active dependencies still exist:")
# MAGIC     dependencies.show()
# MAGIC ```
# MAGIC
# MAGIC ### Performance Comparison
# MAGIC
# MAGIC | Metric | Legacy Custom Code | Lakeflow Connect | Improvement |
# MAGIC |--------|-------------------|------------------|-------------|
# MAGIC | Ingestion time (1M rows) | 45 minutes | 3 minutes | 15x faster |
# MAGIC | Development time | 2 weeks | 2 hours | 40x faster |
# MAGIC | Maintenance (hours/month) | 8 hours | 0.5 hours | 16x less |
# MAGIC | Error rate | 5% (connection issues) | 0.1% (auto-retry) | 50x more reliable |
# MAGIC | Duplicate rows | Common (re-runs) | 0 (upsert) | 100% reduction |
# MAGIC | Security issues | 2 (hardcoded creds) | 0 (Secrets) | Resolved |
# MAGIC | Schema change handling | Manual code updates | Automatic | N/A |
# MAGIC
# MAGIC ### Optimization Beyond Migration
# MAGIC
# MAGIC #### 1. Predicate Pushdown
# MAGIC
# MAGIC ```yaml
# MAGIC # If only recent orders needed downstream
# MAGIC Custom Query:
# MAGIC   SELECT * FROM orders 
# MAGIC   WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
# MAGIC   # Reduces data transfer by 70% (only last 90 days)
# MAGIC ```
# MAGIC
# MAGIC #### 2. Partition Optimization
# MAGIC
# MAGIC ```python
# MAGIC # Re-partition target table for query performance
# MAGIC spark.sql("""
# MAGIC     CREATE TABLE production.sales.orders_optimized
# MAGIC     USING DELTA
# MAGIC     PARTITIONED BY (order_date)
# MAGIC     AS SELECT * FROM production.sales.orders
# MAGIC """)
# MAGIC
# MAGIC # Enable liquid clustering (if available)
# MAGIC spark.sql("""
# MAGIC     ALTER TABLE production.sales.orders
# MAGIC     CLUSTER BY (customer_id, order_date)
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC #### 3. Monitoring and Alerting
# MAGIC
# MAGIC ```python
# MAGIC # Notebook: /monitoring/orders_ingestion_health
# MAGIC
# MAGIC from datetime import datetime, timedelta
# MAGIC
# MAGIC # Check data freshness
# MAGIC max_updated = spark.table("production.sales.orders") \
# MAGIC     .agg({"updated_at": "max"}).collect()[0][0]
# MAGIC
# MAGIC freshness_lag = datetime.now() - max_updated
# MAGIC if freshness_lag > timedelta(hours=2):
# MAGIC     send_alert(f"Orders data is {freshness_lag.total_seconds()/3600} hours stale")
# MAGIC
# MAGIC # Check row count growth
# MAGIC today_count = spark.table("production.sales.orders") \
# MAGIC     .filter(col("order_date") == datetime.now().date()).count()
# MAGIC
# MAGIC avg_daily_count = 50000  # Historical average
# MAGIC if today_count < avg_daily_count * 0.5:
# MAGIC     send_alert(f"Orders volume unusually low: {today_count} vs {avg_daily_count} avg")
# MAGIC ```
# MAGIC
# MAGIC ### Exam Relevance
# MAGIC
# MAGIC This migration demonstrates:
# MAGIC 1. **Connector selection**: Custom code to standard JDBC connector
# MAGIC 2. **Performance optimization**: Parallelization, fetch size, predicate pushdown
# MAGIC 3. **Security**: Databricks Secrets replacing hardcoded credentials
# MAGIC 4. **Reliability**: Built-in retry, upsert logic, schema evolution
# MAGIC 5. **Validation**: Parallel run, data quality checks
# MAGIC 6. **Decommissioning**: Safe migration process
# MAGIC
# MAGIC **Key exam lesson**: Lakeflow Connect is a managed service that replaces custom ingestion code. Benefits include security, reliability, performance, and reduced maintenance. Always prefer managed solutions over custom code when available.

# COMMAND ----------

# DBTITLE 1,Applied 1 Solution
# MAGIC %md
# MAGIC ## Applied 1 Solution: Decision Tree for Ingestion Method Selection
# MAGIC
# MAGIC ### Complete Decision Process
# MAGIC
# MAGIC ```
# MAGIC START
# MAGIC   |
# MAGIC   v
# MAGIC [What is the data source?]
# MAGIC   |
# MAGIC   +-- Files already in cloud storage (S3/ADLS/GCS)
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   [What is the arrival pattern?]
# MAGIC   |     |
# MAGIC   |     +-- Continuous arrival (streaming) OR high frequency (< hourly)
# MAGIC   |     |     |
# MAGIC   |     |     v
# MAGIC   |     |   [Do you need schema evolution or complex transformations?]
# MAGIC   |     |     |
# MAGIC   |     |     +-- Yes --> AUTO LOADER (streaming mode)
# MAGIC   |     |     +-- No, simple ingestion --> AUTO LOADER (still recommended for streaming)
# MAGIC   |     |
# MAGIC   |     +-- Infrequent batch (daily, weekly, monthly)
# MAGIC   |           |
# MAGIC   |           v
# MAGIC   |         COPY INTO
# MAGIC   |
# MAGIC   +-- Database (MySQL, PostgreSQL, Oracle, SQL Server, etc.)
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   [What is the latency requirement?]
# MAGIC   |     |
# MAGIC   |     +-- Real-time (< 1 minute) --> Partner connector (Fivetran) OR custom CDC
# MAGIC   |     +-- Near real-time to hourly --> LAKEFLOW CONNECT (standard JDBC)
# MAGIC   |     +-- Daily or less frequent --> LAKEFLOW CONNECT (standard JDBC)
# MAGIC   |
# MAGIC   +-- SaaS Application (Salesforce, Workday, ServiceNow, etc.)
# MAGIC         |
# MAGIC         v
# MAGIC       [Is there a managed connector available?]
# MAGIC         |
# MAGIC         +-- Yes (Salesforce, Workday, ServiceNow, HubSpot, etc.)
# MAGIC         |     |
# MAGIC         |     v
# MAGIC         |   [What is the latency requirement?]
# MAGIC         |     |
# MAGIC         |     +-- Hourly or less frequent --> LAKEFLOW CONNECT (managed connector)
# MAGIC         |     +-- Real-time (< 1 hour) --> Partner connector with streaming
# MAGIC         |
# MAGIC         +-- No managed connector
# MAGIC               |
# MAGIC               v
# MAGIC             [Does the app have a REST API?]
# MAGIC               |
# MAGIC               +-- Yes --> Custom code OR partner connector
# MAGIC               +-- No --> Contact vendor for integration options
# MAGIC ```
# MAGIC
# MAGIC ### Scenario Walkthroughs
# MAGIC
# MAGIC #### Scenario A: MySQL database, need data within 30 minutes
# MAGIC
# MAGIC ```
# MAGIC Source: Database
# MAGIC   |
# MAGIC   v
# MAGIC Latency: 30 minutes (near real-time)
# MAGIC   |
# MAGIC   v
# MAGIC Decision: LAKEFLOW CONNECT (standard JDBC)
# MAGIC   - Configure incremental loading every 30 minutes
# MAGIC   - Use watermark column for efficiency
# MAGIC   - Standard JDBC connector for MySQL
# MAGIC ```
# MAGIC
# MAGIC **Configuration**:
# MAGIC ```yaml
# MAGIC connector: Standard JDBC
# MAGIC source: MySQL
# MAGIC incremental: Upsert (if updates) or Append (if append-only)
# MAGIC schedule: Every 30 minutes
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario B: CSV files arrive in S3 every 10 minutes
# MAGIC
# MAGIC ```
# MAGIC Source: Files in cloud storage
# MAGIC   |
# MAGIC   v
# MAGIC Arrival pattern: High frequency (10 minutes)
# MAGIC   |
# MAGIC   v
# MAGIC Decision: AUTO LOADER (streaming mode)
# MAGIC   - Checkpoint-based processing
# MAGIC   - Exactly-once guarantee
# MAGIC   - Schema evolution support
# MAGIC ```
# MAGIC
# MAGIC **Implementation**:
# MAGIC ```python
# MAGIC df = spark.readStream.format("cloudFiles") \
# MAGIC     .option("cloudFiles.format", "csv") \
# MAGIC     .option("cloudFiles.schemaLocation", "/schema/location") \
# MAGIC     .load("s3://bucket/path/")
# MAGIC
# MAGIC df.writeStream.format("delta") \
# MAGIC     .option("checkpointLocation", "/checkpoint/location") \
# MAGIC     .table("target_table")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario C: Salesforce data, need hourly sync
# MAGIC
# MAGIC ```
# MAGIC Source: SaaS Application
# MAGIC   |
# MAGIC   v
# MAGIC Managed connector available: Yes (Salesforce)
# MAGIC   |
# MAGIC   v
# MAGIC Latency: Hourly (within managed connector capability)
# MAGIC   |
# MAGIC   v
# MAGIC Decision: LAKEFLOW CONNECT (managed Salesforce connector)
# MAGIC   - OAuth authentication
# MAGIC   - Auto schema discovery
# MAGIC   - Incremental sync via LastModifiedDate
# MAGIC ```
# MAGIC
# MAGIC **Configuration**:
# MAGIC ```yaml
# MAGIC connector: Managed Salesforce
# MAGIC objects: [Opportunity, Account, Contact]
# MAGIC authentication: OAuth 2.0
# MAGIC schedule: Hourly
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario D: Parquet files uploaded monthly to ADLS
# MAGIC
# MAGIC ```
# MAGIC Source: Files in cloud storage
# MAGIC   |
# MAGIC   v
# MAGIC Arrival pattern: Infrequent batch (monthly)
# MAGIC   |
# MAGIC   v
# MAGIC Decision: COPY INTO
# MAGIC   - Simple batch ingestion
# MAGIC   - No streaming overhead
# MAGIC   - Idempotent (safe to re-run)
# MAGIC ```
# MAGIC
# MAGIC **Implementation**:
# MAGIC ```sql
# MAGIC COPY INTO target_table
# MAGIC FROM 'abfss://container@account.dfs.core.windows.net/path/'
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true')
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario E: Custom internal API, need data every 5 minutes
# MAGIC
# MAGIC ```
# MAGIC Source: SaaS Application (custom)
# MAGIC   |
# MAGIC   v
# MAGIC Managed connector: No (custom app)
# MAGIC   |
# MAGIC   v
# MAGIC API available: Yes (REST API)
# MAGIC   |
# MAGIC   v
# MAGIC Decision: Custom code OR partner connector (Airbyte)
# MAGIC   - Build custom notebook with API calls
# MAGIC   - OR use Airbyte custom connector
# MAGIC   - Schedule with Lakeflow Jobs every 5 minutes
# MAGIC ```
# MAGIC
# MAGIC **Custom code approach**:
# MAGIC ```python
# MAGIC import requests
# MAGIC import json
# MAGIC
# MAGIC api_key = dbutils.secrets.get("custom_api", "api_key")
# MAGIC response = requests.get(
# MAGIC     "https://internal-api.company.com/data",
# MAGIC     headers={"Authorization": f"Bearer {api_key}"}
# MAGIC )
# MAGIC
# MAGIC data = response.json()
# MAGIC spark.createDataFrame(data).write \
# MAGIC     .format("delta").mode("append") \
# MAGIC     .saveAsTable("raw.custom_api.data")
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario F: PostgreSQL, need sub-minute latency
# MAGIC
# MAGIC ```
# MAGIC Source: Database
# MAGIC   |
# MAGIC   v
# MAGIC Latency: < 1 minute (real-time)
# MAGIC   |
# MAGIC   v
# MAGIC Decision: Partner connector (Fivetran) OR custom CDC
# MAGIC   - Lakeflow Connect minimum is hourly (too slow)
# MAGIC   - Need log-based CDC for real-time
# MAGIC   - Fivetran supports CDC replication
# MAGIC ```
# MAGIC
# MAGIC **Why not Lakeflow Connect**: Minimum schedule frequency is hourly. For sub-hour latency, need streaming CDC solution.
# MAGIC
# MAGIC ### Exam Tips for Decision Making
# MAGIC
# MAGIC **Red flags to watch for**:
# MAGIC
# MAGIC 1. **"Files in S3" + "Lakeflow Connect"** → WRONG. Lakeflow Connect doesn't read files.
# MAGIC 2. **"MySQL" + "Auto Loader"** → WRONG. Auto Loader doesn't connect to databases.
# MAGIC 3. **"Real-time" + "Lakeflow Connect"** → WRONG. Lakeflow Connect is batch (minimum hourly).
# MAGIC 4. **"Custom SaaS" + "Managed connector"** → WRONG. Managed connectors only for well-known apps.
# MAGIC 5. **"Salesforce" + "Standard JDBC"** → WRONG. Salesforce needs managed connector (REST API, not JDBC).
# MAGIC
# MAGIC **Memory aids**:
# MAGIC * Files → Auto Loader (frequent) OR COPY INTO (infrequent)
# MAGIC * Databases → Lakeflow Connect standard JDBC
# MAGIC * Well-known SaaS → Lakeflow Connect managed
# MAGIC * Real-time → Partner connector OR custom streaming
# MAGIC * Unknown/custom → Start with Lakeflow Connect, fall back to custom code
# MAGIC
# MAGIC **Quick reference table**:
# MAGIC
# MAGIC | Source | Frequency | Method |
# MAGIC |--------|-----------|--------|
# MAGIC | Files | Continuous | Auto Loader |
# MAGIC | Files | Batch (daily+) | COPY INTO |
# MAGIC | Database | Hourly+ | Lakeflow Connect JDBC |
# MAGIC | Database | Real-time | Partner/CDC |
# MAGIC | Salesforce | Hourly+ | Lakeflow Connect Managed |
# MAGIC | Salesforce | Real-time | Partner |
# MAGIC | Custom app | Any | Custom code |
# MAGIC
# MAGIC When in doubt on the exam: if it's a recognizable data source (database type or SaaS brand) and NOT real-time, choose Lakeflow Connect.

# COMMAND ----------

# DBTITLE 1,Applied 2 Solution
# MAGIC %md
# MAGIC ## Applied 2 Solution: Troubleshooting Decision Matrix
# MAGIC
# MAGIC ### Systematic Diagnostic Workflow
# MAGIC
# MAGIC ```
# MAGIC Problem: Lakeflow Connect ingestion failing or slow
# MAGIC   |
# MAGIC   v
# MAGIC [Check: Error message category]
# MAGIC   |
# MAGIC   +-- Authentication/Authorization errors
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   ["Access denied", "Authentication failed", "Invalid credentials"]
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   Diagnostic steps:
# MAGIC   |     1. Verify credentials in Databricks Secrets
# MAGIC   |     2. Test credentials directly against source
# MAGIC   |     3. Check source database user permissions
# MAGIC   |     4. For managed connectors: Re-authenticate (OAuth token expired)
# MAGIC   |     5. Check IP allowlist (source firewall blocking Databricks)
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   Common fixes:
# MAGIC   |     - Update password in Secrets: dbutils.secrets.put(scope, key, password)
# MAGIC   |     - Grant permissions: GRANT SELECT ON database.* TO 'user'@'%';
# MAGIC   |     - Add Databricks IP ranges to source firewall
# MAGIC   |     - For Salesforce: Re-authorize OAuth in connector settings
# MAGIC   |
# MAGIC   +-- Connection/Network errors
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   ["Connection refused", "Timeout", "Host not found"]
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   Diagnostic steps:
# MAGIC   |     1. Test connectivity: %sh nc -zv <host> <port>
# MAGIC   |     2. Check VPC peering/private link configuration
# MAGIC   |     3. Verify security group/firewall rules
# MAGIC   |     4. Confirm source database bind-address (listening on correct interface)
# MAGIC   |     5. Check if source database is running
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   Common fixes:
# MAGIC   |     - Update firewall rules to allow Databricks CIDR blocks
# MAGIC   |     - Set up VPC peering between Databricks and source VPC
# MAGIC   |     - Configure bind-address = 0.0.0.0 (MySQL) to accept remote connections
# MAGIC   |     - Restart source database if down
# MAGIC   |
# MAGIC   +-- Performance/Timeout issues
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   ["Slow ingestion", "Taking hours", "Still running"]
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   [Check: Current configuration]
# MAGIC   |     |
# MAGIC   |     +-- Is parallelization enabled?
# MAGIC   |     |     No --> Enable parallelization (partition_column, num_partitions)
# MAGIC   |     |     Yes --> Check partition count vs connection limit
# MAGIC   |     |
# MAGIC   |     +-- What is fetch_size?
# MAGIC   |     |     Default/Low (< 1000) --> Increase to 10,000-50,000
# MAGIC   |     |     Already high --> Look elsewhere
# MAGIC   |     |
# MAGIC   |     +-- Is incremental loading configured?
# MAGIC   |     |     No --> Enable (watermark column)
# MAGIC   |     |     Yes --> Verify watermark is working (not re-reading all rows)
# MAGIC   |     |
# MAGIC   |     +-- Are you reading unnecessary data?
# MAGIC   |           Yes --> Add custom query with WHERE clause (predicate pushdown)
# MAGIC   |
# MAGIC   +-- Data quality/Schema errors
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   ["Schema mismatch", "Column not found", "Type mismatch"]
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   Diagnostic steps:
# MAGIC   |     1. Compare source schema to target Delta table schema
# MAGIC   |     2. Check for recent schema changes in source
# MAGIC   |     3. Verify data types compatibility
# MAGIC   |     4. Check for NULL values in non-nullable columns
# MAGIC   |     |
# MAGIC   |     v
# MAGIC   |   [What changed?]
# MAGIC   |     |
# MAGIC   |     +-- New column added in source
# MAGIC   |     |     --> Should auto-add (check if schema evolution enabled)
# MAGIC   |     |     --> If disabled: ALTER TABLE ADD COLUMN manually
# MAGIC   |     |
# MAGIC   |     +-- Column removed in source
# MAGIC   |     |     --> Column remains in target (expected behavior)
# MAGIC   |     |     --> New rows will have NULL
# MAGIC   |     |
# MAGIC   |     +-- Column type changed
# MAGIC   |     |     --> Manual intervention required
# MAGIC   |     |     --> Add new column with new type, migrate data, deprecate old
# MAGIC   |     |
# MAGIC   |     +-- Column renamed
# MAGIC   |           --> Appears as new column + old column remains
# MAGIC   |           --> Manual migration: copy old to new, drop old
# MAGIC   |
# MAGIC   +-- Incremental loading not working
# MAGIC         |
# MAGIC         v
# MAGIC       ["Reading all rows every time", "Watermark not advancing"]
# MAGIC         |
# MAGIC         v
# MAGIC       Diagnostic steps:
# MAGIC         1. Verify watermark column in source has no NULLs
# MAGIC         2. Check if watermark column is monotonically increasing
# MAGIC         3. Verify watermark column in SELECT list (if using custom query)
# MAGIC         4. Check connector state: Is max watermark being tracked?
# MAGIC         5. Look for resets: Did someone reset the connector state?
# MAGIC         |
# MAGIC         v
# MAGIC       Common issues:
# MAGIC         - Watermark column has NULL values --> Filter out NULLs or use different column
# MAGIC         - Watermark not in custom query SELECT --> Add to SELECT list
# MAGIC         - Watermark not monotonic (e.g., updated_at can decrease) --> Use created_at
# MAGIC         - Connector state corrupted --> Reset and backfill
# MAGIC ```
# MAGIC
# MAGIC ### Scenario-Based Troubleshooting
# MAGIC
# MAGIC #### Scenario 1: "Access denied for user 'etl_user'@'10.0.1.45'"
# MAGIC
# MAGIC **Diagnosis**:
# MAGIC ```
# MAGIC Error category: Authentication
# MAGIC   |
# MAGIC   v
# MAGIC Step 1: Test credentials
# MAGIC   # From local MySQL client
# MAGIC   mysql -h mysql-prod.company.com -u etl_user -p
# MAGIC   # Enter password from Secrets
# MAGIC   # Result: Access denied
# MAGIC   |
# MAGIC   v
# MAGIC Step 2: Check MySQL user grants
# MAGIC   mysql> SELECT User, Host FROM mysql.user WHERE User = 'etl_user';
# MAGIC   Result: etl_user | localhost (NOT %, so no remote access)
# MAGIC   |
# MAGIC   v
# MAGIC Root cause: User 'etl_user' only allowed from localhost, not remote (Databricks)
# MAGIC ```
# MAGIC
# MAGIC **Fix**:
# MAGIC ```sql
# MAGIC -- Grant access from any host
# MAGIC GRANT SELECT ON sales_db.* TO 'etl_user'@'%' IDENTIFIED BY 'password';
# MAGIC FLUSH PRIVILEGES;
# MAGIC
# MAGIC -- OR grant access from specific Databricks CIDR
# MAGIC GRANT SELECT ON sales_db.* TO 'etl_user'@'10.0.0.0/255.255.0.0';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario 2: "Ingestion taking 4 hours for 10M rows"
# MAGIC
# MAGIC **Diagnosis**:
# MAGIC ```
# MAGIC Error category: Performance
# MAGIC   |
# MAGIC   v
# MAGIC Step 1: Check parallelization
# MAGIC   Current config: num_partitions = 1 (single-threaded)
# MAGIC   |
# MAGIC   v
# MAGIC Step 2: Check fetch size
# MAGIC   Current config: fetch_size = 100 (default, very low)
# MAGIC   |
# MAGIC   v
# MAGIC Step 3: Check for unnecessary data
# MAGIC   Reading: Entire table (10M rows)
# MAGIC   Need: Only last 90 days (2M rows)
# MAGIC   |
# MAGIC   v
# MAGIC Root causes:
# MAGIC   1. Single-threaded ingestion (no parallelization)
# MAGIC   2. Low fetch size (excessive JDBC round trips)
# MAGIC   3. Reading 5x more data than needed
# MAGIC ```
# MAGIC
# MAGIC **Fixes**:
# MAGIC ```yaml
# MAGIC # Fix 1: Enable parallelization
# MAGIC partition_column: transaction_id
# MAGIC num_partitions: 16
# MAGIC # Result: 16x speedup
# MAGIC
# MAGIC # Fix 2: Increase fetch size
# MAGIC fetch_size: 50000
# MAGIC # Result: 500x fewer JDBC round trips
# MAGIC
# MAGIC # Fix 3: Filter at source
# MAGIC custom_query: |
# MAGIC   SELECT * FROM transactions
# MAGIC   WHERE transaction_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)
# MAGIC # Result: 80% less data transferred
# MAGIC
# MAGIC Expected new time: 4 hours / 16 / 2 (fetch) / 5 (filter) = ~90 seconds
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario 3: "Column 'customer_tier' not found"
# MAGIC
# MAGIC **Diagnosis**:
# MAGIC ```
# MAGIC Error category: Schema
# MAGIC   |
# MAGIC   v
# MAGIC Step 1: Compare schemas
# MAGIC   Source: customer_id, name, email, customer_tier (NEW)
# MAGIC   Target Delta: customer_id, name, email (OLD)
# MAGIC   |
# MAGIC   v
# MAGIC Step 2: Check schema evolution setting
# MAGIC   mergeSchema: false (schema evolution DISABLED)
# MAGIC   |
# MAGIC   v
# MAGIC Root cause: New column added to source, but schema evolution disabled
# MAGIC ```
# MAGIC
# MAGIC **Fixes**:
# MAGIC ```yaml
# MAGIC # Option A: Enable schema evolution (preferred)
# MAGIC mergeSchema: true
# MAGIC # Connector will auto-add customer_tier column
# MAGIC
# MAGIC # Option B: Manual ALTER TABLE
# MAGIC ALTER TABLE target_table ADD COLUMN customer_tier STRING;
# MAGIC # Then re-run connector
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Scenario 4: "Incremental loading reading all 20M rows every sync"
# MAGIC
# MAGIC **Diagnosis**:
# MAGIC ```
# MAGIC Error category: Incremental loading failure
# MAGIC   |
# MAGIC   v
# MAGIC Step 1: Check watermark column
# MAGIC   watermark_column: updated_at
# MAGIC   |
# MAGIC   v
# MAGIC Step 2: Query source for NULL watermarks
# MAGIC   SELECT COUNT(*) FROM orders WHERE updated_at IS NULL;
# MAGIC   Result: 5M rows (25% have NULL)
# MAGIC   |
# MAGIC   v
# MAGIC Root cause: Watermark column has NULLs. Connector falls back to full load.
# MAGIC ```
# MAGIC
# MAGIC **Fixes**:
# MAGIC ```yaml
# MAGIC # Option A: Filter out NULLs
# MAGIC custom_query: |
# MAGIC   SELECT * FROM orders WHERE updated_at IS NOT NULL
# MAGIC # But this misses 5M rows permanently
# MAGIC
# MAGIC # Option B: Use different watermark
# MAGIC watermark_column: order_id
# MAGIC # If order_id is auto-increment and reliable
# MAGIC
# MAGIC # Option C: Fix source data (preferred)
# MAGIC UPDATE orders SET updated_at = created_at WHERE updated_at IS NULL;
# MAGIC # Then configure incremental loading normally
# MAGIC ```
# MAGIC
# MAGIC ### Exam-Relevant Troubleshooting Patterns
# MAGIC
# MAGIC **Pattern 1: Authentication fails after working for months**
# MAGIC * Cause: Password changed, OAuth token expired
# MAGIC * Fix: Update Secrets, re-authorize OAuth
# MAGIC
# MAGIC **Pattern 2: Suddenly can't connect after network change**
# MAGIC * Cause: Firewall rules changed, VPC peering broken
# MAGIC * Fix: Restore network connectivity, update security groups
# MAGIC
# MAGIC **Pattern 3: Performance degrades over time**
# MAGIC * Cause: Table grew, still using single-threaded ingestion
# MAGIC * Fix: Enable parallelization (should have from start)
# MAGIC
# MAGIC **Pattern 4: Incremental loading stops working**
# MAGIC * Cause: Watermark column now has NULLs (data quality issue)
# MAGIC * Fix: Fix source data quality, choose better watermark
# MAGIC
# MAGIC **Pattern 5: Schema errors after source changes**
# MAGIC * Cause: Source schema evolved, schema evolution disabled
# MAGIC * Fix: Enable schema evolution OR manual ALTER TABLE
# MAGIC
# MAGIC **Exam tip**: Most troubleshooting questions test whether you know the ROOT CAUSE, not just symptoms. Don't just say "enable parallelization" - explain WHY it's slow (single-threaded, low fetch size, etc.).

# COMMAND ----------

# DBTITLE 1,Study Guide and Exam Strategy
# MAGIC %md
# MAGIC ## Study Guide and Exam Strategy
# MAGIC
# MAGIC ### High-Frequency Exam Topics
# MAGIC
# MAGIC Based on exam weight (21% of Section 2) and official objectives, prioritize:
# MAGIC
# MAGIC **1. Connector Selection (Critical)**
# MAGIC * Standard JDBC vs Managed connectors
# MAGIC * When to use Lakeflow Connect vs Auto Loader vs COPY INTO
# MAGIC * Database vs SaaS vs Files decision matrix
# MAGIC * Memorize: Database = Standard, Well-known SaaS = Managed, Files = Auto Loader/COPY INTO
# MAGIC
# MAGIC **2. Incremental Loading Patterns (Critical)**
# MAGIC * Append vs Upsert decision criteria
# MAGIC * Watermark column selection (immutable vs change-tracking)
# MAGIC * Primary key requirements for upsert
# MAGIC * Common trap: Using wrong watermark (created_at vs updated_at)
# MAGIC
# MAGIC **3. JDBC Configuration (High Frequency)**
# MAGIC * Connection string formats (MySQL vs PostgreSQL vs SQL Server vs Oracle)
# MAGIC * Credential management (Databricks Secrets)
# MAGIC * Parallelization (partition column, num_partitions, fetch_size)
# MAGIC * Custom queries vs table ingestion
# MAGIC
# MAGIC **4. Schema Evolution (High Frequency)**
# MAGIC * Additive changes (auto-handled)
# MAGIC * Deletive changes (columns remain)
# MAGIC * Type changes (manual fix required)
# MAGIC * Managed connector automatic flattening
# MAGIC
# MAGIC **5. Performance Optimization (Medium Frequency)**
# MAGIC * Parallelization configuration
# MAGIC * Fetch size tuning
# MAGIC * Predicate pushdown (custom queries)
# MAGIC * Connection limit awareness
# MAGIC
# MAGIC ### Common Exam Traps
# MAGIC
# MAGIC **Trap 1: Mixing up ingestion methods**
# MAGIC * "Files in S3" + "Lakeflow Connect" = WRONG
# MAGIC * "MySQL" + "Auto Loader" = WRONG
# MAGIC * "Salesforce" + "Standard JDBC" = WRONG
# MAGIC
# MAGIC **Trap 2: SQL dialect confusion**
# MAGIC * Custom queries use SOURCE database syntax, not Spark SQL
# MAGIC * SQL Server uses semicolons, MySQL uses question marks
# MAGIC * Oracle has unique connection string format
# MAGIC
# MAGIC **Trap 3: Watermark column selection**
# MAGIC * Append-only data + updated_at = WRONG (use created_at)
# MAGIC * Upsert + created_at = WRONG (misses updates, use updated_at)
# MAGIC * Watermark with NULLs = WRONG (falls back to full load)
# MAGIC
# MAGIC **Trap 4: Schema evolution assumptions**
# MAGIC * Removed source columns auto-drop from target = WRONG (they remain)
# MAGIC * Type changes auto-handled = WRONG (manual fix needed)
# MAGIC * Managed connectors require manual JSON parsing = WRONG (auto-flatten)
# MAGIC
# MAGIC **Trap 5: Performance optimization**
# MAGIC * Partition on timestamp column = WRONG (must be numeric)
# MAGIC * Ignore connection limits = WRONG (exceeding causes failures)
# MAGIC * Use default fetch size for large tables = WRONG (too many round trips)
# MAGIC
# MAGIC ### Memorization Aids
# MAGIC
# MAGIC **Connector selection**:
# MAGIC * DB + port number = Standard JDBC
# MAGIC * SaaS brand name = Managed
# MAGIC * Files in cloud = Auto Loader (frequent) or COPY INTO (batch)
# MAGIC
# MAGIC **Connection strings**:
# MAGIC * MySQL/PostgreSQL: `?` and `&` for parameters
# MAGIC * SQL Server: `;` (semicolons) for parameters
# MAGIC * Oracle: `:` (colons) throughout
# MAGIC
# MAGIC **Incremental loading**:
# MAGIC * Immutable + append-only = created_at + Append
# MAGIC * Mutable + need current state = updated_at + Upsert
# MAGIC * No timestamp = Full load
# MAGIC
# MAGIC **Schema evolution**:
# MAGIC * Additive (new) = AUTO-YES
# MAGIC * Deletive (removed) = NO-DROP
# MAGIC * Mutative (type change) = MANUAL-FIX
# MAGIC
# MAGIC ### Pre-Exam Checklist
# MAGIC
# MAGIC Review these concepts 24 hours before exam:
# MAGIC
# MAGIC - [ ] Can you draw the decision tree for ingestion method selection?
# MAGIC - [ ] Can you write JDBC connection strings for MySQL, PostgreSQL, SQL Server, Oracle?
# MAGIC - [ ] Can you explain append vs upsert and when to use each?
# MAGIC - [ ] Can you identify correct watermark columns in different scenarios?
# MAGIC - [ ] Can you configure parallelization (partition column, num_partitions)?
# MAGIC - [ ] Do you know the three schema evolution outcomes (additive/deletive/mutative)?
# MAGIC - [ ] Can you list performance optimizations in order of impact?
# MAGIC - [ ] Can you troubleshoot authentication, connection, performance, schema issues?
# MAGIC - [ ] Do you know what managed connectors auto-handle vs require manual work?
# MAGIC - [ ] Can you explain predicate pushdown and when to use custom queries?
# MAGIC
# MAGIC ### Practice Strategy
# MAGIC
# MAGIC **Week 1: Fundamentals**
# MAGIC * Complete all 15 exercises
# MAGIC * Focus on connector selection and incremental loading
# MAGIC * Memorize connection string formats
# MAGIC
# MAGIC **Week 2: Application**
# MAGIC * Complete all 5 MCQs
# MAGIC * Work through both challenges
# MAGIC * Time yourself (exam conditions)
# MAGIC
# MAGIC **Week 3: Mastery**
# MAGIC * Practice both applieds (decision trees)
# MAGIC * Rework any exercises you missed
# MAGIC * Quiz yourself on exam traps
# MAGIC
# MAGIC **Day before exam**:
# MAGIC * Review this summary page
# MAGIC * Skim exam tips from each solution
# MAGIC * Practice drawing decision trees from memory
# MAGIC * Get good sleep (don't cram)
# MAGIC
# MAGIC ### Final Exam Tips
# MAGIC
# MAGIC 1. **Read carefully**: "Files in S3" vs "MySQL database" changes the entire answer
# MAGIC 2. **Eliminate wrong answers**: If you see "Standard JDBC for Salesforce", eliminate immediately
# MAGIC 3. **Watch for qualifiers**: "Real-time" vs "hourly" vs "daily" determines method
# MAGIC 4. **Check all parts**: Multi-part questions test if you know BOTH connector type AND configuration
# MAGIC 5. **Don't overthink**: If it sounds like a well-known SaaS app, it's a managed connector
# MAGIC
# MAGIC **Time management**:
# MAGIC * Lakeflow Connect is 21% of Section 2 (Data Ingestion)
# MAGIC * Section 2 is likely 10-15 questions total
# MAGIC * Expect 2-4 questions specifically on Lakeflow Connect
# MAGIC * Spend ~2 minutes per question, move on if stuck
# MAGIC
# MAGIC **If you're unsure**:
# MAGIC * Managed connectors are ALWAYS correct for well-known SaaS apps
# MAGIC * Standard JDBC is ALWAYS correct for relational databases
# MAGIC * Auto Loader is ALWAYS correct for continuous file ingestion
# MAGIC * COPY INTO is ALWAYS correct for infrequent batch file ingestion
# MAGIC * When in doubt, Databricks Secrets is the right answer for credentials
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## You're Ready
# MAGIC
# MAGIC You've completed comprehensive training on Lakeflow Connect. This topic is critical (21% weight) but also straightforward once you master the decision patterns. Trust your preparation, read questions carefully, and remember the core distinctions: databases vs files, standard vs managed, append vs upsert.
# MAGIC
# MAGIC Good luck on the exam.

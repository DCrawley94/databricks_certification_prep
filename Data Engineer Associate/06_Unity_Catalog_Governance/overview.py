# Databricks notebook source
# DBTITLE 1,Topic 6: Unity Catalog Governance
# MAGIC %md
# MAGIC # Topic 6: Unity Catalog Governance
# MAGIC
# MAGIC ## Introduction
# MAGIC
# MAGIC Unity Catalog provides centralized governance, security, and data management across Databricks workspaces and clouds.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC * Unity Catalog hierarchy and namespaces
# MAGIC * Permissions and grants (USAGE, SELECT, MODIFY, etc.)
# MAGIC * Managed vs external tables
# MAGIC * External locations and storage credentials
# MAGIC * Column-level masking and row-level security
# MAGIC * ABAC policies for dynamic access control
# MAGIC * Information schema queries
# MAGIC
# MAGIC ### Why This Matters for the Exam
# MAGIC * Section 7 (Governance and Security): 15% of exam (~7 questions)
# MAGIC * Permissions model heavily tested
# MAGIC * Managed vs external tables
# MAGIC * ABAC policies (new requirement per May 2026 exam guide)
# MAGIC * Column masking and row filters
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 1: Unity Catalog Hierarchy
# MAGIC %md
# MAGIC ## Concept 1: Unity Catalog Hierarchy (VERY HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### Three-Level Namespace
# MAGIC
# MAGIC Unity Catalog uses a three-level namespace:
# MAGIC
# MAGIC ```
# MAGIC metastore
# MAGIC   │
# MAGIC   ├── catalog_1
# MAGIC   │     ├── schema_a
# MAGIC   │     │     ├── table_1
# MAGIC   │     │     └── table_2
# MAGIC   │     └── schema_b
# MAGIC   │           └── table_3
# MAGIC   │
# MAGIC   └── catalog_2
# MAGIC         └── schema_c
# MAGIC               └── table_4
# MAGIC ```
# MAGIC
# MAGIC **Fully qualified name**: `catalog.schema.table`
# MAGIC
# MAGIC Example: `production.sales.orders`
# MAGIC
# MAGIC ### Metastore
# MAGIC
# MAGIC **What**: Top-level container for Unity Catalog metadata
# MAGIC
# MAGIC **Scope**: Account-level (spans multiple workspaces)
# MAGIC
# MAGIC **Key characteristics**:
# MAGIC * One metastore per region
# MAGIC * Attached to workspaces
# MAGIC * Contains catalogs
# MAGIC
# MAGIC ### Catalog
# MAGIC
# MAGIC **What**: First level of organization
# MAGIC
# MAGIC **Use cases**:
# MAGIC * Environment separation (dev, staging, prod)
# MAGIC * Business unit isolation (sales, marketing, finance)
# MAGIC * Data lake layers (bronze, silver, gold)
# MAGIC
# MAGIC **Creation**:
# MAGIC ```sql
# MAGIC CREATE CATALOG IF NOT EXISTS production;
# MAGIC ```
# MAGIC
# MAGIC ### Schema (Database)
# MAGIC
# MAGIC **What**: Second level of organization
# MAGIC
# MAGIC **Use cases**:
# MAGIC * Domain grouping (customers, orders, products)
# MAGIC * Project grouping
# MAGIC * Access control boundaries
# MAGIC
# MAGIC **Creation**:
# MAGIC ```sql
# MAGIC CREATE SCHEMA IF NOT EXISTS production.sales;
# MAGIC ```
# MAGIC
# MAGIC ### Table/View
# MAGIC
# MAGIC **What**: Third level (actual data container)
# MAGIC
# MAGIC **Types**:
# MAGIC * Managed tables
# MAGIC * External tables
# MAGIC * Views
# MAGIC * Materialized views
# MAGIC
# MAGIC **Creation**:
# MAGIC ```sql
# MAGIC CREATE TABLE production.sales.orders (
# MAGIC     order_id BIGINT,
# MAGIC     customer_id BIGINT,
# MAGIC     amount DECIMAL(10,2),
# MAGIC     order_date DATE
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC ### Default Catalog and Schema
# MAGIC
# MAGIC **Set default catalog**:
# MAGIC ```sql
# MAGIC USE CATALOG production;
# MAGIC ```
# MAGIC
# MAGIC **Set default schema**:
# MAGIC ```sql
# MAGIC USE SCHEMA sales;
# MAGIC -- Or
# MAGIC USE production.sales;
# MAGIC ```
# MAGIC
# MAGIC **After setting defaults, can reference tables directly**:
# MAGIC ```sql
# MAGIC SELECT * FROM orders;  -- Resolves to production.sales.orders
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "How many levels in Unity Catalog namespace?"
# MAGIC * Answer: 3 levels (catalog.schema.table)
# MAGIC
# MAGIC **Question**: "What is the scope of a metastore?"
# MAGIC * Answer: Account-level (spans workspaces)
# MAGIC
# MAGIC **Common trap**: Thinking catalog is the top level (wrong - metastore is)
# MAGIC
# MAGIC **Memory aid**: "M-C-S-T: Metastore → Catalog → Schema → Table"
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 2: Permissions and Grants
# MAGIC %md
# MAGIC ## Concept 2: Permissions and Grants (VERY HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### Permission Types
# MAGIC
# MAGIC #### USAGE
# MAGIC
# MAGIC **Required for**: Accessing catalog or schema
# MAGIC
# MAGIC **Does NOT grant**: Data access (read/write)
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant catalog access
# MAGIC GRANT USAGE ON CATALOG production TO `analysts`;
# MAGIC
# MAGIC -- Grant schema access
# MAGIC GRANT USAGE ON SCHEMA production.sales TO `analysts`;
# MAGIC ```
# MAGIC
# MAGIC **Think of USAGE as**: "Permission to navigate" (like execute permission on a directory)
# MAGIC
# MAGIC #### SELECT
# MAGIC
# MAGIC **Allows**: Reading table data
# MAGIC
# MAGIC ```sql
# MAGIC GRANT SELECT ON TABLE production.sales.orders TO `analysts`;
# MAGIC
# MAGIC -- Grant SELECT on all tables in schema
# MAGIC GRANT SELECT ON SCHEMA production.sales TO `analysts`;
# MAGIC ```
# MAGIC
# MAGIC #### MODIFY
# MAGIC
# MAGIC **Allows**: INSERT, UPDATE, DELETE, MERGE, TRUNCATE
# MAGIC
# MAGIC ```sql
# MAGIC GRANT MODIFY ON TABLE production.sales.orders TO `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC #### CREATE
# MAGIC
# MAGIC **Allows**: Creating child objects
# MAGIC
# MAGIC ```sql
# MAGIC -- Allow creating schemas in catalog
# MAGIC GRANT CREATE SCHEMA ON CATALOG production TO `data_engineers`;
# MAGIC
# MAGIC -- Allow creating tables in schema
# MAGIC GRANT CREATE TABLE ON SCHEMA production.sales TO `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC #### ALL PRIVILEGES
# MAGIC
# MAGIC **Grants**: All permissions on object
# MAGIC
# MAGIC ```sql
# MAGIC GRANT ALL PRIVILEGES ON SCHEMA production.sales TO `admins`;
# MAGIC ```
# MAGIC
# MAGIC ### Permission Hierarchy
# MAGIC
# MAGIC Permissions flow down the hierarchy:
# MAGIC
# MAGIC ```
# MAGIC Metastore
# MAGIC   │
# MAGIC   └── Catalog (needs USAGE)
# MAGIC         │
# MAGIC         └── Schema (needs USAGE)
# MAGIC               │
# MAGIC               └── Table (needs SELECT/MODIFY/etc.)
# MAGIC ```
# MAGIC
# MAGIC **To read a table, user needs**:
# MAGIC 1. USAGE on catalog
# MAGIC 2. USAGE on schema
# MAGIC 3. SELECT on table
# MAGIC
# MAGIC ### GRANT Syntax
# MAGIC
# MAGIC **Basic pattern**:
# MAGIC ```sql
# MAGIC GRANT <privilege> ON <object_type> <object_name> TO <principal>;
# MAGIC ```
# MAGIC
# MAGIC **Examples**:
# MAGIC ```sql
# MAGIC -- To user
# MAGIC GRANT SELECT ON TABLE production.sales.orders TO `user@example.com`;
# MAGIC
# MAGIC -- To group (note backticks)
# MAGIC GRANT SELECT ON TABLE production.sales.orders TO `analysts`;
# MAGIC
# MAGIC -- To service principal
# MAGIC GRANT SELECT ON TABLE production.sales.orders TO `sp-etl-prod`;
# MAGIC ```
# MAGIC
# MAGIC ### REVOKE Syntax
# MAGIC
# MAGIC ```sql
# MAGIC REVOKE <privilege> ON <object_type> <object_name> FROM <principal>;
# MAGIC
# MAGIC -- Example
# MAGIC REVOKE INSERT ON TABLE production.sales.orders FROM `analysts`;
# MAGIC ```
# MAGIC
# MAGIC ### DENY (Explicit Denial)
# MAGIC
# MAGIC **Purpose**: Explicitly block access (overrides GRANTs)
# MAGIC
# MAGIC ```sql
# MAGIC DENY SELECT ON TABLE production.sales.sensitive_data TO `analysts`;
# MAGIC ```
# MAGIC
# MAGIC **Precedence**: DENY > GRANT
# MAGIC
# MAGIC **Use case**: Grant broad access, then deny specific sensitive resources
# MAGIC
# MAGIC ### Show Grants
# MAGIC
# MAGIC **View permissions on object**:
# MAGIC ```sql
# MAGIC SHOW GRANTS ON TABLE production.sales.orders;
# MAGIC ```
# MAGIC
# MAGIC **View user's permissions**:
# MAGIC ```sql
# MAGIC SHOW GRANTS TO `user@example.com`;
# MAGIC ```
# MAGIC
# MAGIC ### Ownership
# MAGIC
# MAGIC **Set owner**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.sales.orders OWNER TO `admin_user`;
# MAGIC ```
# MAGIC
# MAGIC **Owners have**: Full control (like ALL PRIVILEGES)
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "User cannot SELECT from table despite SELECT grant. Why?"
# MAGIC * Answer: Missing USAGE on catalog or schema
# MAGIC
# MAGIC **Question**: "Which privilege allows creating tables in schema?"
# MAGIC * Answer: CREATE TABLE (or ALL PRIVILEGES)
# MAGIC
# MAGIC **Question**: "Does USAGE grant data read access?"
# MAGIC * Answer: No - USAGE only allows navigation, not data access
# MAGIC
# MAGIC **Common trap**: Forgetting USAGE is required at catalog/schema levels
# MAGIC
# MAGIC **Memory aid**: "USAGE to navigate, SELECT to read, MODIFY to write"
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 3: Managed vs External Tables
# MAGIC %md
# MAGIC ## Concept 3: Managed vs External Tables (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### Managed Tables
# MAGIC
# MAGIC **Definition**: Unity Catalog manages both metadata AND data
# MAGIC
# MAGIC **Data location**: Unity Catalog-managed cloud storage
# MAGIC
# MAGIC **Creation**:
# MAGIC ```sql
# MAGIC CREATE TABLE production.sales.orders (
# MAGIC     order_id BIGINT,
# MAGIC     amount DECIMAL(10,2)
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Deletion behavior**:
# MAGIC ```sql
# MAGIC DROP TABLE production.sales.orders;
# MAGIC -- Result: Both metadata AND data deleted
# MAGIC ```
# MAGIC
# MAGIC **Use cases**:
# MAGIC * Curated data within Unity Catalog
# MAGIC * Full lifecycle management by UC
# MAGIC * Recommended for most use cases
# MAGIC
# MAGIC ### External Tables
# MAGIC
# MAGIC **Definition**: Unity Catalog manages metadata, data stays in external location
# MAGIC
# MAGIC **Data location**: User-managed cloud storage (S3/ADLS/GCS)
# MAGIC
# MAGIC **Creation**:
# MAGIC ```sql
# MAGIC CREATE EXTERNAL TABLE production.raw.source_data
# MAGIC LOCATION 's3://my-bucket/data/'
# MAGIC ```
# MAGIC
# MAGIC **Deletion behavior**:
# MAGIC ```sql
# MAGIC DROP TABLE production.raw.source_data;
# MAGIC -- Result: Only metadata deleted, data remains in S3
# MAGIC ```
# MAGIC
# MAGIC **Use cases**:
# MAGIC * Data shared with non-Databricks systems
# MAGIC * Legacy data in existing locations
# MAGIC * Strict data sovereignty requirements
# MAGIC
# MAGIC ### Comparison
# MAGIC
# MAGIC | Feature | Managed Table | External Table |
# MAGIC |---------|---------------|----------------|
# MAGIC | Metadata management | UC | UC |
# MAGIC | Data management | UC | User |
# MAGIC | Data location | UC storage | User-specified |
# MAGIC | DROP behavior | Deletes data | Keeps data |
# MAGIC | Recommended | Default choice | Special cases |
# MAGIC | Exam frequency | Very High | Very High |
# MAGIC
# MAGIC ### Converting Between Types
# MAGIC
# MAGIC **Managed to External**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.sales.orders
# MAGIC SET TBLPROPERTIES ('external' = 'true');
# MAGIC ```
# MAGIC
# MAGIC **External to Managed**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.raw.source_data
# MAGIC SET TBLPROPERTIES ('external' = 'false');
# MAGIC ```
# MAGIC
# MAGIC ### External Locations
# MAGIC
# MAGIC **Required for**: Creating external tables
# MAGIC
# MAGIC **Creation**:
# MAGIC ```sql
# MAGIC CREATE EXTERNAL LOCATION s3_raw
# MAGIC URL 's3://my-bucket/raw/'
# MAGIC WITH (STORAGE CREDENTIAL aws_s3_cred);
# MAGIC ```
# MAGIC
# MAGIC **Grants**:
# MAGIC ```sql
# MAGIC GRANT READ FILES, WRITE FILES
# MAGIC ON EXTERNAL LOCATION s3_raw
# MAGIC TO `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC ### Storage Credentials
# MAGIC
# MAGIC **Purpose**: Authenticate to cloud storage
# MAGIC
# MAGIC **Creation** (via UI or CLI, not SQL)
# MAGIC
# MAGIC **Use**: Referenced in external location definition
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "What happens to data when external table is dropped?"
# MAGIC * Answer: Data remains in storage, only metadata deleted
# MAGIC
# MAGIC **Question**: "What happens to data when managed table is dropped?"
# MAGIC * Answer: Both metadata and data deleted
# MAGIC
# MAGIC **Question**: "Which table type is recommended by default?"
# MAGIC * Answer: Managed tables
# MAGIC
# MAGIC **Common trap**: Thinking external tables delete data on DROP (they don't)
# MAGIC
# MAGIC **Memory aid**: "Managed = UC owns all, External = UC owns metadata only"
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 4: ABAC Policies (NEW - HIGH EXAM FREQUENCY)
# MAGIC %md
# MAGIC ## Concept 4: ABAC Policies (Attribute-Based Access Control) (NEW - HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### What are ABAC Policies?
# MAGIC
# MAGIC ABAC policies provide **centralized, dynamic access control** based on user attributes (group membership, job titles, departments) WITHOUT modifying table definitions.
# MAGIC
# MAGIC ### ABAC vs Traditional Column Masks / Row Filters
# MAGIC
# MAGIC | Feature | Traditional Masks/Filters | ABAC Policies |
# MAGIC |---------|---------------------------|---------------|
# MAGIC | Definition location | On table (ALTER TABLE) | Centralized policy store |
# MAGIC | Scope | Single table | Multiple tables |
# MAGIC | Management | Per-table setup | Policy-based |
# MAGIC | Reusability | Must recreate per table | Reusable across tables |
# MAGIC | Exam focus | Medium | High (NEW) |
# MAGIC
# MAGIC **Key difference**: ABAC policies are **centralized and reusable**, traditional masks/filters are **per-table**.
# MAGIC
# MAGIC ### ABAC Policy Components
# MAGIC
# MAGIC #### 1. Policy Definition
# MAGIC
# MAGIC **Purpose**: Define access rule based on attributes
# MAGIC
# MAGIC **Example - Row Filter Policy**:
# MAGIC ```sql
# MAGIC CREATE ROW FILTER POLICY region_filter
# MAGIC AS (region STRING)
# MAGIC RETURN region IN (
# MAGIC     SELECT region FROM authorized_regions
# MAGIC     WHERE user = current_user()
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Example - Column Mask Policy**:
# MAGIC ```sql
# MAGIC CREATE COLUMN MASK POLICY ssn_mask
# MAGIC AS (ssn STRING)
# MAGIC RETURN 
# MAGIC     CASE
# MAGIC         WHEN is_account_group_member('compliance') THEN ssn
# MAGIC         WHEN is_account_group_member('analysts') THEN CONCAT('XXX-XX-', RIGHT(ssn, 4))
# MAGIC         ELSE 'REDACTED'
# MAGIC     END;
# MAGIC ```
# MAGIC
# MAGIC #### 2. Policy Application
# MAGIC
# MAGIC **Apply row filter**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.sales.orders
# MAGIC APPLY ROW FILTER region_filter ON (region);
# MAGIC ```
# MAGIC
# MAGIC **Apply column mask**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.customers
# MAGIC APPLY COLUMN MASK ssn_mask ON (ssn);
# MAGIC ```
# MAGIC
# MAGIC ### ABAC Policy Types
# MAGIC
# MAGIC #### Row Filter Policies
# MAGIC
# MAGIC **Purpose**: Filter which rows user can see
# MAGIC
# MAGIC **Common use cases**:
# MAGIC * Multi-tenant data isolation
# MAGIC * Geographic restrictions
# MAGIC * Department-level access
# MAGIC
# MAGIC **Example - Department-Based Access**:
# MAGIC ```sql
# MAGIC CREATE ROW FILTER POLICY dept_filter
# MAGIC AS (department STRING)
# MAGIC RETURN department = (
# MAGIC     SELECT department FROM employee_info
# MAGIC     WHERE email = current_user()
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE production.sales.transactions
# MAGIC APPLY ROW FILTER dept_filter ON (department);
# MAGIC ```
# MAGIC
# MAGIC **Result**: Users only see rows matching their department
# MAGIC
# MAGIC #### Column Mask Policies
# MAGIC
# MAGIC **Purpose**: Transform or redact column values
# MAGIC
# MAGIC **Common use cases**:
# MAGIC * PII protection (SSN, credit cards)
# MAGIC * Salary information
# MAGIC * Sensitive business data
# MAGIC
# MAGIC **Example - Tiered Salary Visibility**:
# MAGIC ```sql
# MAGIC CREATE COLUMN MASK POLICY salary_mask
# MAGIC AS (salary DECIMAL)
# MAGIC RETURN 
# MAGIC     CASE
# MAGIC         WHEN is_account_group_member('hr_admin') THEN salary
# MAGIC         WHEN is_account_group_member('managers') THEN ROUND(salary, -3)
# MAGIC         ELSE NULL
# MAGIC     END;
# MAGIC
# MAGIC ALTER TABLE production.hr.employees
# MAGIC APPLY COLUMN MASK salary_mask ON (salary);
# MAGIC ```
# MAGIC
# MAGIC **Result**:
# MAGIC * HR admins: See exact salary
# MAGIC * Managers: See rounded salary (e.g., 75000 instead of 75432)
# MAGIC * Others: See NULL
# MAGIC
# MAGIC ### Policy Functions
# MAGIC
# MAGIC **Check group membership**:
# MAGIC ```sql
# MAGIC is_account_group_member('group_name')
# MAGIC ```
# MAGIC
# MAGIC **Get current user**:
# MAGIC ```sql
# MAGIC current_user()
# MAGIC ```
# MAGIC
# MAGIC **Check multiple groups**:
# MAGIC ```sql
# MAGIC CASE
# MAGIC     WHEN is_account_group_member('admin') THEN <full_access>
# MAGIC     WHEN is_account_group_member('analyst') THEN <limited_access>
# MAGIC     ELSE <no_access>
# MAGIC END
# MAGIC ```
# MAGIC
# MAGIC ### Removing Policies
# MAGIC
# MAGIC **Drop row filter**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.sales.orders
# MAGIC DROP ROW FILTER;
# MAGIC ```
# MAGIC
# MAGIC **Drop column mask**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.customers
# MAGIC DROP COLUMN MASK ON (ssn);
# MAGIC ```
# MAGIC
# MAGIC **Delete policy definition**:
# MAGIC ```sql
# MAGIC DROP ROW FILTER POLICY region_filter;
# MAGIC DROP COLUMN MASK POLICY ssn_mask;
# MAGIC ```
# MAGIC
# MAGIC ### ABAC Policy Advantages
# MAGIC
# MAGIC **1. Centralized Management**:
# MAGIC * Define policy once
# MAGIC * Apply to multiple tables
# MAGIC * Update in one place
# MAGIC
# MAGIC **2. Dynamic Access**:
# MAGIC * Automatically adjusts to user attributes
# MAGIC * No manual grant updates needed
# MAGIC * Group membership changes propagate automatically
# MAGIC
# MAGIC **3. Reusability**:
# MAGIC * Same policy across tables
# MAGIC * Consistent security rules
# MAGIC * Reduced maintenance
# MAGIC
# MAGIC **4. Auditability**:
# MAGIC * Central policy definitions
# MAGIC * Clear access logic
# MAGIC * Easier compliance
# MAGIC
# MAGIC ### Real-World Example
# MAGIC
# MAGIC **Scenario**: Multi-region sales data, users should only see their region
# MAGIC
# MAGIC **Step 1 - Create Policy**:
# MAGIC ```sql
# MAGIC CREATE ROW FILTER POLICY regional_access
# MAGIC AS (sales_region STRING)
# MAGIC RETURN sales_region IN (
# MAGIC     SELECT region FROM user_regions
# MAGIC     WHERE user_email = current_user()
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Step 2 - Apply to Tables**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.sales.orders
# MAGIC APPLY ROW FILTER regional_access ON (sales_region);
# MAGIC
# MAGIC ALTER TABLE production.sales.customers
# MAGIC APPLY ROW FILTER regional_access ON (customer_region);
# MAGIC
# MAGIC ALTER TABLE production.sales.revenue
# MAGIC APPLY ROW FILTER regional_access ON (region);
# MAGIC ```
# MAGIC
# MAGIC **Step 3 - Manage Access via Table**:
# MAGIC ```sql
# MAGIC INSERT INTO user_regions VALUES ('user@example.com', 'US_WEST');
# MAGIC -- Now user automatically sees only US_WEST data in all three tables
# MAGIC ```
# MAGIC
# MAGIC **Benefits**:
# MAGIC * Single policy for multiple tables
# MAGIC * No per-table grants needed
# MAGIC * Add users by updating `user_regions` table
# MAGIC * Change regions without policy modifications
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "What is the main advantage of ABAC policies over traditional column masks?"
# MAGIC * Answer: Centralized and reusable across multiple tables
# MAGIC
# MAGIC **Question**: "How do you apply a row filter policy to a table?"
# MAGIC * Answer: `ALTER TABLE <table> APPLY ROW FILTER <policy> ON (<column>)`
# MAGIC
# MAGIC **Question**: "Which function checks if user is in a group?"
# MAGIC * Answer: `is_account_group_member('group_name')`
# MAGIC
# MAGIC **Question**: "What happens when you DROP a table with ABAC policies applied?"
# MAGIC * Answer: Table and its data deleted (if managed); policy definition remains and can be applied to other tables
# MAGIC
# MAGIC **Common trap**: Thinking ABAC policies are stored on the table (wrong - they're centralized and referenced)
# MAGIC
# MAGIC **Memory aid**: "ABAC = Centralized rules, applied to many tables"
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 5: Traditional Column Masking & Row Filters
# MAGIC %md
# MAGIC ## Concept 5: Traditional Column Masking & Row Filters (MEDIUM EXAM FREQUENCY)
# MAGIC
# MAGIC ### Column-Level Masking (Per-Table)
# MAGIC
# MAGIC **Purpose**: Dynamically transform column values based on user identity
# MAGIC
# MAGIC **Syntax**:
# MAGIC ```sql
# MAGIC CREATE FUNCTION mask_ssn(ssn STRING)
# MAGIC RETURN 
# MAGIC     CASE
# MAGIC         WHEN is_account_group_member('compliance') THEN ssn
# MAGIC         ELSE 'XXX-XX-XXXX'
# MAGIC     END;
# MAGIC
# MAGIC ALTER TABLE production.customers
# MAGIC ALTER COLUMN ssn SET MASK mask_ssn;
# MAGIC ```
# MAGIC
# MAGIC **Result**:
# MAGIC * Compliance group: Sees real SSN
# MAGIC * All others: Sees masked value
# MAGIC
# MAGIC **Remove mask**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.customers
# MAGIC ALTER COLUMN ssn DROP MASK;
# MAGIC ```
# MAGIC
# MAGIC ### Row-Level Security (Per-Table)
# MAGIC
# MAGIC **Purpose**: Filter rows based on user identity
# MAGIC
# MAGIC **Syntax**:
# MAGIC ```sql
# MAGIC CREATE FUNCTION row_filter(region STRING)
# MAGIC RETURN region = current_user().split('@')[0];
# MAGIC
# MAGIC ALTER TABLE production.sales
# MAGIC SET ROW FILTER row_filter ON (region);
# MAGIC ```
# MAGIC
# MAGIC **Result**: Users only see rows where region matches their username prefix
# MAGIC
# MAGIC **Remove filter**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.sales
# MAGIC DROP ROW FILTER;
# MAGIC ```
# MAGIC
# MAGIC ### Differences from ABAC Policies
# MAGIC
# MAGIC | Aspect | Traditional (Per-Table) | ABAC (Centralized) |
# MAGIC |--------|------------------------|--------------------|
# MAGIC | Scope | Single table | Multiple tables |
# MAGIC | Definition | Function + ALTER TABLE | Policy + ALTER TABLE |
# MAGIC | Reuse | Must recreate per table | Apply same policy |
# MAGIC | Management | Decentralized | Centralized |
# MAGIC | Exam coverage | Medium | High (NEW) |
# MAGIC
# MAGIC **When to use traditional approach**:
# MAGIC * Single table with unique masking logic
# MAGIC * Table-specific requirements
# MAGIC * Simple transformations
# MAGIC
# MAGIC **When to use ABAC policies**:
# MAGIC * Multiple tables need same logic
# MAGIC * Centralized security management
# MAGIC * Organization-wide policies
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "How do you apply column mask to existing table?"
# MAGIC * Answer: `ALTER TABLE <table> ALTER COLUMN <col> SET MASK <function>`
# MAGIC
# MAGIC **Question**: "Difference between SET MASK and APPLY COLUMN MASK?"
# MAGIC * Answer: SET MASK uses inline function (per-table), APPLY COLUMN MASK uses ABAC policy (centralized)
# MAGIC
# MAGIC **Memory aid**: "SET = single table, APPLY = policy (reusable)"
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Key Takeaways & Exam Focus
# MAGIC %md
# MAGIC ## Key Takeaways & Exam Focus
# MAGIC
# MAGIC ### Most Testable Concepts
# MAGIC
# MAGIC 1. **Unity Catalog Hierarchy** (Very High Frequency)
# MAGIC    * Three levels: catalog.schema.table
# MAGIC    * Metastore scope: account-level
# MAGIC    * Fully qualified names required
# MAGIC
# MAGIC 2. **Permissions Model** (Very High Frequency)
# MAGIC    * USAGE required for catalog/schema navigation
# MAGIC    * SELECT for reading, MODIFY for writing
# MAGIC    * Permission flow: catalog USAGE → schema USAGE → table SELECT
# MAGIC    * DENY overrides GRANT
# MAGIC
# MAGIC 3. **Managed vs External Tables** (Very High Frequency)
# MAGIC    * Managed: UC manages metadata + data
# MAGIC    * External: UC manages metadata only
# MAGIC    * DROP behavior: managed deletes data, external keeps data
# MAGIC    * External requires storage credentials + external locations
# MAGIC
# MAGIC 4. **ABAC Policies** (High Frequency - NEW)
# MAGIC    * Centralized row filters and column masks
# MAGIC    * CREATE POLICY → ALTER TABLE APPLY
# MAGIC    * Reusable across multiple tables
# MAGIC    * `is_account_group_member()` for group checks
# MAGIC    * Row filter vs column mask purposes
# MAGIC
# MAGIC 5. **Information Schema** (Medium Frequency)
# MAGIC    * Query metadata: `information_schema.tables`, `information_schema.columns`
# MAGIC    * `SHOW GRANTS` for permission audits
# MAGIC
# MAGIC ### Decision Matrices
# MAGIC
# MAGIC **Table Type Selection**:
# MAGIC * Data fully managed by UC → Managed table (default)
# MAGIC * Data shared with external systems → External table
# MAGIC * Strict data location requirements → External table
# MAGIC * Need automatic cleanup on DROP → Managed table
# MAGIC
# MAGIC **Access Control Approach**:
# MAGIC * Single table, unique logic → Traditional column mask / row filter
# MAGIC * Multiple tables, same logic → ABAC policy
# MAGIC * Organization-wide security → ABAC policy
# MAGIC * Simple table-specific mask → Traditional approach
# MAGIC
# MAGIC **Permission Strategy**:
# MAGIC * Read-only analysts → USAGE (catalog/schema) + SELECT (tables)
# MAGIC * Data engineers → USAGE + SELECT + MODIFY + CREATE
# MAGIC * Admins → ALL PRIVILEGES + ownership
# MAGIC * Block specific access → DENY (overrides grants)
# MAGIC
# MAGIC ### Common Exam Traps
# MAGIC
# MAGIC **Trap 1**: "User has SELECT on table but can't query it"
# MAGIC * Missing: USAGE on catalog or schema
# MAGIC
# MAGIC **Trap 2**: "External table DROP deletes data"
# MAGIC * False! Only metadata deleted, data remains
# MAGIC
# MAGIC **Trap 3**: "USAGE grants data read access"
# MAGIC * False! USAGE only allows navigation, need SELECT for data
# MAGIC
# MAGIC **Trap 4**: "Column masks are centralized across tables"
# MAGIC * False for traditional masks! True for ABAC policies.
# MAGIC
# MAGIC **Trap 5**: "Catalog is the top level of hierarchy"
# MAGIC * False! Metastore is top level, catalog is second.
# MAGIC
# MAGIC ### Syntax Quick Reference
# MAGIC
# MAGIC **Create Hierarchy**:
# MAGIC ```sql
# MAGIC CREATE CATALOG production;
# MAGIC CREATE SCHEMA production.sales;
# MAGIC CREATE TABLE production.sales.orders (...);
# MAGIC ```
# MAGIC
# MAGIC **Grant Permissions**:
# MAGIC ```sql
# MAGIC GRANT USAGE ON CATALOG production TO `analysts`;
# MAGIC GRANT USAGE ON SCHEMA production.sales TO `analysts`;
# MAGIC GRANT SELECT ON TABLE production.sales.orders TO `analysts`;
# MAGIC ```
# MAGIC
# MAGIC **Create External Table**:
# MAGIC ```sql
# MAGIC CREATE EXTERNAL TABLE production.raw.data
# MAGIC LOCATION 's3://bucket/path/';
# MAGIC ```
# MAGIC
# MAGIC **ABAC Row Filter**:
# MAGIC ```sql
# MAGIC CREATE ROW FILTER POLICY policy_name
# MAGIC AS (column TYPE) RETURN <condition>;
# MAGIC
# MAGIC ALTER TABLE table_name
# MAGIC APPLY ROW FILTER policy_name ON (column);
# MAGIC ```
# MAGIC
# MAGIC **ABAC Column Mask**:
# MAGIC ```sql
# MAGIC CREATE COLUMN MASK POLICY policy_name
# MAGIC AS (column TYPE) RETURN <transformation>;
# MAGIC
# MAGIC ALTER TABLE table_name
# MAGIC APPLY COLUMN MASK policy_name ON (column);
# MAGIC ```
# MAGIC
# MAGIC **Show Grants**:
# MAGIC ```sql
# MAGIC SHOW GRANTS ON TABLE production.sales.orders;
# MAGIC ```
# MAGIC
# MAGIC ### Study Priorities
# MAGIC
# MAGIC **High Priority** (spend most time here):
# MAGIC 1. Three-level namespace and hierarchy
# MAGIC 2. USAGE + SELECT + MODIFY permissions
# MAGIC 3. Managed vs external table DROP behavior
# MAGIC 4. ABAC policy creation and application
# MAGIC 5. Permission troubleshooting (missing USAGE)
# MAGIC
# MAGIC **Medium Priority**:
# MAGIC 1. External locations and storage credentials
# MAGIC 2. DENY vs GRANT precedence
# MAGIC 3. Traditional column masks vs ABAC
# MAGIC 4. Information schema queries
# MAGIC 5. Ownership transfers
# MAGIC
# MAGIC **Lower Priority** (know concepts, less syntax detail):
# MAGIC 1. Service principal grants
# MAGIC 2. Advanced permission hierarchies
# MAGIC 3. Metastore administration
# MAGIC 4. Detailed information schema tables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **End of Topic 6 Overview**

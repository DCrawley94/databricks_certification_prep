# Databricks notebook source
# DBTITLE 1,Topic 6 Solutions - All Exercises
# MAGIC %md
# MAGIC # Topic 6: Unity Catalog & Governance - Complete Solutions
# MAGIC
# MAGIC ## Exercises 1-20, MCQs, Challenge, Applied
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 1: Three-Level Namespace
# MAGIC **Answer**: The three levels are: **Catalog → Schema → Table**
# MAGIC
# MAGIC Hierarchy: `catalog.schema.table`
# MAGIC
# MAGIC **Explanation**: Unity Catalog uses a three-level namespace where metastore (account-level) contains catalogs, catalogs contain schemas, and schemas contain tables/views.
# MAGIC
# MAGIC **Exam trap**: Don't forget metastore exists above catalogs (but it's implicit in namespace).
# MAGIC
# MAGIC **Memory aid**: "C-S-T: Catalog.Schema.Table"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 2: Create Catalog and Schema
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC CREATE CATALOG IF NOT EXISTS analytics;
# MAGIC CREATE SCHEMA IF NOT EXISTS analytics.sales;
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: Catalogs and schemas are created with simple CREATE statements. Use IF NOT EXISTS to avoid errors if they exist.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 3: USAGE Permission
# MAGIC **Answer**: Missing `USAGE` permission on catalog (`production`) or schema (`sales`), or both.
# MAGIC
# MAGIC **Explanation**: To query a table, user needs:
# MAGIC 1. USAGE on catalog
# MAGIC 2. USAGE on schema  
# MAGIC 3. SELECT on table
# MAGIC
# MAGIC Missing any of these blocks access.
# MAGIC
# MAGIC **Exam trap**: Very common exam question - SELECT alone is insufficient.
# MAGIC
# MAGIC **Memory aid**: "Need USAGE path to reach SELECT destination"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 4: Grant Complete Access
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC GRANT USAGE ON CATALOG production TO `analysts`;
# MAGIC GRANT USAGE ON SCHEMA production.sales TO `analysts`;
# MAGIC GRANT SELECT ON TABLE production.sales.orders TO `analysts`;
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: All three grants required. Order doesn't matter but logical to grant top-down.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 5: MODIFY Permission
# MAGIC **Answer**: MODIFY allows: INSERT, UPDATE, DELETE, MERGE, TRUNCATE
# MAGIC
# MAGIC **Explanation**: MODIFY is the "write" permission. Does NOT include creating/dropping tables (need CREATE/DROP privileges).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 6: DENY vs GRANT
# MAGIC **Answer**: No, user cannot read the table. DENY overrides GRANT.
# MAGIC
# MAGIC **Explanation**: DENY has precedence over GRANT. Even with schema-level SELECT, table-level DENY blocks access.
# MAGIC
# MAGIC **Exam trap**: High-frequency question on precedence rules.
# MAGIC
# MAGIC **Memory aid**: "DENY wins the fight"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 7: Show Grants
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC SHOW GRANTS ON TABLE production.sales.orders;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 8: Revoke Permission
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC REVOKE INSERT ON TABLE production.sales.orders FROM `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 9: Transfer Ownership
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.sales.orders OWNER TO `admin@example.com`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 10: Permission Hierarchy
# MAGIC **Answer**: Three permissions required:
# MAGIC 1. USAGE on catalog_a
# MAGIC 2. USAGE on catalog_a.schema_b
# MAGIC 3. SELECT on catalog_a.schema_b.table_c
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## ABAC Policies (Exercises 11-15)
# MAGIC
# MAGIC ### Exercise 11: ABAC vs Traditional
# MAGIC **Answer**: ABAC policies are **centralized and reusable** across multiple tables, while traditional masks/filters must be defined per-table.
# MAGIC
# MAGIC **Explanation**: 
# MAGIC - Traditional: CREATE FUNCTION + ALTER TABLE (per table)
# MAGIC - ABAC: CREATE POLICY (once) + ALTER TABLE APPLY (to many tables)
# MAGIC
# MAGIC **Exam trap**: Key differentiator for exam - centralization and reusability.
# MAGIC
# MAGIC **Memory aid**: "ABAC = Apply Once, use Many times"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 12: Create Row Filter Policy
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC CREATE ROW FILTER POLICY region_access
# MAGIC AS (region STRING)
# MAGIC RETURN region IN (
# MAGIC     SELECT authorized_region
# MAGIC     FROM user_regions
# MAGIC     WHERE user_email = current_user()
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: Policy checks if row's region matches user's authorized regions from lookup table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 13: Apply Row Filter
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC ALTER TABLE production.sales.orders
# MAGIC APPLY ROW FILTER region_access ON (sales_region);
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: Applies the policy to the `sales_region` column of the table.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 14: Create Column Mask Policy
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC CREATE COLUMN MASK POLICY ssn_mask
# MAGIC AS (ssn STRING)
# MAGIC RETURN 
# MAGIC     CASE
# MAGIC         WHEN is_account_group_member('compliance') THEN ssn
# MAGIC         WHEN is_account_group_member('hr_managers') THEN CONCAT('XXX-XX-', RIGHT(ssn, 4))
# MAGIC         ELSE 'REDACTED'
# MAGIC     END;
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: Tiered visibility based on group membership using `is_account_group_member()`.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 15: Remove ABAC Policy
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC -- a) Remove row filter from table
# MAGIC ALTER TABLE production.sales.orders
# MAGIC DROP ROW FILTER;
# MAGIC
# MAGIC -- b) Delete policy definition
# MAGIC DROP ROW FILTER POLICY region_access;
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: Two-step process: unapply from tables, then drop policy.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Tables & Metadata (Exercises 16-20)
# MAGIC
# MAGIC ### Exercise 16: Managed vs External
# MAGIC **Answer**:
# MAGIC - **Managed table**: Both metadata AND data deleted
# MAGIC - **External table**: Only metadata deleted, data remains in storage location
# MAGIC
# MAGIC **Exam trap**: Most frequent managed vs external question.
# MAGIC
# MAGIC **Memory aid**: "Managed = total cleanup, External = metadata only"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 17: Create External Table
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC CREATE EXTERNAL TABLE raw.source_data
# MAGIC LOCATION 's3://mybucket/data/';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 18: Convert Table Type
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC ALTER TABLE table_name
# MAGIC SET TBLPROPERTIES ('external' = 'true');
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 19: External Location Components
# MAGIC **Answer**: Two components required:
# MAGIC 1. **Storage credential** (authenticates to cloud storage)
# MAGIC 2. **External location** (URL + credential reference)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 20: Information Schema
# MAGIC **Answer**:
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM production.information_schema.tables
# MAGIC WHERE table_schema = 'sales';
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## MCQ Answers
# MAGIC
# MAGIC **MCQ 1**: B) 3 levels (catalog.schema.table)
# MAGIC
# MAGIC **MCQ 2**: B) Account-level
# MAGIC
# MAGIC **MCQ 3**: B) Only metadata deleted, data remains
# MAGIC
# MAGIC **MCQ 4**: B) Can be applied to multiple tables
# MAGIC
# MAGIC **MCQ 5**: C) Navigate to catalog/schema
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Challenge Solution
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1: Create catalog and schemas
# MAGIC CREATE CATALOG global_sales;
# MAGIC CREATE SCHEMA global_sales.us_region;
# MAGIC CREATE SCHEMA global_sales.eu_region;
# MAGIC CREATE SCHEMA global_sales.apac_region;
# MAGIC
# MAGIC -- Step 2: Create tables (example for us_region, repeat for others)
# MAGIC CREATE TABLE global_sales.us_region.orders (
# MAGIC     order_id BIGINT,
# MAGIC     customer_ssn STRING,
# MAGIC     amount DECIMAL(10,2),
# MAGIC     region STRING
# MAGIC );
# MAGIC -- Repeat for eu_region.orders and apac_region.orders
# MAGIC
# MAGIC -- Step 3: Create ABAC row filter policy
# MAGIC CREATE ROW FILTER POLICY regional_row_filter
# MAGIC AS (region STRING)
# MAGIC RETURN region IN (
# MAGIC     SELECT authorized_region
# MAGIC     FROM access_control.user_regions
# MAGIC     WHERE user_email = current_user()
# MAGIC );
# MAGIC
# MAGIC -- Step 4: Create ABAC column mask policy
# MAGIC CREATE COLUMN MASK POLICY ssn_protection
# MAGIC AS (ssn STRING)
# MAGIC RETURN 
# MAGIC     CASE
# MAGIC         WHEN is_account_group_member('global_compliance') THEN ssn
# MAGIC         ELSE 'XXX-XX-XXXX'
# MAGIC     END;
# MAGIC
# MAGIC -- Step 5: Apply policies to all tables
# MAGIC ALTER TABLE global_sales.us_region.orders
# MAGIC APPLY ROW FILTER regional_row_filter ON (region);
# MAGIC ALTER TABLE global_sales.us_region.orders
# MAGIC APPLY COLUMN MASK ssn_protection ON (customer_ssn);
# MAGIC -- Repeat for eu_region and apac_region tables
# MAGIC
# MAGIC -- Step 6: Grant permissions
# MAGIC GRANT USAGE ON CATALOG global_sales TO `us_analysts`;
# MAGIC GRANT USAGE ON SCHEMA global_sales.us_region TO `us_analysts`;
# MAGIC GRANT SELECT ON SCHEMA global_sales.us_region TO `us_analysts`;
# MAGIC -- Repeat for eu_analysts and apac_analysts with their respective schemas
# MAGIC
# MAGIC GRANT USAGE ON CATALOG global_sales TO `data_engineers`;
# MAGIC GRANT USAGE, SELECT, MODIFY ON ALL SCHEMAS IN CATALOG global_sales TO `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Applied: Permission Strategy Framework
# MAGIC
# MAGIC ### 1. USAGE vs SELECT vs MODIFY
# MAGIC
# MAGIC **USAGE when**: User needs to navigate/discover catalog or schema
# MAGIC - List tables in schema
# MAGIC - See schema metadata
# MAGIC - Access child objects (with appropriate permissions)
# MAGIC
# MAGIC **SELECT when**: User needs read-only data access
# MAGIC - Query tables
# MAGIC - Create dashboards
# MAGIC - Run reports
# MAGIC
# MAGIC **MODIFY when**: User needs write access
# MAGIC - INSERT, UPDATE, DELETE operations
# MAGIC - Data pipeline write stages
# MAGIC - ETL processes
# MAGIC
# MAGIC ### 2. GRANT vs DENY
# MAGIC
# MAGIC **Use GRANT** (default):
# MAGIC - Normal permission assignment
# MAGIC - Positive access control
# MAGIC - Most use cases
# MAGIC
# MAGIC **Use DENY** (exception-based):
# MAGIC - Override broader grants
# MAGIC - Protect sensitive subsets
# MAGIC - Example: GRANT SELECT on schema, DENY SELECT on sensitive table
# MAGIC
# MAGIC **Rule**: DENY always wins over GRANT
# MAGIC
# MAGIC ### 3. ABAC vs Traditional Masks
# MAGIC
# MAGIC **Use ABAC policies when**:
# MAGIC - Same logic needed across multiple tables
# MAGIC - Centralized security management required
# MAGIC - Organization-wide policy enforcement
# MAGIC - Example: Regional access rules for 20 tables
# MAGIC
# MAGIC **Use traditional masks when**:
# MAGIC - Single table with unique requirements
# MAGIC - Table-specific transformation logic
# MAGIC - Simple, one-off masking
# MAGIC - Example: Specific PII field in one table
# MAGIC
# MAGIC ### 4. Managed vs External Tables
# MAGIC
# MAGIC **Use managed tables** (default):
# MAGIC - Data fully owned by Unity Catalog
# MAGIC - Want automatic cleanup on DROP
# MAGIC - Standard data warehouse tables
# MAGIC - No external sharing requirements
# MAGIC
# MAGIC **Use external tables when**:
# MAGIC - Data shared with non-Databricks systems
# MAGIC - Strict data location requirements (compliance)
# MAGIC - Legacy data in existing storage
# MAGIC - Need to preserve data after table deletion
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **End of Topic 6 Solutions**

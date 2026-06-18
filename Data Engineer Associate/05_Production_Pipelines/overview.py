# Databricks notebook source
# DBTITLE 1,Topic 5: Production Pipelines & CI/CD
# MAGIC %md
# MAGIC # Topic 5: Production Pipelines & CI/CD
# MAGIC
# MAGIC ## Introduction
# MAGIC
# MAGIC This topic covers orchestration, scheduling, and deployment of production data pipelines using Lakeflow Jobs, Automation Bundles, and Git integration.
# MAGIC
# MAGIC ### What You'll Learn
# MAGIC * Lakeflow Jobs configuration and orchestration
# MAGIC * Task dependencies and control flow
# MAGIC * Job scheduling and triggers
# MAGIC * Automation Bundles (DABs) for CI/CD
# MAGIC * Git Folders integration and workflows
# MAGIC * Deployment patterns across environments
# MAGIC
# MAGIC ### Why This Matters for the Exam
# MAGIC * Section 4 (Jobs): 16% of exam (~7 questions)
# MAGIC * Section 5 (CI/CD): 10% of exam (~5 questions)
# MAGIC * Combined: 26% of exam (~12 questions)
# MAGIC * Heavily focused on Jobs UI, task configuration, and bundle deployment
# MAGIC * Git workflow and environment promotion patterns
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 1: Lakeflow Jobs Overview
# MAGIC %md
# MAGIC ## Concept 1: Lakeflow Jobs Overview (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### What are Lakeflow Jobs?
# MAGIC
# MAGIC Lakeflow Jobs (formerly Databricks Workflows) orchestrate multi-task data pipelines with:
# MAGIC * DAG-based task dependencies
# MAGIC * Automatic retries and failure handling
# MAGIC * Scheduled and triggered execution
# MAGIC * Integrated monitoring and alerting
# MAGIC
# MAGIC ### Job Components
# MAGIC
# MAGIC #### Task Types
# MAGIC
# MAGIC **1. Notebook Task**
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "process_data",
# MAGIC   "notebook_task": {
# MAGIC     "notebook_path": "/Users/user@example.com/process",
# MAGIC     "base_parameters": {"env": "prod"}
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **2. SQL Query Task**
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "aggregate",
# MAGIC   "sql_task": {
# MAGIC     "query": {
# MAGIC       "query_id": "abc123"
# MAGIC     },
# MAGIC     "warehouse_id": "xyz789"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **3. Pipeline Task (SDP)**
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "run_pipeline",
# MAGIC   "pipeline_task": {
# MAGIC     "pipeline_id": "pipeline123"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **4. Dashboard Task**
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "refresh_dashboard",
# MAGIC   "dashboard_task": {
# MAGIC     "dashboard_id": "dash456"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Task Dependencies
# MAGIC
# MAGIC **Sequential Execution**:
# MAGIC ```json
# MAGIC {
# MAGIC   "tasks": [
# MAGIC     {"task_key": "ingest"},
# MAGIC     {"task_key": "transform", "depends_on": [{"task_key": "ingest"}]},
# MAGIC     {"task_key": "aggregate", "depends_on": [{"task_key": "transform"}]}
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Parallel + Join Pattern**:
# MAGIC ```json
# MAGIC {
# MAGIC   "tasks": [
# MAGIC     {"task_key": "source_a"},
# MAGIC     {"task_key": "source_b"},
# MAGIC     {
# MAGIC       "task_key": "join",
# MAGIC       "depends_on": [
# MAGIC         {"task_key": "source_a"},
# MAGIC         {"task_key": "source_b"}
# MAGIC       ]
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Job Clusters vs All-Purpose Clusters
# MAGIC
# MAGIC | Feature | Job Cluster | All-Purpose Cluster |
# MAGIC |---------|-------------|---------------------|
# MAGIC | Lifecycle | Auto-terminates after job | Manually managed |
# MAGIC | Cost | Cheaper (no idle time) | More expensive |
# MAGIC | Use case | Production scheduled jobs | Interactive development |
# MAGIC | Sharing | Job-specific | Shared across users |
# MAGIC | Exam focus | Very High | Medium |
# MAGIC
# MAGIC ### Key Configuration Parameters
# MAGIC
# MAGIC **Retries**:
# MAGIC ```json
# MAGIC {
# MAGIC   "max_retries": 3,
# MAGIC   "retry_on_timeout": true,
# MAGIC   "timeout_seconds": 3600
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Email Notifications**:
# MAGIC ```json
# MAGIC {
# MAGIC   "email_notifications": {
# MAGIC     "on_start": ["team@example.com"],
# MAGIC     "on_success": [],
# MAGIC     "on_failure": ["oncall@example.com"],
# MAGIC     "no_alert_for_skipped_runs": true
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Concurrent Runs**:
# MAGIC ```json
# MAGIC {
# MAGIC   "max_concurrent_runs": 1  // Default, prevents overlapping runs
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "Which cluster type should be used for scheduled production ETL?"
# MAGIC * Answer: Job cluster (auto-terminates, cost-effective)
# MAGIC
# MAGIC **Question**: "How do you configure Task B to run after Task A?"
# MAGIC * Answer: `depends_on: [{task_key: "task_a"}]`
# MAGIC
# MAGIC **Common trap**: Thinking all-purpose clusters are better for production (wrong - job clusters are recommended)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 2: Job Scheduling and Triggers
# MAGIC %md
# MAGIC ## Concept 2: Job Scheduling and Triggers (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### Trigger Types
# MAGIC
# MAGIC #### 1. Scheduled (Time-Based)
# MAGIC
# MAGIC **Cron Expression** (Quartz format - 7 fields):
# MAGIC ```
# MAGIC <seconds> <minutes> <hours> <day-of-month> <month> <day-of-week> <year>
# MAGIC ```
# MAGIC
# MAGIC **Examples**:
# MAGIC ```
# MAGIC 0 0 2 * * ? *        // Daily at 2 AM
# MAGIC 0 30 */6 * * ? *     // Every 6 hours at :30
# MAGIC 0 0 8 ? * MON *      // Every Monday at 8 AM
# MAGIC 0 15 10 15 * ? *     // 15th of every month at 10:15 AM
# MAGIC ```
# MAGIC
# MAGIC **Configuration**:
# MAGIC ```json
# MAGIC {
# MAGIC   "schedule": {
# MAGIC     "quartz_cron_expression": "0 0 2 * * ? *",
# MAGIC     "timezone_id": "America/Los_Angeles",
# MAGIC     "pause_status": "UNPAUSED"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC #### 2. File Arrival Trigger
# MAGIC
# MAGIC **Purpose**: Start job when new files land in cloud storage
# MAGIC
# MAGIC **Configuration**:
# MAGIC ```json
# MAGIC {
# MAGIC   "trigger": {
# MAGIC     "file_arrival": {
# MAGIC       "url": "s3://bucket/path/",
# MAGIC       "min_time_between_triggers_seconds": 60
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC #### 3. Table Update Trigger
# MAGIC
# MAGIC **Purpose**: Start job when Unity Catalog table is updated
# MAGIC
# MAGIC **Configuration**:
# MAGIC ```json
# MAGIC {
# MAGIC   "trigger": {
# MAGIC     "table_update": {
# MAGIC       "table_names": ["catalog.schema.table"],
# MAGIC       "wait_after_last_change_seconds": 60
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC ### Time-Based vs Data-Driven Triggers
# MAGIC
# MAGIC | Trigger Type | Use Case | Exam Frequency |
# MAGIC |--------------|----------|----------------|
# MAGIC | **Time-based (Cron)** | Nightly batch, periodic refresh | Very High |
# MAGIC | **File arrival** | Event-driven ingestion | High |
# MAGIC | **Table update** | Downstream dependencies | Medium |
# MAGIC
# MAGIC ### When to Use Each
# MAGIC
# MAGIC **Time-based**:
# MAGIC * Predictable schedule (daily, hourly)
# MAGIC * Business hours processing
# MAGIC * Regulatory reporting deadlines
# MAGIC
# MAGIC **File arrival**:
# MAGIC * Unpredictable file landing times
# MAGIC * Real-time processing requirements
# MAGIC * Event-driven architectures
# MAGIC
# MAGIC **Table update**:
# MAGIC * Multi-stage pipelines
# MAGIC * Downstream transformations
# MAGIC * Complex dependencies
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "How many fields in Quartz cron expression?"
# MAGIC * Answer: 7 fields (seconds, minutes, hours, day-of-month, month, day-of-week, year)
# MAGIC
# MAGIC **Question**: "Which trigger for files landing at unpredictable times?"
# MAGIC * Answer: File arrival trigger
# MAGIC
# MAGIC **Common trap**: Using 5-field cron (Unix) instead of 7-field (Quartz)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 3: Automation Bundles (DABs)
# MAGIC %md
# MAGIC ## Concept 3: Automation Bundles (DABs) (HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### What are Automation Bundles?
# MAGIC
# MAGIC Automation Bundles (formerly Databricks Asset Bundles) package workspace assets (jobs, pipelines, notebooks) for deployment across environments.
# MAGIC
# MAGIC ### Bundle Structure
# MAGIC
# MAGIC **databricks.yml** (root configuration):
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: data_pipeline
# MAGIC
# MAGIC workspace:
# MAGIC   host: https://your-workspace.cloud.databricks.com
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     daily_etl:
# MAGIC       name: "Daily ETL - ${bundle.target}"
# MAGIC       tasks:
# MAGIC         - task_key: ingest
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/ingest.py
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     workspace:
# MAGIC       root_path: /Users/${workspace.current_user.userName}/.bundle/dev
# MAGIC   
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     workspace:
# MAGIC       root_path: /Shared/.bundle/prod
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         daily_etl:
# MAGIC           schedule:
# MAGIC             quartz_cron_expression: "0 0 2 * * ? *"
# MAGIC ```
# MAGIC
# MAGIC ### Key Bundle Concepts
# MAGIC
# MAGIC #### 1. Environment-Specific Variables
# MAGIC
# MAGIC **Using ${bundle.target}**:
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     etl:
# MAGIC       name: "ETL Job - ${bundle.target}"  // "ETL Job - dev" or "ETL Job - prod"
# MAGIC       tasks:
# MAGIC         - task_key: process
# MAGIC           notebook_task:
# MAGIC             base_parameters:
# MAGIC               environment: "${bundle.target}"
# MAGIC ```
# MAGIC
# MAGIC #### 2. Target-Specific Overrides
# MAGIC
# MAGIC ```yaml
# MAGIC targets:
# MAGIC   dev:
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         etl:
# MAGIC           max_concurrent_runs: 5  // Dev allows concurrent testing
# MAGIC   
# MAGIC   prod:
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         etl:
# MAGIC           max_concurrent_runs: 1  // Prod prevents overlaps
# MAGIC           email_notifications:
# MAGIC             on_failure: ["oncall@example.com"]
# MAGIC ```
# MAGIC
# MAGIC #### 3. Variables and Parameterization
# MAGIC
# MAGIC ```yaml
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     description: "Unity Catalog name"
# MAGIC     default: "dev_catalog"
# MAGIC
# MAGIC targets:
# MAGIC   prod:
# MAGIC     variables:
# MAGIC       catalog: "prod_catalog"
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     etl:
# MAGIC       tasks:
# MAGIC         - notebook_task:
# MAGIC             base_parameters:
# MAGIC               catalog: "${var.catalog}"  // "dev_catalog" or "prod_catalog"
# MAGIC ```
# MAGIC
# MAGIC ### Bundle CLI Commands
# MAGIC
# MAGIC **Validate Bundle**:
# MAGIC ```bash
# MAGIC databricks bundle validate -t dev
# MAGIC ```
# MAGIC
# MAGIC **Deploy Bundle**:
# MAGIC ```bash
# MAGIC databricks bundle deploy -t dev
# MAGIC databricks bundle deploy -t prod
# MAGIC ```
# MAGIC
# MAGIC **Run Deployed Job**:
# MAGIC ```bash
# MAGIC databricks bundle run -t dev daily_etl
# MAGIC ```
# MAGIC
# MAGIC **Destroy Bundle**:
# MAGIC ```bash
# MAGIC databricks bundle destroy -t dev
# MAGIC ```
# MAGIC
# MAGIC ### Bundle Modes
# MAGIC
# MAGIC | Mode | Purpose | Behavior |
# MAGIC |------|---------|----------|
# MAGIC | **development** | Testing, iteration | Isolated user workspace, frequent changes |
# MAGIC | **production** | Stable deployment | Shared location, controlled changes |
# MAGIC
# MAGIC ### Directory Structure
# MAGIC
# MAGIC ```
# MAGIC my_project/
# MAGIC ├── databricks.yml          # Bundle configuration
# MAGIC ├── notebooks/
# MAGIC │   ├── ingest.py
# MAGIC │   └── transform.py
# MAGIC ├── pipelines/
# MAGIC │   └── bronze_silver.py
# MAGIC └── tests/
# MAGIC     └── test_transform.py
# MAGIC ```
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "What file defines bundle configuration?"
# MAGIC * Answer: `databricks.yml`
# MAGIC
# MAGIC **Question**: "How to deploy same code to dev and prod with different configs?"
# MAGIC * Answer: Use `targets` with environment-specific overrides
# MAGIC
# MAGIC **Question**: "Which CLI command deploys a bundle?"
# MAGIC * Answer: `databricks bundle deploy -t <target>`
# MAGIC
# MAGIC **Common trap**: Thinking bundles are only for jobs (wrong - they package jobs, pipelines, notebooks, and more)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Concept 4: Git Folders (Databricks Git Integration)
# MAGIC %md
# MAGIC ## Concept 4: Git Folders (Databricks Git Integration) (MEDIUM-HIGH EXAM FREQUENCY)
# MAGIC
# MAGIC ### What are Git Folders?
# MAGIC
# MAGIC Git Folders (formerly Databricks Repos) integrate Git version control directly into the Databricks workspace UI.
# MAGIC
# MAGIC ### Git Folder Workflow
# MAGIC
# MAGIC #### 1. Create/Clone Repository
# MAGIC
# MAGIC **UI Steps**:
# MAGIC 1. Workspace → Git Folders
# MAGIC 2. Add Folder → Clone from Git repository
# MAGIC 3. Enter Git URL (GitHub, GitLab, Bitbucket, Azure DevOps)
# MAGIC 4. Select branch
# MAGIC
# MAGIC **What gets synced**:
# MAGIC * Notebooks (.py, .sql, .scala, .R)
# MAGIC * Python modules (.py)
# MAGIC * Configuration files
# MAGIC * Requirements.txt, setup.py
# MAGIC
# MAGIC #### 2. Branch Management
# MAGIC
# MAGIC **Create Branch**:
# MAGIC 1. Click branch dropdown (top right)
# MAGIC 2. "Create branch"
# MAGIC 3. Enter branch name
# MAGIC 4. Select source branch
# MAGIC
# MAGIC **Switch Branch**:
# MAGIC 1. Click branch dropdown
# MAGIC 2. Select existing branch
# MAGIC 3. Workspace updates to show branch content
# MAGIC
# MAGIC **Branch Protection**:
# MAGIC * Main/master branches often protected
# MAGIC * Changes require pull requests
# MAGIC * No direct commits to protected branches
# MAGIC
# MAGIC #### 3. Commit and Push Changes
# MAGIC
# MAGIC **UI Workflow**:
# MAGIC 1. Make changes to notebooks/files
# MAGIC 2. Click "Git" in left sidebar
# MAGIC 3. Review changed files
# MAGIC 4. Enter commit message
# MAGIC 5. Click "Commit & Push"
# MAGIC
# MAGIC **Best Practices**:
# MAGIC * Commit frequently with descriptive messages
# MAGIC * One logical change per commit
# MAGIC * Format: "[Component] Description"
# MAGIC   * Example: "[ETL] Add validation for null customer IDs"
# MAGIC
# MAGIC #### 4. Pull Request Workflow
# MAGIC
# MAGIC **Standard Process**:
# MAGIC 1. Create feature branch
# MAGIC 2. Make changes and commit
# MAGIC 3. Push to remote
# MAGIC 4. Create PR in Git provider (GitHub/GitLab)
# MAGIC 5. Code review
# MAGIC 6. Merge to main
# MAGIC 7. Pull latest main in Databricks
# MAGIC
# MAGIC #### 5. Pulling Latest Changes
# MAGIC
# MAGIC **UI Steps**:
# MAGIC 1. Click "Git" → "Pull"
# MAGIC 2. Or: Switch to branch (auto-pulls latest)
# MAGIC
# MAGIC **Merge Conflicts**:
# MAGIC * Databricks shows conflict markers
# MAGIC * Resolve in notebook
# MAGIC * Commit resolution
# MAGIC
# MAGIC ### Git Folder Structure
# MAGIC
# MAGIC **Recommended Layout**:
# MAGIC ```
# MAGIC my_repo/
# MAGIC ├── .gitignore
# MAGIC ├── notebooks/
# MAGIC │   ├── bronze_ingestion.py
# MAGIC │   ├── silver_transform.py
# MAGIC │   └── gold_aggregate.py
# MAGIC ├── src/
# MAGIC │   ├── utils.py
# MAGIC │   └── validators.py
# MAGIC ├── tests/
# MAGIC │   └── test_transform.py
# MAGIC ├── databricks.yml  # For Automation Bundles
# MAGIC └── requirements.txt
# MAGIC ```
# MAGIC
# MAGIC ### .gitignore Best Practices
# MAGIC
# MAGIC ```
# MAGIC # Databricks-specific
# MAGIC .databricks/
# MAGIC *.egg-info/
# MAGIC __pycache__/
# MAGIC *.pyc
# MAGIC
# MAGIC # Data files (don't commit data)
# MAGIC *.csv
# MAGIC *.parquet
# MAGIC *.json
# MAGIC data/
# MAGIC
# MAGIC # Secrets (NEVER commit)
# MAGIC .env
# MAGIC config/secrets.yaml
# MAGIC ```
# MAGIC
# MAGIC ### Git Integration with Jobs
# MAGIC
# MAGIC **Option 1: Git Source in Job**:
# MAGIC ```json
# MAGIC {
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "process",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "notebooks/process"
# MAGIC       },
# MAGIC       "git_source": {
# MAGIC         "git_url": "https://github.com/org/repo",
# MAGIC         "git_branch": "main",
# MAGIC         "git_provider": "gitHub"
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Option 2: Deploy via Bundle**:
# MAGIC * Cleaner for production
# MAGIC * Versioned deployments
# MAGIC * Environment-specific configs
# MAGIC
# MAGIC ### Environment Strategy
# MAGIC
# MAGIC **Branch Pattern**:
# MAGIC ```
# MAGIC main (production)
# MAGIC   │
# MAGIC   ├── staging (pre-production testing)
# MAGIC   │
# MAGIC   └── feature/* (development branches)
# MAGIC        │
# MAGIC        ├── feature/add-validation
# MAGIC        └── feature/new-source
# MAGIC ```
# MAGIC
# MAGIC **Deployment Flow**:
# MAGIC 1. Develop on feature branch
# MAGIC 2. PR to staging → deploy to staging environment
# MAGIC 3. Test in staging
# MAGIC 4. PR to main → deploy to production
# MAGIC
# MAGIC ### Exam Tips
# MAGIC
# MAGIC **Question**: "How do you commit changes in Databricks Git Folders?"
# MAGIC * Answer: Git sidebar → Review changes → Enter commit message → Commit & Push
# MAGIC
# MAGIC **Question**: "Can you commit directly to a protected main branch?"
# MAGIC * Answer: No - requires pull request
# MAGIC
# MAGIC **Question**: "What should NOT be committed to Git?"
# MAGIC * Answer: Data files, secrets, credentials, .env files
# MAGIC
# MAGIC **Question**: "How to run job from specific Git branch?"
# MAGIC * Answer: Use `git_source` with `git_branch` parameter in task configuration
# MAGIC
# MAGIC **Common trap**: Thinking Git Folders only sync notebooks (wrong - they sync Python modules, configs, and other files too)
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Key Takeaways & Exam Focus
# MAGIC %md
# MAGIC ## Key Takeaways & Exam Focus
# MAGIC
# MAGIC ### Most Testable Concepts
# MAGIC
# MAGIC 1. **Job Configuration** (Very High Frequency)
# MAGIC    * Task types: notebook, SQL, pipeline, dashboard
# MAGIC    * Task dependencies: `depends_on` array
# MAGIC    * Job clusters vs all-purpose clusters (cost, lifecycle)
# MAGIC    * Retries, timeouts, email notifications
# MAGIC
# MAGIC 2. **Scheduling and Triggers** (High Frequency)
# MAGIC    * Quartz cron: 7 fields (sec min hour day month dow year)
# MAGIC    * File arrival triggers
# MAGIC    * Table update triggers
# MAGIC    * Time-based vs data-driven decision
# MAGIC
# MAGIC 3. **Automation Bundles** (High Frequency)
# MAGIC    * `databricks.yml` structure
# MAGIC    * `targets` for environment-specific config
# MAGIC    * Variables and overrides
# MAGIC    * CLI commands: `validate`, `deploy`, `run`, `destroy`
# MAGIC
# MAGIC 4. **Git Folders** (Medium-High Frequency)
# MAGIC    * Branch creation and switching
# MAGIC    * Commit and push workflow
# MAGIC    * Pull request process
# MAGIC    * What NOT to commit (.gitignore patterns)
# MAGIC    * `git_source` in jobs
# MAGIC
# MAGIC ### Decision Matrices
# MAGIC
# MAGIC **Cluster Selection**:
# MAGIC * Scheduled production job → Job cluster
# MAGIC * Interactive development → All-purpose cluster
# MAGIC * Ad-hoc notebook testing → All-purpose cluster
# MAGIC * Automated CI/CD pipeline → Job cluster
# MAGIC
# MAGIC **Trigger Selection**:
# MAGIC * Nightly batch at 2 AM → Time-based (cron)
# MAGIC * Files land unpredictably → File arrival trigger
# MAGIC * Process after upstream table update → Table update trigger
# MAGIC * Hourly incremental load → Time-based (cron)
# MAGIC
# MAGIC **Deployment Approach**:
# MAGIC * Single environment → Direct workspace development
# MAGIC * Dev + Prod environments → Automation Bundles
# MAGIC * Complex multi-stage pipeline → Automation Bundles + Git
# MAGIC * Team collaboration → Git Folders + Bundles
# MAGIC
# MAGIC ### Common Exam Traps
# MAGIC
# MAGIC **Trap 1**: "All-purpose clusters are better for production jobs"
# MAGIC * False! Job clusters auto-terminate (cheaper, recommended)
# MAGIC
# MAGIC **Trap 2**: "Quartz cron has 5 fields like Unix cron"
# MAGIC * False! Quartz has 7 fields (adds seconds and year)
# MAGIC
# MAGIC **Trap 3**: "Bundles only deploy jobs"
# MAGIC * False! Bundles deploy jobs, pipelines, notebooks, and more
# MAGIC
# MAGIC **Trap 4**: "You can commit directly to main branch"
# MAGIC * False (usually)! Protected branches require pull requests
# MAGIC
# MAGIC **Trap 5**: "Data files should be committed to Git"
# MAGIC * False! Use .gitignore for data files and secrets
# MAGIC
# MAGIC ### Syntax Quick Reference
# MAGIC
# MAGIC **Task Dependency**:
# MAGIC ```json
# MAGIC "depends_on": [{"task_key": "upstream_task"}]
# MAGIC ```
# MAGIC
# MAGIC **Cron (Daily 2 AM)**:
# MAGIC ```
# MAGIC 0 0 2 * * ? *
# MAGIC ```
# MAGIC
# MAGIC **File Arrival Trigger**:
# MAGIC ```json
# MAGIC "file_arrival": {"url": "s3://bucket/path/"}
# MAGIC ```
# MAGIC
# MAGIC **Bundle Deploy**:
# MAGIC ```bash
# MAGIC databricks bundle deploy -t prod
# MAGIC ```
# MAGIC
# MAGIC **Git Commit**: UI → Git sidebar → Commit & Push
# MAGIC
# MAGIC ### Study Priorities
# MAGIC
# MAGIC **High Priority** (spend most time here):
# MAGIC 1. Job cluster configuration and lifecycle
# MAGIC 2. Task dependencies syntax
# MAGIC 3. Quartz cron format (7 fields)
# MAGIC 4. Bundle structure (databricks.yml, targets)
# MAGIC 5. Git Folders commit/push workflow
# MAGIC
# MAGIC **Medium Priority**:
# MAGIC 1. Email notification configuration
# MAGIC 2. File arrival and table update triggers
# MAGIC 3. Bundle CLI commands
# MAGIC 4. Git branch management
# MAGIC 5. Environment-specific overrides
# MAGIC
# MAGIC **Lower Priority** (know concepts, less syntax detail):
# MAGIC 1. Concurrent run limits
# MAGIC 2. Timeout configurations
# MAGIC 3. Advanced bundle variables
# MAGIC 4. Git merge conflict resolution
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **End of Topic 5 Overview**

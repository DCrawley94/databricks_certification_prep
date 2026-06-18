# Databricks notebook source
# DBTITLE 1,Solutions Header
# MAGIC %md
# MAGIC # Topic 5: Production Pipelines & CI/CD - Solutions
# MAGIC
# MAGIC ## Organization
# MAGIC
# MAGIC * **Exercises 1-10**: Lakeflow Jobs
# MAGIC * **Exercises 11-15**: Automation Bundles
# MAGIC * **Exercises 16-20**: Git Folders
# MAGIC * **MCQs 1-5**: Answers with explanations
# MAGIC * **Challenges 1-2**: Complete solutions
# MAGIC * **Applied**: Extended explanation

# COMMAND ----------

# DBTITLE 1,Solutions 1-10: Lakeflow Jobs
# MAGIC %md
# MAGIC ## Solutions 1-10: Lakeflow Jobs Fundamentals
# MAGIC
# MAGIC ### Exercise 1: Task Types
# MAGIC
# MAGIC **Answer**: B) SQL task
# MAGIC
# MAGIC **Explanation**: While you CAN run SQL in a notebook task, the dedicated SQL task type is more appropriate and efficient for pure SQL queries. It runs on SQL warehouses and integrates better with query monitoring.
# MAGIC
# MAGIC **Exam trap**: Don't confuse with notebook task (option A) - always use the most specific task type.
# MAGIC
# MAGIC **Memory aid**: "Specific task for specific job" - SQL query → SQL task, notebook → notebook task.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 2: Task Dependencies
# MAGIC
# MAGIC **Answer**:
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "task_c",
# MAGIC   "depends_on": [
# MAGIC     {"task_key": "task_a"},
# MAGIC     {"task_key": "task_b"}
# MAGIC   ],
# MAGIC   "notebook_task": {
# MAGIC     "notebook_path": "/path/to/notebook"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Explanation**: The `depends_on` array lists all upstream dependencies. Task C won't start until BOTH A and B complete successfully.
# MAGIC
# MAGIC **Exam trap**: Don't write `"depends_on": "task_a"` (string) - it MUST be an array of objects with `task_key` fields.
# MAGIC
# MAGIC **Memory aid**: "Array of dependencies, each with task_key" - `depends_on: [{task_key: "..."}]`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 3: Job vs All-Purpose Clusters
# MAGIC
# MAGIC **Answer**: Use a **job cluster**.
# MAGIC
# MAGIC **Reasons**:
# MAGIC 1. **Auto-termination**: Cluster shuts down after job completes (no idle costs)
# MAGIC 2. **Cost-effective**: Only pay for 2 hours of compute
# MAGIC 3. **Isolated**: No resource contention with other users
# MAGIC 4. **Recommended pattern**: Databricks best practice for scheduled production jobs
# MAGIC
# MAGIC **All-purpose cluster drawbacks**:
# MAGIC * Stays running (24 hours of billing vs 2 hours)
# MAGIC * Requires manual shutdown
# MAGIC * More expensive for scheduled workloads
# MAGIC
# MAGIC **Exam trap**: Don't think "all-purpose is always better" - job clusters are BETTER for scheduled jobs.
# MAGIC
# MAGIC **Memory aid**: "Job cluster for jobs, all-purpose for humans" - scheduled automation vs interactive work.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 4: Quartz Cron Expression
# MAGIC
# MAGIC **Answers**:
# MAGIC
# MAGIC a) **Every Monday at 9:00 AM**:
# MAGIC ```
# MAGIC 0 0 9 ? * MON *
# MAGIC ```
# MAGIC
# MAGIC b) **Every 6 hours starting at midnight**:
# MAGIC ```
# MAGIC 0 0 */6 * * ? *
# MAGIC ```
# MAGIC
# MAGIC c) **15th day of every month at 3:30 PM**:
# MAGIC ```
# MAGIC 0 30 15 15 * ? *
# MAGIC ```
# MAGIC
# MAGIC **Format breakdown** (7 fields):
# MAGIC ```
# MAGIC <sec> <min> <hour> <day-of-month> <month> <day-of-week> <year>
# MAGIC   0     0      9          ?           *        MON         *
# MAGIC ```
# MAGIC
# MAGIC **Key rules**:
# MAGIC * Day-of-month and day-of-week are mutually exclusive: use `?` for the unused one
# MAGIC * `*` means "every"
# MAGIC * `*/6` means "every 6"
# MAGIC * `MON` is Monday (can also use `2` for Monday)
# MAGIC
# MAGIC **Exam trap**: Don't use 5-field Unix cron format - Databricks uses 7-field Quartz.
# MAGIC
# MAGIC **Memory aid**: "7 fields: Second Minute Hour Day Month DayOfWeek Year" (SMHDMDY)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 5: Retry Configuration
# MAGIC
# MAGIC **Answer**:
# MAGIC ```json
# MAGIC {
# MAGIC   "max_retries": 3,
# MAGIC   "retry_on_timeout": true,
# MAGIC   "timeout_seconds": 3600
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * `max_retries: 3` - Retry up to 3 times (4 total attempts: original + 3 retries)
# MAGIC * `retry_on_timeout: true` - Retry if job hits timeout limit
# MAGIC * `timeout_seconds: 3600` - 1 hour = 3600 seconds
# MAGIC
# MAGIC **Exam trap**: `max_retries` is the number of RETRIES, not total attempts. 3 retries = 4 total attempts.
# MAGIC
# MAGIC **Memory aid**: "Retry count + original = total attempts"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 6: Email Notifications
# MAGIC
# MAGIC **Answer**:
# MAGIC ```json
# MAGIC {
# MAGIC   "email_notifications": {
# MAGIC     "on_failure": ["data-team@example.com"],
# MAGIC     "on_success": ["manager@example.com"],
# MAGIC     "no_alert_for_skipped_runs": true
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * `on_failure` - Sent when job fails (after all retries exhausted)
# MAGIC * `on_success` - Sent when job completes successfully
# MAGIC * `on_start` - Also available (not requested here)
# MAGIC * `no_alert_for_skipped_runs` - Don't alert if run skipped due to previous run still active
# MAGIC
# MAGIC **Exam trap**: Email lists are arrays, even for single recipient: `["email@example.com"]`
# MAGIC
# MAGIC **Memory aid**: "Three notification triggers: start, success, failure"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 7: File Arrival Trigger
# MAGIC
# MAGIC **Answer**:
# MAGIC ```json
# MAGIC {
# MAGIC   "trigger": {
# MAGIC     "file_arrival": {
# MAGIC       "url": "s3://my-bucket/incoming/",
# MAGIC       "min_time_between_triggers_seconds": 300
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * `url` - Cloud storage path to monitor (S3, ADLS, GCS)
# MAGIC * `min_time_between_triggers_seconds` - Cooldown period (300 sec = 5 min)
# MAGIC * Job triggers automatically when new files detected
# MAGIC
# MAGIC **Use cases**:
# MAGIC * Unpredictable file landing times
# MAGIC * Event-driven processing
# MAGIC * Near real-time ingestion
# MAGIC
# MAGIC **Exam trap**: Don't confuse with table update trigger - file arrival is for cloud storage, table update is for Unity Catalog tables.
# MAGIC
# MAGIC **Memory aid**: "File in cloud storage → file_arrival trigger"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 8: Table Update Trigger
# MAGIC
# MAGIC **Answer**:
# MAGIC ```json
# MAGIC {
# MAGIC   "trigger": {
# MAGIC     "table_update": {
# MAGIC       "table_names": ["catalog.schema.source_table"],
# MAGIC       "wait_after_last_change_seconds": 120
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * `table_names` - Array of Unity Catalog tables to monitor (fully qualified names)
# MAGIC * `wait_after_last_change_seconds` - Wait period after last change (120 sec = 2 min)
# MAGIC * Prevents triggering on every micro-batch in streaming updates
# MAGIC
# MAGIC **Use cases**:
# MAGIC * Downstream transformations
# MAGIC * Multi-stage pipeline orchestration
# MAGIC * Data-driven scheduling
# MAGIC
# MAGIC **Exam trap**: Must use fully qualified table names: `catalog.schema.table`
# MAGIC
# MAGIC **Memory aid**: "Table update → Unity Catalog table monitoring"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 9: Multi-Task Job Design
# MAGIC
# MAGIC **DAG Diagram**:
# MAGIC ```
# MAGIC       Task 1 (Ingest)
# MAGIC          /    \
# MAGIC         /      \
# MAGIC    Task 2    Task 3  (parallel transforms)
# MAGIC         \      /
# MAGIC          \    /
# MAGIC        Task 4 (Aggregate)
# MAGIC ```
# MAGIC
# MAGIC **Answer**:
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "Multi-Stage Pipeline",
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "ingest",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/pipelines/ingest"
# MAGIC       }
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "transform_a",
# MAGIC       "depends_on": [{"task_key": "ingest"}],
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/pipelines/transform_a"
# MAGIC       }
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "transform_b",
# MAGIC       "depends_on": [{"task_key": "ingest"}],
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/pipelines/transform_b"
# MAGIC       }
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "aggregate",
# MAGIC       "depends_on": [
# MAGIC         {"task_key": "transform_a"},
# MAGIC         {"task_key": "transform_b"}
# MAGIC       ],
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/pipelines/aggregate"
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * Tasks 2 & 3 both depend only on Task 1 → run in parallel
# MAGIC * Task 4 depends on both Tasks 2 & 3 → waits for both
# MAGIC * Jobs UI shows this as DAG graph automatically
# MAGIC
# MAGIC **Exam trap**: Parallel execution requires explicit dependency specification - don't assume tasks run in order.
# MAGIC
# MAGIC **Memory aid**: "Shared dependency → parallel; Multiple dependencies → join point"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 10: Job Parameters
# MAGIC
# MAGIC **Answer**:
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "process",
# MAGIC   "notebook_task": {
# MAGIC     "notebook_path": "/pipelines/process",
# MAGIC     "base_parameters": {
# MAGIC       "start_date": "2026-06-01",
# MAGIC       "end_date": "2026-06-10"
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **In the notebook**, access parameters:
# MAGIC ```python
# MAGIC # Python
# MAGIC dbutils.widgets.get("start_date")
# MAGIC dbutils.widgets.get("end_date")
# MAGIC ```
# MAGIC
# MAGIC Or:
# MAGIC ```sql
# MAGIC -- SQL
# MAGIC SELECT * FROM events
# MAGIC WHERE event_date BETWEEN '${start_date}' AND '${end_date}'
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * `base_parameters` - Dictionary of param name → value
# MAGIC * Parameters passed as widgets to notebook
# MAGIC * Can use variables or hardcoded values
# MAGIC
# MAGIC **Exam trap**: Parameters are passed via `base_parameters`, not `parameters` or `args`.
# MAGIC
# MAGIC **Memory aid**: "base_parameters for notebooks, arguments for SQL queries"

# COMMAND ----------

# DBTITLE 1,Solutions 11-15: Automation Bundles
# MAGIC %md
# MAGIC ## Solutions 11-15: Automation Bundles (DABs)
# MAGIC
# MAGIC ### Exercise 11: Bundle Structure
# MAGIC
# MAGIC **Answer**: The primary configuration file is `databricks.yml`, located in the root directory of the bundle.
# MAGIC
# MAGIC **Complete structure**:
# MAGIC ```
# MAGIC my_bundle/
# MAGIC ├── databricks.yml     # Primary config (REQUIRED)
# MAGIC ├── notebooks/
# MAGIC │   └── *.py
# MAGIC ├── pipelines/
# MAGIC │   └── *.py
# MAGIC └── tests/
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * `databricks.yml` - Root configuration defining bundle resources
# MAGIC * Resources: jobs, pipelines, experiments, models
# MAGIC * Targets: environment-specific configurations
# MAGIC * Variables: parameterization across environments
# MAGIC
# MAGIC **Exam trap**: Don't confuse with `bundle.yml` or `config.yml` - it's `databricks.yml`.
# MAGIC
# MAGIC **Memory aid**: "databricks.yml is the bundle's main.py"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 12: Environment-Specific Configuration
# MAGIC
# MAGIC **Answer**:
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: my_pipeline
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     etl_job:
# MAGIC       name: "ETL Pipeline - ${bundle.target}"
# MAGIC       tasks:
# MAGIC         - task_key: process
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/process.py
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * `${bundle.target}` - Built-in variable containing current target name
# MAGIC * When deployed to `dev`: "ETL Pipeline - dev"
# MAGIC * When deployed to `prod`: "ETL Pipeline - prod"
# MAGIC * Enables resource isolation across environments
# MAGIC
# MAGIC **Other useful built-in variables**:
# MAGIC * `${workspace.current_user.userName}` - Current user
# MAGIC * `${workspace.root_path}` - Bundle deployment path
# MAGIC * `${bundle.name}` - Bundle name
# MAGIC
# MAGIC **Exam trap**: Use `${bundle.target}`, not `${environment}` or `${target}`.
# MAGIC
# MAGIC **Memory aid**: "${bundle.target} for environment name"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 13: Target Overrides
# MAGIC
# MAGIC **Answer**:
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: my_pipeline
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     etl_job:
# MAGIC       name: "ETL Job"
# MAGIC       tasks:
# MAGIC         - task_key: process
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/process.py
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         etl_job:
# MAGIC           max_concurrent_runs: 5
# MAGIC   
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         etl_job:
# MAGIC           max_concurrent_runs: 1
# MAGIC           email_notifications:
# MAGIC             on_failure:
# MAGIC               - oncall@example.com
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * Base configuration in `resources` (shared across all targets)
# MAGIC * Target-specific overrides in `targets.<target>.resources`
# MAGIC * Dev allows parallel testing with 5 concurrent runs
# MAGIC * Prod restricts to 1 run (prevents race conditions) + alerts
# MAGIC
# MAGIC **Merge behavior**: Target overrides are MERGED with base config, not replaced.
# MAGIC
# MAGIC **Exam trap**: Don't duplicate entire job config in targets - only specify overrides.
# MAGIC
# MAGIC **Memory aid**: "Base config + target overrides = deployed config"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 14: Variables
# MAGIC
# MAGIC **Answer**:
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: my_pipeline
# MAGIC
# MAGIC variables:
# MAGIC   catalog_name:
# MAGIC     description: "Unity Catalog name"
# MAGIC     default: dev_catalog
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     etl_job:
# MAGIC       name: "ETL Job"
# MAGIC       tasks:
# MAGIC         - task_key: process
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/process.py
# MAGIC             base_parameters:
# MAGIC               catalog: "${var.catalog_name}"
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     # Uses default: dev_catalog
# MAGIC   
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     variables:
# MAGIC       catalog_name: prod_catalog
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * Variables defined at bundle level with defaults
# MAGIC * Referenced using `${var.variable_name}`
# MAGIC * Target-specific overrides in `targets.<target>.variables`
# MAGIC * Dev inherits default (`dev_catalog`)
# MAGIC * Prod overrides to `prod_catalog`
# MAGIC
# MAGIC **Variable types**:
# MAGIC * Simple strings (shown here)
# MAGIC * Complex values (booleans, numbers, lookups)
# MAGIC
# MAGIC **Exam trap**: Variable reference syntax is `${var.name}`, not `${variable.name}` or `$name`.
# MAGIC
# MAGIC **Memory aid**: "${var.name} for variables, ${bundle.target} for environment"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 15: Bundle CLI Commands
# MAGIC
# MAGIC **Answers**:
# MAGIC
# MAGIC **a) Validate bundle for dev**:
# MAGIC ```bash
# MAGIC databricks bundle validate -t dev
# MAGIC ```
# MAGIC
# MAGIC **b) Deploy bundle to prod**:
# MAGIC ```bash
# MAGIC databricks bundle deploy -t prod
# MAGIC ```
# MAGIC
# MAGIC **c) Run job named daily_etl in dev**:
# MAGIC ```bash
# MAGIC databricks bundle run -t dev daily_etl
# MAGIC ```
# MAGIC
# MAGIC **d) Destroy bundle deployment in dev**:
# MAGIC ```bash
# MAGIC databricks bundle destroy -t dev
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * `validate` - Checks YAML syntax and resource definitions (no deployment)
# MAGIC * `deploy` - Deploys bundle to specified target
# MAGIC * `run` - Triggers immediate run of deployed job
# MAGIC * `destroy` - Removes all bundle resources from target
# MAGIC
# MAGIC **Common workflow**:
# MAGIC ```bash
# MAGIC # 1. Validate locally
# MAGIC databricks bundle validate -t dev
# MAGIC
# MAGIC # 2. Deploy to dev
# MAGIC databricks bundle deploy -t dev
# MAGIC
# MAGIC # 3. Test
# MAGIC databricks bundle run -t dev my_job
# MAGIC
# MAGIC # 4. Deploy to prod (after testing)
# MAGIC databricks bundle deploy -t prod
# MAGIC ```
# MAGIC
# MAGIC **Exam trap**: Target flag is `-t`, not `--target` or `-e` (though `--target` also works).
# MAGIC
# MAGIC **Memory aid**: "V-D-R-D: Validate, Deploy, Run, Destroy"

# COMMAND ----------

# DBTITLE 1,Solutions 16-20: Git Folders
# MAGIC %md
# MAGIC ## Solutions 16-20: Git Folders (Databricks Git Integration)
# MAGIC
# MAGIC ### Exercise 16: Git Workflow Steps
# MAGIC
# MAGIC **Answer - UI Steps for Commit & Push**:
# MAGIC
# MAGIC 1. **Make changes** - Edit notebooks/files in workspace
# MAGIC 2. **Open Git panel** - Click "Git" icon in left sidebar
# MAGIC 3. **Review changes** - See list of modified files
# MAGIC 4. **Enter commit message** - Descriptive message in commit box
# MAGIC 5. **Commit & Push** - Click "Commit & Push" button
# MAGIC 6. **Confirm** - Changes pushed to remote repository
# MAGIC
# MAGIC **Alternative workflow** (commit then push separately):
# MAGIC 1. Click "Commit" (saves locally)
# MAGIC 2. Click "Push" (sends to remote)
# MAGIC
# MAGIC **Best practices for commit messages**:
# MAGIC ```
# MAGIC [Component] Brief description
# MAGIC
# MAGIC Optional longer explanation
# MAGIC ```
# MAGIC
# MAGIC Examples:
# MAGIC * `[ETL] Add validation for null customer IDs`
# MAGIC * `[Bronze] Fix Auto Loader schema inference`
# MAGIC * `[Gold] Optimize aggregation query performance`
# MAGIC
# MAGIC **Explanation**:
# MAGIC * Git panel shows real-time diff of changes
# MAGIC * Commit creates local snapshot
# MAGIC * Push synchronizes with remote Git provider
# MAGIC * Can't push to protected branches (requires PR)
# MAGIC
# MAGIC **Exam trap**: Databricks Git Folders auto-sync with remote - you don't need separate `git pull` commands.
# MAGIC
# MAGIC **Memory aid**: "Edit → Git panel → Commit message → Push"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 17: Branch Management
# MAGIC
# MAGIC **Answer - UI Steps**:
# MAGIC
# MAGIC **Create feature branch**:
# MAGIC 1. Click **branch dropdown** (top-right, shows current branch "main")
# MAGIC 2. Click **"Create branch"**
# MAGIC 3. Enter branch name: `feature/add-validation`
# MAGIC 4. Select source branch: `main`
# MAGIC 5. Click **"Create"**
# MAGIC 6. Workspace switches to new branch automatically
# MAGIC
# MAGIC **Switch to existing branch**:
# MAGIC 1. Click **branch dropdown**
# MAGIC 2. Select branch name from list
# MAGIC 3. Workspace updates to show branch content
# MAGIC 4. Latest commits automatically pulled
# MAGIC
# MAGIC **Explanation**:
# MAGIC * Creating branch makes local + remote copy
# MAGIC * Source branch is the starting point (usually `main`)
# MAGIC * Switching branches updates all files in workspace
# MAGIC * Changes are isolated per branch
# MAGIC
# MAGIC **Branch naming conventions**:
# MAGIC * `feature/description` - New features
# MAGIC * `bugfix/description` - Bug fixes
# MAGIC * `hotfix/description` - Urgent production fixes
# MAGIC * `experiment/description` - Experimental changes
# MAGIC
# MAGIC **Exam trap**: Don't think you need command-line Git - Databricks UI handles everything.
# MAGIC
# MAGIC **Memory aid**: "Branch dropdown for all branch operations"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 18: .gitignore Best Practices
# MAGIC
# MAGIC **Answer - Files/Patterns to NEVER Commit**:
# MAGIC
# MAGIC ```gitignore
# MAGIC # Databricks-specific
# MAGIC .databricks/
# MAGIC *.egg-info/
# MAGIC __pycache__/
# MAGIC *.pyc
# MAGIC *.pyo
# MAGIC
# MAGIC # Data files (NEVER commit data)
# MAGIC *.csv
# MAGIC *.json
# MAGIC *.parquet
# MAGIC *.avro
# MAGIC *.orc
# MAGIC data/
# MAGIC raw_data/
# MAGIC output/
# MAGIC
# MAGIC # Secrets and credentials (CRITICAL)
# MAGIC .env
# MAGIC *.key
# MAGIC *.pem
# MAGIC *.p12
# MAGIC config/secrets.yaml
# MAGIC config/credentials.json
# MAGIC
# MAGIC # IDE and OS files
# MAGIC .vscode/
# MAGIC .idea/
# MAGIC .DS_Store
# MAGIC Thumbs.db
# MAGIC
# MAGIC # Build artifacts
# MAGIC dist/
# MAGIC build/
# MAGIC *.whl
# MAGIC *.egg
# MAGIC
# MAGIC # Temporary files
# MAGIC *.tmp
# MAGIC *.log
# MAGIC *.swp
# MAGIC ```
# MAGIC
# MAGIC **Critical rules**:
# MAGIC 1. **NEVER commit secrets** - API keys, passwords, tokens, certificates
# MAGIC 2. **NEVER commit data files** - Use storage, not Git
# MAGIC 3. **NEVER commit credentials** - Use secret scopes instead
# MAGIC 4. **Don't commit binaries** - Large files bloat repository
# MAGIC 5. **Don't commit environment-specific configs** - Use bundle variables
# MAGIC
# MAGIC **What TO commit**:
# MAGIC * Source code (.py, .sql, .scala, .R notebooks)
# MAGIC * Configuration templates (with placeholders)
# MAGIC * Documentation (.md files)
# MAGIC * Requirements files (requirements.txt, setup.py)
# MAGIC * Bundle configs (databricks.yml)
# MAGIC * Tests
# MAGIC
# MAGIC **Exam trap**: Thinking data files are okay if small - NEVER commit data, regardless of size.
# MAGIC
# MAGIC **Memory aid**: "Code yes, data no; templates yes, secrets NEVER"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 19: Git Source in Jobs
# MAGIC
# MAGIC **Answer**:
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "process",
# MAGIC   "notebook_task": {
# MAGIC     "notebook_path": "notebooks/process.py"
# MAGIC   },
# MAGIC   "git_source": {
# MAGIC     "git_url": "https://github.com/myorg/data-pipelines",
# MAGIC     "git_branch": "main",
# MAGIC     "git_provider": "gitHub"
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Explanation**:
# MAGIC * `git_url` - Full repository URL (HTTPS, not SSH)
# MAGIC * `git_branch` - Branch to pull code from
# MAGIC * `git_provider` - One of: `gitHub`, `gitLab`, `bitbucketCloud`, `bitbucketServer`, `azureDevOpsServices`
# MAGIC * `notebook_path` - Relative path within repository (no leading slash)
# MAGIC
# MAGIC **Job execution flow**:
# MAGIC 1. Job starts
# MAGIC 2. Clones repository from specified URL
# MAGIC 3. Checks out specified branch
# MAGIC 4. Runs notebook from relative path
# MAGIC 5. Cleanup after job completes
# MAGIC
# MAGIC **Advantages**:
# MAGIC * Version-controlled code
# MAGIC * Can deploy different branches to different jobs
# MAGIC * No manual sync needed
# MAGIC * Atomic deployments
# MAGIC
# MAGIC **Disadvantages vs Bundles**:
# MAGIC * Less environment-specific configuration
# MAGIC * No variable substitution
# MAGIC * Manual job setup
# MAGIC
# MAGIC **Exam trap**: `git_provider` is case-sensitive - use `gitHub` not `github` or `GitHub`.
# MAGIC
# MAGIC **Memory aid**: "git_source = (url, branch, provider)"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exercise 20: Protected Branch Workflow
# MAGIC
# MAGIC **Answer - Complete Workflow**:
# MAGIC
# MAGIC **1. Create feature branch**:
# MAGIC ```
# MAGIC main (protected)
# MAGIC   │
# MAGIC   └── feature/my-change
# MAGIC ```
# MAGIC
# MAGIC UI: Branch dropdown → Create branch → `feature/my-change` from `main`
# MAGIC
# MAGIC **2. Make changes on feature branch**:
# MAGIC * Edit notebooks/files
# MAGIC * Test changes
# MAGIC * Commit frequently with descriptive messages
# MAGIC
# MAGIC **3. Push changes**:
# MAGIC * Git panel → Review changes
# MAGIC * Commit message: `[Component] Description`
# MAGIC * Click "Commit & Push"
# MAGIC
# MAGIC **4. Create pull request** (in Git provider, e.g., GitHub):
# MAGIC * Navigate to repository on GitHub
# MAGIC * Click "New pull request"
# MAGIC * Base: `main`, Compare: `feature/my-change`
# MAGIC * Add description, reviewers
# MAGIC * Submit PR
# MAGIC
# MAGIC **5. Code review**:
# MAGIC * Team reviews changes
# MAGIC * Discussion, comments, suggestions
# MAGIC * Make fixes by pushing new commits to feature branch
# MAGIC
# MAGIC **6. PR approval**:
# MAGIC * Reviewers approve
# MAGIC * CI/CD checks pass (if configured)
# MAGIC * Merge to `main`
# MAGIC
# MAGIC **7. Pull latest main** (back in Databricks):
# MAGIC * Switch to `main` branch (auto-pulls latest)
# MAGIC * Or: Git panel → "Pull"
# MAGIC
# MAGIC **8. Delete feature branch** (cleanup):
# MAGIC * GitHub: Delete branch after merge
# MAGIC * Databricks: Branch dropdown → can switch away from deleted branch
# MAGIC
# MAGIC **Why protect main**:
# MAGIC * Prevents accidental direct commits
# MAGIC * Enforces code review
# MAGIC * Maintains code quality
# MAGIC * Enables CI/CD gates
# MAGIC * Creates audit trail
# MAGIC
# MAGIC **Exam trap**: You CANNOT bypass protected branch restrictions by force pushing from Databricks.
# MAGIC
# MAGIC **Memory aid**: "Feature branch → Commit → Push → PR → Review → Merge → Pull"

# COMMAND ----------

# DBTITLE 1,MCQ Solutions
# MAGIC %md
# MAGIC ## MCQ Solutions
# MAGIC
# MAGIC ### MCQ 1: Cron Expression
# MAGIC
# MAGIC **Answer**: C) 7 fields
# MAGIC
# MAGIC **Explanation**:
# MAGIC Quartz cron (used by Databricks Jobs) has 7 fields:
# MAGIC ```
# MAGIC <seconds> <minutes> <hours> <day-of-month> <month> <day-of-week> <year>
# MAGIC     0         0        2          *          *         ?          *
# MAGIC ```
# MAGIC
# MAGIC This differs from Unix cron (5 fields) which lacks seconds and year.
# MAGIC
# MAGIC **Why other options are wrong**:
# MAGIC * A) 5 fields - Unix cron format (not Databricks)
# MAGIC * B) 6 fields - No standard cron format has 6 fields
# MAGIC * D) 8 fields - No cron format has 8 fields
# MAGIC
# MAGIC **Exam trap**: Many people know Unix cron (5 fields) and choose A.
# MAGIC
# MAGIC **Memory aid**: "7 fields: Second Minute Hour Day Month DayOfWeek Year"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 2: Cluster Cost
# MAGIC
# MAGIC **Answer**: B) Job cluster that auto-terminates
# MAGIC
# MAGIC **Explanation**:
# MAGIC Job clusters:
# MAGIC * Start when job starts
# MAGIC * Terminate when job completes
# MAGIC * Only pay for actual job runtime
# MAGIC * No idle time costs
# MAGIC * Databricks recommended pattern for scheduled jobs
# MAGIC
# MAGIC For nightly 2-hour job:
# MAGIC * Job cluster: 2 hours of billing
# MAGIC * All-purpose cluster: 24 hours of billing (if left running)
# MAGIC
# MAGIC **Why other options are wrong**:
# MAGIC * A) All-purpose with autoscaling - Still incurs idle costs, requires manual management
# MAGIC * C) High-concurrency cluster - Designed for concurrent users, not scheduled jobs
# MAGIC * D) Single-node cluster - Can work but not cost-optimal vs job cluster
# MAGIC
# MAGIC **Exam trap**: Thinking "all-purpose is always better" - wrong for scheduled production workloads.
# MAGIC
# MAGIC **Memory aid**: "Job cluster for jobs, all-purpose for people"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 3: Bundle Deployment
# MAGIC
# MAGIC **Answer**: C) `databricks bundle deploy -t prod`
# MAGIC
# MAGIC **Explanation**:
# MAGIC Correct syntax: `databricks bundle deploy -t <target>`
# MAGIC
# MAGIC Full workflow:
# MAGIC ```bash
# MAGIC # Validate
# MAGIC databricks bundle validate -t prod
# MAGIC
# MAGIC # Deploy
# MAGIC databricks bundle deploy -t prod
# MAGIC
# MAGIC # Run (optional)
# MAGIC databricks bundle run -t prod job_name
# MAGIC ```
# MAGIC
# MAGIC **Why other options are wrong**:
# MAGIC * A) `databricks deploy -t prod` - Missing `bundle` subcommand
# MAGIC * B) `databricks bundle push prod` - No `push` command (wrong verb)
# MAGIC * D) `databricks assets deploy prod` - No `assets` command (old terminology)
# MAGIC
# MAGIC **Exam trap**: Confusing with `git push` or other CLI patterns.
# MAGIC
# MAGIC **Memory aid**: "databricks bundle <verb> -t <target>"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 4: Git Protected Branch
# MAGIC
# MAGIC **Answer**: B) Create a feature branch, commit there, and create a pull request
# MAGIC
# MAGIC **Explanation**:
# MAGIC Protected branch workflow:
# MAGIC 1. Cannot commit directly to protected branch (main)
# MAGIC 2. Create feature branch from main
# MAGIC 3. Make changes on feature branch
# MAGIC 4. Commit and push to feature branch
# MAGIC 5. Create pull request from feature branch to main
# MAGIC 6. Code review and approval
# MAGIC 7. Merge PR to main
# MAGIC
# MAGIC **Why other options are wrong**:
# MAGIC * A) Request admin access - Wrong approach; protection exists for a reason
# MAGIC * C) Force push - Impossible; protection prevents this
# MAGIC * D) Disable protection in Databricks - Protection is at Git provider level (GitHub/GitLab), not Databricks
# MAGIC
# MAGIC **Exam trap**: Thinking Databricks controls branch protection (it's the Git provider).
# MAGIC
# MAGIC **Memory aid**: "Protected main = feature branch + PR required"
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 5: Task Dependency
# MAGIC
# MAGIC **Answer**: C) `Task B: {"depends_on": [{"task_key": "task_a"}]}`
# MAGIC
# MAGIC **Explanation**:
# MAGIC Correct syntax:
# MAGIC ```json
# MAGIC {
# MAGIC   "task_key": "task_b",
# MAGIC   "depends_on": [
# MAGIC     {"task_key": "task_a"}
# MAGIC   ],
# MAGIC   "notebook_task": {...}
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Key requirements**:
# MAGIC * `depends_on` is an ARRAY
# MAGIC * Each element is an OBJECT with `task_key` field
# MAGIC * Can have multiple dependencies: `[{"task_key": "a"}, {"task_key": "b"}]`
# MAGIC
# MAGIC **Why other options are wrong**:
# MAGIC * A) `"after": "task_a"` - Wrong field name
# MAGIC * B) `"depends_on": "task_a"` - Should be array of objects, not string
# MAGIC * D) `"requires": ["task_a"]` - Wrong field name
# MAGIC
# MAGIC **Exam trap**: Using string instead of array of objects.
# MAGIC
# MAGIC **Memory aid**: "depends_on: array of {task_key: ...} objects"

# COMMAND ----------

# DBTITLE 1,Challenge Solutions
# MAGIC %md
# MAGIC ## Challenge Solutions
# MAGIC
# MAGIC ### Challenge 1: Complete CI/CD Pipeline
# MAGIC
# MAGIC **Answer**:
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: data_pipeline
# MAGIC
# MAGIC variables:
# MAGIC   catalog_name:
# MAGIC     description: "Unity Catalog name"
# MAGIC     default: dev_catalog
# MAGIC
# MAGIC workspace:
# MAGIC   host: https://your-workspace.cloud.databricks.com
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     etl_pipeline:
# MAGIC       name: "ETL Pipeline - ${bundle.target}"
# MAGIC       
# MAGIC       tasks:
# MAGIC         - task_key: bronze_ingest
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/bronze_ingest.py
# MAGIC             base_parameters:
# MAGIC               catalog: "${var.catalog_name}"
# MAGIC           new_cluster:
# MAGIC             spark_version: "13.3.x-scala2.12"
# MAGIC             node_type_id: "i3.xlarge"
# MAGIC             num_workers: 2
# MAGIC         
# MAGIC         - task_key: silver_transform
# MAGIC           depends_on:
# MAGIC             - task_key: bronze_ingest
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/silver_transform.py
# MAGIC             base_parameters:
# MAGIC               catalog: "${var.catalog_name}"
# MAGIC           new_cluster:
# MAGIC             spark_version: "13.3.x-scala2.12"
# MAGIC             node_type_id: "i3.xlarge"
# MAGIC             num_workers: 2
# MAGIC         
# MAGIC         - task_key: gold_aggregate
# MAGIC           depends_on:
# MAGIC             - task_key: silver_transform
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/gold_aggregate.py
# MAGIC             base_parameters:
# MAGIC               catalog: "${var.catalog_name}"
# MAGIC           new_cluster:
# MAGIC             spark_version: "13.3.x-scala2.12"
# MAGIC             node_type_id: "i3.xlarge"
# MAGIC             num_workers: 2
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     workspace:
# MAGIC       root_path: /Users/${workspace.current_user.userName}/.bundle/dev
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         etl_pipeline:
# MAGIC           max_concurrent_runs: 5
# MAGIC   
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     workspace:
# MAGIC       root_path: /Shared/.bundle/prod
# MAGIC     variables:
# MAGIC       catalog_name: prod_catalog
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         etl_pipeline:
# MAGIC           max_concurrent_runs: 1
# MAGIC           schedule:
# MAGIC             quartz_cron_expression: "0 0 3 * * ? *"
# MAGIC             timezone_id: "UTC"
# MAGIC             pause_status: "UNPAUSED"
# MAGIC           email_notifications:
# MAGIC             on_failure:
# MAGIC               - oncall@example.com
# MAGIC ```
# MAGIC
# MAGIC **Git Repository Structure**:
# MAGIC ```
# MAGIC data_pipeline/
# MAGIC ├── databricks.yml
# MAGIC ├── .gitignore
# MAGIC ├── notebooks/
# MAGIC │   ├── bronze_ingest.py
# MAGIC │   ├── silver_transform.py
# MAGIC │   └── gold_aggregate.py
# MAGIC └── tests/
# MAGIC ```
# MAGIC
# MAGIC **Deployment Workflow**:
# MAGIC
# MAGIC ```bash
# MAGIC # Dev environment
# MAGIC git checkout dev
# MAGIC databricks bundle validate -t dev
# MAGIC databricks bundle deploy -t dev
# MAGIC databricks bundle run -t dev etl_pipeline
# MAGIC
# MAGIC # After testing, merge to main
# MAGIC git checkout main
# MAGIC git merge dev
# MAGIC
# MAGIC # Prod deployment
# MAGIC databricks bundle validate -t prod
# MAGIC databricks bundle deploy -t prod
# MAGIC # Job runs automatically at 3 AM via schedule
# MAGIC ```
# MAGIC
# MAGIC **Key Features**:
# MAGIC
# MAGIC 1. **Environment isolation**:
# MAGIC    * Dev: User workspace, concurrent runs allowed
# MAGIC    * Prod: Shared location, single run, scheduled
# MAGIC
# MAGIC 2. **Parameterization**:
# MAGIC    * Catalog name varies by environment
# MAGIC    * Same code, different data
# MAGIC
# MAGIC 3. **Sequential dependencies**:
# MAGIC    * Bronze → Silver → Gold
# MAGIC    * Each depends on previous completion
# MAGIC
# MAGIC 4. **Production features**:
# MAGIC    * Daily 3 AM schedule (prod only)
# MAGIC    * Email alerts on failure
# MAGIC    * Single concurrent run (prevents overlaps)
# MAGIC
# MAGIC 5. **Job clusters**:
# MAGIC    * Auto-terminate after each task
# MAGIC    * Cost-effective for scheduled workloads
# MAGIC
# MAGIC **Explanation for exam**:
# MAGIC * Base configuration defines common structure
# MAGIC * Target overrides customize per environment
# MAGIC * Variables enable parameterization
# MAGIC * Schedule only in prod (dev runs on-demand)
# MAGIC * Email notifications only in prod
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Challenge 2: Troubleshooting Job Failure
# MAGIC
# MAGIC **Problem Analysis**:
# MAGIC
# MAGIC **Current behavior**:
# MAGIC ```
# MAGIC Task 1 (bronze) ✓
# MAGIC    │
# MAGIC    ├─── Task 2 (silver_a) ✗ 30% timeout failures
# MAGIC    │
# MAGIC    └─── Task 3 (silver_b) ✓
# MAGIC         │
# MAGIC         └─── Task 4 (gold) ✗ Blocked by Task 2 failure
# MAGIC ```
# MAGIC
# MAGIC **Issue**: Task 4 depends on Task 2, so it never runs when Task 2 fails.
# MAGIC
# MAGIC **Desired behavior**: Task 4 should run even if Task 2 fails, processing data from Task 3.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Solution 1: Remove Task 2 Dependency from Task 4**
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "tasks": [
# MAGIC     {"task_key": "bronze"},
# MAGIC     {
# MAGIC       "task_key": "silver_a",
# MAGIC       "depends_on": [{"task_key": "bronze"}]
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "silver_b",
# MAGIC       "depends_on": [{"task_key": "bronze"}]
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "gold",
# MAGIC       "depends_on": [
# MAGIC         {"task_key": "silver_b"}  // Only depends on silver_b now
# MAGIC       ]
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Trade-offs**:
# MAGIC * ✓ Task 4 runs when Task 2 fails
# MAGIC * ✓ Simple configuration change
# MAGIC * ✗ Gold layer missing data from silver_a source
# MAGIC * ✗ Inconsistent gold layer (sometimes includes silver_a, sometimes doesn't)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Solution 2: Allow Task 2 Failure with depends_on Outcome**
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "tasks": [
# MAGIC     {"task_key": "bronze"},
# MAGIC     {
# MAGIC       "task_key": "silver_a",
# MAGIC       "depends_on": [{"task_key": "bronze"}],
# MAGIC       "timeout_seconds": 1800,
# MAGIC       "max_retries": 2
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "silver_b",
# MAGIC       "depends_on": [{"task_key": "bronze"}]
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "gold",
# MAGIC       "depends_on": [
# MAGIC         {
# MAGIC           "task_key": "silver_a",
# MAGIC           "outcome": "false"  // Run even if silver_a fails
# MAGIC         },
# MAGIC         {"task_key": "silver_b"}
# MAGIC       ]
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Trade-offs**:
# MAGIC * ✓ Task 4 runs regardless of Task 2 outcome
# MAGIC * ✓ Gold layer consistently processes available data
# MAGIC * ✓ Can detect and handle partial data in Task 4 logic
# MAGIC * ✗ More complex Task 4 logic (must check which sources available)
# MAGIC * ✗ Potential data quality issues if not handled properly
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Solution 3: Separate Gold Tasks (Preferred)**
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "tasks": [
# MAGIC     {"task_key": "bronze"},
# MAGIC     {
# MAGIC       "task_key": "silver_a",
# MAGIC       "depends_on": [{"task_key": "bronze"}],
# MAGIC       "timeout_seconds": 1800,
# MAGIC       "max_retries": 2
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "silver_b",
# MAGIC       "depends_on": [{"task_key": "bronze"}]
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "gold_from_a",
# MAGIC       "depends_on": [{"task_key": "silver_a"}]
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "gold_from_b",
# MAGIC       "depends_on": [{"task_key": "silver_b"}]
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC **Trade-offs**:
# MAGIC * ✓ Clear separation of concerns
# MAGIC * ✓ gold_from_b always runs when silver_b succeeds
# MAGIC * ✓ Failures isolated to specific data sources
# MAGIC * ✓ Easier debugging and monitoring
# MAGIC * ✓ Can merge gold layers downstream if needed
# MAGIC * ✗ More tasks to maintain
# MAGIC * ✗ Requires refactoring gold layer logic
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Recommended Solution: Solution 3**
# MAGIC
# MAGIC **Rationale**:
# MAGIC 1. **Resilience**: Independent gold tasks prevent cascade failures
# MAGIC 2. **Clarity**: Each task has single responsibility
# MAGIC 3. **Monitoring**: Easier to track which sources are problematic
# MAGIC 4. **Flexibility**: Can add retries/timeouts per source
# MAGIC 5. **Best practice**: Aligns with failure isolation principles
# MAGIC
# MAGIC **Implementation Notes**:
# MAGIC
# MAGIC ```python
# MAGIC # gold_from_a.py
# MAGIC spark.sql("""
# MAGIC     INSERT INTO gold.metrics
# MAGIC     SELECT 
# MAGIC         'source_a' as source,
# MAGIC         aggregated_metrics
# MAGIC     FROM silver.table_a
# MAGIC """)
# MAGIC
# MAGIC # gold_from_b.py
# MAGIC spark.sql("""
# MAGIC     INSERT INTO gold.metrics
# MAGIC     SELECT 
# MAGIC         'source_b' as source,
# MAGIC         aggregated_metrics
# MAGIC     FROM silver.table_b
# MAGIC """)
# MAGIC ```
# MAGIC
# MAGIC Downstream consumers can:
# MAGIC * Query combined data: `SELECT * FROM gold.metrics`
# MAGIC * Filter by source if needed: `WHERE source = 'source_a'`
# MAGIC * Detect missing sources: `SELECT DISTINCT source FROM gold.metrics WHERE date = CURRENT_DATE()`
# MAGIC
# MAGIC **Exam relevance**:
# MAGIC * Understanding task dependencies and failure propagation
# MAGIC * Designing resilient pipelines
# MAGIC * Using `depends_on` with outcomes
# MAGIC * Trade-offs between monolithic and modular task design

# COMMAND ----------

# DBTITLE 1,Applied Explanation & Study Guide
# MAGIC %md
# MAGIC ## Applied: Deployment Strategy Decision Framework
# MAGIC
# MAGIC ### Full Explanation
# MAGIC
# MAGIC The applied provided in practice_tasks helps determine the appropriate CI/CD approach based on four key factors:
# MAGIC
# MAGIC **1. Team Size**
# MAGIC * Single developer: Direct workspace development
# MAGIC * 2-3 developers: Git Folders for version control
# MAGIC * 4+ developers: Git Folders + Automation Bundles
# MAGIC * 5+ developers: Full CI/CD with automated testing
# MAGIC
# MAGIC **2. Number of Environments**
# MAGIC * 1 environment: Simple workspace development
# MAGIC * 2 environments (dev + prod): Basic Git or simple Bundle
# MAGIC * 3+ environments (dev/staging/prod): Bundles required for env management
# MAGIC
# MAGIC **3. Change Frequency**
# MAGIC * Monthly: Manual deployment acceptable
# MAGIC * Weekly: Git Folders sufficient
# MAGIC * Daily: Automation Bundles recommended
# MAGIC * Multiple times daily: Full CI/CD pipeline required
# MAGIC
# MAGIC **4. Complexity**
# MAGIC * Simple (few notebooks, basic transforms): Direct development or Git only
# MAGIC * Moderate (multiple pipelines, dependencies): Git + Bundles
# MAGIC * Complex (many pipelines, integrations, testing): Full CI/CD automation
# MAGIC
# MAGIC ### Decision Tree
# MAGIC
# MAGIC ```
# MAGIC Start
# MAGIC   │
# MAGIC   └── Team size = 1 AND Environments = 1?
# MAGIC        ├── YES → Direct workspace development
# MAGIC        └── NO
# MAGIC             │
# MAGIC             └── Team size <= 3 AND Environments <= 2?
# MAGIC                  ├── YES → Change frequency high?
# MAGIC                  │          ├── YES → Git + Bundle
# MAGIC                  │          └── NO → Git only
# MAGIC                  └── NO
# MAGIC                       │
# MAGIC                       └── Complexity = simple?
# MAGIC                            ├── YES → Git + Bundle with env overrides
# MAGIC                            └── NO → Full CI/CD (Git + Bundle + automation)
# MAGIC ```
# MAGIC
# MAGIC ### Real-World Examples
# MAGIC
# MAGIC **Example 1: Solo Data Engineer**
# MAGIC * Team: 1 person
# MAGIC * Environments: 1 (prod only)
# MAGIC * Frequency: Weekly updates
# MAGIC * Complexity: Simple ETL
# MAGIC
# MAGIC **Recommendation**: Direct workspace development
# MAGIC
# MAGIC **Rationale**: Overhead of Git/Bundles not justified. Use notebook revisions for version history.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Example 2: Small Analytics Team**
# MAGIC * Team: 3 people
# MAGIC * Environments: 2 (dev, prod)
# MAGIC * Frequency: Daily updates
# MAGIC * Complexity: Moderate (5 notebooks, 2 pipelines)
# MAGIC
# MAGIC **Recommendation**: Git Folders + Automation Bundle
# MAGIC
# MAGIC **Rationale**: Git for collaboration, Bundle for environment management. Simple CI/CD via bundle deploy.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Example 3: Enterprise Data Platform**
# MAGIC * Team: 10+ people
# MAGIC * Environments: 4 (dev, test, staging, prod)
# MAGIC * Frequency: Multiple times daily
# MAGIC * Complexity: High (50+ notebooks, 10+ pipelines, integrations)
# MAGIC
# MAGIC **Recommendation**: Full CI/CD
# MAGIC
# MAGIC **Setup**:
# MAGIC ```yaml
# MAGIC # .github/workflows/deploy.yml
# MAGIC name: Deploy Bundle
# MAGIC
# MAGIC on:
# MAGIC   push:
# MAGIC     branches: [main, staging, dev]
# MAGIC
# MAGIC jobs:
# MAGIC   deploy:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - uses: actions/checkout@v3
# MAGIC       
# MAGIC       - name: Install Databricks CLI
# MAGIC         run: pip install databricks-cli
# MAGIC       
# MAGIC       - name: Validate Bundle
# MAGIC         run: databricks bundle validate -t ${{ env.TARGET }}
# MAGIC       
# MAGIC       - name: Deploy Bundle
# MAGIC         run: databricks bundle deploy -t ${{ env.TARGET }}
# MAGIC         env:
# MAGIC           DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
# MAGIC           DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
# MAGIC ```
# MAGIC
# MAGIC **Rationale**: Automated testing, staged rollouts, audit trail, team collaboration at scale.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Exam Application
# MAGIC
# MAGIC **Exam Question Pattern**: "A team of 5 data engineers maintains pipelines across dev, staging, and prod environments with daily deployments. Which approach is most appropriate?"
# MAGIC
# MAGIC **Answer**: Automation Bundles with Git Folders
# MAGIC
# MAGIC **Justification**:
# MAGIC * Team size (5) requires version control → Git
# MAGIC * Multiple environments (3) requires parameterization → Bundles
# MAGIC * Daily deployments benefit from automated bundle deploy
# MAGIC * Not complex enough to require full GitHub Actions CI/CD
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Study Guide for Topic 5
# MAGIC
# MAGIC ### High-Priority Concepts (Exam Frequency: Very High)
# MAGIC
# MAGIC **1. Job Clusters vs All-Purpose Clusters**
# MAGIC * Memorize: Job clusters auto-terminate, cheaper for scheduled jobs
# MAGIC * Practice: Identify scenarios requiring each type
# MAGIC
# MAGIC **2. Task Dependencies**
# MAGIC * Memorize: `"depends_on": [{"task_key": "upstream"}]` syntax
# MAGIC * Practice: Draw DAGs from dependency configurations
# MAGIC
# MAGIC **3. Quartz Cron Format**
# MAGIC * Memorize: 7 fields (sec min hour day month dow year)
# MAGIC * Practice: Write cron expressions for common schedules
# MAGIC
# MAGIC **4. Automation Bundle Structure**
# MAGIC * Memorize: `databricks.yml`, `targets`, `variables`, `resources`
# MAGIC * Practice: Write complete bundle configs from scratch
# MAGIC
# MAGIC **5. Bundle CLI Commands**
# MAGIC * Memorize: `validate`, `deploy`, `run`, `destroy` with `-t` flag
# MAGIC * Practice: Sequence commands for complete deployment workflow
# MAGIC
# MAGIC ### Medium-Priority Concepts (Exam Frequency: High)
# MAGIC
# MAGIC **1. File Arrival and Table Update Triggers**
# MAGIC * Know when to use each
# MAGIC * Understand configuration parameters
# MAGIC
# MAGIC **2. Git Folders Workflow**
# MAGIC * Branch creation and switching
# MAGIC * Commit and push process
# MAGIC * Pull request workflow for protected branches
# MAGIC
# MAGIC **3. Bundle Variables and Overrides**
# MAGIC * `${bundle.target}` and `${var.name}` syntax
# MAGIC * Target-specific overrides
# MAGIC
# MAGIC **4. Email Notifications**
# MAGIC * `on_start`, `on_success`, `on_failure` options
# MAGIC * Array syntax for multiple recipients
# MAGIC
# MAGIC ### Lower-Priority Concepts (Exam Frequency: Medium)
# MAGIC
# MAGIC **1. Retry Configuration**
# MAGIC * `max_retries`, `retry_on_timeout`, `timeout_seconds`
# MAGIC * Difference between retry count and total attempts
# MAGIC
# MAGIC **2. Git Source in Jobs**
# MAGIC * `git_url`, `git_branch`, `git_provider` syntax
# MAGIC * When to use vs Automation Bundles
# MAGIC
# MAGIC **3. .gitignore Patterns**
# MAGIC * What never to commit (secrets, data)
# MAGIC * What to commit (code, configs)
# MAGIC
# MAGIC ### Practice Recommendations
# MAGIC
# MAGIC **Week 1**: Jobs Fundamentals
# MAGIC * Create 5 different job configurations
# MAGIC * Practice writing cron expressions
# MAGIC * Draw task dependency DAGs
# MAGIC
# MAGIC **Week 2**: Automation Bundles
# MAGIC * Write complete `databricks.yml` from memory
# MAGIC * Practice target overrides and variables
# MAGIC * Run through validate/deploy/run cycle
# MAGIC
# MAGIC **Week 3**: Git Workflows
# MAGIC * Practice branch creation and switching
# MAGIC * Commit and push workflow
# MAGIC * Create pull requests
# MAGIC
# MAGIC **Week 4**: Integration & Review
# MAGIC * Complete Challenge 1 (full CI/CD pipeline)
# MAGIC * Complete Challenge 2 (troubleshooting)
# MAGIC * Review all MCQs and exercises
# MAGIC
# MAGIC ### Exam Day Checklist
# MAGIC
# MAGIC ☐ Can write Quartz cron expression for any schedule  
# MAGIC ☐ Know job cluster advantages over all-purpose  
# MAGIC ☐ Can write task dependency syntax from memory  
# MAGIC ☐ Know bundle file name (`databricks.yml`)  
# MAGIC ☐ Can write bundle deploy command  
# MAGIC ☐ Know Git commit/push workflow steps  
# MAGIC ☐ Know what NEVER to commit to Git  
# MAGIC ☐ Can identify appropriate trigger type (time/file/table)  
# MAGIC ☐ Know ${bundle.target} and ${var.name} syntax  
# MAGIC ☐ Can draw DAG from task configuration
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **End of Topic 5 Solutions**

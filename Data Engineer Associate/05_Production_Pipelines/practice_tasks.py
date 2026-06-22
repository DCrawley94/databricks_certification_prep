# Databricks notebook source
# DBTITLE 1,Instructions
# MAGIC %md
# MAGIC # Topic 5: Production Pipelines & CI/CD - Practice Tasks
# MAGIC
# MAGIC ## How to Use This Notebook
# MAGIC
# MAGIC Attempt each exercise before checking solutions.py
# MAGIC
# MAGIC ## Exercise Categories
# MAGIC
# MAGIC * **Exercises 1-10**: Lakeflow Jobs fundamentals
# MAGIC * **Exercises 11-15**: Automation Bundles (DABs)
# MAGIC * **Exercises 16-20**: Git Folders workflows
# MAGIC * **MCQs 1-5**: Exam-style questions
# MAGIC * **Challenges 1-2**: Multi-step scenarios
# MAGIC * **Applied**: Decision framework

# COMMAND ----------

# DBTITLE 1,Exercise 1: Task Types
# MAGIC %md
# MAGIC ## Exercise 1: Task Types
# MAGIC **Question**: You need to execute a SQL query as part of a job. Which task type should you use?
# MAGIC
# MAGIC A) Notebook task with SQL cell  
# MAGIC B) SQL task  
# MAGIC C) Pipeline task  
# MAGIC D) Python task

# COMMAND ----------

# DBTITLE 1,Solution 1
# Your solution for exercise 1

ex1 = 'B'

# COMMAND ----------

# DBTITLE 1,Exercise 2: Task Dependencies
# MAGIC %md
# MAGIC ## Exercise 2: Task Dependencies
# MAGIC **Question**: Complete the job configuration below by adding Task C, which should run only after both Task A and Task B complete successfully. Task C runs a notebook at `/path/to/task_c`.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "multi_task_job",
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "task_a",
# MAGIC       "notebook_task": {"notebook_path": "/path/to/task_a"}
# MAGIC     },
# MAGIC     {
# MAGIC       "task_key": "task_b",
# MAGIC       "notebook_task": {"notebook_path": "/path/to/task_b"}
# MAGIC     }
# MAGIC     // ADD: Task C configuration here
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Add the complete task object for Task C with proper dependencies.

# COMMAND ----------

# DBTITLE 1,Solution 2
# Your solution for exercise 2

ex2 = {
    "task_key": "task_c",
    "depends_on": [
        {"task_key": "task_a"},
        {"task_key": "task_b"}
    ],
    "notebook_task": {
        "notebook_path": "/path/to/notebook"
    }
}

# COMMAND ----------

# DBTITLE 1,Exercise 3: Job vs All-Purpose Clusters
# MAGIC %md
# MAGIC ## Exercise 3: Job vs All-Purpose Clusters
# MAGIC **Question**: Your team runs a nightly ETL job that takes 2 hours. The job runs at 2 AM and should minimize costs. Which cluster type should you use and why?

# COMMAND ----------

# DBTITLE 1,Solution 3
# Your solution for exercise 3

ex3 = 'job cluster'

# COMMAND ----------

# DBTITLE 1,Exercise 4: Quartz Cron Expression
# MAGIC %md
# MAGIC ## Exercise 4: Quartz Cron Expression
# MAGIC **Question**: Write a Quartz cron expression for:
# MAGIC a) Every Monday at 9:00 AM  
# MAGIC b) Every 6 hours starting at midnight  
# MAGIC c) 15th day of every month at 3:30 PM

# COMMAND ----------

# DBTITLE 1,Solution 4
# Your solution for exercise 4
# <seconds> <minutes> <hours> <day-of-month> <month> <day-of-week> <year>

ex4a = "0 0 9 ? * MON *"  # Every Monday at 9:00 AM
ex4b = "0 0 */6 * * * *"  # Every 6 hours
ex4c = "0 30 15 15 * ? *"  # 15th of month at 3:30 PM

# COMMAND ----------

# DBTITLE 1,Exercise 5: Retry Configuration
# MAGIC %md
# MAGIC ## Exercise 5: Retry Configuration
# MAGIC **Question**: You have a job that occasionally fails due to transient network issues. Complete the job configuration below to add retry logic: retry up to 3 times on failure with a 1-hour timeout.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "daily_etl",
# MAGIC   "tasks": [
# MAGIC     {"task_key": "ingest", "notebook_task": {...}},
# MAGIC     {"task_key": "transform", "notebook_task": {...}}
# MAGIC   ]
# MAGIC   // ADD: Retry and timeout configuration here
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Add the necessary keys and values for retry configuration.

# COMMAND ----------

# DBTITLE 1,Solution 5
# Your solution for exercise 5

ex5 = {
    "tasks": [
        {"task_key": "source_a"},
        {"task_key": "source_b"},
        {
            "task_key": "join",
            "depends_on": [
                {"task_key": "source_a"},
                {"task_key": "source_b"}
            ]
        }
    ]
    ,"max_retries": 3
    ,"retry_on_timeout": True
    ,"timeout_seconds": 3600
}

# COMMAND ----------

# DBTITLE 1,Exercise 6: Email Notifications
# MAGIC %md
# MAGIC ## Exercise 6: Email Notifications
# MAGIC **Question**: Add email notification configuration to send alerts to `data-team@example.com` on job failure and `manager@example.com` on success.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "daily_etl",
# MAGIC   "tasks": [...],
# MAGIC   "max_retries": 3
# MAGIC   // ADD: Email notification configuration here
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Add the necessary keys and values for email notifications.

# COMMAND ----------

# DBTITLE 1,Solution 6
# Your solution for exercise 6

ex6 = {
  "name": "daily_etl",
  "tasks": [],
  "max_retries": 3,
  "timeout_seconds": 3600,
  "email_notifications": {
    "on_success": ["manager@example.com"],
    "on_failure": ["data-team@example.com"]
  }
}

# COMMAND ----------

# DBTITLE 1,Exercise 7: File Arrival Trigger
# MAGIC %md
# MAGIC ## Exercise 7: File Arrival Trigger
# MAGIC **Question**: Configure this job to trigger automatically when new files arrive in `s3://my-bucket/incoming/`, with a minimum of 5 minutes between triggers.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "file_processor",
# MAGIC   "tasks": [...]
# MAGIC   // ADD: Trigger configuration here
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Add the necessary trigger configuration with file_arrival settings.

# COMMAND ----------

# DBTITLE 1,Solution 7
# Your solution for exercise 7

ex7 = {
  "name": "file_processor",
  "tasks": [],
  "trigger": {
    "file_arrival": {
      "url": "s3://my-bucket/incoming/",
      "min_time_between_triggers_seconds": 300
    }
  }
}

# COMMAND ----------

# DBTITLE 1,Exercise 8: Table Update Trigger
# MAGIC %md
# MAGIC ## Exercise 8: Table Update Trigger
# MAGIC **Question**: Configure this job to trigger when `catalog.schema.source_table` is updated, waiting 2 minutes (120 seconds) after the last change.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "downstream_processor",
# MAGIC   "tasks": [...]
# MAGIC   // ADD: Trigger configuration here
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Add the necessary trigger configuration with table_update settings.

# COMMAND ----------

# DBTITLE 1,Solution 8
# Your solution for exercise 8

ex8 = {
  "name": "downstream_processor",
  "tasks": [],
  "trigger": {
    "table_update": {
      "table_names": ["catalog.schema.source_table"],
      "wait_after_last_change_seconds": 120
    }
  }
}

# COMMAND ----------

# DBTITLE 1,Exercise 9: Multi-Task Job Design
# MAGIC %md
# MAGIC ## Exercise 9: Multi-Task Job Design
# MAGIC **Question**: Complete the job configuration below to implement this workflow:
# MAGIC - Task 1 (`ingest`): Ingest data (notebook)
# MAGIC - Task 2 (`transform_a`) & Task 3 (`transform_b`): Transform data in parallel (both depend on Task 1)
# MAGIC - Task 4 (`aggregate`): Aggregate results (depends on Tasks 2 & 3)
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "parallel_etl",
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "ingest",
# MAGIC       "notebook_task": {"notebook_path": "/path/to/ingest"}
# MAGIC     }
# MAGIC     // ADD: Define transform_a, transform_b, and aggregate tasks with proper dependencies
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Add the three remaining task definitions with their dependency configurations.

# COMMAND ----------

# DBTITLE 1,Solution 9
# Your solution for exercise 9

ex9 = {
  "name": "parallel_etl",
  "tasks": [
    {
      "task_key": "ingest",
      "notebook_task": {"notebook_path": "/path/to/ingest"}
    },
    {
      "task_key": "transform_a",
      "notebook_task": {"notebook_path": "/path/to/transform_a"},
      "depends_on": [
        {"task_key": "ingest"}
      ]
    },
    {
      "task_key": "transform_b",
      "notebook_task": {"notebook_path": "/path/to/transform_b"},
      "depends_on": [
        {"task_key": "ingest"}
      ]
    },
    {
      "task_key": "aggregate",
      "notebook_task": {"notebook_path": "/path/to/aggregate"},
      "depends_on": [
        {"task_key": "transform_a"},
        {"task_key": "transform_b"}
      ]
    }
  ]
}

# COMMAND ----------

# DBTITLE 1,Exercise 10: Job Parameters
# MAGIC %md
# MAGIC ## Exercise 10: Job Parameters
# MAGIC **Question**: Your notebook expects parameters `start_date` and `end_date`. Complete the task configuration below to pass these parameters with values `2024-01-01` and `2024-01-31`.
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "tasks": [
# MAGIC     {
# MAGIC       "task_key": "process_data",
# MAGIC       "notebook_task": {
# MAGIC         "notebook_path": "/path/to/notebook"
# MAGIC         // ADD: Parameter configuration here
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Add the necessary configuration to pass the two date parameters to the notebook.

# COMMAND ----------

# DBTITLE 1,Solution 10
# Your solution for exercise 10

ex10 = {
  "tasks": [
    {
      "task_key": "process_data",
      "notebook_task": {
        "notebook_path": "/path/to/notebook",
        "base_parameters": {"start_date": "2024-01-01", "end_date": "2024-01-31"}
      }
    }
  ]
}

# COMMAND ----------

# DBTITLE 1,Applied Exercise: Create a Job in the UI
# MAGIC %md
# MAGIC ## Applied Exercise: Create a Multi-Task Job in the UI
# MAGIC
# MAGIC **Objective**: Build hands-on experience with the Databricks Jobs interface by creating an actual job.
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC Create a job named **exactly**: `cert_prep_topic5_etl_pipeline`
# MAGIC
# MAGIC **Job Configuration**:
# MAGIC - Name: `cert_prep_topic5_etl_pipeline` (exact match required for verification)
# MAGIC - Schedule: Daily at 2:00 AM in your timezone
# MAGIC - Max concurrent runs: 1
# MAGIC - Timeout: 1 hour (3600 seconds)
# MAGIC - Email notification on failure to your email address
# MAGIC
# MAGIC **Task 1 - ingest_data**:
# MAGIC - Task key: `ingest_data`
# MAGIC - Type: Notebook task
# MAGIC - Notebook: Create a new notebook called `job_test_ingest` with this code:
# MAGIC   ```python
# MAGIC   print("Ingesting data...")
# MAGIC   record_count = 100
# MAGIC   dbutils.jobs.taskValues.set(key="record_count", value=record_count)
# MAGIC   print(f"Ingested {record_count} records")
# MAGIC   ```
# MAGIC - Compute: **Serverless** (recommended - fastest startup, auto-scaling, modern best practice)
# MAGIC   - Alternative: New job cluster (single node, smallest runtime) if your workspace doesn't have serverless enabled
# MAGIC
# MAGIC **Task 2 - transform_data** (depends on Task 1):
# MAGIC - Task key: `transform_data`
# MAGIC - Type: Notebook task
# MAGIC - Notebook: Create a new notebook called `job_test_transform` with this code:
# MAGIC   ```python
# MAGIC   # Get value from upstream task
# MAGIC   record_count = dbutils.jobs.taskValues.get(taskKey="ingest_data", key="record_count", default=0)
# MAGIC   print(f"Transforming {record_count} records...")
# MAGIC   transformed_count = record_count * 2
# MAGIC   dbutils.jobs.taskValues.set(key="transformed_count", value=transformed_count)
# MAGIC   print(f"Transformed to {transformed_count} records")
# MAGIC   ```
# MAGIC - Compute: Same as Task 1 (serverless or job cluster)
# MAGIC - Depends on: `ingest_data`
# MAGIC
# MAGIC **Task 3 - validate** (depends on Task 2):
# MAGIC - Task key: `validate`
# MAGIC - Type: Notebook task
# MAGIC - Notebook: Create a new notebook called `job_test_validate` with this code:
# MAGIC   ```python
# MAGIC   transformed_count = dbutils.jobs.taskValues.get(taskKey="transform_data", key="transformed_count", default=0)
# MAGIC   print(f"Validating {transformed_count} records...")
# MAGIC   if transformed_count > 0:
# MAGIC       print("✓ Validation passed")
# MAGIC   else:
# MAGIC       raise Exception("Validation failed: No records to validate")
# MAGIC   ```
# MAGIC - Compute: Same as Task 1 (serverless or job cluster)
# MAGIC - Depends on: `transform_data`
# MAGIC
# MAGIC ### Deliverables
# MAGIC
# MAGIC 1. **Run the job once** - Start a manual run and let it complete
# MAGIC 2. **Mark completion below** - Set `ex10_applied_complete = True` after creating and running the job
# MAGIC 3. **Verification** - The assistant can check for the job using the exact name
# MAGIC
# MAGIC ### Learning Outcomes
# MAGIC
# MAGIC * Navigate Jobs UI and create job from scratch
# MAGIC * Configure task dependencies and visualize DAG
# MAGIC * Pass values between tasks using `dbutils.jobs.taskValues`
# MAGIC * Configure job-level settings (schedule, retries, notifications)
# MAGIC * Monitor job execution and view task results
# MAGIC * Understand serverless compute vs job cluster tradeoffs
# MAGIC
# MAGIC ### Compute Selection: Serverless vs Job Clusters
# MAGIC
# MAGIC **Serverless (Recommended)**:
# MAGIC - Fastest startup time (no cluster provisioning wait)
# MAGIC - Automatic scaling based on workload
# MAGIC - Pay only for actual execution time
# MAGIC - No cluster management overhead
# MAGIC - Modern best practice for production jobs
# MAGIC - **Required on free/Community Edition workspaces**
# MAGIC
# MAGIC **Job Clusters (Alternative)**:
# MAGIC - Cost-effective for scheduled jobs (vs always-on all-purpose clusters)
# MAGIC - More control over cluster configuration
# MAGIC - Useful when specific driver/worker sizes needed
# MAGIC - Auto-terminates after job completes
# MAGIC - Traditional approach, still widely used
# MAGIC
# MAGIC **For the exam**: Understand BOTH patterns. Questions may ask when to use job clusters vs all-purpose clusters (job clusters for scheduled automation). Real-world production increasingly uses serverless.
# MAGIC
# MAGIC ### UI Navigation Hints
# MAGIC
# MAGIC 1. **Create Job**: Workflows → Jobs → Create Job
# MAGIC 2. **Add Tasks**: Click "Add task" for each task, configure dependencies in the "Depends on" dropdown
# MAGIC 3. **View DAG**: The graph visualization appears automatically as you add dependencies
# MAGIC 4. **Configure Job Settings**: Use the "Job details" panel on the right side
# MAGIC 5. **Schedule**: Click "Add trigger" → "Scheduled" → Configure cron expression
# MAGIC 6. **Run Job**: Click "Run now" button in top right
# MAGIC 7. **Monitor**: View the run details page to see task execution and outputs

# COMMAND ----------

# DBTITLE 1,Applied Exercise Solution/Status
# Applied Exercise: Create Job in UI
# Mark this as True after you've created and run the job

ex10_applied_complete = True

# Job name for verification: cert_prep_topic5_etl_pipeline
# The assistant can verify this job exists using the Jobs API

# COMMAND ----------

# DBTITLE 1,Exercise 11: Bundle Structure
# MAGIC %md
# MAGIC ## Exercise 11: Bundle Structure
# MAGIC **Question**: What is the primary configuration file for an Automation Bundle, and where should it be located?

# COMMAND ----------

# DBTITLE 1,Solution 11
# Your solution for exercise 11

ex11 = None

# COMMAND ----------

# DBTITLE 1,Exercise 12: Environment-Specific Configuration
# MAGIC %md
# MAGIC ## Exercise 12: Environment-Specific Configuration
# MAGIC **Question**: Create a `databricks.yml` snippet that defines a job named "ETL Pipeline" with the environment name appended (e.g., "ETL Pipeline - dev", "ETL Pipeline - prod").

# COMMAND ----------

# DBTITLE 1,Solution 12
# Your solution for exercise 12

ex12 = None

# COMMAND ----------

# DBTITLE 1,Exercise 13: Target Overrides
# MAGIC %md
# MAGIC ## Exercise 13: Target Overrides
# MAGIC **Question**: You have a bundle with `dev` and `prod` targets. In dev, you want `max_concurrent_runs: 5`. In prod, you want `max_concurrent_runs: 1` and email notifications on failure to `oncall@example.com`. Write the `databricks.yml` configuration.

# COMMAND ----------

# DBTITLE 1,Solution 13
# Your solution for exercise 13

ex13 = None

# COMMAND ----------

# DBTITLE 1,Exercise 14: Variables
# MAGIC %md
# MAGIC ## Exercise 14: Variables
# MAGIC **Question**: Define a variable `catalog_name` with default value `dev_catalog`. Override it in the `prod` target to use `prod_catalog`. Reference this variable in a notebook task parameter.

# COMMAND ----------

# DBTITLE 1,Solution 14
# Your solution for exercise 14

ex14 = None

# COMMAND ----------

# DBTITLE 1,Exercise 15: Bundle CLI Commands
# MAGIC %md
# MAGIC ## Exercise 15: Bundle CLI Commands
# MAGIC **Question**: Write the CLI commands to:
# MAGIC a) Validate a bundle for the dev target  
# MAGIC b) Deploy a bundle to the prod target  
# MAGIC c) Run a job named `daily_etl` in the dev target  
# MAGIC d) Destroy a bundle deployment in the dev target

# COMMAND ----------

# DBTITLE 1,Solution 15
# Your solution for exercise 15

ex15a = None  # Validate
ex15b = None  # Deploy
ex15c = None  # Run job
ex15d = None  # Destroy

# COMMAND ----------

# DBTITLE 1,Exercise 16: Git Workflow Steps
# MAGIC %md
# MAGIC ## Exercise 16: Git Workflow Steps
# MAGIC **Question**: List the UI steps to commit and push changes from Databricks Git Folders.

# COMMAND ----------

# DBTITLE 1,Solution 16
# Your solution for exercise 16

ex16 = None

# COMMAND ----------

# DBTITLE 1,Exercise 17: Branch Management
# MAGIC %md
# MAGIC ## Exercise 17: Branch Management
# MAGIC **Question**: You're working on the `main` branch and need to create a feature branch called `feature/add-validation`. Describe the UI steps to create and switch to this branch.

# COMMAND ----------

# DBTITLE 1,Solution 17
# Your solution for exercise 17

ex17 = None

# COMMAND ----------

# DBTITLE 1,Exercise 18: .gitignore Best Practices
# MAGIC %md
# MAGIC ## Exercise 18: .gitignore Best Practices
# MAGIC **Question**: What files/directories should NEVER be committed to Git in a Databricks project? List at least 5 patterns for `.gitignore`.

# COMMAND ----------

# DBTITLE 1,Solution 18
# Your solution for exercise 18

ex18 = None

# COMMAND ----------

# DBTITLE 1,Exercise 19: Git Source in Jobs
# MAGIC %md
# MAGIC ## Exercise 19: Git Source in Jobs
# MAGIC **Question**: Configure a job task to run a notebook from Git repository `https://github.com/myorg/data-pipelines`, branch `main`, notebook path `notebooks/process.py`. Write the JSON configuration.

# COMMAND ----------

# DBTITLE 1,Solution 19
# Your solution for exercise 19

ex19 = None

# COMMAND ----------

# DBTITLE 1,Exercise 20: Protected Branch Workflow
# MAGIC %md
# MAGIC ## Exercise 20: Protected Branch Workflow
# MAGIC **Question**: Your repository has `main` as a protected branch. Describe the complete workflow from making changes to getting them into `main`, including the pull request process.

# COMMAND ----------

# DBTITLE 1,Solution 20
# Your solution for exercise 20

ex20 = None

# COMMAND ----------

# DBTITLE 1,MCQs 1-5
# MAGIC %md
# MAGIC ## Multiple Choice Questions (MCQs)
# MAGIC
# MAGIC ### MCQ 1: Cron Expression
# MAGIC **Question**: How many fields are in a Quartz cron expression used by Databricks Jobs?
# MAGIC
# MAGIC A) 5 fields  
# MAGIC B) 6 fields  
# MAGIC C) 7 fields  
# MAGIC D) 8 fields
# MAGIC
# MAGIC ### MCQ 2: Cluster Cost
# MAGIC **Question**: Which cluster type is most cost-effective for scheduled production ETL jobs that run nightly?
# MAGIC
# MAGIC A) All-purpose cluster with autoscaling  
# MAGIC B) Job cluster that auto-terminates  
# MAGIC C) High-concurrency cluster  
# MAGIC D) Single-node cluster
# MAGIC
# MAGIC ### MCQ 3: Bundle Deployment
# MAGIC **Question**: Which CLI command deploys an Automation Bundle to the production environment?
# MAGIC
# MAGIC A) `databricks deploy -t prod`  
# MAGIC B) `databricks bundle push prod`  
# MAGIC C) `databricks bundle deploy -t prod`  
# MAGIC D) `databricks assets deploy prod`
# MAGIC
# MAGIC ### MCQ 4: Git Protected Branch
# MAGIC **Question**: You try to commit directly to the `main` branch in Git Folders, but it's protected. What must you do?
# MAGIC
# MAGIC A) Request admin access to override protection  
# MAGIC B) Create a feature branch, commit there, and create a pull request  
# MAGIC C) Use force push to bypass protection  
# MAGIC D) Disable branch protection in Databricks settings
# MAGIC
# MAGIC ### MCQ 5: Task Dependency
# MAGIC **Question**: Task B should run only after Task A completes successfully. What is the correct syntax?
# MAGIC
# MAGIC A) `Task B: {"after": "task_a"}`  
# MAGIC B) `Task B: {"depends_on": "task_a"}`  
# MAGIC C) `Task B: {"depends_on": [{"task_key": "task_a"}]}`  
# MAGIC D) `Task B: {"requires": ["task_a"]}`

# COMMAND ----------

# DBTITLE 1,MCQ Solutions
# Your answers for MCQs 1-5

mcq1 = None
mcq2 = None
mcq3 = None
mcq4 = None
mcq5 = None

# COMMAND ----------

# DBTITLE 1,Challenge 1: Complete CI/CD Pipeline
# MAGIC %md
# MAGIC ## Challenge 1: Complete CI/CD Pipeline
# MAGIC **Scenario**: Your team needs a complete CI/CD setup with:
# MAGIC * Git repository with `main` and `dev` branches
# MAGIC * Automation Bundle deploying to dev and prod environments
# MAGIC * 3-task job: Bronze ingestion → Silver transformation → Gold aggregation
# MAGIC * Dev: allows concurrent runs, uses `dev_catalog`
# MAGIC * Prod: prevents concurrent runs, uses `prod_catalog`, sends failure alerts to `oncall@example.com`
# MAGIC * Scheduled to run daily at 3 AM in prod only
# MAGIC
# MAGIC **Task**: Write the complete `databricks.yml` configuration.

# COMMAND ----------

# DBTITLE 1,Challenge 1 Solution
# Your solution for Challenge 1

challenge1 = None

# COMMAND ----------

# DBTITLE 1,Challenge 2: Troubleshooting Job Failure
# MAGIC %md
# MAGIC ## Challenge 2: Troubleshooting Job Failure
# MAGIC **Scenario**: A job with 4 tasks fails intermittently:
# MAGIC * Task 1 (bronze): Succeeds consistently
# MAGIC * Task 2 (silver_a): Fails 30% of the time with timeout
# MAGIC * Task 3 (silver_b): Succeeds consistently
# MAGIC * Task 4 (gold): Doesn't run when Task 2 fails
# MAGIC
# MAGIC The team wants Task 4 to run even if Task 2 fails, processing whatever data is available.
# MAGIC
# MAGIC **Task**:
# MAGIC 1. Identify the issue with current configuration
# MAGIC 2. Propose two solutions with trade-offs
# MAGIC 3. Write updated task configuration implementing your preferred solution

# COMMAND ----------

# DBTITLE 1,Challenge 2 Solution
# Your solution for Challenge 2

challenge2 = None

# COMMAND ----------

# DBTITLE 1,Applied: Deployment Strategy Decision Framework
# MAGIC %md
# MAGIC ## Applied: Deployment Strategy Decision Framework
# MAGIC
# MAGIC Create a function that recommends the appropriate deployment strategy based on team size, environments, change frequency, and complexity.

# COMMAND ----------

# DBTITLE 1,Applied Solution
# Your solution for Applied

def select_deployment_strategy(team_size, environments, change_frequency, complexity):
    """
    Decision framework for CI/CD strategy.
    
    Args:
        team_size: int (1-50)
        environments: list of env names
        change_frequency: str ("daily", "weekly", "monthly")
        complexity: str ("simple", "moderate", "complex")
    
    Returns:
        Recommended deployment approach
    """
    pass

# COMMAND ----------

# DBTITLE 1,MCQs 1-5
# MAGIC %md
# MAGIC ## Multiple Choice Questions (MCQs)
# MAGIC
# MAGIC ### MCQ 1: Cron Expression
# MAGIC **Question**: How many fields are in a Quartz cron expression used by Databricks Jobs?
# MAGIC
# MAGIC A) 5 fields  
# MAGIC B) 6 fields  
# MAGIC C) 7 fields  
# MAGIC D) 8 fields
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 2: Cluster Cost
# MAGIC **Question**: Which cluster type is most cost-effective for scheduled production ETL jobs that run nightly?
# MAGIC
# MAGIC A) All-purpose cluster with autoscaling  
# MAGIC B) Job cluster that auto-terminates  
# MAGIC C) High-concurrency cluster  
# MAGIC D) Single-node cluster
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 3: Bundle Deployment
# MAGIC **Question**: Which CLI command deploys an Automation Bundle to the production environment?
# MAGIC
# MAGIC A) `databricks deploy -t prod`  
# MAGIC B) `databricks bundle push prod`  
# MAGIC C) `databricks bundle deploy -t prod`  
# MAGIC D) `databricks assets deploy prod`
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 4: Git Protected Branch
# MAGIC **Question**: You try to commit directly to the `main` branch in Git Folders, but it's protected. What must you do?
# MAGIC
# MAGIC A) Request admin access to override protection  
# MAGIC B) Create a feature branch, commit there, and create a pull request  
# MAGIC C) Use force push to bypass protection  
# MAGIC D) Disable branch protection in Databricks settings
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### MCQ 5: Task Dependency
# MAGIC **Question**: Task B should run only after Task A completes successfully. What is the correct syntax?
# MAGIC
# MAGIC A) `Task B: {"after": "task_a"}`  
# MAGIC B) `Task B: {"depends_on": "task_a"}`  
# MAGIC C) `Task B: {"depends_on": [{"task_key": "task_a"}]}`  
# MAGIC D) `Task B: {"requires": ["task_a"]}`

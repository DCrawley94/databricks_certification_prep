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

ex1 = None

# COMMAND ----------

# DBTITLE 1,Exercise 2: Task Dependencies
# MAGIC %md
# MAGIC ## Exercise 2: Task Dependencies
# MAGIC **Question**: Configure Task C to run only after both Task A and Task B complete successfully. Write the JSON configuration.

# COMMAND ----------

# DBTITLE 1,Solution 2
# Your solution for exercise 2

ex2 = None

# COMMAND ----------

# DBTITLE 1,Exercise 3: Job vs All-Purpose Clusters
# MAGIC %md
# MAGIC ## Exercise 3: Job vs All-Purpose Clusters
# MAGIC **Question**: Your team runs a nightly ETL job that takes 2 hours. The job runs at 2 AM and should minimize costs. Which cluster type should you use and why?

# COMMAND ----------

# DBTITLE 1,Solution 3
# Your solution for exercise 3

ex3 = None

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

ex4a = None  # Every Monday at 9:00 AM
ex4b = None  # Every 6 hours
ex4c = None  # 15th of month at 3:30 PM

# COMMAND ----------

# DBTITLE 1,Exercise 5: Retry Configuration
# MAGIC %md
# MAGIC ## Exercise 5: Retry Configuration
# MAGIC **Question**: Configure a job to retry up to 3 times on failure with a 1-hour timeout. Write the JSON configuration.

# COMMAND ----------

# DBTITLE 1,Solution 5
# Your solution for exercise 5

ex5 = None

# COMMAND ----------

# DBTITLE 1,Exercise 6: Email Notifications
# MAGIC %md
# MAGIC ## Exercise 6: Email Notifications
# MAGIC **Question**: Configure email notifications to send to `data-team@example.com` on job failure, and to `manager@example.com` on success. Write the JSON.

# COMMAND ----------

# DBTITLE 1,Solution 6
# Your solution for exercise 6

ex6 = None

# COMMAND ----------

# DBTITLE 1,Exercise 7: File Arrival Trigger
# MAGIC %md
# MAGIC ## Exercise 7: File Arrival Trigger
# MAGIC **Question**: Configure a job to trigger when new files land in `s3://my-bucket/incoming/`, with a minimum of 5 minutes between triggers.

# COMMAND ----------

# DBTITLE 1,Solution 7
# Your solution for exercise 7

ex7 = None

# COMMAND ----------

# DBTITLE 1,Exercise 8: Table Update Trigger
# MAGIC %md
# MAGIC ## Exercise 8: Table Update Trigger
# MAGIC **Question**: Configure a job to trigger when `catalog.schema.source_table` is updated, waiting 2 minutes after the last change.

# COMMAND ----------

# DBTITLE 1,Solution 8
# Your solution for exercise 8

ex8 = None

# COMMAND ----------

# DBTITLE 1,Exercise 9: Multi-Task Job Design
# MAGIC %md
# MAGIC ## Exercise 9: Multi-Task Job Design
# MAGIC **Question**: Design a job with the following requirements:
# MAGIC - Task 1: Ingest data (notebook)
# MAGIC - Task 2 & 3: Transform data in parallel (both depend on Task 1)
# MAGIC - Task 4: Aggregate results (depends on Tasks 2 & 3)
# MAGIC
# MAGIC Draw the DAG and write the task configuration.

# COMMAND ----------

# DBTITLE 1,Solution 9
# Your solution for exercise 9

ex9 = None

# COMMAND ----------

# DBTITLE 1,Exercise 10: Job Parameters
# MAGIC %md
# MAGIC ## Exercise 10: Job Parameters
# MAGIC **Question**: Your notebook expects parameters `start_date` and `end_date`. How do you pass these from a job task? Write the configuration.

# COMMAND ----------

# DBTITLE 1,Solution 10
# Your solution for exercise 10

ex10 = None

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

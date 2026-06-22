# Databricks notebook source
# Get value from upstream task
record_count = dbutils.jobs.taskValues.get(taskKey="ingest_data", key="record_count", default=0)
print(f"Transforming {record_count} records...")
transformed_count = record_count * 2
dbutils.jobs.taskValues.set(key="transformed_count", value=transformed_count)
print(f"Transformed to {transformed_count} records")

# Databricks notebook source
transformed_count = dbutils.jobs.taskValues.get(taskKey="transform_data", key="transformed_count", default=0)
print(f"Validating {transformed_count} records...")
if transformed_count > 0:
    print("✓ Validation passed")
else:
    raise Exception("Validation failed: No records to validate")

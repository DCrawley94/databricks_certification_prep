# Databricks notebook source
print("Ingesting data...")
record_count = 100
dbutils.jobs.taskValues.set(key="record_count", value=record_count)
print(f"Ingested {record_count} records")

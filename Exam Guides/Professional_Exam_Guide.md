# Databricks Certified Data Engineer Professional
## Exam Guide - Comprehensive Topic List

**Exam Version**: November 30, 2025

---

## Exam Format

* **Questions**: 59 scored multiple-choice questions
* **Duration**: 120 minutes
* **Fee**: USD $200 (plus applicable taxes)
* **Delivery**: Online proctored or test center
* **Test Aids**: None allowed (including API documentation)
* **Prerequisites**: None required; related course attendance and one year of hands-on experience in Data Engineering tasks outlined in the exam guide are highly recommended
* **Validity**: 2 years
* **Recertification**: Required every 2 years

---

## Target Audience

The Databricks Certified Data Engineering Professional exam validates a candidate's advanced skills in building, optimizing, and maintaining production-grade data engineering solutions on the Databricks Data Intelligence Platform. Successful candidates demonstrate expertise across core platform features such as Delta Lake, Unity Catalog, Auto Loader, Lakeflow Spark Declarative Pipelines, Databricks Compute (including serverless), Lakeflow Jobs and the Medallion Architecture. This certification assesses the ability to design secure, reliable, and cost-effective ETL Pipelines, process complex data from diverse sources using Python and SQL, and apply best practices in schema management, observability, governance, and performance optimization. Candidates are also tested on implementing streaming workloads, orchestrating workflows, leveraging DevOps & CI/CD, and deploying with tools like the Databricks CLI, REST API, and Asset Bundles.

---

## Recommended Training

**Instructor-Led**:
* Advanced Data Engineering With Databricks

**Self-Paced (Databricks Academy)**:
* Databricks Streaming and Lakeflow Spark Declarative Pipelines
* Databricks Data Privacy
* Databricks Performance Optimization
* Automated Deployment with Databricks Asset Bundle

---

## Exam Sections and Topics

### Section 1: Developing Code for Data Processing using Python and SQL

* Using Python and tools for development
* Design and implement modular functions for processing tabular data
* Chain transformations using DataFrame.transform
* Define and invoke Python and Pandas UDFs
* Manage external third-party library requirements (PyPI, wheels, archives)
* Troubleshoot module import and dependency conflicts across notebooks and jobs
* Leverage the built-in debugger to identify exceptions and logic errors
* Test DataFrame transformations using assertDataFrameEqual and assertSchemaEqual functions

### Section 2: Data Ingestion, Processing, and Transformation

* Extract and load data into Delta Lake
* Leverage Auto Loader with data quarantining for robust file ingestion
* Configure for different file formats, cloud providers, and authentication modes
* Understand schema enforcement and schema evolution
* Understanding differences between batch and streaming modes
* Understand checkpoint management

### Section 3: Lakeflow Spark Declarative Pipelines (SDP)

* Identify and understand the core abstractions of SDP: streaming tables and materialized views
* Compare SDP with Spark Structured Streaming to choose the optimal approach
* Implement control flow operators (if/else and for/each) in SDP
* Leverage APPLY CHANGES APIs for Change Data Capture (CDC) simplification
* Configure environments and dependencies for SDP
* Understand retry and failure strategies in SDP
* Use auto optimization capabilities in SDP
* Leverage data quarantining strategies in SDP

### Section 4: Orchestration, Observability, and Monitoring

* Orchestrate workflows using Lakeflow Jobs with advanced features
* Leverage REST APIs/CLI to automate creating, updating, and monitoring ETL workloads
* Leverage the Query Profiler UI to understand and optimize workload execution
* Understand SDP Event Logs to diagnose data quality and pipeline issues
* Create SQL Alerts to proactively monitor data quality, freshness, and anomalies
* Create alerts in the Lakeflow Jobs UI for job status and performance
* Use Jobs API to configure email notifications for pipeline failures
* Understand system tables for observability (resource utilization, cost, auditing)

### Section 5: Delta Lake and Performance Optimization

* Understand Delta Lake's metadata, catalog-metastore operations, and ACID compliance
* Compare Change Data Feed vs streaming tables/SDP for incremental processing
* Apply Delta Lake clone to understand how shallow and deep clones interact with source/target
* Leverage deletion vectors for efficient delete operations
* Understand the role of liquid clustering
* Optimize query performance through file pruning and data skipping
* Understand the advantages of UC managed tables for automatic optimizations

### Section 6: Governance, Security, and Data Sharing

* Understand security architecture and best practices
* Implement row-level security and column masking
* Understand attribute-based access control (ABAC)
* Configure Delta Sharing for Databricks-to-Databricks (D2D) sharing
* Understand open sharing protocol for Databricks-to-other (D2O) sharing
* Understand Lakehouse Federation to configure and work with supported source systems

### Section 7: Production Engineering and CI/CD

* Design scalable project structures for Databricks Asset Bundles (DABs)
* Implement environment-specific overrides using DAB variables and targets
* Automate the deployment of notebooks, jobs, and pipelines using the Databricks CLI
* Integrate DABs into CI/CD workflows with automated validation and deployment
* Configure integration tests for data pipelines
* Implement unit tests using assertDataFrameEqual and assertSchemaEqual

---

## Key Differences from Associate Level

* **Advanced Development**: Python/Pandas UDFs, external library management, debugging, testing
* **SDP Deep Dive**: Control flow operators, APPLY CHANGES for CDC, advanced configuration
* **Delta Sharing & Federation**: Sharing data between deployments and federating external sources
* **System Tables & Monitoring**: Production observability, cost analysis, proactive alerting
* **Performance Optimization**: Deletion vectors, liquid clustering, Change Data Feed patterns
* **DevOps & Automation**: Asset Bundles (DABs), CLI automation, comprehensive testing strategies

---

## Notes

* Requires deeper understanding of production-grade implementations
* No percentages provided for individual sections in the official guide
* Emphasis on automation, optimization, and enterprise-scale patterns
* Focus areas: SDP advanced features, Delta Sharing, system tables, Asset Bundles, testing

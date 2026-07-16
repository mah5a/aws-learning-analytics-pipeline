
# AWS Learning Analytics Pipeline

An end-to-end **AWS Data Engineering** project that builds a scalable Learning Analytics pipeline using the **Open University Learning Analytics Dataset (OULAD)**.

The project demonstrates how raw educational data can be ingested, transformed, cataloged, and analyzed using AWS cloud services including **Amazon S3, AWS Glue, Glue Crawlers, Glue Data Catalog, and Amazon Athena**.

---

# Architecture

<p align="center">
  <img src="architecture/AWS Learning Analytics Pipeline.png" alt="AWS Learning Analytics Pipeline Architecture" width="450">
</p>

---

# Project Overview

This project implements a cloud-based ETL pipeline for educational data analytics.

The pipeline:

- Stores raw datasets in Amazon S3
- Automatically discovers schemas using AWS Glue Crawlers
- Performs ETL transformations using AWS Glue (PySpark)
- Engineers student learning features
- Stores processed datasets in Amazon S3 Curated Zone
- Registers metadata in AWS Glue Data Catalog
- Executes analytical SQL queries using Amazon Athena

---

# Tech Stack

| Category | Technology |
|-----------|------------|
| Cloud | AWS |
| Storage | Amazon S3 |
| ETL | AWS Glue |
| Metadata | AWS Glue Data Catalog |
| Crawling | AWS Glue Crawlers |
| Query Engine | Amazon Athena |
| Programming | Python |
| Processing | PySpark |
| SQL | Athena SQL |
| Version Control | Git & GitHub |

---

# Dataset

**Open University Learning Analytics Dataset (OULAD)**

The dataset contains information about:

- Student demographics
- Student assessments
- Student interactions (VLE)
- Courses and presentations

---

# ETL Pipeline

## Step 1 — Raw Data

CSV datasets are uploaded into the Raw Zone in Amazon S3.

Examples:

- studentInfo.csv
- studentAssessment.csv
- studentVLE.csv

---

## Step 2 — Metadata Discovery

AWS Glue Crawlers automatically discover:

- Tables
- Columns
- Data Types

Metadata is stored in AWS Glue Data Catalog.

---

## Step 3 — ETL Processing

AWS Glue ETL jobs perform:

- Data cleaning
- Aggregation
- Feature Engineering
- Joining datasets
- Schema mapping

---

## Step 4 — Curated Zone

Processed datasets are written into Amazon S3 Curated Zone in Parquet format.

---

## Step 5 — Analytics

Amazon Athena queries the curated datasets for reporting and analytics.

---

# Feature Engineering

The pipeline creates learning analytics features including:

- Average Assessment Score
- Failed Assessments
- Average Submission Delay
- Total Clicks
- Average Daily Clicks
- First Activity Day
- Last Activity Day

These features can be used for:

- Student performance analysis
- Engagement analytics
- Predictive modeling
- Dropout risk prediction

---

# Repository Structure

```text
aws-learning-analytics-pipeline
│
├── architecture/
├── athena/
│   └── queries.sql
│
├── docs/
│
├── glue_jobs/
│
├── screenshots/
│   ├── athena/
│   ├── crawler/
│   ├── glue/
│   └── s3/
│
└── README.md
```

---

# Sample Athena Queries

```sql
SELECT AVG(avg_score)
FROM student_features;
```

```sql
SELECT *
FROM student_features
WHERE failed_assessments > 0;
```

# Future Improvements

Potential enhancements include:

- Apache Airflow orchestration
- AWS Lambda automation
- Amazon EventBridge scheduling
- CI/CD using GitHub Actions
- Infrastructure as Code using Terraform
- Data Quality Validation
- Cloud Monitoring & Logging

---

# Skills Demonstrated

- AWS Data Engineering
- Cloud ETL Pipelines
- Amazon S3
- AWS Glue
- Glue Crawlers
- Glue Data Catalog
- PySpark
- SQL Analytics
- Amazon Athena
- Feature Engineering
- Data Modeling
- Git & GitHub

---

# Author

**Mahsa Karamimehr**

Data Analyst | Aspiring Data Engineer

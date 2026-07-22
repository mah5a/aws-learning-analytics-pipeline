# AWS Learning Analytics Pipeline

An end-to-end **AWS Data Engineering** project that builds a cloud-based Learning Analytics pipeline using the **Open University Learning Analytics Dataset (OULAD)**.

This project demonstrates how educational data is ingested, transformed, cataloged, orchestrated, queried, and visualized using modern AWS data engineering services.

---

# Architecture

<p align="center">
  <img src="architecture/AWS Learning Analytics Pipeline.png" alt="AWS Learning Analytics Pipeline Architecture" width="650">
</p>

---

# Project Overview

This project simulates a production-style batch data pipeline on AWS.

The pipeline:

- Stores raw educational datasets in Amazon S3
- Orchestrates the workflow using Apache Airflow
- Performs ETL transformations with AWS Glue (PySpark)
- Engineers learning analytics features
- Stores curated datasets in Amazon S3 (Parquet)
- Updates metadata using AWS Glue Crawlers
- Queries data using Amazon Athena
- Visualizes insights with Power BI

---

# Pipeline Workflow

```text
Amazon S3 (Raw Data)
        │
        ▼
Apache Airflow
        │
        ▼
AWS Glue ETL (PySpark)
        │
        ▼
Amazon S3 (Curated)
        │
        ▼
AWS Glue Crawler
        │
        ▼
Glue Data Catalog
        │
        ▼
Amazon Athena
        │
        ▼
Power BI Dashboard
```

---

# Tech Stack

| Category | Technology |
|-----------|------------|
| Cloud | AWS |
| Storage | Amazon S3 |
| ETL | AWS Glue |
| Processing | PySpark |
| Orchestration | Apache Airflow |
| Metadata | AWS Glue Data Catalog |
| Crawling | AWS Glue Crawlers |
| Query Engine | Amazon Athena |
| Dashboard | Power BI |
| Programming | Python |
| Containerization | Docker |
| Version Control | Git & GitHub |

---

# Dataset

**Open University Learning Analytics Dataset (OULAD)**

The dataset includes:

- Student demographics
- Student assessments
- Student interactions (VLE)
- Courses and presentations

---

# ETL Pipeline

### 1. Raw Data Ingestion

CSV files are uploaded into the Amazon S3 Raw Zone.

Example datasets:

- studentInfo.csv
- studentAssessment.csv
- studentVLE.csv

---

### 2. Workflow Orchestration

Apache Airflow automates the pipeline by:

- Validating source files in Amazon S3
- Triggering AWS Glue ETL jobs
- Running AWS Glue Crawlers
- Validating output with Amazon Athena

---

### 3. Data Transformation

AWS Glue ETL jobs perform:

- Data cleaning
- Dataset joins
- Feature engineering
- Schema mapping
- Aggregations

---

### 4. Curated Data

Processed data is written to Amazon S3 in Parquet format for efficient analytics.

---

### 5. Metadata Management

AWS Glue Crawlers automatically update the Glue Data Catalog.

---

### 6. Analytics

Amazon Athena executes SQL queries directly on curated datasets.

---

# Feature Engineering

The pipeline generates learning analytics features such as:

- Average Assessment Score
- Failed Assessments
- Average Submission Delay
- Total Clicks
- Average Daily Clicks
- First Activity Day
- Last Activity Day

These features support:

- Student performance analysis
- Engagement analytics
- Learning behavior analysis
- Predictive modeling
- Dropout risk prediction

---

# Repository Structure

```text
aws-learning-analytics-pipeline/
│
├── airflow/
│   ├── dags/
│   └── docker-compose.yaml
│
├── architecture/
│
├── athena/
│   └── queries.sql
│
├── docker/
│   └── Dockerfile
│
├── docs/
│
├── glue_jobs/
│
├── powerbi/
│   └── AWS_learning_analytics_dashboard.pbix
│
├── screenshots/
│   ├── athena/
│   ├── glue/
│   └── s3/
│
└── README.md
```

---

# Sample Athena Queries

Average assessment score:

```sql
SELECT AVG(avg_score)
FROM student_features;
```

Students with failed assessments:

```sql
SELECT *
FROM student_features
WHERE failed_assessments > 0;
```

Top-performing students:

```sql
SELECT id_student, avg_score
FROM student_features
ORDER BY avg_score DESC
LIMIT 10;
```

Average submission delay:

```sql
SELECT AVG(avg_submission_delay)
FROM student_features;
```

---

# Power BI Dashboard

The curated dataset is connected to Power BI to visualize:

- Student performance
- Assessment scores
- Learning engagement
- Click activity
- Submission behavior

---

# Future Improvements

Planned enhancements include:

- AWS Lambda automation
- Amazon EventBridge scheduling
- CI/CD using GitHub Actions
- Infrastructure as Code (Terraform)
- CloudWatch monitoring
- Expanded data quality validation

---

# Skills Demonstrated

- End-to-End AWS Data Engineering
- ETL Pipeline Development
- Amazon S3
- AWS Glue
- Apache Airflow
- PySpark
- Amazon Athena
- SQL Analytics
- Power BI
- Docker
- Feature Engineering
- Data Modeling
- Git & GitHub

---

# Author

**Mahsa Kmehr**

Data Analyst | Aspiring Data Engineer

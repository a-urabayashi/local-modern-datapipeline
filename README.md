# ETL Pipeline (WIP)

**=� Work In Progress =�**

This directory contains the ETL (Extract, Transform, Load) pipeline for the Healthcare Performance Data Platform.

## Overview

The ETL pipeline is responsible for:
- Extracting data from various healthcare data sources
- Transforming and standardizing the data
- Loading processed data into the analytics database

## Architecture

- **Apache Airflow** - Workflow orchestration
- **Apache Spark** - Distributed data processing
- **Docker** - Containerized deployment
- **MinIO** - S3-compatible object storage for data lake
- **dbt + Snowflake** - Analytics transformation layer

## Directory Structure

- `dags/` - Airflow DAG definitions
- `scripts/` - ETL scripts and utilities
- `spark/` - Spark configuration and scripts
- `snowflake_analytics/` - dbt project for analytics transformations
- `docker-compose.yml` - Docker compose configuration for local development

## Setup Instructions

```bash
# Start the ETL services
docker-compose up -d

# Access services:
# Airflow UI: http://localhost:8080
# MinIO Console: http://localhost:9001
# Spark UI: http://localhost:8081
```

## Data Sources (Planned)

- EMR (Electronic Medical Records) systems
- Billing and claims data
- Operational metrics
- External benchmarking data

## TODO

- [ ] Implement data source connectors
- [ ] Create transformation pipelines
- [ ] Set up data quality checks
- [ ] Configure monitoring and alerting
- [ ] Add data lineage tracking
- [ ] Implement incremental processing
- [ ] Complete dbt models for analytics
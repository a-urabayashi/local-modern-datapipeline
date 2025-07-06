# Modern Data Pipeline

A scalable ETL pipeline built with Apache Airflow, Spark, and dbt for processing large-scale datasets from Hugging Face.

## 🚧 WIP
This project is under active development.

## Overview
This pipeline processes Japanese text data from HuggingFace datasets, demonstrating modern data engineering practices with:
- Streaming data ingestion
- Distributed processing
- Data quality validation
- Analytics transformations

## Architecture

### Core Components

- **Apache Airflow** - Workflow orchestration and scheduling
- **MinIO** - S3-compatible object storage for data lake
- **DuckDB** - In-process analytics database
- **dbt** - Data transformation and modeling
- **Docker** - Containerized deployment
- **Spark** - Distributed processing (container ready for FastAPI service)

### Data Flow

1. **Extract**: Stream data from HuggingFace datasets
2. **Transform**: Process data in configurable chunks
3. **Store**: Save as Parquet files in MinIO
4. **Load**: Import into DuckDB for analytics
5. **Model**: Run dbt transformations for data quality and insights

## Current Implementation

### Data Sources
- **HuggingFace FineWeb** - Japanese web text corpus (380M+ rows)
- **Wikipedia Japanese** - Japanese Wikipedia articles (10M+ rows)

### Processing Strategies
The pipeline supports multiple execution strategies:
- `test` - Small sample for testing (1,000 rows)
- `memory_optimized` - Conservative memory usage
- `conservative` - Balanced approach
- `balanced` - Standard processing
- `aggressive` - Maximum throughput

### Features
- ✅ Streaming data ingestion with memory management
- ✅ Chunked processing with configurable batch sizes
- ✅ Data quality validation and reporting
- ✅ dbt models for analytics (content analysis, quality metrics)
- ✅ Comprehensive error handling and fallback mechanisms
- ⚠️  FastAPI service integration (client implemented, service pending)
- ⚠️  Delta Lake support (planned)

## Quick Start

### Prerequisites
- Docker and Docker Compose
- 8GB+ RAM recommended
- 50GB+ disk space for data storage

### Setup

1. Clone the repository and navigate to the project directory

2. Create `.env` file from sample:
```bash
cp .env.sample .env
```

3. Start the services:
```bash
docker-compose up -d
```

4. Wait for services to initialize (2-3 minutes)

5. Access the services:
   - **Airflow UI**: http://localhost:8080 (admin/admin)
   - **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)

### Running the Pipeline

1. Open Airflow UI at http://localhost:8080
2. Enable the `local_data_processing_fastapi` DAG
3. Trigger a manual run with config:
```json
{
  "execution_strategy": "test",
  "max_rows": 1000
}
```

## Project Structure

```
.
├── dags/
│   ├── dags.py                    # Main DAG definition
│   ├── huggingface_utils.py       # HuggingFace data utilities
│   └── fastapi_delta_tasks.py     # FastAPI client (for future service)
├── duckdb_pipeline/               # dbt project
│   ├── models/
│   │   ├── staging/              # Raw data staging
│   │   └── marts/                # Business logic models
│   └── dbt_project.yml
├── scripts/
│   └── minio_setup.py            # Storage initialization
├── spark/                        # Spark container config
├── docker-compose.yml            # Service orchestration
└── requirements.txt              # Python dependencies
```

## Monitoring

### Pipeline Status
- Check DAG runs in Airflow UI
- View task logs for detailed execution info
- Monitor data quality reports in task outputs

### Storage
- MinIO dashboard shows bucket usage and object counts
- Parquet files organized by dataset and date

## Development

### Adding New Data Sources
1. Create a new extraction function in `huggingface_utils.py`
2. Add corresponding task in the DAG
3. Create staging and mart models in dbt

### Modifying Processing Logic
- Adjust chunk sizes and strategies in DAG configuration
- Update memory limits in `huggingface_utils.py`
- Modify dbt models for new analytics requirements

## Roadmap

- [ ] Implement FastAPI service for real-time processing
- [ ] Add Delta Lake for ACID transactions
- [ ] Integrate Snowflake for cloud analytics
- [ ] Add more data sources and transformations
- [ ] Implement data lineage tracking
- [ ] Add comprehensive monitoring and alerting

## Troubleshooting

### Common Issues

1. **Out of Memory Errors**
   - Use a more conservative execution strategy
   - Reduce chunk sizes in configuration

2. **MinIO Connection Errors**
   - Ensure MinIO is running: `docker-compose ps`
   - Check MinIO logs: `docker-compose logs minio`

3. **DAG Import Errors**
   - Check Airflow scheduler logs
   - Verify Python dependencies are installed

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.
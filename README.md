# Data Pipeline Portfolio

A production-ready, end-to-end data pipeline showcasing modern data engineering tools and best practices.

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│  Apache Airflow │───▶│   dbt + Trino   │
│  (Synthetic)    │    │  (Orchestration)│    │ (Transformations)│
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   MinIO (S3)    │
                       │ (Object Storage)│
                       └─────────────────┘
```

## Tech Stack

- **Orchestration**: Apache Airflow 2.8+
- **SQL Engine**: Trino 435
- **Object Storage**: MinIO (S3-compatible)
- **Transformations**: dbt-core 1.7+ with dbt-trino adapter
- **Metastore**: Hive Metastore with MySQL backend
- **Containerization**: Docker Compose
- **Language**: Python 3.11

## Quick Start

1. **Clone and setup**:
   ```bash
   git clone <repository-url>
   cd data-pipeline-portfolio
   cp env.example .env
   ```

2. **Start services**:
   ```bash
   make up
   ```

3. **Access services**:
   - Airflow UI: http://localhost:8081 (airflow/airflow)
   - Trino UI: http://localhost:8080
   - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

4. **Run the pipeline**:
   - Enable the DAG in Airflow UI
   - Monitor execution in the Airflow dashboard

## Project Structure

```
data-pipeline-portfolio/
├── airflow/
│   ├── dags/                    # Airflow DAGs
│   ├── plugins/                 # Custom operators
│   ├── scripts/                 # Utility scripts
│   └── requirements.txt         # Python dependencies
├── dbt_project/
│   ├── models/                  # dbt models
│   ├── tests/                   # dbt tests
│   └── dbt_project.yml         # dbt configuration
├── trino/
│   └── etc/                    # Trino configuration
├── docker-compose.yml          # Service definitions
└── README.md
```

## Data Model

The pipeline processes synthetic e-commerce data:

- **customers**: Customer information
- **orders**: Order transactions
- **products**: Product catalog
- **analytics**: Aggregated business metrics

## Configuration

Key environment variables in `.env`:

```bash
# Airflow
AIRFLOW_UID=50000
AIRFLOW_GID=0

# Trino
TRINO_HOST=trino-coordinator
TRINO_PORT=8080
TRINO_CATALOG=hive
TRINO_SCHEMA=default

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
```

## Development

1. **Add new DAGs**: Place in `airflow/dags/`
2. **Add custom operators**: Place in `airflow/plugins/operators/`
3. **Add dbt models**: Place in `dbt_project/models/`
4. **Update Trino config**: Modify files in `trino/etc/`

## Troubleshooting

- **Service health**: `make logs`
- **Reset everything**: `make down && make up`
- **Check Trino**: `make shell-trino`
- **Check MinIO**: Access console at http://localhost:9001

## Production Considerations

- Use external databases for Airflow metadata
- Configure proper authentication and authorization
- Set up monitoring and alerting
- Use secrets management for sensitive data
- Implement proper backup and disaster recovery

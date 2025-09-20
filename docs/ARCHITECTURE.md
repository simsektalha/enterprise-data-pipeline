# Enterprise Data Pipeline Architecture

## Overview

This document describes the architecture of the enterprise data pipeline, which demonstrates a modern data stack using Apache Airflow, Apache Spark, Apache Kafka, HDFS, YARN, Hive, Impala, Apache Iceberg, and Starburst Enterprise.

## High-Level Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Orchestration │    │   Data Storage  │
│                 │    │                 │    │                 │
│ • E-commerce    │───▶│   Apache        │───▶│      HDFS       │
│   Applications  │    │   Airflow       │    │   (Distributed  │
│ • Web Services  │    │   (DAGs)        │    │    Storage)     │
│ • IoT Devices   │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Streaming     │    │   Resource      │    │   Data Lake     │
│   Processing    │    │   Management    │    │   Format        │
│                 │    │                 │    │                 │
│ • Apache Kafka  │    │ • Apache YARN   │    │ • Apache Iceberg│
│ • Spark         │    │ • Job Scheduler │    │   (ACID)        │
│   Streaming     │    │ • Resource      │    │ • Parquet Files │
│                 │    │   Allocation    │    │ • Partitioning  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Batch         │    │   SQL Engines   │    │   Analytics     │
│   Processing    │    │                 │    │   Layer         │
│                 │    │ • Starburst     │    │                 │
│ • Apache Spark  │    │   Enterprise    │    │ • dbt Models    │
│ • YARN Jobs     │    │ • Apache Impala │    │ • Data Marts    │
│ • Data          │    │ • Hive Metastore│    │ • Business      │
│   Transformations│    │                 │    │   Intelligence  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Component Details

### 1. Data Ingestion Layer

#### Apache Kafka
- **Purpose**: Real-time data streaming
- **Configuration**: 
  - Topics: `orders`, `customers`, `products`
  - Partitions: 3 per topic
  - Replication Factor: 1 (development)
- **Port**: 9092
- **Health Check**: `kafka-broker-api-versions`

#### Zookeeper
- **Purpose**: Kafka coordination and metadata storage
- **Port**: 2181
- **Data Directory**: `/var/lib/zookeeper/data`

### 2. Storage Layer

#### HDFS (Hadoop Distributed File System)
- **Purpose**: Distributed file storage
- **Configuration**:
  - Namenode: `namenode:9000`
  - Datanode: Automatic data replication
  - Block Size: 128MB (default)
- **Web UI**: http://localhost:9870
- **Data Paths**:
  - `/data/streaming/` - Real-time data
  - `/data/processed/` - Batch processed data
  - `/data/checkpoint/` - Spark streaming checkpoints

#### MinIO (S3-Compatible Storage)
- **Purpose**: Object storage for Iceberg tables
- **Configuration**:
  - Access Key: `minioadmin`
  - Secret Key: `minioadmin`
  - Bucket: `data-lake`
- **Web UI**: http://localhost:9001

### 3. Resource Management

#### Apache YARN
- **Purpose**: Resource management and job scheduling
- **Configuration**:
  - ResourceManager: `resourcemanager:8088`
  - NodeManager: Automatic resource allocation
  - Memory: 1638MB per node
- **Web UI**: http://localhost:8088

### 4. Processing Engines

#### Apache Spark
- **Purpose**: Distributed data processing
- **Configuration**:
  - Master: `spark://spark-master:7077`
  - YARN Integration: `yarn` mode
  - Adaptive Query Execution: Enabled
- **Applications**:
  - `kafka_to_hdfs_streaming.py` - Real-time processing
  - `sample_spark_job.py` - Batch processing
- **Web UI**: http://localhost:8080

#### Apache Airflow
- **Purpose**: Workflow orchestration
- **Configuration**:
  - DAGs: `enterprise_data_pipeline_dag.py`
  - Task Groups: Data Generation, Ingestion, Processing, Storage, Analytics
  - Custom Operators: `StarburstOperator`, `SparkOperator`
- **Web UI**: http://localhost:8080
- **Credentials**: admin/admin

### 5. SQL Engines

#### Starburst Enterprise
- **Purpose**: High-performance SQL analytics
- **Configuration**:
  - Coordinator: `starburst-coordinator:8080`
  - Worker: `starburst-worker`
  - Catalogs: `iceberg`, `hive`
- **Features**:
  - Query result caching
  - Dynamic filtering
  - Join distribution strategies

#### Apache Impala
- **Purpose**: Interactive SQL queries
- **Configuration**:
  - Coordinator: `impala-coordinator:25000`
  - Catalog: `impala-catalog:25020`
  - StateStore: `impala-statestore:25010`
- **Features**:
  - MPP (Massively Parallel Processing)
  - In-memory processing
  - Real-time analytics

### 6. Data Lake Format

#### Apache Iceberg
- **Purpose**: ACID transactions and schema evolution
- **Features**:
  - Time travel queries
  - Schema evolution
  - Partition evolution
  - Snapshot management
- **Tables**:
  - `iceberg.default.orders`
  - `iceberg.default.customers`
  - `iceberg.default.products`
  - `iceberg.default.daily_revenue`
  - `iceberg.default.customer_cohorts`

### 7. Data Modeling

#### dbt (Data Build Tool)
- **Purpose**: Data transformations and modeling
- **Configuration**:
  - Adapter: `trino` (Starburst compatible)
  - Profile: `enterprise_data_pipeline`
  - Target: `dev`
- **Models**:
  - **Staging**: `stg_orders`, `stg_customers`, `stg_products`
  - **Marts**: `daily_revenue`, `customer_cohorts`
  - **Analytics**: Business intelligence tables

### 8. Metastore

#### Hive Metastore
- **Purpose**: Metadata management
- **Backend**: MySQL
- **Configuration**:
  - Host: `hive-metastore:9083`
  - Database: `hive_metastore`
  - User: `hive`
- **Features**:
  - Schema management
  - Table metadata
  - Partition information

## Data Flow

### 1. Real-time Processing
```
Kafka Topics → Spark Streaming → HDFS → Iceberg Tables → Starburst/Impala
```

### 2. Batch Processing
```
HDFS → Spark Batch → Iceberg Tables → dbt Models → Analytics
```

### 3. Query Processing
```
Starburst/Impala → Iceberg Tables → Results → Dashboards
```

## Security Considerations

### Network Security
- Internal Docker networking
- Port exposure for development only
- No external access by default

### Authentication
- Airflow: Basic authentication
- Starburst: No authentication (development)
- HDFS: No authentication (development)
- Kafka: No authentication (development)

### Data Security
- Data encryption in transit (TLS)
- Data encryption at rest (HDFS)
- Access control lists (ACLs)

## Monitoring and Observability

### Health Checks
- **HDFS**: Cluster health and storage
- **YARN**: Resource usage and job status
- **Starburst**: Query engine availability
- **Kafka**: Topic and consumer lag
- **Airflow**: DAG runs and task status

### Metrics
- Data processing throughput
- Query performance
- Resource utilization
- Error rates and retry counts

### Logging
- Centralized logging with Docker Compose
- Service-specific log aggregation
- Error tracking and alerting

## Performance Optimization

### Spark Optimization
- Adaptive Query Execution (AQE)
- Dynamic Partition Pruning
- Columnar Storage (Parquet)
- Compression (Snappy)

### Starburst Optimization
- Query result caching
- Dynamic filtering
- Join distribution strategies
- Memory management

### HDFS Optimization
- Replication factor tuning
- Block size optimization
- Compression codecs
- Namenode HA

## Scalability Considerations

### Horizontal Scaling
- Add more Kafka partitions
- Scale Spark workers
- Add HDFS datanodes
- Scale Starburst workers

### Vertical Scaling
- Increase memory allocation
- Optimize JVM settings
- Tune query execution parameters

## Disaster Recovery

### Data Backup
- HDFS data replication
- Iceberg table snapshots
- Airflow DAG backups
- Configuration backups

### Recovery Procedures
- Service restart procedures
- Data recovery from snapshots
- Configuration restoration
- Health check validation

## Development Workflow

### Local Development
1. Start services with `make up`
2. Develop and test locally
3. Use utility scripts for testing
4. Monitor with web UIs

### Testing
1. Unit tests for individual components
2. Integration tests for data flow
3. Data quality tests with dbt
4. Performance testing

### Deployment
1. Configuration management
2. Environment-specific settings
3. Service orchestration
4. Health monitoring

## Troubleshooting

### Common Issues
1. Port conflicts
2. Memory limitations
3. Permission issues
4. Service dependencies

### Debugging
1. Check service logs
2. Verify configuration
3. Test connectivity
4. Monitor resource usage

### Recovery
1. Restart services
2. Clear corrupted data
3. Restore from backups
4. Reconfigure settings

---

This architecture provides a robust, scalable foundation for enterprise data processing with modern tools and best practices.


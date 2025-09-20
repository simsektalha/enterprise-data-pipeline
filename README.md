# Enterprise Data Pipeline

A comprehensive, production-ready data pipeline showcasing modern enterprise data stack technologies including Apache Airflow, Apache Spark, Apache Kafka, HDFS, YARN, Hive, Impala, Apache Iceberg, and Starburst Enterprise.

## 🏗️ Architecture

This project demonstrates a complete enterprise data pipeline with the following components:

### Core Technologies
- **Orchestration**: Apache Airflow 2.8+
- **Streaming**: Apache Kafka 3.5+
- **Storage**: HDFS (Hadoop Distributed File System)
- **Resource Management**: YARN (Yet Another Resource Negotiator)
- **SQL Engine**: Starburst Enterprise (Trino-based)
- **Interactive Queries**: Apache Impala
- **Processing**: Apache Spark 3.4+ with YARN
- **Data Lake Format**: Apache Iceberg (ACID transactions)
- **Metastore**: Hive Metastore with MySQL backend
- **Transformations**: dbt-core 1.7+ with Starburst adapter
- **Containerization**: Docker Compose

### Data Flow
```
Kafka → Spark Streaming → HDFS → Spark Batch → Iceberg Tables → Starburst/Impala → dbt → Analytics
```

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.11+
- Git

### 1. Clone and Setup
```bash
git clone https://github.com/simsektalha/enterprise-data-pipeline.git
cd enterprise-data-pipeline
cp env.example .env
```

### 2. Start the Pipeline
```bash
# Start all services
make up

# Or start individual components
docker-compose up -d
```

### 3. Verify Services
```bash
# Check service status
make status

# View logs
make logs
```

### 4. Access Web UIs
- **Airflow**: http://localhost:8080 (admin/admin)
- **Starburst**: http://localhost:8080 (coordinator)
- **Impala**: http://localhost:25000
- **Spark Master**: http://localhost:8080
- **HDFS NameNode**: http://localhost:9870
- **YARN ResourceManager**: http://localhost:8088

## 📁 Project Structure

```
enterprise-data-pipeline/
├── airflow/                    # Airflow configuration and DAGs
│   ├── dags/                   # Airflow DAGs
│   ├── plugins/                # Custom operators and hooks
│   └── requirements.txt        # Python dependencies
├── dbt_project/                # dbt project for transformations
│   ├── models/                 # dbt models (staging, marts, analytics)
│   ├── macros/                 # dbt macros
│   ├── tests/                  # dbt tests
│   └── seeds/                  # dbt seed data
├── spark/                      # Spark configuration and applications
│   ├── conf/                   # Spark configuration
│   └── apps/                   # Spark applications
├── starburst/                  # Starburst Enterprise configuration
│   └── etc/                    # Starburst configuration files
├── impala/                     # Impala configuration
│   └── conf/                   # Impala configuration files
├── scripts/                    # Utility scripts
├── docker-compose.yml          # Docker Compose configuration
├── Makefile                    # Utility commands
└── README.md                   # This file
```

## 🔧 Configuration

### Environment Variables
Copy `env.example` to `.env` and configure:

```bash
# Airflow
AIRFLOW_UID=50000
AIRFLOW_GID=0

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# MySQL (Hive Metastore)
MYSQL_ROOT_PASSWORD=password
MYSQL_DATABASE=hive_metastore
MYSQL_USER=hive
MYSQL_PASSWORD=hive

# Hadoop
CORE_CONF_fs_defaultFS=hdfs://namenode:9000
CORE_CONF_hadoop_http_staticuser_user=root
CORE_CONF_hadoop_proxyuser_hue_hosts=*
CORE_CONF_hadoop_proxyuser_hue_groups=*
CORE_CONF_io_compression_codecs=org.apache.hadoop.io.compress.SnappyCodec
HDFS_CONF_dfs_webhdfs_enabled=true
HDFS_CONF_dfs_permissions_enabled=false
HDFS_CONF_dfs_nameservices=hdfscluster
HDFS_CONF_dfs_ha_namenodes_hdfscluster=nn1,nn2
HDFS_CONF_dfs_namenode_rpc_address_hdfscluster_nn1=namenode:9000
HDFS_CONF_dfs_namenode_rpc_address_hdfscluster_nn2=namenode2:9000
HDFS_CONF_dfs_namenode_http_address_hdfscluster_nn1=namenode:9870
HDFS_CONF_dfs_namenode_http_address_hdfscluster_nn2=namenode2:9870
HDFS_CONF_dfs_namenode_shared_edits_dir=qjournal://journal1:8485;journal2:8485;journal3:8485/hdfscluster
HDFS_CONF_dfs_client_failover_proxy_provider_hdfscluster=org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
HDFS_CONF_dfs_journalnode_edits_dir=/hadoop/dfs/journal
YARN_CONF_yarn_log___aggregation___enable=true
YARN_CONF_yarn_log_server_url=http://historyserver:8188/applicationhistory/logs/
YARN_CONF_yarn_resourcemanager_recovery_enabled=true
YARN_CONF_yarn_resourcemanager_store_class=org.apache.hadoop.yarn.server.resourcemanager.recovery.FileSystemRMStateStore
YARN_CONF_yarn_resourcemanager_hostname=resourcemanager
YARN_CONF_yarn_resourcemanager_address=resourcemanager:8032
YARN_CONF_yarn_resourcemanager_scheduler_address=resourcemanager:8030
YARN_CONF_yarn_resourcemanager_resource___tracker_address=resourcemanager:8031
YARN_CONF_yarn_timeline___service_enabled=true
YARN_CONF_yarn_timeline___service_generic___application___history_enabled=true
YARN_CONF_yarn_timeline___service_hostname=historyserver
YARN_CONF_mapreduce_map_output_compress=true
YARN_CONF_mapred_map_output_compress_codec=org.apache.hadoop.io.compress.SnappyCodec
YARN_CONF_yarn_nodemanager_resource_memory___mb=1638
YARN_CONF_yarn_scheduler_maximum___allocation___mb=1638
YARN_CONF_yarn_scheduler_minimum___allocation___mb=128
YARN_CONF_yarn_nodemanager_vmem___check___enabled=false
```

## 🎯 Features

### 1. Streaming Data Pipeline
- **Kafka Producers**: Generate synthetic e-commerce data
- **Spark Streaming**: Real-time data processing from Kafka to HDFS
- **Schema Evolution**: Handle changing data schemas gracefully

### 2. Batch Processing
- **Spark Batch Jobs**: Process historical data
- **YARN Integration**: Resource management and job scheduling
- **HDFS Storage**: Distributed file system for data persistence

### 3. Data Lake with ACID Transactions
- **Apache Iceberg**: ACID transactions and time travel
- **Schema Evolution**: Add/modify columns without breaking existing queries
- **Partitioning**: Date-based partitioning for optimal performance
- **Compaction**: Automatic file compaction and optimization

### 4. SQL Analytics
- **Starburst Enterprise**: High-performance SQL engine
- **Impala**: Interactive SQL queries
- **dbt Transformations**: Data modeling and transformations
- **Data Quality**: Automated testing and validation

### 5. Orchestration
- **Airflow DAGs**: Workflow orchestration and scheduling
- **Task Groups**: Organized pipeline stages
- **Health Checks**: Monitor cluster health
- **Error Handling**: Retry logic and failure notifications

## 🛠️ Usage

### Starting the Pipeline
```bash
# Start all services
make up

# Start specific services
docker-compose up -d kafka zookeeper
docker-compose up -d namenode datanode
docker-compose up -d resourcemanager nodemanager
docker-compose up -d starburst-coordinator starburst-worker
docker-compose up -d impala-coordinator impala-catalog impala-statestore
docker-compose up -d spark-master spark-worker
docker-compose up -d airflow
```

### Running Data Pipeline
```bash
# Generate sample data
python scripts/kafka_producer.py

# Setup Iceberg tables
make setup-iceberg

# Run dbt transformations
make dbt-run

# Run Airflow DAGs
# Access Airflow UI at http://localhost:8080
```

### Monitoring and Debugging
```bash
# View logs
make logs

# Check specific service logs
make logs-kafka
make logs-hdfs
make logs-yarn
make logs-starburst
make logs-impala
make logs-spark
make logs-airflow

# Connect to service shells
make shell-kafka
make shell-hdfs
make shell-yarn
make shell-starburst
make shell-impala
make shell-spark
```

## 📊 Data Models

### Staging Layer
- `stg_orders`: Cleaned and standardized order data
- `stg_customers`: Customer information with data quality checks
- `stg_products`: Product catalog with validation

### Marts Layer
- `daily_revenue`: Daily revenue metrics and KPIs
- `customer_cohorts`: Customer retention and cohort analysis

### Analytics Layer
- Business intelligence tables
- Aggregated metrics
- Performance dashboards

## 🧪 Testing

### Data Quality Tests
```bash
# Run dbt tests
make dbt-test

# Run specific tests
cd dbt_project && dbt test --select test_type:singular
```

### Integration Tests
```bash
# Test Kafka connectivity
python scripts/kafka_producer.py

# Test HDFS connectivity
python scripts/hdfs_utils.py

# Test Starburst connectivity
python scripts/starburst_utils.py
```

## 🔍 Monitoring

### Health Checks
- **HDFS**: Check cluster health and storage
- **YARN**: Monitor resource usage and job status
- **Starburst**: Verify query engine availability
- **Kafka**: Check topic and consumer lag
- **Airflow**: Monitor DAG runs and task status

### Metrics
- Data processing throughput
- Query performance
- Resource utilization
- Error rates and retry counts

## 🚨 Troubleshooting

### Common Issues

1. **Port Conflicts**
   ```bash
   # Check port usage
   netstat -tulpn | grep :8080
   
   # Stop conflicting services
   sudo systemctl stop apache2
   ```

2. **Memory Issues**
   ```bash
   # Increase Docker memory limit
   # In Docker Desktop: Settings > Resources > Memory
   ```

3. **Permission Issues**
   ```bash
   # Fix Airflow permissions
   sudo chown -R 50000:0 airflow/
   ```

4. **Service Dependencies**
   ```bash
   # Check service health
   docker-compose ps
   
   # Restart specific service
   docker-compose restart starburst-coordinator
   ```

### Logs
```bash
# View all logs
docker-compose logs

# View specific service logs
docker-compose logs starburst-coordinator
docker-compose logs airflow
```

## 📈 Performance Optimization

### Spark Configuration
- Adaptive query execution
- Dynamic partition pruning
- Columnar storage (Parquet)
- Compression (Snappy)

### Starburst Configuration
- Query result caching
- Dynamic filtering
- Join distribution strategies
- Memory management

### HDFS Configuration
- Replication factor
- Block size optimization
- Compression codecs
- Namenode HA

## 🔐 Security

### Authentication
- Airflow: Basic authentication
- Starburst: No authentication (development)
- HDFS: No authentication (development)
- Kafka: No authentication (development)

### Network Security
- Internal Docker networking
- Port exposure for development
- No external access by default

## 📚 Documentation

### API Documentation
- **Airflow**: http://localhost:8080/docs
- **Starburst**: http://localhost:8080/ui
- **Impala**: http://localhost:25000

### Data Documentation
- **dbt Docs**: `make dbt-docs`
- **Schema Documentation**: Generated automatically
- **Data Lineage**: Available in dbt docs

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Software Foundation
- Starburst Data
- dbt Labs
- Docker Inc.
- The open-source community

## 📞 Support

For questions and support:
- Create an issue in the repository
- Check the troubleshooting section
- Review the logs for error messages

---

**Note**: This is a demonstration project for learning and portfolio purposes. For production use, additional security, monitoring, and operational considerations should be implemented.
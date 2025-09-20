# Deployment Guide

This guide provides step-by-step instructions for deploying the enterprise data pipeline.

## Prerequisites

### System Requirements
- **OS**: Linux, macOS, or Windows with WSL2
- **RAM**: Minimum 8GB, Recommended 16GB+
- **CPU**: Minimum 4 cores, Recommended 8+ cores
- **Storage**: Minimum 50GB free space
- **Docker**: Version 20.10+
- **Docker Compose**: Version 2.0+

### Software Dependencies
- Docker Desktop or Docker Engine
- Git
- Python 3.11+ (for local development)
- Make (optional, for convenience commands)

## Installation Steps

### 1. Clone Repository
```bash
git clone https://github.com/simsektalha/enterprise-data-pipeline.git
cd enterprise-data-pipeline
```

### 2. Environment Configuration
```bash
# Copy environment template
cp env.example .env

# Edit environment variables
nano .env
```

### 3. Directory Setup
```bash
# Create necessary directories
mkdir -p airflow/logs airflow/plugins airflow/dags
mkdir -p spark/conf spark/apps spark/data
mkdir -p starburst/etc/catalog
mkdir -p impala/conf
mkdir -p dbt_project/models/staging
mkdir -p dbt_project/models/marts
mkdir -p dbt_project/models/analytics
mkdir -p dbt_project/tests
mkdir -p dbt_project/macros
mkdir -p dbt_project/seeds
```

### 4. Start Services
```bash
# Start all services
make up

# Or start individually
docker-compose up -d
```

### 5. Verify Installation
```bash
# Check service status
make status

# View logs
make logs
```

## Service Configuration

### Airflow Setup
```bash
# Initialize Airflow database
docker-compose exec airflow airflow db init

# Create admin user
docker-compose exec airflow airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Start Airflow webserver
docker-compose exec airflow airflow webserver --port 8080 &
```

### Starburst Setup
```bash
# Wait for Starburst to be ready
sleep 30

# Test connection
docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "SHOW CATALOGS"
```

### HDFS Setup
```bash
# Create HDFS directories
docker-compose exec namenode hdfs dfs -mkdir -p /data/streaming
docker-compose exec namenode hdfs dfs -mkdir -p /data/processed
docker-compose exec namenode hdfs dfs -mkdir -p /data/checkpoint

# Set permissions
docker-compose exec namenode hdfs dfs -chmod 777 /data
```

### Kafka Setup
```bash
# Create topics
docker-compose exec kafka kafka-topics --create --topic orders --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker-compose exec kafka kafka-topics --create --topic customers --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker-compose exec kafka kafka-topics --create --topic products --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# List topics
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

## Data Pipeline Setup

### 1. Setup Iceberg Tables
```bash
# Run Iceberg setup script
make setup-iceberg

# Or manually
python scripts/iceberg_utils.py
```

### 2. Generate Sample Data
```bash
# Generate and send data to Kafka
python scripts/kafka_producer.py

# Or use Airflow DAG
# Access Airflow UI and trigger the enterprise_data_pipeline DAG
```

### 3. Run dbt Transformations
```bash
# Install dbt dependencies
cd dbt_project && dbt deps

# Run dbt models
make dbt-run

# Run dbt tests
make dbt-test
```

## Monitoring Setup

### 1. Access Web UIs
- **Airflow**: http://localhost:8080 (admin/admin)
- **Starburst**: http://localhost:8080 (coordinator)
- **Impala**: http://localhost:25000
- **Spark Master**: http://localhost:8080
- **HDFS NameNode**: http://localhost:9870
- **YARN ResourceManager**: http://localhost:8088

### 2. Health Checks
```bash
# Check all services
make status

# Check specific services
make logs-kafka
make logs-hdfs
make logs-yarn
make logs-starburst
make logs-impala
make logs-spark
make logs-airflow
```

### 3. Data Validation
```bash
# Test Kafka connectivity
python scripts/kafka_producer.py

# Test HDFS connectivity
python scripts/hdfs_utils.py

# Test Starburst connectivity
python scripts/starburst_utils.py

# Test dbt connectivity
make dbt-debug
```

## Production Deployment

### 1. Environment Configuration
```bash
# Production environment variables
export ENVIRONMENT=production
export LOG_LEVEL=INFO
export MAX_MEMORY=8g
export MAX_CORES=4
```

### 2. Security Configuration
```bash
# Enable authentication
# Update docker-compose.yml with security settings
# Configure SSL/TLS certificates
# Set up firewall rules
```

### 3. Resource Allocation
```bash
# Update docker-compose.yml with production resource limits
# Configure memory and CPU limits
# Set up monitoring and alerting
```

### 4. Backup Configuration
```bash
# Set up automated backups
# Configure data retention policies
# Test recovery procedures
```

## Troubleshooting

### Common Issues

#### 1. Port Conflicts
```bash
# Check port usage
netstat -tulpn | grep :8080

# Stop conflicting services
sudo systemctl stop apache2
sudo systemctl stop nginx
```

#### 2. Memory Issues
```bash
# Check Docker memory limit
docker system info | grep -i memory

# Increase Docker memory limit
# In Docker Desktop: Settings > Resources > Memory
```

#### 3. Permission Issues
```bash
# Fix Airflow permissions
sudo chown -R 50000:0 airflow/

# Fix HDFS permissions
docker-compose exec namenode hdfs dfs -chmod 777 /data
```

#### 4. Service Dependencies
```bash
# Check service health
docker-compose ps

# Restart specific service
docker-compose restart starburst-coordinator

# Check service logs
docker-compose logs starburst-coordinator
```

### Debugging Commands

#### 1. Service Status
```bash
# Check all services
docker-compose ps

# Check specific service
docker-compose ps starburst-coordinator
```

#### 2. Log Analysis
```bash
# View all logs
docker-compose logs

# View specific service logs
docker-compose logs starburst-coordinator

# Follow logs in real-time
docker-compose logs -f starburst-coordinator
```

#### 3. Connectivity Tests
```bash
# Test Kafka
docker-compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Test HDFS
docker-compose exec namenode hdfs dfs -ls /

# Test Starburst
docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "SHOW CATALOGS"
```

## Maintenance

### 1. Regular Tasks
```bash
# Clean up old logs
make clean-logs

# Update dependencies
make update-deps

# Backup configuration
make backup-config
```

### 2. Performance Tuning
```bash
# Monitor resource usage
docker stats

# Optimize Spark configuration
# Update spark/conf/spark-defaults.conf

# Optimize Starburst configuration
# Update starburst/etc/config.properties
```

### 3. Data Management
```bash
# Clean up old data
docker-compose exec namenode hdfs dfs -rm -r /data/old

# Optimize Iceberg tables
python scripts/iceberg_utils.py optimize_table

# Vacuum old snapshots
python scripts/iceberg_utils.py vacuum_table
```

## Scaling

### 1. Horizontal Scaling
```bash
# Add more Kafka partitions
docker-compose exec kafka kafka-topics --alter --topic orders --partitions 6 --bootstrap-server localhost:9092

# Scale Spark workers
docker-compose up -d --scale spark-worker=3

# Scale Starburst workers
docker-compose up -d --scale starburst-worker=3
```

### 2. Vertical Scaling
```bash
# Increase memory allocation
# Update docker-compose.yml with higher memory limits

# Increase CPU allocation
# Update docker-compose.yml with higher CPU limits
```

## Backup and Recovery

### 1. Data Backup
```bash
# Backup HDFS data
docker-compose exec namenode hdfs dfs -get /data /backup/hdfs-data

# Backup Airflow DAGs
cp -r airflow/dags /backup/airflow-dags

# Backup configuration
cp -r . /backup/config
```

### 2. Recovery Procedures
```bash
# Restore HDFS data
docker-compose exec namenode hdfs dfs -put /backup/hdfs-data /data

# Restore Airflow DAGs
cp -r /backup/airflow-dags/* airflow/dags/

# Restore configuration
cp -r /backup/config/* ./
```

## Support

### 1. Documentation
- README.md - Project overview
- ARCHITECTURE.md - Technical architecture
- DEPLOYMENT.md - This deployment guide

### 2. Community Support
- GitHub Issues - Bug reports and feature requests
- Documentation - Comprehensive guides and examples
- Examples - Sample code and configurations

### 3. Professional Support
- Enterprise support available
- Custom configurations
- Performance optimization
- Training and consulting

---

This deployment guide provides comprehensive instructions for setting up and maintaining the enterprise data pipeline in various environments.


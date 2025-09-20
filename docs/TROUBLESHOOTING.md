# Troubleshooting Guide

This guide helps diagnose and resolve common issues with the enterprise data pipeline.

## Quick Diagnostics

### 1. Check Service Status
```bash
# Check all services
make status

# Check specific service
docker-compose ps starburst-coordinator
```

### 2. View Logs
```bash
# View all logs
make logs

# View specific service logs
make logs-kafka
make logs-hdfs
make logs-yarn
make logs-starburst
make logs-impala
make logs-spark
make logs-airflow
```

### 3. Test Connectivity
```bash
# Test Kafka
python scripts/kafka_producer.py

# Test HDFS
python scripts/hdfs_utils.py

# Test Starburst
python scripts/starburst_utils.py
```

## Common Issues and Solutions

### 1. Port Conflicts

#### Problem
```
Error: Port 8080 is already in use
```

#### Solution
```bash
# Check what's using the port
netstat -tulpn | grep :8080

# Stop conflicting service
sudo systemctl stop apache2
sudo systemctl stop nginx

# Or change port in docker-compose.yml
```

### 2. Memory Issues

#### Problem
```
Error: Container killed due to memory limit
```

#### Solution
```bash
# Check Docker memory limit
docker system info | grep -i memory

# Increase Docker memory limit
# In Docker Desktop: Settings > Resources > Memory > 8GB+

# Or reduce service memory usage
# Update docker-compose.yml with lower memory limits
```

### 3. Permission Issues

#### Problem
```
Error: Permission denied
```

#### Solution
```bash
# Fix Airflow permissions
sudo chown -R 50000:0 airflow/

# Fix HDFS permissions
docker-compose exec namenode hdfs dfs -chmod 777 /data

# Fix file permissions
chmod +x scripts/*.py
```

### 4. Service Dependencies

#### Problem
```
Error: Service dependency not ready
```

#### Solution
```bash
# Check service health
docker-compose ps

# Wait for dependencies
sleep 30

# Restart specific service
docker-compose restart starburst-coordinator

# Check service logs
docker-compose logs starburst-coordinator
```

### 5. Database Connection Issues

#### Problem
```
Error: Could not connect to database
```

#### Solution
```bash
# Check database status
docker-compose ps mysql

# Restart database
docker-compose restart mysql

# Wait for database to be ready
sleep 30

# Test connection
docker-compose exec mysql mysql -u root -p -e "SHOW DATABASES;"
```

### 6. HDFS Issues

#### Problem
```
Error: HDFS cluster not healthy
```

#### Solution
```bash
# Check HDFS status
docker-compose exec namenode hdfs dfsadmin -report

# Restart HDFS services
docker-compose restart namenode datanode

# Wait for HDFS to be ready
sleep 60

# Check HDFS health
python scripts/hdfs_utils.py
```

### 7. Kafka Issues

#### Problem
```
Error: Kafka broker not available
```

#### Solution
```bash
# Check Kafka status
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Restart Kafka services
docker-compose restart zookeeper kafka

# Wait for Kafka to be ready
sleep 30

# Test Kafka
python scripts/kafka_producer.py
```

### 8. Starburst Issues

#### Problem
```
Error: Starburst coordinator not responding
```

#### Solution
```bash
# Check Starburst status
docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "SHOW CATALOGS"

# Restart Starburst services
docker-compose restart starburst-coordinator starburst-worker

# Wait for Starburst to be ready
sleep 60

# Test Starburst
python scripts/starburst_utils.py
```

### 9. Spark Issues

#### Problem
```
Error: Spark job failed
```

#### Solution
```bash
# Check Spark status
docker-compose exec spark-master spark-shell --version

# Restart Spark services
docker-compose restart spark-master spark-worker

# Wait for Spark to be ready
sleep 30

# Test Spark
python scripts/spark_utils.py
```

### 10. Airflow Issues

#### Problem
```
Error: Airflow webserver not starting
```

#### Solution
```bash
# Check Airflow status
docker-compose exec airflow airflow webserver --port 8080

# Restart Airflow services
docker-compose restart airflow

# Wait for Airflow to be ready
sleep 60

# Check Airflow logs
docker-compose logs airflow
```

## Advanced Troubleshooting

### 1. Network Issues

#### Problem
```
Error: Service not reachable
```

#### Solution
```bash
# Check Docker network
docker network ls

# Inspect network
docker network inspect enterprise-data-pipeline_default

# Test connectivity between services
docker-compose exec starburst-coordinator ping hive-metastore
```

### 2. Resource Issues

#### Problem
```
Error: Out of memory
```

#### Solution
```bash
# Check resource usage
docker stats

# Check system resources
free -h
df -h

# Optimize resource allocation
# Update docker-compose.yml with appropriate limits
```

### 3. Configuration Issues

#### Problem
```
Error: Configuration not found
```

#### Solution
```bash
# Check configuration files
ls -la starburst/etc/
ls -la spark/conf/
ls -la impala/conf/

# Validate configuration
docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "SHOW CATALOGS"
```

### 4. Data Issues

#### Problem
```
Error: Data not found
```

#### Solution
```bash
# Check HDFS data
docker-compose exec namenode hdfs dfs -ls /data

# Check Iceberg tables
python scripts/iceberg_utils.py

# Check dbt models
make dbt-debug
```

## Performance Issues

### 1. Slow Queries

#### Problem
```
Query taking too long
```

#### Solution
```bash
# Check query execution plan
docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "EXPLAIN SELECT * FROM iceberg.default.orders"

# Optimize query
# Add indexes, partitions, or filters

# Check resource usage
docker stats
```

### 2. High Memory Usage

#### Problem
```
High memory consumption
```

#### Solution
```bash
# Check memory usage
docker stats

# Optimize memory settings
# Update spark/conf/spark-defaults.conf
# Update starburst/etc/config.properties

# Restart services
docker-compose restart
```

### 3. Slow Data Processing

#### Problem
```
Data processing is slow
```

#### Solution
```bash
# Check resource allocation
docker stats

# Optimize Spark configuration
# Update spark/conf/spark-defaults.conf

# Check HDFS performance
docker-compose exec namenode hdfs dfsadmin -report
```

## Log Analysis

### 1. Airflow Logs
```bash
# View Airflow logs
docker-compose logs airflow

# View specific task logs
docker-compose exec airflow airflow tasks log enterprise_data_pipeline data_generation 2024-01-01
```

### 2. Spark Logs
```bash
# View Spark logs
docker-compose logs spark-master spark-worker

# View Spark application logs
docker-compose exec spark-master spark-shell --version
```

### 3. Starburst Logs
```bash
# View Starburst logs
docker-compose logs starburst-coordinator starburst-worker

# View Starburst query logs
docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "SHOW CATALOGS"
```

### 4. HDFS Logs
```bash
# View HDFS logs
docker-compose logs namenode datanode

# View HDFS audit logs
docker-compose exec namenode hdfs dfsadmin -report
```

## Recovery Procedures

### 1. Service Recovery
```bash
# Restart all services
docker-compose restart

# Restart specific service
docker-compose restart starburst-coordinator

# Force restart
docker-compose down && docker-compose up -d
```

### 2. Data Recovery
```bash
# Restore from backup
cp -r /backup/hdfs-data /data

# Restore Iceberg tables
python scripts/iceberg_utils.py

# Restore dbt models
make dbt-run
```

### 3. Configuration Recovery
```bash
# Restore configuration
cp -r /backup/config/* ./

# Restart services
docker-compose restart
```

## Prevention

### 1. Regular Maintenance
```bash
# Clean up old logs
make clean-logs

# Update dependencies
make update-deps

# Backup configuration
make backup-config
```

### 2. Monitoring
```bash
# Set up monitoring
# Monitor resource usage
# Monitor service health
# Monitor data quality
```

### 3. Testing
```bash
# Run tests regularly
make dbt-test

# Test connectivity
python scripts/kafka_producer.py

# Test data quality
python scripts/iceberg_utils.py
```

## Getting Help

### 1. Check Documentation
- README.md - Project overview
- ARCHITECTURE.md - Technical architecture
- DEPLOYMENT.md - Deployment guide

### 2. Check Logs
```bash
# View all logs
make logs

# View specific service logs
make logs-starburst
```

### 3. Check Status
```bash
# Check service status
make status

# Check connectivity
python scripts/kafka_producer.py
```

### 4. Community Support
- GitHub Issues - Bug reports and feature requests
- Documentation - Comprehensive guides and examples
- Examples - Sample code and configurations

---

This troubleshooting guide provides comprehensive solutions for common issues and helps maintain a healthy data pipeline.


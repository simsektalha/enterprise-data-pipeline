.PHONY: help up down logs shell-trino shell-minio clean

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start all services
	docker-compose up -d

down: ## Stop all services
	docker-compose down

logs: ## Show logs for all services
	docker-compose logs -f

logs-airflow: ## Show Airflow logs
	docker-compose logs -f airflow-webserver airflow-scheduler

logs-trino: ## Show Trino logs
	docker-compose logs -f trino-coordinator trino-worker

logs-minio: ## Show MinIO logs
	docker-compose logs -f minio

logs-kafka: ## Show Kafka logs
	docker-compose logs -f kafka zookeeper

logs-hdfs: ## Show HDFS logs
	docker-compose logs -f namenode datanode

logs-yarn: ## Show YARN logs
	docker-compose logs -f resourcemanager nodemanager

logs-starburst: ## Show Starburst logs
	docker-compose logs -f starburst-coordinator starburst-worker

logs-impala: ## Show Impala logs
	docker-compose logs -f impala-coordinator impala-catalog impala-statestore

logs-spark: ## Show Spark logs
	docker-compose logs -f spark-master spark-worker

shell-starburst: ## Connect to Starburst CLI
	docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080

shell-minio: ## Connect to MinIO CLI
	docker-compose exec minio mc alias set myminio http://minio:9000 minioadmin minioadmin

shell-kafka: ## Connect to Kafka CLI
	docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning

shell-hdfs: ## Connect to HDFS CLI
	docker-compose exec namenode hdfs dfs -ls /

shell-yarn: ## Connect to YARN CLI
	docker-compose exec resourcemanager yarn node -list

shell-impala: ## Connect to Impala CLI
	docker-compose exec impala-coordinator impala-shell

shell-spark: ## Connect to Spark CLI
	docker-compose exec spark-master spark-shell

setup-iceberg: ## Setup Iceberg tables
	python scripts/iceberg_utils.py

clean: ## Clean up everything
	docker-compose down -v
	docker system prune -f

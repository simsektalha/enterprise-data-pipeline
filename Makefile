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

shell-trino: ## Connect to Trino CLI
	docker-compose exec trino-coordinator trino --server trino-coordinator:8080

shell-minio: ## Connect to MinIO CLI
	docker-compose exec minio mc alias set myminio http://minio:9000 minioadmin minioadmin

shell-kafka: ## Connect to Kafka CLI
	docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning

clean: ## Clean up everything
	docker-compose down -v
	docker system prune -f

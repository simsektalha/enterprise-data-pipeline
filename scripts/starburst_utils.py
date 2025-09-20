#!/usr/bin/env python3
"""
Starburst utility functions for query execution and management
"""

import subprocess
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_starburst_query(query):
    """Run Starburst query via docker exec"""
    full_command = f'docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "{query}"'
    try:
        result = subprocess.run(full_command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"Starburst query successful: {query}")
            return result.stdout
        else:
            logger.error(f"Starburst query failed: {query}")
            logger.error(f"Error: {result.stderr}")
            return None
    except Exception as e:
        logger.error(f"Exception running Starburst query: {e}")
        return None

def show_catalogs():
    """Show available catalogs"""
    logger.info("Showing Starburst catalogs...")
    return run_starburst_query("SHOW CATALOGS")

def show_schemas(catalog="hive"):
    """Show schemas in a catalog"""
    logger.info(f"Showing schemas in catalog: {catalog}")
    return run_starburst_query(f"SHOW SCHEMAS FROM {catalog}")

def show_tables(catalog="hive", schema="default"):
    """Show tables in a schema"""
    logger.info(f"Showing tables in {catalog}.{schema}")
    return run_starburst_query(f"SHOW TABLES FROM {catalog}.{schema}")

def describe_table(catalog="hive", schema="default", table="table_name"):
    """Describe a table structure"""
    logger.info(f"Describing table {catalog}.{schema}.{table}")
    return run_starburst_query(f"DESCRIBE {catalog}.{schema}.{table}")

def create_database(catalog="hive", database="test_db"):
    """Create a database"""
    logger.info(f"Creating database {catalog}.{database}")
    return run_starburst_query(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{database}")

def create_iceberg_table(catalog="iceberg", schema="default", table="test_table"):
    """Create an Iceberg table"""
    logger.info(f"Creating Iceberg table {catalog}.{schema}.{table}")
    query = f"""
    CREATE TABLE {catalog}.{schema}.{table} (
        id BIGINT,
        name VARCHAR(100),
        created_at TIMESTAMP
    ) WITH (
        format = 'PARQUET',
        location = 's3://test-bucket/{table}'
    )
    """
    return run_starburst_query(query)

def check_starburst_health():
    """Check Starburst cluster health"""
    logger.info("Checking Starburst cluster health...")
    
    # Check if coordinator is accessible
    catalogs_output = show_catalogs()
    if catalogs_output:
        logger.info("Starburst coordinator is accessible")
        return True
    else:
        logger.error("Starburst coordinator is not accessible")
        return False

def get_query_stats():
    """Get query statistics"""
    logger.info("Getting query statistics...")
    return run_starburst_query("SELECT * FROM system.runtime.queries")

def get_cluster_info():
    """Get cluster information"""
    logger.info("Getting cluster information...")
    return run_starburst_query("SELECT * FROM system.runtime.nodes")

def main():
    """Main function to demonstrate Starburst operations"""
    logger.info("Starting Starburst operations...")
    
    # Check cluster health
    if check_starburst_health():
        logger.info("Starburst cluster is healthy")
    else:
        logger.error("Starburst cluster has issues")
    
    # Show cluster information
    show_catalogs()
    get_cluster_info()

if __name__ == "__main__":
    main()




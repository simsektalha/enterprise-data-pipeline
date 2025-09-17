#!/usr/bin/env python3
"""
Impala utility functions for interactive query execution
"""

import subprocess
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_impala_query(query):
    """Run Impala query via docker exec"""
    full_command = f'docker-compose exec impala-coordinator impala-shell -q "{query}"'
    try:
        result = subprocess.run(full_command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"Impala query successful: {query}")
            return result.stdout
        else:
            logger.error(f"Impala query failed: {query}")
            logger.error(f"Error: {result.stderr}")
            return None
    except Exception as e:
        logger.error(f"Exception running Impala query: {e}")
        return None

def show_databases():
    """Show available databases"""
    logger.info("Showing Impala databases...")
    return run_impala_query("SHOW DATABASES")

def show_tables(database="default"):
    """Show tables in a database"""
    logger.info(f"Showing tables in database: {database}")
    return run_impala_query(f"SHOW TABLES IN {database}")

def describe_table(database="default", table="table_name"):
    """Describe a table structure"""
    logger.info(f"Describing table {database}.{table}")
    return run_impala_query(f"DESCRIBE {database}.{table}")

def create_database(database="test_db"):
    """Create a database"""
    logger.info(f"Creating database: {database}")
    return run_impala_query(f"CREATE DATABASE IF NOT EXISTS {database}")

def create_table(database="default", table="test_table"):
    """Create a simple table"""
    logger.info(f"Creating table {database}.{table}")
    query = f"""
    CREATE TABLE {database}.{table} (
        id BIGINT,
        name STRING,
        created_at TIMESTAMP
    ) STORED AS PARQUET
    """
    return run_impala_query(query)

def insert_sample_data(database="default", table="test_table"):
    """Insert sample data into a table"""
    logger.info(f"Inserting sample data into {database}.{table}")
    query = f"""
    INSERT INTO {database}.{table} VALUES 
    (1, 'test_record_1', NOW()),
    (2, 'test_record_2', NOW()),
    (3, 'test_record_3', NOW())
    """
    return run_impala_query(query)

def check_impala_health():
    """Check Impala cluster health"""
    logger.info("Checking Impala cluster health...")
    
    # Check if coordinator is accessible
    databases_output = show_databases()
    if databases_output:
        logger.info("Impala coordinator is accessible")
        return True
    else:
        logger.error("Impala coordinator is not accessible")
        return False

def get_query_profile(query_id):
    """Get query profile for a specific query"""
    logger.info(f"Getting query profile for: {query_id}")
    return run_impala_query(f"PROFILE {query_id}")

def invalidate_metadata(database="default", table=None):
    """Invalidate metadata for a table or database"""
    if table:
        logger.info(f"Invalidating metadata for {database}.{table}")
        return run_impala_query(f"INVALIDATE METADATA {database}.{table}")
    else:
        logger.info(f"Invalidating metadata for database: {database}")
        return run_impala_query(f"INVALIDATE METADATA {database}")

def compute_stats(database="default", table="table_name"):
    """Compute statistics for a table"""
    logger.info(f"Computing stats for {database}.{table}")
    return run_impala_query(f"COMPUTE STATS {database}.{table}")

def main():
    """Main function to demonstrate Impala operations"""
    logger.info("Starting Impala operations...")
    
    # Check cluster health
    if check_impala_health():
        logger.info("Impala cluster is healthy")
    else:
        logger.error("Impala cluster has issues")
    
    # Show cluster information
    show_databases()

if __name__ == "__main__":
    main()

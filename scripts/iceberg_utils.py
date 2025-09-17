#!/usr/bin/env python3
"""
Iceberg utility functions for table management and ACID operations
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

def setup_iceberg_tables():
    """Setup Iceberg tables by running the SQL script"""
    logger.info("Setting up Iceberg tables...")
    
    # Read and execute the SQL script
    with open('scripts/setup_iceberg_tables.sql', 'r') as f:
        sql_script = f.read()
    
    # Split by semicolon and execute each statement
    statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
    
    for statement in statements:
        if statement:
            logger.info(f"Executing: {statement[:50]}...")
            result = run_starburst_query(statement)
            if result:
                logger.info("Statement executed successfully")
            else:
                logger.error("Statement failed")

def show_iceberg_tables():
    """Show all Iceberg tables"""
    logger.info("Showing Iceberg tables...")
    return run_starburst_query("SHOW TABLES FROM iceberg.default")

def describe_iceberg_table(table_name):
    """Describe an Iceberg table structure"""
    logger.info(f"Describing Iceberg table: {table_name}")
    return run_starburst_query(f"DESCRIBE iceberg.default.{table_name}")

def get_table_history(table_name):
    """Get table history for an Iceberg table"""
    logger.info(f"Getting history for table: {table_name}")
    return run_starburst_query(f"SELECT * FROM iceberg.default.\"{table_name}$history\"")

def get_table_snapshots(table_name):
    """Get snapshots for an Iceberg table"""
    logger.info(f"Getting snapshots for table: {table_name}")
    return run_starburst_query(f"SELECT * FROM iceberg.default.\"{table_name}$snapshots\"")

def create_time_travel_query(table_name, snapshot_id):
    """Create a time travel query for a specific snapshot"""
    logger.info(f"Creating time travel query for {table_name} at snapshot {snapshot_id}")
    query = f"SELECT * FROM iceberg.default.\"{table_name}@snapshot_id={snapshot_id}\""
    return run_starburst_query(query)

def perform_upsert(table_name, values):
    """Perform an upsert operation on an Iceberg table"""
    logger.info(f"Performing upsert on table: {table_name}")
    
    # Create a temporary table with new data
    temp_table = f"temp_{table_name}_{int(time.time())}"
    
    # Insert new data
    insert_query = f"""
    INSERT INTO iceberg.default.{table_name} VALUES {values}
    """
    
    return run_starburst_query(insert_query)

def optimize_table(table_name):
    """Optimize an Iceberg table (compact small files)"""
    logger.info(f"Optimizing table: {table_name}")
    return run_starburst_query(f"CALL iceberg.system.rewrite_data_files('iceberg.default.{table_name}')")

def vacuum_table(table_name, older_than_hours=24):
    """Vacuum an Iceberg table (remove old snapshots)"""
    logger.info(f"Vacuuming table: {table_name}")
    return run_starburst_query(f"CALL iceberg.system.expire_snapshots('iceberg.default.{table_name}', TIMESTAMP '2024-01-01 00:00:00')")

def get_table_metrics(table_name):
    """Get table metrics and statistics"""
    logger.info(f"Getting metrics for table: {table_name}")
    
    # Get table size
    size_query = f"SELECT COUNT(*) as row_count FROM iceberg.default.{table_name}"
    row_count = run_starburst_query(size_query)
    
    # Get table history
    history = get_table_history(table_name)
    
    # Get snapshots
    snapshots = get_table_snapshots(table_name)
    
    return {
        'row_count': row_count,
        'history': history,
        'snapshots': snapshots
    }

def demonstrate_acid_operations():
    """Demonstrate ACID operations with Iceberg"""
    logger.info("Demonstrating ACID operations...")
    
    # 1. Insert data
    logger.info("1. Inserting data...")
    insert_query = """
    INSERT INTO iceberg.default.orders VALUES
    (6, 104, 1005, 1, 299.99, DATE '2024-01-04', TIMESTAMP '2024-01-04 10:00:00', TIMESTAMP '2024-01-04 10:00:00')
    """
    run_starburst_query(insert_query)
    
    # 2. Update data
    logger.info("2. Updating data...")
    update_query = """
    UPDATE iceberg.default.orders 
    SET price = 249.99, updated_at = TIMESTAMP '2024-01-04 11:00:00'
    WHERE order_id = 6
    """
    run_starburst_query(update_query)
    
    # 3. Delete data
    logger.info("3. Deleting data...")
    delete_query = "DELETE FROM iceberg.default.orders WHERE order_id = 6"
    run_starburst_query(delete_query)
    
    # 4. Show table history
    logger.info("4. Showing table history...")
    get_table_history('orders')

def main():
    """Main function to demonstrate Iceberg operations"""
    logger.info("Starting Iceberg operations...")
    
    # Setup tables
    setup_iceberg_tables()
    
    # Show tables
    show_iceberg_tables()
    
    # Demonstrate ACID operations
    demonstrate_acid_operations()
    
    # Get table metrics
    get_table_metrics('orders')

if __name__ == "__main__":
    import time
    main()

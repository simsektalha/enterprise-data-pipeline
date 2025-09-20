#!/usr/bin/env python3
"""
Spark utility functions for job management and monitoring
"""

import subprocess
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_spark_command(command):
    """Run Spark command via docker exec"""
    full_command = f"docker-compose exec spark-master {command}"
    try:
        result = subprocess.run(full_command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"Spark command successful: {command}")
            return result.stdout
        else:
            logger.error(f"Spark command failed: {command}")
            logger.error(f"Error: {result.stderr}")
            return None
    except Exception as e:
        logger.error(f"Exception running Spark command: {e}")
        return None

def submit_spark_job(job_path, main_class=None, args=""):
    """Submit a Spark job"""
    logger.info(f"Submitting Spark job: {job_path}")
    
    if main_class:
        command = f"spark-submit --class {main_class} {job_path}"
    else:
        command = f"spark-submit {job_path}"
    
    if args:
        command += f" {args}"
    
    return run_spark_command(command)

def list_spark_applications():
    """List running Spark applications"""
    logger.info("Listing Spark applications...")
    return run_spark_command("yarn application -list")

def get_spark_application_status(app_id):
    """Get status of specific Spark application"""
    logger.info(f"Getting status for application: {app_id}")
    return run_spark_command(f"yarn application -status {app_id}")

def kill_spark_application(app_id):
    """Kill a Spark application"""
    logger.info(f"Killing application: {app_id}")
    return run_spark_command(f"yarn application -kill {app_id}")

def get_spark_history():
    """Get Spark history server information"""
    logger.info("Getting Spark history...")
    return run_spark_command("spark-history-server")

def check_spark_health():
    """Check Spark cluster health"""
    logger.info("Checking Spark cluster health...")
    
    # Check if master is accessible
    master_status = run_spark_command("spark-submit --version")
    if master_status:
        logger.info("Spark master is accessible")
        return True
    else:
        logger.error("Spark master is not accessible")
        return False

def run_spark_sql(query):
    """Run Spark SQL query"""
    logger.info(f"Running Spark SQL: {query}")
    return run_spark_command(f'spark-sql -e "{query}"')

def create_spark_session_info():
    """Get Spark session information"""
    logger.info("Getting Spark session information...")
    return run_spark_command("spark-shell --version")

def get_spark_metrics():
    """Get Spark cluster metrics"""
    logger.info("Getting Spark cluster metrics...")
    return run_spark_command("yarn node -list")

def run_pyspark_script(script_path):
    """Run a PySpark script"""
    logger.info(f"Running PySpark script: {script_path}")
    return run_spark_command(f"spark-submit {script_path}")

def main():
    """Main function to demonstrate Spark operations"""
    logger.info("Starting Spark operations...")
    
    # Check cluster health
    if check_spark_health():
        logger.info("Spark cluster is healthy")
    else:
        logger.error("Spark cluster has issues")
    
    # Get cluster information
    get_spark_metrics()
    list_spark_applications()

if __name__ == "__main__":
    main()




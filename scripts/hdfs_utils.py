#!/usr/bin/env python3
"""
HDFS utility functions for data pipeline
"""

import os
import subprocess
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_hdfs_command(command):
    """Run HDFS command via docker exec"""
    full_command = f"docker-compose exec namenode hdfs dfs {command}"
    try:
        result = subprocess.run(full_command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"HDFS command successful: {command}")
            return result.stdout
        else:
            logger.error(f"HDFS command failed: {command}")
            logger.error(f"Error: {result.stderr}")
            return None
    except Exception as e:
        logger.error(f"Exception running HDFS command: {e}")
        return None

def create_hdfs_directories():
    """Create necessary HDFS directories for the pipeline"""
    directories = [
        "/data",
        "/data/raw",
        "/data/processed",
        "/data/analytics",
        "/data/landing",
        "/data/staging"
    ]
    
    for directory in directories:
        logger.info(f"Creating HDFS directory: {directory}")
        run_hdfs_command(f"-mkdir -p {directory}")

def list_hdfs_directory(path="/"):
    """List contents of HDFS directory"""
    logger.info(f"Listing HDFS directory: {path}")
    return run_hdfs_command(f"-ls {path}")

def upload_to_hdfs(local_path, hdfs_path):
    """Upload local file to HDFS"""
    logger.info(f"Uploading {local_path} to {hdfs_path}")
    return run_hdfs_command(f"-put {local_path} {hdfs_path}")

def download_from_hdfs(hdfs_path, local_path):
    """Download file from HDFS to local"""
    logger.info(f"Downloading {hdfs_path} to {local_path}")
    return run_hdfs_command(f"-get {hdfs_path} {local_path}")

def check_hdfs_health():
    """Check HDFS cluster health"""
    logger.info("Checking HDFS cluster health...")
    return run_hdfs_command("dfsadmin -report")

def main():
    """Main function to demonstrate HDFS operations"""
    logger.info("Starting HDFS operations...")
    
    # Create directories
    create_hdfs_directories()
    
    # List root directory
    list_hdfs_directory()
    
    # Check cluster health
    check_hdfs_health()

if __name__ == "__main__":
    main()


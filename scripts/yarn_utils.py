#!/usr/bin/env python3
"""
YARN utility functions for resource management
"""

import subprocess
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_yarn_command(command):
    """Run YARN command via docker exec"""
    full_command = f"docker-compose exec resourcemanager yarn {command}"
    try:
        result = subprocess.run(full_command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"YARN command successful: {command}")
            return result.stdout
        else:
            logger.error(f"YARN command failed: {command}")
            logger.error(f"Error: {result.stderr}")
            return None
    except Exception as e:
        logger.error(f"Exception running YARN command: {e}")
        return None

def get_yarn_nodes():
    """Get information about YARN nodes"""
    logger.info("Getting YARN nodes information...")
    return run_yarn_command("node -list")

def get_yarn_applications():
    """Get information about running YARN applications"""
    logger.info("Getting YARN applications...")
    return run_yarn_command("application -list")

def get_yarn_application_status(app_id):
    """Get status of specific YARN application"""
    logger.info(f"Getting status for application: {app_id}")
    return run_yarn_command(f"application -status {app_id}")

def kill_yarn_application(app_id):
    """Kill a YARN application"""
    logger.info(f"Killing application: {app_id}")
    return run_yarn_command(f"application -kill {app_id}")

def get_yarn_queue_info():
    """Get YARN queue information"""
    logger.info("Getting YARN queue information...")
    return run_yarn_command("queue -status")

def get_yarn_cluster_metrics():
    """Get YARN cluster metrics"""
    logger.info("Getting YARN cluster metrics...")
    return run_yarn_command("node -list -showDetails")

def submit_yarn_job(jar_path, main_class, args=""):
    """Submit a YARN job"""
    logger.info(f"Submitting YARN job: {main_class}")
    command = f"jar {jar_path} {main_class}"
    if args:
        command += f" {args}"
    return run_yarn_command(command)

def check_yarn_health():
    """Check YARN cluster health"""
    logger.info("Checking YARN cluster health...")
    
    # Check nodes
    nodes_output = get_yarn_nodes()
    if nodes_output:
        logger.info("YARN nodes are accessible")
    else:
        logger.error("YARN nodes are not accessible")
    
    # Check applications
    apps_output = get_yarn_applications()
    if apps_output:
        logger.info("YARN applications can be listed")
    else:
        logger.error("Cannot list YARN applications")
    
    return nodes_output and apps_output

def main():
    """Main function to demonstrate YARN operations"""
    logger.info("Starting YARN operations...")
    
    # Check cluster health
    if check_yarn_health():
        logger.info("YARN cluster is healthy")
    else:
        logger.error("YARN cluster has issues")
    
    # Get cluster information
    get_yarn_nodes()
    get_yarn_applications()
    get_yarn_queue_info()

if __name__ == "__main__":
    main()

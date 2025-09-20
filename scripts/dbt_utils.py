#!/usr/bin/env python3
"""
dbt utility functions for the enterprise data pipeline
"""

import subprocess
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_dbt_command(command, project_dir="dbt_project"):
    """Run dbt command in the project directory"""
    full_command = f"cd {project_dir} && dbt {command}"
    try:
        result = subprocess.run(full_command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"dbt command successful: {command}")
            return result.stdout
        else:
            logger.error(f"dbt command failed: {command}")
            logger.error(f"Error: {result.stderr}")
            return None
    except Exception as e:
        logger.error(f"Exception running dbt command: {e}")
        return None

def dbt_debug():
    """Run dbt debug to check configuration"""
    logger.info("Running dbt debug...")
    return run_dbt_command("debug")

def dbt_deps():
    """Install dbt dependencies"""
    logger.info("Installing dbt dependencies...")
    return run_dbt_command("deps")

def dbt_seed():
    """Run dbt seed to load seed data"""
    logger.info("Running dbt seed...")
    return run_dbt_command("seed")

def dbt_run():
    """Run dbt models"""
    logger.info("Running dbt models...")
    return run_dbt_command("run")

def dbt_test():
    """Run dbt tests"""
    logger.info("Running dbt tests...")
    return run_dbt_command("test")

def dbt_compile():
    """Compile dbt models"""
    logger.info("Compiling dbt models...")
    return run_dbt_command("compile")

def dbt_docs_generate():
    """Generate dbt documentation"""
    logger.info("Generating dbt documentation...")
    return run_dbt_command("docs generate")

def dbt_docs_serve():
    """Serve dbt documentation"""
    logger.info("Serving dbt documentation...")
    return run_dbt_command("docs serve --port 8080")

def dbt_snapshot():
    """Run dbt snapshots"""
    logger.info("Running dbt snapshots...")
    return run_dbt_command("snapshot")

def dbt_freshness():
    """Check data freshness"""
    logger.info("Checking data freshness...")
    return run_dbt_command("source freshness")

def dbt_parse():
    """Parse dbt project"""
    logger.info("Parsing dbt project...")
    return run_dbt_command("parse")

def dbt_list():
    """List dbt resources"""
    logger.info("Listing dbt resources...")
    return run_dbt_command("list")

def dbt_show():
    """Show dbt model results"""
    logger.info("Showing dbt model results...")
    return run_dbt_command("show --select stg_orders")

def run_full_pipeline():
    """Run the full dbt pipeline"""
    logger.info("Running full dbt pipeline...")
    
    # Debug
    if not dbt_debug():
        logger.error("dbt debug failed")
        return False
    
    # Install dependencies
    if not dbt_deps():
        logger.error("dbt deps failed")
        return False
    
    # Parse project
    if not dbt_parse():
        logger.error("dbt parse failed")
        return False
    
    # Load seeds
    if not dbt_seed():
        logger.error("dbt seed failed")
        return False
    
    # Run models
    if not dbt_run():
        logger.error("dbt run failed")
        return False
    
    # Run tests
    if not dbt_test():
        logger.error("dbt test failed")
        return False
    
    # Check freshness
    if not dbt_freshness():
        logger.error("dbt freshness check failed")
        return False
    
    logger.info("Full dbt pipeline completed successfully")
    return True

def main():
    """Main function to demonstrate dbt operations"""
    logger.info("Starting dbt operations...")
    
    # Run full pipeline
    if run_full_pipeline():
        logger.info("dbt pipeline completed successfully")
    else:
        logger.error("dbt pipeline failed")

if __name__ == "__main__":
    main()




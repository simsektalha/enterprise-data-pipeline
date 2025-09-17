#!/usr/bin/env python3
"""
Spark Streaming job to consume data from Kafka and write to HDFS
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
from pyspark.sql.streaming import StreamingQuery
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session for streaming"""
    return SparkSession.builder \
        .appName("KafkaToHdfsStreaming") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation.enabled", "true") \
        .getOrCreate()

def define_schemas():
    """Define schemas for different data types"""
    
    # Orders schema
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("status", StringType(), True)
    ])
    
    # Customers schema
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("registration_date", TimestampType(), True),
        StructField("country", StringType(), True)
    ])
    
    return orders_schema, customers_schema

def process_orders_stream(spark, kafka_servers, hdfs_path, checkpoint_location):
    """Process orders stream from Kafka to HDFS"""
    logger.info("Processing orders stream...")
    
    orders_schema, _ = define_schemas()
    
    # Read from Kafka
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "orders") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    parsed_orders = orders_df \
        .select(from_json(col("value").cast("string"), orders_schema).alias("data")) \
        .select("data.*") \
        .withColumn("processed_at", current_timestamp())
    
    # Write to HDFS
    orders_query = parsed_orders \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"{hdfs_path}/orders") \
        .option("checkpointLocation", f"{checkpoint_location}/orders") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return orders_query

def process_customers_stream(spark, kafka_servers, hdfs_path, checkpoint_location):
    """Process customers stream from Kafka to HDFS"""
    logger.info("Processing customers stream...")
    
    _, customers_schema = define_schemas()
    
    # Read from Kafka
    customers_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", "customers") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON data
    parsed_customers = customers_df \
        .select(from_json(col("value").cast("string"), customers_schema).alias("data")) \
        .select("data.*") \
        .withColumn("processed_at", current_timestamp())
    
    # Write to HDFS
    customers_query = parsed_customers \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"{hdfs_path}/customers") \
        .option("checkpointLocation", f"{checkpoint_location}/customers") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    return customers_query

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Kafka to HDFS Streaming Job')
    parser.add_argument('--kafka-servers', required=True, help='Kafka bootstrap servers')
    parser.add_argument('--hdfs-path', required=True, help='HDFS output path')
    parser.add_argument('--checkpoint-location', required=True, help='Checkpoint location')
    
    args = parser.parse_args()
    
    logger.info("Starting Kafka to HDFS streaming job...")
    logger.info(f"Kafka servers: {args.kafka_servers}")
    logger.info(f"HDFS path: {args.hdfs_path}")
    logger.info(f"Checkpoint location: {args.checkpoint_location}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Process orders stream
        orders_query = process_orders_stream(
            spark, 
            args.kafka_servers, 
            args.hdfs_path, 
            args.checkpoint_location
        )
        
        # Process customers stream
        customers_query = process_customers_stream(
            spark, 
            args.kafka_servers, 
            args.hdfs_path, 
            args.checkpoint_location
        )
        
        logger.info("Streaming queries started. Waiting for termination...")
        
        # Wait for termination
        orders_query.awaitTermination()
        customers_query.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("Stopping streaming queries...")
        orders_query.stop()
        customers_query.stop()
    except Exception as e:
        logger.error(f"Error in streaming job: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

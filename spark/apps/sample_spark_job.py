#!/usr/bin/env python3
"""
Sample Spark job for data processing
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as spark_sum, avg, max as spark_max
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("SampleDataProcessingJob") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def process_orders_data(spark):
    """Process orders data"""
    logger.info("Processing orders data...")
    
    # Create sample data
    orders_data = [
        (1, "customer_1", "product_a", 100.0, "2024-01-01"),
        (2, "customer_2", "product_b", 200.0, "2024-01-01"),
        (3, "customer_1", "product_c", 150.0, "2024-01-02"),
        (4, "customer_3", "product_a", 120.0, "2024-01-02"),
        (5, "customer_2", "product_b", 180.0, "2024-01-03"),
    ]
    
    orders_df = spark.createDataFrame(orders_data, 
                                    ["order_id", "customer_id", "product_id", "amount", "order_date"])
    
    # Register as temporary view
    orders_df.createOrReplaceTempView("orders")
    
    # Perform aggregations
    daily_revenue = spark.sql("""
        SELECT 
            order_date,
            COUNT(*) as order_count,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_order_value,
            MAX(amount) as max_order_value
        FROM orders 
        GROUP BY order_date
        ORDER BY order_date
    """)
    
    # Show results
    logger.info("Daily revenue summary:")
    daily_revenue.show()
    
    # Customer analysis
    customer_analysis = spark.sql("""
        SELECT 
            customer_id,
            COUNT(*) as total_orders,
            SUM(amount) as total_spent,
            AVG(amount) as avg_order_value
        FROM orders 
        GROUP BY customer_id
        ORDER BY total_spent DESC
    """)
    
    logger.info("Customer analysis:")
    customer_analysis.show()
    
    return daily_revenue, customer_analysis

def process_kafka_streaming(spark):
    """Process streaming data from Kafka"""
    logger.info("Processing Kafka streaming data...")
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "orders") \
        .option("startingOffsets", "earliest") \
        .load()
    
    # Parse JSON data
    from pyspark.sql.functions import from_json, col
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    
    orders_schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", TimestampType(), True)
    ])
    
    parsed_df = kafka_df \
        .select(from_json(col("value").cast("string"), orders_schema).alias("data")) \
        .select("data.*")
    
    # Write to HDFS
    query = parsed_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "hdfs://namenode:9000/data/streaming/orders") \
        .option("checkpointLocation", "hdfs://namenode:9000/data/streaming/checkpoint") \
        .start()
    
    logger.info("Streaming query started...")
    return query

def main():
    """Main function"""
    logger.info("Starting Spark job...")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Process batch data
        daily_revenue, customer_analysis = process_orders_data(spark)
        
        # Write results to HDFS
        daily_revenue.write \
            .mode("overwrite") \
            .parquet("hdfs://namenode:9000/data/processed/daily_revenue")
        
        customer_analysis.write \
            .mode("overwrite") \
            .parquet("hdfs://namenode:9000/data/processed/customer_analysis")
        
        logger.info("Batch processing completed successfully")
        
        # Uncomment to enable streaming processing
        # streaming_query = process_kafka_streaming(spark)
        # streaming_query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Error in Spark job: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()




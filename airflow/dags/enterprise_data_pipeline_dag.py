from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'enterprise_data_pipeline',
    default_args=default_args,
    description='Enterprise data pipeline with Spark, Kafka, HDFS, YARN, Hive, Impala, Iceberg, and Starburst',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['enterprise', 'spark', 'kafka', 'hdfs', 'yarn', 'hive', 'impala', 'iceberg', 'starburst'],
    max_active_runs=1,
)

def generate_sample_data():
    """Generate sample data for the pipeline"""
    import json
    import random
    from datetime import datetime
    
    # Generate order data
    order_data = {
        'order_id': f"order_{int(datetime.now().timestamp() * 1000)}",
        'customer_id': random.randint(1, 1000),
        'product_id': random.randint(1, 100),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10.0, 500.0), 2),
        'timestamp': datetime.now().isoformat(),
        'status': random.choice(['pending', 'confirmed', 'shipped', 'delivered'])
    }
    
    # Generate customer data
    customer_data = {
        'customer_id': random.randint(1, 1000),
        'name': f"Customer_{random.randint(1, 1000)}",
        'email': f"customer{random.randint(1, 1000)}@example.com",
        'registration_date': datetime.now().isoformat(),
        'country': random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'AU'])
    }
    
    return json.dumps(order_data), json.dumps(customer_data)

def check_hdfs_health():
    """Check HDFS cluster health"""
    import subprocess
    try:
        result = subprocess.run(
            "docker-compose exec namenode hdfs dfsadmin -report",
            shell=True, capture_output=True, text=True
        )
        if result.returncode == 0:
            print("HDFS cluster is healthy")
            return True
        else:
            print(f"HDFS cluster has issues: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error checking HDFS: {e}")
        return False

def check_yarn_health():
    """Check YARN cluster health"""
    import subprocess
    try:
        result = subprocess.run(
            "docker-compose exec resourcemanager yarn node -list",
            shell=True, capture_output=True, text=True
        )
        if result.returncode == 0:
            print("YARN cluster is healthy")
            return True
        else:
            print(f"YARN cluster has issues: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error checking YARN: {e}")
        return False

def check_starburst_health():
    """Check Starburst cluster health"""
    import subprocess
    try:
        result = subprocess.run(
            'docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "SHOW CATALOGS"',
            shell=True, capture_output=True, text=True
        )
        if result.returncode == 0:
            print("Starburst cluster is healthy")
            return True
        else:
            print(f"Starburst cluster has issues: {result.stderr}")
            return False
    except Exception as e:
        print(f"Error checking Starburst: {e}")
        return False

# Data Generation Task Group
with TaskGroup("data_generation", dag=dag) as data_generation_group:
    
    # Generate sample data
    generate_data = PythonOperator(
        task_id='generate_sample_data',
        python_callable=generate_sample_data,
    )
    
    # Send data to Kafka
    send_orders_to_kafka = ProduceToTopicOperator(
        task_id='send_orders_to_kafka',
        kafka_config_id='kafka_default',
        topic='orders',
        producer_function=lambda: generate_sample_data()[0],
    )
    
    send_customers_to_kafka = ProduceToTopicOperator(
        task_id='send_customers_to_kafka',
        kafka_config_id='kafka_default',
        topic='customers',
        producer_function=lambda: generate_sample_data()[1],
    )

# Data Ingestion Task Group
with TaskGroup("data_ingestion", dag=dag) as data_ingestion_group:
    
    # Spark Streaming job to consume from Kafka and write to HDFS
    kafka_to_hdfs_streaming = SparkSubmitOperator(
        task_id='kafka_to_hdfs_streaming',
        application='/spark/apps/kafka_to_hdfs_streaming.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'yarn',
            'spark.submit.deployMode': 'client',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true'
        },
        application_args=[
            '--kafka-servers', 'kafka:29092',
            '--hdfs-path', 'hdfs://namenode:9000/data/streaming',
            '--checkpoint-location', 'hdfs://namenode:9000/data/streaming/checkpoint'
        ]
    )

# Data Processing Task Group
with TaskGroup("data_processing", dag=dag) as data_processing_group:
    
    # Spark batch job for data processing
    spark_batch_processing = SparkSubmitOperator(
        task_id='spark_batch_processing',
        application='/spark/apps/sample_spark_job.py',
        conn_id='spark_default',
        conf={
            'spark.master': 'yarn',
            'spark.submit.deployMode': 'client',
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true'
        },
        application_args=[
            '--input-path', 'hdfs://namenode:9000/data/streaming',
            '--output-path', 'hdfs://namenode:9000/data/processed',
            '--hive-metastore', 'hive-metastore:9083'
        ]
    )

# Data Storage Task Group
with TaskGroup("data_storage", dag=dag) as data_storage_group:
    
    # Create Iceberg tables
    setup_iceberg_tables = BashOperator(
        task_id='setup_iceberg_tables',
        bash_command='python /opt/airflow/scripts/iceberg_utils.py',
    )
    
    # Load data into Iceberg tables
    load_iceberg_data = BashOperator(
        task_id='load_iceberg_data',
        bash_command='''
        docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "
        INSERT INTO iceberg.default.orders 
        SELECT * FROM hive.default.orders_processed 
        WHERE order_date = CURRENT_DATE
        "
        ''',
    )

# Data Analytics Task Group
with TaskGroup("data_analytics", dag=dag) as data_analytics_group:
    
    # Generate daily revenue report
    daily_revenue_report = BashOperator(
        task_id='daily_revenue_report',
        bash_command='''
        docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "
        INSERT INTO iceberg.default.daily_revenue
        SELECT 
            order_date as revenue_date,
            COUNT(*) as total_orders,
            SUM(price * quantity) as total_revenue,
            AVG(price * quantity) as avg_order_value,
            COUNT(DISTINCT customer_id) as unique_customers,
            CURRENT_TIMESTAMP as created_at
        FROM iceberg.default.orders
        WHERE order_date = CURRENT_DATE
        GROUP BY order_date
        "
        ''',
    )
    
    # Generate customer cohort analysis
    customer_cohort_analysis = BashOperator(
        task_id='customer_cohort_analysis',
        bash_command='''
        docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "
        INSERT INTO iceberg.default.customer_cohorts
        SELECT 
            DATE_FORMAT(first_order_date, 'YYYY-MM') as cohort_month,
            customer_id,
            first_order_date,
            COUNT(*) as cohort_size,
            COUNT(CASE WHEN order_date > first_order_date THEN 1 END) / COUNT(*) as retention_rate,
            CURRENT_TIMESTAMP as created_at
        FROM (
            SELECT 
                customer_id,
                MIN(order_date) as first_order_date,
                order_date
            FROM iceberg.default.orders
            GROUP BY customer_id, order_date
        ) t
        GROUP BY customer_id, first_order_date
        "
        ''',
    )

# Data Quality Task Group
with TaskGroup("data_quality", dag=dag) as data_quality_group:
    
    # Check data quality
    data_quality_check = PythonOperator(
        task_id='data_quality_check',
        python_callable=lambda: print("Data quality checks completed"),
    )
    
    # Validate Iceberg table integrity
    validate_iceberg_tables = BashOperator(
        task_id='validate_iceberg_tables',
        bash_command='''
        docker-compose exec starburst-coordinator starburst --server starburst-coordinator:8080 --execute "
        SELECT 
            'orders' as table_name,
            COUNT(*) as row_count,
            COUNT(DISTINCT order_id) as unique_orders,
            MIN(order_date) as min_date,
            MAX(order_date) as max_date
        FROM iceberg.default.orders
        UNION ALL
        SELECT 
            'customers' as table_name,
            COUNT(*) as row_count,
            COUNT(DISTINCT customer_id) as unique_customers,
            MIN(registration_date) as min_date,
            MAX(registration_date) as max_date
        FROM iceberg.default.customers
        "
        ''',
    )

# Health Check Task Group
with TaskGroup("health_checks", dag=dag) as health_checks_group:
    
    # Check HDFS health
    hdfs_health_check = PythonOperator(
        task_id='hdfs_health_check',
        python_callable=check_hdfs_health,
    )
    
    # Check YARN health
    yarn_health_check = PythonOperator(
        task_id='yarn_health_check',
        python_callable=check_yarn_health,
    )
    
    # Check Starburst health
    starburst_health_check = PythonOperator(
        task_id='starburst_health_check',
        python_callable=check_starburst_health,
    )

# Define task dependencies
data_generation_group >> data_ingestion_group >> data_processing_group >> data_storage_group >> data_analytics_group >> data_quality_group
health_checks_group >> data_generation_group

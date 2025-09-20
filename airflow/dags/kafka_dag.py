from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_streaming_pipeline',
    default_args=default_args,
    description='Kafka streaming data pipeline',
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=['kafka', 'streaming', 'data'],
)

def generate_sample_data():
    """Generate sample data for Kafka"""
    import json
    import random
    from datetime import datetime
    
    data = {
        'order_id': f"order_{int(datetime.now().timestamp() * 1000)}",
        'customer_id': random.randint(1, 1000),
        'product_id': random.randint(1, 100),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10.0, 500.0), 2),
        'timestamp': datetime.now().isoformat(),
        'status': random.choice(['pending', 'confirmed', 'shipped', 'delivered'])
    }
    return json.dumps(data)

# Task to generate and send data to Kafka
produce_to_kafka = ProduceToTopicOperator(
    task_id='produce_to_kafka',
    kafka_config_id='kafka_default',
    topic='orders',
    producer_function=generate_sample_data,
    dag=dag,
)

# Task to check Kafka topics
check_kafka_topics = BashOperator(
    task_id='check_kafka_topics',
    bash_command='docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list',
    dag=dag,
)

# Task to consume and display messages
consume_messages = BashOperator(
    task_id='consume_messages',
    bash_command='timeout 10s docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning || true',
    dag=dag,
)

produce_to_kafka >> check_kafka_topics >> consume_messages


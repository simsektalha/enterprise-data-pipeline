#!/usr/bin/env python3
"""
Simple Kafka producer for generating sample data
"""

import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_producer():
    """Create Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: x.encode('utf-8') if x else None
    )

def generate_order_event():
    """Generate a sample order event"""
    return {
        'order_id': f"order_{int(time.time() * 1000)}_{random.randint(1000, 9999)}",
        'customer_id': random.randint(1, 1000),
        'product_id': random.randint(1, 100),
        'quantity': random.randint(1, 5),
        'price': round(random.uniform(10.0, 500.0), 2),
        'timestamp': datetime.now().isoformat(),
        'status': random.choice(['pending', 'confirmed', 'shipped', 'delivered'])
    }

def generate_customer_event():
    """Generate a sample customer event"""
    return {
        'customer_id': random.randint(1, 1000),
        'name': f"Customer_{random.randint(1, 1000)}",
        'email': f"customer{random.randint(1, 1000)}@example.com",
        'registration_date': datetime.now().isoformat(),
        'country': random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'AU'])
    }

def main():
    """Main function to produce events"""
    producer = create_producer()
    
    topics = {
        'orders': generate_order_event,
        'customers': generate_customer_event
    }
    
    try:
        logger.info("Starting Kafka producer...")
        for i in range(100):  # Generate 100 events
            for topic, generator in topics.items():
                event = generator()
                producer.send(topic, value=event, key=event.get('order_id') or event.get('customer_id'))
                logger.info(f"Sent event to {topic}: {event}")
            
            time.sleep(1)  # Wait 1 second between batches
            
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()

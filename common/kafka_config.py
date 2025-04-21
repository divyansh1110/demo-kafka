from confluent_kafka import Producer, Consumer
from .topics import create_new_topics

KAFKA_BROKER_URL = 'localhost:9092'

def get_kafka_producer():
    return Producer({
        'bootstrap.servers': KAFKA_BROKER_URL,
        'acks': 'all',
        'retries': 5,
        'linger.ms': 10,
    })

def get_kafka_consumer(topic, group_id):
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER_URL,
        'group.id': group_id,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    })
    consumer.subscribe([topic])
    return consumer


def generate_topics():
    create_new_topics('orders')
    create_new_topics('payments')
    create_new_topics('notifications')
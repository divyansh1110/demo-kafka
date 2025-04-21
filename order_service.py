from faker import Faker
import time
import random
from common.kafka_config import get_kafka_producer
import logging
import json 

# Constants
ORDERS_TOPIC_NAME = 'orders'

# Initialize
fake = Faker()
producer = get_kafka_producer()
logger = logging.getLogger(__name__)

def generate_fake_order():
    """
    Generate random fake order using Faker.
    """
    return {
        "order_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "product_name": fake.word(),
        "quantity": random.randint(1, 5),
        "price": random.randint(10, 500),
        "timestamp": fake.iso8601(),
    }

def produce_orders():
    logger.info("Order Service started producing orders...")

    try:
        while True:
            order = generate_fake_order()

            key = order["order_id"]
            value = json.dumps(order).encode('utf-8')

            # Produce the message
            producer.produce(ORDERS_TOPIC_NAME, key=key, value=value)
            producer.flush()

            logger.info(f"Produced Order: {order}")

            time.sleep(random.randint(1, 3))  # simulate real time delay

    except Exception as e:
        logger.error(f"Error while producing order: {e}",exc_info=True)

    finally:
        producer.flush()

if __name__ == "__main__":
    produce_orders()

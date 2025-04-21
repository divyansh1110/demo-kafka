from confluent_kafka import KafkaError
import logging
import json
from random import choice
from common.kafka_config import get_kafka_consumer, get_kafka_producer

ORDERS_TOPIC_NAME = "orders"
PAYMENTS_TOPIC_NAME = "payments"

GROUP_ID = "payment_service_group"
BATCH_SIZE = 10
POLL_TIMEOUT = 1.0


logger = logging.getLogger(__name__)
consumer = get_kafka_consumer(ORDERS_TOPIC_NAME, GROUP_ID)
producer = get_kafka_producer()


def process_payment(orders):
    """
    Dummy payment processing logic.
    """

    for order in orders:
        logger.info(f"Processing payment for order: {order}")
        key = order["order_id"]
        payment_status = choice(["SUCCESS", "FAILED"])  # Random success/failure

        if payment_status == "SUCCESS":
            logger.info(
                f"Payment successful for user {order['user_id']} amount ${order['price']}"
            )
            order["payment_status"] = "SUCCESS"
        else:
            logger.warning(
                f"Payment failed for user {order['user_id']} amount ${order['price']}"
            )
            order["payment_status"] = "FAILED"

        value = json.dumps(order).encode('utf-8')

        producer.produce(PAYMENTS_TOPIC_NAME, key=key, value=value)
        producer.flush()


def produce_payments():

    logger.info(f"Payment Service started. Listening to topic '{ORDERS_TOPIC_NAME}'...")

    try:
        while True:
            messages = consumer.consume(
                num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT
            )  # Fetch up to 10 messages at a time
            if not messages:
                continue
            
            orders=[]
            for msg in messages:
                logger.info(f"len of messages for payment service: {len(messages)}")
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"Kafka error: {msg.error()}")
                    continue

                try:
                    order_data = json.loads(msg.value().decode("utf-8"))
                    orders.append(order_data)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode message: {msg.value()}. Error: {e}",exc_info=True)

            if orders:
                logger.info(f"No of orders Received: {len(orders)}")
                process_payment(orders)

    except KeyboardInterrupt:
        logger.info("Payment Service shutting down...")

    finally:
        consumer.close()

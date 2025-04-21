from common.kafka_config import get_kafka_consumer, get_kafka_producer
import json
import logging
from random import choice

PAYMENTS_TOPIC_NAME = "payments"
NOTIFICATION_TOPIC_NAME = "notifications"
GROUP_ID = "inventory_service"
BATCH_SIZE = 10
POLL_TIMEOUT = 1.0

consumer = get_kafka_consumer(PAYMENTS_TOPIC_NAME, GROUP_ID)
producer = get_kafka_producer()
logger = logging.getLogger(__name__)


def process_inventory(orders):
    logger.info(f"Processing batch of {len(orders)} orders...")

    for order in orders:
        logger.info(f"Processing order: {order}")
        key = order["order_id"]

        #  DUMMY LOGIC
        update_inventory_status = choice(
            ["SUCCESS", "FAILED"]
        )  # Random success/failure

        if order["payment_status"] == "SUCCESS":
            if update_inventory_status == "SUCCESS":
                logger.info(
                    f"Inventory updated successfully for order {order['order_id']}"
                )
                order["inventory_status"] = "SUCCESS"
            else:
                logger.warning(f"Inventory update failed for order {order['order_id']}")
                order["inventory_status"] = "FAILED"
        else:
            logger.warning(
                f"Payment failed for order {order['order_id']}. Skipping inventory update."
            )
            order["inventory_status"] = "FAILED"

        value = json.dumps(order).encode("utf-8")
        producer.produce(NOTIFICATION_TOPIC_NAME, key=key, value=value)
        producer.flush()


def update_inventory():
    logger.info("Inventory Service started batch consuming payments...")

    try:
        while True:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT)

            if not messages:
                continue

            payments = []
            for msg in messages:
                logger.info(f"len of msg for inventory service: {len(msg)}")

                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    payments_data = json.loads(msg.value().decode("utf-8"))
                    payments.append(payments_data)

                except Exception as e:
                    logger.error(f"Failed to decode message: {msg.value()}. Error: {e}",exc_info=True)

            if payments:

                process_inventory(payments)

    except KeyboardInterrupt:
        logger.info("Inventory Service shutting down...")

    finally:
        consumer.close()

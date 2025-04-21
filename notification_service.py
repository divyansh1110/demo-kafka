import json
import  logging 
from common.kafka_config import get_kafka_consumer


NOTIFICATION_TOPIC_NAME = "notifications"
GROUP_ID = "notification_service_group"  
BATCH_SIZE = 10
POLL_TIMEOUT = 1.0

logger = logging.getLogger(__name__)
consumer = get_kafka_consumer(NOTIFICATION_TOPIC_NAME, GROUP_ID)


def send_mails(notifications):
    """
    Dummy send_mails logic.
    """
    for order in notifications:
        logger.info(f"Sending notification to user: {order['user_id']} for product: {order['product_name']}")



def send_notification():

    logger.info(f"Notification Service started. Listening to topic '{NOTIFICATION_TOPIC_NAME}'...")

    try:
        while True:
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT)

            if not messages:
                continue

            for msg in messages:
                logger.info(f"len of messages for notification service: {len(messages)}")
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                notifications = []
                try:
                    notification_data = json.loads(msg.value().decode("utf-8"))
                    notifications.append(notification_data)

                except Exception as e:
                    logger.error(f"Failed to decode message: {msg.value()}. Error: {e}",exc_info=True)

                if notifications:
                    send_mails(notifications)

    except KeyboardInterrupt:
        logger.info("Inventory Service shutting down...")

    finally:
        consumer.close()
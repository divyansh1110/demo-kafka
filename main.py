from  common.logger import configure_logging
from common.kafka_config import generate_topics
from  order_service  import produce_orders
from  payment_service import produce_payments
from  inventory_service import update_inventory
from notification_service import send_notification
import threading

configure_logging()
generate_topics()

# Create threads
thread1 = threading.Thread(target=produce_orders)
thread2 = threading.Thread(target=produce_payments)
thread3 = threading.Thread(target=update_inventory)
thread4 = threading.Thread(target=send_notification)


# Start threads
thread1.start()
thread2.start()
thread3.start()
thread4.start()


# Wait for all threads to finish
thread1.join()
thread2.join()
thread3.join()
thread4.join()
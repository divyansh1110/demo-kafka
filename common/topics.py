from confluent_kafka.admin import AdminClient, NewTopic
import logging

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'


def create_new_topics(topic_name: str) -> bool:

    admin_client = AdminClient({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    })

    # Define topic
    topic = NewTopic(
        topic=topic_name,
        num_partitions=2,      
        replication_factor=1    
    )

    # Create topic
    fs = admin_client.create_topics([topic])

    # Check if topic creation was successful
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            logger.info(f"Topic {topic} created successfully!")
            return True
        except Exception as e:
            logger.error(f"Failed to create topic {topic}: {e}")
            return False
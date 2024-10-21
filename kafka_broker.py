from kafka import KafkaProducer
import json
from src.utils.logger import get_logger

# Initialize logger
logger = get_logger('KafkaBroker')

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Kafka broker address
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_message(topic, message):
    """
    Sends a message to the specified Kafka topic.
    Args:
        topic (str): The topic to send the message to.
        message (dict): The message to send (must be serializable).
    """
    try:
        logger.info(f"Sending message to topic {topic}: {message}")
        producer.send(topic, message)
        producer.flush()  # Ensure the message is sent
    except Exception as e:
        logger.error(f"Failed to send message to Kafka: {str(e)}")

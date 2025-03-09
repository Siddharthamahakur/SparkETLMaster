import json

from kafka import KafkaConsumer

from utils.logger import setup_logger

logger = setup_logger("kafka_consumer")


def consume_kafka(topic, bootstrap_servers="localhost:9092"):
    """Consumes messages from a Kafka topic."""
    try:
        consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,
                                 value_deserializer=lambda x: json.loads(x.decode("utf-8")))
        logger.info(f"Listening to Kafka topic: {topic}")

        for message in consumer:
            logger.info(f"Received message: {message.value}")
    except Exception as e:
        logger.error(f"Error in Kafka Consumer: {e}", exc_info=True)

import json
import signal
import sys
from kafka import KafkaConsumer
from utils.logger import setup_logger

logger = setup_logger("kafka_consumer")


def consume_kafka(topic, bootstrap_servers="localhost:9092"):
    """Consumes messages from a Kafka topic."""

    # Signal handler for graceful shutdown
    def signal_handler(sig, frame):
        logger.info("Gracefully shutting down the Kafka consumer...")
        consumer.close()
        sys.exit(0)

    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Create the Kafka consumer once
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",  # Or "latest" based on your use case
            enable_auto_commit=True,  # Enable auto-commit for offsets
            group_id="kafka-consumer-group"
        )

        logger.info(f"Listening to Kafka topic: {topic}")

        # Infinite loop to consume messages
        for message in consumer:
            logger.info(f"Received message from partition {message.partition}, "


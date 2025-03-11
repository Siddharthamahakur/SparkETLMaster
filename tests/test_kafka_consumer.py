import logging
import time

import pytest
from kafka import KafkaConsumer, KafkaError

from extractors.kafka_consumer import consume_kafka

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def kafka_consumer():
    """
    Fixture to create and return a Kafka consumer for testing.
    Ensures proper cleanup after test execution.
    """
    topic = "test_topic"
    bootstrap_servers = "localhost:9092"  # Update if needed

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Prevents affecting actual offsets
            consumer_timeout_ms=5000  # Timeout if no messages are received
        )
        logger.info(f"Kafka consumer connected to topic: {topic}")
        yield consumer
    except KafkaError as e:
        logger.error(f"Failed to connect Kafka consumer: {e}")
        pytest.fail(f"Kafka consumer setup failed: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")


def test_kafka_consumer(kafka_consumer):
    """
    Test the Kafka consumer by consuming messages from a test topic.
    Ensures the function does not raise exceptions and messages are received.
    """
    test_topic = "test_topic"
    max_retries = 3  # Retry attempts in case of temporary failures
    retry_delay = 2  # Seconds to wait between retries

    logger.info(f"Testing Kafka consumer for topic: {test_topic}")

    for attempt in range(max_retries):
        try:
            messages = consume_kafka(test_topic)

            # Ensure messages are received
            assert messages is not None, "No messages were returned by Kafka consumer."
            assert len(messages) > 0, "Kafka consumer returned an empty message list."

            logger.info(f"Kafka consumer test passed. Received {len(messages)} messages.")
            return  # Test passed, exit loop

        except KafkaError as e:
            logger.error(f"Attempt {attempt + 1}: Kafka consumer failed with error: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)  # Wait before retrying
            else:
                pytest.fail(f"Kafka consumer failed after {max_retries} attempts: {e}")

from ingestion.kafka_consumer import consume_kafka


def test_kafka_consumer():
    try:
        consume_kafka("test_topic")
        assert True  # If no exception occurs
    except:
        assert False

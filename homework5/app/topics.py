import logging
import os

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from app.connections import TOPIC, retry


def create_topics():
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    topics = [
        NewTopic(TOPIC, num_partitions=3, replication_factor=3,
                 config={"min.insync.replicas": "2"}),
        NewTopic("_schemas", num_partitions=1, replication_factor=3,
                 config={"cleanup.policy": "compact", "min.insync.replicas": "2"}),
    ]
    for result in admin.create_topics(topics, operation_timeout=15, request_timeout=20).values():
        try:
            result.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise

    metadata = admin.list_topics(timeout=10)
    for topic in topics:
        state = metadata.topics.get(topic.topic)
        if not state or not state.partitions or any(p.leader < 0 for p in state.partitions.values()):
            raise RuntimeError(f"waiting for leaders in {topic.topic}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    retry(create_topics, (KafkaException, RuntimeError), "initialize Kafka topics", attempts=6)

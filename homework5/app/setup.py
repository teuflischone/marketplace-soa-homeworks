import logging
import os

import httpx
import psycopg
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from app.connections import APP_DIR, SQL_DIR, SUBJECT, TOPIC, clickhouse, postgres, registry, retry

log = logging.getLogger(__name__)


def create_topic():
    admin = AdminClient({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    topic = NewTopic(TOPIC, num_partitions=3, replication_factor=3, config={"min.insync.replicas": "2"})
    try:
        admin.create_topics([topic], operation_timeout=15, request_timeout=20)[TOPIC].result()
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise


def register_schema():
    schema = {"schema": (APP_DIR / "movie-event.avsc").read_text()}
    registry(f"/subjects/{SUBJECT}/versions", schema)
    version = registry(f"/subjects/{SUBJECT}", schema)
    log.info("subject=%s version=%s schema_id=%s", SUBJECT, version["version"], version["id"])


def migrate():
    for path in sorted(SQL_DIR.glob("ch_*.sql")):
        clickhouse(path.read_text())
    with postgres() as connection:
        for path in sorted(SQL_DIR.glob("pg_*.sql")):
            connection.execute(path.read_text())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    retry(create_topic, KafkaException, "create Kafka topic")
    retry(register_schema, httpx.HTTPError, "register Avro schema")
    retry(migrate, (httpx.HTTPError, psycopg.OperationalError), "apply migrations")

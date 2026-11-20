import json
import logging
import os
import struct
from contextlib import asynccontextmanager
from datetime import timezone
from io import BytesIO
from threading import Event, Lock, Thread
from uuid import UUID

from confluent_kafka import KafkaException, Producer
from fastapi import FastAPI, HTTPException
from fastavro import parse_schema, schemaless_writer
from fastavro.validation import ValidationError, validate
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from app.connections import APP_DIR, SUBJECT, TOPIC, registry
from app.generator import generate

log = logging.getLogger("uvicorn.error")
SCHEMA_TEXT = (APP_DIR / "movie-event.avsc").read_text()
SCHEMA = parse_schema(json.loads(SCHEMA_TEXT))


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    user_id: str = Field(min_length=1, max_length=255)
    movie_id: str = Field(min_length=1, max_length=255)
    event_type: str
    timestamp: AwareDatetime
    device_type: str
    session_id: str = Field(min_length=1, max_length=255)
    progress_seconds: int | None = Field(default=None, ge=0, le=2147483647, strict=True)

    @model_validator(mode="after")
    def validate_progress(self):
        self.timestamp = self.timestamp.astimezone(timezone.utc)
        if self.event_type.startswith("VIEW_") and self.progress_seconds is None:
            raise ValueError("progress_seconds is required for viewing events")
        return self


class Publisher:
    def __init__(self):
        version = registry(f"/subjects/{SUBJECT}", {"schema": SCHEMA_TEXT})
        self.header = struct.pack(">BI", 0, version["id"])
        self.producer = Producer({
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "enable.idempotence": True,
            "acks": "all",
            "partitioner": "murmur2_random",
            "retry.backoff.ms": 250,
            "retry.backoff.max.ms": 4000,
            "delivery.timeout.ms": 15000,
        })
        self.lock = Lock()

    def publish(self, event):
        record = event.model_dump()
        record["event_id"] = str(event.event_id)
        validate(record, SCHEMA)
        payload = BytesIO()
        payload.write(self.header)
        schemaless_writer(payload, SCHEMA, record)

        errors = []

        def delivered(error, _message):
            if error is not None:
                errors.append(error)

        with self.lock:
            self.producer.produce(TOPIC, key=event.user_id.encode(), value=payload.getvalue(),
                                  on_delivery=delivered)
            pending = self.producer.flush()
        if errors:
            raise KafkaException(errors[0])
        if pending:
            raise TimeoutError("Kafka delivery confirmation timed out")
        log.info("published event_id=%s event_type=%s timestamp=%s",
                 event.event_id, event.event_type, event.timestamp.isoformat())


@asynccontextmanager
async def lifespan(app):
    publisher = app.state.publisher = Publisher()
    stop = Event()
    interval = int(os.environ.get("GENERATOR_INTERVAL_SECONDS", "0"))
    worker = None
    if interval > 0:
        def publish_generated(record):
            try:
                publisher.publish(MovieEvent.model_validate(record))
            except (KafkaException, BufferError, TimeoutError):
                log.exception("generator failed to publish event_id=%s", record["event_id"])
                raise

        worker = Thread(target=generate, args=(publish_generated, stop, interval), daemon=True)
        worker.start()
    yield
    stop.set()
    if worker is not None:
        worker.join(timeout=16)


app = FastAPI(title="Movie Events", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events", status_code=201)
def publish_event(event: MovieEvent):
    try:
        app.state.publisher.publish(event)
    except ValidationError as exc:
        raise HTTPException(422, "event does not match the Avro schema") from exc
    except (KafkaException, BufferError, TimeoutError) as exc:
        log.warning("publish failed event_id=%s error=%s", event.event_id, exc)
        raise HTTPException(503, "Kafka did not acknowledge the event") from exc
    return {"event_id": str(event.event_id)}

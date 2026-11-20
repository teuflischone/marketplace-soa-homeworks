import logging
import os
from pathlib import Path
from time import sleep

import httpx
import psycopg

from app.credentials import POSTGRES_PASSWORD

log = logging.getLogger(__name__)
APP_DIR = Path(__file__).parent
SQL_DIR = APP_DIR / "sql"
TOPIC = "movie-events"
SUBJECT = f"{TOPIC}-value"
BUCKET = "movie-analytics"


def clickhouse(sql, params=None, data=None):
    parameters = {f"param_{name}": value for name, value in (params or {}).items()}
    if data is None:
        response = httpx.post(os.environ["CLICKHOUSE_URL"], params=parameters, content=sql, timeout=30)
    else:
        parameters["query"] = sql
        response = httpx.post(os.environ["CLICKHOUSE_URL"], params=parameters, content=data, timeout=30)
    if not response.is_success:
        raise RuntimeError(f"ClickHouse {response.status_code}: {response.text.strip()}")
    return response.text


def registry(path, payload=None):
    url = os.environ["SCHEMA_REGISTRY_URL"] + path
    response = httpx.get(url, timeout=10) if payload is None else httpx.post(url, json=payload, timeout=10)
    response.raise_for_status()
    return response.json()


def retry(operation, errors, label, attempts=4):
    for attempt in range(attempts):
        try:
            return operation()
        except errors as exc:
            log.warning("%s attempt=%d/%d error=%s", label, attempt + 1, attempts, exc)
            if attempt == attempts - 1:
                raise
            sleep(min(0.5 * 2 ** attempt, 5))


def postgres(**kwargs):
    return psycopg.connect(os.environ["POSTGRES_DSN"], password=POSTGRES_PASSWORD.read_text(),
                           connect_timeout=5, **kwargs)

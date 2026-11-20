import logging

import httpx
import psycopg

from app.connections import APP_DIR, SQL_DIR, SUBJECT, clickhouse, postgres, registry, retry

log = logging.getLogger(__name__)


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
    retry(register_schema, httpx.HTTPError, "register Avro schema")
    retry(migrate, (httpx.HTTPError, psycopg.OperationalError), "apply migrations")

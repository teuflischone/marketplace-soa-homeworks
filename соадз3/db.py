import os
from pathlib import Path

import psycopg
from psycopg.rows import dict_row


def connect():
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def migrate(service):
    directory = Path(__file__).parent / "migrations" / service
    with connect() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY)")
        for path in sorted(directory.glob("V*.sql")):
            with conn.transaction():
                exists = conn.execute("SELECT 1 FROM schema_migrations WHERE version = %s", (path.name,)).fetchone()
                if not exists:
                    conn.execute(path.read_text(encoding="utf-8"))
                    conn.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (path.name,))

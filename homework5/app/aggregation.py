import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
from threading import Event, Lock, Thread
from time import monotonic

import boto3
import httpx
import psycopg
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, HTTPException
from psycopg.rows import dict_row

from app.connections import BUCKET, SQL_DIR, clickhouse, postgres, retry
from app.credentials import S3_CONFIG

log = logging.getLogger("uvicorn.error")
job_lock = Lock()
DAILY = (SQL_DIR / "daily.sql").read_text()
TOP_MOVIES = (SQL_DIR / "top_movies.sql").read_text()
RETENTION = (SQL_DIR / "retention.sql").read_text()
DEPENDENCY_ERRORS = (httpx.HTTPError, psycopg.Error, BotoCoreError, ClientError, RuntimeError)


def utc_today():
    return datetime.now(timezone.utc).date()


def query(sql, day):
    return [json.loads(line) for line in clickhouse(sql, {"day": day.isoformat()}).splitlines()]


def metrics_for(day):
    daily, = query(DAILY, day)
    processed = daily.pop("processed")
    daily["dau"] = int(daily["dau"])
    metrics = [(name, "", value) for name, value in daily.items()]
    metrics.extend(("top_movies", row["movie_id"], int(row["views"])) for row in query(TOP_MOVIES, day))
    cohort = {int(row["age"]): int(row["users"]) for row in query(RETENTION, day)}
    size = cohort.get(0, 0)
    for age in range(8):
        mature = day + timedelta(days=age) <= utc_today()
        value = cohort.get(age, 0) / size if mature and size else None
        metrics.append(("retention", str(age), value))
    return processed, metrics


def materialize(day, metrics, computed_at):
    rows = [
        {"day": day.isoformat(), "metric": metric, "dimension": dimension,
         "value": value, "computed_at": computed_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}
        for metric, dimension, value in metrics
    ]
    clickhouse("TRUNCATE TABLE daily_metrics_staging")
    clickhouse("INSERT INTO daily_metrics_staging FORMAT JSONEachRow",
               data="\n".join(map(json.dumps, rows)))
    clickhouse("ALTER TABLE daily_metrics REPLACE PARTITION {day:Date} FROM daily_metrics_staging",
               {"day": day.isoformat()})


def save_metrics(day, metrics, computed_at):
    keys = [metric + ":" + dimension for metric, dimension, _ in metrics]
    with postgres(row_factory=dict_row) as connection, connection.cursor() as cursor:
        cursor.executemany("""
            INSERT INTO daily_metrics (day, metric, dimension, value, computed_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (day, metric, dimension) DO UPDATE
            SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
        """, [(day, metric, dimension, value, computed_at) for metric, dimension, value in metrics])
        cursor.execute("""
            DELETE FROM daily_metrics WHERE day = %s AND NOT ((metric || ':' || dimension) = ANY(%s))
        """, (day, keys))
        cursor.execute("""
            INSERT INTO daily_exports (day, computed_at) VALUES (%s, %s)
            ON CONFLICT (day) DO UPDATE SET computed_at = EXCLUDED.computed_at
        """, (day, computed_at))


def recalculate(day):
    with job_lock:
        started = monotonic()
        log.info("aggregation started day=%s", day)
        processed = 0
        try:
            processed, metrics = metrics_for(day)
            computed_at = datetime.now(timezone.utc)
            materialize(day, metrics, computed_at)
            retry(lambda: save_metrics(day, metrics, computed_at), psycopg.OperationalError,
                  f"write PostgreSQL day={day}")
        except DEPENDENCY_ERRORS:
            log.exception("aggregation failed day=%s events=%d seconds=%.3f",
                          day, processed, monotonic() - started)
            raise
        log.info("aggregation completed day=%s events=%d metrics=%d seconds=%.3f",
                 day, processed, len(metrics), monotonic() - started)
    return {"day": day, "events": processed, "metrics": len(metrics)}


def storage():
    credential, = json.loads(S3_CONFIG.read_text())["identities"][0]["credentials"]
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name="us-east-1",
                        aws_access_key_id=credential["accessKey"], aws_secret_access_key=credential["secretKey"],
                        config=Config(connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 1},
                                      s3={"addressing_style": "path"}))


def export(day):
    with job_lock, postgres(row_factory=dict_row) as connection:
        rows = connection.execute("""
            SELECT metric, dimension, value, computed_at
            FROM daily_metrics WHERE day = %s ORDER BY metric, dimension
        """, (day,)).fetchall()
        if not rows:
            raise LookupError("no aggregates for this date")
        computed_at = rows[0]["computed_at"]
        for row in rows:
            row["computed_at"] = row["computed_at"].isoformat()
        key = f"daily/{day}/aggregates.json"
        body = json.dumps({"day": day.isoformat(), "metrics": rows}, allow_nan=False).encode()
        s3 = storage()
        try:
            s3.head_bucket(Bucket=BUCKET)
        except ClientError as exc:
            if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise
            s3.create_bucket(Bucket=BUCKET)
        s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json")
        connection.execute("""
            UPDATE daily_exports SET exported_at = computed_at WHERE day = %s AND computed_at = %s
        """, (day, computed_at))
        log.info("exported day=%s metrics=%d key=%s", day, len(rows), key)
    return {"uri": f"s3://{BUCKET}/{key}"}


def export_pending(stop):
    try:
        with postgres(row_factory=dict_row) as connection:
            pending = connection.execute("""
                SELECT day FROM daily_exports
                WHERE day < %s AND (exported_at IS NULL OR exported_at < computed_at)
                ORDER BY day
            """, (utc_today(),)).fetchall()
    except psycopg.Error:
        log.exception("could not read pending exports")
        return
    for item in pending:
        if stop.is_set():
            return
        try:
            export(item["day"])
        except (*DEPENDENCY_ERRORS, LookupError):
            log.exception("scheduled export failed day=%s", item["day"])


def schedule(stop, interval):
    while not stop.is_set():
        today = utc_today()
        for age in range(9):
            if stop.is_set():
                return
            day = today - timedelta(days=age)
            try:
                recalculate(day)
            except DEPENDENCY_ERRORS:
                continue
        export_pending(stop)
        stop.wait(interval)


@asynccontextmanager
async def lifespan(_app):
    stop = Event()
    interval = int(os.environ.get("AGGREGATION_INTERVAL_SECONDS", "60"))
    worker = None
    if interval > 0:
        worker = Thread(target=schedule, args=(stop, interval), daemon=True)
        worker.start()
    yield
    stop.set()


app = FastAPI(title="Cinema Analytics", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/aggregate/{day}")
def aggregate_day(day: date):
    if day > utc_today():
        raise HTTPException(422, "day is in the future")
    try:
        return recalculate(day)
    except DEPENDENCY_ERRORS as exc:
        raise HTTPException(503, "aggregation failed") from exc


@app.post("/export/{day}")
def export_day(day: date):
    try:
        return export(day)
    except LookupError as exc:
        raise HTTPException(404, str(exc)) from exc
    except DEPENDENCY_ERRORS as exc:
        log.exception("manual export failed day=%s", day)
        raise HTTPException(503, "export failed") from exc

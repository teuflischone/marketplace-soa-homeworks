import json
import sys
from datetime import datetime, time, timedelta, timezone
from time import monotonic, sleep
from uuid import uuid4

import httpx
from psycopg.rows import dict_row

from app.aggregation import storage
from app.connections import BUCKET, SUBJECT, clickhouse, postgres, registry

PRODUCER = "http://producer:8000"
AGGREGATION = "http://aggregation:8000"
GRAFANA = "http://grafana:3000"


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def post(url, payload=None, status=200):
    response = httpx.post(url, json=payload, timeout=35)
    check(response.status_code == status, f"{url}: {response.status_code} {response.text}")
    return response.json()


def make_event(day, user=None, kind="VIEW_STARTED", progress=0, movie="arrival"):
    return {
        "event_id": str(uuid4()),
        "user_id": user or str(uuid4()),
        "movie_id": movie,
        "event_type": kind,
        "timestamp": datetime.combine(day, time(11, 22, 33, 567000), timezone.utc).isoformat(),
        "device_type": "TV",
        "session_id": str(uuid4()),
        "progress_seconds": progress,
    }


def publish(event):
    result = post(f"{PRODUCER}/events", event, status=201)
    check(result == {"event_id": event["event_id"]}, result)


def wait_for_events(events):
    deadline = monotonic() + 50
    for event in events:
        while True:
            raw = clickhouse("""
                SELECT event_id, user_id, movie_id, event_type, timestamp, device_type,
                       session_id, progress_seconds
                FROM events FINAL WHERE event_id = {id:UUID}
                FORMAT JSONEachRow
            """, {"id": event["event_id"]})
            if raw:
                actual = json.loads(raw)
                actual["timestamp"] = datetime.fromisoformat(actual["timestamp"]).replace(
                    tzinfo=timezone.utc).isoformat()
                check(actual == event, f"expected {event}, got {actual}")
                break
            check(monotonic() < deadline, f"event not ingested: {event['event_id']}")
            sleep(0.5)


def test_pipeline(day):
    event = make_event(day)
    post(f"{PRODUCER}/events", {**event, "event_type": "BOUGHT"}, status=422)
    post(f"{PRODUCER}/events", {**event, "timestamp": "2026-11-01T01:02:03"}, status=422)
    post(f"{PRODUCER}/events", {**event, "progress_seconds": None}, status=422)
    publish(event)
    publish(event)
    wait_for_events([event])
    print("HTTP → Kafka → ClickHouse: fields and validation OK", flush=True)


def test_metrics(day):
    anna, boris = str(uuid4()), str(uuid4())
    first = make_event(day, anna)
    events = [
        first,
        {**first, "event_id": str(uuid4()), "event_type": "VIEW_FINISHED",
         "timestamp": (datetime.fromisoformat(first["timestamp"]) + timedelta(minutes=2)).isoformat(),
         "progress_seconds": 120},
        make_event(day, boris, movie="dune"),
        make_event(day + timedelta(days=1), anna),
        make_event(day + timedelta(days=7), boris, movie="dune"),
        make_event(day, boris, "LIKED", None, "dune"),
    ]
    for event in events:
        publish(event)
    wait_for_events(events)

    def aggregate(expected, count):
        result = post(f"{AGGREGATION}/aggregate/{day}")
        check(result["events"] == count, result)
        with postgres(row_factory=dict_row) as connection:
            rows = connection.execute("""
                SELECT metric, dimension, value FROM daily_metrics WHERE day = %s
            """, (day,)).fetchall()
        metrics = {(row["metric"], row["dimension"]): row["value"] for row in rows}
        check(metrics == expected, metrics)
        return metrics

    def export(expected):
        key = f"daily/{day}/aggregates.json"
        check(post(f"{AGGREGATION}/export/{day}") == {"uri": f"s3://{BUCKET}/{key}"}, key)
        s3 = storage()
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"daily/{day}/")
        check([item["Key"] for item in response["Contents"]] == [key], response)
        data = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        check(data["day"] == day.isoformat(), data)
        metrics = {(row["metric"], row["dimension"]): row["value"] for row in data["metrics"]}
        timestamps = {datetime.fromisoformat(row["computed_at"]) for row in data["metrics"]}
        check(len(timestamps) == 1 and metrics == expected, data)
        return data

    expected = {
        ("dau", ""): 2,
        ("average_watch_seconds", ""): 120,
        ("view_conversion", ""): 0.5,
        ("top_movies", "arrival"): 1,
        ("top_movies", "dune"): 1,
        **{("retention", str(age)): {0: 1, 1: 0.5, 7: 0.5}.get(age, 0) for age in range(8)},
    }
    aggregate(expected, 4)
    first_export = export(expected)

    finished = {**events[2], "event_id": str(uuid4()), "event_type": "VIEW_FINISHED",
                "timestamp": (datetime.fromisoformat(events[2]["timestamp"])
                              + timedelta(minutes=1)).isoformat(), "progress_seconds": 60}
    publish(finished)
    wait_for_events([finished])
    expected[("average_watch_seconds", "")] = 90
    expected[("view_conversion", "")] = 1
    aggregate(expected, 5)
    second_export = export(expected)
    check(first_export["metrics"][0]["computed_at"] != second_export["metrics"][0]["computed_at"],
          "export timestamp did not change")
    print("ClickHouse → PostgreSQL → S3: all metrics, upsert and overwrite OK", flush=True)


def test_grafana(day):
    dashboard = httpx.get(f"{GRAFANA}/api/dashboards/uid/cinema-analytics", timeout=10)
    dashboard.raise_for_status()
    panels = dashboard.json()["dashboard"]["panels"]
    check(len(panels) == 3, panels)
    for panel in panels:
        start = datetime.combine(day, time.min, timezone.utc)
        end = start + timedelta(days=1)
        result = post(f"{GRAFANA}/api/ds/query", {
            "from": str(int(start.timestamp() * 1000)),
            "to": str(int(end.timestamp() * 1000)),
            "queries": panel["targets"],
        })["results"]["A"]
        check(result.get("status") == 200, result)
        frames = result.get("frames", [])
        check(any(frame["data"]["values"][0] for frame in frames), result)
        if panel["type"] == "table":
            values = frames[0]["data"]["values"]
            check(values[1][0] == 1 and values[2][0] == 0.5 and values[8][0] == 0.5, values)
    print("Grafana: all three panels return aggregate data", flush=True)


if __name__ == "__main__":
    today = datetime.now(timezone.utc).date()
    schema = registry(f"/subjects/{SUBJECT}/versions/1")
    check(schema["version"] == 1 and schema["subject"] == SUBJECT, schema)
    test_pipeline(today - timedelta(days=30))
    if "--pipeline-only" not in sys.argv:
        day = today - timedelta(days=10)
        test_metrics(day)
        test_grafana(day)

import logging
from datetime import datetime, time, timedelta, timezone
from random import Random
from uuid import NAMESPACE_URL, uuid4, uuid5

from confluent_kafka import KafkaException

log = logging.getLogger("uvicorn.error")
MOVIES = ("arrival", "dune", "interstellar", "the-matrix", "whiplash")
DEVICES = ("MOBILE", "DESKTOP", "TV", "TABLET")


def session(rng, user, started, session_id, historical=False):
    movie = rng.choice(MOVIES)
    device = rng.choice(DEVICES)
    events = [(0, "SEARCHED", None), (3, "VIEW_STARTED", 0)]
    if rng.random() < 0.7:
        duration = rng.randint(4800, 7500)
        events.extend([
            (1203, "VIEW_PAUSED", 1200),
            (1240, "VIEW_RESUMED", 1201),
            (duration + 39, "VIEW_FINISHED", duration),
        ])
        if rng.random() < 0.4:
            events.append((duration + 48, "LIKED", None))
    else:
        events.append((270, "VIEW_PAUSED", 267))

    for index, (offset, kind, progress) in enumerate(events):
        yield {
            "event_id": uuid5(NAMESPACE_URL, f"{session_id}:{index}") if historical else uuid4(),
            "user_id": user,
            "movie_id": movie,
            "event_type": kind,
            "timestamp": started + timedelta(seconds=offset),
            "device_type": device,
            "session_id": session_id,
            "progress_seconds": progress,
        }


def history(today):
    for days_ago in range(8, 0, -1):
        cohort = today - timedelta(days=days_ago)
        for number in range(7):
            user = f"demo-{cohort}-{number}"
            rng = Random(user)
            for age in range(8):
                day = cohort + timedelta(days=age)
                returns = age == 0 or rng.random() < 0.68 / (1 + age / 3)
                if not returns or day >= today:
                    continue
                started = datetime.combine(day, time(hour=rng.randint(11, 20)), timezone.utc)
                yield from session(rng, user, started, f"{user}:{day}", historical=True)


def generate(publish, stop, interval):
    today = datetime.now(timezone.utc).date()
    for event in history(today):
        while not stop.is_set():
            try:
                publish(event)
                break
            except (KafkaException, BufferError, TimeoutError):
                if stop.wait(interval):
                    return
        if stop.is_set():
            return
    log.info("demo history published")
    rng = Random()
    while not stop.is_set():
        now = datetime.now(timezone.utc)
        user = f"viewer-{rng.randint(1, 100)}"
        events = session(rng, user, now - timedelta(hours=2, minutes=10), str(uuid4()))
        try:
            for event in events:
                if stop.is_set():
                    return
                publish(event)
        except (KafkaException, BufferError, TimeoutError):
            log.exception("demo session ended before all events were published")
        stop.wait(interval)

CREATE MATERIALIZED VIEW IF NOT EXISTS ingest_events TO events AS
SELECT event_id, user_id, movie_id, event_type, timestamp, device_type, session_id, progress_seconds
FROM events_kafka

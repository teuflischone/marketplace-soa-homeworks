SELECT
    toUInt32(count()) AS processed,
    uniqExact(user_id) AS dau,
    avgOrNullIf(progress_seconds, event_type = 'VIEW_FINISHED') AS average_watch_seconds,
    countIf(event_type = 'VIEW_FINISHED')
        / nullIf(countIf(event_type = 'VIEW_STARTED'), 0) AS view_conversion
FROM events FINAL
WHERE event_date = {day:Date}
FORMAT JSONEachRow

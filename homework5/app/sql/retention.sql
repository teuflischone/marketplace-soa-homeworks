WITH watch_days AS (
    SELECT user_id, event_date
    FROM events
    WHERE event_type = 'VIEW_STARTED' AND event_date <= least(addDays({day:Date}, 7), today())
    GROUP BY user_id, event_date
), activity AS (
    SELECT
        min(event_date) OVER (PARTITION BY user_id) AS first_day,
        event_date
    FROM watch_days
)
SELECT dateDiff('day', first_day, event_date) AS age, count() AS users
FROM activity
WHERE first_day = {day:Date}
GROUP BY age
FORMAT JSONEachRow

SELECT movie_id, count() AS views
FROM events FINAL
WHERE event_date = {day:Date} AND event_type = 'VIEW_STARTED'
GROUP BY movie_id
ORDER BY views DESC, movie_id
LIMIT 10
FORMAT JSONEachRow

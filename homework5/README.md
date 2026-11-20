# ДЗ 5. Аналитика онлайн-кинотеатра

`Producer → Kafka → ClickHouse → Aggregation Service → PostgreSQL → S3`

В ClickHouse Kafka Engine читает `movie-events`, а materialized view переносит события в `events`. Grafana читает готовые агрегаты из ClickHouse.

## Запуск

Из папки `homework5`:

```bash
docker compose up --build -d
docker compose ps -a
```

При первом запуске `credentials` создаёт локальные ключи в Docker volumes. `topics` создаёт топики Kafka до старта Schema Registry. `setup` регистрирует Avro-схему и применяет SQL-миграции. Оба контейнера после настройки выходят с кодом 0.

- [Producer API](http://localhost:8005/docs)
- [Aggregation API](http://localhost:8006/docs)
- [Online Cinema в Grafana](http://localhost:3005/d/cinema-analytics)
- [Объекты S3](http://localhost:9005/buckets/movie-analytics/daily/)

При запуске генератор наполняет стенд сессиями за последние восемь дней, затем отправляет новую сессию раз в 15 секунд. Первый дашборд с данными появляется после цикла агрегации. `GENERATOR_INTERVAL_SECONDS=0` отключает генератор.

```bash
day=$(python3 -c 'from datetime import datetime, timedelta, timezone; print(datetime.now(timezone.utc).date() - timedelta(days=1))')
curl -fsS -X POST "http://localhost:8006/aggregate/$day"
curl -fsS -X POST "http://localhost:8006/export/$day"
```

## События

`POST /events` принимает JSON с полями из [Avro-схемы](app/movie-event.avsc). Временная метка передаётся с часовым поясом. Для событий просмотра нужен `progress_seconds`. Ответ `201` содержит `event_id` и приходит после подтверждения Kafka. Подтверждение настроено через `acks=all`. Идемпотентный продюсер повторяет отправку при временных ошибках с экспоненциальной паузой.

Ключ Kafka — `user_id`. Действия одного пользователя попадают в одну партицию и читаются в порядке записи. Популярный фильм может смотреть много пользователей из разных партиций. Топик имеет три партиции и три реплики, минимальный ISR равен двум. У трёх брокеров общий KRaft-кворум. При остановке одного остаются два участника кворума и две реплики каждой партиции.

`events` разбита по месяцам и отсортирована по дате, времени, пользователю и ID события. Запросы за день используют сортировку по дате. `ReplacingMergeTree` и чтение с `FINAL` убирают повторы одного и того же события при переотправке.

## Агрегаты

- DAU считает уникальных пользователей по всем типам событий за сутки UTC.
- Среднее время просмотра берётся по `VIEW_FINISHED`. Топ-10 фильмов ранжируется по числу `VIEW_STARTED`. Конверсия — число завершений, делённое на число начал за те же сутки.
- Когорта определяется датой первого `VIEW_STARTED`. Повторный просмотр учитывается один раз на пользователя и день. В дальних столбцах по ещё не наступившим дням Grafana оставляет пустые ячейки.

Раз в `AGGREGATION_INTERVAL_SECONDS` (по умолчанию 60) сервис пересчитывает текущий день и восемь предыдущих. Это обновляет retention по мере поступления новых просмотров. В ClickHouse заменяется партиция дня. В PostgreSQL обновляются строки с ключом `(day, metric, dimension)`.

После пересчёта закрытые сутки выгружаются в `s3://movie-analytics/daily/YYYY-MM-DD/aggregates.json`. Несостоявшаяся выгрузка остаётся в очереди в PostgreSQL. Следующий цикл повторяет выгрузку по тому же ключу. Ручной пересчёт и экспорт доступны по дате.

## Тест

```bash
./test.sh
```

Тест запускает отдельный Compose-проект с чистыми томами. Проверяет поля события в ClickHouse, метрики на заданных сессиях, повторный пересчёт, выгрузку и запросы Grafana. Затем останавливает `kafka-1` и повторяет проверку доставки. После завершения удаляет тестовые контейнеры и тома.

Посмотреть топик и версию схемы на запущенном стенде:

```bash
docker compose exec kafka-2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-2:9092 --describe --topic movie-events
curl -fsS http://localhost:8085/subjects/movie-events-value/versions
```

# ДЗ 3. Бронирование авиабилетов

Два сервиса: Booking Service принимает REST-запросы и хранит бронирования; Flight Service хранит рейсы и резервации и предоставляет gRPC. У каждого своя PostgreSQL. Redis Sentinel используется Flight Service для кеша.

## Запуск

Из папки `соадз3`:

```bash
docker compose up --build
```

REST API: `http://localhost:8000`, документация: `http://localhost:8000/docs`. Миграции выполняются при старте сервисов. В миграции Flight Service есть три тестовых рейса на 10–11 ноября 2026 года. Для проверки: `docker compose exec -T booking-service python -m pytest -q tests`. Для остановки: `docker compose down`. Данные сохраняются в Docker volumes; для сброса: `docker compose down -v`.

Переменная `GRPC_API_KEY` в compose задаёт общий ключ сервисов (значение по умолчанию предназначено для локального запуска). При необходимости задайте её перед запуском: `GRPC_API_KEY=... docker compose up --build`.

## Пример запросов

```bash
curl 'http://localhost:8000/flights?origin=SVO&destination=LED&date=2026-11-10'
curl 'http://localhost:8000/flights/11111111-1111-4111-8111-111111111111'
curl -X POST http://localhost:8000/bookings \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"student-1","flight_id":"11111111-1111-4111-8111-111111111111","passenger_name":"Иван Иванов","passenger_email":"ivan@example.com","seat_count":2}'
curl 'http://localhost:8000/bookings?user_id=student-1'
curl 'http://localhost:8000/bookings/UUID_ИЗ_ОТВЕТА'
curl -X POST 'http://localhost:8000/bookings/UUID_ИЗ_ОТВЕТА/cancel'
```

Цена и итоговая стоимость в JSON передаются строками с двумя десятичными знаками, чтобы не терять копейки при работе с дробными числами.

## Контракты и данные

- REST: `GET /flights` (обязательные `origin`, `destination`; дата опциональна), `GET /flights/{id}`, `POST /bookings`, `GET /bookings/{id}`, `GET /bookings?user_id=...`, `POST /bookings/{id}/cancel`.
- gRPC: [proto/flight.proto](proto/flight.proto). Python-код gRPC генерируется `protoc` при сборке Docker-образа. `UpdateFlight` добавлен для изменения цены/статуса и проверки инвалидации кеша.
- Схема обеих БД: [ER.md](ER.md). Миграции: `migrations/flight` и `migrations/booking`. Таблица `schema_migrations` не даёт применить миграцию дважды.
- `NOT_FOUND` означает отсутствие рейса/резервации, `INVALID_ARGUMENT` — неверные параметры, `RESOURCE_EXHAUSTED` — нехватку мест, `ALREADY_EXISTS` — повторное использование `booking_id` с другими параметрами, `FAILED_PRECONDITION` — недопустимый статус, `UNAUTHENTICATED` — неверный ключ. REST переводит ошибки в 400/404/409/503.

## Как работает бронирование

Booking Service сначала получает рейс, затем вызывает `ReserveSeats` с заранее сгенерированным UUID бронирования. В Flight Service блокируется строка рейса (`SELECT FOR UPDATE`), проверяется остаток и в одной транзакции создаётся резервация и уменьшается `available_seats`. Повтор с тем же `booking_id` и теми же параметрами возвращает ту же резервацию. После ответа Booking Service сохраняет стоимость `seat_count * price` и статус `CONFIRMED`. Если локальная запись не удалась, вызывается компенсирующий `ReleaseReservation`.

Отмена блокирует запись бронирования, вызывает `ReleaseReservation` и меняет статус на `CANCELLED`. Flight Service возвращает места и меняет статус резервации в одной транзакции. Повторный `ReleaseReservation` не возвращает места второй раз.

При ошибках `UNAVAILABLE` и `DEADLINE_EXCEEDED` транспортный слой Booking Service делает до трёх попыток с паузами 100 и 200 мс. На бизнес-ошибках повторов нет. Circuit breaker в том же транспортном слое имеет состояния `CLOSED`, `OPEN`, `HALF_OPEN`; порог, окно ошибок и пауза задаются `CB_FAILURE_THRESHOLD`, `CB_WINDOW_SECONDS`, `CB_OPEN_SECONDS`. Переходы пишутся в лог, в `OPEN` REST сразу возвращает 503.

Flight Service использует Redis Sentinel-клиент. `GetFlight` и `SearchFlights` работают по схеме cache-aside, TTL по умолчанию 420 секунд. После резервирования, отмены и обновления рейса удаляется ключ рейса и все ключи поиска. Hit/miss пишутся в лог. Compose запускает мастер, реплику и Sentinel.

Поскольку это две независимые БД без общей распределённой транзакции, между ответом Flight Service и записью Booking Service существует короткий промежуток. При ошибке записи выполняется компенсация; если одновременно недоступны обе БД/сервиса, ручная сверка может понадобиться. Для промышленной системы здесь нужен отдельный механизм восстановления (например, outbox + фоновая сверка).

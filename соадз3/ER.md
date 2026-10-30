# ER-диаграмма

Базы физически разделены. `bookings.flight_id` и `seat_reservations.booking_id` связывают данные между сервисами по UUID, поэтому внешних ключей между ними нет. Каждый сервис отвечает за свою таблицу.

```mermaid
erDiagram
    FLIGHTS ||--o{ SEAT_RESERVATIONS : has
    FLIGHTS ||--o{ BOOKINGS : "flight_id (logical)"
    BOOKINGS ||--|| SEAT_RESERVATIONS : "booking_id (logical)"
    FLIGHTS {
        uuid id PK
        varchar flight_number
        text airline
        char_3 origin
        char_3 destination
        timestamptz departure_at
        timestamptz arrival_at
        integer total_seats
        integer available_seats
        numeric price
        varchar status
    }
    SEAT_RESERVATIONS {
        uuid id PK
        uuid booking_id UK
        uuid flight_id FK
        integer seat_count
        varchar status
    }
    BOOKINGS {
        uuid id PK
        text user_id
        uuid flight_id
        text passenger_name
        text passenger_email
        integer seat_count
        numeric total_price
        varchar status
        timestamptz created_at
    }
```

`FLIGHTS` и `SEAT_RESERVATIONS` находятся в БД Flight Service, `BOOKINGS` — в БД Booking Service. В каждой таблице поля зависят только от первичного ключа; повторяющихся групп и транзитивных зависимостей нет (3НФ). Цена в `bookings.total_price` — снимок стоимости покупки, а не копия текущей цены рейса. Индекс по `(flight_number, UTC-дата departure_at)` гарантирует уникальность рейса в день вылета. Проверки `CHECK` ограничивают количество мест, цену, аэропорты, время прибытия и статусы.

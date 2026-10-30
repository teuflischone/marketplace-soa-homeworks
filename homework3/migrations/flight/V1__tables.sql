CREATE TABLE flights (
    id uuid PRIMARY KEY,
    flight_number varchar(12) NOT NULL,
    airline text NOT NULL CHECK (length(trim(airline)) > 0),
    origin char(3) NOT NULL CHECK (origin ~ '^[A-Z]{3}$'),
    destination char(3) NOT NULL CHECK (destination ~ '^[A-Z]{3}$'),
    departure_at timestamptz NOT NULL,
    arrival_at timestamptz NOT NULL,
    total_seats integer NOT NULL CHECK (total_seats > 0),
    available_seats integer NOT NULL CHECK (available_seats >= 0 AND available_seats <= total_seats),
    price numeric(12, 2) NOT NULL CHECK (price > 0),
    status varchar(12) NOT NULL CHECK (status IN ('SCHEDULED', 'DEPARTED', 'CANCELLED', 'COMPLETED')),
    CHECK (origin <> destination),
    CHECK (arrival_at > departure_at)
);
CREATE UNIQUE INDEX flights_number_day_unique ON flights (flight_number, ((departure_at AT TIME ZONE 'UTC')::date));
CREATE INDEX flights_route_date_idx ON flights (origin, destination, departure_at) WHERE status = 'SCHEDULED';

CREATE TABLE seat_reservations (
    id uuid PRIMARY KEY,
    booking_id uuid NOT NULL UNIQUE,
    flight_id uuid NOT NULL REFERENCES flights(id),
    seat_count integer NOT NULL CHECK (seat_count > 0),
    status varchar(8) NOT NULL CHECK (status IN ('ACTIVE', 'RELEASED', 'EXPIRED'))
);
CREATE INDEX seat_reservations_flight_idx ON seat_reservations (flight_id);

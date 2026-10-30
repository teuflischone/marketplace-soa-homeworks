CREATE TABLE bookings (
    id uuid PRIMARY KEY,
    user_id text NOT NULL CHECK (length(trim(user_id)) > 0),
    flight_id uuid NOT NULL,
    passenger_name text NOT NULL CHECK (length(trim(passenger_name)) > 0),
    passenger_email text NOT NULL CHECK (position('@' in passenger_email) > 1),
    seat_count integer NOT NULL CHECK (seat_count > 0),
    total_price numeric(12, 2) NOT NULL CHECK (total_price > 0),
    status varchar(9) NOT NULL CHECK (status IN ('CONFIRMED', 'CANCELLED')),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX bookings_user_id_idx ON bookings (user_id, created_at DESC);

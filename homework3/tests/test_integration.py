import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import grpc
import httpx
import psycopg
import pytest

import flight_pb2 as pb
import flight_pb2_grpc as rpc

BASE_URL = os.getenv("TEST_BASE_URL", "http://localhost:8000")
FLIGHT_DB = os.getenv("TEST_FLIGHT_DB", "postgresql://flights:flights@flight-db:5432/flights")
FLIGHT_ID = None


@pytest.fixture
def flight():
    flight_id = uuid.uuid4()
    number = "T" + uuid.uuid4().hex[:8].upper()
    departure = datetime.now(timezone.utc) + timedelta(days=3)
    with psycopg.connect(FLIGHT_DB) as conn:
        conn.execute(
            "INSERT INTO flights (id, flight_number, airline, origin, destination, departure_at, arrival_at, total_seats, available_seats, price, status) "
            "VALUES (%s, %s, 'Test Air', 'SVO', 'LED', %s, %s, 1, 1, 100.00, 'SCHEDULED')",
            (flight_id, number, departure, departure + timedelta(hours=2)),
        )
    yield str(flight_id)
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        conn.execute("DELETE FROM bookings WHERE flight_id = %s", (flight_id,))
    with psycopg.connect(FLIGHT_DB) as conn:
        conn.execute("DELETE FROM seat_reservations WHERE flight_id = %s", (flight_id,))
        conn.execute("DELETE FROM flights WHERE id = %s", (flight_id,))


def booking_payload(flight_id):
    return {"user_id": "test-user", "flight_id": flight_id, "passenger_name": "Test Passenger",
            "passenger_email": "test@example.com", "seat_count": 1}


def test_booking_and_cache_invalidation(flight):
    with httpx.Client(base_url=BASE_URL, timeout=5) as client:
        search = client.get("/flights", params={"origin": "SVO", "destination": "LED"})
        assert next(item for item in search.json() if item["id"] == flight)["available_seats"] == 1
        before = client.get(f"/flights/{flight}")
        assert before.status_code == 200
        assert before.json()["available_seats"] == 1
        stub = rpc.FlightServiceStub(grpc.insecure_channel(os.getenv("FLIGHT_GRPC_TARGET", "flight-service:50051")))
        stub.UpdateFlight(pb.UpdateFlightRequest(id=flight, price="125.00"),
                          metadata=(("x-api-key", os.environ["GRPC_API_KEY"]),), timeout=2)
        assert client.get(f"/flights/{flight}").json()["price"] == "125.00"
        search = client.get("/flights", params={"origin": "SVO", "destination": "LED"})
        assert next(item for item in search.json() if item["id"] == flight)["price"] == "125.00"
        created = client.post("/bookings", json=booking_payload(flight))
        assert created.status_code == 201, created.text
        booking = created.json()
        assert booking["total_price"] == "125.00"
        assert booking["status"] == "CONFIRMED"
        assert client.get(f"/flights/{flight}").json()["available_seats"] == 0
        search = client.get("/flights", params={"origin": "SVO", "destination": "LED"})
        assert next(item for item in search.json() if item["id"] == flight)["available_seats"] == 0
        assert len(client.get("/bookings", params={"user_id": "test-user"}).json()) >= 1
        assert client.get(f"/bookings/{booking['id']}").json()["id"] == booking["id"]
        cancelled = client.post(f"/bookings/{booking['id']}/cancel")
        assert cancelled.status_code == 200, cancelled.text
        assert cancelled.json()["status"] == "CANCELLED"
        assert client.get(f"/flights/{flight}").json()["available_seats"] == 1
        search = client.get("/flights", params={"origin": "SVO", "destination": "LED"})
        assert next(item for item in search.json() if item["id"] == flight)["available_seats"] == 1
        assert client.post(f"/bookings/{booking['id']}/cancel").status_code == 409


def test_last_seat_is_not_double_booked(flight):
    def reserve(i):
        with httpx.Client(base_url=BASE_URL, timeout=5) as client:
            return client.post("/bookings", json={**booking_payload(flight), "user_id": f"race-{i}"})
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(reserve, [1, 2]))
    assert sorted(r.status_code for r in responses) == [201, 409], [r.text for r in responses]
    with httpx.Client(base_url=BASE_URL, timeout=5) as client:
        assert client.get(f"/flights/{flight}").json()["available_seats"] == 0


def test_grpc_idempotency_and_auth(flight):
    channel = grpc.insecure_channel(os.getenv("FLIGHT_GRPC_TARGET", "flight-service:50051"))
    stub = rpc.FlightServiceStub(channel)
    booking_id = str(uuid.uuid4())
    request = pb.ReserveSeatsRequest(flight_id=flight, booking_id=booking_id, seat_count=1)
    metadata = (("x-api-key", os.environ["GRPC_API_KEY"]),)
    with pytest.raises(grpc.RpcError) as error:
        stub.GetFlight(pb.GetFlightRequest(id=flight), timeout=2)
    assert error.value.code() == grpc.StatusCode.UNAUTHENTICATED
    first = stub.ReserveSeats(request, metadata=metadata, timeout=2)
    second = stub.ReserveSeats(request, metadata=metadata, timeout=2)
    assert first.id == second.id
    with httpx.Client(base_url=BASE_URL, timeout=5) as client:
        assert client.get(f"/flights/{flight}").json()["available_seats"] == 0
    stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=metadata, timeout=2)
    stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=metadata, timeout=2)
    with httpx.Client(base_url=BASE_URL, timeout=5) as client:
        assert client.get(f"/flights/{flight}").json()["available_seats"] == 1

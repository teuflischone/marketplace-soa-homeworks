import pytest
from fastapi import HTTPException

from booking_service import main as booking


def test_timeout_after_reserve_tries_to_release(monkeypatch):
    calls = []

    def fake_call(method, request):
        calls.append(method)
        if method == "GetFlight":
            return type("Flight", (), {"price": "100.00"})()
        if method == "ReserveSeats":
            raise HTTPException(503, "deadline exceeded")
        if method == "ReleaseReservation":
            raise HTTPException(404, "reservation not found")

    def unexpected_db():
        raise AssertionError("booking must not be created")

    monkeypatch.setattr(booking, "grpc_call", fake_call)
    monkeypatch.setattr(booking, "connect", unexpected_db)
    payload = booking.BookingInput(
        user_id="test", flight_id="11111111-1111-4111-8111-111111111111",
        passenger_name="Test Passenger", passenger_email="test@example.com", seat_count=1,
    )
    with pytest.raises(HTTPException) as exc:
        booking.create_booking(payload)
    assert exc.value.status_code == 503
    assert calls == ["GetFlight", "ReserveSeats", "ReleaseReservation"]

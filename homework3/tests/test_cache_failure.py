import uuid

import grpc
import psycopg
import pytest
from psycopg.rows import dict_row

import flight_pb2 as pb
from flight_service import main as flight_service
from test_integration import FLIGHT_DB, flight


class Aborted(Exception):
    def __init__(self, code):
        self.code = code


class Context:
    def abort(self, code, message):
        raise Aborted(code)


class BrokenCache:
    def invalidate_flight(self, flight_id):
        return False


def test_reservation_rolls_back_when_cache_cannot_be_cleared(flight, monkeypatch):
    monkeypatch.setattr(
        flight_service, "connect",
        lambda: psycopg.connect(FLIGHT_DB, row_factory=dict_row),
    )
    service = flight_service.FlightService.__new__(flight_service.FlightService)
    service.cache = BrokenCache()
    booking_id = str(uuid.uuid4())

    with pytest.raises(Aborted) as error:
        service.ReserveSeats(
            pb.ReserveSeatsRequest(flight_id=flight, booking_id=booking_id, seat_count=1),
            Context(),
        )
    assert error.value.code == grpc.StatusCode.UNAVAILABLE

    with psycopg.connect(FLIGHT_DB) as conn:
        available = conn.execute(
            "SELECT available_seats FROM flights WHERE id = %s", (flight,)
        ).fetchone()[0]
        reservations = conn.execute(
            "SELECT count(*) FROM seat_reservations WHERE booking_id = %s", (booking_id,)
        ).fetchone()[0]
    assert available == 1
    assert reservations == 0


def test_release_rolls_back_when_cache_cannot_be_cleared(flight, monkeypatch):
    monkeypatch.setattr(
        flight_service, "connect",
        lambda: psycopg.connect(FLIGHT_DB, row_factory=dict_row),
    )
    service = flight_service.FlightService.__new__(flight_service.FlightService)
    service.cache = type("WorkingCache", (), {"invalidate_flight": lambda self, flight_id: True})()
    booking_id = str(uuid.uuid4())
    service.ReserveSeats(
        pb.ReserveSeatsRequest(flight_id=flight, booking_id=booking_id, seat_count=1),
        Context(),
    )
    service.cache = BrokenCache()

    with pytest.raises(Aborted) as error:
        service.ReleaseReservation(
            pb.ReleaseReservationRequest(booking_id=booking_id), Context()
        )
    assert error.value.code == grpc.StatusCode.UNAVAILABLE

    with psycopg.connect(FLIGHT_DB) as conn:
        available = conn.execute(
            "SELECT available_seats FROM flights WHERE id = %s", (flight,)
        ).fetchone()[0]
        status = conn.execute(
            "SELECT status FROM seat_reservations WHERE booking_id = %s", (booking_id,)
        ).fetchone()[0]
    assert available == 0
    assert status == "ACTIVE"

import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import date
from decimal import Decimal

import grpc
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, EmailStr, Field, field_validator

import flight_pb2 as pb
from booking_service.grpc_client import CircuitOpen, FlightClient
from db import connect, migrate

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("booking-service")
client = FlightClient()


@asynccontextmanager
async def lifespan(app):
    migrate("booking")
    yield


app = FastAPI(title="Flight booking", lifespan=lifespan)


class BookingInput(BaseModel):
    user_id: str = Field(min_length=1)
    flight_id: uuid.UUID
    passenger_name: str = Field(min_length=1)
    passenger_email: EmailStr
    seat_count: int = Field(gt=0)

    @field_validator("user_id", "passenger_name")
    @classmethod
    def not_blank(cls, value):
        value = value.strip()
        if not value:
            raise ValueError("must not be blank")
        return value


def grpc_call(method, request):
    try:
        return client.call(method, request)
    except CircuitOpen as exc:
        raise HTTPException(503, str(exc)) from exc
    except grpc.RpcError as exc:
        status = {
            grpc.StatusCode.INVALID_ARGUMENT: 400,
            grpc.StatusCode.UNAUTHENTICATED: 502,
            grpc.StatusCode.NOT_FOUND: 404,
            grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
            grpc.StatusCode.ALREADY_EXISTS: 409,
            grpc.StatusCode.FAILED_PRECONDITION: 409,
            grpc.StatusCode.UNAVAILABLE: 503,
            grpc.StatusCode.DEADLINE_EXCEEDED: 503,
        }.get(exc.code(), 502)
        raise HTTPException(status, exc.details() or exc.code().name) from exc


def flight_json(flight):
    return {
        "id": flight.id,
        "flight_number": flight.flight_number,
        "airline": flight.airline,
        "origin": flight.origin,
        "destination": flight.destination,
        "departure_at": flight.departure_at.ToDatetime().isoformat() + "Z",
        "arrival_at": flight.arrival_at.ToDatetime().isoformat() + "Z",
        "total_seats": flight.total_seats,
        "available_seats": flight.available_seats,
        "price": flight.price,
        "status": pb.FlightStatus.Name(flight.status).removeprefix("FLIGHT_STATUS_"),
    }


def booking_json(row):
    return {
        "id": str(row["id"]), "user_id": row["user_id"],
        "flight_id": str(row["flight_id"]), "passenger_name": row["passenger_name"],
        "passenger_email": row["passenger_email"], "seat_count": row["seat_count"],
        "total_price": str(row["total_price"]), "status": row["status"],
        "created_at": row["created_at"].isoformat(),
    }


@app.get("/flights")
def search_flights(origin: str, destination: str, date: date | None = None):
    request = pb.SearchFlightsRequest(origin=origin, destination=destination)
    if date is not None:
        request.date = date.isoformat()
    response = grpc_call("SearchFlights", request)
    return [flight_json(flight) for flight in response.flights]


@app.get("/flights/{flight_id}")
def get_flight(flight_id: uuid.UUID):
    return flight_json(grpc_call("GetFlight", pb.GetFlightRequest(id=str(flight_id))))


@app.post("/bookings", status_code=201)
def create_booking(payload: BookingInput):
    flight = grpc_call("GetFlight", pb.GetFlightRequest(id=str(payload.flight_id)))
    booking_id = uuid.uuid4()
    try:
        grpc_call("ReserveSeats", pb.ReserveSeatsRequest(
            flight_id=str(payload.flight_id), seat_count=payload.seat_count, booking_id=str(booking_id)
        ))
    except HTTPException as exc:
        # После таймаута ответ мог потеряться, хотя места уже заняты.
        if exc.status_code == 503:
            try:
                grpc_call("ReleaseReservation", pb.ReleaseReservationRequest(booking_id=str(booking_id)))
            except HTTPException as release_error:
                if release_error.status_code != 404:
                    log.warning("could not release reservation %s after failed response: %s", booking_id, release_error)
        raise
    total = Decimal(flight.price) * payload.seat_count
    try:
        with connect() as conn:
            with conn.transaction():
                row = conn.execute(
                    "INSERT INTO bookings (id, user_id, flight_id, passenger_name, passenger_email, seat_count, total_price, status) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, 'CONFIRMED') RETURNING *",
                    (booking_id, payload.user_id.strip(), payload.flight_id, payload.passenger_name.strip(),
                     str(payload.passenger_email), payload.seat_count, total),
                ).fetchone()
    except Exception:
        # The reservation is compensated if the local database insert fails.
        try:
            grpc_call("ReleaseReservation", pb.ReleaseReservationRequest(booking_id=str(booking_id)))
        except Exception:
            log.exception("could not compensate reservation for booking %s", booking_id)
        raise
    return booking_json(row)


@app.get("/bookings/{booking_id}")
def get_booking(booking_id: uuid.UUID):
    with connect() as conn:
        row = conn.execute("SELECT * FROM bookings WHERE id = %s", (booking_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "booking not found")
    return booking_json(row)


@app.post("/bookings/{booking_id}/cancel")
def cancel_booking(booking_id: uuid.UUID):
    with connect() as conn:
        with conn.transaction():
            row = conn.execute("SELECT * FROM bookings WHERE id = %s FOR UPDATE", (booking_id,)).fetchone()
            if row is None:
                raise HTTPException(404, "booking not found")
            if row["status"] != "CONFIRMED":
                raise HTTPException(409, "booking already cancelled")
            grpc_call("ReleaseReservation", pb.ReleaseReservationRequest(booking_id=str(booking_id)))
            row = conn.execute(
                "UPDATE bookings SET status = 'CANCELLED' WHERE id = %s RETURNING *", (booking_id,)
            ).fetchone()
    return booking_json(row)


@app.get("/bookings")
def list_bookings(user_id: str = Query(min_length=1)):
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM bookings WHERE user_id = %s ORDER BY created_at DESC", (user_id,)
        ).fetchall()
    return [booking_json(row) for row in rows]

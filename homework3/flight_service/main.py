import hmac
import logging
import os
import re
import uuid
from concurrent import futures
from datetime import date, timezone
from decimal import Decimal, InvalidOperation

import grpc
import redis
from google.protobuf.timestamp_pb2 import Timestamp
from redis.sentinel import Sentinel

import flight_pb2 as pb
import flight_pb2_grpc as rpc
from db import connect, migrate

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("flight-service")
# Чтение из БД и изменение рейса не должны одновременно записывать кеш.
CACHE_LOCK = 42_310

FLIGHT_STATUS = {
    "SCHEDULED": pb.FLIGHT_STATUS_SCHEDULED,
    "DEPARTED": pb.FLIGHT_STATUS_DEPARTED,
    "CANCELLED": pb.FLIGHT_STATUS_CANCELLED,
    "COMPLETED": pb.FLIGHT_STATUS_COMPLETED,
}
RESERVATION_STATUS = {
    "ACTIVE": pb.RESERVATION_STATUS_ACTIVE,
    "RELEASED": pb.RESERVATION_STATUS_RELEASED,
    "EXPIRED": pb.RESERVATION_STATUS_EXPIRED,
}


def timestamp(value):
    result = Timestamp()
    result.FromDatetime(value.astimezone(timezone.utc))
    return result


def flight_message(row):
    return pb.Flight(
        id=str(row["id"]), flight_number=row["flight_number"], airline=row["airline"],
        origin=row["origin"].strip(), destination=row["destination"].strip(),
        departure_at=timestamp(row["departure_at"]), arrival_at=timestamp(row["arrival_at"]),
        total_seats=row["total_seats"], available_seats=row["available_seats"],
        price=str(row["price"]), status=FLIGHT_STATUS[row["status"]],
    )


def reservation_message(row):
    return pb.SeatReservation(
        id=str(row["id"]), booking_id=str(row["booking_id"]),
        flight_id=str(row["flight_id"]), seat_count=row["seat_count"],
        status=RESERVATION_STATUS[row["status"]],
    )


def parse_uuid(value, field, context):
    try:
        return uuid.UUID(value)
    except (ValueError, AttributeError):
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"{field} must be UUID")


class ApiKeyInterceptor(grpc.ServerInterceptor):
    def __init__(self, key):
        self.key = key

    def intercept_service(self, continuation, details):
        handler = continuation(details)
        if handler is None:
            return None
        def authenticated(request, context):
            metadata = dict(context.invocation_metadata())
            supplied = metadata.get("x-api-key", "")
            if not hmac.compare_digest(supplied, self.key):
                context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid service credentials")
            return handler.unary_unary(request, context)
        return grpc.unary_unary_rpc_method_handler(
            authenticated, request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class Cache:
    def __init__(self):
        sentinel = Sentinel(
            [(os.getenv("REDIS_SENTINEL_HOST", "redis-sentinel"), int(os.getenv("REDIS_SENTINEL_PORT", "26379")))],
            socket_timeout=0.3,
        )
        self.client = sentinel.master_for(os.getenv("REDIS_MASTER_NAME", "mymaster"), socket_timeout=0.3)
        self.ttl = int(os.getenv("CACHE_TTL_SECONDS", "420"))

    def get(self, key, message_type):
        try:
            data = self.client.get(key)
            if data is None:
                log.info("cache miss %s", key)
                return None
            log.info("cache hit %s", key)
            result = message_type()
            result.ParseFromString(data)
            return result
        except redis.RedisError as exc:
            log.warning("cache unavailable: %s", exc)
            return None

    def put(self, key, value):
        try:
            self.client.setex(key, self.ttl, value.SerializeToString())
        except redis.RedisError as exc:
            log.warning("cache write unavailable: %s", exc)

    def invalidate_flight(self, flight_id):
        try:
            keys = list(self.client.scan_iter(match="search:*"))
            self.client.delete(f"flight:{flight_id}", *keys)
            log.info("cache invalidated flight=%s search_keys=%s", flight_id, len(keys))
            return True
        except redis.RedisError as exc:
            log.warning("cache invalidation unavailable: %s", exc)
            return False


class FlightService(rpc.FlightServiceServicer):
    def __init__(self):
        self.cache = Cache()

    def SearchFlights(self, request, context):
        origin = request.origin.upper()
        destination = request.destination.upper()
        if not re.fullmatch(r"[A-Z]{3}", origin) or not re.fullmatch(r"[A-Z]{3}", destination) or origin == destination:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "valid origin and destination are required")
        travel_date = None
        if request.HasField("date"):
            try:
                travel_date = date.fromisoformat(request.date)
                if travel_date.isoformat() != request.date:
                    raise ValueError()
            except ValueError:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "date must be YYYY-MM-DD")
        key = f"search:{origin}:{destination}:{travel_date or 'all'}"
        cached = self.cache.get(key, pb.SearchFlightsResponse)
        if cached is not None:
            return cached
        sql = "SELECT * FROM flights WHERE origin = %s AND destination = %s AND status = 'SCHEDULED'"
        args = [origin, destination]
        if travel_date:
            sql += " AND departure_at >= %s AND departure_at < %s"
            from datetime import datetime, timedelta
            start = datetime.combine(travel_date, datetime.min.time(), timezone.utc)
            args.extend([start, start + timedelta(days=1)])
        sql += " ORDER BY departure_at"
        with connect() as conn:
            conn.execute("SELECT pg_advisory_xact_lock_shared(%s)", (CACHE_LOCK,))
            cached = self.cache.get(key, pb.SearchFlightsResponse)
            if cached is not None:
                return cached
            rows = conn.execute(sql, args).fetchall()
            result = pb.SearchFlightsResponse(flights=[flight_message(row) for row in rows])
            self.cache.put(key, result)
            return result

    def GetFlight(self, request, context):
        flight_id = parse_uuid(request.id, "id", context)
        key = f"flight:{flight_id}"
        cached = self.cache.get(key, pb.Flight)
        if cached is not None:
            return cached
        with connect() as conn:
            conn.execute("SELECT pg_advisory_xact_lock_shared(%s)", (CACHE_LOCK,))
            cached = self.cache.get(key, pb.Flight)
            if cached is not None:
                return cached
            row = conn.execute("SELECT * FROM flights WHERE id = %s", (flight_id,)).fetchone()
            if row is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")
            result = flight_message(row)
            self.cache.put(key, result)
            return result

    def ReserveSeats(self, request, context):
        flight_id = parse_uuid(request.flight_id, "flight_id", context)
        booking_id = parse_uuid(request.booking_id, "booking_id", context)
        if request.seat_count <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "seat_count must be positive")
        with connect() as conn:
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (CACHE_LOCK,))
                flight = conn.execute("SELECT * FROM flights WHERE id = %s FOR UPDATE", (flight_id,)).fetchone()
                if flight is None:
                    context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")
                existing = conn.execute("SELECT * FROM seat_reservations WHERE booking_id = %s", (booking_id,)).fetchone()
                if existing is not None:
                    if existing["flight_id"] != flight_id or existing["seat_count"] != request.seat_count:
                        context.abort(grpc.StatusCode.ALREADY_EXISTS, "booking_id belongs to another reservation")
                    if existing["status"] != "ACTIVE":
                        context.abort(grpc.StatusCode.FAILED_PRECONDITION, "reservation already released")
                    return reservation_message(existing)
                if flight["status"] != "SCHEDULED":
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, "flight is not scheduled")
                if flight["available_seats"] < request.seat_count:
                    context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "not enough available seats")
                row = conn.execute(
                    "INSERT INTO seat_reservations (id, booking_id, flight_id, seat_count, status) "
                    "VALUES (%s, %s, %s, %s, 'ACTIVE') ON CONFLICT (booking_id) DO NOTHING RETURNING *",
                    (uuid.uuid4(), booking_id, flight_id, request.seat_count),
                ).fetchone()
                if row is None:
                    context.abort(grpc.StatusCode.ALREADY_EXISTS, "booking_id belongs to another reservation")
                conn.execute("UPDATE flights SET available_seats = available_seats - %s WHERE id = %s", (request.seat_count, flight_id))
                if not self.cache.invalidate_flight(flight_id):
                    context.abort(grpc.StatusCode.UNAVAILABLE, "cache unavailable")
        return reservation_message(row)

    def ReleaseReservation(self, request, context):
        booking_id = parse_uuid(request.booking_id, "booking_id", context)
        with connect() as conn:
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (CACHE_LOCK,))
                existing = conn.execute("SELECT flight_id FROM seat_reservations WHERE booking_id = %s", (booking_id,)).fetchone()
                if existing is None:
                    context.abort(grpc.StatusCode.NOT_FOUND, "reservation not found")
                conn.execute("SELECT 1 FROM flights WHERE id = %s FOR UPDATE", (existing["flight_id"],))
                row = conn.execute("SELECT * FROM seat_reservations WHERE booking_id = %s FOR UPDATE", (booking_id,)).fetchone()
                if row["status"] == "ACTIVE":
                    conn.execute("UPDATE flights SET available_seats = available_seats + %s WHERE id = %s", (row["seat_count"], row["flight_id"]))
                    row = conn.execute("UPDATE seat_reservations SET status = 'RELEASED' WHERE id = %s RETURNING *", (row["id"],)).fetchone()
                    if not self.cache.invalidate_flight(row["flight_id"]):
                        context.abort(grpc.StatusCode.UNAVAILABLE, "cache unavailable")
        return reservation_message(row)

    def UpdateFlight(self, request, context):
        flight_id = parse_uuid(request.id, "id", context)
        price = None
        if request.price:
            try:
                price = Decimal(request.price)
                if price <= 0 or price.as_tuple().exponent < -2:
                    raise InvalidOperation()
            except (InvalidOperation, ValueError):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "price must be positive with at most 2 decimals")
        status = None
        if request.status:
            status = next((name for name, number in FLIGHT_STATUS.items() if number == request.status), None)
            if status is None:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "invalid flight status")
        if price is None and status is None:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "price or status is required")
        with connect() as conn:
            with conn.transaction():
                conn.execute("SELECT pg_advisory_xact_lock(%s)", (CACHE_LOCK,))
                row = conn.execute(
                    "UPDATE flights SET price = COALESCE(%s, price), status = COALESCE(%s, status) WHERE id = %s RETURNING *",
                    (price, status, flight_id),
                ).fetchone()
                if row is None:
                    context.abort(grpc.StatusCode.NOT_FOUND, "flight not found")
                if not self.cache.invalidate_flight(flight_id):
                    context.abort(grpc.StatusCode.UNAVAILABLE, "cache unavailable")
        return flight_message(row)


def main():
    migrate("flight")
    key = os.environ["GRPC_API_KEY"]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16), interceptors=[ApiKeyInterceptor(key)])
    rpc.add_FlightServiceServicer_to_server(FlightService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    log.info("Flight Service listening on 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    main()

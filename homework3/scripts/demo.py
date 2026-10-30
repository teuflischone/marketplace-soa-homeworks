"""Небольшая проверка бронирования через REST API."""
import json
import os
import uuid
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE = os.getenv("BASE_URL", "http://localhost:8000")
FLIGHT_ID = "11111111-1111-4111-8111-111111111111"


def api(method, path, body=None):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    request = Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urlopen(request, timeout=10) as response:
            return json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"{method} {path}: HTTP {exc.code} {exc.read().decode()}") from exc


def main():
    user_id = "demo-" + uuid.uuid4().hex[:8]
    search = api("GET", "/flights?" + urlencode({
        "origin": "SVO", "destination": "LED", "date": "2026-11-10"
    }))
    assert any(item["id"] == FLIGHT_ID for item in search), "тестовый рейс не найден"
    flight = api("GET", f"/flights/{FLIGHT_ID}")
    before = flight["available_seats"]
    assert before >= 2, "на тестовом рейсе меньше двух свободных мест"
    print(f"Рейс {flight['flight_number']}: свободно {before}, цена {flight['price']}")

    booking = api("POST", "/bookings", {
        "user_id": user_id,
        "flight_id": FLIGHT_ID,
        "passenger_name": "Иван Иванов",
        "passenger_email": "ivan@example.com",
        "seat_count": 2,
    })
    booking_id = booking["id"]
    assert booking["status"] == "CONFIRMED"
    assert api("GET", f"/bookings/{booking_id}")["id"] == booking_id
    assert any(item["id"] == booking_id for item in api("GET", "/bookings?" + urlencode({"user_id": user_id})))
    after_booking = api("GET", f"/flights/{FLIGHT_ID}")["available_seats"]
    assert after_booking == before - 2
    print(f"Бронь {booking_id}: {booking['status']}, сумма {booking['total_price']}")
    print(f"После бронирования свободно {after_booking}")

    cancelled = api("POST", f"/bookings/{booking_id}/cancel")
    after_cancel = api("GET", f"/flights/{FLIGHT_ID}")["available_seats"]
    assert cancelled["status"] == "CANCELLED" and after_cancel == before
    print(f"После отмены: {cancelled['status']}, свободно {after_cancel}")
    print("Проверка прошла")


if __name__ == "__main__":
    main()

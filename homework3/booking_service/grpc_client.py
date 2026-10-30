"""gRPC transport middleware: metadata, retry and circuit breaker."""
import logging
import os
import threading
import time
from collections import deque

import grpc

import flight_pb2_grpc as rpc

log = logging.getLogger("booking-service")
TRANSIENT = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}


class CircuitOpen(Exception):
    pass


class FlightClient:
    def __init__(self, stub=None, api_key=None, threshold=None, open_seconds=None, window_seconds=None):
        if stub is None:
            channel = grpc.insecure_channel(os.getenv("FLIGHT_GRPC_TARGET", "flight-service:50051"))
            stub = rpc.FlightServiceStub(channel)
        self.stub = stub
        self.api_key = api_key if api_key is not None else os.environ["GRPC_API_KEY"]
        self.threshold = threshold or int(os.getenv("CB_FAILURE_THRESHOLD", "5"))
        self.open_seconds = open_seconds or float(os.getenv("CB_OPEN_SECONDS", "15"))
        self.window_seconds = window_seconds or float(os.getenv("CB_WINDOW_SECONDS", "60"))
        self.state = "CLOSED"
        self.failures = deque()
        self.opened_at = 0.0
        self.probe_running = False
        self.lock = threading.Lock()

    def _before(self):
        with self.lock:
            now = time.monotonic()
            if self.state == "OPEN":
                if now - self.opened_at < self.open_seconds:
                    raise CircuitOpen("Flight Service temporarily unavailable")
                self.state = "HALF_OPEN"
                self.probe_running = False
                log.warning("circuit OPEN -> HALF_OPEN")
            if self.state == "HALF_OPEN":
                if self.probe_running:
                    raise CircuitOpen("Flight Service recovery check in progress")
                self.probe_running = True

    def _success(self):
        with self.lock:
            if self.state == "HALF_OPEN":
                log.warning("circuit HALF_OPEN -> CLOSED")
            self.state = "CLOSED"
            self.probe_running = False
            self.failures.clear()

    def _failure(self, transient):
        with self.lock:
            now = time.monotonic()
            if self.state == "HALF_OPEN":
                self.state = "OPEN"
                self.opened_at = now
                self.probe_running = False
                log.warning("circuit HALF_OPEN -> OPEN")
                return
            if not transient:
                return
            self.failures.append(now)
            while self.failures and now - self.failures[0] > self.window_seconds:
                self.failures.popleft()
            if len(self.failures) >= self.threshold:
                self.state = "OPEN"
                self.opened_at = now
                log.warning("circuit CLOSED -> OPEN (%s failures)", len(self.failures))

    def call(self, method, request):
        self._before()
        try:
            for attempt in range(3):
                try:
                    result = getattr(self.stub, method)(
                        request, timeout=2.0, metadata=(("x-api-key", self.api_key),)
                    )
                    self._success()
                    return result
                except grpc.RpcError as exc:
                    if exc.code() not in TRANSIENT or attempt == 2:
                        self._failure(exc.code() in TRANSIENT)
                        raise
                    delay = 0.1 * (2 ** attempt)
                    log.warning("%s %s, retry %s/3 in %.1fs", method, exc.code().name, attempt + 2, delay)
                    time.sleep(delay)
        except grpc.RpcError:
            raise
        except Exception:
            self._failure(True)
            raise

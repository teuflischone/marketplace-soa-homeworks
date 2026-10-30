import grpc
import pytest

from booking_service.grpc_client import CircuitOpen, FlightClient


class CallError(grpc.RpcError):
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code


class Stub:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = 0

    def GetFlight(self, request, **kwargs):
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_retry_only_transient(monkeypatch):
    monkeypatch.setattr("booking_service.grpc_client.time.sleep", lambda _: None)
    stub = Stub([CallError(grpc.StatusCode.UNAVAILABLE), CallError(grpc.StatusCode.DEADLINE_EXCEEDED), "ok"])
    client = FlightClient(stub=stub, api_key="test")
    assert client.call("GetFlight", object()) == "ok"
    assert stub.calls == 3

    stub = Stub([CallError(grpc.StatusCode.RESOURCE_EXHAUSTED)])
    client = FlightClient(stub=stub, api_key="test")
    with pytest.raises(CallError):
        client.call("GetFlight", object())
    assert stub.calls == 1


def test_circuit_open_and_half_open(monkeypatch):
    monkeypatch.setattr("booking_service.grpc_client.time.sleep", lambda _: None)
    stub = Stub([CallError(grpc.StatusCode.UNAVAILABLE)] * 6 + ["ok"])
    client = FlightClient(stub=stub, api_key="test", threshold=2, open_seconds=10)
    for _ in range(2):
        with pytest.raises(CallError):
            client.call("GetFlight", object())
    assert client.state == "OPEN"
    assert stub.calls == 6
    with pytest.raises(CircuitOpen):
        client.call("GetFlight", object())
    assert stub.calls == 6
    client.opened_at -= 11
    assert client.call("GetFlight", object()) == "ok"
    assert client.state == "CLOSED"

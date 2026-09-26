import asyncio
import json

import pytest

from app.home_context import (
    core_base, fetch_home_config, find_device_area, lookup_device_area,
    resolve_time_zone, resolve_units,
)
import app.home_context as home_context


DEVICES = [
    {"connections": [["mac", "AA:BB:CC:DD:EE:FF"]], "area_id": "kitchen"},
    {"connections": [["mac", "11:22:33:44:55:66"]], "area_id": None},
]
AREAS = [{"area_id": "kitchen", "name": "Kitchen"}]


@pytest.mark.parametrize(
    ("mac", "expected"),
    [
        ("aa:bb:cc:dd:ee:ff", "Kitchen"),
        ("AA:BB:CC:DD:EE:FF", "Kitchen"),
        ("11:22:33:44:55:66", None),
        ("00:00:00:00:00:00", None),
    ],
)
def test_find_device_area(mac, expected):
    assert find_device_area(DEVICES, AREAS, mac) == expected
    assert find_device_area(DEVICES, [], mac) is None


def test_home_config_resolution():
    config = {
        "time_zone": "America/Chicago",
        "unit_system": {"name": "us_customary"},
        "location_name": "Temple",
        "country": "US",
    }
    assert core_base("") == "http://supervisor/core"
    assert core_base("https://ha.example:8123/prefix/api/mcp") == "https://ha.example:8123/prefix"
    with pytest.raises(ValueError):
        core_base("https://ha.example/api/other")
    assert resolve_time_zone(config, "UTC") == "America/Chicago"
    assert resolve_time_zone({"time_zone": "not/a-zone"}, "Europe/Amsterdam") == "Europe/Amsterdam"
    assert resolve_time_zone({}, None) is None
    assert resolve_units(config) == "US customary (Fahrenheit, miles)"
    assert resolve_units({"unit_system": {"name": "metric"}}) == "metric (Celsius, kilometers)"
    assert resolve_units({"unit_system": {"temperature": "°F", "length": "mi"}}) == "US customary (Fahrenheit, miles)"
    assert resolve_units({}) is None


def test_fetch_home_config_uses_core_endpoint_and_token(monkeypatch):
    async def scenario():
        calls = []

        class Response:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def raise_for_status(self):
                pass

            async def json(self):
                return {"time_zone": "America/Chicago", "location_name": "Home"}

        class Session:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def get(self, url, headers):
                calls.append((url, headers))
                return Response()

        def client_session(*, timeout):
            assert timeout.total == 2.5
            return Session()

        monkeypatch.setattr(home_context.aiohttp, "ClientSession", client_session)
        assert (await fetch_home_config("http://supervisor/core", "secret"))["time_zone"] == "America/Chicago"
        assert calls == [
            ("http://supervisor/core/api/config", {"Authorization": "Bearer secret"})
        ]

    asyncio.run(scenario())


def test_one_shot_area_lookup_auth_and_registry_requests(monkeypatch):
    async def scenario():
        messages = [
            {"type": "auth_required"},
            {"type": "auth_ok"},
            {"type": "result", "id": 1, "success": True, "result": DEVICES},
            {"type": "result", "id": 2, "success": True, "result": AREAS},
        ]
        sent = []

        class Socket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def send(self, value):
                sent.append(json.loads(value))

            async def recv(self):
                return json.dumps(messages.pop(0))

        def connect(url, **kwargs):
            assert url == "wss://ha.example:8123/prefix/api/websocket"
            return Socket()

        monkeypatch.setattr(home_context.websockets, "connect", connect)
        assert await lookup_device_area(
            "https://ha.example:8123/prefix", "secret", "aa:bb:cc:dd:ee:ff"
        ) == "Kitchen"
        assert sent == [
            {"type": "auth", "access_token": "secret"},
            {"id": 1, "type": "config/device_registry/list"},
            {"id": 2, "type": "config/area_registry/list"},
        ]

    asyncio.run(scenario())

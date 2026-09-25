"""Device timer protocol and Realtime tool coverage."""
import asyncio
import json
from types import SimpleNamespace

import pytest

from app.raw_audio_serializer import RawAudioSerializer
from app.timer_tool import TimerBridge, create_timer_tool_handler, get_timer_tool_definitions
from app.websocket_handler import WebSocketHandler
import app.timer_tool as timer_module


def timer(timer_id, name, remaining=30):
    return {
        "id": timer_id, "name": name, "total_s": 60,
        "remaining_s": remaining, "ringing": False,
    }


class Device:
    def __init__(self, serializer):
        self.serializer = serializer
        self.sent = []

    async def send(self, payload):
        self.sent.append(json.loads(payload))

    async def reply(self, payload):
        await self.serializer.deserialize(json.dumps(payload))

    async def ack(self, sent, timers=None, ok=True, error=None):
        message = {
            "type": "timer_ack", "request_id": sent["request_id"],
            "ok": ok, "timers": timers if timers is not None else [],
        }
        if error:
            message["error"] = error
        await self.reply(message)


def setup():
    handler = WebSocketHandler()
    serializer = RawAudioSerializer()
    serializer.set_timer_message_handler(handler.timer_bridge.handle_message)
    device = Device(serializer)
    handler._websockets.add(device)
    return handler.timer_bridge, serializer, device, handler


def test_ack_correlation_and_remaining_times():
    async def scenario():
        bridge, _, device, _ = setup()
        first = asyncio.create_task(bridge.set_timer({"duration_seconds": 60, "name": "Tea"}))
        second = asyncio.create_task(bridge.list_timers())
        await asyncio.sleep(0)
        start, listing = device.sent
        assert start["type"] == "timer_start"
        assert start["duration_s"] == 60
        assert start["name"] == "Tea"
        assert start["id"].startswith("t")
        assert listing["type"] == "timer_list"
        assert start["request_id"] != listing["request_id"]
        await device.ack(listing, [timer("one", "Tea", 42)])
        assert not first.done()
        assert (await second)["timers"][0]["remaining_s"] == 42
        await device.ack(start, [timer("one", "Tea", 58)])
        assert (await first)["success"]
        assert bridge.timers[0]["remaining_s"] == 58
        assert not bridge._pending

    asyncio.run(scenario())


def test_timeout_and_negative_ack_are_failures(monkeypatch):
    async def scenario():
        bridge, _, device, _ = setup()
        monkeypatch.setattr(timer_module, "ACK_TIMEOUT_S", 0.01)
        timed_out = await bridge.set_timer({"minutes": 1})
        assert not timed_out["success"]
        assert "did not confirm" in timed_out["error"]
        await device.ack(device.sent[0], [timer("late", "Late")])
        assert bridge.timers == []
        rejected = asyncio.create_task(bridge.list_timers())
        await asyncio.sleep(0)
        await device.ack(device.sent[1], [], ok=False, error="invalid")
        assert not (await rejected)["success"]
        assert not bridge._pending

    asyncio.run(scenario())


def test_timeout_covers_blocked_send(monkeypatch):
    async def scenario():
        bridge, _, device, _ = setup()
        monkeypatch.setattr(timer_module, "ACK_TIMEOUT_S", 0.01)

        async def blocked_send(payload):
            await asyncio.sleep(1)

        device.send = blocked_send
        result = await bridge.list_timers()
        assert not result["success"]
        assert not bridge._pending

    asyncio.run(scenario())


def test_no_device_and_disconnect_fail_without_claiming_success():
    async def scenario():
        bridge, _, device, handler = setup()
        handler._websockets.clear()
        result = await bridge.set_timer({"seconds": 5})
        assert not result["success"]
        assert device.sent == []
        handler._websockets.add(device)
        pending = asyncio.create_task(bridge.list_timers())
        await asyncio.sleep(0)
        handler._websockets.remove(device)
        bridge.disconnected(device)
        assert not (await pending)["success"]
        assert not bridge._pending

    asyncio.run(scenario())


def test_cancel_name_matches_case_insensitively_and_reports_ambiguity():
    async def scenario():
        bridge, _, device, _ = setup()
        ambiguous = asyncio.create_task(bridge.cancel_timer({"name": "tea"}))
        await asyncio.sleep(0)
        await device.ack(device.sent[0], [timer("one", "Tea"), timer("two", "TEA")])
        result = await ambiguous
        assert not result["success"]
        assert [item["id"] for item in result["timers"]] == ["one", "two"]
        assert len(device.sent) == 1

        unique = asyncio.create_task(bridge.cancel_timer({"name": "coffee"}))
        await asyncio.sleep(0)
        await device.ack(device.sent[1], [timer("three", "Coffee")])
        await asyncio.sleep(0)
        assert device.sent[2]["type"] == "timer_cancel"
        assert device.sent[2]["id"] == "three"
        await device.ack(device.sent[2], [])
        assert (await unique)["success"]
        assert bridge.timers == []

    asyncio.run(scenario())


def test_timer_state_resync_and_finished_message():
    async def scenario():
        bridge, _, device, _ = setup()
        await device.reply({"type": "timer_state", "timers": [timer("one", "Tea", 17)]})
        assert bridge.timers[0]["remaining_s"] == 17
        await device.reply({"type": "timer_finished", "id": "one", "name": "Tea"})
        assert bridge.timers[0]["remaining_s"] == 17
        bridge.connected(device)
        assert bridge.timers == []
        await device.reply({"type": "timer_state", "timers": [timer("two", "Coffee", 9)]})
        assert bridge.timers[0]["id"] == "two"

    asyncio.run(scenario())


def test_cancel_all_and_component_duration():
    async def scenario():
        bridge, _, device, _ = setup()
        setting = asyncio.create_task(bridge.set_timer({
            "hours": 1, "minutes": 2, "seconds": 3,
        }))
        await asyncio.sleep(0)
        assert device.sent[0]["duration_s"] == 3723
        await device.ack(device.sent[0], [timer("one", "")])
        assert (await setting)["success"]

        cancelling = asyncio.create_task(bridge.cancel_timer({"all": True}))
        await asyncio.sleep(0)
        assert device.sent[1] == {
            "type": "timer_cancel",
            "request_id": device.sent[1]["request_id"],
            "all": True,
        }
        await device.ack(device.sent[1])
        assert (await cancelling)["success"]

    asyncio.run(scenario())


def test_tool_definitions_and_model_result_shape():
    definitions = get_timer_tool_definitions()
    assert {tool["name"] for tool in definitions} == {
        "set_timer", "cancel_timer", "list_timers",
    }
    for definition in definitions:
        assert definition["type"] == "function"
        assert definition["parameters"]["type"] == "object"
        assert set(definition["parameters"]["required"]) <= set(
            definition["parameters"]["properties"]
        )
    assert definitions[0]["parameters"]["properties"]["duration_seconds"]["type"] == "integer"
    assert set(definitions[1]["parameters"]["properties"]) == {"name", "id", "all"}

    async def scenario():
        bridge, _, _, handler = setup()
        handler._websockets.clear()
        outputs = []

        async def callback(value):
            outputs.append(json.loads(value))

        params = SimpleNamespace(arguments={}, result_callback=callback)
        await create_timer_tool_handler(bridge, "list_timers")(params)
        assert outputs == [{"success": False, "error": "No voice device connected."}]
        assert not (await bridge.set_timer({"duration_seconds": 0}))["success"]
        assert not (await bridge.set_timer({"duration_seconds": True}))["success"]

    asyncio.run(scenario())

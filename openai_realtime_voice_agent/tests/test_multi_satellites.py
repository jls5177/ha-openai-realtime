"""Real Pipecat pipeline tasks over real websocket connections, without OpenAI I/O."""
import asyncio
import json
from types import SimpleNamespace
import time

import aiohttp
import pytest
import websockets
from pipecat.frames.frames import (
    BotStartedSpeakingFrame, BotStoppedSpeakingFrame, OutputAudioRawFrame,
)
from pipecat.processors.frame_processor import FrameProcessor
from pipecat.processors.aggregators.llm_context import LLMContext

from app.session_manager import SessionManager
from app.satellites import SatelliteRegistry, SatelliteRouter, Diagnostics
from app.main import Application
import app.websocket_handler as handler_module
import app.main as main_module


class FakeRealtime(FrameProcessor):
    def __init__(self, instructions):
        super().__init__()
        self._session_properties = SimpleNamespace(instructions=instructions)
        self._context = LLMContext()
        self._llm_needs_conversation_setup = False
        self.events = []
        self.functions = {}

    def register_function(self, name, handler, *args, **kwargs):
        self.functions[name] = handler

    def event_handler(self, name):
        return lambda callback: callback

    async def send_client_event(self, event):
        self.events.append(event)

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        await self.push_frame(frame, direction)


class StubApp:
    def __init__(self):
        self.session_manager = SessionManager()
        self.interrupt_response = False
        self.follow_up_ms = 0
        self.follow_up_open_delay_ms = 700
        self.wake_open_delay_ms = 700
        self.playback_prebuffer_ms = 0
        self.turn_detection_type = "server_vad"
        self.semantic_vad_create_response = False
        self.tail_device = ""
        self.instructions = "Test instructions"
        self.personality = "monday"
        self.home_location = ""
        self.time_zone = None
        self.units = None
        self.clock_tool = "get_current_time"
        self.ha_base, self.ha_token = "http://home", "test"
        self.created = []

    def make_recorder(self, mac):
        return None

    async def create_openai_service(self, session):
        service = FakeRealtime(self.instructions)
        self.created.append(service)
        self.session_manager.set_current_service(session.mac, service)
        return service


async def ready(registry, count):
    for _ in range(150):
        if len(registry.connected()) == count and all(
            s.session.pipeline_task for s in registry.connected()
        ):
            await asyncio.sleep(.05)
            return
        await asyncio.sleep(.01)
    raise AssertionError("sessions did not start")


async def connect(router, mac, name, token=None):
    client = await websockets.connect(f"ws://127.0.0.1:{router.port}/device",
                                      max_queue=16)
    payload = {"type": "start", "mac": mac, "name": name, "caps": ["announce"]}
    if token is not None:
        payload["token"] = token
    await client.send(json.dumps(payload))
    return client


MAC1 = "aa:bb:cc:dd:ee:01"
MAC2 = "aa:bb:cc:dd:ee:02"


@pytest.fixture
def no_area(monkeypatch):
    async def lookup(base, token, mac):
        return "Kitchen" if mac == MAC1 else "Office"

    monkeypatch.setattr(handler_module, "lookup_device_area", lookup)


@pytest.mark.parametrize("interrupt_response", [False, True])
def test_two_sessions_takeover_context_and_status(tmp_path, no_area, interrupt_response):
    async def scenario():
        app = StubApp()
        app.interrupt_response = interrupt_response
        registry = SatelliteRegistry(tmp_path / "satellites.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0, token="shared")
        diag = Diagnostics(registry, router, token="shared", port=0, enabled=True)
        await router.start()
        await diag.start()
        try:
            a = await connect(router, MAC1, "Kitchen", token="shared")
            b = await connect(router, MAC2, "Office", token="shared")
            await ready(registry, 2)
            hello = json.loads(await a.recv())
            assert hello["type"] == "hello"
            assert hello["interrupt_response"] is interrupt_response
            assert json.loads(await b.recv())["type"] == "hello"
            sa = registry.get(MAC1).session
            sb = registry.get(MAC2).session
            await sa.phase_emitter._emit("replying")
            assert json.loads(await a.recv()) == {"type": "phase", "value": "replying"}
            assert sb.is_idle()
            sb.handler.timer_bridge.handle_message({
                "type": "timer_state", "timers": [
                    {"id": "1", "name": "tea", "total_s": 30, "remaining_s": 30, "ringing": False}
                ]
            })
            assert sa.handler.timer_bridge.timers == []
            assert sb.handler.timer_bridge.timers[0]["name"] == "tea"
            request = asyncio.create_task(sb.handler.timer_bridge.list_timers())
            sent = json.loads(await b.recv())
            assert sent["type"] == "timer_list"
            await b.send(json.dumps({
                "type": "timer_ack", "request_id": sent["request_id"],
                "ok": True, "timers": sb.handler.timer_bridge.timers,
            }))
            assert (await request)["timers"][0]["name"] == "tea"
            received = []
            sa.subscribe("announce_ready", lambda payload: on_announce(payload, received))
            await a.send(json.dumps({"type": "announce_ready", "id": "one"}))
            for _ in range(20):
                if received:
                    break
                await asyncio.sleep(.01)
            assert received == ["one"]
            await sa.queue_frames([BotStartedSpeakingFrame()])
            assert sa.phase_emitter._current == "replying"
            await sa.queue_frames([
                OutputAudioRawFrame(b"\x00\x01" * 960, sample_rate=24000, num_channels=1)
            ])
            assert await asyncio.wait_for(a.recv(), 1) == b"\x00\x01" * 960
            await asyncio.sleep(.05)
            assert registry.get(MAC1).area == "Kitchen"
            assert registry.get(MAC2).area == "Office"
            sa.context.get_messages().append({"role": "user", "content": "Remember tea"})
            async with aiohttp.ClientSession() as http:
                async with http.get(f"http://127.0.0.1:{diag.port}/status") as response:
                    assert response.status == 401
                async with http.get(
                    f"http://127.0.0.1:{diag.port}/status",
                    headers={"Authorization": "Bearer shared"},
                ) as response:
                    status = await response.json()
                    assert response.status == 200
            assert len(status["sessions"]) == 2
            assert {item["area"] for item in status["sessions"]} == {"Kitchen", "Office"}
            assert "p99_ms" in status["loop_lag"]
            replacement = await connect(router, MAC1, "Kitchen 2", token="shared")
            await ready(registry, 2)
            for _ in range(100):
                if registry.get(MAC1).session is not sa:
                    break
                await asyncio.sleep(.01)
            new = registry.get(MAC1).session
            assert new is not sa
            assert new.context.get_messages()[-1]["content"] == "Remember tea"
            await a.wait_closed()
            await asyncio.sleep(.05)  # old handler's late disconnect
            assert registry.get(MAC1).session is new
            assert registry.get(MAC2).session is sb
            assert (tmp_path / "satellites.json").exists()
            await replacement.close()
            await b.close()
        finally:
            await router.close()
            await diag.stop()

    asyncio.run(scenario())


def test_token_timeout_and_slow_writer_isolation(tmp_path, no_area):
    async def scenario():
        app = StubApp()
        registry = SatelliteRegistry(tmp_path / "satellites.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0,
                                 token="secret", handshake_timeout=.1)
        diag = Diagnostics(registry, router, token="secret", port=0)
        await router.start()
        await diag.start()
        try:
            bad = await connect(router, MAC1, "bad", token="wrong")
            await bad.wait_closed()
            assert not app.created
            waiting = await websockets.connect(f"ws://127.0.0.1:{router.port}/any-path")
            await waiting.wait_closed()
            assert not app.created
            slow = await connect(router, MAC1, "slow", token="secret")
            fast = await connect(router, MAC2, "fast", token="secret")
            await ready(registry, 2)
            await slow.recv()
            await fast.recv()
            slow_session = registry.get(MAC1).session
            fast_session = registry.get(MAC2).session
            original_send = slow_session.websocket.send

            async def stalled(payload):
                await asyncio.sleep(3)
                await original_send(payload)

            slow_session.websocket.send = stalled
            for _ in range(3):
                await slow_session.send_json({"type": "phase", "value": "thinking"})
            started = time.monotonic()
            await fast_session.phase_emitter._emit("replying")
            assert json.loads(await asyncio.wait_for(fast.recv(), 1))["value"] == "replying"
            assert time.monotonic() - started < .3
            await slow.wait_closed()
            assert diag.lag()["p99_ms"] < 50
            assert registry.get(MAC2).session is fast_session
            await fast.close()
        finally:
            await router.close()
            await diag.stop()

    asyncio.run(scenario())


async def on_announce(payload, received):
    received.append(payload["id"])


def test_production_factory_creates_distinct_services(tmp_path, monkeypatch, no_area):
    async def scenario():
        monkeypatch.setenv("OPENAI_API_KEY", "fake")
        monkeypatch.setenv("SATELLITES_PATH", str(tmp_path / "devices.json"))
        monkeypatch.setenv("DIAGNOSTICS_PORT", "0")
        monkeypatch.delenv("SUPERVISOR_TOKEN", raising=False)
        monkeypatch.delenv("LONGLIVED_TOKEN", raising=False)

        class FakeService(FakeRealtime):
            def __init__(self, *, api_key, model, session_properties, **kwargs):
                assert api_key == "fake"
                super().__init__(session_properties.instructions)
                self._session_properties = session_properties

        monkeypatch.setattr(main_module, "SafeRealtimeLLMService", FakeService)
        app = Application()
        await app.initialize()
        app.router.host = "127.0.0.1"
        app.router.port = 0
        await app.router.start()
        try:
            first = await connect(app.router, MAC1, "Kitchen")
            second = await connect(app.router, MAC2, "Office")
            await ready(app.registry, 2)
            one, two = (app.registry.get(mac).session for mac in (MAC1, MAC2))
            assert one.openai_service is not two.openai_service
            assert one.handler.timer_bridge is not two.handler.timer_bridge
            assert "set_timer" in one.openai_service.functions
            assert app.session_manager.get_current_service(MAC1) is one.openai_service
            await first.close()
            await second.close()
        finally:
            await app.router.close()

    asyncio.run(scenario())


def test_connection_limit_and_legacy_identity(tmp_path, no_area):
    async def scenario():
        app = StubApp()
        registry = SatelliteRegistry(tmp_path / "devices.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0,
                                 handshake_timeout=.5)
        await router.start()
        try:
            pending = [
                await websockets.connect(f"ws://127.0.0.1:{router.port}/x")
                for _ in range(8)
            ]
            rejected = await websockets.connect(f"ws://127.0.0.1:{router.port}/x")
            await rejected.wait_closed()
            assert rejected.close_code == 1013
            assert not app.created
            for socket in pending:
                await socket.close()
            legacy = await websockets.connect(f"ws://127.0.0.1:{router.port}/x")
            await legacy.send('{"type":"start"}')
            await ready(registry, 1)
            assert registry.get("127.0.0.1").session is not None
            assert json.loads(await legacy.recv())["type"] == "hello"
            await legacy.close()
        finally:
            await router.close()

    asyncio.run(scenario())


def test_overlapping_takeovers_preserve_history(tmp_path, no_area):
    async def scenario():
        app = StubApp()
        registry = SatelliteRegistry(tmp_path / "devices.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0)
        await router.start()
        try:
            first = await connect(router, MAC1, "first")
            await ready(registry, 1)
            original = registry.get(MAC1).session
            original.context.get_messages().append({"role": "user", "content": "first turn"})
            second, third = await asyncio.gather(
                connect(router, MAC1, "second"),
                connect(router, MAC1, "third"),
            )
            await first.wait_closed()
            await asyncio.sleep(.2)
            latest = registry.get(MAC1).session
            assert latest not in (None, original)
            assert latest.context.get_messages()[-1]["content"] == "first turn"
            assert app.session_manager.get_current_service(MAC1) is latest.openai_service
            await second.close()
            await third.close()
        finally:
            await router.close()

    asyncio.run(scenario())

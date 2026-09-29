"""Real Pipecat pipeline tasks over real websocket connections, without OpenAI I/O."""
import asyncio
import json
from pathlib import Path
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
from app.main import SafeRealtimeLLMService
from app.satellite_transport import SatelliteInput
import app.websocket_handler as handler_module
import app.main as main_module


class FakeRealtime(FrameProcessor):
    def __init__(self, instructions):
        super().__init__()
        self._session_properties = SimpleNamespace(instructions=instructions)
        self._context = LLMContext()
        self._llm_needs_conversation_setup = False
        self.announcement_active = False
        self.events = []
        self.event_handlers = {}
        self.functions = {}

    def register_function(self, name, handler, *args, **kwargs):
        self.functions[name] = handler

    def event_handler(self, name):
        def register(callback):
            self.event_handlers[name] = callback
            return callback

        return register

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
        self.announcement_style = "faithful"
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


def replay_service_factory(app):
    async def create(session):
        service = SafeRealtimeLLMService(api_key="fake")
        service.events = []

        async def send(event):
            service.events.append(event.model_dump(exclude_none=True))

        async def connect():
            await service._handle_evt_session_updated(None)

        service.send_client_event = send
        service._connect = connect
        app.session_manager.set_current_service(session.mac, service)
        return service

    return create


async def ready(registry, count):
    for _ in range(150):
        if len(registry.connected()) == count and all(
            s.session.pipeline_task for s in registry.connected()
        ):
            await asyncio.sleep(.05)
            return
        await asyncio.sleep(.01)
    raise AssertionError("sessions did not start")


async def connect(router, mac, name, token=None, dnd=None):
    client = await websockets.connect(f"ws://127.0.0.1:{router.port}/device",
                                      max_queue=16)
    payload = {"type": "start", "mac": mac, "name": name, "caps": ["announce"]}
    if token is not None:
        payload["token"] = token
    if dnd is not None:
        payload["dnd"] = dnd
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


@pytest.mark.parametrize("history_limit", [0, 12])
def test_production_factory_creates_distinct_services(tmp_path, monkeypatch, no_area, history_limit):
    async def scenario():
        monkeypatch.setenv("OPENAI_API_KEY", "fake")
        monkeypatch.setenv("SATELLITES_PATH", str(tmp_path / "devices.json"))
        monkeypatch.setenv("DIAGNOSTICS_PORT", "0")
        monkeypatch.setenv("MAX_CONTEXT_MESSAGES", str(history_limit))
        monkeypatch.setenv("TRANSCRIPTION_LANGUAGE", "")
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
            transcription = one.openai_service._session_properties.audio.input.transcription
            assert transcription is not None
            assert transcription.language is None
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


@pytest.mark.parametrize(
    ("announcement_choice", "tts_choice", "expected"),
    [("", "", "gpt-5-mini"), ("null", "null", "gpt-5-mini"),
     ("gpt-5.5", "gpt-4o-mini-tts", "gpt-5.5")],
)
def test_optional_announcement_models(tmp_path, monkeypatch, announcement_choice, tts_choice, expected):
    async def scenario():
        monkeypatch.setenv("OPENAI_API_KEY", "fake")
        monkeypatch.setenv("WEB_SEARCH_MODEL", "gpt-5-mini")
        monkeypatch.setenv("ANNOUNCEMENT_MODEL", announcement_choice)
        monkeypatch.setenv("ANNOUNCEMENT_TTS_MODEL", tts_choice)
        monkeypatch.setenv("SATELLITES_PATH", str(tmp_path / "devices.json"))
        monkeypatch.delenv("SUPERVISOR_TOKEN", raising=False)
        monkeypatch.delenv("LONGLIVED_TOKEN", raising=False)
        app = Application()
        await app.initialize()
        try:
            assert app.announcement_model == expected
            assert app.announcement_tts_model == "gpt-4o-mini-tts"
        finally:
            await app.announcements.close()

    asyncio.run(scenario())


def test_optional_announcement_model_export_is_guarded():
    run = (Path(__file__).resolve().parents[1] / "root" / "run.sh").read_text()
    guard = "if bashio::config.has_value 'announcement_model'; then"
    export = "ANNOUNCEMENT_MODEL=$(bashio::config 'announcement_model')"
    assert guard in run
    assert run.index(guard) < run.index(export) < run.index("\nfi\n", run.index(guard))


def test_invalid_announcement_style_falls_back_to_faithful(tmp_path, monkeypatch, caplog):
    async def scenario():
        monkeypatch.setenv("OPENAI_API_KEY", "fake")
        monkeypatch.setenv("ANNOUNCEMENT_STYLE", "unexpected")
        monkeypatch.setenv("SATELLITES_PATH", str(tmp_path / "devices.json"))
        monkeypatch.delenv("SUPERVISOR_TOKEN", raising=False)
        monkeypatch.delenv("LONGLIVED_TOKEN", raising=False)
        app = Application()
        await app.initialize()
        try:
            assert app.announcement_style == "faithful"
            assert "Unknown announcement style 'unexpected'; using faithful" in caplog.text
        finally:
            await app.announcements.close()

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


def test_restored_history_sent_once_per_openai_session_without_response(tmp_path, no_area):
    async def scenario():
        app = StubApp()
        registry = SatelliteRegistry(tmp_path / "devices.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0)

        app.create_openai_service = replay_service_factory(app)
        app.turn_detection_type = "semantic_vad"
        app.semantic_vad_create_response = True
        cached = LLMContext(messages=[
            {"role": "system", "content": "Do not replay this as a message"},
            {"role": "user", "content": "What time is tea?"},
            {"role": "assistant", "content": "At five."},
            {"role": "tool", "content": "ignored", "tool_call_id": "id"},
        ])
        app.session_manager.context_caches[MAC1] = SimpleNamespace(
            context=cached, timestamp=time.time())
        await router.start()
        try:
            client = await connect(router, MAC1, "kitchen")
            assert json.loads(await asyncio.wait_for(client.recv(), 2))["type"] == "hello"
            await ready(registry, 1)
            service = registry.get(MAC1).session.openai_service
            items = [e for e in service.events if e["type"] == "conversation.item.create"]
            assert [(e["item"]["role"], e["item"]["content"]) for e in items] == [
                ("user", [{"type": "input_text", "text": "What time is tea?"}]),
                ("assistant", [{"type": "output_text", "text": "At five."}]),
            ]
            assert all(e["item"]["id"] in service._messages_added_manually for e in items)
            assert "id" in service._completed_tool_calls
            assert not any(e["type"] == "response.create" for e in service.events)
            await service._handle_context(registry.get(MAC1).session.context)
            assert not any(e["type"] == "conversation.item.create" and
                           e.get("item", {}).get("type") == "function_call_output"
                           for e in service.events)
            assert not any(e["type"] == "response.create" for e in service.events)
            await service._handle_evt_session_updated(None)
            assert len([e for e in service.events if e["type"] == "conversation.item.create"]) == 2
            service._api_session_ready = False
            await service.reset_conversation()
            assert len([e for e in service.events if e["type"] == "conversation.item.create"]) == 4
            assert not any(e["type"] == "response.create" for e in service.events)
            await client.close()
        finally:
            await router.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("turn_detection", "create_response"),
    [("server_vad", False), ("semantic_vad", False)],
)
def test_replay_without_preseed_does_not_send_history_twice(
    tmp_path, no_area, turn_detection, create_response,
):
    async def scenario():
        app = StubApp()
        app.create_openai_service = replay_service_factory(app)
        app.turn_detection_type = turn_detection
        app.semantic_vad_create_response = create_response
        app.session_manager.context_caches[MAC1] = SimpleNamespace(
            context=LLMContext(messages=[
                {"role": "system", "content": "system prompt"},
                {"role": "user", "content": "first question"},
                {"role": "assistant", "content": "first answer"},
                {"role": "tool", "tool_call_id": "old-call", "content": "old result"},
            ]), timestamp=time.time(),
        )
        registry = SatelliteRegistry(tmp_path / "devices.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0)
        await router.start()
        try:
            client = await connect(router, MAC1, "kitchen")
            await ready(registry, 1)
            session = registry.get(MAC1).session
            service = session.openai_service
            assert service._context is None
            assert service._llm_needs_conversation_setup is False
            assert len([e for e in service.events if e["type"] == "conversation.item.create"]) == 2
            session.context.add_message({"role": "user", "content": "next question"})
            await service._handle_context(session.context)
            items = [e for e in service.events if e["type"] == "conversation.item.create"]
            assert len(items) == 2
            assert [e["type"] for e in service.events].count("response.create") == 1
            assert not any(e["item"].get("type") == "function_call_output" for e in items)
            await client.close()
        finally:
            await router.close()

    asyncio.run(scenario())


def test_reconnect_caps_replay_and_live_context(tmp_path, no_area):
    async def scenario():
        app = StubApp()
        app.session_manager.max_restored_messages = 2
        app.create_openai_service = replay_service_factory(app)
        app.turn_detection_type = "semantic_vad"
        app.semantic_vad_create_response = True
        app.session_manager.context_caches[MAC1] = SimpleNamespace(
            context=LLMContext(messages=[
                {"role": "system", "content": "system prompt"},
                {"role": "user", "content": "old question"},
                {"role": "assistant", "content": "old answer"},
            ]), timestamp=time.time(),
        )
        registry = SatelliteRegistry(tmp_path / "devices.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0)
        await router.start()
        try:
            client = await connect(router, MAC1, "kitchen")
            await ready(registry, 1)
            session = registry.get(MAC1).session
            service = session.openai_service
            for n in range(4):
                session.context.add_message({"role": "user", "content": f"question {n}"})
                session.context.add_message({"role": "assistant", "content": f"answer {n}"})
                session.context.add_message({
                    "role": "tool", "tool_call_id": f"call-{n}", "content": "old tool result",
                })
            assert len(session.context.get_messages()) > 10
            for n in range(2):
                service.events.clear()
                service._api_session_ready = False
                await service.reset_conversation()
                replay = [e["item"] for e in service.events
                          if e["type"] == "conversation.item.create"]
                assert [(item["role"], item["content"][0]["text"]) for item in replay] == [
                    ("user", f"question {3+n}"), ("assistant", f"answer {3+n}"),
                ]
                assert [m["role"] for m in session.context.get_messages()] == [
                    "system", "user", "assistant", "tool",
                ]
                assert not any(e["type"] == "response.create" for e in service.events)
                assert service._llm_needs_conversation_setup is False
                session.context.add_message({"role": "user", "content": f"question {4+n}"})
                session.context.add_message({"role": "assistant", "content": f"answer {4+n}"})
                session.context.add_message({
                    "role": "tool", "tool_call_id": f"call-{4+n}", "content": "old tool result",
                })
            await client.close()
        finally:
            await router.close()

    asyncio.run(scenario())


def test_cached_history_limit_counts_user_and_assistant_not_tools():
    manager = SessionManager(max_restored_messages=2)
    manager.context_caches[MAC1] = SimpleNamespace(
        context=LLMContext(messages=[
            {"role": "system", "content": "system prompt"},
            {"role": "user", "content": "old question"},
            {"role": "tool", "tool_call_id": "old-tool", "content": "old result"},
            {"role": "assistant", "content": "old answer"},
            {"role": "user", "content": "latest question"},
            {"role": "tool", "tool_call_id": "latest-tool", "content": "latest result"},
            {"role": "assistant", "content": "latest answer"},
        ]), timestamp=time.time(),
    )
    restored = manager.create_context_for_new_session(MAC1).get_messages()
    assert [m["content"] for m in restored] == [
        "system prompt", "latest question", "latest result", "latest answer",
    ]


def test_failed_registry_write_does_not_reject_later_connections(tmp_path, no_area, caplog):
    async def scenario():
        app = StubApp()
        blocked = tmp_path / "file"
        blocked.write_text("not a directory")
        registry = SatelliteRegistry(blocked / "satellites.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0)
        await router.start()
        clients = []
        try:
            for mac in (MAC1, MAC2, "aa:bb:cc:dd:ee:03"):
                client = await connect(router, mac, mac)
                clients.append(client)
                assert json.loads(await asyncio.wait_for(client.recv(), 2))["type"] == "hello"
                for _ in range(100):
                    if router._persist_task and router._persist_task.done():
                        break
                    await asyncio.sleep(.01)
                assert router._persist_task.done()
            await ready(registry, 3)
            assert all(s.session.counters["errors"] == 0 for s in registry.connected())
        finally:
            for client in clients:
                await client.close()
            await router.close()

    asyncio.run(scenario())
    assert "persist" in caplog.text.lower()


def test_silent_same_mac_takeover_is_bounded(tmp_path, no_area):
    async def scenario():
        app = StubApp()
        registry = SatelliteRegistry(tmp_path / "devices.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0)
        await router.start()
        try:
            old = await connect(router, MAC1, "old")
            await ready(registry, 1)
            await old.recv()
            old_session = registry.get(MAC1).session
            old.transport.pause_reading()
            new = await connect(router, MAC1, "new")
            assert json.loads(await asyncio.wait_for(new.recv(), 3))["type"] == "hello"
            assert registry.get(MAC1).session.name == "new"
            old.transport.resume_reading()
            await old.wait_closed()
            assert old_session.counters["errors"] == 0
            await new.close()
        finally:
            await router.close()

    asyncio.run(scenario())


def test_audio_before_input_ready_is_dropped_without_disconnect(tmp_path, no_area, monkeypatch):
    async def scenario():
        app = StubApp()
        registry = SatelliteRegistry(tmp_path / "devices.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0)
        original = SatelliteInput.set_transport_ready
        gate = asyncio.Event()
        entered = asyncio.Event()

        async def slow_ready(self, frame):
            entered.set()
            await gate.wait()
            await original(self, frame)

        monkeypatch.setattr(SatelliteInput, "set_transport_ready", slow_ready)
        await router.start()
        try:
            client = await connect(router, MAC1, "kitchen")
            for _ in range(5):
                await client.send(b"\x00\x01" * 160)
            await asyncio.wait_for(entered.wait(), 2)
            await asyncio.sleep(.1)
            gate.set()
            assert json.loads(await asyncio.wait_for(client.recv(), 2))["type"] == "hello"
            await ready(registry, 1)
            session = registry.get(MAC1).session
            assert session.counters["audio_before_ready"] == 5
            await client.send(b"\x00\x01" * 160)
            for _ in range(100):
                if session.counters["audio_in_bytes"]:
                    break
                await asyncio.sleep(.01)
            assert session.counters["audio_in_bytes"] == 320
            await client.close()
        finally:
            gate.set()
            await router.close()

    asyncio.run(scenario())


def test_takeover_during_slow_start_cleans_all_pipelines(tmp_path, no_area):
    async def scenario():
        app = StubApp()
        started = asyncio.Event()
        original = app.create_openai_service

        async def slow_factory(session):
            started.set()
            await asyncio.sleep(.5)
            return await original(session)

        app.create_openai_service = slow_factory
        registry = SatelliteRegistry(tmp_path / "devices.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0)
        await router.start()
        try:
            first = await connect(router, MAC1, "first")
            await asyncio.wait_for(started.wait(), 2)
            second = await connect(router, MAC1, "second")
            await asyncio.wait_for(router.close(), 4)
            assert not app.session_manager.current_services
            assert not [task for task in asyncio.all_tasks()
                        if task.get_name().startswith("pipeline:") and not task.done()]
            await first.wait_closed()
            await second.wait_closed()
        finally:
            await router.close()

    asyncio.run(scenario())


def test_unicode_token_works_for_device_and_diagnostics(tmp_path, no_area):
    async def scenario():
        token = "sésame🔑"
        app = StubApp()
        registry = SatelliteRegistry(tmp_path / "devices.json")
        router = SatelliteRouter(app, registry, "127.0.0.1", 0, token=token)
        diag = Diagnostics(registry, router, token=token, port=0, enabled=True)
        await router.start()
        await diag.start()
        try:
            client = await connect(router, MAC1, "kitchen", token=token)
            assert json.loads(await asyncio.wait_for(client.recv(), 2))["type"] == "hello"
            async with aiohttp.ClientSession() as http:
                async with http.get(f"http://127.0.0.1:{diag.port}/status",
                                    headers={"Authorization": f"Bearer {token}"}) as res:
                    assert res.status == 200
            await client.close()
        finally:
            await router.close()
            await diag.stop()

    asyncio.run(scenario())

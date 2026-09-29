"""Announcements through real Pipecat tasks and fake external services."""
import asyncio
import json
import time
from types import SimpleNamespace

import pytest
import httpx
from openai import BadRequestError
from pipecat.frames.frames import InterruptionFrame, TTSAudioRawFrame, TTSStartedFrame, TTSStoppedFrame

from app.announcements import (
    AnnouncementManager, Job, TTL, append_assistant_note, fact_guard, follow_up_for,
    truncate_message,
)
from app.mqtt_bridge import MQTTBridge, STATUS, availability_topic, discovery
from app.satellites import SatelliteRegistry, SatelliteRouter
from app.home_context import device_registry_area_sync
import app.home_context as home_context
import app.announcements as announcement_module
from test_multi_satellites import MAC1, MAC2, StubApp, connect, ready, no_area


def test_fact_guard_and_follow_up(caplog):
    assert not fact_guard("Alice is at the front door.", "Bob is at the front door.")[0]
    assert not fact_guard("Dr. Jones is here.", "Dr. Smith is here.")[0]
    assert not fact_guard("The range is 5-10.", "The range is 5-12.")[0]
    assert fact_guard("Meet Alex at 7:30 with 20.5% for Mr Smith.", "Alex, Mr Smith: 20.5% at 7:30.")[0]
    assert not fact_guard("Meet Alex at 7:30 with 20.5%.", "Meet Alex at 7:30.")[0]
    assert not fact_guard("Bring 2 and 2 boxes.", "Bring 2 boxes.")[0]
    assert not fact_guard("Temperature -5°C.", "Temperature 5°C.")[0]
    assert not fact_guard("Call Doctor Jones.", "Call Doctor.")[0]
    assert follow_up_for("Coming?")
    assert not follow_up_for("Dinner is ready.")
    assert follow_up_for("Dinner is ready.", "always")
    assert not follow_up_for("Coming?", "never")
    assert len(truncate_message("word " * 110)) <= 500


def test_tts_voice_fallback_and_audio_limit(tmp_path):
    async def scenario():
        app = StubApp()
        app.openai_api_key = "test"
        app.personality = "monday"
        app.voice = "invalid-voice"
        app.announcement_tts_model = "gpt-4o-mini-tts"
        fake = FakeClient()
        voices = []

        def stream(**kwargs):
            voices.append(kwargs["voice"])
            if kwargs["voice"] != "marin":
                response = httpx.Response(400, request=httpx.Request(
                    "POST", "https://api.openai.com/v1/audio/speech"))
                raise BadRequestError("Unsupported voice", response=response, body=None)
            fake.pcm = b"\x10\x00" * (24000 * 31)
            return FakeClient.speak(fake, **kwargs)

        fake.audio.speech.with_streaming_response.create = stream
        manager = AnnouncementManager(app, SatelliteRegistry(tmp_path / "satellites.json"), fake)
        audio = await manager._tts("Hello")
        assert voices == ["invalid-voice", "marin"]
        assert len(audio) == 30 * 24000 * 2
        assert audio[-2:] == b"\x00\x00"
        await manager.close()

    asyncio.run(scenario())


def test_tts_retries_once_without_realtime_fallback(tmp_path):
    async def scenario():
        app = StubApp()
        app.openai_api_key = "test"
        app.personality = "standard"
        app.voice = "marin"
        app.announcement_tts_model = "gpt-4o-mini-tts"
        fake = FakeClient()
        attempts = []

        def broken(**kwargs):
            attempts.append(kwargs["voice"])
            raise ValueError("TTS unavailable")

        fake.audio.speech.with_streaming_response.create = broken
        manager = AnnouncementManager(app, SatelliteRegistry(tmp_path / "satellites.json"), fake)
        with pytest.raises(ValueError, match="TTS unavailable"):
            await manager._tts("Hello")
        assert attempts == ["marin", "marin"]
        await manager.close()

    asyncio.run(scenario())


def test_failure_notification_rate_limit(tmp_path, monkeypatch):
    async def scenario():
        app = StubApp()
        app.openai_api_key = "test"
        app.ha_token = "test"
        manager = AnnouncementManager(app, SatelliteRegistry(tmp_path / "satellites.json"),
                                      FakeClient())
        calls = []

        class Response:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def raise_for_status(self):
                pass

        class Session:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            def post(self, url, *, json, headers):
                calls.append((url, json, headers))
                return Response()

        monkeypatch.setattr(announcement_module.aiohttp, "ClientSession",
                            lambda **kwargs: Session())
        await manager.failure("queue full")
        await manager.failure("queue full")
        assert len(calls) == 1
        assert calls[0][0].endswith("/api/services/persistent_notification/create")
        assert calls[0][2]["Authorization"].endswith("test")
        await manager.close()

    asyncio.run(scenario())


class FakeClient:
    def __init__(self, text="Hello Alex at 7:30.", pcm=b"\x10\x00" * 960):
        self.text, self.pcm = text, pcm
        self.compositions = self.speeches = 0
        self.responses = SimpleNamespace(create=self.compose)
        self.audio = SimpleNamespace(speech=SimpleNamespace(
            with_streaming_response=SimpleNamespace(create=self.speak)))

    async def compose(self, **kwargs):
        self.compositions += 1
        return SimpleNamespace(output_text=self.text)

    def speak(self, **kwargs):
        self.speeches += 1
        client = self

        class Stream:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def iter_bytes(self):
                yield client.pcm

        return Stream()


async def setup(tmp_path, no_area, *, two=False):
    app = StubApp()
    app.announcement_chime = True
    app.announcement_model = "test"
    app.announcement_tts_model = "gpt-4o-mini-tts"
    app.voice = "marin"
    app.openai_api_key = "test"
    registry = SatelliteRegistry(tmp_path / "satellites.json")
    client = FakeClient()
    manager = AnnouncementManager(app, registry, client=client)
    app.announcements = manager
    router = SatelliteRouter(app, registry, "127.0.0.1", 0)
    await router.start()
    sockets = [await connect(router, MAC1, "Kitchen")]
    if two:
        sockets.append(await connect(router, MAC2, "Office"))
    await ready(registry, len(sockets))
    for socket in sockets:
        assert json.loads(await socket.recv())["type"] == "hello"
    return app, registry, manager, client, router, sockets


async def close(manager, router, sockets):
    for socket in sockets:
        await socket.close()
    await router.close()
    await manager.close()


async def next_type(socket, wanted, timeout=4):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        message = await asyncio.wait_for(socket.recv(), deadline - time.monotonic())
        if isinstance(message, str):
            payload = json.loads(message)
            if payload["type"] == wanted:
                return payload
    raise AssertionError(f"No {wanted}")


def test_tts_frames_drive_real_phase_transitions(tmp_path, no_area):
    async def scenario():
        app, registry, manager, _, router, sockets = await setup(tmp_path, no_area)
        socket = sockets[0]
        try:
            session = registry.get(MAC1).session
            session.openai_service._messages_added_manually = {}
            await session.queue_frames([TTSStartedFrame(),
                TTSAudioRawFrame(audio=b"\x10\x00" * 960, sample_rate=24000, num_channels=1),
                TTSStoppedFrame()])
            messages = []
            for _ in range(7):
                payload = await asyncio.wait_for(socket.recv(), 3)
                if isinstance(payload, str):
                    obj = json.loads(payload)
                    messages.append(obj)
                    if obj == {"type": "phase", "value": "idle"}:
                        break
            assert {"type": "phase", "value": "replying"} in messages
            assert {"type": "audio_done"} in messages
            assert messages.index({"type": "phase", "value": "replying"}) < messages.index({"type": "audio_done"}) < messages.index({"type": "phase", "value": "idle"})
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_coalescing_action_history_and_fallback(tmp_path, no_area):
    async def scenario():
        app, registry, manager, client, router, sockets = await setup(tmp_path, no_area, two=True)
        try:
            a, b = sockets
            for mac in (MAC1, MAC2):
                registry.get(mac).session.openai_service._messages_added_manually = {}
            one = await manager.submit("Hello Alex at 7:30.", [MAC1])
            two = await manager.submit("Hello Alex at 7:30.", [MAC2])
            assert one is two
            request_a, request_b = await asyncio.gather(next_type(a, "announce"), next_type(b, "announce"))
            assert request_a["id"] == request_b["id"] == one.id
            assert not request_a["follow_up"]
            await a.send(json.dumps({"type": "announce_ready", "id": one.id}))
            await b.send(json.dumps({"type": "announce_ready", "id": one.id}))
            audio_a, audio_b = await asyncio.gather(next_audio(a), next_audio(b))
            assert audio_a == audio_b
            await a.send(json.dumps({"type": "announce_done", "id": one.id}))
            await b.send(json.dumps({"type": "announce_done", "id": one.id}))
            await asyncio.sleep(.2)
            assert (client.compositions, client.speeches) == (1, 1)
            session = registry.get(MAC1).session
            assert session.context.get_messages()[-1]["content"] == one.spoken
            note = session.openai_service.events[-1].item
            assert note.role == "assistant" and note.content[0].type == "output_text"
            assert note.id in session.openai_service._messages_added_manually
            assert note.id in session.kill_exempt_ids
            client.text = "No facts here."
            fallback = await manager._compose("Meet Alex at 7:30.")
            assert fallback == "Meet Alex at 7:30."
            await a.send(json.dumps({"type": "announce_text", "message": "Hello?", "chime": False,
                                     "follow_up": "never"}))
            assert (await next_type(a, "announce_result"))["ok"]
            action = await next_type(a, "announce")
            assert action["chime"] is False and action["follow_up"] is False
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


async def next_audio(socket):
    for _ in range(9):
        payload = await asyncio.wait_for(socket.recv(), 4)
        if isinstance(payload, bytes):
            return payload
    raise AssertionError("No PCM")


def test_busy_retry_timeout_stale_and_caps(tmp_path, no_area, monkeypatch):
    async def scenario():
        app, registry, manager, _, router, sockets = await setup(tmp_path, no_area)
        socket = sockets[0]
        manager.failure = lambda message: asyncio.sleep(0)
        monkeypatch.setattr("app.announcements.TTL", 1.2)
        try:
            job = await manager.submit("Hello Alex at 7:30.", [MAC1])
            request = await next_type(socket, "announce")
            await socket.send(json.dumps({"type": "announce_ready", "id": "stale"}))
            await asyncio.sleep(.03)
            assert not registry.get(MAC1).session._announcement_reply.done()
            await socket.send(json.dumps({"type": "announce_busy", "id": request["id"]}))
            await asyncio.sleep(.1)
            session = registry.get(MAC1).session
            assert session._announcement_id == job.id
            assert not session.announcement_active
            assert not session.openai_service.announcement_active
            before = len(session.openai_service.events)
            await socket.send(json.dumps({"type": "interrupt"}))
            for _ in range(30):
                if len(session.openai_service.events) > before:
                    break
                await asyncio.sleep(.01)
            assert len(session.openai_service.events) > before
            assert type(session.openai_service.events[-1]).__name__ == "InputAudioBufferClearEvent"
            callback = session.openai_service.event_handlers["on_conversation_item_created"]
            await callback(session.openai_service, "post-stop", SimpleNamespace(role="assistant"))
            assert type(session.openai_service.events[-1]).__name__ == "ResponseCancelEvent"
            assert manager._reply is not None
            assert await manager.submit("Different", [MAC1]) is not None
            for n in range(3):
                assert await manager.submit(f"queued {n}", [MAC1])
            assert await manager.submit("over cap", [MAC1]) is None
            assert (await next_type(socket, "announce_cancel", timeout=2))["id"] == job.id
            assert job not in manager.pending[MAC1]
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_readiness_timeout_cancels_reservation(tmp_path, no_area):
    async def scenario():
        app, registry, manager, _, router, sockets = await setup(tmp_path, no_area)
        socket = sockets[0]
        errors = []

        async def record(error):
            errors.append(error)

        manager.failure = record
        try:
            job = await manager.submit("Hello Alex at 7:30.", [MAC1])
            assert (await next_type(socket, "announce"))["id"] == job.id
            cancel = await next_type(socket, "announce_cancel", timeout=6)
            assert cancel["id"] == job.id
            await asyncio.sleep(.05)
            assert any("readiness timed out" in error for error in errors)
            assert not manager.pending[MAC1]
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_device_cancel_interrupts_queued_pcm_and_reconnect(tmp_path, no_area):
    async def scenario():
        app, registry, manager, client, router, sockets = await setup(tmp_path, no_area)
        socket = sockets[0]
        client.pcm = b"\x10\x00" * (24000 * 4)
        session = registry.get(MAC1).session
        session.openai_service._messages_added_manually = {}
        errors = []

        async def record(error):
            errors.append(error)

        manager.failure = record
        queued = []
        original_queue = session.queue_frames

        async def track(frames):
            queued.extend(frames)
            await original_queue(frames)

        session.queue_frames = track
        try:
            job = await manager.submit("Hello Alex at 7:30.", [MAC1])
            assert (await next_type(socket, "announce"))["id"] == job.id
            await socket.send(json.dumps({"type": "announce_ready", "id": job.id}))
            assert await next_audio(socket)
            await socket.send(json.dumps({"type": "announce_cancelled", "id": job.id,
                                          "reason": "wake"}))
            for _ in range(50):
                if not manager.pending[MAC1]:
                    break
                await asyncio.sleep(.05)
            assert not manager.pending[MAC1]
            assert session.context.get_messages()[-1]["content"].startswith("(interrupted) ")
            assert any(isinstance(frame, InterruptionFrame) for frame in queued)
            assert sum(isinstance(frame, TTSAudioRawFrame) for frame in queued) < 100
            next_job = await manager.submit("Hello Alex at 7:30.", [MAC1])
            assert (await next_type(socket, "announce"))["id"] == next_job.id
            await socket.send(json.dumps({"type": "announce_ready", "id": next_job.id}))
            assert await next_audio(socket)
            await socket.close()
            for _ in range(60):
                if registry.get(MAC1).session is None:
                    break
                await asyncio.sleep(.05)
            assert registry.get(MAC1).session is None
            assert not session.announcement_active
            assert not manager.dispatchers
            assert not manager.pending[MAC1]
            assert session.context.get_messages()[-1]["content"] == "(interrupted) " + next_job.spoken
            assert any("interrupted by disconnect" in error for error in errors)
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


@pytest.mark.parametrize("takeover", [False, True])
def test_pre_audio_job_survives_reconnect(tmp_path, no_area, takeover):
    async def scenario():
        app, registry, manager, fake, router, sockets = await setup(tmp_path, no_area)
        original = sockets[0]
        failures = []

        async def record(error):
            failures.append(error)

        manager.failure = record
        try:
            job = await manager.submit("Hello Alex at 7:30.", [MAC1])
            assert (await next_type(original, "announce"))["id"] == job.id
            if takeover:
                replacement = await connect(router, MAC1, "Kitchen")
                sockets.append(replacement)
                await original.wait_closed()
            else:
                await original.close()
                for _ in range(100):
                    if registry.get(MAC1).session is None:
                        break
                    await asyncio.sleep(.01)
                assert registry.get(MAC1).session is None
                replacement = await connect(router, MAC1, "Kitchen")
                sockets.append(replacement)
            assert json.loads(await replacement.recv())["type"] == "hello"
            await ready(registry, 1)
            assert job in manager.pending[MAC1] and MAC1 in job.remaining
            assert (await next_type(replacement, "announce"))["id"] == job.id
            assert not failures
            new_session = registry.get(MAC1).session
            new_session.openai_service._messages_added_manually = {}
            await replacement.send(json.dumps({"type": "announce_ready", "id": job.id}))
            assert await next_audio(replacement)
            await replacement.send(json.dumps({"type": "announce_done", "id": job.id}))
            for _ in range(100):
                if job not in manager.pending[MAC1]:
                    break
                await asyncio.sleep(.01)
            assert job not in manager.pending[MAC1]
            assert (fake.compositions, fake.speeches) == (1, 1)
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_audio_batches_keep_absolute_half_second_lead(tmp_path, no_area):
    async def scenario():
        app, registry, manager, fake, router, sockets = await setup(tmp_path, no_area)
        fake.pcm = b"\x10\x00" * (24000 * 2)
        socket = sockets[0]
        session = registry.get(MAC1).session
        session.openai_service._messages_added_manually = {}
        batches = []
        original_queue = session.queue_frames

        async def record(frames):
            if any(isinstance(frame, TTSAudioRawFrame) for frame in frames):
                batches.append(time.monotonic())
            await original_queue(frames)

        session.queue_frames = record
        manager.failure = lambda error: asyncio.sleep(0)
        try:
            job = await manager.submit("Hello Alex at 7:30.", [MAC1])
            assert (await next_type(socket, "announce"))["id"] == job.id
            assert not session.announcement_active
            await socket.send(json.dumps({"type": "announce_ready", "id": job.id}))
            for _ in range(100):
                if len(batches) >= 3:
                    break
                await asyncio.sleep(.01)
            assert len(batches) >= 3
            assert session.announcement_active and session.openai_service.announcement_active
            assert batches[1] - batches[0] < .23
            assert batches[2] - batches[0] < .42
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_delivery_error_after_pcm_records_interruption(tmp_path, no_area):
    async def scenario():
        app, registry, manager, fake, router, sockets = await setup(tmp_path, no_area)
        fake.pcm = b"\x10\x00" * (24000 * 2)
        socket = sockets[0]
        session = registry.get(MAC1).session
        session.openai_service._messages_added_manually = {}
        frames_sent = 0
        original_queue = session.queue_frames
        errors = []

        async def fail_second_audio_batch(frames):
            nonlocal frames_sent
            if any(isinstance(frame, TTSAudioRawFrame) for frame in frames):
                frames_sent += 1
                if frames_sent == 2:
                    raise ConnectionError("PCM writer failed")
            await original_queue(frames)

        async def record(error):
            errors.append(error)

        session.queue_frames = fail_second_audio_batch
        manager.failure = record
        try:
            job = await manager.submit("Hello Alex at 7:30.", [MAC1])
            assert (await next_type(socket, "announce"))["id"] == job.id
            await socket.send(json.dumps({"type": "announce_ready", "id": job.id}))
            for _ in range(100):
                if job not in manager.pending[MAC1]:
                    break
                await asyncio.sleep(.01)
            assert job not in manager.pending[MAC1]
            assert session.context.get_messages()[-1]["content"] == "(interrupted) " + job.spoken
            assert any("PCM writer failed" in error for error in errors)
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_mqtt_discovery_availability_and_commands(tmp_path, monkeypatch):
    async def scenario():
        registry = SatelliteRegistry(tmp_path / "satellites.json")
        session = SimpleNamespace(caps=["announce"])
        registry.reserve(MAC1, "Kitchen", ["announce"], session)
        session2 = SimpleNamespace(caps=["announce"])
        registry.reserve(MAC2, "Office", ["announce"], session2)
        app = SimpleNamespace(ha_token=None)
        calls = []

        class Manager:
            async def submit(self, message, targets):
                calls.append((message, set(targets)))

        bridge = MQTTBridge(app, registry, Manager())
        config = discovery(registry.get(MAC1))
        assert config["unique_id"] == "oai_rt_aabbccddee01_announce"
        assert config["device"]["via_device"] == "oai_rt_addon"
        assert len(config["availability"]) == 2 and config["availability_mode"] == "all"
        assert config["command_topic"] == f"openai_realtime/{MAC1}/announce"
        assert config["default_entity_id"] == "notify.kitchen_announce"
        retained = SimpleNamespace(retain=True, topic="openai_realtime/all/announce", payload=b"ignored")
        await bridge.handle_command(retained)
        assert calls == []
        retained.retain = False
        retained.payload = b"Is dinner ready?"
        await bridge.handle_command(retained)
        assert calls == [("Is dinner ready?", {MAC1, MAC2})]
        published = []
        fake = SimpleNamespace(publish=lambda *args, **kwargs: publish(published, *args, **kwargs))
        await fake.publish(availability_topic(MAC1), "offline", retain=True)
        await bridge._publish_device(fake, registry.get(MAC1))
        await fake.publish(STATUS, "online", retain=True)
        assert [x[0] for x in published] == [
            availability_topic(MAC1),
            "homeassistant/notify/oai_rt_aabbccddee01/announce/config",
            STATUS]

    asyncio.run(scenario())


async def publish(published, *args, **kwargs):
    published.append((args[0], args[1]))


def test_area_sync_updates_only_when_different(monkeypatch):
    async def scenario():
        mqtt = {"id": "mqtt-device", "identifiers": [["mqtt", "oai_rt_aabbccddee01"]],
                "area_id": "old"}
        source = {"id": "esphome-device", "connections": [["mac", MAC1]],
                  "config_entries": ["entry"], "area_id": "kitchen"}
        sends = []

        class Socket:
            def __init__(self):
                self.responses = [
                    {"type": "auth_required"}, {"type": "auth_ok"},
                    {"type": "result", "id": 1, "success": True, "result": [source, mqtt]},
                    {"type": "result", "id": 2, "success": True, "result": [{"name": "Kitchen", "area_id": "kitchen"}]},
                    {"type": "result", "id": 3, "success": True, "result": [{"entry_id": "entry", "domain": "esphome"}]},
                    {"type": "result", "id": 4, "success": True, "result": mqtt},
                ]

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def send(self, value):
                sends.append(json.loads(value))

            async def recv(self):
                return json.dumps(self.responses.pop(0))

        monkeypatch.setattr(home_context.websockets, "connect", lambda *args, **kwargs: Socket())
        assert await device_registry_area_sync("http://ha", "token", MAC1, "Kitchen")
        assert sends[-1] == {"id": 4, "type": "config/device_registry/update",
                             "device_id": "mqtt-device", "area_id": "kitchen"}
        mqtt["area_id"] = "kitchen"
        sends.clear()
        assert await device_registry_area_sync("http://ha", "token", MAC1, "Kitchen")
        assert len(sends) == 4  # auth plus three reads; no unnecessary update

    asyncio.run(scenario())


def test_global_cap_and_generation_concurrency(tmp_path):
    async def scenario():
        app = StubApp()
        app.announcement_chime = True
        app.announcement_model = "test"
        app.announcement_tts_model = "gpt-4o-mini-tts"
        app.voice = "marin"
        app.openai_api_key = "test"
        app.ha_token = None
        registry = SatelliteRegistry(tmp_path / "satellites.json")
        macs = [f"aa:bb:cc:dd:ee:{n:02x}" for n in range(11)]
        class Connected:
            caps = ["announce"]

        for mac in macs:
            registry.reserve(mac, mac, ["announce"], Connected())
        fake = FakeClient("Ready.", b"\x01\x00" * 960)
        active = 0
        maximum = 0

        async def slow_compose(**kwargs):
            nonlocal active, maximum
            active += 1
            maximum = max(maximum, active)
            await asyncio.sleep(.08)
            active -= 1
            return SimpleNamespace(output_text="Ready.")

        fake.responses.create = slow_compose
        manager = AnnouncementManager(app, registry, client=fake)
        try:
            jobs = [await manager.submit("Ready.", [mac], coalesce=False) for mac in macs[:10]]
            assert all(jobs)
            assert await manager.submit("Ready.", [macs[10]]) is None
            await asyncio.wait_for(asyncio.gather(*(job.ready.wait() for job in jobs)), 5)
            assert maximum == 2
        finally:
            await manager.close()

    asyncio.run(scenario())


def test_mqtt_broker_restart_resets_availability(tmp_path, monkeypatch):
    async def scenario():
        monkeypatch.setenv("MQTT_HOST", "broker")
        registry = SatelliteRegistry(tmp_path / "satellites.json")
        session = SimpleNamespace(caps=["announce"])
        registry.reserve(MAC1, "Kitchen", ["announce"], session)
        announcements = SimpleNamespace(submit=None)
        published = []
        instances = []
        broken = asyncio.Event()

        class Client:
            def __init__(self, **kwargs):
                assert kwargs["will"].topic == STATUS and kwargs["will"].retain
                instances.append(self)
                self.messages = self.listen()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def subscribe(self, topic):
                assert topic == "openai_realtime/+/announce"

            async def publish(self, topic, payload, **kwargs):
                published.append((len(instances), topic, payload, kwargs.get("retain")))

            async def listen(self):
                await broken.wait()
                broken.clear()
                raise ConnectionError("broker restart")
                yield

        bridge = MQTTBridge(SimpleNamespace(ha_token=None), registry, announcements,
                            client_factory=Client)
        await bridge.start()
        try:
            for _ in range(100):
                if any(t == STATUS and p == "online" for _, t, p, _ in published):
                    break
                await asyncio.sleep(.01)
            first = [(t, p) for n, t, p, _ in published if n == 1]
            assert first[0] == (availability_topic(MAC1), "offline")
            assert first[-2:] == [(STATUS, "online"), (availability_topic(MAC1), "online")]
            broken.set()
            for _ in range(150):
                if len(instances) == 2:
                    break
                await asyncio.sleep(.01)
            assert len(instances) == 2
            second = [(t, p) for n, t, p, _ in published if n == 2]
            assert second[0] == (availability_topic(MAC1), "offline")
        finally:
            await bridge.close()
        assert published[-1][1:3] == (STATUS, "offline")

    asyncio.run(scenario())

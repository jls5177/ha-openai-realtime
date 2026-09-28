"""Satellite identity, connection lifecycle, and per-device WebSocket routing."""
import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
import hmac
import json
import logging
import os
from pathlib import Path
import time

from aiohttp import web
from pipecat.pipeline.task import PipelineTask
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from app.phase_emitter import TurnLiveness
from app.diagnostics import device_tag
from app.raw_audio_serializer import RawAudioSerializer, MAC_PATTERN
from app.satellite_transport import SatelliteTransport
from app.websocket_handler import WebSocketHandler

logger = logging.getLogger(__name__)


@dataclass
class Satellite:
    mac: str
    name: str = ""
    caps: list[str] = field(default_factory=list)
    area: str | None = None
    generation: int = 0
    session: "DeviceSession | None" = None
    cached_history: object | None = None


class SatelliteRegistry:
    """Durable identity and metadata, separate from ephemeral connection state."""

    def __init__(self, path=None):
        self.path = Path(path or os.environ.get("SATELLITES_PATH", "/data/satellites.json"))
        self._devices: dict[str, Satellite] = {}
        self._listeners = []

    async def load(self):
        try:
            data = await asyncio.to_thread(self.path.read_text)
            for item in json.loads(data):
                mac = item["mac"]
                if MAC_PATTERN.fullmatch(mac):
                    self._devices[mac.lower()] = Satellite(mac.lower(), item.get("name", "")[:64])
        except FileNotFoundError:
            pass
        except (OSError, ValueError, KeyError, TypeError):
            logger.exception("Could not read satellites registry")

    def get(self, mac):
        return self._devices.get(mac.lower())

    def all(self):
        return list(self._devices.values())

    def connected(self):
        return [item for item in self.all() if item.session is not None]

    def on_change(self, callback):
        """Register a callback(event, satellite) for connected/disconnected/updated."""
        self._listeners.append(callback)
        return callback

    def notify(self, event, satellite):
        for callback in self._listeners:
            try:
                callback(event, satellite)
            except Exception:
                logger.exception("Satellite registry listener failed")

    def reserve(self, mac, name, caps, session):
        satellite = self._devices.setdefault(mac, Satellite(mac))
        old = satellite.session
        satellite.generation += 1
        satellite.session = session
        satellite.name = name or satellite.name
        satellite.caps = caps
        satellite.area = None
        session.metadata = satellite
        session.generation = satellite.generation
        session.replaced = old is not None
        return old

    def activate(self, session):
        item = session.metadata
        if item.session is session and item.generation == session.generation:
            self.notify("updated" if session.replaced else "connected", item)

    def release(self, session):
        item = session.metadata
        if item and item.generation == session.generation and item.session is session:
            item.session = None
            self.notify("disconnected", item)

    async def persist(self):
        data = json.dumps(
            [{"mac": s.mac, "name": s.name} for s in self.all()
             if MAC_PATTERN.fullmatch(s.mac)], separators=(",", ":")
        )

        def write():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            candidate = self.path.with_suffix(".json.new")
            candidate.write_text(data)
            os.replace(candidate, self.path)

        await asyncio.to_thread(write)


class DeviceSession:
    """One OpenAI pipeline and all disposable state for exactly one socket."""

    def __init__(self, app, registry, websocket, mac, name="", caps=None):
        self.app = app
        self.registry = registry
        self.websocket = websocket
        self.mac = mac
        self.name = name
        self.caps = caps or []
        self.metadata = None
        self.generation = 0
        self.serializer = RawAudioSerializer()
        self.serializer.set_device_message_handler(self.on_device_message)
        self._message_handlers = defaultdict(list)
        self.transport = SatelliteTransport(self, self.serializer)
        self.liveness = TurnLiveness()
        self.phase_emitter = None
        self.openai_service = None
        self.context = None
        self.pipeline_task: PipelineTask | None = None
        self.runner = None
        self.handler = None
        self.recording = None
        self.observers = []
        self._run_task = None
        self._close_task = None
        self._area_task = None
        self._writes = asyncio.Queue(maxsize=128)
        self._writer_task = asyncio.create_task(self._write_loop(), name=f"ws-writer:{mac}")
        self.counters = defaultdict(int)
        self.counters["ws_send_latency_ms_max"] = 0.0
        self.created_at = time.monotonic()
        self._turn_start = None
        self._closed = False
        self._stopped = asyncio.Event()
        self._starting = asyncio.Event()
        self._starting.set()

    def increment(self, key):
        self.counters[key] += 1

    def on_phase(self, phase):
        self.increment("phase_transitions")
        if phase == "listening":
            self._turn_start = time.monotonic()
        elif phase == "replying" and self._turn_start is not None:
            self.counters["turn_latency_ms_last"] = round(
                (time.monotonic() - self._turn_start) * 1000, 2
            )
            self._turn_start = None

    def subscribe(self, message_type, callback):
        """Register async callback(payload) for e.g. announce_* messages."""
        self._message_handlers[message_type].append(callback)

    async def on_device_message(self, message_type, payload):
        if message_type == "ping":
            await self.send_json({"type": "pong"})
        for callback in self._message_handlers[message_type]:
            await callback(payload)

    def is_idle(self):
        return self.phase_emitter is not None and self.phase_emitter._current in (None, "idle")

    async def queue_frames(self, frames):
        if self.pipeline_task is None:
            raise RuntimeError("Session pipeline is not running")
        for frame in frames:
            await self.pipeline_task.queue_frame(frame)

    async def send_json(self, obj):
        self._queue(json.dumps(obj, separators=(",", ":")))

    async def send_bytes(self, payload):
        self._queue(payload)

    def _queue(self, payload):
        if self._closed:
            raise ConnectionError("Satellite disconnected")
        try:
            self._writes.put_nowait(payload)
        except asyncio.QueueFull:
            self.counters["errors"] += 1
            self._close_task = asyncio.create_task(
                self.websocket.close(code=1013, reason="writer queue full")
            )
            raise ConnectionError("Satellite writer queue full")

    async def _write_loop(self):
        try:
            while True:
                payload = await self._writes.get()
                started = time.monotonic()
                await asyncio.wait_for(self.websocket.send(payload), timeout=2)
                elapsed = (time.monotonic() - started) * 1000
                self.counters["ws_send_latency_ms_max"] = max(
                    self.counters["ws_send_latency_ms_max"], round(elapsed, 2)
                )
                self.counters["ws_send_latency_ms_total"] += elapsed
                self.counters["ws_sends"] += 1
        except asyncio.CancelledError:
            raise
        except Exception:
            self.counters["errors"] += 1
            logger.warning("Satellite writer failed; closing socket", exc_info=True)
            await self.websocket.close(code=1011, reason="writer timed out")

    async def start(self):
        if self._closed:
            return
        self._starting.clear()
        try:
            await self._start()
        finally:
            self._starting.set()

    async def _start(self):
        self.recording = self.app.make_recorder(self.mac)
        self.handler = WebSocketHandler(
            session_manager=self.app.session_manager,
            audio_recording_service=self.recording,
            interrupt_response=self.app.interrupt_response,
            follow_up_ms=self.app.follow_up_ms,
            follow_up_open_delay_ms=self.app.follow_up_open_delay_ms,
            wake_open_delay_ms=self.app.wake_open_delay_ms,
            playback_prebuffer_ms=self.app.playback_prebuffer_ms,
            session=self,
        )
        self.serializer.set_timer_message_handler(self.handler.timer_bridge.handle_message)
        self.openai_service = await self.app.create_openai_service(self)
        if self._closed:
            return
        if self.app.turn_detection_type == "semantic_vad" and self.app.semantic_vad_create_response:
            from pipecat.processors.aggregators.llm_context import LLMContext
            if getattr(self.openai_service, "_context", None) is None:
                self.openai_service._context = LLMContext()
                self.openai_service._llm_needs_conversation_setup = False
        self.handler.configure_instructions(
            instructions=self.app.instructions, personality=self.app.personality,
            home_location=self.app.home_location, time_zone=self.app.time_zone,
            units=self.app.units, clock_tool=getattr(self, "clock_tool", "get_current_time"),
            ha_base=self.app.ha_base, ha_token=self.app.ha_token,
        )
        if self.app.tail_device and self.app.tail_device.lower() in (self.mac.lower(), self.name.lower()):
            try:
                from pipecat_tail.observer import TailObserver
                self.observers.append(TailObserver())
            except Exception:
                logger.warning("TailObserver unavailable; continuing without Tail", exc_info=True)
        _, self.runner, self.pipeline_task = self.handler.build_pipeline(
            self.transport, self.openai_service, self.mac
        )
        self.context = self.handler._context_aggregator.user().context
        if hasattr(self.openai_service, "set_history_context"):
            self.openai_service.set_history_context(self.context)
        self.handler.start_area_lookup(self.openai_service, {"mac": self.mac})
        if self._closed:
            return
        self._run_task = asyncio.create_task(
            self.runner.run(self.pipeline_task), name=f"pipeline:{self.mac}"
        )
        self._run_task.add_done_callback(self._pipeline_finished)
        await self.send_json({
            "type": "hello", "audio_out": "pcm",
            "interrupt_response": self.app.interrupt_response,
            "follow_up_ms": self.app.follow_up_ms,
            "follow_up_open_delay_ms": self.app.follow_up_open_delay_ms,
            "wake_open_delay_ms": self.app.wake_open_delay_ms,
            "playback_prebuffer_ms": self.app.playback_prebuffer_ms,
        })
        if self._closed:
            return
        await self.serializer.deserialize('{"type":"start"}')

    def _pipeline_finished(self, task):
        if self._closed:
            return
        try:
            task.result()
        except Exception:
            self.increment("errors")
            logger.exception("Satellite pipeline failed")
        else:
            logger.warning("Satellite pipeline ended unexpectedly")
        self._close_task = asyncio.create_task(
            self.websocket.close(code=1011, reason="pipeline ended")
        )

    async def stop(self):
        if self._closed:
            await self._stopped.wait()
            return
        self._closed = True
        try:
            await self._starting.wait()
            if self.handler and self.handler._area_task:
                self.handler._area_task.cancel()
                await asyncio.gather(self.handler._area_task, return_exceptions=True)
            if self.handler and self.handler._phase_emitter:
                emitter = self.handler._phase_emitter
                pending = (emitter._idle_task, emitter._watchdog_task)
                emitter._cancel_pending_idle()
                emitter._cancel_watchdog()
                await asyncio.gather(
                    *(task for task in pending if task), return_exceptions=True
                )
            if self.handler and hasattr(self.handler, "recovery"):
                await self.handler.recovery.shutdown()
            if self.pipeline_task and self._run_task:
                await self.pipeline_task.cancel()
                await asyncio.gather(self._run_task, return_exceptions=True)
            if self.handler:
                self.handler.timer_bridge.disconnected(self.websocket)
            if self.openai_service:
                if self._run_task:
                    self.app.session_manager.handle_client_disconnect(self.mac, self.openai_service)
                    if self.metadata is not None:
                        self.metadata.cached_history = self.app.session_manager.get_cached_context(self.mac)
                elif self.app.session_manager.get_current_service(self.mac) is self.openai_service:
                    del self.app.session_manager.current_services[self.mac]
                self.app.session_manager.remove_context_aggregator(self.mac)
            if self.recording:
                await asyncio.to_thread(self.recording.cleanup)
        except Exception:
            self.increment("errors")
            logger.exception("Satellite shutdown failed")
        finally:
            try:
                self._writer_task.cancel()
                await asyncio.gather(self._writer_task, return_exceptions=True)
                try:
                    await asyncio.wait_for(self.websocket.close(code=1000, reason="session ended"), 2)
                    if self._close_task:
                        await asyncio.wait_for(self._close_task, 2)
                except asyncio.TimeoutError:
                    logger.warning("Satellite socket did not close cleanly", exc_info=True)
                    self.websocket.transport.abort()
                except Exception:
                    logger.warning("Satellite socket did not close cleanly", exc_info=True)
            finally:
                self._stopped.set()

    def status(self):
        counters = dict(self.counters)
        sends = counters.get("ws_sends", 0)
        counters["ws_send_latency_ms_avg"] = round(
            counters.pop("ws_send_latency_ms_total", 0.0) / sends, 2
        ) if sends else 0.0
        return {
            "mac": self.mac, "name": self.metadata.name if self.metadata else self.name,
            "area": self.metadata.area if self.metadata else None,
            "caps": self.caps, "phase": self.phase_emitter._current if self.phase_emitter else None,
            "uptime_s": round(time.monotonic() - self.created_at, 1),
            "writer_queue_depth": self._writes.qsize(), "counters": counters,
        }


class SatelliteRouter:
    def __init__(self, app, registry, host, port, token="", handshake_timeout=5):
        self.app, self.registry = app, registry
        self.host, self.port, self.token = host, port, token
        self.handshake_timeout = handshake_timeout
        self._sockets = set()
        self._server = None
        self._persist_task = None
        self._takeover_locks = defaultdict(asyncio.Lock)
        self._warned = False

    async def start(self):
        self._server = await serve(
            self._accept, self.host, self.port, write_limit=32768, max_queue=16,
            max_size=65536,
        )
        self.port = self._server.sockets[0].getsockname()[1]
        if not self.token and not self._warned:
            logger.warning("Device token is empty: satellites are unauthenticated")
            self._warned = True

    async def close(self):
        for item in self.registry.connected():
            session = item.session
            await session.stop()
            self.registry.release(session)
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        if self._persist_task:
            await asyncio.gather(self._persist_task, return_exceptions=True)

    async def _persist(self):
        try:
            await self.registry.persist()
        except Exception:
            logger.exception("Could not persist satellites registry")

    async def _accept(self, websocket):
        if len(self._sockets) >= 8:
            await websocket.close(code=1013, reason="connection limit")
            return
        self._sockets.add(websocket)
        session = None
        try:
            try:
                first = await asyncio.wait_for(websocket.recv(), self.handshake_timeout)
                start = json.loads(first) if isinstance(first, str) and len(first) <= 512 else None
            except (asyncio.TimeoutError, ValueError, TypeError):
                start = None
            if not isinstance(start, dict) or start.get("type") != "start":
                await websocket.close(code=1008, reason="start required within 5 seconds")
                return
            mac = start.get("mac")
            if mac is not None and (not isinstance(mac, str) or not MAC_PATTERN.fullmatch(mac)):
                await websocket.close(code=1008, reason="invalid device MAC")
                return
            if self.token and (
                mac is None or not isinstance(start.get("token"), str)
                or not hmac.compare_digest(start["token"].encode("utf-8"), self.token.encode("utf-8"))
            ):
                await websocket.close(code=1008, reason="device authentication failed")
                return
            if mac is None:
                mac = websocket.remote_address[0]
            else:
                mac = mac.lower()
            name = start.get("name", "")
            if not isinstance(name, str) or len(name) > 64:
                await websocket.close(code=1008, reason="invalid name")
                return
            caps = start.get("caps", [])
            if not isinstance(caps, list) or len(caps) > 16 or any(
                not isinstance(c, str) or len(c) > 32 for c in caps
            ):
                await websocket.close(code=1008, reason="invalid capabilities")
                return
            tag_token = device_tag.set(f"{name or mac}/{mac}")
            session = DeviceSession(self.app, self.registry, websocket, mac, name, caps)
            async with self._takeover_locks[mac]:
                old = self.registry.reserve(mac, name, caps, session)
                if old:
                    await old.stop()
                if self.registry.get(mac).session is not session:
                    return
                await session.start()
                self.registry.activate(session)
            if MAC_PATTERN.fullmatch(mac):
                if self._persist_task:
                    try:
                        await self._persist_task
                    except Exception:
                        logger.exception("Could not persist satellites registry")
                self._persist_task = asyncio.create_task(self._persist())
            async for message in websocket:
                await session.transport.input().receive(message)
        except asyncio.CancelledError:
            raise
        except ConnectionClosed:
            pass
        except Exception:
            logger.exception("Satellite connection failed")
            if session:
                session.increment("errors")
        finally:
            if session:
                await session.stop()
                self.registry.release(session)
            self._sockets.discard(websocket)
            if session:
                device_tag.reset(tag_token)


class Diagnostics:
    def __init__(self, registry, router, token="", port=8081, enabled=None):
        self.registry, self.router, self.token, self.port = registry, router, token, port
        self.enabled = port != 0 if enabled is None else enabled
        self.started_at = time.monotonic()
        self.lags = []
        self._runner = None
        self._site = None
        self._monitor = None
        self._summary = None

    async def start(self, host="0.0.0.0"):
        self._monitor = asyncio.create_task(self._lag_loop(), name="loop-lag")
        self._summary = asyncio.create_task(self._summary_loop(), name="satellite-summary")
        if self.enabled:
            application = web.Application()
            application.router.add_get("/status", self._status)
            self._runner = web.AppRunner(application)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, host, self.port)
            await self._site.start()
            self.port = self._site._server.sockets[0].getsockname()[1]

    async def stop(self):
        for task in (self._monitor, self._summary):
            if task:
                task.cancel()
        await asyncio.gather(*(t for t in (self._monitor, self._summary) if t),
                             return_exceptions=True)
        if self._runner:
            await self._runner.cleanup()

    def lag(self):
        values = sorted(self.lags)
        if not values:
            return {"p50_ms": 0, "p99_ms": 0, "max_ms": 0}
        return {
            "p50_ms": round(values[len(values) // 2], 2),
            "p99_ms": round(values[min(len(values) - 1, int(len(values) * .99))], 2),
            "max_ms": round(values[-1], 2),
        }

    async def _lag_loop(self):
        expected = time.monotonic() + .1
        while True:
            await asyncio.sleep(max(0, expected - time.monotonic()))
            now = time.monotonic()
            lag = max(0, (now - expected) * 1000)
            self.lags.append(lag)
            if len(self.lags) > 600:
                self.lags.pop(0)
            if lag > 50:
                logger.warning("Event-loop lag %.1f ms", lag)
            expected = now + .1

    async def _summary_loop(self):
        previous = {}
        while True:
            await asyncio.sleep(60)
            for satellite in self.registry.connected():
                session = satellite.session
                current = (session.counters["audio_in_bytes"], session.counters["audio_out_bytes"])
                before = previous.get(session, (0, 0))
                previous[session] = current
                logger.info("Satellite %s: phase=%s in=%dB/s out=%dB/s queue=%d errors=%d",
                            satellite.mac, session.phase_emitter._current
                            if session.phase_emitter else None,
                            (current[0] - before[0]) // 60, (current[1] - before[1]) // 60,
                            session._writes.qsize(), session.counters["errors"])
            previous = {
                session: value for session, value in previous.items()
                if not session._closed
            }

    async def _status(self, request):
        if self.token and (
            not request.headers.get("Authorization", "").startswith("Bearer ")
            or not hmac.compare_digest(
                request.headers["Authorization"][7:].encode("utf-8"), self.token.encode("utf-8")
            )
        ):
            raise web.HTTPUnauthorized()
        return web.json_response({
            "uptime_s": round(time.monotonic() - self.started_at, 1),
            "loop_lag": self.lag(),
            "sessions": [satellite.session.status() for satellite in self.registry.connected()],
        })

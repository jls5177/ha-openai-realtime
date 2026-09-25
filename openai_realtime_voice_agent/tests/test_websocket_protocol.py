"""Device handshake and turn-boundary control frames."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from app.phase_emitter import PhaseEmitter
from app.websocket_handler import WebSocketHandler
import app.websocket_handler as websocket_module


@pytest.mark.parametrize("interrupt_response", [False, True])
def test_hello_advertises_barge_in_and_connection_resets_speech(interrupt_response):
    async def scenario():
        handler = WebSocketHandler(interrupt_response=interrupt_response)
        emitter = PhaseEmitter(None, interrupt_response=True)
        handler._phase_emitter = emitter
        callbacks = {}

        class Transport:
            def event_handler(self, name):
                def register(callback):
                    callbacks[name] = callback
                    return callback
                return register

        class Socket:
            client = SimpleNamespace(host="device")

            def __init__(self):
                self.messages = []

            async def send(self, message):
                self.messages.append(message)

        async def connected(client_id):
            assert client_id == "device"

        socket = Socket()
        transport = Transport()
        handler.setup_event_handlers(transport, connected)
        emitter._user_speaking = True
        await callbacks["on_client_connected"](transport, socket)
        assert not emitter._user_speaking
        assert json.loads(socket.messages[0]) == {
            "type": "hello",
            "audio_out": "pcm",
            "interrupt_response": interrupt_response,
            "follow_up_ms": 0,
            "follow_up_open_delay_ms": 700,
            "wake_open_delay_ms": 700,
            "playback_prebuffer_ms": 0,
        }
        await handler.broadcast_audio_done()
        assert socket.messages[1] == '{"type":"audio_done"}'

        emitter._user_speaking = True
        await callbacks["on_client_disconnected"](transport, socket)
        assert not emitter._user_speaking
        assert socket not in handler._websockets

    asyncio.run(scenario())


def test_device_interrupt_flush_and_start_reset_speech(monkeypatch):
    async def scenario():
        handler = WebSocketHandler(interrupt_response=True)
        callbacks = {}
        sent_events = []

        class Serializer:
            def set_interrupt_handler(self, callback):
                callbacks["interrupt"] = callback

            def set_session_start_handler(self, callback):
                callbacks["start"] = callback

            def set_mic_flush_handler(self, callback):
                callbacks["flush"] = callback

            def set_wake_handler(self, callback):
                callbacks["wake"] = callback

        class Service:
            _current_assistant_response = None

            def event_handler(self, name):
                return lambda callback: callback

            async def send_client_event(self, event):
                sent_events.append(event)

        class Runner:
            async def run(self, task):
                pass

        monkeypatch.setattr(websocket_module, "Pipeline", lambda components: components)
        monkeypatch.setattr(websocket_module, "PipelineRunner", Runner)
        monkeypatch.setattr(websocket_module, "PipelineTask", lambda *args, **kwargs: object())
        monkeypatch.setattr(websocket_module, "InputResampler", lambda **kwargs: object())
        monkeypatch.setattr(websocket_module, "TranscriptLogger", lambda **kwargs: object())
        handler._serializer = Serializer()
        transport = SimpleNamespace(input=lambda: object(), output=lambda: object())
        handler.build_pipeline(transport, Service(), "device")

        for boundary in ("interrupt", "flush", "start", "wake"):
            handler._phase_emitter._user_speaking = True
            await callbacks[boundary]()
            assert not handler._phase_emitter._user_speaking, boundary
        assert len(sent_events) == 3

    asyncio.run(scenario())

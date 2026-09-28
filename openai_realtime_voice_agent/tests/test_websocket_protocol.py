"""Device handshake and turn-boundary control frames."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from app.phase_emitter import PhaseEmitter
from app.websocket_handler import WebSocketHandler
import app.websocket_handler as websocket_module


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
            def __init__(self, **kwargs):
                pass

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
            if boundary == "start":
                await callbacks[boundary]({"type": "start"})
            else:
                await callbacks[boundary]()
            assert not handler._phase_emitter._user_speaking, boundary
        assert len(sent_events) == 3

    asyncio.run(scenario())

"""The connect callback must use the service already in the pipeline."""

import asyncio

from app.main import Application


def test_connections_keep_the_running_pipeline_service(monkeypatch):
    app = Application()
    service = object()
    calls = []

    class SessionManager:
        def __init__(self):
            self.services = {}
            self.disconnected = []

        def set_current_service(self, client_id, current):
            self.services[client_id] = current

        def get_current_service(self, client_id):
            return self.services.get(client_id)

        def handle_client_disconnect(self, client_id, current):
            self.disconnected.append((client_id, current))

    class RecordingService:
        def start_new_session(self, client_id):
            calls.append(("record", client_id))

        def stop_recording(self):
            calls.append(("stop_recording",))

    class Handler:
        def setup_event_handlers(self, **callbacks):
            self.callbacks = callbacks

    class Runner:
        async def run(self, task):
            callbacks = app.websocket_handler.callbacks
            for client_id in ("device-a", "device-b", "device-a"):
                await callbacks["on_client_connected_callback"](client_id)
                assert callbacks["openai_service_getter"](client_id) is service
            callbacks["on_client_disconnected_callback"]("device-a")

    async def initialize():
        app.session_manager = SessionManager()
        app.audio_recording_service = RecordingService()
        app.websocket_handler = Handler()
        app.websocket_transport = object()
        app.turn_detection_type = "server_vad"
        app.semantic_vad_create_response = False

    async def ensure_service():
        calls.append(("create_service",))
        app.openai_service = service

    def build_pipeline(transport, client_id):
        calls.append(("build_pipeline", client_id, app.openai_service))
        app.runner = Runner()
        app.current_task = object()

    async def cleanup():
        pass

    monkeypatch.setattr(app, "initialize", initialize)
    monkeypatch.setattr(app, "_ensure_openai_service", ensure_service)
    monkeypatch.setattr(app, "_build_pipeline_for_transport", build_pipeline)
    monkeypatch.setattr(app, "cleanup", cleanup)

    asyncio.run(app.run())
    assert calls.count(("create_service",)) == 1
    assert calls.count(("build_pipeline", "server", service)) == 1
    assert app.session_manager.services == {"device-a": service, "device-b": service}
    assert app.session_manager.disconnected == [("device-a", service)]
    assert calls.count(("record", "device-a")) == 2
    assert calls.count(("record", "device-b")) == 1

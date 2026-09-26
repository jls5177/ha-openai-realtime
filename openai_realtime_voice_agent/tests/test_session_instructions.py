import asyncio
from types import SimpleNamespace

from pipecat.services.openai.realtime.events import SessionProperties

from app.session_instructions import apply_instructions
from app.websocket_handler import WebSocketHandler
import app.websocket_handler as websocket_module


def test_apply_instructions_updates_context_and_sends_partial_once():
    async def scenario():
        messages = [{"role": "system", "content": "old"}, {"role": "user", "content": "hi"}]
        sent = []

        class Service:
            _session_properties = SessionProperties(instructions="old", tools=[{"name": "timer"}])
            _context = SimpleNamespace(get_messages=lambda: messages)

            async def send_client_event(self, event):
                sent.append(event.model_dump(exclude_none=True)["session"])

        restored = [{"role": "system", "content": "cached"}]
        restored_context = SimpleNamespace(get_messages=lambda: restored)
        service = Service()
        assert await apply_instructions(service, "new", restored_context)
        restored[0]["content"] = "stale again"
        assert not await apply_instructions(service, "new", restored_context)
        assert service._session_properties.instructions == messages[0]["content"] == "new"
        assert restored[0]["content"] == "new"
        assert service._session_properties.tools == [{"name": "timer"}]
        assert sent == [{"type": "realtime", "instructions": "new"}]

    asyncio.run(scenario())


def test_area_refresh_removes_stale_room_and_ignores_older_lookup(monkeypatch):
    async def scenario():
        sent = []
        stale = asyncio.Event()

        class Service:
            _session_properties = SessionProperties(instructions="initial")
            _context = None

            async def send_client_event(self, event):
                sent.append(event.model_dump(exclude_none=True)["session"]["instructions"])

        async def lookup(base, token, mac):
            if mac == "aa:bb:cc:dd:ee:ff":
                await stale.wait()
                return "Kitchen"
            return "Living Room"

        monkeypatch.setattr(websocket_module, "lookup_device_area", lookup)
        handler = WebSocketHandler()
        handler.configure_instructions(
            instructions="Base", personality="standard", home_location="",
            time_zone="UTC", units=None, clock_tool="get_current_time",
            ha_base="http://ha", ha_token="secret",
        )
        service = Service()
        handler.start_area_lookup(service, {"mac": "aa:bb:cc:dd:ee:ff"})
        await asyncio.sleep(0)
        handler.start_area_lookup(service, {"mac": "11:22:33:44:55:66"})
        await handler._area_task
        stale.set()
        await asyncio.sleep(0)
        assert "Kitchen" not in service._session_properties.instructions
        assert "Living Room" in service._session_properties.instructions
        handler.start_area_lookup(service, {"type": "start"})
        await handler._area_task
        assert "Living Room" not in service._session_properties.instructions
        assert "satellite's room is unknown" in service._session_properties.instructions
        assert "Living Room" not in sent[-1]

    asyncio.run(scenario())

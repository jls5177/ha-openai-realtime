"""Voice timer tools backed by the device's timer protocol."""
import asyncio
import itertools
import json
import logging
import secrets
from typing import Any, TYPE_CHECKING

from websockets.exceptions import ConnectionClosed

if TYPE_CHECKING:
    from pipecat.services.llm_service import FunctionCallParams
    from app.websocket_handler import WebSocketHandler

logger = logging.getLogger(__name__)

# Random per-process prefix so ids never collide with timers the device kept
# from a previous add-on process (the device treats a known id as a replace).
_ID_PREFIX = secrets.token_hex(3)
_TIMER_IDS = itertools.count(1)
_REQUEST_IDS = itertools.count(1)
ACK_TIMEOUT_S = 3.0
UNCERTAIN_MESSAGE = (
    "The device may have applied this request but did not confirm it. "
    "Call list_timers to check before retrying."
)


def get_timer_tool_definitions() -> list[dict[str, Any]]:
    """OpenAI Realtime function definitions for device-owned timers."""
    return [
        {
            "type": "function",
            "name": "set_timer",
            "description": "Set a timer on the connected voice device. Give duration_seconds OR hours/minutes/seconds, and optionally a name. Do not use Home Assistant timer tools.",
            "parameters": {
                "type": "object",
                "properties": {
                    "duration_seconds": {"type": "integer", "description": "Total timer duration in seconds (1 to 86400)."},
                    "hours": {"type": "integer", "description": "Hours in the duration."},
                    "minutes": {"type": "integer", "description": "Minutes in the duration."},
                    "seconds": {"type": "integer", "description": "Seconds in the duration."},
                    "name": {"type": "string", "description": "Optional name for the timer."},
                },
                "required": [],
            },
        },
        {
            "type": "function",
            "name": "cancel_timer",
            "description": "Cancel a timer by its name or id, or cancel all timers. If a name matches more than one timer, ask which one to cancel.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Name of the timer (case-insensitive)."},
                    "id": {"type": "string", "description": "Timer id returned by list_timers."},
                    "all": {"type": "boolean", "description": "True to cancel every timer."},
                },
                "required": [],
            },
        },
        {
            "type": "function",
            "name": "list_timers",
            "description": "List timers on the connected voice device, including their remaining time and whether they are ringing.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    ]


def _failure(message: str) -> dict[str, Any]:
    return {"success": False, "error": message}


def _uncertain(message: str) -> dict[str, Any]:
    return {**_failure(message), "uncertain": True}


def _duration(arguments: dict[str, Any]) -> int:
    parts = ("hours", "minutes", "seconds")
    has_parts = any(key in arguments for key in parts)
    if "duration_seconds" in arguments and has_parts:
        raise ValueError("Specify duration_seconds or hours/minutes/seconds, not both.")
    if "duration_seconds" not in arguments and not has_parts:
        raise ValueError("Provide a timer duration.")
    keys = ("duration_seconds",) if not has_parts else parts
    for key in keys:
        value = arguments.get(key, 0)
        if type(value) is not int or value < 0:
            raise ValueError("Timer duration must contain non-negative whole numbers.")
    total = (
        arguments["duration_seconds"] if not has_parts else
        arguments.get("hours", 0) * 3600 + arguments.get("minutes", 0) * 60 + arguments.get("seconds", 0)
    )
    if not 1 <= total <= 86400:
        raise ValueError("Timer duration must be between 1 and 86400 seconds.")
    return total


class TimerBridge:
    """Correlate device acknowledgements to requests; device owns timer state."""

    def __init__(self, handler: "WebSocketHandler"):
        self._handler = handler
        self._pending: dict[str, tuple[asyncio.Future, Any]] = {}
        self.timers: list[dict[str, Any]] = []

    def connected(self, websocket) -> None:
        self.timers = []

    def disconnected(self, websocket) -> None:
        self.timers = []
        for request_id, (future, target) in list(self._pending.items()):
            if target is websocket:
                self._pending.pop(request_id)
                if not future.done():
                    future.set_result(None)

    def handle_message(self, data: dict[str, Any]) -> None:
        message_type = data.get("type")
        if message_type == "timer_finished":
            logger.info("Device timer finished: %s (%s)", data.get("name", ""), data.get("id", ""))
            return
        if message_type not in ("timer_ack", "timer_state"):
            return
        timers = data.get("timers")
        if not isinstance(timers, list) or any(
            not isinstance(item, dict)
            or not isinstance(item.get("id"), str)
            or not isinstance(item.get("name"), str)
            or type(item.get("total_s")) is not int
            or type(item.get("remaining_s")) is not int
            or type(item.get("ringing")) is not bool
            for item in timers
        ):
            logger.warning("Invalid %s from device", message_type)
            return
        if message_type == "timer_state":
            self.timers = timers
            return
        request_id = data.get("request_id")
        pending = self._pending.get(request_id) if isinstance(request_id, str) else None
        if pending is None:
            logger.debug("Unmatched timer_ack: %s", request_id)
            return
        if type(data.get("ok")) is not bool:
            logger.warning("Invalid timer_ack for request %s", request_id)
            return
        self.timers = timers
        future, _ = pending
        if not future.done():
            future.set_result(data)

    def _new_timer_id(self) -> str:
        known = {timer["id"] for timer in self.timers}
        while True:
            timer_id = f"t{_ID_PREFIX}{next(_TIMER_IDS):x}"
            if timer_id not in known:
                return timer_id

    async def _request(self, payload: dict[str, Any], mutating: bool = False) -> dict[str, Any]:
        sockets = list(self._handler._websockets)
        if not sockets:
            return _failure("No voice device connected.")
        if len(sockets) != 1:
            return _failure("Multiple voice devices connected; timer target is unclear.")
        socket = sockets[0]
        request_id = f"r{_ID_PREFIX}{next(_REQUEST_IDS):x}"
        payload = {"request_id": request_id, **payload}
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = (future, socket)
        try:
            async def send_and_wait():
                await socket.send(json.dumps(payload, separators=(",", ":")))
                return await future

            ack = await asyncio.wait_for(send_and_wait(), ACK_TIMEOUT_S)
        except asyncio.TimeoutError:
            logger.warning("Device did not acknowledge %s (%s)", payload["type"], request_id)
            if mutating:
                return _uncertain(UNCERTAIN_MESSAGE)
            return _failure("Device did not confirm timer request (timed out).")
        except (ConnectionClosed, OSError, RuntimeError) as exc:
            logger.warning("Could not send %s to device: %r", payload["type"], exc)
            return _failure("Device did not confirm timer request (connection failed).")
        finally:
            self._pending.pop(request_id, None)
        if ack is None:
            if mutating:
                return _uncertain(UNCERTAIN_MESSAGE)
            return _failure("Device disconnected before confirming timer request.")
        if not ack["ok"]:
            error = ack.get("error", "unknown")
            logger.warning("Device rejected %s: %s", payload["type"], error)
            return _failure(f"Device did not confirm timer request ({error}).")
        return {"success": True, "timers": ack["timers"]}

    async def set_timer(self, arguments: dict[str, Any]) -> dict[str, Any]:
        try:
            duration = _duration(arguments)
        except ValueError as exc:
            return _failure(str(exc))
        name = arguments.get("name", "")
        if not isinstance(name, str):
            return _failure("Timer name must be text.")
        timer_id = self._new_timer_id()
        result = await self._request({
            "type": "timer_start", "id": timer_id,
            "name": name.strip(), "duration_s": duration,
        }, mutating=True)
        if result["success"]:
            result["message"] = "Timer set."
        return result

    async def list_timers(self, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        result = await self._request({"type": "timer_list"})
        if result["success"]:
            result["message"] = "Current timers." if result["timers"] else "No active timers."
        return result

    async def cancel_timer(self, arguments: dict[str, Any]) -> dict[str, Any]:
        name, timer_id, all_timers = (
            arguments.get("name"), arguments.get("id"), arguments.get("all", False)
        )
        if (type(all_timers) is not bool or
                (name is not None and (not isinstance(name, str) or not name.strip())) or
                (timer_id is not None and (not isinstance(timer_id, str) or not timer_id.strip()))):
            return _failure("Provide a valid timer name, id, or all=true.")
        if sum((bool(name), bool(timer_id), all_timers)) != 1:
            return _failure("Specify exactly one of name, id, or all=true.")
        if name is not None:
            listing = await self.list_timers()
            if not listing["success"]:
                return listing
            matches = [
                timer for timer in listing["timers"]
                if timer["name"].casefold() == name.strip().casefold()
            ]
            if len(matches) > 1:
                return {**_failure("Multiple timers have that name; ask which id to cancel."),
                        "timers": matches}
            if not matches:
                return {**_failure("No timer has that name."), "timers": listing["timers"]}
            timer_id = matches[0]["id"]
        payload = {"type": "timer_cancel", "all": True} if all_timers else {
            "type": "timer_cancel", "id": timer_id.strip()
        }
        result = await self._request(payload, mutating=True)
        if result["success"]:
            result["message"] = "Timer(s) cancelled."
        return result


def create_timer_tool_handler(bridge: TimerBridge, name: str):
    """Create a pipecat function-call handler for one timer operation."""
    operations = {
        "set_timer": bridge.set_timer,
        "cancel_timer": bridge.cancel_timer,
        "list_timers": bridge.list_timers,
    }
    operation = operations[name]

    async def timer_tool_handler(params: "FunctionCallParams") -> None:
        result = await operation(params.arguments or {})
        await params.result_callback(json.dumps(result, separators=(",", ":")))

    return timer_tool_handler

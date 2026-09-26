"""Home Assistant configuration and one-shot satellite area lookup."""

import json
import logging
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
import websockets

logger = logging.getLogger(__name__)


def core_base(ha_mcp_url: str) -> str:
    if not ha_mcp_url:
        return "http://supervisor/core"
    parsed = urlsplit(ha_mcp_url.rstrip("/"))
    if parsed.scheme not in ("http", "https") or not parsed.netloc or not parsed.path.endswith("/api/mcp"):
        raise ValueError("HA_MCP_URL must end in /api/mcp and use http or https for HA context lookup")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path[:-len("/api/mcp")], "", ""))


SUPERVISOR_CORE = "http://supervisor/core"


def context_endpoint(ha_mcp_url: str, longlived_token: str | None,
                     supervisor_token: str | None) -> tuple[str, str | None]:
    """Pick the HA base URL and the token that base accepts.

    The Supervisor proxy only accepts the add-on's own token, so a long-lived
    token is used only with a base taken from a user-provided .../api/mcp URL.
    """
    if ha_mcp_url:
        try:
            return core_base(ha_mcp_url), longlived_token or supervisor_token
        except ValueError:
            logger.info("HA_MCP_URL is not a Home Assistant /api/mcp URL; "
                        "using the Supervisor for home context")
    return SUPERVISOR_CORE, supervisor_token


async def fetch_home_config(base: str, token: str) -> dict:
    """Read HA core config once at startup, with a bounded HTTP timeout."""
    timeout = aiohttp.ClientTimeout(total=2.5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(
            f"{base}/api/config", headers={"Authorization": f"Bearer {token}"}
        ) as response:
            response.raise_for_status()
            config = await response.json()
    if not isinstance(config, dict):
        raise ValueError("HA /api/config did not return a JSON object")
    return config


def resolve_time_zone(config: dict, tz_env: str | None = None) -> str | None:
    for source, value in (("HA config", config.get("time_zone")), ("TZ", tz_env)):
        if not isinstance(value, str) or not value:
            continue
        try:
            ZoneInfo(value)
            return value
        except (ZoneInfoNotFoundError, ValueError):
            logger.warning("Invalid %s time zone %r; ignoring it", source, value)
    return None


def resolve_units(config: dict) -> str | None:
    unit_system = config.get("unit_system")
    name = unit_system.get("name") if isinstance(unit_system, dict) else unit_system
    units = {
        "us_customary": "US customary (Fahrenheit, miles)",
        "metric": "metric (Celsius, kilometers)",
    }.get(name) if isinstance(name, str) else None
    if units or not isinstance(unit_system, dict):
        return units
    if unit_system.get("temperature") == "°F" and unit_system.get("length") == "mi":
        return "US customary (Fahrenheit, miles)"
    if unit_system.get("temperature") == "°C" and unit_system.get("length") == "km":
        return "metric (Celsius, kilometers)"
    return None


def find_device_area(devices: list, areas: list, mac: str) -> str | None:
    """Map a device registry MAC connection to its assigned area name."""
    for device in devices:
        if not isinstance(device, dict):
            continue
        connections = device.get("connections", [])
        if not isinstance(connections, list):
            continue
        if not any(
            isinstance(connection, (list, tuple)) and len(connection) == 2
            and connection[0] == "mac" and isinstance(connection[1], str)
            and connection[1].lower() == mac.lower()
            for connection in connections
        ):
            continue
        area_id = device.get("area_id")
        for area in areas:
            if isinstance(area, dict) and area.get("area_id") == area_id and area_id:
                name = area.get("name")
                return name if isinstance(name, str) and name else None
        return None
    return None


async def lookup_device_area(base: str, token: str, mac: str) -> str | None:
    """Authenticate to HA's WebSocket API and read both registries once."""
    parsed = urlsplit(base)
    scheme = "wss" if parsed.scheme == "https" else "ws"
    url = urlunsplit((scheme, parsed.netloc, parsed.path.rstrip("/") + "/api/websocket", "", ""))
    async with websockets.connect(url, open_timeout=2) as socket:
        greeting = json.loads(await socket.recv())
        if greeting.get("type") != "auth_required":
            raise ValueError(f"Unexpected HA WebSocket greeting: {greeting.get('type')}")
        await socket.send(json.dumps({"type": "auth", "access_token": token}))
        auth = json.loads(await socket.recv())
        if auth.get("type") != "auth_ok":
            raise ValueError(f"HA WebSocket authentication failed: {auth.get('type')}")

        async def registry(command: str, request_id: int) -> list:
            await socket.send(json.dumps({"id": request_id, "type": command}))
            response = json.loads(await socket.recv())
            if response.get("id") != request_id or response.get("type") != "result" or not response.get("success"):
                raise ValueError(f"HA registry request failed: {command}")
            result = response.get("result")
            if not isinstance(result, list):
                raise ValueError(f"HA registry returned invalid data: {command}")
            return result

        devices = await registry("config/device_registry/list", 1)
        areas = await registry("config/area_registry/list", 2)
        return find_device_area(devices, areas, mac)

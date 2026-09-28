"""Home Assistant configuration and one-shot satellite area lookup."""

from contextlib import asynccontextmanager
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


def find_device_area(devices: list, areas: list, mac: str, config_entries: list | None = None) -> str | None:
    """Prefer the ESPHome device, then the first matching MAC with an area."""
    esphome_ids = {
        entry.get("entry_id") for entry in (config_entries or [])
        if isinstance(entry, dict) and entry.get("domain") == "esphome"
    }
    fallback = None
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
        # HA represents ESPHome and MQTT devices with the same MAC separately.
        area_id = device.get("area_id")
        for area in areas:
            if isinstance(area, dict) and area.get("area_id") == area_id and area_id:
                name = area.get("name")
                if isinstance(name, str) and name:
                    entries = device.get("config_entries", [])
                    if isinstance(entries, list) and any(
                        (isinstance(entry, dict) and entry.get("domain") == "esphome")
                        or (isinstance(entry, str) and entry in esphome_ids)
                        for entry in entries
                    ):
                        return name
                    if fallback is None:
                        fallback = name
                break
    return fallback


@asynccontextmanager
async def ha_registry(base: str, token: str):
    """Authenticated HA registry requests over one WebSocket connection."""
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

        async def registry(command: str, request_id: int, **params):
            await socket.send(json.dumps({"id": request_id, "type": command, **params}))
            response = json.loads(await socket.recv())
            if response.get("id") != request_id or response.get("type") != "result" or not response.get("success"):
                raise ValueError(f"HA registry request failed: {command}")
            result = response.get("result")
            if (command.endswith("/list") or command == "config_entries/get") and not isinstance(result, list):
                raise ValueError(f"HA registry returned invalid data: {command}")
            return result

        yield registry


async def lookup_device_area(base: str, token: str, mac: str) -> str | None:
    """Authenticate to HA's WebSocket API and read both registries once."""
    async with ha_registry(base, token) as registry:
        devices = await registry("config/device_registry/list", 1)
        areas = await registry("config/area_registry/list", 2)
        entries = await registry("config_entries/get", 3)
        return find_device_area(devices, areas, mac, entries)


async def device_registry_area_sync(base: str, token: str, mac: str, area: str) -> bool:
    """Copy the ESPHome room to the separate MQTT announcer device if necessary."""
    async with ha_registry(base, token) as request:
        devices = await request("config/device_registry/list", 1)
        areas = await request("config/area_registry/list", 2)
        entries = await request("config_entries/get", 3)
        esphome_ids = {e.get("entry_id") for e in entries if e.get("domain") == "esphome"}
        source = next((device for device in devices
                       if any(isinstance(c, (list, tuple)) and len(c) == 2 and
                              c[0] == "mac" and isinstance(c[1], str) and
                              c[1].lower() == mac.lower()
                              for c in device.get("connections", []))
                       and any((e in esphome_ids if isinstance(e, str) else
                                isinstance(e, dict) and e.get("domain") == "esphome")
                               for e in device.get("config_entries", []))), None)
        if source is None:
            return False
        target_area = source.get("area_id")
        if target_area not in {a.get("area_id") for a in areas if a.get("name") == area}:
            return False
        key = "oai_rt_" + mac.replace(":", "").lower()
        target = next((device for device in devices
                       if ["mqtt", key] in device.get("identifiers", [])), None)
        if not target:
            return False
        if target.get("area_id") == target_area:
            return True
        await request("config/device_registry/update", 4,
                      device_id=target["id"], area_id=target_area)
        return True

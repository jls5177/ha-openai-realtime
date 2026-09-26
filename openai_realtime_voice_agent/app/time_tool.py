"""A clock function tool for installations without Home Assistant's GetDateTime."""

import json
import logging
from calendar import day_name, month_name
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

if TYPE_CHECKING:
    from pipecat.services.llm_service import FunctionCallParams

logger = logging.getLogger(__name__)


def get_time_tool_definition() -> dict:
    return {
        "type": "function",
        "name": "get_current_time",
        "description": "Get the current local time and date at home. Always use for current time or date questions.",
        "parameters": {"type": "object", "properties": {}, "required": []},
    }


def current_time(time_zone: str | None, home_location: str, now: datetime | None = None) -> dict:
    """Format the current instant in the configured zone, or explicitly in UTC."""
    instant = now or datetime.now(timezone.utc)
    try:
        zone = ZoneInfo(time_zone) if time_zone else timezone.utc
    except ZoneInfoNotFoundError:
        logger.warning("Unknown time zone %r; returning UTC", time_zone)
        zone = timezone.utc
        time_zone = None
    local = instant.astimezone(zone)
    weekday = day_name[local.weekday()]
    hour = local.hour % 12 or 12
    return {
        "local_iso_timestamp": local.isoformat(),
        "weekday": weekday,
        "spoken_time": f"{hour}:{local.minute:02d} {'AM' if local.hour < 12 else 'PM'}",
        "date": f"{weekday}, {month_name[local.month]} {local.day}, {local.year}",
        "time_zone": time_zone or "UTC (home time zone unknown)",
        "home_location": home_location,
    }


def create_time_tool_handler(time_zone: str | None, home_location: str):
    async def get_current_time(params: "FunctionCallParams") -> None:
        await params.result_callback(json.dumps(current_time(time_zone, home_location)))

    return get_current_time

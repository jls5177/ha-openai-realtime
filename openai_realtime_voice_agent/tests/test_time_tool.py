import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

from app.time_tool import create_time_tool_handler, current_time, get_time_tool_definition


def test_time_tool_zone_and_unknown_zone():
    instant = datetime(2026, 9, 26, 18, 45, tzinfo=timezone.utc)
    assert current_time("America/Chicago", "Temple, Texas", instant) == {
        "local_iso_timestamp": "2026-09-26T13:45:00-05:00",
        "weekday": "Saturday",
        "spoken_time": "1:45 PM",
        "date": "Saturday, September 26, 2026",
        "time_zone": "America/Chicago",
        "home_location": "Temple, Texas",
    }
    unknown = current_time(None, "", instant)
    assert unknown["local_iso_timestamp"] == "2026-09-26T18:45:00+00:00"
    assert unknown["spoken_time"] == "6:45 PM"
    assert unknown["time_zone"] == "UTC (home time zone unknown)"
    assert current_time("bad/zone", "", instant) == unknown
    assert get_time_tool_definition()["name"] == "get_current_time"


def test_time_tool_handler_returns_json():
    async def scenario():
        answers = []

        async def result_callback(result):
            answers.append(json.loads(result))

        await create_time_tool_handler("UTC", "Temple")(SimpleNamespace(result_callback=result_callback))
        assert answers[0]["time_zone"] == "UTC"
        assert answers[0]["home_location"] == "Temple"

    asyncio.run(scenario())

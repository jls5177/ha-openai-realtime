import pytest

from app.personas import PERSONAS
from app.prompt_builder import VOICE_RULES, build_prompt


@pytest.mark.parametrize("personality", list(PERSONAS))
def test_persona_and_context_order(personality):
    custom = "LANGUAGE: Speak only English.\nKeep the house quiet."
    text = build_prompt(
        custom, personality, "Temple, Texas", "America/Chicago",
        "US customary (Fahrenheit, miles)", "HomeAssistant__GetDateTime", "Kitchen"
    )
    assert text.startswith(custom + "\n\n" + VOICE_RULES + "\n\n" + PERSONAS[personality])
    assert text.endswith(
        "CONTEXT:\nThe home is in Temple, Texas. When a request doesn't name a place, "
        "assume it is about Temple, Texas. Never ask the user where they are.\n"
        "The home's time zone is America/Chicago.\n"
        "Use US customary (Fahrenheit, miles) units.\n"
        "For the current date or time, always call HomeAssistant__GetDateTime — never guess or ask.\n"
        "You are speaking through the satellite in the Kitchen. If the user doesn't name a room, "
        "act on the Kitchen."
    )


def test_no_location_unknown_room_then_changed_room():
    options = dict(
        instructions="Custom.", personality="monday", home_location="",
        time_zone=None, units=None, clock_tool="get_current_time"
    )
    kitchen = build_prompt(**options, area="Kitchen")
    unknown = build_prompt(**options, area=None)
    living = build_prompt(**options, area="Living Room")
    assert "home is in" not in unknown
    assert "time zone is" not in unknown
    assert "Use metric" not in unknown
    assert "call get_current_time" in unknown
    assert "satellite's room is unknown" in unknown
    assert "Kitchen" not in unknown
    assert "Living Room" in living and "Kitchen" not in living
    assert kitchen != unknown != living
    assert "Use metric (Celsius, kilometers) units." in build_prompt(
        **{**options, "units": "metric (Celsius, kilometers)"}, area=None
    )

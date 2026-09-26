"""Compose session instructions from user preferences and current home context."""

from app.personas import PERSONAS

VOICE_RULES = (
    'VOICE RULES: Replies are spoken aloud, so answer in one or two short sentences. Answer, then stop: never tack on offers like "anything else?" or "let me know if you need more". Don\'t read out entity IDs, lists, or technical names; summarize instead ("three lights are on, in the kitchen and the living room"). Call tools silently and speak once, after they finish. For weather, give only current conditions, the high and low, and the chance of rain. Before unlocking a door, opening a garage door or gate, or disarming an alarm, ask for a quick yes and only act after the user confirms in their next turn. If the user says thanks, that\'s all, never mind, or goodbye, reply with at most three words. In an emergency, or if someone sounds hurt or scared, drop any personality and be calm and direct.'
)


def build_prompt(
    instructions: str,
    personality: str,
    home_location: str,
    time_zone: str | None,
    units: str | None,
    clock_tool: str,
    area: str | None,
) -> str:
    """Build the complete prompt; an unknown area never inherits a prior room."""
    context = []
    if home_location:
        context.append(
            f"The home is in {home_location}. When a request doesn't name a place, "
            f"assume it is about {home_location}. Never ask the user where they are."
        )
    if time_zone:
        context.append(f"The home's time zone is {time_zone}.")
    if units:
        context.append(f"Use {units} units.")
    context.append(f"For the current date or time, always call {clock_tool} — never guess or ask.")
    if area:
        context.append(
            f"You are speaking through the satellite in the {area}. "
            f"If the user doesn't name a room, act on the {area}."
        )
    else:
        context.append(
            "The satellite's room is unknown; if a request needs a room and none is named, ask which room."
        )
    return "\n\n".join((instructions, VOICE_RULES, PERSONAS[personality], "CONTEXT:\n" + "\n".join(context)))

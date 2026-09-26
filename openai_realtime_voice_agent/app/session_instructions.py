"""Keep the live and reconnect-time Realtime instructions consistent."""

from pipecat.services.openai.realtime.events import SessionProperties, SessionUpdateEvent


def _update_system_context(context, text: str) -> None:
    messages = context.get_messages() if context is not None else None
    if messages and isinstance(messages[0], dict) and messages[0].get("role") == "system":
        messages[0]["content"] = text


async def apply_instructions(openai_service, text: str, additional_context=None) -> bool:
    """Send only changed instructions, preserving the session's audio and tools."""
    properties = openai_service._session_properties
    _update_system_context(getattr(openai_service, "_context", None), text)
    _update_system_context(additional_context, text)
    if properties.instructions == text:
        return False
    properties.instructions = text
    await openai_service.send_client_event(
        SessionUpdateEvent(session=SessionProperties(instructions=text))
    )
    return True

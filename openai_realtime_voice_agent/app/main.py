"""Main application entry point using Pipecat."""
import os
import sys
import asyncio
import logging
import signal
from typing import Optional
import dotenv
from pipecat.services.openai.realtime.llm import OpenAIRealtimeLLMService
from pipecat.services.openai.realtime import events as realtime_events
from app.satellites import SatelliteRegistry, SatelliteRouter, Diagnostics
from app.announcements import AnnouncementManager
from app.mqtt_bridge import MQTTBridge
from app.diagnostics import install_logging
from app.mcp_service import HomeAssistantMCPService
from app.disconnect_tool import get_disconnect_tool_definition, create_disconnect_tool_handler
from app.web_search_tool import get_web_search_tool_definition, create_web_search_tool_handler
from app.timer_tool import get_timer_tool_definitions, create_timer_tool_handler
from app.home_context import context_endpoint, fetch_home_config, resolve_time_zone, resolve_units
from app.personas import PERSONAS
from app.prompt_builder import build_prompt
from app.time_tool import get_time_tool_definition, create_time_tool_handler
from app.audio_recording_service import AudioRecordingService
from app.session_manager import SessionManager, bounded_history_messages

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Reduce verbosity of noisy loggers
logging.getLogger("aiortc").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("__main__").setLevel(logging.INFO)


def _resolve_choice(env_var: str, custom_env_var: str, default: str) -> str:
    """Resolve a dropdown option that supports a 'custom' escape hatch.

    The add-on UI renders these as a `list(...|custom)` dropdown plus a sibling
    free-text *_custom field. When the dropdown is set to "custom", use the
    custom field's value; otherwise use the dropdown value. Falls back to
    `default` if the resolved value is empty (e.g. "custom" picked but the custom
    field left blank).
    """
    choice = os.environ.get(env_var, default).strip()
    if choice.lower() == "custom":
        custom = os.environ.get(custom_env_var, "").strip()
        if custom:
            return custom
        logger.warning(
            f"⚠️ {env_var}=custom but {custom_env_var} is empty; falling back to {default!r}"
        )
        return default
    return choice or default

dotenv.load_dotenv()


def recordings_dir() -> str:
    """Recording output dir: RECORDINGS_DIR, else /share (visible to users), else ./recordings."""
    configured = os.environ.get("RECORDINGS_DIR")
    if configured:
        return configured
    if os.path.isdir("/share"):
        return "/share/openai_realtime_voice_agent/recordings"
    return "recordings"


class SafeRealtimeLLMService(OpenAIRealtimeLLMService):
    """OpenAIRealtimeLLMService with audio-truncation-on-interruption disabled.

    pipecat's `_truncate_current_audio_response()` (called by `_handle_interruption`
    on EVERY interruption — both our device "stop" AND pipecat's own server-VAD
    barge-in when the user wakes/speaks mid-reply) sends a
    `conversation.item.truncate` with `audio_end_ms = wall-clock ms since audio
    start`. But OpenAI BURSTS the reply faster than real-time, so that elapsed
    value massively overshoots the audio that actually exists, and OpenAI rejects
    it with `invalid_request_error("Audio content of N ms is already shorter than
    M ms")`. That errored truncate wedges the realtime session, so the user's very
    next turn gets NO response — the recurring "interrupt, then immediately ask
    again → silence" bug (confirmed in logs: session goes quiet right after
    `_truncate_current_audio_response`).

    The device stops playback authoritatively on its own, so server-side
    truncation buys us nothing. No-op it. (Cost: OpenAI's conversation history
    keeps the full assistant text the user may not have fully heard — purely
    cosmetic for context.)
    """

    async def _truncate_current_audio_response(self):  # type: ignore[override]
        return

    def set_history_context(self, context, max_messages=0):
        self._history_context = context
        self._history_max_messages = max(0, int(max_messages))
        self._history_seeded = False
        self._completed_tool_calls.update(
            message["tool_call_id"] for message in context.get_messages()
            if isinstance(message, dict) and message.get("tool_call_id")
            and message.get("content") != "IN_PROGRESS"
        )

    def _replay_messages(self):
        context = self._history_context
        messages = context.get_messages()
        limit = self._history_max_messages
        bounded = bounded_history_messages(messages, limit)
        if bounded is not messages:
            context.set_messages(bounded)
            messages = context.get_messages()
        replay = [message for message in messages
                  if isinstance(message, dict) and message.get("role") in ("user", "assistant")]
        return replay[-limit:] if limit else replay

    async def _handle_evt_session_updated(self, evt):  # type: ignore[override]
        # The server VAD creates responses itself. Pipecat's context setup only
        # runs inside _create_response(), so seed the new API conversation here.
        self._run_llm_when_api_session_ready = False
        await super()._handle_evt_session_updated(evt)
        if getattr(self, "_history_seeded", False):
            return
        self._history_seeded = True
        context = getattr(self, "_history_context", None)
        if context is None:
            return
        # These items are seeded explicitly; pipecat's setup would otherwise
        # serialize the same history into a second conversation item.
        self._llm_needs_conversation_setup = False
        for message in self._replay_messages():
            content = message.get("content")
            if isinstance(content, str):
                text = content
            elif isinstance(content, list):
                text = "\n".join(
                    part["text"] for part in content
                    if isinstance(part, dict) and part.get("type") in
                    ("text", "input_text", "output_text") and isinstance(part.get("text"), str)
                )
            else:
                continue
            if not text:
                continue
            role = message["role"]
            item = realtime_events.ConversationItem(
                type="message", role=role,
                content=[realtime_events.ItemContent(
                    type="input_text" if role == "user" else "output_text", text=text
                )],
            )
            event = realtime_events.ConversationItemCreateEvent(item=item)
            self._messages_added_manually[item.id] = True
            await self.send_client_event(event)

    async def reset_conversation(self):  # type: ignore[override]
        """Reconnect WITHOUT forcing a response on the reconnected session.

        pipecat's reset_conversation() (used by ConnectionRecovery on a 60-min cap
        / keepalive drop) reconnects and leaves `_llm_needs_conversation_setup =
        True`. The collision: if a turn was mid-flight when the WS dropped,
        `_create_response()` had already set `_run_llm_when_api_session_ready =
        True` (because `_api_session_ready` went False on disconnect). After the
        reconnect, the `session.updated` handler sees that flag and fires
        `_create_response()` — but under semantic_vad (`create_response=true`) the
        SERVER also auto-creates a response for the user's next turn. Two
        response.create events collide → `conversation_already_has_active_response`,
        and that turn gets no answer (observed: first turn right after a reconnect
        fails, ~1 in 20 reconnects — whenever the user happens to speak in the few
        seconds just after a reconnect).

        Re-seed the live local history into the new API conversation on each
        pipecat-level reconnect (the previous server conversation is gone).
        Clear the response flag before reconnecting so session.updated never
        generates a response in addition to server VAD.
        """
        self._history_seeded = False
        self._run_llm_when_api_session_ready = False
        await super().reset_conversation()
        try:
            self._run_llm_when_api_session_ready = False
            self._llm_needs_conversation_setup = False
        except Exception as e:  # pragma: no cover - defensive
            logger.warning(f"⚠️ could not clear post-reconnect response flags: {e!r}")

    # Error codes that must NOT kill the realtime session. pipecat 0.0.97's
    # _receive_task_handler does `_handle_evt_error(evt); return` on EVERY
    # error event — the reader task dies, the in-flight reply cuts off
    # mid-sentence and the session is deaf until the next connection death.
    # Observed live (2026-06-10): semantic_vad split one utterance into two
    # turns, the server's auto-created second response collided with the
    # first → conversation_already_has_active_response → the playing reply
    # stopped at 4.4 s and the session wedged. These codes are harmless
    # protocol races; the right move is to keep reading.
    BENIGN_ERROR_CODES = {
        # The server auto-created a response while one was still active
        # (VAD split a sentence into two turns). The active response keeps
        # streaming — nothing is broken.
        "conversation_already_has_active_response",
        # response.cancel landed after the response already finished (device
        # "stop" / the post-interrupt racing-response kill) — nothing to
        # cancel, nothing broken.
        "response_cancel_not_active",
        # input_audio_buffer.commit raced our input_audio_buffer.clear (device
        # "stop"): an empty commit is exactly the outcome we wanted.
        "input_audio_buffer_commit_empty",
    }

    async def _maybe_handle_evt_retrieve_conversation_item_error(self, evt):  # type: ignore[override]
        """Generic benign-error filter, hooked into pipecat's receive loop.

        pipecat's `_receive_task_handler` treats a True return from this
        method as "error handled — keep the receive loop alive"; every other
        error event kills the reader task (`_handle_evt_error` + `return`).
        It is the ONLY surviving path, so besides the original retrieve-item
        case (super()), we declare our benign protocol races handled here
        instead of letting them cut off live audio and wedge the session.
        """
        if await super()._maybe_handle_evt_retrieve_conversation_item_error(evt):
            return True
        code = getattr(getattr(evt, "error", None), "code", None)
        if code in self.BENIGN_ERROR_CODES:
            logger.warning(
                f"⚠️ benign realtime error ignored (session stays alive): {code}"
            )
            return True
        return False

    def register_function(self, function_name, handler, start_callback=None, *,
                          cancel_on_interruption: bool = True):  # type: ignore[override]
        """Force cancel_on_interruption=False for every tool registration.

        pipecat cancels in-flight function-call tasks on EVERY user-speech
        interruption — and semantic_vad fires one per utterance fragment, so
        merely continuing your own sentence kills the tool call your previous
        fragment started. By then the HTTP request to Home Assistant has
        usually already been SENT: the action executes, but its result never
        reaches the model, which then tells the user it failed (observed
        live: the lights turned ON while the assistant claimed they
        wouldn't). Our tools are all short-lived (HA service calls, one web
        search), so letting them finish and report the truth always beats
        killing them halfway. This single override covers every registration
        path (MCP tools via pipecat's MCPClient, web_search, disconnect).

        The handler is also wrapped to tick TURN_LIVENESS around its run, so
        the PhaseEmitter's thinking-watchdog knows a tool is in flight and a
        slow tool (web search: 10-20 s of pipeline silence) is never mistaken
        for a dead turn. All our handlers use the single-param
        FunctionCallParams signature, so the wrapper does too (pipecat
        inspects the signature to pick the calling convention).
        """
        liveness = getattr(self, "turn_liveness", None)
        async def liveness_tracked(params):
            if liveness:
                liveness.tool_started()
            try:
                return await handler(params)
            finally:
                if liveness:
                    liveness.tool_finished()

        super().register_function(
            function_name, liveness_tracked, start_callback, cancel_on_interruption=False
        )

    async def _receive_task_handler(self):  # type: ignore[override]
        """Surface OpenAI reader death as an ErrorFrame so recovery can act.

        pipecat's receive loop can end without producing ANY ErrorFrame: a
        silent server-side close ends the `async for` normally, and a network
        drop raises ConnectionClosed, which the task manager merely LOGS
        ("unexpected exception"). Nothing reaches ConnectionRecovery either
        way, so the session sat deaf for HOURS until the next user utterance
        hit the dead socket — losing that utterance (observed live twice).
        Wrap the loop and report its end; ConnectionRecovery treats the
        message as a reconnect trigger.
        """
        try:
            await super()._receive_task_handler()
        except asyncio.CancelledError:
            raise  # our own disconnect/reset tearing the task down — not a death
        except Exception as e:
            await self.push_error(error_msg=f"realtime receive loop died: {e!r}")
            return
        # Loop ended without an exception: a clean server-side close, or the
        # fatal-error path (which already pushed its own ErrorFrame —
        # duplicates collapse in ConnectionRecovery's cooldown/guard).
        await self.push_error(error_msg="realtime receive loop ended — connection closed")


class Application:
    """Main application class using Pipecat."""
    
    def __init__(self):
        """Initialize application."""
        self.mcp_service: Optional[HomeAssistantMCPService] = None
        self.session_manager: Optional[SessionManager] = None
        
    async def initialize(self) -> None:
        """Initialize all components."""
        # Get configuration from environment
        openai_api_key = os.environ.get("OPENAI_API_KEY")
        websocket_port = int(os.environ.get("WEBSOCKET_PORT", "8080"))
        websocket_host = os.environ.get("WEBSOCKET_HOST", "0.0.0.0")
        
        # Get turn detection settings with defaults
        vad_threshold = float(os.environ.get("VAD_THRESHOLD", "0.5"))
        vad_prefix_padding_ms = int(os.environ.get("VAD_PREFIX_PADDING_MS", "300"))
        vad_silence_duration_ms = int(os.environ.get("VAD_SILENCE_DURATION_MS", "800"))

        # Turn detection mode. "semantic_vad" is OpenAI's recommended mode for
        # natural conversation: it detects a *semantic* end-of-utterance instead
        # of a fixed silence window, so it doesn't cut the user off on a pause
        # and is more resistant to speaker->mic echo. "server_vad" is the classic
        # silence-based detector tuned by the vad_* values above.
        turn_detection_type = os.environ.get("TURN_DETECTION_TYPE", "semantic_vad").strip().lower()
        if turn_detection_type not in ("semantic_vad", "server_vad"):
            logger.warning(f"⚠️ Unknown TURN_DETECTION_TYPE '{turn_detection_type}', falling back to semantic_vad")
            turn_detection_type = "semantic_vad"
        # semantic_vad eagerness: "low" waits longest before deciding the user is
        # done (fewest mid-sentence cut-offs). low | medium | high | auto.
        vad_eagerness = os.environ.get("VAD_EAGERNESS", "low").strip().lower()
        if vad_eagerness not in ("low", "medium", "high", "auto"):
            logger.warning(f"⚠️ Unknown VAD_EAGERNESS '{vad_eagerness}', falling back to low")
            vad_eagerness = "low"
        # Whether detected user speech may interrupt the assistant's reply
        # (handsfree barge-in). With imperfect device-side AEC, set this false so
        # speaker echo can't cut replies short; interrupt then only via the
        # device "stop" wake word / center button.
        interrupt_response = os.environ.get("INTERRUPT_RESPONSE", "false").strip().lower() == "true"
        # Who creates the OpenAI response each user turn (semantic_vad only).
        # TRUE (default) = the server creates a response on every detected
        # end-of-turn. This is REQUIRED for multi-turn: Pipecat 0.0.97's realtime
        # service only auto-creates a response for the FIRST context (turn 1) and
        # after tool results; plain 2nd/3rd user turns get NO response unless the
        # server makes it. FALSE reproduces the old single-turn-only behaviour
        # (turn 1 answers, turn 2 hangs in "thinking"). See _ensure_openai_service.
        semantic_vad_create_response = os.environ.get("SEMANTIC_VAD_CREATE_RESPONSE", "true").strip().lower() == "true"
        # Expose the `disconnect_client` tool to the model. DEFAULT FALSE: on the
        # Voice PE the device owns its own session lifecycle (wake word starts a
        # turn, the no-speech watchdog / idle phase ends it), so a model-driven
        # disconnect just tears down the persistent WebSocket mid-conversation —
        # it was seen closing the socket DURING the first reply ("conversation_ended").
        # Only enable if your device relies on the backend to hang up.
        enable_disconnect_tool = os.environ.get("ENABLE_DISCONNECT_TOOL", "false").strip().lower() == "true"
        # Pin the input-transcription language (ISO code, e.g. "nl"). Empty = let
        # the model auto-detect. Helps stop the model drifting to another
        # language; pair it with an explicit language lock in `instructions`.
        transcription_language = os.environ.get("TRANSCRIPTION_LANGUAGE", "").strip()
        # Model that transcribes the user's speech to TEXT (the transcript shown
        # in logs + put in the context). NOTE: this is NOT what gpt-realtime-2
        # uses to understand you — the main model hears the audio natively; this
        # only affects the side-channel transcript. Default "gpt-4o-transcribe".
        # Alternatives: "gpt-4o-mini-transcribe", "whisper-1", and the newer
        # streaming "gpt-realtime-whisper" (purpose-built for the Realtime API,
        # faster/cheaper). If the API rejects a value, transcription silently
        # falls back; check the logs.
        transcription_model = _resolve_choice(
            "TRANSCRIPTION_MODEL", "TRANSCRIPTION_MODEL_CUSTOM", "gpt-4o-transcribe"
        )

        # Get instructions with default
        instructions = os.environ.get(
            "INSTRUCTIONS",
            "You are the Home Assistant voice agent and can control the smart home. "
            "LANGUAGE: Speak and understand only English; never switch language. "
            "Use set_timer, cancel_timer and list_timers for device timers, not Home Assistant timer entities. "
            "Only confirm timer actions when the device confirms them.",
        )
        personality = os.environ.get("PERSONALITY", "monday").strip()
        if personality not in PERSONAS:
            logger.warning("Unknown personality %r; using monday", personality)
            personality = "monday"
        home_location_option = os.environ.get("HOME_LOCATION", "").strip()

        # OpenAI Realtime model + voice. These are dropdowns in the add-on UI with
        # a "custom" sentinel + a sibling *_CUSTOM free-text field; _resolve_choice
        # returns the custom value when the dropdown is "custom", else the dropdown.
        openai_model = _resolve_choice("OPENAI_MODEL", "OPENAI_MODEL_CUSTOM", "gpt-realtime-2")
        openai_voice = _resolve_choice("OPENAI_VOICE", "OPENAI_VOICE_CUSTOM", "marin")

        # Playback speed (post-generation rate): 0.25-1.5, 1.0 = normal. Clamped.
        try:
            openai_speed = float(os.environ.get("OPENAI_SPEED", "1.0"))
        except (TypeError, ValueError):
            openai_speed = 1.0
        openai_speed = max(0.25, min(1.5, openai_speed))
        # Max reply length in output tokens. 0 = unlimited (API default). Caps a
        # runaway monologue + bounds per-response output-token cost.
        try:
            max_output_tokens = int(os.environ.get("MAX_OUTPUT_TOKENS", "0"))
        except (TypeError, ValueError):
            max_output_tokens = 0
        # Pass None when 0/unset so SessionProperties omits it (API default "inf").
        max_output_tokens = max_output_tokens if max_output_tokens > 0 else None
        # Input noise reduction: "near_field" | "far_field" | "" (off). Anything
        # else is treated as off so a typo can't reach the API.
        noise_reduction = os.environ.get("NOISE_REDUCTION", "").strip().lower()
        if noise_reduction not in ("near_field", "far_field"):
            noise_reduction = ""

        # Optional allow-list to trim the (large) ha-mcp tool set exposed to the
        # model. Comma-separated tool names; empty means expose all.
        mcp_tool_allowlist = [t.strip() for t in os.environ.get("MCP_TOOL_ALLOWLIST", "").split(",") if t.strip()]
        
        # Web search: let the assistant look things up online (weather, news,
        # facts). ON by default; existing installs keep their saved option, so an
        # Update won't silently flip it. When on, a `web_search` function tool
        # calls OpenAI's Responses web_search built-in tool server-side (using
        # OPENAI_API_KEY) and returns a short spoken answer. The model is
        # configurable so a different price/quality — or a renamed model — needs
        # no code change.
        enable_web_search = os.environ.get("ENABLE_WEB_SEARCH", "true").lower() == "true"
        enable_timers = os.environ.get("ENABLE_TIMERS", "true").strip().lower() == "true"
        web_search_model = _resolve_choice(
            "WEB_SEARCH_MODEL", "WEB_SEARCH_MODEL_CUSTOM", "gpt-5.5"
        )
        announcement_choice = os.environ.get("ANNOUNCEMENT_MODEL", "").strip()
        announcement_model = (
            web_search_model if announcement_choice.lower() in ("", "null") else announcement_choice
        )
        tts_choice = os.environ.get("ANNOUNCEMENT_TTS_MODEL", "").strip()
        announcement_tts_model = (
            "gpt-4o-mini-tts" if tts_choice.lower() in ("", "null") else tts_choice
        )

        # Get recording setting (optional, defaults to false)
        enable_recording = os.environ.get("ENABLE_RECORDING", "false").lower() == "true"
        
        # Post-reply follow-up window: how many seconds the device keeps the mic
        # open after the assistant finishes so the user can answer back without
        # re-saying the wake word. Sent to the device in the `hello` handshake as
        # follow_up_ms; the device opens the mic (after its TTS tail drains) and
        # shows the listening LED for that long. 0 disables (turn-based).
        try:
            follow_up_listen_seconds = int(os.environ.get("FOLLOW_UP_LISTEN_SECONDS", "8"))
        except (TypeError, ValueError):
            follow_up_listen_seconds = 8
        follow_up_listen_seconds = max(0, min(60, follow_up_listen_seconds))
        follow_up_ms = follow_up_listen_seconds * 1000
        # Delay (ms) before the follow-up mic opens, bridging the device speaker's
        # hardware tail so the mic doesn't catch the reply's own end. Sent to the
        # device in `hello`; lower = snappier, higher = safer against echo.
        try:
            follow_up_open_delay_ms = int(os.environ.get("FOLLOW_UP_OPEN_DELAY_MS", "700"))
        except (TypeError, ValueError):
            follow_up_open_delay_ms = 700
        follow_up_open_delay_ms = max(0, min(5000, follow_up_open_delay_ms))
        # Same idea at the WAKE boundary: delay (ms) after the wake chime before
        # the mic opens, so the chime's own hardware tail doesn't leak into the
        # fresh mic and become a ghost turn (the wake-path twin of
        # follow_up_open_delay_ms — the yaml wake handler reads it via a lambda).
        try:
            wake_open_delay_ms = int(os.environ.get("WAKE_OPEN_DELAY_MS", "700"))
        except (TypeError, ValueError):
            wake_open_delay_ms = 700
        wake_open_delay_ms = max(0, min(5000, wake_open_delay_ms))
        # Playback jitter buffer (ms): the device holds incoming TTS until this
        # much has accumulated before playing, so a brief network hiccup doesn't
        # dry out the speaker chain mid-word (audible crackle). Sent in `hello`.
        try:
            playback_prebuffer_ms = int(os.environ.get("PLAYBACK_PREBUFFER_MS", "150"))
        except (TypeError, ValueError):
            playback_prebuffer_ms = 150
        playback_prebuffer_ms = max(0, min(2000, playback_prebuffer_ms))

        # Get session reuse timeout and initialize session manager
        session_reuse_timeout = float(os.environ.get("SESSION_REUSE_TIMEOUT_SECONDS", "300"))
        # Cap on restored conversation history (0 = unlimited). Bounds per-turn
        # tokens so a long chat doesn't trip OpenAI's TPM rate limit (gpt-realtime
        # re-bills the whole conversation on every response; pipecat has no
        # truncation). Default 12 keeps recent continuity cheaply.
        try:
            max_context_messages = int(os.environ.get("MAX_CONTEXT_MESSAGES", "12"))
        except (TypeError, ValueError):
            max_context_messages = 12
        max_context_messages = max(0, max_context_messages)
        self.session_manager = SessionManager(
            reuse_timeout=session_reuse_timeout,
            max_restored_messages=max_context_messages,
        )
        logger.info(
            f"Session reuse timeout: {session_reuse_timeout} seconds, "
            f"max restored messages: {max_context_messages or 'unlimited'}"
        )
        
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")

        supervisor_token = os.environ.get("LONGLIVED_TOKEN") or os.environ.get("SUPERVISOR_TOKEN")
        ha_mcp_url = os.environ.get("HA_MCP_URL") or "http://supervisor/core/api/mcp"
        self.ha_base, self.ha_token = context_endpoint(
            os.environ.get("HA_MCP_URL", ""), os.environ.get("LONGLIVED_TOKEN"),
            os.environ.get("SUPERVISOR_TOKEN"))
        home_config = {}
        try:
            if self.ha_token:
                home_config = await fetch_home_config(self.ha_base, self.ha_token)
            else:
                logger.warning("No HA token available for home context lookup")
        except Exception as e:
            logger.warning("Could not load HA home configuration: %s", e)
        self.home_location = home_location_option
        self.time_zone = resolve_time_zone(home_config, os.environ.get("TZ"))
        self.units = resolve_units(home_config)

        # Initialize Home Assistant MCP Service
        mcp_client = None
        try:
            if supervisor_token:
                logger.info("Loading Home Assistant MCP tools...")
                self.mcp_service = HomeAssistantMCPService(url=ha_mcp_url, access_token=supervisor_token)
                mcp_client = await self.mcp_service.initialize()
                logger.info("✅ Home Assistant MCP Client initialized")
            else:
                logger.warning("⚠️ SUPERVISOR_TOKEN not set, skipping Home Assistant MCP integration")
        except Exception as e:
            logger.warning(f"⚠️ Failed to initialize Home Assistant MCP Client: {e}")
        
        self.enable_recording = enable_recording
        self.max_context_messages = max_context_messages
        self.follow_up_ms = follow_up_ms
        self.follow_up_open_delay_ms = follow_up_open_delay_ms
        self.wake_open_delay_ms = wake_open_delay_ms
        self.playback_prebuffer_ms = playback_prebuffer_ms
        self.device_token = os.environ.get("DEVICE_TOKEN", "")
        self.tail_device = os.environ.get("TAIL_DEVICE", "").strip()
        self.diagnostics_port = int(os.environ.get("DIAGNOSTICS_PORT", "8081"))
        self.registry = SatelliteRegistry()
        await self.registry.load()
        self.announcement_model = announcement_model
        self.announcement_tts_model = announcement_tts_model
        self.announcement_chime = os.environ.get("ANNOUNCEMENT_CHIME", "true").lower() == "true"
        self.openai_api_key = openai_api_key
        self.voice = openai_voice
        self.personality = personality
        self.announcements = AnnouncementManager(self, self.registry)
        self.mqtt = MQTTBridge(self, self.registry, self.announcements)
        self.router = SatelliteRouter(self, self.registry, websocket_host, websocket_port,
                                      self.device_token)
        self.diagnostics = Diagnostics(self.registry, self.router, self.device_token,
                                       self.diagnostics_port)
        logger.info(
            f"🔁 Follow-up window: {follow_up_listen_seconds}s "
            f"({'enabled' if follow_up_ms > 0 else 'disabled — turn-based'}), "
            f"mic-open delay {follow_up_open_delay_ms}ms, "
            f"wake-open delay {wake_open_delay_ms}ms, "
            f"playback prebuffer {playback_prebuffer_ms}ms"
        )
        
        # Store configuration for session creation
        self.openai_api_key = openai_api_key
        self.vad_threshold = vad_threshold
        self.vad_prefix_padding_ms = vad_prefix_padding_ms
        self.vad_silence_duration_ms = vad_silence_duration_ms
        self.turn_detection_type = turn_detection_type
        self.vad_eagerness = vad_eagerness
        self.interrupt_response = interrupt_response
        self.semantic_vad_create_response = semantic_vad_create_response
        self.enable_disconnect_tool = enable_disconnect_tool
        self.transcription_language = transcription_language
        self.transcription_model = transcription_model
        self.instructions = instructions
        self.personality = personality
        self.model = openai_model
        self.voice = openai_voice
        self.openai_speed = openai_speed
        self.max_output_tokens = max_output_tokens
        self.noise_reduction = noise_reduction
        self.mcp_tool_allowlist = mcp_tool_allowlist
        self.mcp_client = mcp_client
        self.mcp_tools_schema = None
        if mcp_client:
            try:
                self.mcp_tools_schema = await mcp_client.get_tools_schema()
            except Exception:
                logger.exception("Could not fetch MCP schema")
        self.enable_web_search = enable_web_search
        self.enable_timers = enable_timers
        self.web_search_model = web_search_model

        logger.info("✅ Application initialized - ready to accept WebSocket connections")
    
    def make_recorder(self, mac):
        recorder = AudioRecordingService(
            enable_recording=self.enable_recording, sample_rate=24000,
            chunk_duration_seconds=30, output_dir=recordings_dir()
        )
        recorder.start_new_session(mac.replace(":", "-"))
        return recorder

    async def create_openai_service(self, session):
        """Create a new OpenAI service instance for a client.
        
        Args:
            client_id: Optional client ID for session management
        """
        client_id = session.mac
        
        # Create new session
        if client_id:
            logger.info(f"🆕 Creating new OpenAI Session for Client {client_id}...")
        else:
            logger.info("🆕 Creating new OpenAI Session...")

        # Create session properties with audio configuration
        from pipecat.services.openai.realtime.events import (
            SessionProperties,
            AudioConfiguration,
            AudioInput,
            AudioOutput,
            TurnDetection,
            SemanticTurnDetection,
            InputAudioTranscription,
            InputAudioNoiseReduction,
        )

        # Collect all tool definitions for session properties. The
        # disconnect_client tool is opt-in (see enable_disconnect_tool): by
        # default we do NOT expose it, so the model can't hang up the device
        # mid-conversation.
        all_tools = []
        clock_tool = None
        if self.enable_disconnect_tool:
            all_tools.append(get_disconnect_tool_definition())

        # Web search tool (optional). Lets the model look things up online via
        # a secondary OpenAI Responses web_search call in the handler.
        if self.enable_web_search:
            all_tools.append(get_web_search_tool_definition())
        if self.enable_timers:
            all_tools.extend(get_timer_tool_definitions())

        # Get MCP tool definitions if available
        mcp_tools_schema = self.mcp_tools_schema
        if mcp_tools_schema:
            try:
                # Convert MCP tool schemas to OpenAI format, applying the
                # optional allow-list so the realtime session isn't flooded
                # with ha-mcp's 80+ tools.
                exposed = 0
                for function_schema in mcp_tools_schema.standard_tools:
                    if self.mcp_tool_allowlist and function_schema.name not in self.mcp_tool_allowlist:
                        continue
                    openai_tool = {
                        "type": "function",
                        "name": function_schema.name,
                        "description": function_schema.description,
                        "parameters": {
                            "type": "object",
                            "properties": function_schema.properties,
                            "required": function_schema.required
                        }
                    }
                    all_tools.append(openai_tool)
                    if (
                        clock_tool is None
                        and (function_schema.name == "GetDateTime"
                             or function_schema.name.endswith("GetDateTime"))
                    ):
                        clock_tool = function_schema.name
                    exposed += 1

                if self.mcp_tool_allowlist:
                    logger.info(f"✅ Fetched {len(mcp_tools_schema.standard_tools)} MCP tools, exposing {exposed} per allow-list")
                else:
                    logger.info(f"✅ Fetched {len(mcp_tools_schema.standard_tools)} MCP tools")
            except Exception as e:
                logger.warning(f"⚠️ Failed to fetch MCP tool definitions: {e}")

        if clock_tool is None:
            clock_tool = "get_current_time"
            all_tools.append(get_time_tool_definition())

        # Turn detection: semantic_vad (recommended — semantic end-of-turn,
        # echo-resistant, doesn't cut the user off) or classic server_vad.
        if self.turn_detection_type == "semantic_vad":
            turn_detection = SemanticTurnDetection(
                eagerness=self.vad_eagerness,
                # create_response=True (default): the SERVER creates a
                # response on every detected end-of-turn. This is required for
                # multi-turn conversation. Pipecat 0.0.97's
                # OpenAIRealtimeLLMService._handle_context only auto-creates a
                # response for the FIRST context (turn 1) and after tool
                # results (its else-branch just updates the context); a plain
                # 2nd/3rd user turn therefore gets NO response unless the
                # server makes it. We previously set this False to stop a
                # turn-1 double-response (server + Pipecat first-context both
                # creating → `conversation_already_has_active_response`), but
                # that silently broke every turn after the first (device hung
                # in "thinking"). True is the correct trade: the server drives
                # all user-turn responses; Pipecat still creates the post-tool
                # response via _process_completed_function_calls. To stop the
                # turn-1 double (server + Pipecat-first-context both creating →
                # conversation_already_has_active_response), run() seeds
                # self._context once at startup with a kickoff LLMRunFrame, so
                # the user's first real turn hits the else-branch too.
                create_response=self.semantic_vad_create_response,
                interrupt_response=self.interrupt_response,
            )
        else:
            turn_detection = TurnDetection(
                type="server_vad",
                threshold=self.vad_threshold,
                prefix_padding_ms=self.vad_prefix_padding_ms,
                silence_duration_ms=self.vad_silence_duration_ms,
            )

        # Restore needs user transcripts even when no language is pinned;
        # otherwise only assistant turns survive a reconnect.
        transcription = (
            InputAudioTranscription(
                model=self.transcription_model,
                language=self.transcription_language or None,
            )
        )

        # Optional near/far-field input noise reduction (helps the VAD reject
        # background noise / residual speaker leak). None = off (default).
        noise_reduction = (
            InputAudioNoiseReduction(type=self.noise_reduction)
            if self.noise_reduction
            else None
        )

        session_properties = SessionProperties(
            instructions=build_prompt(
                self.instructions, self.personality, self.home_location,
                self.time_zone, self.units, clock_tool, area=None
            ),
            # Cap the reply length: bounds runaway monologues + per-response
            # output-token cost. None = unlimited (the API default "inf").
            max_output_tokens=self.max_output_tokens,
            audio=AudioConfiguration(
                input=AudioInput(
                    turn_detection=turn_detection,
                    transcription=transcription,
                    noise_reduction=noise_reduction,
                ),
                # speed is a post-generation playback rate (0.25-1.5, 1.0 = normal).
                output=AudioOutput(voice=self.voice, speed=self.openai_speed)
            ),
            tools=all_tools
        )

        if self.turn_detection_type == "semantic_vad":
            logger.info(
                f"🎚️ Turn detection: semantic_vad (eagerness={self.vad_eagerness}, "
                f"create_response={self.semantic_vad_create_response}, "
                f"interrupt_response={self.interrupt_response})"
                + (f", transcription={self.transcription_model} (lang={self.transcription_language or 'auto'})" if transcription else " (transcription off)")
            )
        else:
            logger.info(
                f"🎚️ Turn detection: server_vad (threshold={self.vad_threshold}, "
                f"silence_duration_ms={self.vad_silence_duration_ms})"
                + (f", transcription={self.transcription_model} (lang={self.transcription_language or 'auto'})" if transcription else " (transcription off)")
            )

        logger.info(f"🔧 Creating session with {len(all_tools)} tools: {[tool.get('name', 'unknown') for tool in all_tools]}")

        # Create new service instance
        service = SafeRealtimeLLMService(
            api_key=self.openai_api_key,
            model=self.model,
            session_properties=session_properties,
            start_audio_paused=False
        )
        service.turn_liveness = session.liveness
        service.announcement_active = False
        session.clock_tool = clock_tool
        logger.info(f"✅ OpenAI Service created: {type(service).__name__}")
        if clock_tool == "get_current_time":
            service.register_function(
                clock_tool, create_time_tool_handler(self.time_zone, self.home_location)
            )

        # Register disconnect tool handler (only when the tool is exposed)
        if self.enable_disconnect_tool:
            disconnect_tool_handler = create_disconnect_tool_handler(session.transport)
            service.register_function("disconnect_client", disconnect_tool_handler)
            logger.info("✅ Registered disconnect tool handler")

        # Register web search tool handler (only when the tool is exposed)
        if self.enable_web_search:
            service.register_function(
                "web_search",
                create_web_search_tool_handler(
                    self.openai_api_key, self.web_search_model, self.home_location
                ),
            )
            logger.info(f"✅ Registered web_search tool handler (model={self.web_search_model})")

        if self.enable_timers:
            for definition in get_timer_tool_definitions():
                name = definition["name"]
                service.register_function(
                    name, create_timer_tool_handler(session.handler.timer_bridge, name)
                )
            logger.info("✅ Registered device timer tool handlers")
        
        # Register MCP tool handlers if available
        if self.mcp_client and mcp_tools_schema:
            try:
                await self.mcp_client.register_tools_schema(mcp_tools_schema, service)
                logger.info(f"✅ Registered {len(mcp_tools_schema.standard_tools)} MCP tool handlers")
            except Exception as e:
                logger.warning(f"⚠️ Failed to register MCP tool handlers: {e}")
        
        # Register service with session manager
        if client_id:
            self.session_manager.set_current_service(client_id, service)
        
        logger.info("✅ New OpenAI Session created")
        return service

    async def run(self) -> None:
        """Run the application."""
        await self.initialize()
        install_logging()
        done = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(signum, done.set)
            except NotImplementedError:
                pass
        try:
            await self.router.start()
            await self.mqtt.start()
            await self.diagnostics.start()
            logger.info("Satellite router listening on %s:%d", self.router.host, self.router.port)
            await done.wait()
        finally:
            await self.cleanup()
    
    async def cleanup(self) -> None:
        """Cleanup resources."""
        logger.info("Cleaning up application...")
        
        await self.router.close()
        await self.mqtt.close()
        await self.announcements.close()
        await self.diagnostics.stop()
        logger.info("✅ Application cleanup complete")


async def main() -> None:
    """Main entry point."""
    app = Application()
    
    try:
        await app.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

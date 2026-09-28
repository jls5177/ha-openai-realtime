"""WebSocket handler for managing WebSocket connections and pipelines."""
import asyncio
import json
import logging
import time
from typing import Optional, Callable

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask, PipelineParams
from pipecat.services.openai.realtime.llm import OpenAIRealtimeLLMService

from pipecat.processors.frame_processor import FrameProcessor, FrameDirection
from pipecat.frames.frames import Frame, InputAudioRawFrame, OutputAudioRawFrame, StartFrame, EndFrame, ErrorFrame, MetricsFrame
from pipecat.metrics.metrics import LLMUsageMetricsData
from pipecat.audio.utils import create_stream_resampler
from pipecat.services.openai.realtime import events as openai_rt_events

from app.raw_audio_serializer import RawAudioSerializer
from app.session_manager import SessionManager
from app.audio_recording_service import AudioRecordingService
from app.phase_emitter import PhaseEmitter
from app.transcript_logger import TranscriptLogger
from app.timer_tool import TimerBridge
from app.home_context import lookup_device_area
from app.prompt_builder import build_prompt
from app.session_instructions import apply_instructions, _update_system_context

logger = logging.getLogger(__name__)

# The OpenAI Realtime API works in 24 kHz PCM16. The Voice PE firmware plays
# 24 kHz back and streams 16 kHz up. IMPORTANT: pipecat 0.0.97's websocket INPUT
# transport does NOT resample (only the OUTPUT transport does), and OpenAI
# Realtime's pcm16 input rate is hard-locked to 24000 (PCMAudioFormat.rate =
# Literal[24000]) — you cannot tell it the audio is 16 kHz. So the device's
# 16 kHz frames would be read 1.5x too fast / pitched up, garbling the whole
# transcript. The InputResampler below upsamples 16k->24k in the pipeline.
PIPELINE_SAMPLE_RATE = 24000


class SessionActivityTracker(FrameProcessor):
    """Processor that tracks session activity by monitoring audio frames."""
    
    def __init__(self, activity_callback, **kwargs):
        super().__init__(**kwargs)
        self.activity_callback = activity_callback
    
    async def process_frame(self, frame: Frame, direction: FrameDirection):
        if isinstance(frame, StartFrame):
            logger.debug("🎬 SessionActivityTracker: Received StartFrame")
            await super().process_frame(frame, direction)
            await self.push_frame(frame, direction)
            return
        elif isinstance(frame, EndFrame):
            logger.debug("🏁 SessionActivityTracker: Received EndFrame")
            await self.push_frame(frame, direction)
            return
        
        # Track activity on any audio frame
        if isinstance(frame, (InputAudioRawFrame, OutputAudioRawFrame)):
            if self.activity_callback:
                self.activity_callback()
            logger.debug(f"🎵 SessionActivityTracker: Processing {type(frame).__name__} ({len(frame.audio)} bytes)")
        
        # Pass frame through to next processor
        await self.push_frame(frame, direction)


class SessionMetrics(FrameProcessor):
    def __init__(self, session):
        super().__init__()
        self.session = session

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if isinstance(frame, MetricsFrame):
            for item in frame.data:
                if isinstance(item, LLMUsageMetricsData):
                    for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
                        self.session.counters[key] += getattr(item.value, key, 0) or 0
        await self.push_frame(frame, direction)


class InputResampler(FrameProcessor):
    """Upsample incoming device mic audio to the OpenAI Realtime input rate.

    The Voice PE streams 16 kHz PCM16. pipecat 0.0.97's websocket input transport
    forwards those frames unchanged, and OpenAI Realtime reads pcm16 input at a
    fixed 24 kHz — so without this the audio is interpreted ~1.5x too fast,
    badly degrading transcription (e.g. first word dropped, words mangled). This
    sits right after transport.input() and resamples each InputAudioRawFrame to
    out_rate. Uses a streaming resampler so there are no per-chunk edge artifacts.
    """

    def __init__(self, out_rate: int = PIPELINE_SAMPLE_RATE, **kwargs):
        super().__init__(**kwargs)
        self._out_rate = out_rate
        self._resampler = create_stream_resampler()
        self._logged = False

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InputAudioRawFrame) and frame.sample_rate != self._out_rate:
            if not frame.audio:
                return  # nothing to resample / forward; don't emit empty audio
            try:
                resampled = await self._resampler.resample(
                    frame.audio, frame.sample_rate, self._out_rate
                )
            except Exception as e:
                logger.warning(f"⚠️ input resample {frame.sample_rate}->{self._out_rate} failed: {e!r}")
                return  # drop rather than forward wrong-rate audio
            # The streaming resampler buffers internally and can return empty
            # bytes while priming or on a tiny chunk. OpenAI rejects an
            # input_audio_buffer.append with empty audio ("got empty bytes"), so
            # drop those frames — the samples stay buffered and come out next call.
            if not resampled:
                return
            if not self._logged:
                logger.info(
                    f"🎙️ Resampling device input {frame.sample_rate}Hz -> {self._out_rate}Hz for OpenAI"
                )
                self._logged = True
            frame = InputAudioRawFrame(
                audio=resampled,
                sample_rate=self._out_rate,
                num_channels=frame.num_channels,
            )
        await self.push_frame(frame, direction)


class ConnectionRecovery(FrameProcessor):
    """Auto-reconnect the OpenAI Realtime session when its WebSocket dies.

    pipecat 0.0.97's OpenAIRealtimeLLMService has NO reconnect logic: when the
    OpenAI WS drops (1011 keepalive ping timeout, 1001 going away on the 60-min
    cap, 1006, or any send/receive failure) it treats the send error as fatal and
    floods ErrorFrame — ~15/s, one per forwarded mic frame — forever. The single
    persistent session is then dead until the add-on restarts, so the device gets
    no answer to any further turn (observed live: a 1011 flood after which the
    next question got silence).

    This processor watches the ErrorFrames as they travel upstream to the task
    source, and on the first connection-death signature it:
      1. emits `idle` to the device so it unsticks (LED + mic reset), and
      2. calls service.reset_conversation() — the one PUBLIC method that does
         _disconnect() + _connect() + re-sends the session config (instructions,
         tools, turn detection) — to bring the session back IN PLACE. No pipeline
         rebuild: the running pipeline keeps the same service object, which is
         exactly the one reset_conversation reconnects.
    A guard + cooldown collapse the error flood into a single reconnect attempt,
    retrying at most every RECONNECT_COOLDOWN_S while the link stays down.
    """

    # Substrings that mark a dead/closed OpenAI websocket (vs an app-level error
    # like a tool failure, which we must NOT reconnect on). These appear on the
    # SEND-side flood ("Error sending client event: …"), so they're paired with
    # the "client event" check below to avoid reacting to a device disconnect.
    _DEATH_MARKERS = (
        "keepalive ping timeout",
        "going away",
        "no close frame",
        "ConnectionClosed",
        "connection is closed",
        "sent 1011",
        "sent 1001",
        "1006",
    )
    # Substrings that UNAMBIGUOUSLY mean OUR OpenAI session is gone and must be
    # reconnected, regardless of how the error surfaced. The 60-minute cap can
    # arrive as a proactive OpenAI *error event* (code='session_expired', "Your
    # session hit the maximum duration of 60 minutes.") with NO "client event"
    # send-flood and NO close-code marker — so the paired check above misses it
    # and the session stays dead until the add-on restarts. These markers force a
    # reconnect on their own. They can only come from OpenAI (not a device close),
    # so no "client event" guard is needed.
    _SESSION_DEAD_MARKERS = (
        "session_expired",
        "maximum duration",
    )
    RECONNECT_COOLDOWN_S = 5.0
    IDLE_UNSTICK_COOLDOWN_S = 2.0
    # Proactive refresh: reconnect BEFORE OpenAI's 60-min session cap, but only
    # while the house is genuinely quiet, so the cap practically never lands
    # mid-conversation (where it costs the user a turn).
    REFRESH_AGE_S = 55 * 60   # refresh once the session is this old
    REFRESH_QUIET_S = 60.0    # ... and no mic audio flowed for this long
    REFRESH_CHECK_S = 60.0    # poll cadence of the background check

    def __init__(self, openai_service, emit_idle=None, phase_emitter=None,
                 on_reconnect=None, on_error=None, **kwargs):
        super().__init__(**kwargs)
        self._service = openai_service
        self._emit_idle = emit_idle  # async callable(value:str), e.g. broadcast_phase
        # Preferred idle route: PhaseEmitter.force_idle() keeps the emitter's
        # phase state consistent AND suppresses the racing `thinking` from VAD
        # stop events still in flight (observed: a raw broadcast idle was
        # overridden 400 ms later and the device sat in `thinking` with an
        # open mic for 44 s). emit_idle stays as fallback wiring.
        self._phase_emitter = phase_emitter
        self._on_reconnect = on_reconnect
        self._on_error = on_error
        self._reconnecting = False
        self._last_attempt = 0.0
        self._last_idle_unstick = 0.0
        # Diagnostics: when the current OpenAI session connected, so we can log its
        # age at a drop (the 60-min cap shows up as ~3600 s) and the reconnect
        # duration (the brief gap the user hears).
        self._connected_at = time.monotonic()
        # Proactive-refresh state. This processor sits right behind
        # transport.input(), so every mic frame passes through it — the cheapest
        # possible "is anyone interacting?" signal (the device only streams the
        # mic during an active turn or the follow-up window).
        self._last_input_audio = time.monotonic()
        self._refresh_task = None
        self._tasks = set()

    def _spawn(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def shutdown(self):
        for task in list(self._tasks):
            task.cancel()
        if self._refresh_task:
            self._refresh_task.cancel()
        await asyncio.gather(
            *(task for task in (*self._tasks, self._refresh_task) if task is not None),
            return_exceptions=True,
        )

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if self._refresh_task is None:
            self._refresh_task = self._spawn(self._proactive_refresh_loop())
        if isinstance(frame, InputAudioRawFrame):
            # Only kept for the proactive-refresh "is anyone interacting?" check.
            # (Stale-audio clearing is now done at the cut-off source — the device
            # sends {"type":"flush"} when a follow-up window times out — not
            # reactively on mic-resume, which disturbed the VAD and caused garbage.)
            self._last_input_audio = time.monotonic()
        if isinstance(frame, ErrorFrame) and not self._reconnecting:
            if self._on_error:
                self._on_error()
            msg = str(getattr(frame, "error", "") or "")
            # Two reconnect triggers:
            #  (a) the OpenAI send-side flood ("Error sending client event: …" +
            #      a close-code marker) — OUR WS died mid-send. We require the
            #      "client event" signature so a normal DEVICE-side disconnect
            #      (also 1011/ConnectionClosed, but the device went away) does NOT
            #      trigger an OpenAI reconnect.
            #  (b) an unambiguous OpenAI session-dead error event (session_expired
            #      / "maximum duration") — this is the 60-min cap surfacing as a
            #      proactive error event with NO send-flood, so (a) misses it.
            #      It can only come from OpenAI, so it needs no "client event" guard.
            send_flood = "client event" in msg and any(m in msg for m in self._DEATH_MARKERS)
            session_dead = any(m in msg for m in self._SESSION_DEAD_MARKERS)
            # (c) the OpenAI READ side died or ended (network drop / silent
            #     server close). pipecat produces no ErrorFrame for these at
            #     all — SafeRealtimeLLMService wraps the receive loop and
            #     reports them with this message. Without it the session sat
            #     deaf for hours until the next utterance hit the dead socket.
            reader_dead = "realtime receive loop" in msg
            if send_flood or session_dead or reader_dead:
                now = time.monotonic()
                if now - self._last_attempt >= self.RECONNECT_COOLDOWN_S:
                    self._reconnecting = True
                    self._last_attempt = now
                    self._spawn(self._recover(msg))
            else:
                # Non-connection-death error that ENDS a turn without a reply:
                # most importantly an OpenAI rate-limit ("Rate limit reached …"),
                # but also any other transient response.create failure. No bot
                # speech was produced, so PhaseEmitter never fires
                # BotStopped→idle; the device is left stuck in `thinking`
                # (LED keeps blinking) with no device-side watchdog to recover.
                # Emit one `idle` to unstick it so the user can just try again.
                # Guarded by a short cooldown so a rare flood collapses to one.
                now = time.monotonic()
                if now - self._last_idle_unstick >= self.IDLE_UNSTICK_COOLDOWN_S:
                    self._last_idle_unstick = now
                    self._spawn(self._unstick_idle(msg))
        await self.push_frame(frame, direction)

    async def _recover(self, reason: str):
        t0 = time.monotonic()
        age_s = t0 - self._connected_at
        try:
            logger.warning(
                f"🔌 OpenAI Realtime connection lost after {age_s:.0f}s "
                f"({reason[:90]}) — reconnecting…"
            )
            # Unstick the device first, regardless of how the reconnect goes.
            try:
                await self._go_idle(f"reconnect: {reason[:60]}")
            except Exception as e:
                logger.warning(f"⚠️ could not emit idle during recovery: {e!r}")
            reset = getattr(self._service, "reset_conversation", None)
            if reset is None:
                logger.error("❌ service has no reset_conversation(); cannot reconnect in place")
                return
            await reset()
            if self._on_reconnect:
                self._on_reconnect()
            self._connected_at = time.monotonic()
            logger.info(
                f"✅ OpenAI Realtime session reconnected in {self._connected_at - t0:.1f}s "
                f"(gap the user may have heard)"
            )
        except Exception as e:
            logger.error(f"❌ OpenAI reconnect attempt failed: {e!r}")
        finally:
            self._reconnecting = False

    async def _proactive_refresh_loop(self):
        """Refresh the OpenAI session BEFORE the 60-min cap, during real idle.

        The cap reconnect is recoverable (~3 s), but when it lands
        mid-conversation that turn hiccups. Refreshing proactively while
        nothing is happening means users practically never meet the cap.
        "Quiet" is double-checked: no assistant response in flight AND no mic
        audio for REFRESH_QUIET_S — so it can never fire during a turn, a
        reply, or an open follow-up window.
        """
        while True:
            try:
                await asyncio.sleep(self.REFRESH_CHECK_S)
                if self._reconnecting:
                    continue
                now = time.monotonic()
                age = now - self._connected_at
                quiet = now - self._last_input_audio
                busy = getattr(self._service, "_current_assistant_response", None) is not None
                if (age >= self.REFRESH_AGE_S and quiet >= self.REFRESH_QUIET_S
                        and not getattr(self._service, "announcement_active", False)
                        and not busy and now - self._last_attempt >= self.RECONNECT_COOLDOWN_S):
                    self._reconnecting = True
                    self._last_attempt = now
                    logger.info(
                        f"🔄 proactive session refresh (session {age/60:.0f} min old, "
                        f"quiet for {quiet:.0f}s) — staying ahead of the 60-min cap"
                    )
                    await self._recover("proactive refresh before the 60-min session cap")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"⚠️ proactive refresh loop error: {e!r}")

    async def _go_idle(self, reason: str) -> None:
        """Put the device in idle for a dead turn — via PhaseEmitter when wired."""
        if self._phase_emitter is not None:
            await self._phase_emitter.force_idle(reason)
        elif self._emit_idle is not None:
            await self._emit_idle("idle")

    async def _unstick_idle(self, reason: str):
        """Emit `idle` to the device after a turn-ending error (e.g. rate limit).

        The session is still alive (no reconnect needed) — we just nudge the
        device out of its stuck `thinking` blink so the user can retry.
        """
        try:
            logger.warning(f"⚠️ turn ended on error, emitting idle to unstick device ({reason[:90]})")
            await self._go_idle(f"turn ended on error: {reason[:60]}")
        except Exception as e:
            logger.warning(f"⚠️ could not emit idle after turn-ending error: {e!r}")


class WebSocketHandler:
    """Handles WebSocket transport initialization, pipeline building, and event management."""
    
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        session_manager: Optional[SessionManager] = None,
        audio_recording_service: Optional[AudioRecordingService] = None,
        interrupt_response: bool = False,
        follow_up_ms: int = 0,
        follow_up_open_delay_ms: int = 700,
        wake_open_delay_ms: int = 700,
        playback_prebuffer_ms: int = 0,
        session=None,
    ):
        """
        Initialize WebSocket handler.

        Args:
            host: Host address to bind to
            port: Port to listen on
            session_manager: Session manager instance
            audio_recording_service: Audio recording service instance
            interrupt_response: Whether hands-free barge-in is enabled
            follow_up_ms: How long (ms) the device should keep the mic open
                after a reply so the user can answer without a wake word. Sent to
                the device in the `hello` handshake. 0 = turn-based (no window).
            follow_up_open_delay_ms: How long (ms) the device waits after a reply
                finishes before opening that follow-up mic (bridges the speaker
                hardware tail). Sent in the `hello` handshake.
            wake_open_delay_ms: How long (ms) the device waits after the wake
                chime before opening the mic, so the chime's hardware tail can't
                leak into the fresh mic as a ghost turn. Sent in `hello`.
        """
        self.host = host
        self.port = port
        self.session_manager = session_manager
        self.audio_recording_service = audio_recording_service
        self.interrupt_response = interrupt_response
        self.follow_up_ms = max(0, int(follow_up_ms))
        self.follow_up_open_delay_ms = max(0, int(follow_up_open_delay_ms))
        self.wake_open_delay_ms = max(0, int(wake_open_delay_ms))
        self.playback_prebuffer_ms = max(0, int(playback_prebuffer_ms))
        self.session = session

        self.transport = None
        self.pipeline: Optional[Pipeline] = None
        self.runner: Optional[PipelineRunner] = None
        self.current_task: Optional[PipelineTask] = None
        # The serializer instance the transport reads through. Kept so
        # build_pipeline can wire its device-interrupt callback to the OpenAI
        # service.
        self._serializer: Optional[RawAudioSerializer] = None
        self._phase_emitter: Optional[PhaseEmitter] = None
        # Connected device websockets, used to push va_client control/phase
        # messages as TEXT frames (the audio path uses the binary serializer).
        self._websockets: set = set()
        self.timer_bridge = TimerBridge(self)
        self._instruction_context: dict | None = None
        self._area_task: asyncio.Task | None = None
        self._area_generation = 0
        self._context_aggregator = None
        if session is not None:
            self._serializer = session.serializer
            self._websockets.add(session.websocket)
            self.timer_bridge.connected(session.websocket)

    def configure_instructions(self, *, instructions: str, personality: str,
                               home_location: str, time_zone: str | None,
                               units: str | None, clock_tool: str,
                               ha_base: str | None, ha_token: str | None) -> None:
        self._instruction_context = {
            "instructions": instructions,
            "personality": personality,
            "home_location": home_location,
            "time_zone": time_zone,
            "units": units,
            "clock_tool": clock_tool,
        }
        self._ha_base = ha_base
        self._ha_token = ha_token

    def start_area_lookup(self, openai_service, start: dict) -> None:
        """A fresh connection first loses any previous room, then resolves its own."""
        if self._instruction_context is None:
            return
        self._area_generation += 1
        if self._area_task is not None:
            self._area_task.cancel()
        self._area_task = asyncio.create_task(
            self._resolve_area(openai_service, start, self._area_generation)
        )

    async def _resolve_area(self, openai_service, start: dict, generation: int) -> None:
        context = self._context_aggregator.user().context if self._context_aggregator else None
        try:
            await apply_instructions(
                openai_service, build_prompt(**self._instruction_context, area=None), context
            )
        except Exception:
            logger.exception("Could not clear previous satellite area from the live session")
            return
        mac = start.get("mac")
        area = None
        if mac and self._ha_base and self._ha_token:
            try:
                area = await asyncio.wait_for(
                    lookup_device_area(self._ha_base, self._ha_token, mac), timeout=3
                )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Satellite area lookup failed: %s", e)
        if generation != self._area_generation:
            return
        logger.info("Satellite area: %s", area or "unknown")
        if self.session is not None:
            self.session.metadata.area = area
            self.session.registry.notify("updated", self.session.metadata)
        if area:
            try:
                await apply_instructions(
                    openai_service, build_prompt(**self._instruction_context, area=area), context
                )
            except Exception:
                logger.exception("Could not apply satellite area to the live session")
    
    def build_pipeline(
        self,
        transport,
        openai_service: OpenAIRealtimeLLMService,
        client_id: str,
        activity_callback: Optional[Callable[[], None]] = None
    ) -> tuple[Pipeline, PipelineRunner, PipelineTask]:
        """
        Build pipeline for a WebSocket transport connection.
        
        Args:
            transport: The WebSocket transport instance
            openai_service: The OpenAI service instance
            client_id: Unique identifier for the client device
            activity_callback: Optional callback for session activity tracking
            
        Returns:
            Tuple of (Pipeline, PipelineRunner, PipelineTask)
        """
        logger.info(f"🔗 Building pipeline for client: {client_id}")
        
        if openai_service is None:
            raise RuntimeError("OpenAI service must be created before building pipeline")
        
        logger.info(f"🔗 Building pipeline with WebSocket transport and OpenAI service: {type(openai_service).__name__}")
        
        # Create activity trackers
        input_activity_tracker = SessionActivityTracker(
            activity_callback=activity_callback or (lambda: None)
        )
        output_activity_tracker = SessionActivityTracker(
            activity_callback=activity_callback or (lambda: None)
        )
        
        # Create context aggregator with cached context if available
        context_aggregator = None
        context_initializer = None
        if self.session_manager:
            context_aggregator = self.session_manager.create_context_aggregator(client_id)
            context_initializer = self.session_manager.create_context_initializer(client_id, context_aggregator)
            _update_system_context(
                context_aggregator.user().context, openai_service._session_properties.instructions
            )
        self._context_aggregator = context_aggregator
        
        # Build pipeline components. InputResampler runs FIRST (right after the
        # transport) so every later stage — VAD, context aggregator, OpenAI
        # service — sees correctly-rated 24 kHz audio instead of the device's
        # raw 16 kHz (which OpenAI would otherwise read 1.5x too fast).
        # Built early so ConnectionRecovery can route its unstick/reconnect
        # idle through PhaseEmitter.force_idle() (consistent phase state +
        # racing-`thinking` suppression); it is APPENDED near the end of the
        # pipeline below, before transport.output().
        phase_emitter = PhaseEmitter(
            send_phase=self.broadcast_phase,
            send_audio_done=self.broadcast_audio_done,
            interrupt_response=self.interrupt_response,
            liveness=self.session.liveness if self.session else None,
            on_phase=self.session.on_phase if self.session else None,
        )
        self._phase_emitter = phase_emitter
        if self.session:
            self.session.phase_emitter = phase_emitter

        recovery = ConnectionRecovery(
            openai_service=openai_service, emit_idle=self.broadcast_phase,
            phase_emitter=phase_emitter,
            on_reconnect=(lambda: self.session.increment("openai_reconnects"))
            if self.session else None,
            on_error=(lambda: self.session.increment("errors")) if self.session else None,
        )
        self.recovery = recovery
        pipeline_components = [
            transport.input(),
            # Watch for OpenAI connection-death ErrorFrames (they travel upstream
            # to the task source, so place this upstream of the service) and
            # reconnect in place. Without it a 1011/1001 drop bricks the session.
            recovery,
            InputResampler(out_rate=PIPELINE_SAMPLE_RATE),
            input_activity_tracker,
        ]
        
        # Add input audio recorder to capture ONLY InputAudioRawFrame
        input_recorder = self.audio_recording_service.get_input_recorder() if self.audio_recording_service else None
        if input_recorder:
            pipeline_components.append(input_recorder)
        
        # Continue with rest of pipeline, with transcript-logging taps. The
        # assistant reply text (TTSTextFrame) flows DOWNSTREAM out of the LLM
        # while the user's TranscriptionFrame is pushed UPSTREAM (so the user
        # aggregator can consume it) — opposite directions, so they need taps on
        # opposite sides of the service (see transcript_logger.py): "user" before
        # the LLM, "assistant" after it.
        if context_aggregator:
            pipeline_components.extend([
                context_aggregator.user(),
                TranscriptLogger(capture="user"),
                openai_service,
                TranscriptLogger(capture="assistant"),
                context_aggregator.assistant(),
            ])
        else:
            pipeline_components.extend([
                TranscriptLogger(capture="user"),
                openai_service,
                TranscriptLogger(capture="assistant"),
            ])

        pipeline_components.append(output_activity_tracker)

        # Emit va_client phase messages (listening/thinking/replying/idle) to
        # the device, derived from Pipecat speaking frames as they pass
        # downstream. Placed before transport.output() so it sees both the
        # user (UserStarted/Stopped) and bot (BotStarted/Stopped) frames.
        # (Constructed above, before ConnectionRecovery.)
        pipeline_components.append(phase_emitter)
        if self.session:
            pipeline_components.append(SessionMetrics(self.session))

        # Add output audio recorder to capture ONLY OutputAudioRawFrame
        output_recorder = self.audio_recording_service.get_output_recorder() if self.audio_recording_service else None
        if output_recorder:
            pipeline_components.append(output_recorder)

        pipeline_components.append(transport.output())
        
        # Add context initializer if we have cached messages
        if context_initializer:
            pipeline_components.append(context_initializer)
        
        pipeline = Pipeline(pipeline_components)
        logger.info("✅ Pipeline created for WebSocket connection")
        
        # Audio recording is handled by AudioFrameRecorder processors in the pipeline
        if self.audio_recording_service:
            logger.info("🎙️ Audio recording enabled - will record input and output audio")
        
        # Create pipeline runner and task
        # Disable idle timeout - server should always stay ready for connections
        runner = PipelineRunner(handle_sigint=False)
        task = PipelineTask(
            pipeline, idle_timeout_secs=None, cancel_on_idle_timeout=False,
            params=PipelineParams(enable_metrics=True, enable_usage_metrics=True,
                                  audio_in_sample_rate=PIPELINE_SAMPLE_RATE,
                                  audio_out_sample_rate=PIPELINE_SAMPLE_RATE),
            observers=self.session.observers if self.session else None,
        )
        logger.info("✅ Pipeline initialized successfully")

        # Wire the device "stop" interrupt. The serializer calls this when it
        # sees {"type":"interrupt"} from the device.
        #
        # The DEVICE stops playback AUTHORITATIVELY: on "stop" its firmware
        # flushes the PSRAM queue and drops all further incoming TTS
        # (suppress_incoming_audio_) until the next turn boundary. So the backend
        # does NOT need to clear its own output here — the user already hears
        # silence. The backend's only job is to stop OpenAI generating MORE
        # tokens: a plain response.cancel, and ONLY while a response is actually
        # active (avoids the noisy response_cancel_not_active in the common
        # already-burst-finished case).
        #
        # We deliberately do NOT queue an InterruptionTaskFrame anymore. It made
        # pipecat run _handle_interruption → _truncate_current_audio_response(),
        # which tells OpenAI to truncate the assistant audio at the *playback*
        # position. But OpenAI bursts the reply faster than real-time, so that
        # position overshoots the audio that actually exists and OpenAI rejects
        # the truncate with invalid_request_error ("Audio content of N ms is
        # already shorter than M ms"). That error left the realtime session in a
        # broken state where the user's VERY NEXT turn got NO response — the
        # recurring "say stop, then immediately ask again → silence" bug. Since
        # the device already silenced playback, dropping the truncate costs us
        # nothing and keeps the next turn alive. (The backend still drains its
        # already-buffered output to the device, which the device discards —
        # minor wasted bandwidth, tracked as roadmap #3; no extra tokens because
        # response.cancel stops further generation.)
        # FOLLOW-UP-WINDOW STOP (the "stop heard as a question" bug). During the
        # post-reply follow-up window the device mic is OPEN and streaming, so by
        # the time the device's local wake-word detects "stop" and sends us the
        # interrupt, the stop word's audio is ALREADY in OpenAI's input buffer.
        # Left alone, the server VAD commits it as a user turn and — with
        # create_response=true — the model literally ANSWERS the word "stop"
        # ("Ik hou me stil…"). The device's local detection must therefore be
        # authoritative on the cloud side too, in two layers:
        #   1) input_audio_buffer.clear discards the not-yet-committed stop-word
        #      audio (the device closed its own mic gate in the same instant),
        #      so in the common case no turn is created at all;
        #   2) if the server VAD committed BEFORE our clear landed (tight race),
        #      OpenAI creates a response moments later anyway — so any assistant
        #      conversation item that appears within INTERRUPT_KILL_WINDOW_S of
        #      a device interrupt is cancelled on arrival (handler below). A
        #      legitimate next turn cannot fall inside that window: after a stop
        #      the mic is closed, and a fresh wake-word turn needs the chime +
        #      speech + VAD end-of-turn (> 2 s) before a response is created.
        _interrupt_kill_until = {"t": 0.0}
        INTERRUPT_KILL_WINDOW_S = 1.5
        # A device "stop" must cancel the NEXT assistant response too, not only
        # the one currently playing. After a stop, the only responses OpenAI can
        # still produce before the user speaks again are unwanted:
        #   - the cancelled reply's already-generated tail;
        #   - a slow tool's answer (web search ~2-4 s) the user stopped mid-run,
        #     created on the tool result OUTSIDE the 1.5 s time-window;
        #   - most common: OpenAI's STT hearing the user's spoken "stop" as a
        #     turn and the model REPLYING to it ("Okay, I'll stop"), which lands
        #     ~1.8 s later — just outside 1.5 s (observed 2026-06-14 22:51: the
        #     device flashed red but a fresh "I'll be quiet" reply played, so the
        #     user had to say stop twice).
        # The time-window alone misses the >1.5 s cases. This flag, armed on
        # EVERY device interrupt, makes _kill_racing_response cancel that one
        # next response regardless of timing. It is consumed when used and
        # cleared at the next genuine turn boundary (real speech via
        # on_real_speech, and {"type":"wake"}) — and a legitimate next turn needs
        # the user to actually speak — so it can never cancel a real turn.
        _kill_next_response = {"v": False}

        async def _on_device_interrupt():
            if self.session is not None and self.session.announcement_active:
                return
            phase_emitter.reset_user_speaking()
            _interrupt_kill_until["t"] = time.monotonic() + INTERRUPT_KILL_WINDOW_S
            # Arm the next-response kill on EVERY stop (see the flag comment):
            # the 1.5 s time-window alone misses responses that land later —
            # OpenAI replying to the spoken "stop", or a slow tool's answer.
            _kill_next_response["v"] = True
            try:
                await openai_service.send_client_event(openai_rt_events.InputAudioBufferClearEvent())
                logger.info("🛑 device interrupt → input_audio_buffer.clear sent (drop in-flight user audio)")
            except Exception as e:
                logger.info(f"🛑 device interrupt → input_audio_buffer.clear no-op ({e!r})")
            try:
                if getattr(openai_service, "_current_assistant_response", None) is not None:
                    await openai_service.send_client_event(openai_rt_events.ResponseCancelEvent())
                    logger.info("🛑 device interrupt → response.cancel sent (response was still active)")
                else:
                    logger.info("🛑 device interrupt → no active response to cancel (device already silenced)")
            except Exception as e:
                logger.info(f"🛑 device interrupt → response.cancel no-op ({e!r})")

        @openai_service.event_handler("on_conversation_item_created")
        async def _kill_racing_response(service, item_id, item):
            # Pipecat fires this for every conversation.item.added; only an
            # ASSISTANT item right after a device interrupt is the racing
            # response to the stop word the user just cancelled.
            if getattr(item, "role", None) != "assistant":
                return
            if self.session is not None and (
                item_id in self.session.kill_exempt_ids or self.session.announcement_active
            ):
                self.session.kill_exempt_ids.discard(item_id)
                return
            within_window = time.monotonic() < _interrupt_kill_until["t"]
            kill_armed = _kill_next_response["v"]
            if not within_window and not kill_armed:
                return
            # Consume the flag: this assistant item is the unwanted response the
            # user's stop pre-empted — a stop-acknowledgement ("Okay, I'll stop"),
            # a stopped tool's answer, or the cancelled reply's tail.
            _kill_next_response["v"] = False
            try:
                await openai_service.send_client_event(openai_rt_events.ResponseCancelEvent())
                logger.info(
                    "🛑 response raced in right after a device interrupt → "
                    "response.cancel (post-stop)"
                )
            except Exception as e:
                logger.info(f"🛑 post-interrupt racing-response cancel no-op ({e!r})")

        async def _on_device_session_start(start: dict):
            # va_client sends {"type":"start"} once per WebSocket CONNECTION
            # (on connect) — NOT per wake. A reconnect mid-utterance (wifi
            # blip, backend restart with session reuse) can leave half an
            # utterance in OpenAI's input buffer; start every (re)connection
            # with a clean one. The per-WAKE/follow-up stale-buffer case is
            # covered by the device's {"type":"flush"} on follow-up timeout.
            phase_emitter.reset_user_speaking()
            if self.session is None:
                self.start_area_lookup(openai_service, start)
            try:
                await openai_service.send_client_event(openai_rt_events.InputAudioBufferClearEvent())
                logger.info("🎬 device (re)connected → input_audio_buffer.clear (clean start)")
            except Exception as e:
                logger.debug(f"🎬 connect-time input clear no-op ({e!r})")

        async def _on_device_mic_flush():
            # The device sends {"type":"flush"} when a follow-up window times out
            # mid-stream. Drop any uncommitted partial utterance NOW, at the
            # cut-off, so a later wake can't "complete" it into a stale answer.
            # This replaced the reactive clear-on-mic-resume, which fired on
            # every wake and disturbed the server VAD → spurious garbage commits.
            # Also a turn boundary for the dangling-VAD guard: the follow-up
            # closed without speech, so any later server-VAD stop is dangling.
            phase_emitter.note_wake()
            try:
                await openai_service.send_client_event(openai_rt_events.InputAudioBufferClearEvent())
                logger.info("🧽 follow-up cut-off → input_audio_buffer.clear (drop partial utterance)")
            except Exception as e:
                logger.debug(f"🧽 mic-flush input clear no-op ({e!r})")

        async def _on_device_wake():
            # va_client sends {"type":"wake"} on every wake (start_session). Mark
            # the turn boundary for the dangling-VAD guard (A): until the user
            # actually speaks, a server-VAD end-of-turn is a stale pre-wake
            # segment closing late → suppress its thinking + cancel its garbage
            # response (handled in PhaseEmitter via the kill-window callbacks).
            phase_emitter.note_wake()
            # New turn boundary: drop any pending post-tool kill so it can't
            # leak onto this fresh turn's response.
            _kill_next_response["v"] = False

        # Wire the dangling-VAD guard's kill-window into the PhaseEmitter. It
        # reuses the SAME _interrupt_kill_until + _kill_racing_response machinery
        # as the device stop: on a dangling stop, arm it so the auto-created
        # garbage response is cancelled; on a real UserStartedSpeaking, clear it
        # so a genuine new turn's response is never cancelled.
        def _clear_kill_window():
            # Real user speech = a genuine new turn — disarm BOTH the time
            # window and the post-tool flag so neither can cancel it.
            _interrupt_kill_until["t"] = 0.0
            _kill_next_response["v"] = False

        phase_emitter.set_kill_window_handlers(
            on_dangling=lambda: _interrupt_kill_until.__setitem__(
                "t", time.monotonic() + INTERRUPT_KILL_WINDOW_S),
            on_real_speech=_clear_kill_window,
        )

        if self._serializer is not None:
            self._serializer.set_interrupt_handler(_on_device_interrupt)
            self._serializer.set_session_start_handler(_on_device_session_start)
            self._serializer.set_mic_flush_handler(_on_device_mic_flush)
            self._serializer.set_wake_handler(_on_device_wake)

        return pipeline, runner, task
    
    async def _send_json(self, websocket, obj: dict) -> None:
        """Send a JSON object to one device as a TEXT websocket frame.

        IMPORTANT: use COMPACT separators (no space after ':' or ','). The Voice
        PE va_client does a literal substring match on `"value":"<phase>"`
        (va_client.cpp handle_text_), so the default json.dumps output
        `"value": "listening"` (with a space) would NOT match and the device
        would silently ignore every phase. Compact output `"value":"listening"`
        matches. This is what made listening/thinking/replying never reach the
        device (LED stuck idle, no-speech watchdog never cancelled).
        """
        try:
            if self.session is not None:
                await self.session.send_json(obj)
            else:
                await websocket.send(json.dumps(obj, separators=(",", ":")))
        except Exception as e:
            logger.warning(f"⚠️ Could not send {obj.get('type')} to device: {e!r}")

    async def broadcast_json(self, obj: dict) -> None:
        """Send a JSON object to every connected device as a TEXT frame."""
        for ws in list(self._websockets):
            await self._send_json(ws, obj)

    async def broadcast_phase(self, value: str) -> None:
        """Send a va_client phase message to every connected device."""
        # TEMP instrumentation: log the broadcast + how many device sockets we
        # think are connected (was debug).
        logger.info(f"➡️ broadcast phase '{value}' to {len(self._websockets)} device(s)")
        await self.broadcast_json({"type": "phase", "value": value})

    async def broadcast_audio_done(self) -> None:
        """Notify the device of bot audio completion without waiting for idle."""
        await self.broadcast_json({"type": "audio_done"})

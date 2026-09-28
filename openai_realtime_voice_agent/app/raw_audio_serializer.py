"""Simple serializer for raw binary PCM audio frames."""
import json
import logging
import os
import re
from pipecat.frames.frames import InputAudioRawFrame, OutputAudioRawFrame, Frame
from pipecat.serializers.base_serializer import FrameSerializer, FrameSerializerType

logger = logging.getLogger(__name__)
MAC_PATTERN = re.compile(r"[0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5}\Z")


def validated_start(data: dict) -> dict:
    """Keep only valid, bounded device identity fields; bare starts are valid."""
    start = {"type": "start"}
    if "mac" in data:
        if isinstance(data["mac"], str) and MAC_PATTERN.fullmatch(data["mac"]):
            start["mac"] = data["mac"].lower()
        else:
            logger.warning("Device start has an invalid MAC; room lookup unavailable")
    if "name" in data:
        if isinstance(data["name"], str) and len(data["name"]) <= 64:
            start["name"] = data["name"]
        else:
            logger.warning("Device start has an invalid name; ignoring it")
    if isinstance(data.get("caps"), list):
        start["caps"] = [
            cap for cap in data["caps"][:16]
            if isinstance(cap, str) and len(cap) <= 32
        ]
    return start


class RawAudioSerializer(FrameSerializer):
    """Serializer that treats all binary messages as raw PCM audio.

    Incoming JSON control frames are consumed here and routed to callbacks;
    outgoing phase/control frames are sent as TEXT by the WebSocketHandler.
    """

    def __init__(self, input_sample_rate: int | None = None):
        # The Home Assistant Voice PE firmware (va_client) streams 16 kHz PCM16
        # mono from the XMOS mic. We tag incoming frames with the device's true
        # rate. NOTE: pipecat 0.0.97's input transport does NOT resample — the
        # InputResampler processor in websocket_handler.py upsamples 16k->24k
        # before the audio reaches OpenAI (which requires 24 kHz pcm16 input).
        if input_sample_rate is None:
            input_sample_rate = int(os.environ.get("DEVICE_INPUT_SAMPLE_RATE", "16000"))
        self._input_sample_rate = input_sample_rate
        # Async callback invoked when the device sends {"type":"interrupt"} (the
        # "stop" wake word). Set by WebSocketHandler.build_pipeline once it has
        # the OpenAI service. We deliberately do NOT emit a pipecat
        # InterruptionFrame for this: pipecat's OWN VAD already emits
        # InterruptionFrame (StartInterruptionFrame) on every user-start-speaking,
        # so reacting to that class would cancel the response on ANY speech.
        self._on_interrupt = None
        # Async callback invoked when the device sends {"type":"start"}. NB the
        # va_client sends this once per WebSocket CONNECTION (on connect), NOT
        # per wake-word session. Used to start every (re)connection with a
        # clean OpenAI input buffer — a reconnect mid-utterance leaves half an
        # utterance behind, which session reuse would replay ahead of the next
        # turn. The per-WAKE stale-buffer case (follow-up window cutting a
        # sentence; observed live 2026-06-12) is covered separately by
        # ConnectionRecovery's mic-resume gap detector in websocket_handler.py.
        self._on_session_start = None
        # Async callback for {"type":"flush"} — the device sends this when a
        # follow-up window times out mid-stream, to drop any uncommitted partial
        # utterance from OpenAI's input buffer AT THE CUT-OFF (so no reactive
        # clear-on-wake is needed). Set by WebSocketHandler.build_pipeline.
        self._on_mic_flush = None
        # Async callback for {"type":"wake"} — sent by va_client on every wake.
        # Resets the dangling-VAD guard's "speech since wake" tracker. Set by
        # WebSocketHandler.build_pipeline.
        self._on_wake = None
        # Timer control frames share the TEXT channel with wake/flush.
        self._on_timer_message = None
        self._on_device_message = None

    def set_interrupt_handler(self, handler):
        """Register the async no-arg callback fired on a device 'interrupt'."""
        self._on_interrupt = handler

    def set_session_start_handler(self, handler):
        """Register the async callback receiving a validated device 'start'."""
        self._on_session_start = handler

    def set_mic_flush_handler(self, handler):
        """Register the async no-arg callback fired on a device 'flush'."""
        self._on_mic_flush = handler

    def set_wake_handler(self, handler):
        """Register the async no-arg callback fired on a device 'wake'."""
        self._on_wake = handler

    def set_timer_message_handler(self, handler):
        """Register the callback for parsed device timer control messages."""
        self._on_timer_message = handler

    def set_device_message_handler(self, handler):
        """Subscribe to otherwise unhandled JSON control messages."""
        self._on_device_message = handler

    @property
    def type(self) -> FrameSerializerType:
        """Get the serialization type - binary for raw audio."""
        return FrameSerializerType.BINARY

    async def deserialize(self, message: bytes) -> InputAudioRawFrame:
        """Deserialize binary message as raw PCM audio frame.

        Args:
            message: Binary PCM audio data (16-bit, mono, device sample rate)

        Returns:
            InputAudioRawFrame with the audio data, or None if invalid
        """
        # Device CONTROL frames arrive as TEXT (str). pipecat 0.0.97's websocket
        # transport has NO on_message event and routes EVERY incoming frame
        # through this serializer, so the device's {"type":"interrupt"} (sent
        # when the user says the "stop" wake word) would be silently dropped and
        # the assistant's reply would never stop. Handle it via the registered
        # interrupt callback (which sends an explicit OpenAI response.cancel) and
        # inject NO frame into the pipeline — emitting a pipecat InterruptionFrame
        # here would be indistinguishable from the VAD's own per-utterance
        # interruptions and would cancel the reply on any speech.
        if isinstance(message, str):
            try:
                data = json.loads(message)
            except (ValueError, TypeError):
                return None
            if isinstance(data, dict) and data.get("type") == "interrupt":
                logger.info("🛑 device interrupt received")
                if self._on_interrupt is not None:
                    try:
                        await self._on_interrupt()
                    except Exception as e:
                        logger.warning(f"⚠️ device interrupt handler failed: {e!r}")
            elif isinstance(data, dict) and data.get("type") == "start":
                # Sent by va_client once per WS connection (on connect). Mic
                # audio only flows after a wake, so clearing the stale OpenAI
                # input buffer here cannot eat new speech.
                logger.info("🎬 device connection start received")
                if self._on_session_start is not None:
                    try:
                        await self._on_session_start(validated_start(data))
                    except Exception as e:
                        logger.warning(f"⚠️ device session-start handler failed: {e!r}")
            elif isinstance(data, dict) and data.get("type") == "flush":
                # A follow-up window timed out mid-stream: drop any uncommitted
                # partial utterance at the cut-off so a later wake can't complete
                # it into a stale answer.
                logger.info("🧽 device mic flush received")
                if self._on_mic_flush is not None:
                    try:
                        await self._on_mic_flush()
                    except Exception as e:
                        logger.warning(f"⚠️ device mic-flush handler failed: {e!r}")
            elif isinstance(data, dict) and data.get("type") == "wake":
                # Sent by va_client on every wake (start_session). Marks a fresh
                # turn boundary for the dangling-VAD guard: until the user
                # actually speaks, any server-VAD end-of-turn is a stale segment
                # from the previous turn closing late (→ garbage response).
                logger.info("👋 device wake received")
                if self._on_wake is not None:
                    try:
                        await self._on_wake()
                    except Exception as e:
                        logger.warning(f"⚠️ device wake handler failed: {e!r}")
            elif isinstance(data, dict) and data.get("type") in (
                "timer_ack", "timer_state", "timer_finished"
            ):
                if self._on_timer_message is not None:
                    self._on_timer_message(data)
            elif isinstance(data, dict) and data.get("type") == "ping":
                if self._on_device_message:
                    await self._on_device_message("ping", data)
            elif isinstance(data, dict) and isinstance(data.get("type"), str):
                if self._on_device_message:
                    await self._on_device_message(data["type"], data)
            # interrupt / ping / start / other control frames: nothing to inject.
            return None

        if not isinstance(message, bytes):
            # Skip anything that isn't bytes or a known text control frame.
            return None

        # Validate audio format: 16-bit = 2 bytes per sample
        if len(message) % 2 != 0:
            logger.warning(f"⚠️ Received audio with odd byte count: {len(message)} bytes, skipping")
            return None

        # Create InputAudioRawFrame at the device's mic rate; the InputResampler
        # processor (right after transport.input()) upsamples it to 24 kHz.
        frame = InputAudioRawFrame(
            audio=message,
            sample_rate=self._input_sample_rate,
            num_channels=1
        )

        return frame
    
    async def serialize(self, frame: Frame) -> bytes:
        """Serialize frame to binary message.
        
        For output audio frames, we just return the raw audio bytes.
        Other frames are not serialized (return empty bytes).
        """
        if isinstance(frame, OutputAudioRawFrame):
            audio_bytes = frame.audio
            logger.debug(f"📤 Serializing OutputAudioRawFrame: {len(audio_bytes)} bytes")
            return audio_bytes
        # For other frame types, return empty bytes (not serialized)
        logger.debug(f"📤 Serializing non-audio frame: {type(frame).__name__}, returning empty bytes")
        return b""

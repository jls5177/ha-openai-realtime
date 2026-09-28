"""Pipecat transport bound to a single, already-authenticated WebSocket."""
import asyncio
import time

from pipecat.frames.frames import (
    CancelFrame, EndFrame, InputAudioRawFrame, InterruptionFrame,
    OutputAudioRawFrame, StartFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.transports.base_transport import BaseTransport
from pipecat.transports.websocket.server import WebsocketServerParams


class SatelliteInput(BaseInputTransport):
    def __init__(self, transport, params):
        super().__init__(params)
        self.transport = transport

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self.transport.serializer.setup(frame)
        await self.set_transport_ready(frame)

    async def receive(self, message):
        frame = await self.transport.serializer.deserialize(message)
        if isinstance(frame, InputAudioRawFrame):
            self.transport.session.counters["audio_in_bytes"] += len(frame.audio)
            await self.push_audio_frame(frame)
        elif frame:
            await self.push_frame(frame)


class SatelliteOutput(BaseOutputTransport):
    def __init__(self, transport, params):
        super().__init__(params)
        self.transport = transport
        self._send_interval = 0.0
        self._next_send_time = 0.0

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self.transport.serializer.setup(frame)
        self._send_interval = (self.audio_chunk_size / self.sample_rate) / 2
        await self.set_transport_ready(frame)

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InterruptionFrame):
            self._next_send_time = 0.0

    async def stop(self, frame: EndFrame):
        await super().stop(frame)

    async def cancel(self, frame: CancelFrame):
        await super().cancel(frame)

    async def send_message(self, frame):
        await self._send(frame)

    async def write_audio_frame(self, frame: OutputAudioRawFrame) -> bool:
        payload = await self.transport.serializer.serialize(
            OutputAudioRawFrame(frame.audio, sample_rate=self.sample_rate,
                                num_channels=self.transport.params.audio_out_channels)
        )
        if payload:
            self.transport.session.counters["audio_out_bytes"] += len(payload)
            await self.transport.session.send_bytes(payload)
        now = time.monotonic()
        delay = max(0.0, self._next_send_time - now)
        if delay:
            await asyncio.sleep(delay)
            self._next_send_time += self._send_interval
        else:
            self._next_send_time = time.monotonic() + self._send_interval
        return True

    async def _send(self, frame):
        payload = await self.transport.serializer.serialize(frame)
        if payload:
            await self.transport.session.send_bytes(payload)


class SatelliteTransport(BaseTransport):
    def __init__(self, session, serializer):
        super().__init__()
        self.session = session
        self.serializer = serializer
        self.params = WebsocketServerParams(
            serializer=serializer, audio_in_enabled=True, audio_out_enabled=True,
            audio_in_sample_rate=24000, audio_out_sample_rate=24000,
        )
        self._input = SatelliteInput(self, self.params)
        self._output = SatelliteOutput(self, self.params)

    def input(self):
        return self._input

    def output(self):
        return self._output

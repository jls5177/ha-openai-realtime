"""Phase transitions with Pipecat frames stubbed for the barge-in race."""

import asyncio
import importlib.util
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture
def phases(monkeypatch):
    class Frame:
        pass

    class FrameProcessor:
        def __init__(self, **kwargs):
            self.forwarded = []

        async def process_frame(self, frame, direction):
            pass

        async def push_frame(self, frame, direction):
            self.forwarded.append(frame)

    frame_module = types.ModuleType("pipecat.frames.frames")
    frame_module.Frame = Frame
    for name in ("UserStartedSpeakingFrame", "UserStoppedSpeakingFrame",
                 "BotStartedSpeakingFrame", "BotStoppedSpeakingFrame"):
        setattr(frame_module, name, type(name, (Frame,), {}))
    processor_module = types.ModuleType("pipecat.processors.frame_processor")
    processor_module.FrameProcessor = FrameProcessor
    processor_module.FrameDirection = type("FrameDirection", (), {})
    for name in ("pipecat", "pipecat.frames", "pipecat.processors"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, frame_module.__name__, frame_module)
    monkeypatch.setitem(sys.modules, processor_module.__name__, processor_module)

    path = Path(__file__).resolve().parents[1] / "app" / "phase_emitter.py"
    spec = importlib.util.spec_from_file_location("phase_emitter_test_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, frame_module


def test_barge_in_keeps_listening_after_late_bot_frames(phases):
    module, frames = phases

    async def scenario():
        sent = []

        async def send_phase(value):
            sent.append(value)

        emitter = module.PhaseEmitter(send_phase, idle_debounce_s=0.01,
                                      interrupt_response=True)

        async def send(name):
            frame = getattr(frames, name)()
            await emitter.process_frame(frame, None)
            assert emitter.forwarded[-1] is frame

        await send("BotStartedSpeakingFrame")
        await send("UserStartedSpeakingFrame")
        await send("BotStartedSpeakingFrame")  # late segment of interrupted reply
        await send("BotStoppedSpeakingFrame")
        assert emitter._idle_task is None
        await asyncio.sleep(0.03)
        assert sent == ["replying", "listening"]

        await send("UserStoppedSpeakingFrame")
        assert sent[-1] == "thinking"  # real barge-in turn, not stale VAD tail
        await send("BotStartedSpeakingFrame")
        await send("BotStoppedSpeakingFrame")
        await asyncio.sleep(0.03)
        assert sent == ["replying", "listening", "thinking", "replying", "idle"]
        emitter._cancel_watchdog()

    asyncio.run(scenario())


def test_default_phase_behavior_stays_unchanged(phases):
    module, frames = phases

    async def scenario():
        sent = []

        async def send_phase(value):
            sent.append(value)

        emitter = module.PhaseEmitter(send_phase, idle_debounce_s=0.01)
        await emitter.process_frame(frames.BotStartedSpeakingFrame(), None)
        await emitter.process_frame(frames.UserStartedSpeakingFrame(), None)
        await emitter.process_frame(frames.BotStoppedSpeakingFrame(), None)
        await asyncio.sleep(0.03)
        assert sent == ["replying", "listening", "idle"]

        await emitter.process_frame(frames.BotStartedSpeakingFrame(), None)
        await emitter.process_frame(frames.UserStoppedSpeakingFrame(), None)
        assert sent[-1] == "replying"  # stale VAD tail still suppressed

    asyncio.run(scenario())


def test_debounce_does_not_override_listening(phases):
    module, _ = phases

    async def scenario():
        sent = []

        async def send_phase(value):
            sent.append(value)

        emitter = module.PhaseEmitter(send_phase, idle_debounce_s=0,
                                      interrupt_response=True)
        await emitter._emit("listening")
        await emitter._emit_idle_after_debounce()
        assert sent == ["listening"]

    asyncio.run(scenario())

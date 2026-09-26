"""WAV recorder lifecycle and periodic header updates."""

import asyncio
import importlib.util
import sys
import types
import wave
from pathlib import Path

from app.audio_recorder import AudioRecorder


def test_recorder_updates_wav_headers_periodically(tmp_path, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr("app.audio_recorder.time.monotonic", lambda: clock[0])
    recorder = AudioRecorder(str(tmp_path))
    recorder.start_recording("device")
    input_file = next(tmp_path.glob("input_*.wav"))
    output_file = next(tmp_path.glob("output_*.wav"))

    recorder.record_input_audio(b"\x01\x02")
    recorder.record_output_audio(b"\x03\x04")
    recorder._input_file.flush()
    assert input_file.read_bytes()[40:44] == b"\x00\x00\x00\x00"

    clock[0] = 1.0
    recorder.record_input_audio(b"\x05\x06")
    with wave.open(str(input_file), "rb") as wav:
        assert wav.getframerate() == 24000
        assert wav.getnframes() == 2
    with wave.open(str(output_file), "rb") as wav:
        assert wav.getnframes() == 1

    recorder.record_output_audio(b"\x07\x08")
    recorder.stop_recording()
    with wave.open(str(output_file), "rb") as wav:
        assert wav.getnframes() == 2


def test_service_does_not_open_wav_until_client_connects(tmp_path, monkeypatch):
    class Frame:
        def __init__(self, audio=b""):
            self.audio = audio

    class FrameProcessor:
        def __init__(self, **kwargs):
            self.forwarded = []

        async def process_frame(self, frame, direction):
            pass

        async def push_frame(self, frame, direction):
            self.forwarded.append(frame)

    frames = types.ModuleType("pipecat.frames.frames")
    frames.Frame = Frame
    for name in ("StartFrame", "InputAudioRawFrame", "OutputAudioRawFrame"):
        setattr(frames, name, type(name, (Frame,), {}))
    processor = types.ModuleType("pipecat.processors.frame_processor")
    processor.FrameProcessor = FrameProcessor
    processor.FrameDirection = type("FrameDirection", (), {})
    for name in ("pipecat", "pipecat.frames", "pipecat.processors"):
        monkeypatch.setitem(sys.modules, name, types.ModuleType(name))
    monkeypatch.setitem(sys.modules, frames.__name__, frames)
    monkeypatch.setitem(sys.modules, processor.__name__, processor)
    path = Path(__file__).resolve().parents[1] / "app" / "audio_recording_service.py"
    spec = importlib.util.spec_from_file_location("audio_recording_test_module", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    service = module.AudioRecordingService(enable_recording=True, output_dir=str(tmp_path))
    assert list(tmp_path.glob("*.wav")) == []
    input_recorder = service.get_input_recorder()
    output_recorder = service.get_output_recorder()
    service.start_new_session("device")

    async def record():
        await input_recorder.process_frame(frames.InputAudioRawFrame(b"\x01\x02"), None)
        await output_recorder.process_frame(frames.OutputAudioRawFrame(b"\x03\x04"), None)

    asyncio.run(record())
    service.stop_recording()
    assert len(input_recorder.forwarded) == len(output_recorder.forwarded) == 1
    for path in tmp_path.glob("*.wav"):
        with wave.open(str(path), "rb") as wav:
            assert wav.getnframes() == 1


def test_recordings_dir_prefers_env(monkeypatch):
    from app.main import recordings_dir

    monkeypatch.setenv("RECORDINGS_DIR", "/tmp/custom")
    assert recordings_dir() == "/tmp/custom"


def test_recordings_dir_uses_share_when_present(monkeypatch):
    import app.main as main

    monkeypatch.delenv("RECORDINGS_DIR", raising=False)
    monkeypatch.setattr(main.os.path, "isdir", lambda p: p == "/share")
    assert main.recordings_dir() == "/share/openai_realtime_voice_agent/recordings"
    monkeypatch.setattr(main.os.path, "isdir", lambda p: False)
    assert main.recordings_dir() == "recordings"

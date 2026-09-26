import asyncio
import json

import pytest

from app.raw_audio_serializer import RawAudioSerializer


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"type": "start"}, {"type": "start"}),
        ({"type": "start", "mac": "AA:BB:CC:DD:EE:FF", "name": "satellite"},
         {"type": "start", "mac": "aa:bb:cc:dd:ee:ff", "name": "satellite"}),
        ({"type": "start", "mac": "not-a-mac", "name": "x" * 65},
         {"type": "start"}),
        ({"type": "start", "mac": 123, "name": 42}, {"type": "start"}),
        ({"type": "start", "mac": "aa:bb:cc:dd:ee:ff", "name": "x" * 64},
         {"type": "start", "mac": "aa:bb:cc:dd:ee:ff", "name": "x" * 64}),
    ],
)
def test_start_payload_validation(payload, expected):
    async def scenario():
        received = []

        async def on_start(start):
            received.append(start)

        serializer = RawAudioSerializer()
        serializer.set_session_start_handler(on_start)
        assert await serializer.deserialize(json.dumps(payload)) is None
        assert received == [expected]

    asyncio.run(scenario())

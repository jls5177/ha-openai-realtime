"""Do Not Disturb delivery, expiry, catch-up and reconnect behavior."""
import asyncio
import json
import logging
import time
from types import SimpleNamespace

import pytest
import aiohttp

from app.announcements import Job
from app.satellites import Diagnostics
from test_announcements import close, next_audio, next_type, setup
from test_multi_satellites import MAC1, MAC2, connect, no_area, ready


async def until(predicate):
    for _ in range(150):
        if predicate():
            return
        await asyncio.sleep(.01)
    raise AssertionError("Condition not met")


def test_all_satellites_skips_dnd_without_delaying_other_target(tmp_path, no_area):
    async def scenario():
        _, registry, manager, client, router, sockets = await setup(
            tmp_path, no_area, two=True, second_dnd=True
        )
        try:
            job = await manager.submit("Dinner is ready.", [MAC1, MAC2])
            request = await next_type(sockets[0], "announce")
            assert request["id"] == job.id
            assert [item.message for item in manager.held[MAC2]] == ["Dinner is ready."]
            assert not manager.pending[MAC2]
            assert not job.remaining.intersection({MAC2})
            await sockets[0].send(json.dumps({"type": "announce_ready", "id": job.id}))
            assert await next_audio(sockets[0])
            assert client.speeches == 1
            assert registry.get(MAC2).session.dnd
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(sockets[1].recv(), .1)
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_busy_dnd_holds_and_release_uses_original(tmp_path, no_area):
    async def scenario():
        app, _, manager, _, router, sockets = await setup(tmp_path, no_area)
        app.announcement_style = "verbatim"
        try:
            job = await manager.submit("A question?", [MAC1])
            assert (await next_type(sockets[0], "announce"))["id"] == job.id
            await sockets[0].send(json.dumps(
                {"type": "announce_busy", "id": job.id, "reason": "dnd"}
            ))
            await until(lambda: len(manager.held.get(MAC1, [])) == 1)
            assert not manager.pending[MAC1]
            assert manager.held[MAC1][0].follow_up
            await sockets[0].send('{"type":"dnd","value":false}')
            request = await next_type(sockets[0], "announce")
            assert request["follow_up"] is True
            assert manager.pending[MAC1][0].spoken.endswith("A question?")
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_hold_cap_expiry_and_zero_setting(tmp_path, no_area, caplog):
    async def scenario():
        app, _, manager, _, router, sockets = await setup(tmp_path, no_area, dnd=True)
        try:
            with caplog.at_level(logging.INFO, logger="app.announcements"):
                for i in range(6):
                    await manager.submit(f"item {i}", [MAC1])
                assert [item.message for item in manager.held[MAC1]] == [
                    f"item {i}" for i in range(1, 6)
                ]
                app.dnd_hold_minutes = 0
                await manager.submit("discard this", [MAC1])
                assert len(manager.held[MAC1]) == 5
                assert "dropping DND announcement" in caplog.text
                app.dnd_hold_minutes = 10
                manager.held[MAC1][0].created -= 11 * 60
                app.announcement_style = "verbatim"
                await sockets[0].send('{"type":"dnd","value":false}')
                request = await next_type(sockets[0], "announce")
                assert request["follow_up"] is False
                await until(lambda: bool(manager.pending[MAC1]))
                catch_up = manager.pending[MAC1][0]
                assert len(catch_up.catch_up) == 4
                assert "item 1" not in catch_up.spoken
                assert "item 2" in catch_up.spoken
                assert "expired 1 DND announcements" in caplog.text
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_held_does_not_use_pending_cap_and_release_waits_for_slot(tmp_path, no_area):
    async def scenario():
        app, _, manager, _, router, sockets = await setup(tmp_path, no_area, dnd=True)
        app.announcement_style = "verbatim"
        try:
            for i in range(5):
                await manager.submit(f"held {i}", [MAC1])
            assert not manager.pending.get(MAC1)
            manager.pending[MAC1] = [
                Job(f"pending {i}", {MAC1}, time.monotonic(), False, False)
                for i in range(5)
            ]
            await sockets[0].send('{"type":"dnd","value":false}')
            await until(lambda: not manager.registry.get(MAC1).session.dnd)
            assert len(manager.held[MAC1]) == 5
            assert len(manager.pending[MAC1]) == 5
            manager.pending[MAC1].pop()
            manager._release_waiting()
            await until(lambda: any(job.catch_up for job in manager.pending[MAC1]))
            assert len(manager.pending[MAC1]) == 5
            assert len(next(job for job in manager.pending[MAC1] if job.catch_up).catch_up) == 5
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


@pytest.mark.parametrize("style", ["verbatim", "faithful", "creative"])
def test_single_catch_up_lead_in_and_history(tmp_path, no_area, style):
    async def scenario():
        app, registry, manager, client, router, sockets = await setup(
            tmp_path, no_area, dnd=True
        )
        app.announcement_style = style
        client.text = "Dinner is ready."
        try:
            await manager.submit("Dinner is ready.", [MAC1])
            session = registry.get(MAC1).session
            session.openai_service._messages_added_manually = {}
            manager.held[MAC1][0].created -= 8 * 60
            await sockets[0].send('{"type":"dnd","value":false}')
            request = await next_type(sockets[0], "announce")
            assert request["chime"] is True
            job = manager.pending[MAC1][0]
            assert job.spoken.startswith("While you were busy, 8 minutes ago: ")
            assert job.spoken.endswith("Dinner is ready.")
            assert client.compositions == (0 if style == "verbatim" else 1)
            assert client.speeches == 1
            await sockets[0].send(json.dumps({"type": "announce_ready", "id": job.id}))
            assert await next_audio(sockets[0])
            await sockets[0].send(json.dumps({"type": "announce_done", "id": job.id}))
            await until(lambda: bool(session.context.get_messages())
                        and session.context.get_messages()[-1]["content"] == job.spoken)
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_three_held_one_summary_one_tts_and_no_follow_up(tmp_path, no_area):
    async def scenario():
        app, _, manager, client, router, sockets = await setup(
            tmp_path, no_area, dnd=True
        )
        app.announcement_style = "creative"
        client.text = "Alex, Bob and Sam have messages."
        try:
            for message in ("Alex arrived?", "Bob arrived?", "Sam arrived?"):
                await manager.submit(message, [MAC1])
            await sockets[0].send('{"type":"dnd","value":false}')
            request = await next_type(sockets[0], "announce")
            job = manager.pending[MAC1][0]
            assert request["id"] == job.id and request["follow_up"] is False
            assert len(job.catch_up) == 3
            assert (client.compositions, client.speeches) == (1, 1)
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_faithful_summary_guard_falls_back_to_aged_verbatim(tmp_path, no_area):
    async def scenario():
        app, _, manager, client, router, sockets = await setup(
            tmp_path, no_area, dnd=True
        )
        app.announcement_style = "faithful"
        client.text = "Someone will be there."
        try:
            await manager.submit("Meet Alex at 7:30.", [MAC1])
            await manager.submit("Call Bob at 8:45.", [MAC1])
            await sockets[0].send('{"type":"dnd","value":false}')
            await next_type(sockets[0], "announce")
            spoken = manager.pending[MAC1][0].spoken
            assert "Alex at 7:30" in spoken and "Bob at 8:45" in spoken
            assert spoken.startswith("While you were busy,")
            assert client.compositions == 2 and client.speeches == 1
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_toggle_during_generation_restores_originals_and_single_release(tmp_path, no_area):
    async def scenario():
        app, _, manager, client, router, sockets = await setup(
            tmp_path, no_area, dnd=True
        )
        app.announcement_style = "creative"
        started = asyncio.Event()
        gate = asyncio.Event()

        async def slow_compose(**kwargs):
            started.set()
            await gate.wait()
            client.compositions += 1
            return SimpleNamespace(output_text="A recap.")

        client.responses.create = slow_compose
        try:
            await manager.submit("First update.", [MAC1])
            await manager.submit("Second update.", [MAC1])
            await sockets[0].send('{"type":"dnd","value":false}')
            await started.wait()
            await sockets[0].send('{"type":"dnd","value":true}')
            await until(lambda: len(manager.held.get(MAC1, [])) == 2)
            assert not manager.pending[MAC1]
            gate.set()
            await sockets[0].send('{"type":"dnd","value":false}')
            await sockets[0].send('{"type":"dnd","value":true}')
            await sockets[0].send('{"type":"dnd","value":false}')
            request = await next_type(sockets[0], "announce")
            assert len(manager.pending[MAC1][0].catch_up) == 2
            assert client.compositions == 1 and client.speeches == 1
            assert len(manager.releases) <= 1
            assert request["follow_up"] is False
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_reenter_after_tts_before_dispatch_restores_originals(tmp_path, no_area):
    async def scenario():
        app, _, manager, client, router, sockets = await setup(
            tmp_path, no_area, dnd=True
        )
        app.announcement_style = "verbatim"
        entered = asyncio.Event()
        gate = asyncio.Event()
        real_tts = manager._tts

        async def slow_tts(text):
            entered.set()
            await gate.wait()
            return await real_tts(text)

        manager._tts = slow_tts
        try:
            await manager.submit("Original.", [MAC1])
            await sockets[0].send('{"type":"dnd","value":false}')
            await entered.wait()
            await sockets[0].send('{"type":"dnd","value":true}')
            await until(lambda: len(manager.held.get(MAC1, [])) == 1)
            gate.set()
            assert manager.held[MAC1][0].message == "Original."
            assert not manager.pending[MAC1]
            assert client.speeches == 0
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_reenter_after_composition_before_playback_reholds(tmp_path, no_area):
    async def scenario():
        app, registry, manager, client, router, sockets = await setup(
            tmp_path, no_area, dnd=True
        )
        app.announcement_style = "verbatim"
        session = registry.get(MAC1).session
        try:
            await manager.submit("Not yet.", [MAC1])
            session.phase_emitter._current = "listening"
            await sockets[0].send('{"type":"dnd","value":false}')
            await until(lambda: bool(manager.pending[MAC1]))
            await sockets[0].send('{"type":"dnd","value":true}')
            await until(lambda: len(manager.held.get(MAC1, [])) == 1)
            assert manager.held[MAC1][0].message == "Not yet."
            assert not manager.pending[MAC1]
            assert client.speeches == 1
            assert session._announcement_id is None
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_reenter_during_reservation_cancels_and_reholds(tmp_path, no_area):
    async def scenario():
        app, _, manager, client, router, sockets = await setup(
            tmp_path, no_area, dnd=True
        )
        app.announcement_style = "verbatim"
        try:
            await manager.submit("Original.", [MAC1])
            await sockets[0].send('{"type":"dnd","value":false}')
            request = await next_type(sockets[0], "announce")
            await sockets[0].send('{"type":"dnd","value":true}')
            assert (await next_type(sockets[0], "announce_cancel"))["id"] == request["id"]
            assert [item.message for item in manager.held[MAC1]] == ["Original."]
            assert not manager.pending[MAC1]
            assert client.speeches == 1
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_reconnect_false_releases_held_and_legacy_dnd_default(tmp_path, no_area):
    async def scenario():
        app, registry, manager, _, router, sockets = await setup(
            tmp_path, no_area, dnd=True
        )
        app.announcement_style = "verbatim"
        try:
            await manager.submit("Missed announcement.", [MAC1])
            await sockets[0].close()
            await until(lambda: registry.get(MAC1).session is None)
            replacement = await connect(router, MAC1, "Kitchen", dnd=False)
            sockets.append(replacement)
            await ready(registry, 1)
            assert json.loads(await replacement.recv())["type"] == "hello"
            assert not registry.get(MAC1).session.dnd
            assert (await next_type(replacement, "announce"))["chime"]
            await replacement.close()
            await until(lambda: registry.get(MAC1).session is None)
            legacy = await connect(router, MAC1, "Kitchen")
            sockets.append(legacy)
            await ready(registry, 1)
            assert json.loads(await legacy.recv())["type"] == "hello"
            assert not registry.get(MAC1).session.dnd
            await legacy.close()
            await until(lambda: registry.get(MAC1).session is None)
            malformed = await connect(router, MAC1, "Kitchen", dnd="yes")
            sockets.append(malformed)
            await ready(registry, 1)
            assert json.loads(await malformed.recv())["type"] == "hello"
            assert not registry.get(MAC1).session.dnd
        finally:
            await close(manager, router, sockets)

    asyncio.run(scenario())


def test_status_and_invalid_dnd_payload(tmp_path, no_area, caplog):
    async def scenario():
        _, registry, manager, _, router, sockets = await setup(tmp_path, no_area)
        diag = Diagnostics(registry, router, port=0, enabled=True)
        try:
            await diag.start("127.0.0.1")
            session = registry.get(MAC1).session
            assert session.status()["dnd"] is False
            await sockets[0].send('{"type":"dnd","value":"true"}')
            await asyncio.sleep(.05)
            assert session.status()["dnd"] is False
            assert "invalid DND value" in caplog.text
            await sockets[0].send('{"type":"dnd","value":true}')
            await until(lambda: session.status()["dnd"])
            async with aiohttp.ClientSession() as http:
                async with http.get(f"http://127.0.0.1:{diag.port}/status") as response:
                    assert response.status == 200
                    assert (await response.json())["sessions"][0]["dnd"] is True
        finally:
            await diag.stop()
            await close(manager, router, sockets)

    asyncio.run(scenario())

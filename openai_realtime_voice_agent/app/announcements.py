"""Bounded, coalesced announcement generation and per-device delivery."""
import asyncio
from collections import Counter
from dataclasses import dataclass, field
import logging
import re
import time
import uuid

import aiohttp
from openai import AsyncOpenAI
from openai import BadRequestError
from pipecat.frames.frames import (
    InterruptionFrame, TTSAudioRawFrame, TTSStartedFrame, TTSStoppedFrame,
)
from pipecat.services.openai.realtime import events

from app.personas import ANNOUNCEMENT_STYLES, PERSONAS
from app.prompt_builder import VOICE_RULES

logger = logging.getLogger(__name__)
TTL = 300
CHUNK = 1920  # 40 ms at 24 kHz, 16-bit mono.
NUMBER = re.compile(r"(?<!\w)[-+]?\d+(?::\d+)*(?:\.\d+)?%?")
WORD = re.compile(r"\b[^\W\d_][\w'-]*\b", re.UNICODE)
COMMON = {
    "i", "a", "the", "an", "on", "at", "in", "to", "for", "and", "but",
    "or", "it", "is", "please", "meet", "bring", "call", "tell", "remind",
}


def fact_guard(original: str, composed: str) -> tuple[bool, str]:
    """Check protected numbers and proper names without relying on the model."""
    missing = []
    output_numbers = Counter(NUMBER.findall(composed))
    for number, count in Counter(NUMBER.findall(original)).items():
        if output_numbers[number] < count:
            missing.append(f"number {number}" + (f" ({count} times)" if count > 1 else ""))
    words = [m.group().casefold() for m in WORD.finditer(original)
             if m.group()[0].isupper()
             and m.group().casefold() not in COMMON]
    output = Counter(m.group().casefold() for m in WORD.finditer(composed))
    for word, count in Counter(words).items():
        if output[word] < count:
            missing.append(f"name {word}" + (f" ({count} times)" if count > 1 else ""))
    return not missing, ", ".join(missing)


def held_age(seconds: float) -> str:
    minutes = int(max(0.0, seconds) / 60)
    if minutes == 0:
        return "less than a minute ago"
    return f"{minutes} {'minute' if minutes == 1 else 'minutes'} ago"


def truncate_message(message: str) -> str:
    if len(message) <= 500:
        return message
    prefix = message[:499].rsplit(" ", 1)[0]
    result = (prefix or message[:499]).rstrip() + "…"
    logger.warning("Announcement truncated to 500 characters")
    return result


def follow_up_for(message: str, setting: str = "auto") -> bool:
    return setting == "always" or (setting == "auto" and "?" in message)


async def append_assistant_note(session, text: str):
    """Keep local and remote conversation histories in sync before follow-up."""
    service = session.openai_service
    if session.context is not None:
        session.context.add_message({"role": "assistant", "content": text})
    if service is not None:
        item = events.ConversationItem(
            type="message", role="assistant",
            content=[events.ItemContent(type="output_text", text=text)],
        )
        service._messages_added_manually[item.id] = True
        session.kill_exempt_ids.add(item.id)
        await service.send_client_event(events.ConversationItemCreateEvent(item=item))


@dataclass
class Job:
    message: str
    targets: set[str]
    created: float
    chime: bool
    follow_up: bool
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:24])
    ready: asyncio.Event = field(default_factory=asyncio.Event)
    pcm: bytes = b""
    spoken: str = ""
    error: str = ""
    remaining: set[str] = field(default_factory=set)
    catch_up: list["Held"] = field(default_factory=list)

    def __post_init__(self):
        self.remaining = set(self.targets)


@dataclass
class Held:
    message: str
    created: float
    chime: bool
    follow_up: bool


class AnnouncementManager:
    def __init__(self, app, registry, client=None):
        self.app, self.registry = app, registry
        self.client = client or AsyncOpenAI(api_key=app.openai_api_key, max_retries=0)
        self._owns_client = client is None
        self.semaphore = asyncio.Semaphore(2)
        self.pending: dict[str, list[Job]] = {}
        self.held: dict[str, list[Held]] = {}
        self.releases: dict[str, asyncio.Task] = {}
        self.dispatchers = {}
        self.generators = set()
        self.expirations = set()
        self.coalescing: list[Job] = []
        self.last_notification = float("-inf")
        self._closing = False

    def start_session(self, session):
        queue = asyncio.Queue()
        self.pending[session.mac] = self.pending.get(session.mac, [])
        session._announcement_queue = queue
        session.subscribe("announce_ready", lambda payload: self._reply(session, "ready", payload))
        session.subscribe("announce_busy", lambda payload: self._reply(session, "busy", payload))
        session.subscribe("announce_done", lambda payload: self._reply(session, "done", payload))
        session.subscribe("announce_cancelled", lambda payload: self._reply(session, "cancelled", payload))
        session.subscribe("announce_text", lambda payload: self._action(session, payload))
        self.dispatchers[session] = asyncio.create_task(self._dispatch(session), name=f"announce:{session.mac}")
        for job in list(self.pending[session.mac]):
            if session.mac in job.remaining:
                if session.dnd:
                    self._hold_job(session.mac, job)
                else:
                    queue.put_nowait(job)
        if not session.dnd:
            self._schedule_release(session.mac)

    async def stop_session(self, session):
        task = self.dispatchers.pop(session, None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        session.announcement_active = False
        session._announcement_reply = None

    async def close(self):
        self._closing = True
        for task in list(self.releases.values()):
            task.cancel()
        for task in list(self.generators):
            task.cancel()
        for task in list(self.expirations):
            task.cancel()
        await asyncio.gather(*self.releases.values(), *self.generators,
                             *self.expirations, return_exceptions=True)
        if self._owns_client:
            await self.client.close()

    async def failure(self, reason):
        logger.warning("Announcement failed: %s", reason)
        now = time.monotonic()
        if now - self.last_notification < 60:
            return
        self.last_notification = now
        if not self.app.ha_token:
            return
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as client:
                async with client.post(
                    f"{self.app.ha_base}/api/services/persistent_notification/create",
                    json={"message": f"Announcement failed: {reason}", "title": "Voice announcement"},
                    headers={"Authorization": f"Bearer {self.app.ha_token}"},
                ) as response:
                    response.raise_for_status()
        except Exception:
            logger.warning("Could not create HA announcement notification", exc_info=True)

    async def submit(self, message, targets, *, chime=None, follow_up="auto", coalesce=True):
        if not isinstance(message, str) or not message.strip():
            await self.failure("Empty announcement")
            return None
        if follow_up not in ("auto", "always", "never"):
            await self.failure("Invalid follow_up setting")
            return None
        message = truncate_message(message.strip())
        targets = set(targets)
        for mac in list(targets):
            satellite = self.registry.get(mac)
            if satellite and satellite.session and "announce" not in satellite.session.caps:
                targets.remove(mac)
                await self.failure(f"{mac}: satellite lacks announce capability")
        if not targets:
            await self.failure("No connected satellites")
            return None
        now = time.monotonic()
        self._prune(now)
        active = {mac for mac in targets
                  if not (self.registry.get(mac) and self.registry.get(mac).session
                          and getattr(self.registry.get(mac).session, "dnd", False))}
        if any(len(self.pending.get(mac, [])) >= 5 for mac in active) or (
            sum(len(j.remaining) for j in self.jobs()) + len(active) > 10
        ):
            await self.failure("Announcement queue full")
            return None
        chime = self.app.announcement_chime if chime is None else chime
        follows = follow_up_for(message, follow_up)
        for mac in targets - active:
            self._hold(mac, Held(message, now, chime, follows))
        if not active:
            job = Job(message, targets, now, chime, follows)
            job.remaining.clear()
            job.ready.set()
            return job
        job = next((j for j in self.coalescing if j.message == message and
                    j.chime == chime and j.follow_up == follows and
                    now - j.created <= .75 and j.targets.isdisjoint(targets)), None) if coalesce else None
        if job:
            job.targets.update(targets)
            job.remaining.update(active)
        else:
            job = Job(message, set(targets), now, chime, follows)
            job.remaining = set(active)
            self.coalescing.append(job)
            task = asyncio.create_task(self._generate(job), name=f"generate:{job.id}")
            self.generators.add(task)
            task.add_done_callback(self.generators.discard)
            expiry = asyncio.create_task(self._expire(job))
            self.expirations.add(expiry)
            expiry.add_done_callback(self.expirations.discard)
        for mac in active:
            self.pending.setdefault(mac, []).append(job)
            satellite = self.registry.get(mac)
            if satellite and satellite.session and satellite.session in self.dispatchers:
                satellite.session._announcement_queue.put_nowait(job)
        return job

    def _hold(self, mac, item):
        minutes = getattr(self.app, "dnd_hold_minutes", 10)
        if not minutes or time.monotonic() - item.created >= minutes * 60:
            logger.info("Satellite %s: dropping DND announcement (holding disabled or expired)", mac)
            return
        items = self.held.setdefault(mac, [])
        items.append(item)
        items.sort(key=lambda held: held.created)
        if len(items) > 5:
            dropped = items.pop(0)
            logger.info("Satellite %s: DND hold full; dropping oldest (age %.0fs)",
                        mac, time.monotonic() - dropped.created)
        logger.info("Satellite %s: holding announcement during DND (%d held)", mac, len(items))

    def _hold_job(self, mac, job):
        if mac not in job.remaining:
            return
        for item in job.catch_up or [Held(job.message, job.created, job.chime, job.follow_up)]:
            self._hold(mac, item)
        job.remaining.discard(mac)
        if job in self.pending.get(mac, []):
            self.pending[mac].remove(job)

    def dnd_changed(self, session):
        mac = session.mac
        if session.dnd:
            task = self.releases.get(mac)
            if task and not task.done():
                task.cancel()
            for job in list(self.pending.get(mac, [])):
                if (session._announcement_id != job.id
                        or (job.catch_up and not session._announcement_pcm_started)):
                    self._hold_job(mac, job)
                    if (session._announcement_id == job.id and session._announcement_reply
                            and not session._announcement_reply.done()):
                        session._announcement_reply.set_result("dnd")
        else:
            self._schedule_release(mac)

    def _schedule_release(self, mac):
        if self._closing or not self.held.get(mac):
            return
        self._prune_held(mac)
        if (not self.held.get(mac) or len(self.pending.get(mac, [])) >= 5
                or sum(len(job.remaining) for job in self.jobs()) >= 10
                or (mac in self.releases and not self.releases[mac].done())):
            return
        task = asyncio.create_task(self._release(mac), name=f"dnd-release:{mac}")
        self.releases[mac] = task
        task.add_done_callback(lambda completed: self._release_finished(mac, completed))

    def _release_finished(self, mac, task):
        if self.releases.get(mac) is task:
            del self.releases[mac]
            satellite = self.registry.get(mac)
            if (task.cancelled() and self.held.get(mac) and satellite and satellite.session
                    and not satellite.session.dnd):
                self._schedule_release(mac)

    def _release_waiting(self):
        for mac in list(self.held):
            satellite = self.registry.get(mac)
            if satellite and satellite.session and not satellite.session.dnd:
                self._schedule_release(mac)

    def _prune_held(self, mac):
        items = self.held.get(mac, [])
        limit = getattr(self.app, "dnd_hold_minutes", 10) * 60
        now = time.monotonic()
        valid = [item for item in items if limit and now - item.created < limit]
        if len(valid) != len(items):
            logger.info("Satellite %s: expired %d DND announcements", mac, len(items) - len(valid))
        if valid:
            self.held[mac] = valid
        else:
            self.held.pop(mac, None)

    async def _release(self, mac):
        self._prune_held(mac)
        now = time.monotonic()
        items = self.held.pop(mac, [])
        valid = items
        if not valid:
            return
        try:
            async with self.semaphore:
                ages = [held_age(now - item.created) for item in valid]
                entries = "\n".join(f"{age}: {item.message}"
                                    for age, item in zip(ages, valid))
                template = ("While you were busy, " + entries.replace("\n", "; ")
                            if len(valid) > 1 else f"While you were busy, {entries}")
                if self.app.announcement_style == "verbatim":
                    spoken = template
                else:
                    for attempt in range(2):
                        try:
                            remaining = TTL - (time.monotonic() - now)
                            if remaining <= 0:
                                raise TimeoutError("DND catch-up expired during generation")
                            if len(valid) == 1:
                                body = await asyncio.wait_for(
                                    self._compose(valid[0].message), remaining
                                )
                                spoken = f"While you were busy, {ages[0]}: {body}"
                            else:
                                spoken = await asyncio.wait_for(
                                    self._compose(
                                        entries, guard_original="\n".join(
                                            item.message for item in valid
                                        ), fallback=template, catch_up=True,
                                    ), remaining,
                                )
                            break
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            if attempt:
                                raise
                            logger.warning("DND catch-up composition retry", exc_info=True)
                remaining = TTL - (time.monotonic() - now)
                if remaining <= 0:
                    raise TimeoutError("DND catch-up expired before speech generation")
                pcm = await asyncio.wait_for(self._tts(spoken), remaining)
            satellite = self.registry.get(mac)
            session = satellite.session if satellite else None
            if not session or session.dnd or session not in self.dispatchers:
                for item in valid:
                    self._hold(mac, item)
                return
            if (len(self.pending.get(mac, [])) >= 5
                    or sum(len(other.remaining) for other in self.jobs()) >= 10):
                for item in valid:
                    self._hold(mac, item)
                return
            if time.monotonic() - now >= TTL:
                raise TimeoutError("DND catch-up expired during generation")
            job = Job(valid[0].message, {mac}, now, any(item.chime for item in valid),
                      valid[0].follow_up if len(valid) == 1 else False)
            job.catch_up = valid
            job.spoken, job.pcm = spoken, pcm
            job.ready.set()
            self.pending.setdefault(mac, []).append(job)
            session._announcement_queue.put_nowait(job)
        except asyncio.CancelledError:
            for item in valid:
                self._hold(mac, item)
            raise
        except Exception as exc:
            for item in valid:
                self._hold(mac, item)
            valid = []
            await self.failure(f"{mac}: DND catch-up failed: {exc}")

    def jobs(self):
        return {j.id: j for jobs in self.pending.values() for j in jobs}.values()

    def _prune(self, now):
        for mac, jobs in self.pending.items():
            session = self.registry.get(mac).session if self.registry.get(mac) else None
            for job in list(jobs):
                if (now - job.created >= TTL and
                        (session is None or getattr(session, "_announcement_id", None) != job.id)):
                    jobs.remove(job)
                    job.remaining.discard(mac)

    async def _expire(self, job):
        await asyncio.sleep(max(0, TTL - (time.monotonic() - job.created)))
        pending_before = set(job.remaining)
        self._prune(time.monotonic())
        self._release_waiting()
        if job.remaining and not job.ready.is_set():
            job.error = "Announcement expired during generation"
            job.ready.set()
        if pending_before - job.remaining:
            await self.failure(f"Announcement {job.id} expired before delivery")

    async def _generate(self, job):
        try:
            await asyncio.sleep(.75)
            if not job.remaining:
                return
            async with self.semaphore:
                if time.monotonic() - job.created >= TTL:
                    raise TimeoutError("Announcement expired before generation")
                for attempt in range(2):
                    try:
                        remaining = TTL - (time.monotonic() - job.created)
                        if remaining <= 0:
                            raise TimeoutError("Announcement expired during generation")
                        job.spoken = await asyncio.wait_for(self._compose(job.message), remaining)
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        if attempt:
                            raise
                        logger.warning("Announcement composition retry", exc_info=True)
                remaining = TTL - (time.monotonic() - job.created)
                if remaining <= 0:
                    raise TimeoutError("Announcement expired during generation")
                job.pcm = await asyncio.wait_for(self._tts(job.spoken), remaining)
        except asyncio.CancelledError:
            job.error = "Announcement stopped"
            raise
        except Exception as exc:
            job.error = str(exc)
            await self.failure(job.error)
        finally:
            job.ready.set()
            if job in self.coalescing:
                self.coalescing.remove(job)

    async def _compose(self, message, *, guard_original=None, fallback=None, catch_up=False):
        style = self.app.announcement_style
        logger.debug("Announcement original: %s", message)
        if style == "verbatim":
            logger.info("announcement text (%s): %s", style, message)
            return message
        rules = (
            "CREATIVE ANNOUNCE RULES: Rewrite the announcement however you like, in character: "
            "jokes, roasts, dramatic flair, and household in-jokes from the instructions are all welcome. "
            "At most 3 short spoken sentences. The listener must still come away knowing what the message "
            "is about, including any time, number or name that actually matters. "
            "Ask a question only if the original asks one. No URLs or markdown. No tools."
            if style == "creative" else
            "FAITHFUL ANNOUNCE RULES: Rephrase in character in 1–2 short spoken sentences. "
            "Keep every fact exactly (names, numbers, times, places, units). "
            "Add no new facts. Ask a question only if the original asks one. "
            "No URLs or markdown. No tools."
        )
        prompt = "\n\n".join((
            self.app.instructions, PERSONAS[self.app.personality], VOICE_RULES, rules,
            f"Home location: {self.app.home_location}. Time zone: {self.app.time_zone or 'unknown'}.",
        ))
        if catch_up:
            prompt += ("\n\nCATCH-UP: Make ONE short spoken summary in character. Cover every "
                       "item and its relative age. Preserve each item's point. "
                       "Do not invent events or combine distinct facts.")
        for attempt in range(2 if style == "faithful" else 1):
            try:
                response = await self.client.responses.create(
                    model=self.app.announcement_model, instructions=prompt, input=message,
                )
                composed = (response.output_text or "").strip()
                if not composed:
                    raise ValueError("Empty composed announcement")
            except asyncio.CancelledError:
                raise
            except Exception:
                # Only the first call may fail up to _generate's retry; a failed
                # correction falls back to the original instead of re-billing.
                if not attempt:
                    raise
                logger.warning("Announcement faithful retry failed; using original", exc_info=True)
                break
            if style == "creative":
                logger.info("announcement text (%s): %s", style, composed)
                return composed
            safe, reason = fact_guard(guard_original if guard_original is not None else message,
                                      composed)
            if safe:
                logger.info("Announcement faithful composition %s passed fact guard",
                            "retry" if attempt else "initial")
                logger.info("announcement text (%s): %s", style, composed)
                return composed
            if attempt:
                logger.warning("Announcement faithful retry failed fact guard (%s); using original", reason)
            else:
                logger.warning("Announcement faithful fact guard failed (%s); retrying with correction", reason)
                prompt += f"\n\nCORRECTION: You dropped: {reason}. Include every missing item."
        logger.info("announcement text (%s): %s", style, fallback or message)
        return fallback or message

    async def _tts(self, text):
        async def fetch(voice):
            pcm = bytearray()
            async with self.client.audio.speech.with_streaming_response.create(
                model=self.app.announcement_tts_model, voice=voice, input=text,
                instructions=ANNOUNCEMENT_STYLES[self.app.personality],
                response_format="pcm",
            ) as response:
                async for chunk in response.iter_bytes():
                    pcm.extend(chunk[:max(0, 24000 * 2 * 30 - len(pcm))])
                    if len(pcm) >= 24000 * 2 * 30:
                        break
            data = bytes(pcm[:len(pcm) // 2 * 2])
            if not data:
                raise ValueError("Empty TTS audio")
            return fade_out(data)

        voice = self.app.voice
        for attempt in range(2):
            try:
                return await fetch(voice)
            except asyncio.CancelledError:
                raise
            except BadRequestError as exc:
                if attempt:
                    raise
                if voice != "marin" and "voice" in str(exc).lower():
                    voice = "marin"
                    logger.warning("Announcement voice rejected; trying marin", exc_info=True)
                else:
                    logger.warning("Announcement TTS retry", exc_info=True)
            except Exception:
                if attempt:
                    raise
                logger.warning("Announcement TTS retry", exc_info=True)

    async def _reply(self, session, kind, payload):
        if not isinstance(payload, dict) or payload.get("id") != session._announcement_id:
            return
        satellite = self.registry.get(session.mac)
        if (session._announcement_generation != session.generation or satellite is None
                or satellite.generation != session.generation or satellite.session is not session):
            return
        if kind == "busy" and payload.get("reason") == "dnd" and not session.dnd:
            session.dnd = True
            logger.info("Satellite %s: DND on (device busy response)", session.mac)
            self.dnd_changed(session)
        future = session._announcement_reply
        if future is not None and not future.done():
            future.set_result("dnd" if kind == "busy" and payload.get("reason") == "dnd"
                              else kind)

    async def _action(self, session, payload):
        if not isinstance(payload, dict) or not isinstance(payload.get("chime", True), bool):
            job = None
        else:
            job = await self.submit(payload.get("message"), [session.mac],
                                    chime=payload.get("chime", True),
                                    follow_up=payload.get("follow_up", "auto"), coalesce=False)
        await session.send_json({"type": "announce_result", "ok": job is not None,
                                 **({"error": "Invalid or full announcement queue"} if job is None else {})})

    async def _note_interrupted(self, session, job):
        if session._announcement_noted:
            return
        try:
            await append_assistant_note(session, "(interrupted) " + job.spoken)
            session._announcement_noted = True
        except Exception as exc:
            await self.failure(f"{session.mac}: interrupted history note failed: {exc}")

    async def _dispatch(self, session):
        try:
            while True:
                job = await session._announcement_queue.get()
                if session.mac not in job.remaining:
                    continue
                drop = True
                try:
                    if "announce" not in session.caps:
                        raise ValueError("Satellite lacks announce capability")
                    if session.dnd:
                        self._hold_job(session.mac, job)
                        continue
                    while not job.ready.is_set() and session.mac in job.remaining:
                        remaining = TTL - (time.monotonic() - job.created)
                        if remaining <= 0:
                            raise TimeoutError("Announcement expired during generation")
                        try:
                            await asyncio.wait_for(job.ready.wait(), min(.1, remaining))
                        except asyncio.TimeoutError:
                            pass
                    if session.mac not in job.remaining:
                        continue
                    if job.error:
                        raise RuntimeError(job.error)
                    while not session.is_idle() and not session.dnd and time.monotonic() - job.created < TTL:
                        await asyncio.sleep(.1)
                    if session.mac not in job.remaining:
                        continue
                    if session.dnd:
                        self._hold_job(session.mac, job)
                        continue
                    if time.monotonic() - job.created >= TTL:
                        raise TimeoutError("Announcement expired")
                    if await self._play(session, job) == "dnd":
                        self._hold_job(session.mac, job)
                except asyncio.CancelledError:
                    if session._announcement_pcm_started:
                        await self._note_interrupted(session, job)
                        await self.failure(f"{session.mac}: announcement interrupted by disconnect")
                    else:
                        drop = False
                    raise
                except Exception as exc:
                    if session._announcement_pcm_started:
                        await self._note_interrupted(session, job)
                    await self.failure(f"{session.mac}: {exc}")
                finally:
                    if drop:
                        job.remaining.discard(session.mac)
                        if job in self.pending.get(session.mac, []):
                            self.pending[session.mac].remove(job)
                        self._release_waiting()
                    session._announcement_pcm_started = False
                    session._announcement_noted = False
        except asyncio.CancelledError:
            raise

    async def _cancel(self, session, job):
        try:
            if not session._closed:
                await session.send_json({"type": "announce_cancel", "id": job.id})
        finally:
            if session.announcement_active and session.pipeline_task:
                await session.queue_frames([InterruptionFrame()])

    async def _play(self, session, job):
        session._announcement_id = job.id
        session._announcement_generation = session.generation
        loop = asyncio.get_running_loop()
        try:
            while time.monotonic() - job.created < TTL:
                if session.dnd:
                    return "dnd"
                session._announcement_reply = loop.create_future()
                await session.send_json({"type": "announce", "id": job.id,
                                         "chime": job.chime, "follow_up": job.follow_up})
                try:
                    reply = await asyncio.wait_for(session._announcement_reply, 5)
                except asyncio.TimeoutError:
                    raise TimeoutError("Announcement readiness timed out")
                if reply == "busy":
                    await asyncio.sleep(min(3, max(0, TTL - (time.monotonic() - job.created))))
                    continue
                if reply == "dnd":
                    if job.catch_up:
                        await self._cancel(session, job)
                    return "dnd"
                if reply == "cancelled":
                    logger.info("Announcement %s cancelled during reservation", job.id)
                    return
                if reply == "ready":
                    if session.dnd and job.catch_up:
                        await self._cancel(session, job)
                        return "dnd"
                    break
            else:
                raise TimeoutError("Announcement expired while device busy")
            session.announcement_active = True
            if session.openai_service is not None:
                session.openai_service.announcement_active = True
            session._announcement_reply = loop.create_future()
            await session.queue_frames([TTSStartedFrame()])
            interrupted = False
            start = loop.time()
            sent_bytes = 0
            for offset in range(0, len(job.pcm), CHUNK * 8):
                if session.dnd and job.catch_up and not session._announcement_pcm_started:
                    await self._cancel(session, job)
                    return "dnd"
                if session._closed:
                    raise ConnectionError("Satellite disconnected during playback")
                if session._announcement_reply.done():
                    interrupted = session._announcement_reply.result() == "cancelled"
                    if interrupted:
                        break
                frames = [TTSAudioRawFrame(audio=job.pcm[i:i + CHUNK],
                                           sample_rate=24000, num_channels=1)
                          for i in range(offset, min(offset + CHUNK * 8, len(job.pcm)), CHUNK)]
                session._announcement_pcm_started = True
                await session.queue_frames(frames)
                sent_bytes += sum(len(frame.audio) for frame in frames)
                await asyncio.sleep(max(0, start + sent_bytes / 48000 - .5 - loop.time()))
            if interrupted:
                await self._cancel(session, job)
                await append_assistant_note(session, "(interrupted) " + job.spoken)
                session._announcement_noted = True
            else:
                await append_assistant_note(session, job.spoken)
                session._announcement_noted = True
                await session.queue_frames([TTSStoppedFrame()])
            if not interrupted:
                try:
                    await asyncio.wait_for(session._announcement_reply, 15)
                except asyncio.TimeoutError:
                    if not session.is_idle():
                        raise TimeoutError("Announcement playback timed out")
        except asyncio.CancelledError:
            await self._cancel(session, job)
            raise
        except Exception:
            await self._cancel(session, job)
            raise
        finally:
            session.announcement_active = False
            if session.openai_service is not None:
                session.openai_service.announcement_active = False
            session._announcement_id = None
            session._announcement_reply = None


def fade_out(data: bytes) -> bytes:
    """Apply a 10 ms linear tail when cutting PCM (also smooths a natural end)."""
    tail = min(240, len(data) // 2)
    result = bytearray(data)
    for n in range(tail):
        i = len(data) - tail * 2 + n * 2
        sample = int.from_bytes(data[i:i + 2], "little", signed=True)
        result[i:i + 2] = int(sample * (tail - n - 1) / tail).to_bytes(2, "little", signed=True)
    return bytes(result)

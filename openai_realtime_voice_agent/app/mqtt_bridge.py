"""Optional MQTT notify discovery, availability and command ingress."""
import asyncio
import json
import logging
import os
import re
import ssl

import aiomqtt
from app.home_context import device_registry_area_sync

logger = logging.getLogger(__name__)
STATUS = "openai_realtime/status"
ROOT = "openai_realtime"


def device_key(mac):
    return "oai_rt_" + mac.replace(":", "").lower()


def command_topic(mac):
    return f"{ROOT}/{mac.lower()}/announce"


def availability_topic(mac):
    return f"{ROOT}/{mac.lower()}/availability"


def discovery(satellite):
    key = device_key(satellite.mac)
    slug = re.sub(r"[^a-z0-9]+", "_", (satellite.name or satellite.mac).lower()).strip("_")
    return {
        "name": "Announce", "unique_id": f"{key}_announce",
        "default_entity_id": f"notify.{slug}_announce",
        "command_topic": command_topic(satellite.mac),
        "availability": [{"topic": topic, "payload_available": "online",
                          "payload_not_available": "offline"}
                         for topic in (STATUS, availability_topic(satellite.mac))],
        "availability_mode": "all",
        "device": {
            "identifiers": [key], "name": f"{satellite.name or satellite.mac} Announcer",
            "manufacturer": "OpenAI Realtime add-on", "model": "Satellite announcer",
            "via_device": "oai_rt_addon",
        },
    }


def all_discovery():
    return {
        "name": "Announce", "unique_id": "oai_rt_all_announce",
        "default_entity_id": "notify.all_satellites_announce",
        "command_topic": f"{ROOT}/all/announce",
        "availability": [{"topic": STATUS, "payload_available": "online",
                          "payload_not_available": "offline"}],
        "device": {"identifiers": ["oai_rt_addon"], "name": "OpenAI Realtime add-on",
                   "manufacturer": "OpenAI Realtime add-on", "model": "Satellite announcer"},
    }


class MQTTBridge:
    def __init__(self, app, registry, announcements, client_factory=None):
        self.app, self.registry, self.announcements = app, registry, announcements
        self.client_factory = client_factory or aiomqtt.Client
        self.enabled = os.environ.get("MQTT_DISCOVERY", "true").lower() == "true"
        self.host = os.environ.get("MQTT_HOST", "")
        self.port = int(os.environ.get("MQTT_PORT", "1883") or 1883)
        self.username = os.environ.get("MQTT_USERNAME") or None
        self.password = os.environ.get("MQTT_PASSWORD") or None
        self.secure = os.environ.get("MQTT_SSL", "false").lower() == "true"
        self._task = None
        self._client = None
        self._updates = asyncio.Queue(maxsize=64)
        self._area_tasks = set()
        registry.on_change(self._changed)

    def _changed(self, event, satellite):
        if self._task:
            if self._updates.full():
                self._updates.get_nowait()
            self._updates.put_nowait((event, satellite))

    async def start(self):
        if not self.enabled or not self.host:
            logger.info("MQTT discovery unavailable or disabled; ESPHome action remains available")
            return
        self._task = asyncio.create_task(self._run(), name="mqtt-announce")

    async def close(self):
        if self._task:
            if self._client:
                try:
                    for satellite in self.registry.all():
                        await self._client.publish(availability_topic(satellite.mac), "offline", retain=True)
                    await self._client.publish(STATUS, "offline", retain=True)
                except Exception:
                    logger.warning("Could not publish MQTT shutdown availability", exc_info=True)
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        for task in self._area_tasks:
            task.cancel()
        await asyncio.gather(*self._area_tasks, return_exceptions=True)

    async def _publish_device(self, client, satellite):
        await client.publish(
            f"homeassistant/notify/{device_key(satellite.mac)}/announce/config",
            json.dumps(discovery(satellite)), retain=True,
        )

    async def _run(self):
        delay = 1
        while True:
            try:
                async with self.client_factory(
                    hostname=self.host, port=self.port, username=self.username,
                    password=self.password,
                    tls_context=ssl.create_default_context() if self.secure else None,
                    will=aiomqtt.Will(STATUS, "offline", retain=True),
                    max_queued_incoming_messages=128,
                ) as client:
                    self._client = client
                    delay = 1
                    await client.subscribe(f"{ROOT}/+/announce")
                    # Reset stale retained states before advertising availability.
                    for satellite in self.registry.all():
                        await client.publish(availability_topic(satellite.mac), "offline", retain=True)
                    for satellite in self.registry.all():
                        await self._publish_device(client, satellite)
                    await client.publish(
                        "homeassistant/notify/oai_rt_all/announce/config",
                        json.dumps(all_discovery()), retain=True,
                    )
                    await client.publish(STATUS, "online", retain=True)
                    for satellite in self.registry.connected():
                        if "announce" in satellite.caps:
                            await client.publish(availability_topic(satellite.mac), "online", retain=True)
                    reader = asyncio.create_task(self._commands(client))
                    updates = asyncio.create_task(self._consume_updates(client))
                    try:
                        done, _ = await asyncio.wait((reader, updates), return_when=asyncio.FIRST_COMPLETED)
                        for task in done:
                            task.result()
                    finally:
                        for task in (reader, updates):
                            task.cancel()
                        await asyncio.gather(reader, updates, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("MQTT broker unavailable; retrying in %ss", delay, exc_info=True)
            finally:
                self._client = None
            await asyncio.sleep(delay)
            delay = min(60, delay * 2)

    async def _consume_updates(self, client):
        while True:
            event, satellite = await self._updates.get()
            if event in ("connected", "updated"):
                await self._publish_device(client, satellite)
            await client.publish(
                availability_topic(satellite.mac),
                "online" if satellite.session and "announce" in satellite.caps else "offline",
                retain=True,
            )
            if event in ("connected", "updated") and satellite.area:
                task = asyncio.create_task(self._sync_area(satellite.mac, satellite.area))
                self._area_tasks.add(task)
                task.add_done_callback(self._area_tasks.discard)

    async def _sync_area(self, mac, area):
        if not self.app.ha_token:
            return
        for attempt in range(3):
            try:
                if await device_registry_area_sync(self.app.ha_base, self.app.ha_token, mac, area):
                    return
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Could not sync MQTT announcer area for %s", mac, exc_info=True)
            if attempt < 2:
                await asyncio.sleep(2)

    async def _commands(self, client):
        async for message in client.messages:
            await self.handle_command(message)

    async def handle_command(self, message):
        if message.retain:
            return
        topic = str(message.topic)
        if topic == f"{ROOT}/all/announce":
            targets = [s.mac for s in self.registry.connected() if "announce" in s.caps]
        else:
            targets = [s.mac for s in self.registry.all()
                       if command_topic(s.mac) == topic and s.session]
        if not targets:
            return
        if len(message.payload) > 4096:
            await self.announcements.failure("MQTT announcement exceeds input limit")
            return
        try:
            text = bytes(message.payload).decode("utf-8")
        except UnicodeDecodeError:
            await self.announcements.failure("Invalid MQTT announcement encoding")
            return
        await self.announcements.submit(text, targets)

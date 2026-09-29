# Changelog

All notable changes to this add-on. Newest first.

## 0.6.6-sat.11

- Add creative, faithful (default) and verbatim announcement styles. Creative uses
  custom instructions for playful announcements; verbatim skips rewriting.
  Faithful and creative both see your custom instructions (household names).
  If the faithful correction call fails, the original is spoken rather than
  composing again.
  Faithful retries once with a targeted correction when its fact check fails,
  then falls back to the original message. Follow-up still uses the original text.

## 0.6.5-sat.10

- Fix "Failed to save: Missing option 'tail_device'" when you change any
  setting. Blank text options (`tail_device`, `device_token`, `home_location`,
  `ha_mcp_url`, `longlived_token`, `mcp_tool_allowlist`) are now optional, so
  the configuration saves with them left empty.

## 0.6.4-sat.9

- **Multiple satellites from one add-on.**
  - Each device gets its own session, with its own OpenAI connection, room, timers, conversation
    history and recording, keyed by the MAC in its `start` message.
  - A device that reconnects replaces its old session and gets its recent history back.
  - Up to eight devices at a time.
- **Optional `device_token`:** devices must send a matching `va_token`, and a 5 s handshake deadline
  applies.
- **Diagnostics.**
  - Every log line is tagged with its device.
  - Event-loop lag monitor.
  - Per-device counters, with a summary every 60 s.
  - `GET /status` on `diagnostics_port` (8081).
  - Optional `tail_device` for pipecat-ai/tail.
- **Announcements.** Home Assistant can have the assistant announce a message in its personality.
  - The message is rephrased once, with a fact check that falls back to the original wording. Speech
    is generated once with OpenAI TTS, so every target plays the same audio.
  - Satellites play a chime first. The mic opens afterwards only if the message asks a question.
  - Announcements queue while a device is busy, for up to 5 minutes.
  - Available through MQTT notify entities (one per satellite plus "All satellites", with rooms synced
    from the ESPHome device) and the Satellite1 `announce` ESPHome action. Requires Satellite1 realtime
    firmware with the `announce` capability.
- **Room lookup** now uses Home Assistant's `config_entries/get` command and prefers the ESPHome device.
- **Conversation history** replayed after a reconnect is capped and sent only once. User turns are
  now always transcribed, so they're kept too.

## 0.6.3-sat.8

- Add `gpt-realtime-2.1` and `gpt-realtime-2.1-mini` to the model list. The
  default stays `gpt-realtime-2`.

## 0.6.2-sat.7

- Each release now bumps the patch number (`X.Y.Z`). Home Assistant could not
  order the old `-sat1.N` suffix, so it never offered the update.

- Add a dedicated `personality` selector (`standard`, `monday`, `cat`,
  `monday_cat`) and optional `home_location` for local answers. Keep spoken
  Voice Rules separate from the shorter default instructions.
- Use Home Assistant's configured local time and device area for contextual
  answers where available; newer Satellite firmware supplies MAC/name for
  area lookup. The time tool is selected automatically.
- Document the single-Satellite transport limit and clarify that prompt-only
  confirmation is not a safety boundary for locks, garages or alarms.

## 0.6.1-sat1.6

- `enable_recording` now writes WAVs to
  `/share/openai_realtime_voice_agent/recordings/` (the add-on maps `/share`),
  so the files can be retrieved. Override with the `RECORDINGS_DIR` env var.

## 0.6.1-sat1.5

- Add `gpt-6-sol` and `gpt-6-luna` to the web search model list. They do not
  support the Realtime API, so the voice model is unchanged.

## 0.6.1-sat1.4

- Timer ids use a random per-process prefix and skip ids already on the device,
  so an add-on restart can no longer replace a timer that survived on the device.
- When a set or cancel request is sent but never acknowledged (timeout or
  disconnect), report an `uncertain` result that tells the assistant to check
  `list_timers` before retrying, instead of a definite failure that could lead
  to a duplicate timer.

## 0.6.1-sat1.3

- Add device-owned voice timer tools to set, list and cancel timers (by name, id
  or all), with remaining times and a three-second acknowledgement timeout.
- Sync timer state after device reconnects; unconfirmed or unsupported requests
  report failure rather than claiming a timer was set.
- Add `enable_timers` (on by default). Requires compatible Voice PE firmware;
  timers ring locally and do not survive a device reboot.

## 0.6.1-sat1.2

- Include `interrupt_response` (boolean) in every device `hello` so firmware
  that supports the handshake can enable hands-free barge-in when configured.
- Clear stale user-speaking state on wake, flush, device interrupt, forced idle,
  and device connect/disconnect, so a cancelled utterance cannot suppress
  subsequent replying or idle phases.
- Send `{"type":"audio_done"}` on bot speech end, before the debounced idle
  phase, to let supporting firmware finish buffered playback promptly. Older
  firmware ignores this unrecognized control message.

## 0.6.1-sat1.1

- Reuse the realtime service bound to the running pipeline on device connects,
  so disconnect context caching, manual interrupts, and in-place reconnects
  all address the live session.
- Repair optional debug WAV recording: attach recorders before building the
  handler, create files only when a device connects, and refresh WAV headers
  roughly once per second instead of flushing every audio frame.
- Expose opt-in hands-free barge-in (`interrupt_response`, default `false`).
  During a barge-in, late bot speech frames no longer return the device to idle
  while the user is still speaking. Requires firmware `barge_in` and good AEC.

Backend fixes hand-ported from [kyvaith/ha-openai-realtime](https://github.com/kyvaith/ha-openai-realtime)
by Tomasz Witke (981dce2, ba8fa91, 0d210a8, 3dba292, ec77a70, c5481de).

## 0.6.0

> ⚠️ **This update has two parts — please update both:**
> 1. **This add-on** (the update you're installing now).
> 2. **The Voice PE firmware** — open **ESPHome Device Builder** and click **Update** (or **Install**) on your device.
>
> The device and the add-on use one shared protocol; updating only one half can cause odd behaviour.

A reliability and voice-control polish release.

**Stop word**

- **Saying "stop" now usually works on the first try.** The spoken "stop" could
  previously be answered by the assistant a moment later, so you sometimes had to
  repeat it; that follow-on reply is now cancelled, so a single "stop" is
  typically enough.
- **Saying "stop" during a web search returns the device to rest promptly** — the
  light ring no longer keeps showing the "replying" animation for several seconds.
- **Fewer accidental stops** on the assistant's own speech.
- The light ring briefly flashes **red** to confirm your "stop" was registered. *(firmware)*

**Reliability**

- **No more unresponsive sessions.** A silently dropped connection to OpenAI is
  now detected and repaired within seconds, instead of leaving the assistant deaf
  until a restart.
- **The roughly hourly reconnect now happens proactively during a quiet moment**,
  so it practically never interrupts a conversation.
- **Smart-home commands are no longer cancelled** if you keep talking while they run.
- The light can no longer get **stuck on "thinking"**, and long web searches get
  all the time they need.

**No more "answers out of nowhere"**

- The assistant no longer occasionally replies — or repeats its previous answer —
  right after the wake word when you said nothing.
- A sentence that got cut off is no longer answered minutes later on your next wake.

**Settings**

- New **"Wake mic delay"** setting: a short pause after the wake chime before the
  mic opens, so the chime can't be mistaken for speech (default 700 ms).
- The **"Follow-up mic delay"** default is now **700 ms**. Existing installs keep
  their saved value — raise yours if the assistant ever answers right after its
  own reply.

## 0.5.0

A big stable release: everything built and tested on the dev channel over the
past days. **Also update the Voice PE firmware** (v1.1.0 — one click in ESPHome
Builder) to get the full effect of the "stop" improvements; the two halves
work best together.

- **"Stop" now works through the whole reply AND the after-reply listening
  window.** The device detects the word more reliably, and the bridge treats
  it as authoritative: in-flight audio is discarded and an answer OpenAI had
  already started for the stop word itself is cancelled on arrival — no more
  "Okay, I'll be quiet" replies to your "stop".
- **Fixed: an answer could cut off mid-sentence, after which the assistant
  went deaf** until the next reconnect. Harmless protocol races (e.g. your
  sentence being split into two turns by a pause) no longer kill the session.
- **Fixed an audio race that could inject noise/hiss into replies** (firmware,
  paired with this release).
- **Mute behaves properly now** (firmware): the ring goes dark with red
  markers by the microphones, and muting also ends an open listening window
  immediately — both from Home Assistant and with the physical side switch.
- **The LED Ring switch in Home Assistant works again** (firmware): entity off
  = device dark at rest; entity on = the gentle "ready" pulse.
- **Completely reworked Configuration tab**: options grouped logically
  (Basics → Model & voice → Conversation → Web search → Audio →
  Home Assistant → Advanced), every description rewritten in plain practical
  language, and a full Dutch translation included (shown automatically when
  your HA is set to Dutch). Confusing or broken switches were removed; rarely
  needed expert fields stay hidden until you need them.
- **The add-on now has its own icon.**
- Friendlier defaults for new installs: follow-up mic delay 200 ms and
  playback buffer 150 ms. **Existing installs keep their saved values** — if
  yours still say 0, consider setting 200/150 manually (Conversation / Audio
  groups) for fewer ghost triggers and less crackle.

### Heads-up: the firmware stub template was improved

The per-device stub in ESPHome Builder used to reference the firmware in a
form that lets ESPHome **cache the downloaded YAML for a day** — clicking
Update shortly after a release could then silently rebuild yesterday's code.
The stub templates in the firmware repo are fixed; existing users can apply
the same fix once by replacing **only the `packages:` block** in their
device's YAML in ESPHome Builder (everything else — your name, secrets,
`dashboard_import` — stays exactly the same):

```yaml
packages:
  realtime:
    url: https://github.com/xandervanerven/home-assistant-voice-pe
    ref: main
    files: [home-assistant-voice.realtime.yaml]
    refresh: 0s
```

Current templates for reference:
[esphome-builder.dhcp.yaml](https://github.com/xandervanerven/home-assistant-voice-pe/blob/main/esphome-builder.dhcp.yaml) ·
[esphome-builder.static-ip.yaml](https://github.com/xandervanerven/home-assistant-voice-pe/blob/main/esphome-builder.static-ip.yaml)

## 0.4.26

- **Web search is now ON by default**, using **gpt-5.5** (the best-quality search
  model), so the assistant can look things up online — weather, news, facts — out
  of the box. **Existing installs keep their saved setting**: if you had it off,
  switch `enable_web_search` on (and set `web_search_model` to `gpt-5.5`) in the
  add-on Configuration. The cheaper mini/nano models stay available.

## 0.4.25

- **Fix:** the first thing you said in the few seconds right after an automatic
  reconnect (e.g. after the 60-minute session cap) could be ignored
  (`conversation_already_has_active_response`). The reconnected session no longer
  creates a duplicate response, so that turn answers normally.

## 0.4.24

- **Renamed** to **OpenAI Realtime 2 Voice Agent**.
- Rewrote the store/info description and added a full **Documentation** tab
  (install steps, OpenAI key, Home Assistant MCP setup, recommended settings, web
  search, credits). Removed stale text from the original upstream client.
- Default system prompt is now an English, voice-tuned prompt (silent tool calls,
  varied confirmations, language pinning). Your own saved prompt is not changed.
- Default `follow_up_open_delay_ms` and `playback_prebuffer_ms` set to `0` (raise
  them if the device hears its own tail or you hear crackle).

## 0.4.23

- **Fix:** the 60-minute session cap sometimes left the session dead until a
  restart. It now reconnects automatically in all cases (both the keepalive-drop
  and the `session_expired` forms).

## 0.4.22

- **New options:** voice **speed** (0.25–1.5), **max reply length**
  (`max_output_tokens`), and **input noise reduction** (off / near-field /
  far-field). All default to current behaviour.

## 0.4.21

- Model, voice, web-search-model and transcription-model options are now
  **dropdowns** with the known-good values, each with a **custom** entry if you
  need a value not in the list.

## 0.4.20

- **New:** optional **web search**. Turn on `enable_web_search` to let the
  assistant look things up online (weather, news, facts). Uses your OpenAI key;
  off by default. Model configurable via `web_search_model` (default gpt-5.4-mini).

## 0.4.19

- Clarified the MCP option help text for both the built-in HA MCP Server and the
  unofficial ha-mcp add-on.

## 0.4.18

- **Fix:** removed a meaningless filler reply ("I'm ready to continue…") that could
  appear on the first turn of a session.

## 0.4.17

- **Fix:** cap restored conversation history (`max_context_messages`, default 12) to
  bound per-turn token cost and avoid hitting OpenAI's rate limit.

## 0.4.16

- **Fix:** the device no longer gets stuck blinking "thinking" after a turn-ending
  error (e.g. a rate limit) — it returns to idle so you can retry.

## 0.4.14

- **New:** `playback_prebuffer_ms` jitter buffer to reduce occasional crackle at the
  start of replies.

## 0.4.12 – 0.4.13

- **Fix:** "say stop, then immediately ask again → silence". Disabled the broken
  server-side audio truncation that wedged the next turn.

## 0.4.9 – 0.4.11

- **New:** auto-reconnect the OpenAI Realtime session when its connection drops
  (keepalive timeout / 60-minute cap), instead of going dead until a restart.
  Refined so a normal device disconnect doesn't trigger an unnecessary reconnect.

## 0.4.6 – 0.4.8

- **New:** configurable post-reply **follow-up listening window** (answer back
  without re-saying the wake word) + its open-delay, and per-option help text in the
  UI.
- **New:** the assistant's and user's transcripts are logged to the add-on log
  (`🤖 assistant:` / `🗣️ user:`).

## 0.4.0 – 0.4.4

- **Fix:** resample the device's 16 kHz mic to the 24 kHz OpenAI requires (garbled
  speech), and drop empty audio chunks.
- **New:** device **"stop"** interrupt now actually cancels the reply and clears
  buffered audio.

## 0.3.x

- Switched the target to **gpt-realtime-2**, pinned pipecat-ai 0.0.97, and tuned
  turn detection (semantic VAD), phase delivery to the device, and the startup
  sequence to stop double-responses. Made the disconnect tool and transcription
  model configurable.

## Earlier

- Initial pipecat + WebSocket implementation (forked from
  [fjfricke/ha-openai-realtime](https://github.com/fjfricke/ha-openai-realtime)).

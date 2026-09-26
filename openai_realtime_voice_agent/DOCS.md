# OpenAI Realtime 2 Voice Agent — Documentation

This add-on runs an **OpenAI `gpt-realtime-2`** voice session and bridges it to Home
Assistant control, device timers and web search. It is the backend half of a two-part project; the
front half is custom **firmware for the Home Assistant Voice PE** device (see
[Firmware](#firmware-home-assistant-voice-pe-only) below).

```
Voice PE device  ──WebSocket──▶  this add-on  ──▶  OpenAI Realtime API
(mic up / speaker down)               │  tools
                                       ▼
                              HA MCP Server (/api/mcp)  → controls your home
```

---

## 1. Install the add-on

1. In Home Assistant go to **Settings → Add-ons → Add-on Store**.
2. Top-right **⋮ → Repositories**, add:
   `https://github.com/xandervanerven/ha-openai-realtime`
3. Find **OpenAI Realtime 2 Voice Agent** in the store and click **Install**.
   (There is no prebuilt image — Home Assistant builds it locally the first time,
   which takes a few minutes.)
4. Open the add-on's **Configuration** tab to set it up (next sections).

## 2. Get an OpenAI API key

1. Go to <https://platform.openai.com/> → **API keys** → **Create new secret key**.
2. Make sure **billing** is set up on your OpenAI account — `gpt-realtime-2` (audio)
   and web search are paid usage.
3. Paste the key into the add-on's **`openai_api_key`** option.

> **Heads-up on rate limits:** new accounts start at a low tokens-per-minute (TPM)
> tier. `gpt-realtime-2` audio is token-heavy, so if you see *"Rate limit reached"*
> in the logs, raise your usage tier in the OpenAI dashboard, or keep
> `max_context_messages` modest (default 12).

## 3. Let it control Home Assistant (MCP)

The assistant controls your home through Home Assistant's **official MCP Server**.

1. In HA: **Settings → Devices & Services → Add Integration → "Model Context
   Protocol Server"** and add it.
2. **Expose the entities** you want voice control over to **Assist**
   (Settings → Voice assistants → *Exposed entities*). The MCP server only offers
   what's exposed. For a hard safety guarantee, **do not expose locks, garage
   doors or alarm controls to Assist**. "Confirm before unlocking" is only a
   prompt rule, not an enforced safeguard.
3. In the add-on, leave **`ha_mcp_url`** **blank** — it then uses the built-in
   endpoint (`http://supervisor/core/api/mcp`) with the add-on's own token. Leave
   **`longlived_token`** blank too, unless startup logs a 401/403 on
   `/core/api/mcp` (then paste a HA long-lived token there).

You get a small fixed set of Assist tools (`HassTurnOn`, `HassTurnOff`,
`HassLightSet`, `GetLiveContext`, `GetDateTime`, …). **`GetLiveContext`** is the
"what's the current state?" tool — keep it; it's what answers *"is the light on?"*.

**`mcp_tool_allowlist`** (optional): a comma-separated whitelist of tool names. Leave
blank to expose all, or trim to just what you use, e.g.:
`HassTurnOn,HassTurnOff,HassLightSet,GetLiveContext,GetDateTime`

For room-aware commands such as "turn on the lights in here", assign the
**Satellite device** to an area in Home Assistant (Settings → Devices & Services
→ Devices). Newer firmware sends its MAC address and name so the add-on can look
up the device's HA area. With older firmware the room is unknown; name the room
explicitly or expect a clarification rather than assuming where the device is.

## 4. Recommended starting settings

**The defaults are the recommended settings** — for a first run you only need the
API key, the MCP integration (section 3), and ideally your language. The
Configuration tab is grouped: **🔑 Basics → 🗣️ Model & voice → 💬 Conversation →
🌐 Web search → ⏱️ Timers → 🎚️ Audio → 🏠 Home Assistant → ⚙️ Advanced → 🔍 Debug**, and every
option has plain-language inline help.

| Option | Default | Note |
|---|---|---|
| `openai_model` | `gpt-realtime-2` | speech-to-speech model; `gpt-realtime-2.1` is newer, `gpt-realtime-2.1-mini` is the low-cost option with tool use |
| `openai_voice` | `marin` | `marin`/`cedar` are the newest voices |
| `transcription_language` | *(blank)* | set your ISO code (e.g. `nl`): locks the language + logs the user transcript |
| `instructions` | *(English default)* | custom language/house rules; change the LANGUAGE line for your language; Voice Rules always apply |
| `personality` | `monday` | `standard` (neutral), `monday` (dry wit), `cat` (playful), or `monday_cat` (both) |
| `home_location` | *(blank)* | optional town/region for local answers; HA supplies the time zone and units |
| `follow_up_listen_seconds` | `8` | mic stays open this long so you can answer back |
| `follow_up_open_delay_ms` | `700` | echo guard before the follow-up mic opens; lower = snappier but risks ghost turns |
| `wake_open_delay_ms` | `700` | the same echo guard right after the wake chime; lower = snappier wake but risks a ghost turn |
| `vad_eagerness` | `low` | waits longest before deciding you're done talking |
| `interrupt_response` | `false` | hands-free barge-in; needs firmware `barge_in` and good AEC |
| `playback_prebuffer_ms` | `150` | raise to ~250 if you hear crackle; 0 = play immediately |
| `max_context_messages` | `12` | bounds per-turn token cost |
| `enable_web_search` | `true` | online lookups; set `false` to disable |
| `enable_timers` | `true` | device-owned voice timers; needs compatible firmware |
| `web_search_model` | `gpt-5.5` | best-quality search model; mini/nano are cheaper |

The legacy `server_vad` turn-detection fields live at the bottom of ⚙️ Advanced and
only appear when you enable **"Show unused optional configuration options"** —
leave them unset unless you have a specific reason.

## 5. Web search

When **`enable_web_search`** is on (**the default**), the assistant gets a `web_search` tool. When
it needs current or general info (weather, news, facts), it calls that tool; the
add-on then makes a **second, server-side OpenAI call** (the Responses API
`web_search` built-in tool, on **`web_search_model`**) and reads a short spoken
answer back.

- Uses your **existing OpenAI key** — no extra account.
- Default model `gpt-5.5` (best quality). Cheaper options trade price/quality
  (`gpt-5.4`, `gpt-5-mini`, the nano models, …) — a few cents per search.
  `gpt-6-sol` and the very cheap `gpt-6-luna` are also listed. GPT-6 models are
  text models, not Realtime models, so they can only be used for web search;
  the voice itself stays on a `gpt-realtime-*` model.
- Adds ~1–3 s while it searches (the device shows "thinking").
- If the model name is rejected, the assistant just says it couldn't search — it
  won't crash the session, so you can change `web_search_model` and retry.

**Voice timers:** With `enable_timers` on (the default) and compatible firmware (such as
the Satellite1 realtime variant), ask to set a timer for up to 24 hours, list timers with remaining time,
or cancel one by name or all at once. Timers run and ring on the device even if
the WebSocket disconnects; they do not survive a device reboot. A timer is only
confirmed after the device acknowledges it. Older firmware ignores timer requests,
so after three seconds the assistant reports that the timer was not confirmed.
If a request was sent but the acknowledgement was lost, the assistant checks the
timer list before retrying rather than creating a duplicate.
When several timers share a name, the assistant asks which one to cancel.

## 6. Options reference & tuning

Every option has a description on the **Configuration** tab. The ones worth knowing:

- **Model / voice / transcription model** are dropdowns with a **`custom`** entry +
  a `*_custom` text field if you want a value not in the list.
- **`transcription_language`** turns the side-channel transcript on. With it set you
  get `🗣️ user: …` lines in the add-on log (handy for debugging); it does **not**
  change what the model understands — the main model hears your audio natively.
- **`personality` / `instructions`**: select a persona independently of the
  custom instructions. The always-present Voice Rules govern spoken brevity
  and tool use even if you edit `instructions`. Existing installations may
  have saved the old cheerful/style prompt; remove those lines from your
  saved instructions if you want the selected persona to come through cleanly.
  Keep the LANGUAGE rule if you need to lock the spoken language.
- **`home_location` / local time**: set a town or region (e.g. `Amsterdam, NL`)
  for location-aware answers. HA's `location_name` is a device/home label,
  not necessarily a city, so a blank value does not assume a place. Configure
  your time zone under **Settings → System → General** in Home Assistant;
  local time follows that time zone.
  The assistant picks the time tool automatically when you ask for the time
  — you do not need to ask for a tool by name.
- **`follow_up_open_delay_ms` / `playback_prebuffer_ms`** default to `700` / `150`
  — an echo guard and jitter cushion. Lowering them makes the device feel
  snappier, but below ~700 ms open delay the reply's own speaker tail can leak
  into the fresh follow-up mic and become a ghost turn (the assistant "answers
  nobody" or repeats itself); raise the prebuffer if you hear crackle at the
  start of replies.
- **`interrupt_response`** enables hands-free barge-in: OpenAI's server VAD may
  cut off the assistant's reply when you speak over it, and the device switches
  to listening (flushing queued TTS). Leave it `false` unless the Voice PE
  firmware has `barge_in` enabled and good acoustic echo cancellation (AEC);
  otherwise the speaker's own audio may interrupt the reply. With this option
  off, the device "stop" wake word or center button still interrupts replies.
  The add-on advertises this boolean in the WebSocket `hello` handshake; older
  firmware that does not read this key still uses its own `barge_in` setting.
- **Device protocol:** the add-on sends `{"type":"audio_done"}` as soon as bot
  speech ends so supporting firmware can drain the final playback buffer. The
  `idle` phase remains debounced for 1.5 seconds to bridge gaps between speech
  segments. Older firmware ignores the unknown `audio_done` message.
- **`enable_recording`** saves input (what the device mic sent) and output
  WAV files to `/share/openai_realtime_voice_agent/recordings/` for debugging;
  fetch them with the Samba/SSH add-ons or the File editor. Files start when a device connects
  and are updated roughly once per second while recording; leave it off
  unless troubleshooting.

## 7. Reading the logs

The add-on log shows each turn: `🗣️ user:` (when transcription language is set),
`🤖 assistant:` (the reply text), `📞 phase ->` (device state), tool calls, and
`🔌 …reconnecting` / `✅ reconnected` on a connection recovery. View it on the add-on
**Log** tab.

## Known limitations

- **One Satellite per add-on instance.** The current audio/WebSocket transport
  serves one active device at a time; use a separate add-on instance per
  Satellite rather than pointing multiple Satellites at the same port.
- **Room lookup needs device identity.** Newer firmware sends the device
  MAC/name for HA area lookup; older firmware leaves the room unknown.
- **Voice timers require compatible firmware; voice alarms are not supported.**
  Timers belong to the device rather than Home Assistant timer entities. Older
  firmware will not confirm timer commands and no timer is claimed as set.
- **A brief reconnect about once an hour.** OpenAI limits a realtime session to
  60 minutes. The add-on refreshes proactively during a quiet moment, so you'll
  rarely notice it, but a reconnect can occasionally cause a ~1–2 second pause.
- **Rarely, the assistant may stop itself** on a word in its own reply that sounds
  like "stop" — just ask again.

**Using "stop":** say "stop" (or press the center button) to interrupt the assistant
*while it's speaking* — during a reply, or during the short listening window right
after one. It has no effect before the assistant has started answering (there's
nothing to stop yet).

## Firmware (Home Assistant Voice PE only)

This add-on expects the custom **Voice PE firmware** that turns the device into a
thin client (it streams mic audio here and plays the reply). That firmware:

- is **specific to the Home Assistant Voice PE** hardware (ESP32-S3 + XMOS), and
- lives in its own **public** repository:
  **[xandervanerven/home-assistant-voice-pe](https://github.com/xandervanerven/home-assistant-voice-pe)**.

You flash it once via a tiny per-device "stub" in ESPHome Builder; after that, firmware
updates are **one click** — no tokens, no copy-pasting. That repo has the full
from-scratch guide (flashing + adopting the device in ESPHome Builder).

## Credits

- Backend forked from **[fjfricke/ha-openai-realtime](https://github.com/fjfricke/ha-openai-realtime)** (Felix Fricke).
- Firmware thin-client design based on **[maxmaxme/home-assistant-voice-pe](https://github.com/maxmaxme/home-assistant-voice-pe)**, a fork of **[esphome/home-assistant-voice-pe](https://github.com/esphome/home-assistant-voice-pe)** (Nabu Casa / ESPHome).
- Inspiration from **[marcinnowak79/home-assistant-voice-pe](https://github.com/marcinnowak79/home-assistant-voice-pe)** (gemini-live-proxy).
- Built on **[pipecat-ai](https://github.com/pipecat-ai/pipecat)**, the **OpenAI Realtime API**, and the official **[Home Assistant MCP Server](https://www.home-assistant.io/integrations/mcp_server/)** integration.

#!/usr/bin/with-contenv bashio
set -e

# --- 🔑 Basics ---
OPENAI_API_KEY=$(bashio::config 'openai_api_key')
INSTRUCTIONS=$(bashio::config 'instructions')
PERSONALITY=$(bashio::config 'personality')

# --- 🗣️ Model & voice ---
OPENAI_MODEL=$(bashio::config 'openai_model')
OPENAI_VOICE=$(bashio::config 'openai_voice')
OPENAI_SPEED=$(bashio::config 'openai_speed')
MAX_OUTPUT_TOKENS=$(bashio::config 'max_output_tokens')

# --- 💬 Conversation ---
FOLLOW_UP_LISTEN_SECONDS=$(bashio::config 'follow_up_listen_seconds')
FOLLOW_UP_OPEN_DELAY_MS=$(bashio::config 'follow_up_open_delay_ms')
WAKE_OPEN_DELAY_MS=$(bashio::config 'wake_open_delay_ms')
VAD_EAGERNESS=$(bashio::config 'vad_eagerness')
PHASE_IDLE_DEBOUNCE_MS=$(bashio::config 'phase_idle_debounce_ms')
INTERRUPT_RESPONSE=$(bashio::config 'interrupt_response')

# --- 🌐 Web search ---
ENABLE_WEB_SEARCH=$(bashio::config 'enable_web_search')
WEB_SEARCH_MODEL=$(bashio::config 'web_search_model')
ANNOUNCEMENT_TTS_MODEL=$(bashio::config 'announcement_tts_model')
ANNOUNCEMENT_STYLE=$(bashio::config 'announcement_style')
MQTT_DISCOVERY=$(bashio::config 'mqtt_discovery')
ANNOUNCEMENT_CHIME=$(bashio::config 'announcement_chime')

# --- ⏱️ Timers ---
ENABLE_TIMERS=$(bashio::config 'enable_timers')

# --- 🎚️ Audio ---
PLAYBACK_PREBUFFER_MS=$(bashio::config 'playback_prebuffer_ms')
NOISE_REDUCTION=$(bashio::config 'noise_reduction')

# --- 🏠 Home Assistant ---

# --- ⚙️ Advanced ---
WEBSOCKET_PORT=$(bashio::config 'websocket_port')
DIAGNOSTICS_PORT=$(bashio::config 'diagnostics_port')
SESSION_REUSE_TIMEOUT_SECONDS=$(bashio::config 'session_reuse_timeout_seconds')
MAX_CONTEXT_MESSAGES=$(bashio::config 'max_context_messages')
TRANSCRIPTION_MODEL=$(bashio::config 'transcription_model')

# --- 🔍 Debug ---
ENABLE_RECORDING=$(bashio::config 'enable_recording')

# Validate required configuration
if [ -z "$OPENAI_API_KEY" ]; then
    bashio::log.error "OPENAI_API_KEY is required but not set"
    exit 1
fi

# Export environment variables
export OPENAI_API_KEY
export INSTRUCTIONS
export PERSONALITY
export OPENAI_MODEL
export OPENAI_VOICE
export OPENAI_SPEED
export MAX_OUTPUT_TOKENS
export FOLLOW_UP_LISTEN_SECONDS
export FOLLOW_UP_OPEN_DELAY_MS
export WAKE_OPEN_DELAY_MS
export VAD_EAGERNESS
export PHASE_IDLE_DEBOUNCE_MS
export INTERRUPT_RESPONSE
export ENABLE_WEB_SEARCH
export WEB_SEARCH_MODEL
export ANNOUNCEMENT_TTS_MODEL
export ANNOUNCEMENT_STYLE
export MQTT_DISCOVERY
export ANNOUNCEMENT_CHIME
export ENABLE_TIMERS
export PLAYBACK_PREBUFFER_MS
export NOISE_REDUCTION
export WEBSOCKET_PORT
export DIAGNOSTICS_PORT
export SESSION_REUSE_TIMEOUT_SECONDS
export MAX_CONTEXT_MESSAGES
export TRANSCRIPTION_MODEL
export ENABLE_RECORDING

# The *_custom escape hatches (🗣️/🌐/⚙️) are optional WITHOUT defaults —
# bashio::config prints "null" for unset optionals, and main.py's
# _resolve_choice would treat that literal string as a real custom value.
# Only export when actually set.
if bashio::config.has_value 'openai_model_custom'; then
    OPENAI_MODEL_CUSTOM=$(bashio::config 'openai_model_custom')
    export OPENAI_MODEL_CUSTOM
fi
if bashio::config.has_value 'openai_voice_custom'; then
    OPENAI_VOICE_CUSTOM=$(bashio::config 'openai_voice_custom')
    export OPENAI_VOICE_CUSTOM
fi
if bashio::config.has_value 'web_search_model_custom'; then
    WEB_SEARCH_MODEL_CUSTOM=$(bashio::config 'web_search_model_custom')
    export WEB_SEARCH_MODEL_CUSTOM
fi
if bashio::config.has_value 'announcement_model'; then
    ANNOUNCEMENT_MODEL=$(bashio::config 'announcement_model')
    export ANNOUNCEMENT_MODEL
fi
if bashio::config.has_value 'transcription_model_custom'; then
    TRANSCRIPTION_MODEL_CUSTOM=$(bashio::config 'transcription_model_custom')
    export TRANSCRIPTION_MODEL_CUSTOM
fi

# Legacy server_vad escape hatch (⚙️ Advanced, optional WITHOUT defaults).
# bashio::config prints the string "null" for unset optional keys, which would
# crash main.py's float()/int() parsing — so only export when actually set.
# Unset = main.py's hardwired defaults (semantic_vad; 0.5/300/800 if server_vad
# is ever selected).
if bashio::config.has_value 'turn_detection_type'; then
    TURN_DETECTION_TYPE=$(bashio::config 'turn_detection_type')
    export TURN_DETECTION_TYPE
fi
if bashio::config.has_value 'vad_threshold'; then
    VAD_THRESHOLD=$(bashio::config 'vad_threshold')
    export VAD_THRESHOLD
fi
if bashio::config.has_value 'vad_prefix_padding_ms'; then
    VAD_PREFIX_PADDING_MS=$(bashio::config 'vad_prefix_padding_ms')
    export VAD_PREFIX_PADDING_MS
fi
if bashio::config.has_value 'vad_silence_duration_ms'; then
    VAD_SILENCE_DURATION_MS=$(bashio::config 'vad_silence_duration_ms')
    export VAD_SILENCE_DURATION_MS
fi

# Removed options (v0.4.29) — no longer exported; main.py env defaults take
# over: SEMANTIC_VAD_CREATE_RESPONSE=true, ENABLE_DISCONNECT_TOOL=false,
# DEVICE_INPUT_SAMPLE_RATE=16000.

# Optional text options (blank = unset). bashio::config prints "null" for
# unset optionals, so export only real values; main.py treats absent as "".
for opt in transcription_language ha_mcp_url longlived_token mcp_tool_allowlist home_location device_token tail_device; do
    if bashio::config.has_value "${opt}"; then
        var=$(echo "${opt}" | tr '[:lower:]' '[:upper:]')
        export "${var}=$(bashio::config "${opt}")"
    fi
done

# SUPERVISOR_TOKEN is automatically provided by Home Assistant when homeassistant_api: true
if bashio::services.available "mqtt"; then
    MQTT_HOST=$(bashio::services mqtt "host")
    MQTT_PORT=$(bashio::services mqtt "port")
    MQTT_USERNAME=$(bashio::services mqtt "username")
    MQTT_PASSWORD=$(bashio::services mqtt "password")
    MQTT_SSL=$(bashio::services mqtt "ssl")
    export MQTT_HOST MQTT_PORT MQTT_USERNAME MQTT_PASSWORD MQTT_SSL
fi

# Start the application
export PYTHONUNBUFFERED=1
exec python3 -m app.main

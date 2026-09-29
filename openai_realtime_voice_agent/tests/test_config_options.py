"""Guards add-on option definitions against Supervisor save failures."""
from pathlib import Path
import re

import yaml

ADDON = Path(__file__).resolve().parents[1]
CONFIG = yaml.safe_load((ADDON / "config.yaml").read_text())
RUN_SH = (ADDON / "root" / "run.sh").read_text()


def test_blank_defaults_are_optional():
    # The add-on UI omits blank text fields on save; a required blank option
    # then fails with "Missing option '<key>'".
    required_blank = [
        key for key, value in CONFIG["options"].items()
        if value == "" and not str(CONFIG["schema"][key]).endswith("?")
        and key != "openai_api_key"
    ]
    assert required_blank == []


def test_optional_options_are_not_read_unconditionally():
    # bashio::config prints "null" for unset optionals.
    for key, kind in CONFIG["schema"].items():
        if str(kind).endswith("?"):
            assert not re.search(rf"^\w+=\$\(bashio::config '{key}'\)", RUN_SH, re.M), key


def test_every_option_has_a_schema_entry():
    assert set(CONFIG["options"]) <= set(CONFIG["schema"])


def test_announcement_style_is_configured_and_exported():
    assert CONFIG["options"]["announcement_style"] == "faithful"
    assert CONFIG["schema"]["announcement_style"] == "list(creative|faithful|verbatim)"
    assert "ANNOUNCEMENT_STYLE=$(bashio::config 'announcement_style')" in RUN_SH
    assert re.search(r"^export ANNOUNCEMENT_STYLE$", RUN_SH, re.M)


def test_dnd_hold_is_configured_and_exported():
    assert CONFIG["options"]["dnd_hold_minutes"] == 10
    assert CONFIG["schema"]["dnd_hold_minutes"] == "int(0,60)"
    assert "DND_HOLD_MINUTES=$(bashio::config 'dnd_hold_minutes')" in RUN_SH
    assert re.search(r"^export DND_HOLD_MINUTES$", RUN_SH, re.M)

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

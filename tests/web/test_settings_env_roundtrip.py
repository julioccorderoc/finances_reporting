"""The environment is the ONLY channel into a reload child, so it must be total.

Under the reload supervisor the app runs in a ``spawn()``-ed interpreter:
nothing ``serve_cmd`` built in the parent survives, and the child rebuilds
``WebSettings`` from ``os.environ`` alone. Two things follow.

First, a field that ``to_env`` forgets does not raise — it silently reverts
to its default in the child. For ``db_path`` that means serving a different
ledger than the one the owner asked for.

Second, and worse, the dangerous default is ``host="127.0.0.1"``: the bearer
middleware short-circuits on exactly that value, so a child that falls back
to defaults would serve an unauthenticated viewer on a socket its parent
bound to the LAN. ``from_env`` therefore fails CLOSED — it refuses to
construct anything at all unless the parent's handshake and every required
key are present.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from finances.web.settings import (
    ENV_DB_PATH,
    ENV_HOST,
    ENV_PORT,
    ENV_REGEN,
    ENV_RELOAD_CHILD,
    ENV_TOKEN,
    _FIELD_TO_ENV,
    WebSettings,
)


def _settings(tmp_path: Path, **overrides: object) -> WebSettings:
    base: dict[str, object] = {
        "host": "127.0.0.1",
        "port": 19999,
        "token": "s3cret",
        "db_path": tmp_path / "x.db",
        "regen_report_on_shutdown": False,
    }
    base.update(overrides)
    return WebSettings(**base)  # type: ignore[arg-type]


def test_to_env_from_env_preserves_every_field(tmp_path: Path) -> None:
    settings = _settings(tmp_path)

    assert WebSettings.from_env(settings.to_env()) == settings


def test_every_model_field_has_an_env_key() -> None:
    """The real regression risk: a new field nobody added to ``to_env``.

    It would not fail anywhere — the child would just quietly run with the
    default. This reflective guard is what makes that a red test instead.
    """
    assert set(WebSettings.model_fields) == set(_FIELD_TO_ENV)


def test_the_mapping_is_what_to_env_actually_emits() -> None:
    """Guard the mapping against becoming a mirror nothing executes.

    ``model_fields == _FIELD_TO_ENV`` only proves the mapping is complete.
    If ``to_env`` kept its own hardcoded dict, a field could be added to
    the mapping — satisfying the test above — and still never be exported.
    Both sides must be pinned to the same source.
    """
    emitted = set(WebSettings().to_env())

    assert emitted == set(_FIELD_TO_ENV.values()) | {ENV_RELOAD_CHILD}


def test_a_fully_non_default_settings_object_survives_the_round_trip(
    tmp_path: Path,
) -> None:
    """Every field carries a value it could not have gotten by defaulting.

    A field dropped anywhere along the wire — ``_FIELD_TO_ENV``, the
    encoder, the required-key check, the decoder — reverts to its default,
    which is exactly what a same-as-default fixture cannot see.
    """
    defaults = WebSettings()
    settings = _settings(
        tmp_path, host="127.0.0.1", port=24601, token="not-the-default"
    )

    restored = WebSettings.from_env(settings.to_env())

    assert restored == settings
    for field in WebSettings.model_fields:
        if field == "host":  # the reload child is localhost-only by rule
            continue
        assert getattr(restored, field) != getattr(defaults, field), (
            f"{field} round-tripped to its default — the fixture cannot "
            "distinguish a carried value from a dropped one"
        )


def test_from_env_refuses_without_the_handshake() -> None:
    with pytest.raises(RuntimeError, match=ENV_RELOAD_CHILD):
        WebSettings.from_env({})


def test_from_env_names_the_missing_keys(tmp_path: Path) -> None:
    env = _settings(tmp_path).to_env()
    del env[ENV_DB_PATH]

    with pytest.raises(RuntimeError, match=ENV_DB_PATH):
        WebSettings.from_env(env)


def test_from_env_rejects_a_non_localhost_host(tmp_path: Path) -> None:
    """Reload is localhost-only by construction, not by convention."""
    env = _settings(tmp_path).to_env()
    env[ENV_HOST] = "0.0.0.0"

    with pytest.raises(RuntimeError, match="localhost"):
        WebSettings.from_env(env)


def test_empty_token_round_trips_to_none(tmp_path: Path) -> None:
    settings = _settings(tmp_path, token=None)

    assert settings.to_env()[ENV_TOKEN] == ""
    assert WebSettings.from_env(settings.to_env()).token is None


def test_regen_flag_round_trips_both_ways(tmp_path: Path) -> None:
    on = _settings(tmp_path, regen_report_on_shutdown=True)
    off = _settings(tmp_path, regen_report_on_shutdown=False)

    assert on.to_env()[ENV_REGEN] == "1"
    assert off.to_env()[ENV_REGEN] == "0"
    assert WebSettings.from_env(on.to_env()).regen_report_on_shutdown is True
    assert WebSettings.from_env(off.to_env()).regen_report_on_shutdown is False


def test_port_round_trips_as_an_int(tmp_path: Path) -> None:
    env = _settings(tmp_path, port=18765).to_env()

    assert env[ENV_PORT] == "18765"
    assert WebSettings.from_env(env).port == 18765


def test_regen_defaults_on_so_the_no_reload_path_is_unchanged() -> None:
    """``--no-reload`` must behave exactly as the viewer always has."""
    assert WebSettings().regen_report_on_shutdown is True

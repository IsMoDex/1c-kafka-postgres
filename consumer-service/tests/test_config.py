from __future__ import annotations

import pytest

from consumer.config import Config

_ENV_NAMES = {
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_CONSUMER_GROUP",
    "KAFKA_TOPICS",
    "KAFKA_DLQ_SUFFIX",
    "PG_DSN",
    "MAX_RETRIES",
    "BATCH_MAX_MESSAGES",
    "BATCH_MAX_SECONDS",
    "HEALTH_PORT",
}


@pytest.fixture(autouse=True)
def _clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_defaults_are_valid() -> None:
    config = Config.from_env()
    assert config.max_retries == 3
    assert config.batch_max_messages == 500


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("KAFKA_BOOTSTRAP_SERVERS", " "),
        ("KAFKA_CONSUMER_GROUP", ""),
        ("KAFKA_TOPICS", " , "),
        ("KAFKA_DLQ_SUFFIX", " "),
        ("PG_DSN", " "),
        ("MAX_RETRIES", "-1"),
        ("BATCH_MAX_MESSAGES", "0"),
        ("BATCH_MAX_SECONDS", "0"),
        ("BATCH_MAX_SECONDS", "nan"),
        ("HEALTH_PORT", "65536"),
    ],
)
def test_invalid_values_fail_fast(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        Config.from_env()


def test_topics_are_trimmed_and_unique(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_TOPICS", " first , second ")
    assert Config.from_env().topics == ["first", "second"]

    monkeypatch.setenv("KAFKA_TOPICS", "first,first")
    with pytest.raises(ValueError, match="duplicates"):
        Config.from_env()

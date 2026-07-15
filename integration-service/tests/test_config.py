from __future__ import annotations

import pytest

from integration.config import Config

_ENV_NAMES = {
    "KAFKA_BOOTSTRAP_SERVERS",
    "TOPIC_OWNERSHIP_FORMS",
    "TOPIC_COUNTERPARTIES",
    "PG_DSN",
    "SOURCE_TYPE",
    "ONEC_BASE_URL",
    "ONEC_USERNAME",
    "ONEC_PASSWORD",
    "ONEC_TIMEOUT",
    "ONEC_VERIFY_SSL",
    "ONEC_HTTP_RETRIES",
    "ONEC_PAGE_SIZE",
    "FK_BARRIER_TIMEOUT",
}


@pytest.fixture(autouse=True)
def _clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _ENV_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_defaults_are_valid_for_mock() -> None:
    config = Config.from_env()
    assert config.source_type == "mock"
    assert config.onec_page_size == 500


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("KAFKA_BOOTSTRAP_SERVERS", " "),
        ("ONEC_TIMEOUT", "0"),
        ("ONEC_TIMEOUT", "nan"),
        ("ONEC_HTTP_RETRIES", "-1"),
        ("ONEC_PAGE_SIZE", "5001"),
        ("FK_BARRIER_TIMEOUT", "-1"),
        ("ONEC_VERIFY_SSL", "1"),
    ],
)
def test_invalid_values_fail_fast(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        Config.from_env()


def test_onec_requires_real_absolute_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOURCE_TYPE", " onec ")
    with pytest.raises(ValueError, match="ONEC_BASE_URL"):
        Config.from_env()

    monkeypatch.setenv("ONEC_BASE_URL", "https://onec.example.test/hs/integration")
    assert Config.from_env().source_type == "onec"


def test_topics_must_be_different(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TOPIC_COUNTERPARTIES", "1c.ownership_forms.v1")
    with pytest.raises(ValueError, match="must be different"):
        Config.from_env()

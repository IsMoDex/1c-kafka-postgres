"""Retry и pagination HTTP-источника 1С."""

from __future__ import annotations

from unittest.mock import Mock

import httpx
import pytest

from integration.sources.onec_http import OneCHttpSource


def test_retryable_http_error_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    request = httpx.Request("GET", "http://onec.test/counterparties")
    client = Mock()
    client.get.side_effect = [
        httpx.Response(503, request=request),
        httpx.Response(200, request=request, json=[]),
    ]
    source = object.__new__(OneCHttpSource)
    source._client = client
    source._retries = 1
    monkeypatch.setattr("integration.sources.onec_http.time.sleep", lambda _: None)

    assert source._get("/counterparties") == []
    assert client.get.call_count == 2


def test_client_error_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    request = httpx.Request("GET", "http://onec.test/counterparties")
    client = Mock()
    client.get.return_value = httpx.Response(400, request=request)
    source = object.__new__(OneCHttpSource)
    source._client = client
    source._retries = 3
    sleep = Mock()
    monkeypatch.setattr("integration.sources.onec_http.time.sleep", sleep)

    with pytest.raises(httpx.HTTPStatusError):
        source._get("/counterparties")

    client.get.assert_called_once()
    sleep.assert_not_called()


def test_transport_error_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    request = httpx.Request("GET", "http://onec.test/counterparties")
    client = Mock()
    client.get.side_effect = [
        httpx.ConnectError("offline", request=request),
        httpx.Response(200, request=request, json=[]),
    ]
    source = object.__new__(OneCHttpSource)
    source._client = client
    source._retries = 1
    monkeypatch.setattr("integration.sources.onec_http.time.sleep", lambda _: None)

    assert source._get("/counterparties") == []
    assert client.get.call_count == 2


def test_get_all_reads_pages_until_short_page() -> None:
    source = object.__new__(OneCHttpSource)
    source._page_size = 1
    source._get = Mock(side_effect=[[{"id": "a"}], [{"id": "b"}], []])

    assert source._get_all("/ownership-forms", None) == [{"id": "a"}, {"id": "b"}]
    assert source._get.call_count == 3
    assert source._get.call_args_list[1].args[1]["offset"] == 1

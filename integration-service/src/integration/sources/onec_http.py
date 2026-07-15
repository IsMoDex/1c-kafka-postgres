"""
HTTP-источник данных реальной 1С.

Ожидает собственный HTTP-сервис 1С (Вариант Б ТЗ), отдающий JSON:
    GET {base}/ownership-forms
    GET {base}/counterparties
    GET {base}/counterparties?changed_since=<RFC3339>

Формат элементов ответа совпадает с payload события (см. models.py и docs/).
Базовая аутентификация 1С — через ONEC_USERNAME / ONEC_PASSWORD.

OData-вариант (Вариант А ТЗ) описан в docs/architecture.md как альтернатива;
при необходимости подключается отдельной реализацией Source без изменения
остального кода.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import httpx

from integration.models import Counterparty, OwnershipForm
from integration.sources.base import Source

if TYPE_CHECKING:
    from datetime import datetime

HTTP_TOO_MANY_REQUESTS = 429
HTTP_SERVER_ERROR = 500


class OneCHttpSource(Source):
    def __init__(  # noqa: PLR0913 - HTTP connection settings are intentionally explicit.
        self,
        base_url: str,
        username: str = "",
        password: str = "",
        timeout: float = 30.0,
        verify_ssl: bool = True,  # noqa: FBT001, FBT002 - Preserve the existing positional API.
        retries: int = 3,
        page_size: int = 500,
    ) -> None:
        self._retries = max(0, retries)
        self._page_size = min(5000, max(1, page_size))
        auth = (username, password) if username else None
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            auth=auth,
            timeout=timeout,
            verify=verify_ssl,
            headers={"Accept": "application/json"},
        )

    def _get(self, path: str, params: dict | None = None) -> list[dict]:
        for attempt in range(self._retries + 1):
            try:
                resp = self._client.get(path, params=params)
                resp.raise_for_status()
                data = resp.json()
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                retryable = isinstance(exc, httpx.TransportError) or (
                    exc.response.status_code == HTTP_TOO_MANY_REQUESTS or exc.response.status_code >= HTTP_SERVER_ERROR
                )
                if not retryable or attempt >= self._retries:
                    raise
                time.sleep(min(2**attempt, 5))
            else:
                if not isinstance(data, list):
                    message = f"Ожидался JSON-массив от 1С по пути {path}, получено: {type(data)}"
                    raise ValueError(message)  # noqa: TRY004 - Invalid response content is a value error.
                return data
        message = "Недостижимая ветка HTTP retry"
        raise RuntimeError(message)

    def _get_all(self, path: str, changed_since: datetime | None) -> list[dict]:
        result: list[dict] = []
        offset = 0
        while True:
            params: dict[str, object] = {"limit": self._page_size, "offset": offset}
            if changed_since:
                params["changed_since"] = changed_since.isoformat()
            page = self._get(path, params)
            result.extend(page)
            if len(page) < self._page_size:
                return result
            offset += len(page)

    def fetch_ownership_forms(self, changed_since: datetime | None = None) -> list[OwnershipForm]:
        return [OwnershipForm(**r) for r in self._get_all("/ownership-forms", changed_since)]

    def fetch_counterparties(self, changed_since: datetime | None = None) -> list[Counterparty]:
        return [Counterparty(**r) for r in self._get_all("/counterparties", changed_since)]

    def close(self) -> None:
        self._client.close()

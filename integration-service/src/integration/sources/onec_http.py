"""HTTP-источник данных реальной 1С.

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
from datetime import datetime
from typing import Optional

import httpx

from integration.models import Counterparty, OwnershipForm
from integration.sources.base import Source


class OneCHttpSource(Source):
    def __init__(
        self,
        base_url: str,
        username: str = "",
        password: str = "",
        timeout: float = 30.0,
        verify_ssl: bool = True,
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

    def _get(self, path: str, params: Optional[dict] = None) -> list[dict]:
        for attempt in range(self._retries + 1):
            try:
                resp = self._client.get(path, params=params)
                if resp.status_code == 429 or resp.status_code >= 500:
                    resp.raise_for_status()
                resp.raise_for_status()
                data = resp.json()
                if not isinstance(data, list):
                    raise ValueError(
                        f"Ожидался JSON-массив от 1С по пути {path}, получено: {type(data)}"
                    )
                return data
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                retryable = isinstance(exc, httpx.TransportError) or (
                    exc.response.status_code == 429 or exc.response.status_code >= 500
                )
                if not retryable or attempt >= self._retries:
                    raise
                time.sleep(min(2 ** attempt, 5))
        raise RuntimeError("Недостижимая ветка HTTP retry")

    def _get_all(self, path: str, changed_since: Optional[datetime]) -> list[dict]:
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

    def fetch_ownership_forms(
        self, changed_since: Optional[datetime] = None
    ) -> list[OwnershipForm]:
        return [OwnershipForm(**r) for r in self._get_all("/ownership-forms", changed_since)]

    def fetch_counterparties(
        self, changed_since: Optional[datetime] = None
    ) -> list[Counterparty]:
        return [Counterparty(**r) for r in self._get_all("/counterparties", changed_since)]

    def close(self) -> None:
        self._client.close()

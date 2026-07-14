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
    ) -> None:
        auth = (username, password) if username else None
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            auth=auth,
            timeout=timeout,
            verify=verify_ssl,
            headers={"Accept": "application/json"},
        )

    def _get(self, path: str, params: Optional[dict] = None) -> list[dict]:
        resp = self._client.get(path, params=params)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, list):
            raise ValueError(f"Ожидался JSON-массив от 1С по пути {path}, получено: {type(data)}")
        return data

    def fetch_ownership_forms(
        self, changed_since: Optional[datetime] = None
    ) -> list[OwnershipForm]:
        params = {"changed_since": changed_since.isoformat()} if changed_since else None
        return [OwnershipForm(**r) for r in self._get("/ownership-forms", params)]

    def fetch_counterparties(
        self, changed_since: Optional[datetime] = None
    ) -> list[Counterparty]:
        params = {"changed_since": changed_since.isoformat()} if changed_since else None
        return [Counterparty(**r) for r in self._get("/counterparties", params)]

    def close(self) -> None:
        self._client.close()

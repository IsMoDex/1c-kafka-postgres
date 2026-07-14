"""Абстракция источника данных 1С.

Интерфейс ``Source`` позволяет integration-service работать одинаково
с реальной 1С (``OneCHttpSource``) и с воспроизводимым mock (``MockSource``).
Это гарантирует, что контур Kafka → PostgreSQL демонстрируется независимо
от доступности 1С.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

from integration.models import Counterparty, OwnershipForm


class Source(ABC):
    """Источник справочных данных 1С."""

    @abstractmethod
    def fetch_ownership_forms(
        self, changed_since: Optional[datetime] = None
    ) -> list[OwnershipForm]:
        """Формы собственности. Если задан changed_since — только изменённые."""
        raise NotImplementedError

    @abstractmethod
    def fetch_counterparties(
        self, changed_since: Optional[datetime] = None
    ) -> list[Counterparty]:
        """Контрагенты. Если задан changed_since — только изменённые."""
        raise NotImplementedError

    def close(self) -> None:  # noqa: B027 — необязательный хук
        """Освобождение ресурсов (по умолчанию ничего не делает)."""

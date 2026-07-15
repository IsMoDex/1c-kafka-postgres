"""
Mock-источник, имитирующий HTTP/OData API 1С.

Назначение: воспроизводимая демонстрация всего контура Kafka → PostgreSQL
без запущенной 1С (разрешено ТЗ). Отдаёт те же данные и семантику, что и
реальный HTTP-сервис 1С, включая:
  * changed_since (инкрементальная выборка по updated_at);
  * мягкое удаление (deleted=true);
  * стабильные идентификаторы (id формы = код, id контрагента = GUID).

Состояние хранится в JSON-файле (SEED_STATE_PATH), чтобы между запусками
CLI можно было имитировать «изменение контрагента» и «пометку удаления»
для демо-сценария из раздела 11 ТЗ.
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

from integration.models import Counterparty, OwnershipForm
from integration.sources.base import Source

# Базовый seed демо-данных: 4 формы собственности, 5 контрагентов.
_BASE_TS = "2026-07-10T12:00:00+00:00"

_SEED = {
    "ownership_forms": [
        {"id": "ooo", "code": "000000001", "name": "ООО", "deleted": False, "updated_at": _BASE_TS},
        {"id": "ip", "code": "000000002", "name": "ИП", "deleted": False, "updated_at": _BASE_TS},
        {"id": "ao", "code": "000000003", "name": "АО", "deleted": False, "updated_at": _BASE_TS},
        {"id": "pao", "code": "000000004", "name": "ПАО", "deleted": False, "updated_at": _BASE_TS},
    ],
    "counterparties": [
        {
            "id": "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001",
            "code": "000001",
            "name": "ООО Ромашка",
            "inn": "7701234567",
            "kpp": "770101001",
            "ownership_form_id": "ooo",
            "deleted": False,
            "updated_at": _BASE_TS,
        },
        {
            "id": "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0002",
            "code": "000002",
            "name": "ИП Иванов И.И.",
            "inn": "770212345678",
            "kpp": None,
            "ownership_form_id": "ip",
            "deleted": False,
            "updated_at": _BASE_TS,
        },
        {
            "id": "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0003",
            "code": "000003",
            "name": "АО Север",
            "inn": "7803001122",
            "kpp": "780301001",
            "ownership_form_id": "ao",
            "deleted": False,
            "updated_at": _BASE_TS,
        },
        {
            "id": "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0004",
            "code": "000004",
            "name": "ПАО Энергия",
            "inn": "7704556677",
            "kpp": "770401001",
            "ownership_form_id": "pao",
            "deleted": False,
            "updated_at": _BASE_TS,
        },
        {
            "id": "b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0005",
            "code": "000005",
            "name": "ООО Вектор",
            "inn": "5001889900",
            "kpp": "500101001",
            "ownership_form_id": "ooo",
            "deleted": False,
            "updated_at": _BASE_TS,
        },
    ],
}


def _parse_dt(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    return datetime.fromisoformat(value)


class MockSource(Source):
    """Файловый mock источника 1С с поддержкой changed_since."""

    def __init__(self, state_path: str | None = None) -> None:
        self._path = Path(state_path or os.getenv("SEED_STATE_PATH", "/data/mock_state.json"))
        self._state = self._load()

    # ── загрузка/сохранение состояния ────────────────────────────────────
    def _load(self) -> dict[str, list[dict[str, object]]]:
        if self._path.exists():
            return json.loads(self._path.read_text(encoding="utf-8"))
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(_SEED, ensure_ascii=False, indent=2), encoding="utf-8")
        return json.loads(json.dumps(_SEED))

    def _save(self) -> None:
        self._path.write_text(json.dumps(self._state, ensure_ascii=False, indent=2), encoding="utf-8")

    # ── реализация интерфейса Source ─────────────────────────────────────
    def fetch_ownership_forms(self, changed_since: datetime | None = None) -> list[OwnershipForm]:
        rows = self._state["ownership_forms"]
        rows = self._filter_changed(rows, changed_since)
        return [OwnershipForm.model_validate(r) for r in rows]

    def fetch_counterparties(self, changed_since: datetime | None = None) -> list[Counterparty]:
        rows = self._state["counterparties"]
        rows = self._filter_changed(rows, changed_since)
        return [Counterparty.model_validate(r) for r in rows]

    @staticmethod
    def _filter_changed(
        rows: list[dict[str, object]],
        changed_since: datetime | None,
    ) -> list[dict[str, object]]:
        if changed_since is None:
            return rows
        out = []
        for r in rows:
            dt = _parse_dt(r.get("updated_at"))
            if dt is not None and dt > changed_since:
                out.append(r)
        return out

    # ── помощники для демо-сценария (мутации состояния) ──────────────────
    def touch_counterparty(self, cp_id: str, **changes: object) -> None:
        """Изменить контрагента и обновить updated_at (для demo incremental)."""
        now = datetime.now(UTC).isoformat()
        for r in self._state["counterparties"]:
            if r["id"] == cp_id:
                r.update(changes)
                r["updated_at"] = now
                break
        self._save()

    def soft_delete_counterparty(self, cp_id: str) -> None:
        """Пометить контрагента удалённым (deleted=true) с новым updated_at."""
        self.touch_counterparty(cp_id, deleted=True)

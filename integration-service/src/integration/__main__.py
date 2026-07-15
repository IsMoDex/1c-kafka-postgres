"""
CLI integration-service (typer).

Команды:
  sync full           — полная синхронизация
  sync incremental    — инкрементальная синхронизация
  demo touch <id>     — изменить контрагента в mock (для demo incremental)
  demo delete <id>    — пометить контрагента удалённым в mock

Примеры:
  python -m integration sync full
  python -m integration sync incremental
  docker compose exec integration-service python -m integration sync full
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import typer

from integration.config import Config
from integration.logging_setup import configure_logging
from integration.sources.mock import MockSource
from integration.sources.onec_http import OneCHttpSource
from integration.sync import Synchronizer

if TYPE_CHECKING:
    from integration.sources.base import Source

app = typer.Typer(add_completion=False, help="Producer 1С → Kafka")
sync_app = typer.Typer(help="Синхронизация справочников")
demo_app = typer.Typer(help="Помощники демо-сценария (только для mock-источника)")
app.add_typer(sync_app, name="sync")
app.add_typer(demo_app, name="demo")

log = configure_logging("integration-service")


def _build_source(cfg: Config) -> Source:
    if cfg.source_type == "mock":
        log.info("source_selected", type="mock")
        return MockSource()
    if cfg.source_type == "onec":
        if "HOST_IPV4_NOT_SET" in cfg.onec_base_url or "<HOST_IPV4>" in cfg.onec_base_url:
            message = (
                "Для SOURCE_TYPE=onec замените placeholder в ONEC_BASE_URL. "
                "Используйте http://host.docker.internal/roshim/hs/integration; "
                "если Docker Desktop возвращает 502, укажите реальный IPv4 хоста, "
                "например http://172.23.128.1/roshim/hs/integration."
            )
            raise typer.BadParameter(message)
        log.info("source_selected", type="onec", base_url=cfg.onec_base_url)
        return OneCHttpSource(
            base_url=cfg.onec_base_url,
            username=cfg.onec_username,
            password=cfg.onec_password,
            timeout=cfg.onec_timeout,
            verify_ssl=cfg.onec_verify_ssl,
            retries=cfg.onec_http_retries,
            page_size=cfg.onec_page_size,
        )
    message = f"Неизвестный SOURCE_TYPE={cfg.source_type!r} (ожидается mock|onec)"
    raise typer.BadParameter(message)


def _run(mode: str) -> None:
    cfg = Config.from_env()
    source = _build_source(cfg)
    try:
        Synchronizer(cfg, source).run(mode)
    except Exception as exc:
        log.exception("sync_error", mode=mode, error=str(exc))
        source.close()
        raise typer.Exit(code=1) from None
    source.close()


@sync_app.command("full")
def sync_full() -> None:
    """Полная синхронизация: выгрузить все записи справочников."""
    _run("full")


@sync_app.command("incremental")
def sync_incremental() -> None:
    """Инкрементальная синхронизация: только изменённые записи (по watermark)."""
    _run("incremental")


@demo_app.command("touch")
def demo_touch(
    cp_id: str = typer.Argument(..., help="GUID контрагента"),
    name: str | None = typer.Option(None, help="Новое наименование"),
) -> None:
    """Изменить контрагента в mock-источнике (обновляет updated_at)."""
    src = MockSource()
    changes = {"name": name} if name else {}
    src.touch_counterparty(cp_id, **changes)
    log.info("demo_touch", id=cp_id, changes=changes)


@demo_app.command("delete")
def demo_delete(cp_id: str = typer.Argument(..., help="GUID контрагента")) -> None:
    """Пометить контрагента удалённым (deleted=true) в mock-источнике."""
    src = MockSource()
    src.soft_delete_counterparty(cp_id)
    log.info("demo_delete", id=cp_id)


if __name__ == "__main__":
    app()

# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:0.11.17 AS uv

FROM python:3.12-slim-bookworm AS build

COPY --from=uv /uv /uvx /bin/

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

FROM build AS test

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen
COPY tests ./tests

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SEED_STATE_PATH=/data/mock_state.json
RUN mkdir -p /data

ENTRYPOINT ["python", "-m", "integration"]
CMD ["--help"]

FROM python:3.12-slim-bookworm AS prod

RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka1 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system app \
    && useradd --system --gid app --create-home app \
    && mkdir -p /data \
    && chown app:app /data

WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SEED_STATE_PATH=/data/mock_state.json

COPY --from=build --chown=app:app /app/.venv /app/.venv
COPY --from=build --chown=app:app /app/src /app/src

USER app
ENTRYPOINT ["python", "-m", "integration"]
CMD ["--help"]

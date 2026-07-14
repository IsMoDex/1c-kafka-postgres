# syntax=docker/dockerfile:1
FROM python:3.12-slim

# librdkafka runtime для confluent-kafka
RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src
COPY tests ./tests

RUN pip install --upgrade pip && pip install ".[test]"

# состояние mock-источника (volume в compose при необходимости)
RUN mkdir -p /data
ENV SEED_STATE_PATH=/data/mock_state.json

# контейнер держится живым (sleep infinity в compose); синхронизацию
# запускаем через `docker compose exec ... python -m integration sync ...`
ENTRYPOINT ["python", "-m", "integration"]
CMD ["--help"]

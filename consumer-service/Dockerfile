# syntax=docker/dockerfile:1
FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src

RUN pip install --upgrade pip && pip install .

EXPOSE 8081

ENTRYPOINT ["python", "-m", "consumer"]

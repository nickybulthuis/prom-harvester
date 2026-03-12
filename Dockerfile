# ---------------------------------------------------------------------------
# Stage 1 — build a virtual environment from pyproject.toml
# ---------------------------------------------------------------------------
FROM python:3.13-slim AS builder

WORKDIR /build

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy dependency manifest and source
COPY pyproject.toml .
COPY src/ src/

# Create venv, install all production dependencies and the application.
RUN uv venv /venv \
 && uv pip install --python /venv/bin/python .

# ---------------------------------------------------------------------------
# Stage 2 — minimal runtime image
# ---------------------------------------------------------------------------
FROM python:3.13-slim

RUN groupadd --system harvester \
 && useradd --system --gid harvester --no-create-home harvester

COPY --from=builder /venv /venv

ENV PATH="/venv/bin:$PATH"
ENV HARVESTER_CONFIGURATION_FILE=/data/configuration.yaml
ENV HARVESTER_APP_HOST=0.0.0.0

EXPOSE 9000

USER harvester

ENTRYPOINT ["python", "-m", "harvester"]

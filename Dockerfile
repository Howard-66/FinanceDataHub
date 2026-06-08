ARG FDH_IMAGE_PLATFORM=linux/amd64
FROM --platform=${FDH_IMAGE_PLATFORM} ghcr.io/astral-sh/uv:python3.11-bookworm-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_SYSTEM_PYTHON=1 \
    TZ=Asia/Shanghai

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libgomp1 \
        libstdc++6 \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock README.md ./
COPY finance_data_hub ./finance_data_hub
COPY industry_config.json ./industry_config.json
COPY scripts/check_akshare_hk_runtime.py ./scripts/check_akshare_hk_runtime.py

RUN uv pip install --system --no-cache .
RUN python scripts/check_akshare_hk_runtime.py

CMD ["fdh-cli", "schedule", "start", "--config", "/app/schedules.yml"]

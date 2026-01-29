FROM mcr.microsoft.com/playwright/python:v1.34.0-jammy

# Системные deps (только curl, остальное в образе)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# uv как бинарник (быстрее pip, меньше размер)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app

# Только pyproject.toml + lock сначала (кэш deps)
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-install-isolated

# Копируем КОД ПОСЛЕ deps (кэш сохраняется)
COPY . .

# Браузеры Playwright (образ имеет, но гарантия)
RUN playwright install chromium

# Директории
RUN mkdir -p output cache cookies logs screenshots input temp \
    && chmod -R 777 output cache cookies logs screenshots input temp

# ENV в docker-compose
ENV PYTHONUNBUFFERED=1
ENV TZ=Europe/Moscow

# uv run (editable support)
CMD ["uv", "run", "bot.py"]

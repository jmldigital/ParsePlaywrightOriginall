FROM mcr.microsoft.com/playwright/python:v1.34.0-jammy

# Установка uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Настройки UV
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    TZ=Europe/Moscow \
    UV_PYTHON_DOWNLOADS=never \
    UV_PYTHON=python3

WORKDIR /app

# Системные пакеты
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN /app/.venv/bin/pip install ImageHash==4.3.2 browserforge==1.0.0

# 1. Копируем конфиг
COPY pyproject.toml uv.lock* ./

# 2. Установка зависимостей (ТОЛЬКО БИБЛИОТЕКИ)
RUN uv sync --no-install-project --no-dev

# 3. Копируем код
COPY . .

# 4. Финальная проверка (без установки самого проекта)
RUN uv sync --no-dev --no-install-project

# Папки
RUN mkdir -p output cache cookies logs input temp \
    && chmod -R 777 output cache cookies logs input temp

# 🔥 ИСПРАВЛЕННАЯ СТРОКА ЗАПУСКА:
CMD ["/app/.venv/bin/python", "bot.py"]
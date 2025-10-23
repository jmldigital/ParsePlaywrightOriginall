# Используем официальный образ Playwright с Python 3.11
FROM mcr.microsoft.com/playwright/python:v1.34.0-jammy

# Установка дополнительных системных зависимостей (если нужны)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Установка uv для быстрой установки пакетов
RUN pip install --no-cache-dir uv

# Рабочая директория
WORKDIR /app

# Копируем файлы проекта
COPY pyproject.toml .
COPY . .

# Устанавливаем зависимости через uv
RUN uv pip install --system -r pyproject.toml

# Создаём необходимые директории
RUN mkdir -p output cache cookies logs screenshots input temp

# Переменные окружения (опционально, можно задать в docker-compose.yml)
ENV TZ=Europe/Moscow
ENV PYTHONUNBUFFERED=1

# Точка входа — bot.py
CMD ["python", "bot.py"]

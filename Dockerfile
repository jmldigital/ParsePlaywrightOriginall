FROM python:3.11-slim

# Установка системных зависимостей для Playwright и Chrome
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    unzip \
    curl \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxss1 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpangocairo-1.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    && rm -rf /var/lib/apt/lists/*

# Установка uv (ультрабыстрый менеджер пакетов)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:$PATH"

# Рабочая директория
WORKDIR /app

# Копируем файлы проекта
COPY pyproject.toml .
COPY . .

# Устанавливаем зависимости через uv
RUN uv pip install --system -r pyproject.toml

# Устанавливаем Playwright браузеры
RUN playwright install chromium
RUN playwright install-deps chromium

# Создаём необходимые директории
RUN mkdir -p output cache cookies logs screenshots input temp

# Точка входа
CMD ["python", "bot.py"]

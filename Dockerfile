FROM apache/airflow:2.10.4-python3.12

USER root

# Установка системных зависимостей для Playwright
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev \
    # Зависимости для Playwright/Chromium
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxcb1 \
    libxkbcommon0 \
    libx11-6 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libpango-1.0-0 \
    libcairo2 \
    libasound2 \
    libatspi2.0-0 \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
    clickhouse-driver \
    python-dotenv \
    aiohttp \
    playwright \
    sofascore_wrapper  # ← ДОБАВЛЕНО

# Установка браузера и зависимостей
RUN playwright install chromium
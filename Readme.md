# 🚀 Price Parser (Crawlee + Playwright)

Парсер автозапчастей с поддержкой:

* 💰 Парсинга цен (Yumbo)
* ⚖️ Парсинга весов (Japarts → Armtek fallback)
* 📦 Батч-обработки (до десятков тысяч строк)
* ⚡ Асинхронного выполнения (Crawlee + Playwright)

❗ Telegram отключён (`SEND_TO_TELEGRAM = False`)
Поэтмоу парсер запускается вручную внутри Docker-контейнера.
Предварительно загружаем файл "наличие.xlsx" на сервер в папку /home/ParsBotCrawly/input/
Названия обязательныех колонок: 1 - номер, 3 - Бренд (для поиска или цен ямбо нужен только номер(1) и бренд(3))
---



# 📌 1. Общая архитектура

## 🧠 Компоненты системы

```
Excel (input)
      ↓
preprocess_dataframe
      ↓
ParserCrawler (Crawlee)
      ↓
┌───────────────┬───────────────┐
│   ЦЕНЫ        │    ВЕСА       │
│ main-yambo    │ main.py       │
└──────┬────────┴───────┬───────┘
       ↓                ↓
   Yumbo parser     Japarts → Armtek
       ↓                ↓
       └──────→ DataFrame ←──────┘
                      ↓
          batch finalize / save
                      ↓
               output/*.xlsx
```

---

# 🔄 2. Pipeline (пошагово)

## 📊 Общий пайплайн

```
[1] Загрузка Excel 
        ↓
[2] Очистка и нормализация
        ↓
[3] Разбиение на батчи (BATCH_SIZE)
        ↓
[4] Crawlee обработка (async)
        ↓
[5] Парсинг сайта
        ↓
[6] Сохранение в DataFrame
        ↓
[7] Промежуточное сохранение
        ↓
[8] Финализация (Excel)
```

---

## ⚖️ Pipeline весов

```
Batch
  ↓
Japarts (быстро)
  ↓
если нет → Armtek (fallback)
  ↓
консолидация весов
```

👉 Логика Japarts: 
👉 Логика Armtek API: 

---

## 💰 Pipeline цен

```
Batch
  ↓
Yumbo (один сайт)
  ↓
поиск по partNo
  ↓
фильтр по бренду
  ↓
извлечение цены
```

👉 Основной парсер: 

---

# ⚙️ 3. Потоки и concurrency (Crawlee)

## 🧵 Модель выполнения

```
Main Event Loop (asyncio)
        ↓
PlaywrightCrawler
        ↓
┌───────────────┬───────────────┬───────────────┐
│ Worker 1      │ Worker 2      │ Worker N      │
│ page instance │ page instance │ page instance │
└───────────────┴───────────────┴───────────────┘
```

---

## 🔁 Обработка одного элемента

```
Request → request_handler()
        ↓
pause check (RateLimit)
        ↓
парсер (site-specific)
        ↓
save_result (lock)
```

---

## 🔒 Синхронизация

* `asyncio.Lock()` → запись в DataFrame
* `asyncio.Event()` → глобальная пауза
* `SessionPool` → reuse сессий

---

# 📂 4. Структура проекта

```
app/
├── main.py                  # веса (Japarts + Armtek)
├── main-yambo.py           # цены (Yumbo)
├── config.py               # конфиг
├── utils.py                # утилиты
├── scraper_japarts_pure.py
├── yumbo_parse_price.py
├── armtek_pure_aiohtpp.py
├── price_adjuster.py
├── captcha_manager.py
├── state_manager.py
├── cache_manager.py
├── input/
├── output/
├── logs/
└── storage/
```

---

# 📥 5. Входные данные

Файл:

```
input/наличие.xlsx
```

Обязательные колонки:

| Колонка | Значение |
| ------- | -------- |
| `1`     | Артикул  |
| `3`     | Бренд    |

👉 Настройки: 

---

# 📤 6. Выходные данные

```
output/
├── цены_конкурентов.xlsx
├── веса_деталей.xlsx
├── batch_finalize.xlsx
```

---

# ▶️ 7. Запуск

## 💰 Цены

```
docker exec -it price-parser-bot-crowly /app/.venv/bin/python /app/main-yambo.py
```

## ⚖️ Веса

```
docker exec -it price-parser-bot-crowly /app/.venv/bin/python /app/main.py
```

---

# 🐳 8. Docker

## 📦 docker-compose

👉 Конфиг: 

### Особенности:

* ограничение CPU / RAM
* shm_size = 4GB (для Playwright)
* volumes:

  * input/output/logs
  * cache/cookies

---

## 🔨 Сборка контейнера

### 1. Build

```
docker-compose build
```

или:

```
docker build -t price-parser .
```

---

### 2. Запуск

```
docker-compose up -d
```

---

### 3. Проверка

```
docker ps
```

---

# 📦 9. Зависимости

👉 requirements: 

Основные:

* crawlee
* playwright
* pandas
* aiohttp
* 2captcha (не участвует в этом парсере)

---

# ⚙️ 10. Конфигурация

Файл:

```
config.py
```

Ключевые параметры:

```python
BATCH_SIZE = 500 (сохраняем промежуточно каждые BATCH_SIZE строк, файл batch_finalize.xlsx)
MAX_ROWS = 35000

ENABLE_PRICE_PARSING = True
ENABLE_WEIGHT_PARSING = False
```

---

# 🧹 11. Очистка перед запуском

Автоматически выполняется:

* очистка debug папок
* очистка storage
* удаление STOP.flag

👉 реализация: 

---

# 🛑 12. Остановка

Создать файл:

```
input/STOP.flag
```

---

# ⚠️ 13. Возможные проблемы

## ❌ RateLimit

* Armtek может вернуть 429
* включается пауза

## ❌ Капча

* решается через 2Captcha (не участвует, краули оперезапускает сесиию сам)

## ❌ Нет данных

* fallback Japarts → Armtek

---

# 🧠 14. Оптимизации

* блокировка медиа (ускорение)
* batch processing
* async crawling
* session reuse

---

# 📊 15. Кратко

| Режим | Скрипт        |
| ----- | ------------- |
| Цены  | main-yambo.py | 
| Веса  | main.py       |

---

# ✅ Итог

Система — это:

* асинхронный парсер
* с fallback логикой
* батчевой обработкой
* docker-ready инфраструктурой



# ✅ Файлы парсера
scraper_japarts_pure.py (ищет веса на jparts.ru)
armtek_pure_aiohtpp.py (ищет веса на armtek.ru) Есть апи, можн осразу фетчить запросом, но в начале полуаем токен, для валидации 

Скорость зависит от кол-ва воркеров и задается в config.py
ARMTEK_WORKERS = 15 воркеры для армтек - больше, так как прасим через api (браузер нужен только для валидного токена один раз, для каждой сессии - если отдает 429)
JPARTS_WORKERS = 5 - 5 контекстов, 5 вкладок, RAM забивается






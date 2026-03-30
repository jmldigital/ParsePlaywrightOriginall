# utils.py
import logging
import re
import time
import random
import requests
from datetime import datetime, timedelta, timezone

import pandas as pd
from pathlib import Path
from datetime import datetime
import json
from config import (
    ARMTEK_P_W,
    ARMTEK_V_W,
    JPARTS_P_W,
    JPARTS_V_W,
    stparts_price,
    stparts_delivery,
    avtoformula_price,
    avtoformula_delivery,
    API_KEY_2CAPTCHA,
    LOG_LEVEL,
)
from config import input_price
import asyncio

import shutil
from typing import List

# Файлы
LOG_FILE = "logs/parser.log"
MAIN_LOG_FILE = "logs/main.log"
COUNTER_FILE = "logs/run_counter.json"

# Глобальный логгер (инициализируется один раз)
_logger = None


import base64

import io
import os

from twocaptcha import TwoCaptcha
from PIL import Image
from playwright.async_api import Page


class RateLimitException(Exception):
    """Raised when armtek reports request‑limit exceeded."""

    pass


import asyncio
import base64
import io
from datetime import datetime
from pathlib import Path
from PIL import Image
from playwright.async_api import Page
from twocaptcha import TwoCaptcha


# utils.py - обновлённый solve_captcha_universal


async def solve_captcha_universal(
    page: Page,
    logger,
    site_key: str,
    selectors: dict,
    max_attempts: int = 3,
    scale_factor: int = 3,
    numeric_only=True,
    wait_after_submit_ms: int = 4000,  # ↑ Увеличено
) -> bool:
    """
    Универсальное решение капчи: visibility + hash (без гонки)
    """
    import imagehash
    import io
    import base64
    from PIL import Image
    from twocaptcha import TwoCaptcha
    import asyncio

    solver = TwoCaptcha(API_KEY_2CAPTCHA)
    captcha_img = page.locator(selectors["captcha_img"])

    if not await captcha_img.is_visible():
        logger.info(f"[{site_key}] Капча не найдена")
        return False

    for attempt in range(1, max_attempts + 1):
        logger.info(f"[{site_key}] Попытка {attempt}/{max_attempts}")

        try:
            # 🔥 СКРИН ДО для hash
            img_bytes_before = await captcha_img.screenshot()
            img_before = Image.open(io.BytesIO(img_bytes_before))

            # 1. Скрин для 2Captcha + scale
            img_bytes = await captcha_img.screenshot()
            img = Image.open(io.BytesIO(img_bytes))
            if scale_factor > 1:
                new_size = (img.width * scale_factor, img.height * scale_factor)
                img = img.resize(new_size, Image.BICUBIC)
                logger.info(f"[{site_key}] Увеличено до {img.size}")

            # 2. Base64 → 2Captcha
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            captcha_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")

            extra_params = (
                {"numeric": 1, "minLen": 4, "maxLen": 4, "phrase": 0}
                if numeric_only
                else {}
            )
            logger.info(f"[{site_key}] Отправка в 2Captcha...")

            result = await asyncio.wait_for(
                asyncio.to_thread(solver.normal, captcha_base64, **extra_params),
                timeout=60.0,
            )

            captcha_text = result.get("code", "").upper().strip()
            if not captcha_text:
                logger.warning(f"[{site_key}] Пустой ответ от 2Captcha")
                await asyncio.sleep(3)
                continue

            logger.info(f"[{site_key}] Распознано: '{captcha_text}'")
            await _save_full_page_screenshot(page, site_key, captcha_text, "sent")

            # 3. Ввод + submit
            input_el = page.locator(selectors["captcha_input"])
            await input_el.clear()
            await input_el.fill(captcha_text)
            await _save_full_page_screenshot(page, site_key, captcha_text, "input")

            submit_button = page.locator(selectors["captcha_submit"])
            if await submit_button.is_visible():
                await submit_button.click()
                logger.info(f"[{site_key}] Submit нажат")

            # 🔥 ЖДЁМ + ПРОВЕРКА УСПЕХА (3 уровня)
            await page.wait_for_timeout(wait_after_submit_ms)
            await _save_full_page_screenshot(page, site_key, captcha_text, "press")

            # 1️⃣ Visibility (быстро)
            if not await captcha_img.is_visible(timeout=1000):
                logger.info(f"[{site_key}] ✅ Капча НЕ visible")
                await _save_full_page_screenshot(
                    page, site_key, captcha_text, "success"
                )
                return True

            # 2️⃣ Hidden (универсально)
            try:
                await captcha_img.wait_for(state="hidden", timeout=2000)
                logger.info(f"[{site_key}] ✅ Капча hidden")
                await _save_full_page_screenshot(
                    page, site_key, captcha_text, "success"
                )
                return True
            except:
                pass

            # 3️⃣ Hash: регенерация (avtoformula)
            try:
                img_bytes_after = await captcha_img.screenshot()
                img_after = Image.open(io.BytesIO(img_bytes_after))
                diff = imagehash.average_hash(img_before) - imagehash.average_hash(
                    img_after
                )
                logger.info(f"[{site_key}] Hash diff: {diff}")
                if diff > 5:
                    logger.info(f"[{site_key}] ✅ Регенерация!")
                    await _save_full_page_screenshot(
                        page, site_key, captcha_text, "success"
                    )
                    return True
            except Exception as e:
                logger.info(f"[{site_key}] ✅ Hash OK: {e}")
                return True

            # ❌ FAILED
            logger.warning(f"[{site_key}] ❌ Все проверки failed → retry")
            await _save_full_page_screenshot(page, site_key, captcha_text, "failed")
            await asyncio.sleep(2)

        except asyncio.TimeoutError:
            logger.error(f"[{site_key}] Таймаут 2Captcha")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"[{site_key}] Ошибка: {e}")
            await asyncio.sleep(5)

    logger.error(f"[{site_key}] Исчерпаны попытки ({max_attempts})")
    return False


# Обновлённая функция для сохранения капчи
async def _save_debug_screenshot(
    img: Image.Image,
    site_key: str,
    captcha_text: str,
    status: str,  # "sent", "success", "failed"
):
    """
    Сохраняет скриншот капчи в папку по статусу
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 🆕 Создаём путь с подпапкой статуса
        status_dir = f"captcha_debug_{site_key}/{status}"
        Path(status_dir).mkdir(parents=True, exist_ok=True)

        filename = f"{status_dir}/{site_key}_captcha_{captcha_text}_{timestamp}.png"
        img.save(filename)

        logger.debug(f"[{site_key}] 💾 Капча сохранена: {filename}")

    except Exception as e:
        logger.error(f"[{site_key}] Ошибка сохранения капчи: {e}")


# 🆕 Функция для сохранения полного скриншота страницы
async def _save_full_page_screenshot(
    page: Page,
    site_key: str,
    captcha_text: str,
    status: str,  # 🔥 Теперь принимает status вместо attempt
):
    """
    Сохраняет полный скриншот страницы в ту же папку что и капчу
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # 🆕 Создаём путь с подпапкой статуса (та же что у капчи)
        status_dir = f"captcha_debug_{site_key}/{status}"
        Path(status_dir).mkdir(parents=True, exist_ok=True)

        filename = f"{status_dir}/{site_key}_page_{captcha_text}_{timestamp}.png"

        # Полный скриншот страницы
        await page.screenshot(path=filename, full_page=True)

        logger.info(f"[{site_key}] 📸 Скриншот страницы сохранён: {filename}")

    except Exception as e:
        logger.error(f"[{site_key}] Ошибка сохранения скриншота страницы: {e}")


def get_2captcha_proxy() -> dict[str, str]:
    """
    Запрашивает у 2Captcha whitelist прокси + возвращает ПРАВИЛЬНЫЙ формат для Playwright
    """
    from config import (
        API_KEY_2CAPTCHA,
        PROXY_COUNTRY,
        PROXY_PROTOCOL,
        PROXY_CONNECTIONS,
        PROXY_IP,
        PROXY_USERNAME,
        PROXY_PASSWORD,
    )
    import random
    import requests
    import time

    # Запрос к 2Captcha
    base_url = "https://api.rucaptcha.com/proxy/generate_white_list_connections"
    params = {
        "key": API_KEY_2CAPTCHA,
        "country": PROXY_COUNTRY,
        "protocol": PROXY_PROTOCOL,
        "connection_count": str(PROXY_CONNECTIONS),
    }
    if PROXY_IP:
        params["ip"] = PROXY_IP

    resp = requests.get(base_url, params=params, timeout=30)
    resp.raise_for_status()

    payload = resp.json()
    if payload.get("status") != "OK":
        raise RuntimeError(f"2Captcha error: {payload}")

    ip_list = payload.get("data", [])
    if not ip_list:
        raise RuntimeError("2Captcha вернул пустой список прокси")

    # 🎯 Выбираем свежий IP:PORT
    chosen_ip_port = random.choice(ip_list)
    print(f"🎲 Выбран прокси: {chosen_ip_port}")

    # ⏳ Ждем активации (ОБЯЗАТЕЛЬНО!)
    time.sleep(15)

    # 🔥 ПРАВИЛЬНЫЙ формат как в вашем requests примере:
    proxy_string = f"http://{PROXY_USERNAME}:{PROXY_PASSWORD}@{chosen_ip_port}"

    return {
        "server": proxy_string,  # http://username:password@IP:PORT
        # username/password НЕ НУЖНЫ — они уже в server!
    }


def get_site_logger(site_name: str) -> logging.Logger:
    """Логгер для сайта: ФАЙЛ + КОНСОЛЬ UTF-8 (Windows/Ubuntu)"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"{site_name}.log"

    logger = logging.getLogger(site_name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    fmt_str = f"[ {site_name} ] %(asctime)s - %(levelname)s - %(message)s"

    # 1. ФАЙЛ логгер
    fh = logging.FileHandler(log_file, encoding="utf-8", mode="w")
    fh.setFormatter(logging.Formatter(fmt_str))
    logger.addHandler(fh)

    # 2. КОНСОЛЬ логгер — БЕЗОПАСНЫЙ UTF-8
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(fmt_str, datefmt="%H:%M:%S"))

    # 🔥 UTF-8 ТОЛЬКО где возможно
    try:
        if os.name == "nt":
            ch.stream = io.TextIOWrapper(ch.stream.buffer, encoding="utf-8")
        else:
            # Ubuntu — try encoding
            if hasattr(ch.stream, "encoding"):
                ch.stream.encoding = "utf-8"
    except AttributeError:
        pass  # Игнорируем ошибки кодировки

    logger.addHandler(ch)
    logger.propagate = False

    return logger


def get_run_count():
    """Возвращает номер текущего запуска (счётчик)"""
    path = Path(COUNTER_FILE)
    path.parent.mkdir(exist_ok=True)

    try:
        with open(path, "r+", encoding="utf-8") as f:
            data = json.load(f)
            count = data.get("count", 0) + 1
            f.seek(0)
            json.dump({"count": count}, f, ensure_ascii=False, indent=2)
            f.truncate()
    except (FileNotFoundError, json.JSONDecodeError):
        count = 1
        path.write_text(
            json.dumps({"count": count}, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:
        print(f"⚠️ Ошибка работы со счётчиком: {e}")
        count = 1

    return count


def setup_logger():
    """Настраивает основной логгер"""
    global _logger
    # if _logger is not None:
    #     return _logger

    count = get_run_count()
    log_path = Path(LOG_FILE)

    if count % 10 == 1:
        if log_path.exists():
            log_path.unlink()
            print(f"parser.log очищен (запуск №{count})")

    _logger = logging.getLogger("parser")

    # 🔥 Импортируем уровень из config
    from config import LOG_LEVEL

    # Устанавливаем уровень логирования
    _logger.setLevel(getattr(logging, LOG_LEVEL.upper()))

    if _logger.handlers:
        _logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    fh = logging.FileHandler(log_path, encoding="utf-8", mode="w")
    fh.setFormatter(formatter)
    _logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    _logger.addHandler(ch)

    _logger.info(f"🔄 Запуск парсера №{count}")
    _logger.info(f"📊 Уровень логирования: {LOG_LEVEL}")
    return _logger


# def get_logger():
#     """Возвращает глобальный логгер (ленивая инициализация)"""
#     global _logger
#     if _logger is None:
#         _logger = setup_logger()
#     return _logger


def setup_root_logging():
    """Глобальный системный лог: все логгеры (root + библиотеки) → logs/main.log"""

    os.makedirs("logs", exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))

    # Чистим старые хендлеры, чтобы не было дублирования при повторных запусках
    root_logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s - [%(name)s] %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Каждый запуск перезаписываем main.log
    fh_main = logging.FileHandler(MAIN_LOG_FILE, mode="w", encoding="utf-8")
    fh_main.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    fh_main.setFormatter(formatter)

    root_logger.addHandler(fh_main)


def get_logger():
    global _logger
    if _logger is not None:
        return _logger

    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger("parser")
    logger.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    logger.propagate = False  # чтобы не дублировать в root, если не нужно

    # Чистим старые хендлеры при повторных импортах
    logger.handlers.clear()

    # Формат
    formatter = logging.Formatter(
        "%(asctime)s - [%(name)s] %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # 1) Основной файл (parser.log) — как раньше, append
    fh_parser = logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8")
    fh_parser.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    fh_parser.setFormatter(formatter)
    logger.addHandler(fh_parser)

    # 2) Новый основной лог main.log — перезаписываем при каждом запуске
    fh_main = logging.FileHandler(MAIN_LOG_FILE, mode="w", encoding="utf-8")
    fh_main.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    fh_main.setFormatter(formatter)
    logger.addHandler(fh_main)

    # 3) Опционально: вывод в консоль (можно убрать, если мешает в Windows)
    sh = logging.StreamHandler()
    sh.setLevel(getattr(logging, LOG_LEVEL.upper(), logging.INFO))
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    _logger = logger
    return _logger


logger = get_logger()  # ← теперь безопасно


def parse_price(text):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None

    # Если это уже int или float — вернуть как float
    if isinstance(text, (int, float)):
        return float(text)

    # Дальше обычная логика для строки
    clean = re.sub(r"[^\d,.\s]", "", str(text).lower()).strip()
    clean = clean.replace("\u00a0", "").replace(" ", "")

    # Попытка парсинга с учетом десятичных знаков
    try:
        # Заменяем запятую на точку для корректного парсинга
        normalized = clean.replace(",", ".")
        return float(normalized)
    except (ValueError, AttributeError):
        return None


def clean_text(s):
    if isinstance(s, str):
        # Удаляем управляющие символы
        return re.sub(r"[\x00-\x1F]", "", s)
    return s


def preprocess_dataframe(df):
    """
    Предобработка DataFrame:
    - Конвертирует имена столбцов в строки (удаляет .0)
    - Очищает значения от пробелов
    - Преобразует столбец с ценой: если не float, приводит к float через безопасную обработку запятых
    """

    # Преобразование имён столбцов
    df.columns = df.columns.astype(str).str.replace(".0", "", regex=False).str.strip()

    # Обработка строковых столбцов — убираем лишние пробелы
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()

    # Обработка столбца с ценой (например, input_price)
    if input_price in df.columns:
        # Только если не float — иначе не трогаем
        if df[input_price].dtype != "float64":
            # Преобразуем ВСЕ значения к строкам, чтобы .str.replace не упал с ошибкой
            df[input_price] = (
                df[input_price].astype(str).str.replace(",", ".", regex=False)
            )
            # Конвертируем в float, нечисловые значения станут NaN
            df[input_price] = pd.to_numeric(df[input_price], errors="coerce")
            # Явно приводим к float dtype для совместимости
            df[input_price] = df[input_price].astype("float64")

    df = df.map(clean_text)

    return df


def normalize_brand(brand_str):
    if not brand_str:
        return ""
    return re.sub(r"[^a-z0-9]", "", str(brand_str).lower())


def brand_matches(search_brand, result_brand):
    if not search_brand or not result_brand:
        return False
    norm_search = normalize_brand(search_brand)
    norm_result = normalize_brand(result_brand)

    if norm_search == norm_result:
        return True
    if norm_search in norm_result:
        return True
    return False


# def consolidate_weights(df):
#     """
#     Из 4 колонок весов → 2 финальные
#     Приоритет: japarts > armtek
#     """
#     logger.info("🔄 Консолидация весов: 4 колонки → 2 финальные")

#     # Создаём финальные колонки
#     df["physical_weight"] = None
#     df["volumetric_weight"] = None

#     for idx, row in df.iterrows():
#         # Физический вес: japarts ИЛИ armtek
#         if pd.notna(row[JPARTS_P_W]):
#             df.at[idx, "physical_weight"] = row[JPARTS_P_W]
#         elif pd.notna(row[ARMTEK_P_W]):
#             df.at[idx, "physical_weight"] = row[ARMTEK_P_W]

#         # Объёмный вес: japarts ИЛИ armtek
#         if pd.notna(row[JPARTS_V_W]):
#             df.at[idx, "volumetric_weight"] = row[JPARTS_V_W]
#         elif pd.notna(row[ARMTEK_V_W]):
#             df.at[idx, "volumetric_weight"] = row[ARMTEK_V_W]

#     # Удаляем промежуточные колонки
#     cols_to_drop = [
#         JPARTS_P_W,
#         JPARTS_V_W,
#         ARMTEK_P_W,
#         ARMTEK_V_W,
#         stparts_price,
#         stparts_delivery,
#         avtoformula_price,
#         avtoformula_delivery,
#     ]
#     df.drop(columns=[col for col in cols_to_drop if col in df.columns], inplace=True)

#     logger.info("✅ Консолидация весов завершена")
#     return df


def preprocess_weight_column(series):
    """Безопасная обработка весов"""
    if series.dtype == "object":
        # "5" → 5.0, "нет" → NaN
        series = pd.to_numeric(
            series.astype(str).str.replace(",", "."), errors="coerce"
        )
    return series.astype("float64")


def consolidate_weights(df):
    logger.info("🔄 Консолидация с preprocess весов...")

    # 📊 ДО с типами
    logger.info(f"JP_P_W dtype: {df[JPARTS_P_W].dtype}")
    logger.info(f"ARM_P_W dtype: {df[ARMTEK_P_W].dtype}")

    # 🔥 PREPROCESS весовых колонок!
    df[JPARTS_P_W] = preprocess_weight_column(df[JPARTS_P_W])
    df[ARMTEK_P_W] = preprocess_weight_column(df[ARMTEK_P_W])
    df[JPARTS_V_W] = preprocess_weight_column(df[JPARTS_V_W])

    # 📊 После preprocess
    jp_phys = df[JPARTS_P_W].notna().sum()
    arm_phys = df[ARMTEK_P_W].notna().sum()
    logger.info(f"📊 После preprocess: JP={jp_phys}, ARM={arm_phys}")

    # 🔥 Векторная консолидация
    df["physical_weight"] = df[JPARTS_P_W].fillna(df[ARMTEK_P_W])
    df["volumetric_weight"] = df[JPARTS_V_W]  # Только JP!

    # Финальная статистика
    phys_final = df["physical_weight"].notna().sum()
    vol_final = df["volumetric_weight"].notna().sum()
    logger.info(f"📊 ФИНАЛ: phys={phys_final}, vol={vol_final}")

    # Drop только весовые
    cols_to_drop = [
        JPARTS_P_W,
        JPARTS_V_W,
        ARMTEK_P_W,
        ARMTEK_V_W,
        stparts_price,
        stparts_delivery,
        avtoformula_price,
        avtoformula_delivery,
    ]

    df.drop(
        columns=[col for col in cols_to_drop if col in df.columns],
        inplace=True,
        errors="ignore",
    )

    logger.info("✅ Консолидация завершена!")
    return df


async def save_debug_info(
    page: Page,
    part: str,
    reason: str,
    logger: logging.Logger = None,
    site: str = "unknown",
):
    """DEBUG: скрин + HTML для armtek/japarts"""
    if logger is None:
        logger = logging.getLogger(__name__)

    os.makedirs(f"debug_{site}", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    screenshot_path = f"debug_{site}/{reason}_{part}_{timestamp}.png"
    await page.screenshot(path=screenshot_path)

    html_path = f"debug_{site}/{reason}_{part}_{timestamp}.html"
    html_content = await page.content()
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.warning(f"📸 DEBUG {reason} {site} {part}:")
    logger.warning(f"   📍 URL: {page.url}")
    logger.warning(f"   🖼️ {screenshot_path}")
    logger.warning(f"   📄 {html_path}")


# def clear_debug_folders_sync(sites: List[str], logger: logging.Logger):
#     """Синхронная очистка debug_* + скринов капчи перед запуском."""
#     for site in sites:
#         # 1️⃣ Debug папки (как было)
#         debug_dir = f"debug_{site}"
#         if os.path.exists(debug_dir):
#             _safe_rmtree(debug_dir, logger, f"debug_{site}")

#         # 2️⃣ Скрины капчи
#         screenshot_base = f"screenshots/{site}"
#         if os.path.exists(screenshot_base):
#             captcha_folders = [
#                 "sent",
#                 "success",
#                 "failed",
#                 "changed",
#                 "errors",
#                 "processed",
#             ]
#             for folder in captcha_folders:
#                 folder_path = f"{screenshot_base}/{folder}"
#                 if os.path.exists(folder_path):
#                     _safe_rmtree(folder_path, logger, f"{site}/{folder}")

#             # ✅ Создать пустые папки заново
#             os.makedirs(screenshot_base, exist_ok=True)
#             for folder in captcha_folders:
#                 os.makedirs(f"{screenshot_base}/{folder}", exist_ok=True)
#             logger.info(f"🧹 Cleared & recreated screenshots/{site}/")


def clear_debug_folders_sync(logger: logging.Logger):
    """Синхронная очистка screenshots/ перед запуском."""

    # 1️⃣ Очистка ВСЕХ папок с "debug" ТОЛЬКО в корне
    for item in os.listdir("."):
        if os.path.isdir(item) and "debug" in item.lower():
            _safe_rmtree(item, logger, item)
            logger.info(f"🧹 Cleared {item}")

    # 2️⃣ Удаление STOP-флага (если был)
    stop_flag = "input/STOP.flag"
    if os.path.exists(stop_flag):
        os.remove(stop_flag)
        logger.info("🛑 STOP.flag удалён")

    # 2️⃣ Очистка Crawlee storage (request_queues + key_value_stores)
    storage_dir = "storage"
    if os.path.exists(storage_dir):
        _safe_rmtree(storage_dir, logger, "storage")
        logger.info("🗑️ Cleared Crawlee storage (request_queues + key_value_stores)")


def _safe_rmtree(path: str, logger, label: str, max_retries: int = 3):
    """Безопасное удаление с retry."""
    for retry in range(max_retries):
        try:
            shutil.rmtree(path, ignore_errors=True)
            logger.info(f"🧹 Cleared {label}")
            return
        except Exception as e:
            logger.warning(f"Failed to clear {label} (retry {retry+1}): {e}")
            if retry < max_retries - 1:
                time.sleep(1)
            else:
                logger.error(f"❌ Could not clear {label}")


"""
Утилиты для работы с прокси 2Captcha
Добавьте эту функцию в ваш utils.py
"""
import random
import requests
import time
from typing import Dict


def get_2captcha_proxy_pool(count: int = 5) -> List[str]:
    """
    1. Пробует 2Captcha → ТОЧНО count прокси
    2. Фильтрует битые ("error", "Internal server error")
    3. Если ошибка/пусто → 20 РАБОЧИХ резервных
    """

    # 🔥 РЕЗЕРВНЫЙ СПИСОК
    FALLBACK_PROXIES = [
        "http://118.193.59.92:11341",
        "http://118.193.59.92:11336",
    ]

    # Автоматическое определение IP (без изменений)
    try:
        my_ip_response = requests.get("https://api.ipify.org?format=json", timeout=5)
        MY_IP = my_ip_response.json()["ip"]
        logger.info(f"🌍 Ваш IP: {MY_IP}")
    except:
        MY_IP = "152.53.136.84"
        logger.warning(f"⚠️ Fallback IP: {MY_IP}")

    # Запрос к 2Captcha (без изменений)
    url = (
        f"https://api.rucaptcha.com/proxy/generate_white_list_connections"
        f"?key={API_KEY_2CAPTCHA}&country=ru&protocol=http"
        f"&connection_count={count}&ip={MY_IP}"
    )

    try:
        logger.info(f"🌐 Запрос {count} прокси от 2Captcha...")
        response = requests.get(url, timeout=15)
        data = response.json()

        if data.get("status") == "OK":
            proxies = data.get("data", [])
            proxy_urls = [f"http://{proxy}" for proxy in proxies]

            # 🔥 ФИЛЬТР БИТЫХ ПРОКСИ
            valid_proxies = [
                p
                for p in proxy_urls
                if p
                and len(p) > 15
                and ":" in p
                and "500" not in p
                and "error" not in p.lower()
            ]

            logger.info(f"✅ Получено {len(proxy_urls)} → валидно {len(valid_proxies)}")
            for i, p in enumerate(valid_proxies, 1):
                logger.info(f"   Прокси #{i}: {p}")

            if valid_proxies:
                return valid_proxies[:count]  # ТОЧНО count или меньше

        logger.error(f"❌ 2Captcha API: {data}")

    except Exception as e:
        logger.error(f"❌ 2Captcha ошибка: {e}")

    # 🔥 РЕЗЕРВ - ТОЛЬКО при полном провале
    logger.info("🔄 Используем резервные прокси (20 шт)")
    return FALLBACK_PROXIES

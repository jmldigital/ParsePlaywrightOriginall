# utils.py
import logging
import re
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
)
from config import input_price
import asyncio

# Файлы
LOG_FILE = "logs/parser.log"
COUNTER_FILE = "logs/run_counter.json"

# Глобальный логгер (инициализируется один раз)
_logger = None


import base64

import io
import os

from twocaptcha import TwoCaptcha
from PIL import Image
from playwright.async_api import Page

API_KEY_2CAPTCHA = os.getenv("API_KEY_2CAPTCHA")  # или откуда ты его берёшь


async def solve_captcha_universal(
    page: Page,
    logger,
    site_key: str,
    selectors: dict,
    max_attempts: int = 3,
    scale_factor: int = 3,
    check_changed: bool = True,
    wait_after_submit_ms: int = 5000,
) -> bool:
    """
    Универсальное решение капчи через 2Captcha.

    :param page: Playwright Page
    :param logger: логгер (logger_avto / logger_armtek / др.)
    :param site_key: строка для логов и имён файлов ("avtoformula", "armtek", ...)
    :param selectors: словарь с селекторами:
        {
            "captcha_img": "...",
            "captcha_input": "...",
            "submit": "..."  # CSS / XPath
        }
    :param max_attempts: максимум попыток распознать и отправить капчу
    :param scale_factor: во сколько раз увеличивать картинку (1 — без увеличения)
    :param check_changed: проверять ли, изменилась ли капча во время распознавания
    :param wait_after_submit_ms: пауза после отправки, мс
    """
    solver = TwoCaptcha(API_KEY_2CAPTCHA)

    captcha_text = None
    img = None
    original_img_bytes = None

    try:
        captcha_img = page.locator(selectors["captcha_img"])

        # Если капчи нет — выходим
        if not await captcha_img.is_visible():
            logger.info(f"[{site_key}] Капча не обнаружена")
            return False

        for attempt in range(1, max_attempts + 1):
            logger.info(
                f"[{site_key}] 📸 Скриншот капчи (попытка {attempt}/{max_attempts})"
            )

            # 1) Скриншот
            original_img_bytes = await captcha_img.screenshot()
            logger.info(
                f"[{site_key}] 📸 Скриншот капчи получен, размер: {len(original_img_bytes)} байт"
            )

            if not original_img_bytes or len(original_img_bytes) < 100:
                raise Exception(
                    "Получены пустые или слишком маленькие данные изображения"
                )

            # 2) Открываем и масштабируем
            img = Image.open(io.BytesIO(original_img_bytes))
            logger.info(
                f"[{site_key}] ✅ Изображение открыто: {img.format} {img.size} {img.mode}"
            )

            if scale_factor != 1:
                img = img.resize(
                    (img.width * scale_factor, img.height * scale_factor),
                    Image.BICUBIC,
                )
                logger.info(
                    f"[{site_key}] 🔍 Изображение увеличено до: {img.size}, scale={scale_factor}"
                )

            # 3) Готовим base64
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            captcha_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")

            # 4) Отправляем в 2Captcha
            logger.info(f"[{site_key}] Отправляем капчу в 2Captcha")
            result = await asyncio.to_thread(solver.normal, captcha_base64)
            captcha_text = result["code"]
            logger.info(f"[{site_key}] ✅ Капча распознана (оригинал): {captcha_text}")

            # Приводим к верхнему регистру — полезно для буквенных капч
            captcha_text = captcha_text.upper()
            logger.info(f"[{site_key}] ✅ Капча в верхнем регистре: {captcha_text}")

            # 5) (опционально) проверяем, не изменилась ли капча
            if check_changed:
                current_img_bytes = await captcha_img.screenshot()
                if current_img_bytes != original_img_bytes:
                    logger.warning(
                        f"[{site_key}] ⚠️ Капча изменилась во время распознавания, пробуем ещё раз"
                    )
                    os.makedirs(f"screenshots/{site_key}/changed", exist_ok=True)
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    Image.open(io.BytesIO(original_img_bytes)).save(
                        f"screenshots/{site_key}/changed/original_{ts}.png"
                    )
                    Image.open(io.BytesIO(current_img_bytes)).save(
                        f"screenshots/{site_key}/changed/changed_{ts}.png"
                    )
                    continue  # следующая попытка

            # 6) Вводим капчу
            input_el = page.locator(selectors["captcha_input"])
            await input_el.fill(captcha_text)
            logger.info(f"[{site_key}] ✅ Капча введена: {captcha_text}")

            # 7) Нажимаем submit
            submit_sel = selectors["submit"]
            submit_button = page.locator(submit_sel)
            await submit_button.click()
            logger.info(f"[{site_key}] ✅ Нажата кнопка отправки ({submit_sel})")

            # 8) Ждём обновления страницы
            await page.wait_for_timeout(wait_after_submit_ms)

            # 9) Проверяем, исчезла ли капча
            if not await captcha_img.is_visible():
                logger.info(f"[{site_key}] ✅ Капча успешно решена, элемент исчез")

                os.makedirs(f"screenshots/{site_key}/success", exist_ok=True)
                ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                success_path = (
                    f"screenshots/{site_key}/success/success_{captcha_text}_{ts}.png"
                )
                img.save(success_path)
                logger.info(f"[{site_key}] 🎉 Успешная капча сохранена: {success_path}")
                return True

            # Капча не ушла — делаем лог и идём на следующую попытку
            logger.warning(
                f"[{site_key}] ⚠️ Капча всё ещё видна после попытки {attempt}/{max_attempts}"
            )
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            os.makedirs(f"screenshots/{site_key}/failed", exist_ok=True)
            await page.screenshot(
                path=f"screenshots/{site_key}/failed/page_failed_{captcha_text}_{ts}.png"
            )
            os.makedirs(f"screenshots/{site_key}/processed", exist_ok=True)
            img.save(
                f"screenshots/{site_key}/processed/processed_{captcha_text}_{ts}.png"
            )

            await page.wait_for_timeout(2000)

        logger.error(
            f"[{site_key}] ❌ Превышено максимальное количество попыток ({max_attempts})"
        )
        return False

    except Exception as e:
        logger.error(f"[{site_key}] ❌ Ошибка решения капчи: {e}", exc_info=True)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        label = captcha_text if captcha_text else "unknown"

        try:
            os.makedirs(f"screenshots/{site_key}/errors", exist_ok=True)
            await page.screenshot(
                path=f"screenshots/{site_key}/errors/error_page_{label}_{ts}.png"
            )
        except Exception as se:
            logger.error(f"[{site_key}] Не удалось сохранить скриншот страницы: {se}")

        try:
            if img is not None:
                os.makedirs(f"screenshots/{site_key}/processed", exist_ok=True)
                img.save(
                    f"screenshots/{site_key}/processed/error_processed_{label}_{ts}.png"
                )
        except Exception as se:
            logger.error(f"[{site_key}] Не удалось сохранить обработанную капчу: {se}")

        return False


def get_site_logger(site_name: str) -> logging.Logger:
    """Создает отдельный логгер для конкретного сайта"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"{site_name}.log"

    logger = logging.getLogger(site_name)
    if logger.handlers:
        return logger  # избегаем дублирования

    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_file, encoding="utf-8", mode="w")
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

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
    if _logger is not None:
        return _logger

    count = get_run_count()
    log_path = Path(LOG_FILE)

    if count % 10 == 1:
        if log_path.exists():
            log_path.unlink()
            print(f"parser.log очищен (запуск №{count})")

    _logger = logging.getLogger("parser")
    _logger.setLevel(logging.INFO)

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
    return _logger


def get_logger():
    """Возвращает глобальный логгер (ленивая инициализация)"""
    global _logger
    if _logger is None:
        _logger = setup_logger()
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

    df = df.applymap(clean_text)

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


def consolidate_weights(df):
    """
    Из 4 колонок весов → 2 финальные
    Приоритет: japarts > armtek
    """
    logger.info("🔄 Консолидация весов: 4 колонки → 2 финальные")

    # Создаём финальные колонки
    df["physical_weight"] = None
    df["volumetric_weight"] = None

    for idx, row in df.iterrows():
        # Физический вес: japarts ИЛИ armtek
        if pd.notna(row[JPARTS_P_W]):
            df.at[idx, "physical_weight"] = row[JPARTS_P_W]
        elif pd.notna(row[ARMTEK_P_W]):
            df.at[idx, "physical_weight"] = row[ARMTEK_P_W]

        # Объёмный вес: japarts ИЛИ armtek
        if pd.notna(row[JPARTS_V_W]):
            df.at[idx, "volumetric_weight"] = row[JPARTS_V_W]
        elif pd.notna(row[ARMTEK_V_W]):
            df.at[idx, "volumetric_weight"] = row[ARMTEK_V_W]

    # Удаляем промежуточные колонки
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
    df.drop(columns=[col for col in cols_to_drop if col in df.columns], inplace=True)

    logger.info("✅ Консолидация весов завершена")
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

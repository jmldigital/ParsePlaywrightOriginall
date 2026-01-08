"""
Асинхронный парсер japarts.ru для поиска веса по артикулу
С FALLBACK-скриншотами и HTML при ошибках!
"""

import re
import os
from datetime import datetime
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout
from config import SELECTORS
from utils import get_site_logger
import logging

logger = get_site_logger("japarts")

BASE_URL = "https://www.japarts.ru"
WAIT_TIMEOUT = 10000

# 🆕 Создаём папку для скриншотов
os.makedirs("debug_japarts", exist_ok=True)


async def save_debug_info(page: Page, part: str, reason: str):
    """Сохраняет скриншот + HTML + URL для анализа"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Скриншот
    screenshot_path = f"debug_japarts/{reason}_{part}_{timestamp}.png"
    await page.screenshot(path=screenshot_path)

    # HTML
    html_path = f"debug_japarts/{reason}_{part}_{timestamp}.html"
    html_content = await page.content()
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    # URL
    current_url = page.url
    logger.warning(f"📸 DEBUG {reason} для {part}:")
    logger.warning(f"   📍 URL: {current_url}")
    logger.warning(f"   🖼️  Скрин: {screenshot_path}")
    logger.warning(f"   📄 HTML: {html_path}")


async def scrape_weight_japarts(
    page: Page, part: str, logger: logging.Logger
) -> tuple[str, str]:
    """
    Парсер japarts.ru — DEBUG ТОЛЬКО при таймауте!
    """
    try:
        url = "https://www.japarts.ru/"
        logger.info(f"🔍 japarts.ru: поиск весов для {part}")

        await page.goto(url, wait_until="networkidle", timeout=WAIT_TIMEOUT)

        search_input = page.locator(SELECTORS["japarts"]["search_input"])
        search_button = page.locator(SELECTORS["japarts"]["search_button"])

        await search_input.fill(part)
        logger.info(f"✅ Артикул '{part}' введён")

        await search_button.click()
        logger.info("✅ Кнопка 'Найти' нажата")

        await page.wait_for_load_state("networkidle", timeout=WAIT_TIMEOUT)
        logger.info(f"📍 URL после поиска: {page.url}")

        # 🆕 ПРОВЕРКА "НЕ НАЙДЕНО" ПЕРЕД дебагом!
        html_content = await page.content()
        if "Записей по вашему запросу не найдено" in html_content:
            logger.info(f"🚫 {part}: Запись не найдена (нормально)")
            return None, None

        # Ищем веса
        weight_locator = page.locator(SELECTORS["japarts"]["weight_row"])
        if await weight_locator.count() == 0:
            logger.warning(f"❌ Вес не найден для {part} (возможно аналоги без веса)")
            return None, None

        # Берем первый текст
        weight_text = await weight_locator.first.text_content(timeout=1000)
        if not weight_text or not weight_text.strip() or "Нет веса" in weight_text:
            logger.warning(f"ℹ️  {part}: пусто или 'Нет веса'")
            return None, None

        logger.info(f"📏 japarts.ru: '{weight_text.strip()}'")

        # Парсим
        physical_match = re.search(
            r"Вес[:\s]*([\d.,]+)\s*кг", weight_text, re.IGNORECASE
        )
        volumetric_match = re.search(
            r"объемный[:\s]*вес[:\s]*([\d.,]+)\s*кг", weight_text, re.IGNORECASE
        )

        physical_weight = (
            physical_match.group(1).replace(",", ".") if physical_match else None
        )
        volumetric_weight = (
            volumetric_match.group(1).replace(",", ".") if volumetric_match else None
        )

        logger.info(
            f"✅ japarts.ru: физ={physical_weight}кг, объем={volumetric_weight}кг"
        )
        return physical_weight, volumetric_weight

    except PlaywrightTimeout as e:
        # 🆕 DEBUG ТОЛЬКО при ТАЙМАУТЕ (капча/блокировка)!
        await save_debug_info(page, part, f"TIMEOUT_{e.__class__.__name__}")
        logger.error(f"⏰ Таймаут japarts.ru для {part} (возможна капча!): {e}")
        return None, None

    except Exception as e:
        logger.error(f"❌ japarts.ru ошибка для {part}: {e}")
        return None, None

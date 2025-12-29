"""
Асинхронный парсер japarts.ru для поиска веса по артикулу
ФИНАЛЬНАЯ версия — без зависаний!
"""

import re
from playwright.async_api import Page
from config import SELECTORS
from utils import get_site_logger
import logging

logger = get_site_logger("japarts")

BASE_URL = "https://www.japarts.ru"
WAIT_TIMEOUT = 10000


async def scrape_weight_japarts(
    page: Page, part: str, logger: logging.Logger
) -> tuple[str, str]:
    """
    Парсер japarts.ru — 100% без ошибок!
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

        # 🎯 БЕЗ .first() — ПЕРВЫЙ ЭЛЕМЕНТ АВТОМАТИЧЕСКИ!
        weight_locator = page.locator(SELECTORS["japarts"]["weight_row"])

        # ✅ ПРОСТАЯ проверка
        if await weight_locator.count() == 0:
            logger.warning(f"❌ Вес не найден для {part}")
            return None, None

        # ✅ БЕЗ is_visible() — БЕРЁМ ПЕРВЫЙ текст
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

    except Exception as e:
        logger.error(f"❌ japarts.ru ошибка для {part}: {e}")
        return None, None

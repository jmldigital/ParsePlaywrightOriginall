"""
Асинхронный парсер japarts.ru для поиска веса по артикулу
С FALLBACK-скриншотами и HTML при ошибках!
"""

import asyncio
import os
from datetime import datetime
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout
from config import SELECTORS
from utils import get_site_logger, save_debug_info
import logging

logger = get_site_logger("japarts")

BASE_URL = "https://www.japarts.ru"
WAIT_TIMEOUT = 10000

# 🆕 Создаём папку для скриншотов
os.makedirs("debug_japarts", exist_ok=True)


# async def scrape_weight_japarts(
#     page: Page, part: str, logger: logging.Logger
# ) -> tuple[str, str]:
#     """
#     Japarts.ru - ✅ ФИКС TypeError (await все!)
#     """
#     try:
#         await page.goto(
#             "https://www.japarts.ru/?id=price",
#             wait_until="domcontentloaded",
#             timeout=20000,
#         )

#         search_input = page.locator(SELECTORS["japarts"]["search_input"]).first
#         await search_input.wait_for()
#         await search_input.fill(part)

#         search_button = page.locator(SELECTORS["japarts"]["search_button"]).first
#         await search_button.click()

#         await page.wait_for_timeout(5000)  # Таблица готова [file:43]

#         content = await page.content()
#         if "Записей по вашему запросу не найдено" in content:
#             logger.info("%s: не найдена", part)
#             return None, None

#         # 🔥 ФИКС: await для всех async!
#         font_locator = page.locator("font")
#         font_count = await font_locator.count()

#         if font_count == 0:
#             logger.warning("%s: нет font", part)
#             return None, None

#         # Перебираем font (await text_content)
#         for i in range(min(font_count, 20)):  # Max 20 для скорости
#             font = font_locator.nth(i)
#             text = await font.text_content()

#             if text and "Вес" in text:
#                 import re

#                 p_match = re.search(r"Вес[:\s]*([\d.,]+)\s*кг", text, re.IGNORECASE)
#                 v_match = re.search(
#                     r"объемный[:\s]*вес[:\s]*([\d.,]+)\s*кг", text, re.IGNORECASE
#                 )

#                 pw = p_match.group(1).replace(",", ".") if p_match else None
#                 vw = v_match.group(1).replace(",", ".") if v_match else None

#                 if pw:
#                     logger.info("%s: %s/%s (font #%d)", part, pw, vw or "-", i)
#                     return pw, vw

#         logger.warning("%s: вес не найден в %d font", part, font_count)
#         return None, None

#     except Exception as e:
#         logger.error("❌ %s: %s", part, str(e))
#         await save_debug_info(page, part, type(e).__name__)
#         return None, None


async def scrape_weight_japarts(
    page: Page, part: str, logger: logging.Logger
) -> tuple[str, str]:
    """
    Japarts.ru - ✅ ФИНАЛ (без Locator комбо!)
    """
    try:
        await page.goto(
            "https://www.japarts.ru/?id=price",
            wait_until="domcontentloaded",
            timeout=20000,
        )

        search_input = page.locator(SELECTORS["japarts"]["search_input"]).first
        await search_input.wait_for()
        await search_input.fill(part)

        search_button = page.locator(SELECTORS["japarts"]["search_button"]).first
        await search_button.click()

        # 🔥 ДИНАМИКА: короткая пауза + content (как раньше работало!)
        try:
            await page.wait_for_timeout(3000)
        except asyncio.TimeoutError:  # Только таймаут Playwright
            logger.warning(f"{part}: POST-wait timeout — debug saved")
            await save_debug_info(page, part, "TimeoutError", logger, "japarts")

        # БЫСТРАЯ проверка по content (0.5с, 100% надежно!)
        content = await page.content()
        if "Записей по вашему запросу не найдено" in content:
            logger.info("%s: не найдена", part)
            return None, None

        # Вес (твой селектор)
        weight_loc = page.locator(SELECTORS["japarts"]["weight_row"]).first
        weight_text = await weight_loc.text_content(timeout=5000)

        if not weight_text or "Нет веса" in weight_text:
            logger.warning("%s: вес не найден '%s'", part, weight_text[:30])
            return None, None

        import re

        p_match = re.search(r"Вес[:\s]*([\d.,]+)\s*кг", weight_text, re.IGNORECASE)
        v_match = re.search(
            r"объемный[:\s]*вес[:\s]*([\d.,]+)\s*кг", weight_text, re.IGNORECASE
        )

        pw = p_match.group(1).replace(",", ".") if p_match else None
        vw = v_match.group(1).replace(",", ".") if v_match else None

        logger.info("%s: %s/%s", part, pw, vw or "-")
        return pw, vw

    except Exception as e:
        logger.error("❌ %s: %s", part, str(e))
        await save_debug_info(page, part, type(e).__name__, logger, "japarts")
        return None, None

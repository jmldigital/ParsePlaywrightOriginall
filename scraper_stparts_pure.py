"""
Stparts парсер - ТОЛЬКО парсинг DOM
Навигация в Crawlee!
"""

import re
import asyncio
from typing import Tuple, Optional
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout
from config import SELECTORS
from utils import parse_price, brand_matches


async def wait_for_results_or_empty(page: Page) -> str:
    """Ждёт результаты или 'нет результатов'"""
    try:
        await page.wait_for_function(
            """
            (selector) => document.querySelector(selector) ||
                document.querySelector('div.fr-alert.fr-alert-warning.alert-noResults')
            """,
            arg=SELECTORS["stparts"]["results_table"],
            timeout=8000,
        )

        if await page.locator(
            "div.fr-alert.fr-alert-warning.alert-noResults"
        ).is_visible():
            return "no_results"

        return "has_results"
    except PlaywrightTimeout:
        return "timeout"


async def parse_stparts_price(
    page: Page, brand: str, part: str, logger
) -> Tuple[Optional[float], Optional[str]]:
    """
    ТОЛЬКО парсинг цены из уже открытой страницы
    Страница УЖЕ на /search/{brand}/{part} или /search?pcode={part}
    """
    try:
        # Проверка капчи
        if await page.locator(SELECTORS["stparts"]["captcha_img"]).is_visible():
            logger.warning("🔒 Капча Stparts")
            return "NeedCaptcha", "NeedCaptcha"

        # Ждём результаты
        status = await wait_for_results_or_empty(page)
        if status != "has_results":
            return None, None

        # Парсинг таблицы
        table = page.locator(SELECTORS["stparts"]["results_table"])
        await table.wait_for(state="visible", timeout=8000)
        rows = table.locator(SELECTORS["stparts"]["result_row"])
        row_count = await rows.count()

        if row_count == 0:
            logger.info(f"❌ Stparts: нет строк для {brand}/{part}")
            return None, None

        # Поиск лучшего результата (приоритет: в наличии + совпадение бренда)
        async def find_best(priority_in_stock: bool):
            for i in range(row_count):
                row = rows.nth(i)

                # Бренд
                brand_in_row = (
                    await row.locator(SELECTORS["stparts"]["brand"]).text_content()
                    or ""
                ).strip()
                if not brand_matches(brand, brand_in_row):
                    continue

                # Срок поставки
                delivery_min = (
                    await row.locator(SELECTORS["stparts"]["delivery"]).text_content()
                    or ""
                ).strip()

                # Приоритет "в наличии" (срок = 1)
                if priority_in_stock and not re.match(r"^1(\D|$)", delivery_min):
                    continue

                # Цена
                price_text = (
                    await row.locator(SELECTORS["stparts"]["price"]).text_content()
                    or ""
                ).strip()
                price = parse_price(price_text)

                if price is not None:
                    delivery_clean = (
                        delivery_min.replace("\n", " ").replace("\r", "").strip()
                    )
                    logger.info(
                        "✅ Stparts {} ({}) : {} ₽".format(
                            brand_in_row, delivery_clean, price
                        )
                    )
                    return price, delivery_min

            return None, None

        # 1. Пробуем с приоритетом "в наличии"
        result = await find_best(priority_in_stock=True)
        if result[0]:
            return result

        # 2. Без приоритета
        result = await find_best(priority_in_stock=False)
        if result[0]:
            return result

        logger.info(f"❌ Stparts: подходящих результатов нет для {brand}/{part}")
        return None, None

    except PlaywrightTimeout:
        logger.warning(f"⏰ Stparts timeout: {brand}/{part}")
        return None, None
    except Exception as e:
        logger.error(f"❌ Stparts error: {e}")
        return None, None


async def parse_stparts_name(page: Page, part: str, logger) -> Optional[str]:
    """
    ТОЛЬКО парсинг названия из уже открытой страницы
    Страница УЖЕ на /search?pcode={part}
    """
    try:
        # Проверка капчи
        if await page.locator(SELECTORS["stparts"]["captcha_img"]).is_visible():
            logger.warning("🔒 Капча Stparts (name)")
            return "NeedCaptcha"

        # Проверка "не найдено"
        no_results = page.locator("div.fr-alert.fr-alert-warning.alert-noResults")
        try:
            await no_results.wait_for(state="visible", timeout=3000)
            logger.info(f"🚫 Stparts: товар не найден ({part})")
            return None
        except PlaywrightTimeout:
            pass  # Товар найден

        # Ждём появление таблиц
        try:
            await page.wait_for_selector(
                f"{SELECTORS['stparts']['case_table']}, {SELECTORS['stparts']['alt_results_table']}",
                timeout=10000,
                state="visible",
            )
        except PlaywrightTimeout:
            logger.warning(f"⏰ Stparts: таймаут таблиц ({part})")
            return None

        # Проверка таблицы globalCase (приоритет)
        case_table = page.locator(SELECTORS["stparts"]["case_table"])
        if await case_table.count() > 0:
            desc_cells = case_table.locator(SELECTORS["stparts"]["case_description"])
            if await desc_cells.count() > 0:
                description = await desc_cells.nth(0).text_content()
                if description and description.strip():
                    logger.info(f"✅ Stparts globalCase: {description.strip()}")
                    return description.strip()

        # Fallback: таблица globalResult
        alt_table = page.locator(SELECTORS["stparts"]["alt_results_table"])
        if await alt_table.count() > 0:
            desc_cells = alt_table.locator(
                SELECTORS["stparts"]["alt_result_description"]
            )
            if await desc_cells.count() > 0:
                description = await desc_cells.nth(0).text_content()
                if description and description.strip():
                    logger.info(f"✅ Stparts globalResult: {description.strip()}")
                    return description.strip()

        logger.info(f"❌ Stparts: название не найдено ({part})")
        return None

    except PlaywrightTimeout:
        logger.warning(f"⏰ Stparts name timeout: {part}")
        return None
    except Exception as e:
        logger.error(f"❌ Stparts name error: {e}")
        return None

"""
Armtek парсер - ТОЛЬКО парсинг DOM
Навигация делается в Crawlee!
"""

import re
import asyncio
from typing import Tuple, Optional
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from utils import save_debug_info

from config import SELECTORS


async def close_city_dialog(page: Page):
    """Закрывает диалог города"""
    try:
        if await page.locator("button:has-text('Верно')").is_visible(timeout=1000):
            await page.locator("button:has-text('Верно')").click()
            return
        if await page.locator("div.geo-control__click-area").is_visible(timeout=500):
            await page.locator("div.geo-control__click-area").click(force=True)
    except Exception:
        pass


async def determine_state(page: Page) -> str:
    """
    Определяет состояние страницы после загрузки
    Crawlee уже сделал goto(), мы только проверяем результат
    """
    selectors = {
        "cards": SELECTORS["armtek"]["product_card-list"],
        "list": SELECTORS["armtek"]["product_list"],
        "no_results": SELECTORS["armtek"]["no_results"],
        "captcha": SELECTORS["armtek"]["captcha"],
        "rate_limit": SELECTORS["armtek"]["rate_limit"],
        "cloudflare": SELECTORS["armtek"]["rate_limit"],
        # 🔥 НОВОЕ СОСТОЯНИЕ!
        "card_direct": SELECTORS["armtek"]["specifications"],  # Вкладка характеристик
        "product_info": SELECTORS["armtek"]["product-card-info"],  # Данные товара
    }

    tasks = {
        asyncio.create_task(
            page.wait_for_selector(sel, state="visible", timeout=10000)
        ): name
        for name, sel in selectors.items()
    }

    done, pending = await asyncio.wait(
        tasks.keys(), return_when=asyncio.FIRST_COMPLETED
    )

    for task in pending:
        task.cancel()

    try:
        first_task = list(done)[0]
        await first_task
        return tasks[first_task]
    except PlaywrightTimeout:
        return "timeout"
    except Exception:
        return "error"


async def parse_weight_armtek(
    page: Page, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    """
    ТОЛЬКО парсинг веса из DOM
    Страница УЖЕ загружена Crawlee на URL поиска
    """

    await close_city_dialog(page)

    # Определяем состояние
    state = await determine_state(page)

    if state == "no_results":
        # logger.info(f"❌ Не найдено: {part}")
        return None, None
    elif state == "captcha":
        return "NeedCaptcha", "NeedCaptcha"
    elif state == "rate_limit":
        return "NeedProxy", "NeedProxy"
    elif state == "cloudflare":
        return "CloudFlare", "CloudFlare"
    elif state in ("timeout", "error"):
        return None, None

    # Переход к карточке товара
    try:
        if state == "cards":
            link = page.locator(SELECTORS["armtek"]["product_cards"]).first
        elif state == "list":
            link = page.locator(SELECTORS["armtek"]["product_list"]).first
        if state in ("product_info", "card_direct"):  # 🔥 Уже карточка!
            logger.debug(f"🎯 [{part}] Уже карточка (state={state})")
        else:
            return None, None

        if state not in ("product_info", "card_direct"):  # 🔥 Только для списков!
            if await link.count() == 0 or not await link.get_attribute("href"):
                return None, None

            href = await link.get_attribute("href")
            full_url = href if href.startswith("http") else "https://armtek.ru" + href
            await page.goto(full_url, wait_until="domcontentloaded", timeout=30000)

            await page.wait_for_selector(
                SELECTORS["armtek"]["product-card-info"], timeout=8000
            )

        # Даём время на рендер JSON → HTML
        for _ in range(10):
            content = await page.locator(
                SELECTORS["armtek"]["product-card-info"]
            ).text_content()
            if content and len(content.strip()) > 20:
                break
            await page.wait_for_timeout(300)

    except Exception as e:
        logger.error(f"Ошибка навигации к карточке: {e}")
        await save_debug_info(page, part, "card_error", logger, "armtek")
        return None, None

    # Парсинг веса (3 попытки)
    weight = await extract_weight(page)
    if weight:
        # logger.info(f"✅ Вес: {weight} ({part})")
        # return "NeedProxy", None
        return weight, None

    # Попытка 2: клик по вкладке характеристик
    try:
        tech_tab = page.locator(SELECTORS["armtek"]["specifications"])
        if await tech_tab.count() > 0:
            await tech_tab.click()
            await page.wait_for_timeout(1000)
            weight = await extract_weight(page)
    except Exception:
        pass

    if weight:
        logger.info(f"✅ Вес (после клика): {weight} ({part})")
        return weight, None

    # Попытка 3: последний шанс
    await page.wait_for_timeout(2000)
    weight = await extract_weight(page)

    if weight:
        logger.info(f"✅ Вес (delayed): {weight} ({part})")
        return weight, None

    logger.warning(f"❌ Вес не найден: {part}")
    return None, None


async def extract_weight(page: Page) -> Optional[str]:
    """Извлечение веса из DOM"""
    selectors = [SELECTORS["armtek"]["product-card-weight"]]

    for sel in selectors:
        try:
            elements = page.locator(sel)
            count = await elements.count()
            for i in range(count):
                text = await elements.nth(i).text_content()
                if text:
                    match = re.search(
                        r"(\d+(?:[.,]\d+)?)\s*(?:кг|kg)", text, re.IGNORECASE
                    )
                    if match:
                        return match.group(1).replace(",", ".")
        except Exception:
            continue

    return None

"""
Асинхронный парсер armtek.ru для поиска ФИЗИЧЕСКОГО веса
С обработкой капчи через 2Captcha!
"""

import re
import os
from typing import Callable, Tuple

from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from config import SELECTORS, PAGE_GOTO_TIMEOUT, CLOUD_FLARE_DETEKTOR
from utils import (
    get_site_logger,
    save_debug_info,
)  # 🆕 ИЗ utils.py!


import logging

logger = get_site_logger("armtek")

BASE_URL = "https://armtek.ru"
WAIT_TIMEOUT = 15000  # Больше для капчи
os.makedirs("debug_armtek", exist_ok=True)


"""
Асинхронный парсер armtek.ru (SPA)
Исправлено: ожидание полной прорисовки контента перед парсингом.
"""

import re
import os
import asyncio
from typing import Tuple, Optional

from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from config import PAGE_GOTO_TIMEOUT
from utils import get_site_logger, save_debug_info

logger = get_site_logger("armtek")
os.makedirs("debug_armtek", exist_ok=True)

sel_cards = "project-ui-article-card, app-article-card-tile, div[data-id]"
sel_list_results = "div.results-list a, .search-result__list a"
sel_no_results = "div.not-found.ng-star-inserted div.not-found__image"
sel_captcha = "sproit-ui-modal p:has-text('Введите код с картинки')"
sel_rate_limit = "sproit-ui-modal p:has-text('Превышен лимит запросов')"
sel_cloudflare = "#cf-chl-widget, .lds-ring, input.ctp-button"


async def close_city_dialog_if_any(page: Page):
    """Закрывает диалог города (быстро)."""
    try:
        if await page.locator("button:has-text('Верно')").is_visible(timeout=1000):
            await page.locator("button:has-text('Верно')").click()
            return

        if await page.locator("div.geo-control__click-area").is_visible(timeout=500):
            await page.locator("div.geo-control__click-area").click(force=True)
    except Exception:
        pass


async def determine_page_state(page: Page) -> str:
    """
    Определяет состояние страницы поиска.
    Таймауты увеличены до 10-15 сек, так как сайт может долго крутить спиннер.
    """

    sel_cards = "project-ui-article-card, app-article-card-tile, div[data-id]"
    sel_list_results = "div.results-list a, .search-result__list a"
    # sel_no_results = (
    #     "project-ui-search-result p:has-text('По вашему запросу ничего не найдено')"
    # )
    sel_no_results = "div.not-found.ng-star-inserted div.not-found__image"

    sel_captcha = "sproit-ui-modal p:has-text('Введите код с картинки')"
    sel_rate_limit = "sproit-ui-modal p:has-text('Превышен лимит запросов')"
    sel_cloudflare = "#cf-chl-widget, .lds-ring, input.ctp-button"

    # Создаем задачи
    tasks = {
        asyncio.create_task(
            page.wait_for_selector(sel_cards, state="visible", timeout=12000)
        ): "success_cards",
        asyncio.create_task(
            page.wait_for_selector(sel_list_results, state="visible", timeout=12000)
        ): "success_list",
        asyncio.create_task(
            page.wait_for_selector(sel_no_results, state="visible", timeout=8000)
        ): "no_results",
        asyncio.create_task(
            page.wait_for_selector(sel_captcha, state="visible", timeout=5000)
        ): "captcha",
        asyncio.create_task(
            page.wait_for_selector(sel_rate_limit, state="visible", timeout=5000)
        ): "rate_limit",
        asyncio.create_task(
            page.wait_for_selector(sel_cloudflare, state="attached", timeout=8000)
        ): "cloudflare",
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


async def scrape_weight_armtek(
    page: Page, part: str, logger
) -> Tuple[str | None, str | None]:

    search_url = f"https://armtek.ru/search?text={part}"

    # 1. Поиск
    try:
        await page.goto(
            search_url, wait_until="domcontentloaded", timeout=PAGE_GOTO_TIMEOUT
        )
    except Exception as e:
        logger.warning(f"Ошибка загрузки: {e}")
        return None, None

    await close_city_dialog_if_any(page)

    # 2. Определение результата
    state = await determine_page_state(page)

    if state == "timeout":
        # Если таймаут, пробуем подождать еще немного и проверить наличие спиннера загрузки
        logger.warning(f"⏳ Таймаут поиска (долго грузится): {part}")
        await page.wait_for_timeout(2000)
        # Повторная быстрая проверка
        if await page.locator("project-ui-article-card").count() > 0:
            state = "success_cards"
        else:
            await save_debug_info(page, part, "unknown_state", logger, "armtek")
            return None, None

    if state == "no_results":
        logger.info(f"❌ Артикул не найден: {part}")
        # 🔥 ДОБАВЬ ДЕБАГ перед tasks:
        logger.info(f"🔍 DEBUG селекторы: ")
        logger.info(f"  Cards count: {await page.locator(sel_cards).count()}")
        logger.info(f"  No results count: {await page.locator(sel_no_results).count()}")
        await save_debug_info(page, part, "no_results", logger, "armtek")
        return None, None
    elif state == "captcha":
        return "NeedCaptcha", "NeedCaptcha"
    elif state == "rate_limit":
        return "NeedProxy", "NeedProxy"
    elif state == "cloudflare":
        await save_debug_info(page, part, "ClaudFlare", logger, "armtek")
        return "ClaudFlare", "ClaudFlare"
    elif state == "error":
        return None, None

    # 3. Переход к карточке
    product_link_locator = None
    if state == "success_cards":
        product_link_locator = page.locator(
            "project-ui-article-card a, app-article-card-tile a"
        ).first
    elif state == "success_list":
        logger.info(f"📋 Найден список, берем первый: {part}")
        product_link_locator = page.locator(
            "div.results-list a, .search-result__list a"
        ).first

    try:
        if not product_link_locator or await product_link_locator.count() == 0:
            return None, None

        href = await product_link_locator.get_attribute("href", timeout=3000)
        if not href:
            return None, None

        full_url = href if href.startswith("http") else "https://armtek.ru" + href

        # Переход
        await page.goto(
            full_url, wait_until="domcontentloaded", timeout=PAGE_GOTO_TIMEOUT
        )

        # 🔥 ВАЖНО: Даем странице "подышать" после перехода.
        # SPA требует времени на рендер JSON данных в HTML.
        # Мы ждем, пока элемент с информацией о товаре станет ВИДИМЫМ.
        try:
            # Ждем сам контейнер
            await page.wait_for_selector(
                "product-card-info", state="visible", timeout=8000
            )

            # 🔥 Ждем, пока внутри контейнера появится хоть какой-то текст (значит данные прилетели)
            # Это предотвращает скриншоты пустых страниц
            for _ in range(10):  # Макс 2 сек ожидания наполнения
                content = await page.locator("product-card-info").text_content()
                if (
                    content and len(content.strip()) > 20
                ):  # Если текста достаточно много
                    break
                await page.wait_for_timeout(300)

        except PlaywrightTimeout:
            logger.warning(f"⚠️ Карточка не прогрузилась (контейнер не найден): {part}")
            await save_debug_info(page, part, "card_load_fail", logger, "armtek")
            return None, None

    except Exception as e:
        logger.error(f"Ошибка навигации: {e}")
        return None, None

    # 4. Парсинг веса (с повторной попыткой)
    # Попытка 1: Сразу
    weight = await extract_weight_text(page)
    if weight:
        logger.info(f"✅ Вес найден: {weight} ({part})")
        return weight, None

    # Попытка 2: Клик по характеристикам (иногда они скрыты)
    try:
        tech_tab = page.locator('a[href="#tech-info"]')
        if await tech_tab.count() > 0:
            await tech_tab.click()
            await page.wait_for_timeout(1000)  # Чуть больше времени на перерисовку таба
            weight = await extract_weight_text(page)
    except Exception:
        pass

    if weight:
        logger.info(f"✅ Вес найден (после клика): {weight} ({part})")
        return weight, None

    # Попытка 3: Последний шанс, возможно страница еще догружается
    # Ждем 2 сек и пробуем еще раз (только если не нашли)
    await page.wait_for_timeout(2000)
    weight = await extract_weight_text(page)

    if weight:
        logger.info(f"✅ Вес найден (delayed): {weight} ({part})")
        return weight, None

    logger.warning(f"❌ Вес не найден в карточке: {part}")
    # await save_debug_info(page, part, "weight_missing_in_card", logger, "armtek")
    return None, None


async def extract_weight_text(page: Page) -> Optional[str]:
    """Ищет вес в загруженном DOM"""
    locators = [
        "product-card-info div:has-text('Вес')",
        "product-card-info tr:has-text('Вес')",
        ".product-params__item:has-text('Вес')",
        "div.params-row:has-text('Вес')",
        "li:has-text('Вес')",
    ]

    for sel in locators:
        try:
            elements = page.locator(sel)
            count = await elements.count()
            for i in range(count):
                text = await elements.nth(i).text_content()
                if text:
                    # Ищем "0.45 кг", "0,45кг", "1 kg"
                    match = re.search(
                        r"(\d+(?:[.,]\d+)?)\s*(?:кг|kg)", text, re.IGNORECASE
                    )
                    if match:
                        return match.group(1).replace(",", ".")
        except Exception:
            continue
    return None

"""
Armtek парсер - ТОЛЬКО парсинг DOM
Навигация делается в Crawlee!
"""

import re
import asyncio
from typing import Tuple, Optional
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from utils import save_debug_info


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
        "cards": "project-ui-article-card, app-article-card-tile",
        "list": "div.results-list a, .search-result__list a",
        "no_results": "div.not-found.ng-star-inserted div.not-found__image",
        "captcha": "sproit-ui-modal p:has-text('Введите код с картинки')",
        "rate_limit": "sproit-ui-modal p:has-text('Превышен лимит запросов')",
        "cloudflare": "#cf-chl-widget, .lds-ring",
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


# async def parse_weight_armtek(
#     page: Page, part: str, logger
# ) -> Tuple[Optional[str], Optional[str]]:
#     """
#     ТОЛЬКО парсинг веса из DOM
#     Страница УЖЕ загружена Crawlee на URL поиска
#     """

#     await close_city_dialog(page)

#     # Определяем состояние
#     state = await determine_state(page)

#     if state == "no_results":
#         # logger.info(f"❌ Не найдено: {part}")
#         return None, None
#     elif state == "captcha":
#         return "NeedCaptcha", "NeedCaptcha"
#     elif state == "rate_limit":
#         return "NeedProxy", "NeedProxy"
#     elif state == "cloudflare":
#         return "CloudFlare", "CloudFlare"
#     elif state in ("timeout", "error"):
#         return None, None

#     # Переход к карточке товара
#     try:
#         if state == "cards":
#             link = page.locator(
#                 "project-ui-article-card a, app-article-card-tile a"
#             ).first
#         elif state == "list":
#             link = page.locator("div.results-list a, .search-result__list a").first
#         else:
#             return None, None

#         if await link.count() == 0:
#             return None, None

#         href = await link.get_attribute("href", timeout=3000)
#         if not href:
#             return None, None

#         full_url = href if href.startswith("http") else "https://armtek.ru" + href

#         # Переход на карточку
#         await page.goto(full_url, wait_until="domcontentloaded", timeout=30000)

#         # Ждём загрузки данных (SPA!)
#         await page.wait_for_selector("product-card-info", state="visible", timeout=8000)

#         # Даём время на рендер JSON → HTML
#         for _ in range(10):
#             content = await page.locator("product-card-info").text_content()
#             if content and len(content.strip()) > 20:
#                 break
#             await page.wait_for_timeout(300)

#     except Exception as e:
#         logger.error(f"Ошибка навигации к карточке: {e}")
#         await save_debug_info(page, part, "card_error", logger, "armtek")
#         return None, None

#     # Парсинг веса (3 попытки)
#     weight = await extract_weight(page)
#     if weight:
#         # logger.info(f"✅ Вес: {weight} ({part})")
#         # return "NeedProxy", None
#         return weight, None

#     # Попытка 2: клик по вкладке характеристик
#     try:
#         tech_tab = page.locator('a[href="#tech-info"]')
#         if await tech_tab.count() > 0:
#             await tech_tab.click()
#             await page.wait_for_timeout(1000)
#             weight = await extract_weight(page)
#     except Exception:
#         pass

#     if weight:
#         logger.info(f"✅ Вес (после клика): {weight} ({part})")
#         return weight, None

#     # Попытка 3: последний шанс
#     await page.wait_for_timeout(2000)
#     weight = await extract_weight(page)

#     if weight:
#         logger.info(f"✅ Вес (delayed): {weight} ({part})")
#         return weight, None

#     logger.warning(f"❌ Вес не найден: {part}")
#     return None, None

# 🎮 Счётчик вызовов
_call_counter = 0


async def parse_weight_armtek(
    page: Page, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    """
    🎮 СИМУЛЯТОР - загружает страницу, делает скриншот, возвращает сценарий
    """
    global _call_counter
    _call_counter += 1

    N = 5  # Размер цикла

    logger.info(f"🎮 [SIM] Вызов #{_call_counter} | Артикул: {part}")

    # Закрываем диалог города
    await close_city_dialog(page)

    # Ждём любого селектора
    state = await determine_state(page)
    logger.info(f"🎮 [SIM] Состояние страницы: {state}")

    # Скриншот
    await save_debug_info(
        page, part, f"simulator_call_{_call_counter}_state_{state}", logger, "armtek"
    )

    # Определяем сценарий по номеру вызова
    if _call_counter <= N:
        # Цикл 1: N задач - обычный режим
        logger.info(f"✅ [SIM] Цикл 1: Обычный режим ({_call_counter}/{N})")
        return None, None

    elif _call_counter <= N * 2:
        # Цикл 2: N*2 задач - NeedProxy
        logger.warning(f"🚦 [SIM] Цикл 2: NeedProxy ({_call_counter}/{N*2})")
        return "NeedProxy", "NeedProxy"

    elif _call_counter <= N * 3:
        # Цикл 3: N*3 задач - NeedProxy снова
        logger.warning(f"🚦 [SIM] Цикл 3: NeedProxy снова ({_call_counter}/{N*3})")
        return "NeedProxy", "NeedProxy"

    elif _call_counter <= N * 4:
        # Цикл 4: N*4 задач - CloudFlare
        logger.warning(f"☁️ [SIM] Цикл 4: CloudFlare ({_call_counter}/{N*4})")
        return "CloudFlare", "CloudFlare"

    else:
        # После всех циклов - обычный режим
        logger.info(f"✅ [SIM] После циклов: Обычный режим")
        return None, None


async def extract_weight(page: Page) -> Optional[str]:
    """Извлечение веса из DOM"""
    selectors = [
        "product-card-info div:has-text('Вес')",
        "product-card-info tr:has-text('Вес')",
        ".product-params__item:has-text('Вес')",
        "div.params-row:has-text('Вес')",
        "li:has-text('Вес')",
    ]

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

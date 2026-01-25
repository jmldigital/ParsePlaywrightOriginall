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


# async def close_city_dialog_if_any(page: Page, logger: logging.Logger):
#     """Закрывает/подтверждает окно выбора города, если оно есть."""
#     try:
#         btn = page.locator("button:has-text('Верно')")
#         count = await btn.count()
#         if count > 0 and await btn.first.is_visible():
#             logger.info("🗺️ Нажимаю кнопку 'Верно'")
#             await btn.first.click()
#             await page.wait_for_timeout(500)
#             return

#         overlay = page.locator("div.geo-control__click-area")
#         ov_count = await overlay.count()
#         if ov_count > 0 and await overlay.first.is_visible():
#             logger.info("🗺️ Кликаю по geo-control__click-area для закрытия диалога")
#             await overlay.first.click()
#             await page.wait_for_timeout(500)
#             return
#     except Exception as e:
#         logger.warning(f"⚠️ Ошибка при закрытии диалога города: {e}")


# async def detect_cloudflare(page, part, logger):
#     """🔍 Ловит ЛЮБОЙ Cloudflare"""
#     await page.wait_for_timeout(CLOUD_FLARE_DETEKTOR)

#     cf_indicators = [
#         "text='Проверяем, человек ли вы'",
#         ".lds-ring",
#         "input.ctp-button",
#         ".ctp-button",
#         "#cf-chl-widget",
#         "div.main-content h1.zone-name-title",
#         "#NNbwm6",
#     ]

#     content = await page.content()
#     if any(
#         marker in content for marker in ["challenge-oper", "cf-browser", "ctp-button"]
#     ):
#         logger.warning(f"☁️ CLOUDFLARE HTML: {part}")
#         return True

#     for selector in cf_indicators:
#         if await page.locator(selector).count() > 0:
#             logger.warning(f"☁️ CLOUDFLARE ({selector}): {part}")
#             return True

#     return False


# async def diagnose_error_state(page: Page, part: str, logger):
#     """🎯 100% ЛОВИТ КАПЧУ с правильными таймаутами"""

#     # ⏰ 6 сек на полную загрузку + анимации
#     await page.wait_for_timeout(6000)

#     # 🔥 0️⃣ CLOUDFLARE!
#     try:
#         if await detect_cloudflare(page, part, logger):
#             await save_debug_info(page, part, "cloudflare", logger, "armtek")
#             return "cloudflare"
#     except Exception as e:
#         logger.debug(f"Cloudflare check fail: {e}")
#         pass

#     # 1️⃣ RATE LIMIT
#     try:
#         rate_limit_modal = page.locator(
#             "sproit-ui-modal p:has-text('Превышен лимит запросов')"
#         )
#         await rate_limit_modal.wait_for(
#             state="visible", timeout=4000
#         )  # ✅ Ждём появления!
#         logger.warning(f"🚫 Rate limit detected: {part}")
#         await save_debug_info(page, part, "rate_limit", logger, "armtek")
#         return "rate_limit"
#     except Exception:
#         pass  # Rate limit нет → продолжаем

#     # 2️⃣ CAPTCHA — ТОЧНЫЙ СЕЛЕКТОР!
#     try:
#         captcha_modal = page.locator(
#             "sproit-ui-modal p:has-text('Введите код с картинки')"
#         )
#         await captcha_modal.wait_for(
#             state="visible", timeout=4000
#         )  # ✅ Ждём появления!
#         logger.info(f"🎯 CAPTCHA НАЙДЕНА: {part}")
#         await save_debug_info(page, part, "captcha_detected", logger, "armtek")
#         return "captcha_detected"
#     except Exception:
#         pass

#     # # 3️⃣ NOTHING FOUND — АНАЛОГ CAPTCHA СЕЛЕКТОРА!
#     # try:
#     #     no_result_text = page.locator(
#     #         "project-ui-search-result p:has-text('По вашему запросу ничего не найдено')"
#     #     )
#     #     await no_result_text.wait_for(
#     #         state="visible", timeout=4000
#     #     )  # ✅ Ждём появления!
#     #     logger.info(f"❌ No results found we are cach it!!: {part}")
#     #     # await save_debug_info(page, part, "no_search_results", logger, "armtek")
#     #     return "no_search_results"
#     # except Exception:
#     #     pass

#     logger.warning(f"⏰ No cards → global timeout: {part}")
#     await save_debug_info(page, part, "global_timeout", logger, "armtek")
#     return "global_timeout"


# async def scrape_weight_armtek(
#     page: Page, part: str, logger
# ) -> Tuple[str | None, str | None]:
#     """
#     Стабильный парсер ARMTEK с ожиданиями + debug.
#     """

#     # 1. Поиск
#     search_url = f"https://armtek.ru/search?text={part}"
#     await page.goto(
#         search_url, wait_until="domcontentloaded", timeout=PAGE_GOTO_TIMEOUT
#     )
#     # это временная пепяка
#     # await page.wait_for_timeout(5000)

#     await close_city_dialog_if_any(page, logger)


#     # ⚡ 0.3 сек максимум на "нет результатов"
#     try:
#         no_result_locator = page.locator(
#             "project-ui-search-result p:has-text('По вашему запросу ничего не найдено')"
#         )
#         if (
#             await no_result_locator.count() > 0
#             and await no_result_locator.first.is_visible()
#         ):
#             logger.info(f"❌ No results: {part}")
#             return None, None
#     except Exception:
#         pass

#     # 2. 🔥 СТАБИЛЬНОЕ ожидание карточек (как в старом)
#     max_card_wait = 4
#     for card_attempt in range(max_card_wait):
#         try:
#             await page.wait_for_selector(
#                 "project-ui-article-card, app-article-card-tile, .scroll-item, div[data-id]",
#                 timeout=PAGE_GOTO_TIMEOUT,
#                 state="attached",
#             )
#             await page.wait_for_timeout(1500)  # ✅ Стабилизация!
#             logger.debug(f"✅ Карточки #{card_attempt+1}")
#             break
#         except PlaywrightTimeout:
#             if card_attempt < max_card_wait - 1:
#                 logger.debug(f"⏳ Ждём карточки #{card_attempt+1}")
#                 await page.wait_for_timeout(1000)
#             else:
#                 error_type = await diagnose_error_state(page, part, logger)
#                 if error_type == "rate_limit":
#                     return "NeedProxy", "NeedProxy"
#                 elif error_type == "captcha_detected":
#                     return "NeedCaptcha", "NeedCaptcha"
#                 elif error_type == "cloudflare":
#                     return "ClaudFlare", "ClaudFlare"
#                 elif error_type == "no_search_results":
#                     return None, None
#                 else:
#                     return None, None

#     # 3. 🔥 Множественные селекторы (как в старом)
#     card_selectors = [
#         "project-ui-article-card",
#         "app-article-card-tile",
#         SELECTORS["armtek"]["product_cards"],
#     ]

#     products = None
#     for sel_name, selector in [
#         ("article-card", card_selectors[0]),
#         ("app-tile", card_selectors[1]),
#         *[(f"backup-{i}", s) for i, s in enumerate(card_selectors[2:], 1)],
#     ]:
#         try:
#             count = await page.locator(selector).count()
#             if count > 0:
#                 logger.debug(f"✅ {sel_name}: {count} карт.")
#                 products = page.locator(selector)
#                 break
#         except Exception:
#             logger.debug(f"{sel_name} skip")

#     if not products or await products.count() == 0:
#         logger.warning(f"❌ Нет продуктов: {part}")
#         await save_debug_info(page, part, "no_products", logger, "armtek")
#         return None, None

#     # 4. Первая карточка
#     first_card = products.first
#     href = await first_card.locator("a").first.get_attribute("href", timeout=3000)
#     if not href:
#         logger.warning(f"❌ Нет ссылки: {part}")
#         return None, None

#     full_url = href if href.startswith("http") else "https://armtek.ru" + href
#     await page.goto(full_url, wait_until="domcontentloaded", timeout=PAGE_GOTO_TIMEOUT)

#     # 5. 🔥 Вес с стабилизацией (как в старом)
#     await page.wait_for_load_state("domcontentloaded", timeout=PAGE_GOTO_TIMEOUT)
#     await page.evaluate("window.scrollTo(0, 0)")
#     await page.wait_for_timeout(2000)

#     card_info = page.locator("product-card-info")
#     if await card_info.count() == 0:
#         logger.warning(f"❌ Нет card_info: {part}")
#         return None, None

#     # Тех. характеристики
#     tech_link = page.locator('a[href="#tech-info"]').first
#     if await tech_link.count() > 0 and await tech_link.is_visible():
#         await tech_link.click(force=True)
#         await card_info.wait_for(state="visible", timeout=5000)

#     # 🔥 Поиск веса с retry (как в старом)
#     weight_selectors_list = SELECTORS["armtek"]["weight_selectors"]
#     for weight_retry in range(2):
#         for selector_idx, selector in enumerate(weight_selectors_list, 1):
#             try:
#                 full_selector = f"product-card-info {selector}".strip()
#                 weight_values = page.locator(full_selector)
#                 count = await weight_values.count()

#                 logger.debug(f"🔍 Вес #{selector_idx}: {count} (retry={weight_retry})")

#                 for i in range(count):
#                     try:
#                         timeout_ms = 3000 if weight_retry > 0 else 1000
#                         text = await weight_values.nth(i).text_content(
#                             timeout=timeout_ms
#                         )

#                         if text and "кг" in str(text).lower():
#                             match = re.search(
#                                 r"(\d+(?:[.,]\d+)?)\s*кг", str(text), re.IGNORECASE
#                             )
#                             if match:
#                                 weight = match.group(1).replace(",", ".")
#                                 logger.info(f"✅ ARMTEK {part}: {weight} кг")
#                                 return weight, None
#                     except:
#                         continue
#             except Exception as e:
#                 logger.debug(f"Селектор #{selector_idx} error: {e}")

#         if weight_retry == 0:
#             await page.wait_for_timeout(2000)  # Пауза между retry

#     # ❌ Вес не найден + DEBUG
#     logger.warning(f"❌ ARMTEK {part}: вес не найден")
#     await save_debug_info(page, part, "weight_not_found", logger, "armtek")  # ✅ Скрин!
#     return None, None

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
    sel_no_results = (
        "project-ui-search-result p:has-text('По вашему запросу ничего не найдено')"
    )
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
            page.wait_for_selector(sel_cloudflare, state="attached", timeout=5000)
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
        return None, None
    elif state == "captcha":
        return "NeedCaptcha", "NeedCaptcha"
    elif state == "rate_limit":
        return "NeedProxy", "NeedProxy"
    elif state == "cloudflare":
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

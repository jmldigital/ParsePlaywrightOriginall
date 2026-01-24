"""
Асинхронный парсер armtek.ru для поиска ФИЗИЧЕСКОГО веса
С обработкой капчи через 2Captcha!
"""

import re
import os
from typing import Callable, Tuple

from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from config import (
    SELECTORS,
    # Ключ 2Captcha из config.py
)
from utils import (
    get_site_logger,
    save_debug_info,
)  # 🆕 ИЗ utils.py!


import logging

logger = get_site_logger("armtek")

BASE_URL = "https://armtek.ru"
WAIT_TIMEOUT = 15000  # Больше для капчи
os.makedirs("debug_armtek", exist_ok=True)


async def close_city_dialog_if_any(page: Page, logger: logging.Logger):
    """Закрывает/подтверждает окно выбора города, если оно есть."""
    try:
        btn = page.locator("button:has-text('Верно')")
        count = await btn.count()
        if count > 0 and await btn.first.is_visible():
            logger.info("🗺️ Нажимаю кнопку 'Верно'")
            await btn.first.click()
            await page.wait_for_timeout(500)
            return

        overlay = page.locator("div.geo-control__click-area")
        ov_count = await overlay.count()
        if ov_count > 0 and await overlay.first.is_visible():
            logger.info("🗺️ Кликаю по geo-control__click-area для закрытия диалога")
            await overlay.first.click()
            await page.wait_for_timeout(500)
            return
    except Exception as e:
        logger.warning(f"⚠️ Ошибка при закрытии диалога города: {e}")


async def diagnose_error_state(page: Page, part: str, logger):
    """🎯 100% ЛОВИТ КАПЧУ с правильными таймаутами"""

    # ⏰ 6 сек на полную загрузку + анимации
    await page.wait_for_timeout(6000)

    # 1️⃣ RATE LIMIT
    try:
        rate_limit_modal = page.locator(
            "sproit-ui-modal p:has-text('Превышен лимит запросов')"
        )
        await rate_limit_modal.wait_for(
            state="visible", timeout=4000
        )  # ✅ Ждём появления!
        logger.warning(f"🚫 Rate limit detected: {part}")
        await save_debug_info(page, part, "rate_limit", logger, "armtek")
        return "rate_limit"
    except Exception:
        pass  # Rate limit нет → продолжаем

    # 2️⃣ CAPTCHA — ТОЧНЫЙ СЕЛЕКТОР!
    try:
        captcha_modal = page.locator(
            "sproit-ui-modal p:has-text('Введите код с картинки')"
        )
        await captcha_modal.wait_for(
            state="visible", timeout=4000
        )  # ✅ Ждём появления!
        logger.info(f"🎯 CAPTCHA НАЙДЕНА: {part}")
        await save_debug_info(page, part, "captcha_detected", logger, "armtek")
        return "captcha_detected"
    except Exception:
        pass

    # 3️⃣ NOTHING FOUND — АНАЛОГ CAPTCHA СЕЛЕКТОРА!
    try:
        no_result_text = page.locator(
            "project-ui-search-result p:has-text('По вашему запросу ничего не найдено')"
        )
        await no_result_text.wait_for(
            state="visible", timeout=4000
        )  # ✅ Ждём появления!
        logger.info(f"❌ No results found we are cach it!!: {part}")
        # await save_debug_info(page, part, "no_search_results", logger, "armtek")
        return "no_search_results"
    except Exception:
        pass

    logger.warning(f"⏰ No cards → global timeout: {part}")
    await save_debug_info(page, part, "global_timeout", logger, "armtek")
    return "global_timeout"


# async def scrape_weight_armtek(
#     page: Page, part: str, logger
# ) -> Tuple[str | None, str | None]:
#     """
#     Простой парсер ARMTEK:
#     - Находит капчу → return "NeedCaptcha"
#     - RateLimit → return "NeedProxy", "NeedProxy"
#     - Нет результатов → None, None
#     - Вес → "1.23", None
#     """

#     # 1. Переход на поиск
#     search_url = f"https://armtek.ru/search?text={part}"
#     await page.goto(search_url, wait_until="domcontentloaded", timeout=15000)

#     # 2. Закрытие города
#     await close_city_dialog_if_any(page, logger)

#     # 3. Ждём карточки (15 сек)
#     try:
#         await page.wait_for_selector(
#             "project-ui-article-card, app-article-card-tile, .scroll-item, div[data-id]",
#             timeout=15000,
#         )
#     except PlaywrightTimeout:
#         # 🎯 ДИАГНОСТИКА состояний
#         error_type = await diagnose_error_state(page, part, logger)
#         if error_type == "rate_limit":
#             return "NeedProxy", "NeedProxy"
#         elif error_type == "captcha_detected":
#             return "NeedCaptcha", "NeedCaptcha"  # ← КРИТИЧНО!
#         elif error_type == "no_search_results":
#             return None, None
#         else:
#             return None, None

#     # 4. Берём первую карточку → вес
#     products = page.locator("project-ui-article-card, app-article-card-tile")
#     if await products.count() == 0:
#         return None, None

#     first_card = products.first
#     href = await first_card.locator("a").first.get_attribute("href")
#     if not href:
#         return None, None

#     # 5. Открываем карточку → ищем вес
#     full_url = href if href.startswith("http") else "https://armtek.ru" + href
#     await page.goto(full_url, wait_until="domcontentloaded", timeout=15000)

#     # Тех. характеристики
#     tech_link = page.locator('a[href="#tech-info"]').first
#     if await tech_link.count() > 0:
#         await tech_link.click()

#     # Ищем вес по селекторам
#     weight_selectors = SELECTORS["armtek"]["weight_selectors"]
#     for selector in weight_selectors:
#         weights = page.locator(f"product-card-info {selector}")
#         count = await weights.count()

#         for i in range(count):
#             try:
#                 text = await weights.nth(i).text_content(timeout=2000)
#                 if text and "кг" in str(text).lower():
#                     match = re.search(
#                         r"(\d+(?:[.,]\d+)?)\s*кг", str(text), re.IGNORECASE
#                     )
#                     if match:
#                         weight = match.group(1).replace(",", ".")
#                         logger.info(f"✅ ARMTEK {part}: {weight} кг")
#                         return weight, None
#             except:
#                 continue

#     logger.warning(f"❌ ARMTEK {part}: вес не найден")
#     await save_debug_info(page, part, "not_found", logger, "armtek")
#     return None, None


async def scrape_weight_armtek(
    page: Page, part: str, logger
) -> Tuple[str | None, str | None]:
    """
    Стабильный парсер ARMTEK с ожиданиями + debug.
    """

    # 1. Поиск
    search_url = f"https://armtek.ru/search?text={part}"
    await page.goto(search_url, wait_until="domcontentloaded", timeout=15000)
    await close_city_dialog_if_any(page, logger)

    # 2. 🔥 СТАБИЛЬНОЕ ожидание карточек (как в старом)
    max_card_wait = 4
    for card_attempt in range(max_card_wait):
        try:
            await page.wait_for_selector(
                "project-ui-article-card, app-article-card-tile, .scroll-item, div[data-id]",
                timeout=10000,
                state="attached",
            )
            await page.wait_for_timeout(1500)  # ✅ Стабилизация!
            logger.debug(f"✅ Карточки #{card_attempt+1}")
            break
        except PlaywrightTimeout:
            if card_attempt < max_card_wait - 1:
                logger.debug(f"⏳ Ждём карточки #{card_attempt+1}")
                await page.wait_for_timeout(1000)
            else:
                error_type = await diagnose_error_state(page, part, logger)
                if error_type == "rate_limit":
                    return "NeedProxy", "NeedProxy"
                elif error_type == "captcha_detected":
                    return "NeedCaptcha", "NeedCaptcha"
                elif error_type == "no_search_results":
                    return None, None
                else:
                    return None, None

    # 3. 🔥 Множественные селекторы (как в старом)
    card_selectors = [
        "project-ui-article-card",
        "app-article-card-tile",
        SELECTORS["armtek"]["product_cards"],
    ]

    products = None
    for sel_name, selector in [
        ("article-card", card_selectors[0]),
        ("app-tile", card_selectors[1]),
        *[(f"backup-{i}", s) for i, s in enumerate(card_selectors[2:], 1)],
    ]:
        try:
            count = await page.locator(selector).count()
            if count > 0:
                logger.debug(f"✅ {sel_name}: {count} карт.")
                products = page.locator(selector)
                break
        except Exception:
            logger.debug(f"{sel_name} skip")

    if not products or await products.count() == 0:
        logger.warning(f"❌ Нет продуктов: {part}")
        await save_debug_info(page, part, "no_products", logger, "armtek")
        return None, None

    # 4. Первая карточка
    first_card = products.first
    href = await first_card.locator("a").first.get_attribute("href", timeout=3000)
    if not href:
        logger.warning(f"❌ Нет ссылки: {part}")
        return None, None

    full_url = href if href.startswith("http") else "https://armtek.ru" + href
    await page.goto(full_url, wait_until="domcontentloaded", timeout=20000)

    # 5. 🔥 Вес с стабилизацией (как в старом)
    await page.wait_for_load_state("domcontentloaded", timeout=5000)
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(2000)

    card_info = page.locator("product-card-info")
    if await card_info.count() == 0:
        logger.warning(f"❌ Нет card_info: {part}")
        return None, None

    # Тех. характеристики
    tech_link = page.locator('a[href="#tech-info"]').first
    if await tech_link.count() > 0 and await tech_link.is_visible():
        await tech_link.click(force=True)
        await card_info.wait_for(state="visible", timeout=5000)

    # 🔥 Поиск веса с retry (как в старом)
    weight_selectors_list = SELECTORS["armtek"]["weight_selectors"]
    for weight_retry in range(2):
        for selector_idx, selector in enumerate(weight_selectors_list, 1):
            try:
                full_selector = f"product-card-info {selector}".strip()
                weight_values = page.locator(full_selector)
                count = await weight_values.count()

                logger.debug(f"🔍 Вес #{selector_idx}: {count} (retry={weight_retry})")

                for i in range(count):
                    try:
                        timeout_ms = 3000 if weight_retry > 0 else 1000
                        text = await weight_values.nth(i).text_content(
                            timeout=timeout_ms
                        )

                        if text and "кг" in str(text).lower():
                            match = re.search(
                                r"(\d+(?:[.,]\d+)?)\s*кг", str(text), re.IGNORECASE
                            )
                            if match:
                                weight = match.group(1).replace(",", ".")
                                logger.info(f"✅ ARMTEK {part}: {weight} кг")
                                return weight, None
                    except:
                        continue
            except Exception as e:
                logger.debug(f"Селектор #{selector_idx} error: {e}")

        if weight_retry == 0:
            await page.wait_for_timeout(2000)  # Пауза между retry

    # ❌ Вес не найден + DEBUG
    logger.warning(f"❌ ARMTEK {part}: вес не найден")
    await save_debug_info(page, part, "weight_not_found", logger, "armtek")  # ✅ Скрин!
    return None, None

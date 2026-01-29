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


async def wait_skeletons_gone(page: Page, logger, timeout: int = 30000) -> bool:
    """Ждём исчезновение скелетонов в product-card"""
    try:
        # ✅ Конкретно скелетоны в карточке товара
        skeletons = page.locator(".product-card sproit-ui-skeleton")
        await skeletons.wait_for(state="hidden", timeout=timeout)
        logger.debug("✅ Скелетоны в карточке исчезли")
        return True
    except:
        logger.warning("⚠️ Скелетоны в карточке висят")
        return False


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
            page.wait_for_selector(
                f"{sel}:has(*) >> nth=0", state="visible", timeout=20000
            )
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

    # 🔥 1️⃣ Ждём исчезновения скелетонов (ВСЕГДА!)
    if not await wait_skeletons_gone(page, logger, timeout=30000):
        logger.warning(f"⏳ Скелетоны не исчезли: {part}")
        await save_debug_info(page, part, "skeleton_timeout", logger, "armtek")
        return None, None

    # Определяем состояние
    state = await determine_state(page)

    logger.debug(f"✅ Состояние для {page} найдено {state}")

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
            list_selectors = SELECTORS["armtek"]["product_list"]
            link = page.locator(f"{list_selectors} a[href*='/product/']").first
            count = await link.count()
            if count > 0:
                href = await link.get_attribute("href")
                text = await link.text_content()
                logger.debug(f"🔗 List FOUND: href='{href}' | text='{text[:50]}...'")
            else:
                logger.debug("🔗 List: 0 ссылок")
        elif state == "product_info":
            list_selectors = SELECTORS["armtek"]["product_list"]
            link = page.locator(f"{list_selectors} a[href*='/product/']").first
            count = await link.count()
            if count > 0:
                href = await link.get_attribute("href")
                text = await link.text_content()
                logger.debug(
                    f"🔗 product_info FOUND: href='{href}' | text='{text[:50]}...'"
                )
            else:
                logger.debug("🔗 product_info: 0 ссылок")
        else:
            return None, None

        if await link.count() == 0:
            return None, None

        href = await link.get_attribute("href", timeout=3000)
        if not href:
            return None, None

        full_url = href if href.startswith("http") else "https://armtek.ru" + href

        logger.debug(f"✅  {page} {state} формируем ссылку для перехода {full_url} ")

        # Переход на карточку
        await page.goto(full_url, wait_until="domcontentloaded", timeout=30000)

        # 🔥 1️⃣ Ждём исчезновения скелетонов (ВСЕГДА!)
        if not await wait_skeletons_gone(page, logger, timeout=30000):
            logger.warning(f"⏳ Скелетоны не исчезли: {part}")
            await save_debug_info(page, part, "skeleton_timeout", logger, "armtek")
            return None, None

        # Ждём появления ссылки "Все характеристики" href="#tech-info"
        tech_link_selector = 'a[href="#tech-info"]'
        await page.locator(tech_link_selector).first.wait_for(
            state="visible", timeout=30000
        )

        # Проверяем, что ссылка кликабельна и имеет текст
        tech_link = page.locator(tech_link_selector).first
        link_text = await tech_link.text_content()
        logger.debug(f"🔗 Tech link найдена: '{link_text}'")

        # КЛИК по ссылке для загрузки характеристик
        await tech_link.click()
        await page.wait_for_timeout(1500)  # Даём время на рендер вкладки

    except Exception as e:
        logger.error(f"Ошибка навигации к карточке: {e}")
        await save_debug_info(page, part, "card_error", logger, "armtek")
        return None, None

    # Парсинг веса (3 попытки)
    weight = await extract_weight(page, part, logger)
    if weight:
        logger.debug(f"🎯 Вес: {weight} ({part})")
        # return "NeedProxy", None
        return weight, None

    # Попытка 3: последний шанс
    await page.wait_for_timeout(2000)
    weight = await extract_weight(page, part, logger)

    if weight:
        logger.info(f"🎯 Вес (delayed): {weight} ({part})")
        return weight, None

    logger.warning(f"❌ Вес не найден: {part}")
    await save_debug_info(page, part, "not_found", logger, "armtek")
    return None, None


# async def extract_weight(page: Page) -> Optional[str]:
#     """Извлечение веса из DOM"""
#     selectors = [SELECTORS["armtek"]["weight_selectors"]]

#     for sel in selectors:
#         try:
#             elements = page.locator(sel)
#             count = await elements.count()
#             for i in range(count):
#                 text = await elements.nth(i).text_content()
#                 if text:
#                     match = re.search(
#                         r"(\d+(?:[.,]\d+)?)\s*(?:кг|kg)", text, re.IGNORECASE
#                     )
#                     if match:
#                         return match.group(1).replace(",", ".")
#         except Exception:

#             pass
#             continue

#     return None


async def extract_weight(page: Page, part: str, logger) -> Optional[str]:
    """Извлечение веса из DOM + скриншот если fail"""
    selectors = [SELECTORS["armtek"]["product-card-weight"]]

    for sel in selectors:
        try:
            elements = page.locator(sel)
            count = await elements.count()
            logger.debug(f"🔍 {sel}: {count} элементов")

            for i in range(count):
                text = await elements.nth(i).text_content()
                if text:
                    match = re.search(
                        r"(\d+(?:[.,]\d+)?)\s*(?:кг|kg)", text, re.IGNORECASE
                    )
                    if match:
                        logger.debug(f"✅ Вес найден: {match.group(1)}")
                        return match.group(1).replace(",", ".")
        except Exception as e:
            logger.debug(f"❌ Селектор {sel}: {e}")
            continue

    # 🔥 СКРИНШОТ если вес НЕТ НАЙДЕН
    try:
        await save_debug_info(page, part, "no_weight_found", logger, "armtek")
        logger.warning(f"📸 Скриншот сохранён: no_weight_{part}")
    except Exception as e:
        logger.error(f"❌ Скриншот failed: {e}")

    return None

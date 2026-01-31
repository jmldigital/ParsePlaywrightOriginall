"""
Armtek парсер - ТОЛЬКО парсинг DOM
Навигация делается в Crawlee!
"""

import re
import asyncio
import time
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


async def determine_state(page: Page, timeout: int = 5000) -> tuple[str, str]:
    """
    Определяет состояние страницы после загрузки

    Returns:
        tuple[str, str]: (state_name, matched_selector)

    States:
        ✅ READY (можно парсить):
            - cards: список товаров
            - card_direct: карточка товара (характеристики видны)
            - product_info: инфо о товаре
            - no_results: ничего не найдено

        🚫 BLOCKING (нужно действие):
            - captcha: капча
            - rate_limit: блокировка по лимиту
            - cloudflare: CloudFlare challenge

        ⏳ LOADING (нужно ждать):
            - loading: прелоадер/скелетоны видны

        ❌ ERROR:
            - timeout: ничего не нашлось
            - error: ошибка
    """

    # 🔥 ВСЕ состояния в одном race (включая loading)
    selectors = {
        # Ready states (высокий приоритет - проверяем с :has(*) для непустых)
        "cards": SELECTORS["armtek"]["product_card-list"],
        "no_results": SELECTORS["armtek"]["no_results"],
        "captcha": SELECTORS["armtek"]["captcha"],
        "rate_limit": SELECTORS["armtek"]["rate_limit"],
        "cloudflare": SELECTORS["armtek"]["rate_limit"],
        "card_direct": SELECTORS["armtek"]["specifications"],
        "product_info": SELECTORS["armtek"]["product-card-info"],
        # 🆕 Loading states (ищем ВИДИМЫЕ прелоадеры/скелетоны)
        "loading": SELECTORS["armtek"]["loading"],
    }

    tasks = {}

    for state_name, selector_string in selectors.items():
        individual_selectors = [
            s.strip() for s in selector_string.split(",") if s.strip()
        ]

        for sel in individual_selectors:
            # Для loading - просто ищем видимый элемент
            # Для остальных - ищем непустой (:has(*))
            if state_name == "loading":
                full_sel = sel  # уже содержит :visible
            else:
                full_sel = f"{sel}:has(*) >> nth=0"

            task = asyncio.create_task(
                page.wait_for_selector(full_sel, state="visible", timeout=timeout)
            )
            tasks[task] = (state_name, sel)

    done, pending = await asyncio.wait(
        tasks.keys(), return_when=asyncio.FIRST_COMPLETED
    )

    # Отменяем остальные
    for task in pending:
        task.cancel()

    try:
        first_task = list(done)[0]
        await first_task  # Проверяем на ошибки
        state_name, matched_selector = tasks[first_task]
        return state_name, matched_selector

    except PlaywrightTimeout:
        return "timeout", ""
    except Exception:
        return "error", ""


# async def wait_for_ready_state(
#     page: Page, logger, timeout: int = 30000, poll_interval: int = 500
# ) -> tuple[str, str]:
#     """
#     🔥 Ждёт пока страница перейдёт в готовое состояние
#     Крутится в цикле пока state == "loading"

#     Args:
#         page: Playwright page
#         logger: логгер
#         timeout: общий таймаут (мс)
#         poll_interval: интервал между проверками (мс)

#     Returns:
#         tuple[str, str]: (state_name, matched_selector)
#     """
#     start_time = time.time()
#     attempt = 0

#     while True:
#         attempt += 1
#         elapsed_ms = (time.time() - start_time) * 1000

#         # 🔥 Проверяем таймаут
#         if elapsed_ms > timeout:
#             logger.warning(
#                 f"⏳ Таймаут ожидания готового состояния ({timeout}ms, {attempt} попыток)"
#             )
#             return "timeout", ""

#         # 🔥 Определяем текущее состояние (короткий таймаут для быстрой проверки)
#         remaining = int(timeout - elapsed_ms)
#         check_timeout = min(3000, remaining)  # Не больше 3 сек на проверку

#         state, selector = await determine_state(page, timeout=check_timeout)

#         # ✅ Готовые состояния - выходим
#         if state in (
#             "cards",
#             "card_direct",
#             "product_info",
#             "no_results",
#             "captcha",
#             "rate_limit",
#             "cloudflare",
#             "error",
#         ):
#             logger.debug(
#                 f"✅ Готовое состояние: {state} (за {elapsed_ms:.0f}ms, {attempt} попыток)"
#             )
#             return state, selector

#         # ⏳ Loading - ждём и повторяем
#         if state == "loading":
#             if attempt == 1:
#                 logger.info(f"⏳ Обнаружен прелоадер/скелетоны, ждём...")
#             elif attempt % 5 == 0:  # Логируем каждые 5 попыток
#                 logger.debug(f"⏳ Всё ещё loading... ({elapsed_ms:.0f}ms)")

#             await page.wait_for_timeout(poll_interval)
#             continue

#         # ❓ Timeout от determine_state - пробуем ещё раз
#         if state == "timeout":
#             logger.debug(f"⏳ Состояние не определено, повторяем... ({attempt})")
#             await page.wait_for_timeout(poll_interval)
#             continue

#         # ❌ Неизвестное состояние
#         logger.warning(f"⚠️ Неизвестное состояние: {state}")
#         return state, selector


async def wait_for_ready_state(
    page: Page, logger, timeout: int = 30000, poll_interval: int = 500
) -> tuple[str, str]:
    """
    Ждёт пока страница перейдёт в готовое состояние

    1. Сначала проверяет что страница "живая" (логотип)
    2. Потом ждёт готового состояния контента
    """
    start_time = time.time()

    # ═══════════════════════════════════════════════════════════════
    # 🔥 ШАГ 1: Проверка "живости" страницы (логотип)
    # ═══════════════════════════════════════════════════════════════

    try:
        logo = page.locator('img[alt="armtek logo"][src*="logo-armtek"]').first
        await logo.wait_for(state="attached", timeout=10000)
        logger.debug("✅ Логотип ARMTEK OK — страница живая")
    except PlaywrightTimeout:
        logger.warning("❌ Логотип не найден — страница не загрузилась")
        return "dead_page", ""
    except Exception as e:
        logger.warning(f"⚠️ Ошибка проверки логотипа: {e}")
        # Продолжаем — возможно страница всё равно работает

    # ═══════════════════════════════════════════════════════════════
    # 🔥 ШАГ 2: Ожидание готового состояния контента
    # ═══════════════════════════════════════════════════════════════

    attempt = 0

    while True:
        attempt += 1
        elapsed_ms = (time.time() - start_time) * 1000

        if elapsed_ms > timeout:
            logger.warning(f"⏳ Таймаут ожидания ({timeout}ms, {attempt} попыток)")
            return "timeout", ""

        remaining = int(timeout - elapsed_ms)
        check_timeout = min(3000, remaining)

        state, selector = await determine_state(page, timeout=check_timeout)

        # ✅ Готовые состояния — выходим
        if state in (
            "cards",
            "card_direct",
            "product_info",
            "no_results",
            "captcha",
            "rate_limit",
            "cloudflare",
            "error",
        ):
            logger.debug(
                f"✅ Состояние: {state} ({elapsed_ms:.0f}ms, {attempt} попыток)"
            )
            return state, selector

        # ⏳ Loading — ждём и повторяем
        if state == "loading":
            if attempt == 1:
                logger.info("⏳ Прелоадер/скелетоны, ждём...")
            elif attempt % 5 == 0:
                logger.debug(f"⏳ Всё ещё loading... ({elapsed_ms:.0f}ms)")

            await page.wait_for_timeout(poll_interval)
            continue

        # Timeout от determine_state — пробуем ещё
        if state == "timeout":
            logger.debug(f"⏳ Состояние не определено, повтор... ({attempt})")
            await page.wait_for_timeout(poll_interval)
            continue

        logger.warning(f"⚠️ Неизвестное состояние: {state}")
        return state, selector


async def parse_weight_armtek(
    page: Page, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    """
    ТОЛЬКО парсинг веса из DOM
    Страница УЖЕ загружена Crawlee на URL поиска
    """

    await close_city_dialog(page)

    # 🔥 ГЛАВНОЕ ИЗМЕНЕНИЕ: ждём готового состояния вместо отдельной функции
    state, container_sel = await wait_for_ready_state(page, logger, timeout=30000)

    logger.debug(f"✅ Состояние для {part}: {state}, селектор: {container_sel}")

    # ═══════════════════════════════════════════════════════════════
    # ОБРАБОТКА СОСТОЯНИЙ
    # ═══════════════════════════════════════════════════════════════

    # 🆕 Страница "мёртвая" — нужен retry
    if state == "dead_page":
        logger.warning(f"💀 [{part}] Страница не загрузилась (нет логотипа)")
        # await save_debug_info(page, part, "dead_page", logger, "armtek")
        return "DeadPage", "DeadPage"  # Crawlee сделает retry

    # 🎯 ПРЯМО НА КАРТОЧКЕ → парсим характеристики
    if state == "card_direct":
        logger.info(f"🎯 [{part}] ПРЯМО НА КАРТОЧКЕ → парсим")
        weight = await extract_weight(page, part, logger)
        return weight or (None, None)

    # 🚫 BLOCKING STATES
    if state == "no_results":
        logger.info(f"❌ [{part}] Ничего не найдено")
        await save_debug_info(page, part, "no_results", logger, "armtek")
        return None, None

    if state == "captcha":
        logger.warning(f"🔒 [{part}] Капча!")
        return "NeedCaptcha", "NeedCaptcha"

    if state == "rate_limit":
        logger.warning(f"🚫 [{part}] Rate limit!")
        return "NeedProxy", "NeedProxy"

    if state == "cloudflare":
        logger.warning(f"☁️ [{part}] CloudFlare!")
        return "CloudFlare", "CloudFlare"

    # ⏳ LOADING - не дождались готового состояния
    if state == "loading":
        logger.warning(f"⏳ [{part}] Прелоадер не исчез за таймаут")
        await save_debug_info(page, part, "loading_timeout", logger, "armtek")
        return "PreloaderState", "PreloaderState"  # 🆕 Новый статус!

    # ❌ ERROR STATES
    if state in ("timeout", "error"):
        logger.warning(f"❌ [{part}] Таймаут/ошибка определения состояния")
        await save_debug_info(page, part, "state_timeout", logger, "armtek")
        return None, None

    # ═══════════════════════════════════════════════════════════════
    # ПЕРЕХОД К КАРТОЧКЕ ТОВАРА
    # ═══════════════════════════════════════════════════════════════

    try:
        link = await find_product_link(page, state, container_sel, logger)

        if not link:
            await save_debug_info(page, part, "no_link", logger, "armtek")
            return None, None

        href = await link.get_attribute("href", timeout=3000)
        if not href:
            await save_debug_info(page, part, "no_href", logger, "armtek")
            return None, None

        full_url = href if href.startswith("http") else "https://armtek.ru" + href
        logger.debug(f"🔗 Переходим: {full_url}")

        # Переход на карточку
        await page.goto(full_url, wait_until="domcontentloaded", timeout=30000)

        # 🔥 Снова ждём готового состояния на карточке
        card_state, _ = await wait_for_ready_state(page, logger, timeout=30000)

        if card_state == "loading":
            logger.warning(f"⏳ [{part}] Карточка не загрузилась (прелоадер)")
            await save_debug_info(page, part, "card_loading_timeout", logger, "armtek")
            return "PreloaderState", "PreloaderState"

        if card_state in ("timeout", "error"):
            logger.warning(f"❌ [{part}] Карточка не загрузилась")
            await save_debug_info(page, part, "card_timeout", logger, "armtek")
            return None, None

        # Ждём и кликаем по "Все характеристики"
        await click_tech_info(page, part, logger)

    except Exception as e:
        logger.error(f"❌ [{part}] Ошибка навигации: {e}")
        await save_debug_info(page, part, "navigation_error", logger, "armtek")
        return None, None

    # ═══════════════════════════════════════════════════════════════
    # ПАРСИНГ ВЕСА
    # ═══════════════════════════════════════════════════════════════

    weight = await extract_weight(page, part, logger)
    if weight:
        logger.debug(f"🎯 [{part}] Вес: {weight}")
        return weight, None

    # Последняя попытка
    await page.wait_for_timeout(2000)
    weight = await extract_weight(page, part, logger)

    if weight:
        logger.debug(f"🎯 [{part}] Вес (delayed): {weight}")
        return weight, None

    logger.warning(f"❌ [{part}] Вес не найден")
    await save_debug_info(page, part, "no_weight", logger, "armtek")
    return None, None


async def find_product_link(page: Page, state: str, container_sel: str, logger):
    """Находит ссылку на товар в зависимости от состояния"""

    link_selectors = [
        f"{container_sel} a.title[href^='/product/']",
        f"{container_sel} a.suggestion-itemtitle-name[href^='/product/']",
        f"{container_sel} a[href^='/product/']",
    ]

    for sel in link_selectors:
        try:
            all_links = await page.locator(sel).all()
            for temp_link in all_links:
                href = await temp_link.get_attribute("href")
                if href:
                    text = await temp_link.text_content()
                    logger.debug(
                        f"🔗 Найдена ссылка: {sel} | href={href} | text={text[:50] if text else ''}..."
                    )
                    return temp_link
        except Exception as e:
            logger.debug(f"⚠️ Селектор {sel}: {e}")
            continue

    return None


async def click_tech_info(page: Page, part: str, logger):
    """Кликает по вкладке 'Все характеристики'"""
    tech_link_selector = 'a[href="#tech-info"]'

    try:
        await page.locator(tech_link_selector).first.wait_for(
            state="visible", timeout=10000
        )
        tech_link = page.locator(tech_link_selector).first
        await tech_link.click()
        await page.wait_for_timeout(1500)
        logger.debug(f"✅ [{part}] Клик по tech-info")
    except PlaywrightTimeout:
        logger.debug(f"⚠️ [{part}] Tech-info не найден, продолжаем")


async def extract_weight(page: Page, part: str, logger) -> Optional[str]:
    """Извлечение веса из DOM"""
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

    return None

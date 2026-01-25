# scraper_avtoformula_async.py
"""
Асинхронный парсер avtoformula.ru через Playwright
С поддержкой ре-логина, ожидания результатов, проверки разлогина
и установки режима "с аналогами".
"""
from PIL import Image, ImageEnhance
import io
import os
import re
import time
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout
from config import SELECTORS, API_KEY_2CAPTCHA
from utils import logger, parse_price, brand_matches
from auth import is_logged_in
import asyncio
import logging
from utils import get_site_logger, solve_captcha_universal

logger = get_site_logger("avtoformula")
from twocaptcha import TwoCaptcha
import base64
import datetime
import httpx

MAX_WAIT_SECONDS = 15
CHECK_INTERVAL = 0.5  # секунды
AUTH_CHECK_INTERVAL = 10  # сек


async def scrape_avtoformula_pw(
    page: Page, brand: str, part: str, logger: logging.Logger
) -> tuple:
    """Асинхронный парсер avtoformula.ru с поддержкой капчи."""
    try:
        # Пробуем сначала стандартный поиск
        await page.goto("https://www.avtoformula.ru", wait_until="networkidle")

        # Устанавливаем режим "без аналогов"
        try:
            mode_select = page.locator("#smode")
            await mode_select.wait_for(state="visible", timeout=5000)
            await mode_select.select_option("A0")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось установить режим 'без аналогов': {e}")

        # Ввод артикула
        article_field = page.locator(f"#{SELECTORS['avtoformula']['article_field']}")
        await article_field.wait_for(state="visible", timeout=10000)
        await article_field.fill(part)
        await page.locator(SELECTORS["avtoformula"]["search_button"]).click()

        # ✅ КРИТИЧНО: Сначала проверяем капчу ОДИН РАЗ
        if await page.locator(SELECTORS["avtoformula"]["captcha_img"]).is_visible():
            logger.warning("⚠️ Обнаружена капча на avtoformula.ru")
            return "NeedCaptcha"

        # Ожидание результатов (БЕЗ повторной проверки капчи!)
        start = time.time()
        while True:
            elapsed = time.time() - start

            # ❌ УБРАЛИ проверку капчи отсюда - она уже решена выше!

            # Проверка "не найдено"
            html = await page.content()
            if "К сожалению, в поставках" in html:
                logger.info(f"🚫 {brand}/{part} не найден")
                return None, None

            # Проверка таблицы
            table = page.locator(SELECTORS["avtoformula"]["results_table"])
            rows = table.locator("tr")
            count = await rows.count()
            if count > 1:
                break

            if elapsed > MAX_WAIT_SECONDS:
                logger.warning(
                    f"⏰ Таймаут ожидания результатов: {brand}/{part}, пробуем прямой URL"
                )
                # ❌ НЕ передаём captcha_solved! Fallback - НОВАЯ страница, может быть новая капча!
                return await fallback_avtoformula_search(page, brand, part, logger)

            await asyncio.sleep(CHECK_INTERVAL)

        # Обработка результатов (как было)
        min_price, min_delivery = None, None
        count = await rows.count()
        for i in range(1, count):
            row = rows.nth(i)
            brand_in_row = (
                await row.locator(SELECTORS["avtoformula"]["brand_cell"]).text_content()
                or ""
            ).strip()
            if not brand_matches(brand, brand_in_row):
                continue

            delivery_text = (
                await row.locator(
                    SELECTORS["avtoformula"]["delivery_cell"]
                ).text_content()
                or ""
            ).strip()
            price_text = (
                await row.locator(SELECTORS["avtoformula"]["price_cell"]).text_content()
                or ""
            ).strip()

            delivery_days_match = re.search(r"\d+", delivery_text)
            if not delivery_days_match:
                continue
            delivery_days = int(delivery_days_match.group())

            price = parse_price(price_text)
            if price is None:
                continue

            if (
                min_delivery is None
                or delivery_days < min_delivery
                or (delivery_days == min_delivery and price < min_price)
            ):
                min_delivery, min_price = delivery_days, price

        if min_price:
            logger.info(f"💰 {brand}/{part}: {min_price} ₽ ({min_delivery} дней)")
            return min_price, f"{min_delivery} дней"
        else:
            logger.info(f"❌ {brand}/{part}: подходящие результаты не найдены")
            return None, None

    except PlaywrightTimeout:
        logger.warning(f"⏰ Таймаут при загрузке страницы: {brand}/{part}")
        return await fallback_avtoformula_search(
            page, brand, part, logger, captcha_solved=False
        )
    except Exception as e:
        error_msg = str(e).lower()
        if "зарегистрируйтесь" in error_msg or "авториз" in error_msg:
            logger.error(f"❗ Разлогин: {e}")
            raise
        else:
            logger.error(f"❗ Ошибка парсинга avtoformula: {e}")
            return None, None


async def fallback_avtoformula_search(
    page: Page,
    brand: str,
    part: str,
    logger: logging.Logger,
    captcha_solved: bool = False,
) -> tuple:
    """
    Fallback-поиск через прямой URL.
    captcha_solved: True если капча уже решена в основной функции
    """
    try:
        fallback_url = f"https://www.avtoformula.ru/search.html?article={part}&smode=A&searchTemplate=default&delivery_time=0&sort___search_results_by=final_price"
        await page.goto(fallback_url, wait_until="networkidle", timeout=45000)
        logger.info(f"Fallback: загружена страница по прямому URL: {fallback_url}")

        await page.wait_for_timeout(3000)

        # ✅ ТОЛЬКО если капча НЕ была решена ранее

        if await page.locator(SELECTORS["avtoformula"]["captcha_img"]).is_visible():
            logger.warning("⚠️ Обнаружена капча на avtoformula.ru (fallback)")
            return "NeedCaptcha"
            # await page.wait_for_timeout(3000)

        # Проверка отсутствия товара
        html = await page.content()
        if (
            "К сожалению, в поставках" in html
            or "не обнаружены" in html
            or "не найдено" in html.lower()
        ):
            logger.info(f"🚫 Fallback: товар не найден для {brand}/{part}")
            return None, None

        # Ждём таблицу
        try:
            await page.wait_for_selector(
                SELECTORS["avtoformula"]["results_table"],
                timeout=15000,
                state="visible",
            )
        except PlaywrightTimeout:
            logger.warning(f"⏰ Fallback: таймаут ожидания таблицы для {brand}/{part}")
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = (
                f"screenshots/timeout_fallback_price_{part}_{timestamp}.png"
            )
            await page.screenshot(path=screenshot_path)
            logger.warning(f"📸 Скриншот таймаута сохранён: {screenshot_path}")
            return None, None

        # Обработка результатов (как было)
        table = page.locator(SELECTORS["avtoformula"]["results_table"])
        rows = table.locator("tr")
        count = await rows.count()

        if count <= 1:
            logger.info(f"Fallback: результаты не найдены для {brand}/{part}")
            return None, None

        min_price, min_delivery = None, None
        for i in range(1, count):
            row = rows.nth(i)
            brand_in_row = (
                await row.locator(SELECTORS["avtoformula"]["brand_cell"]).text_content()
                or ""
            ).strip()
            if not brand_matches(brand, brand_in_row):
                continue

            delivery_text = (
                await row.locator(
                    SELECTORS["avtoformula"]["delivery_cell"]
                ).text_content()
                or ""
            ).strip()
            price_text = (
                await row.locator(SELECTORS["avtoformula"]["price_cell"]).text_content()
                or ""
            ).strip()

            delivery_days_match = re.search(r"\d+", delivery_text)
            if not delivery_days_match:
                continue
            delivery_days = int(delivery_days_match.group())

            price = parse_price(price_text)
            if price is None:
                continue

            if (
                min_delivery is None
                or delivery_days < min_delivery
                or (delivery_days == min_delivery and price < min_price)
            ):
                min_delivery, min_price = delivery_days, price

        if min_price:
            logger.info(
                f"✅ Fallback: найдено {brand}/{part}: {min_price} ₽ ({min_delivery} дней)"
            )
            return min_price, f"{min_delivery} дней"
        else:
            logger.info(
                f"Fallback: подходящие результаты не найдены для {brand}/{part}"
            )
            return None, None

    except PlaywrightTimeout as e:
        logger.warning(f"⏰ Fallback таймаут для {brand}/{part}: {e}")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_path = (
            f"screenshots/timeout_exception_fallback_price_{part}_{timestamp}.png"
        )
        try:
            await page.screenshot(path=screenshot_path)
            logger.warning(f"📸 Скриншот таймаута сохранён: {screenshot_path}")
        except:
            pass


async def scrape_avtoformula_name_async(
    page: Page, part: str, logger: logging.Logger
) -> str:
    """
    Парсер avtoformula.ru для поиска только названия детали по номеру.
    С поддержкой капчи и fallback на прямой URL.
    """
    try:
        await page.goto("https://www.avtoformula.ru", wait_until="networkidle")
        # logger.info(f"🌐 Страница загружена: avtoformula.ru")

        # Ввод артикула
        article_field = page.locator(f"#{SELECTORS['avtoformula']['article_field']}")
        await article_field.wait_for(state="visible", timeout=10000)
        await article_field.fill(part)
        await page.locator(SELECTORS["avtoformula"]["search_button"]).click()
        # logger.info(f"🔍 Поиск артикула: {part}")

        # ✅ КРИТИЧНО: Сначала проверяем капчу ОДИН РАЗ
        if await page.locator(SELECTORS["avtoformula"]["captcha_img"]).is_visible():
            logger.warning("⚠️ Обнаружена капча на avtoformula.ru")
            return "NeedCaptcha"

        # Ожидание появления результатов (БЕЗ повторной проверки капчи!)
        start = time.time()
        while True:
            elapsed = time.time() - start

            # ❌ УБРАЛИ проверку капчи отсюда!

            # Проверка наличия таблицы с результатами
            table_count = await page.locator(
                SELECTORS["avtoformula"]["results_table"]
            ).count()
            if table_count > 0:
                break

            if elapsed > MAX_WAIT_SECONDS:
                logger.warning(
                    f"⏰ Таймаут ожидания результатов для {part}, пробуем прямой URL"
                )
                return await fallback_avtoformula_name_search(
                    page, part, logger, captcha_solved=True
                )

            await asyncio.sleep(CHECK_INTERVAL)

        # Получаем первый элемент с описанием детали
        first_desc_cell_selector = f"{SELECTORS['avtoformula']['results_table']} tr:nth-child(2) td.td_spare_info"
        first_desc = await page.locator(first_desc_cell_selector).text_content()

        if first_desc:
            description = first_desc.strip()
            logger.info(f"{part} название: {description}")
            return description
        else:
            logger.info(f"Название детали avtoformula не найдено для артикула {part}")
            return None

    except PlaywrightTimeout:
        logger.warning(f"⏰ Таймаут ожидания результатов для {part}")
        return await fallback_avtoformula_name_search(
            page, part, logger, captcha_solved=False
        )
    except Exception as e:
        logger.error(f"Ошибка парсинга названия детали avtoformula для {part}: {e}")
        return None


async def fallback_avtoformula_name_search(
    page: Page, part: str, logger: logging.Logger, captcha_solved: bool = False
) -> str:
    """
    Fallback-поиск названия детали через прямой URL.
    captcha_solved: True если капча уже решена в основной функции
    """
    try:
        fallback_url = f"https://www.avtoformula.ru/search.html?article={part}&smode=A&searchTemplate=default&delivery_time=0&sort___search_results_by=final_price"
        await page.goto(fallback_url, wait_until="networkidle", timeout=45000)
        logger.info(f"Fallback: загружена страница по прямому URL: {fallback_url}")

        await page.wait_for_timeout(3000)

        # ✅ ТОЛЬКО если капча НЕ была решена ранее
        if await page.locator(SELECTORS["avtoformula"]["captcha_img"]).is_visible():
            logger.warning("⚠️ Обнаружена капча на avtoformula.ru (fallback)")
            return "NeedCaptcha"

        # Проверка отсутствия товара
        html = await page.content()
        if (
            "К сожалению, в поставках" in html
            or "не обнаружены" in html
            or "не найдено" in html.lower()
        ):
            logger.info(f"🚫 Fallback: товар не найден для {part}")
            return None

        # Ждём таблицу
        try:
            await page.wait_for_selector(
                SELECTORS["avtoformula"]["results_table"],
                timeout=15000,
                state="visible",
            )
        except PlaywrightTimeout:
            logger.warning(f"⏰ Fallback: таймаут ожидания таблицы для {part}")
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            screenshot_path = (
                f"screenshots/timeout_fallback_name_{part}_{timestamp}.png"
            )
            await page.screenshot(path=screenshot_path)
            logger.warning(f"📸 Скриншот таймаута сохранён: {screenshot_path}")
            return None

        first_desc_cell_selector = f"{SELECTORS['avtoformula']['results_table']} tr:nth-child(2) td.td_spare_info"
        first_desc = await page.locator(first_desc_cell_selector).text_content()

        if first_desc:
            description = first_desc.strip()
            logger.info(
                f"✅ Fallback: найдено название детали avtoformula: {description}"
            )
            return description
        else:
            logger.info(
                f"Fallback: название детали avtoformula не найдено для артикула {part}"
            )
            return None

    except PlaywrightTimeout as e:
        logger.warning(f"⏰ Fallback таймаут для {part}: {e}")
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_path = (
            f"screenshots/timeout_exception_fallback_name_{part}_{timestamp}.png"
        )
        try:
            await page.screenshot(path=screenshot_path)
            logger.warning(f"📸 Скриншот таймаута сохранён: {screenshot_path}")
        except:
            pass
        return None
    except Exception as e:
        logger.error(
            f"❌ Fallback ошибка парсинга названия детали avtoformula для {part}: {e}"
        )
        return None

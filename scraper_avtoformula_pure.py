"""
Avtoformula парсер - ТОЛЬКО парсинг DOM
Поиск через форму + fallback на прямой URL
"""

import re
import time
import asyncio
from typing import Tuple, Optional
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout
from config import SELECTORS
from utils import parse_price, brand_matches


MAX_WAIT_SECONDS = 15
CHECK_INTERVAL = 0.5


async def parse_avtoformula_price(
    page: Page, brand: str, part: str, logger
) -> Tuple[Optional[float], Optional[str]]:
    """
    ТОЛЬКО парсинг цены из уже открытой страницы
    Crawlee уже открыл главную страницу Avtoformula
    Мы делаем поиск через форму
    """
    try:
        # Установка режима "без аналогов"
        try:
            mode_select = page.locator("#smode")
            await mode_select.wait_for(state="visible", timeout=5000)
            await mode_select.select_option("A0")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось установить режим A0: {e}")

        # Ввод артикула и поиск
        article_field = page.locator(f"#{SELECTORS['avtoformula']['article_field']}")
        await article_field.wait_for(state="visible", timeout=10000)
        await article_field.fill(part)
        await page.locator(SELECTORS["avtoformula"]["search_button"]).click()

        # Проверка капчи (ОДИН раз)
        if await page.locator(SELECTORS["avtoformula"]["captcha_img"]).is_visible():
            logger.warning("🔒 Капча Avtoformula")
            return "NeedCaptcha", "NeedCaptcha"

        # Ожидание результатов
        start = time.time()
        while True:
            elapsed = time.time() - start

            # Проверка "не найдено"
            html = await page.content()
            if "К сожалению, в поставках" in html:
                logger.info(f"🚫 Avtoformula: не найдено {brand}/{part}")
                return None, None

            # Проверка таблицы
            table = page.locator(SELECTORS["avtoformula"]["results_table"])
            rows = table.locator("tr")
            count = await rows.count()
            if count > 1:
                break

            if elapsed > MAX_WAIT_SECONDS:
                logger.warning(f"⏰ Avtoformula timeout → fallback URL")
                return await _fallback_avtoformula_price(page, brand, part, logger)

            await asyncio.sleep(CHECK_INTERVAL)

        # Парсинг результатов
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
                f"✅ Avtoformula {brand}/{part}: {min_price} ₽ ({min_delivery} дней)"
            )
            return min_price, f"{min_delivery} дней"
        else:
            logger.info(f"❌ Avtoformula: подходящих нет {brand}/{part}")
            return None, None

    except PlaywrightTimeout:
        logger.warning(f"⏰ Avtoformula timeout → fallback")
        return await _fallback_avtoformula_price(page, brand, part, logger)
    except Exception as e:
        error_msg = str(e).lower()
        if "зарегистрируйтесь" in error_msg or "авториз" in error_msg:
            logger.error(f"❗ Разлогин Avtoformula: {e}")
            raise  # Crawlee должен перелогиниться
        else:
            logger.error(f"❌ Avtoformula error: {e}")
            return None, None


async def _fallback_avtoformula_price(
    page: Page, brand: str, part: str, logger
) -> Tuple[Optional[float], Optional[str]]:
    """Fallback через прямой URL"""
    try:
        fallback_url = f"https://www.avtoformula.ru/search.html?article={part}&smode=A&searchTemplate=default&delivery_time=0&sort___search_results_by=final_price"
        await page.goto(fallback_url, wait_until="networkidle", timeout=45000)
        logger.info("📍 Fallback URL загружен")

        await page.wait_for_timeout(3000)

        # Капча
        if await page.locator(SELECTORS["avtoformula"]["captcha_img"]).is_visible():
            logger.warning("🔒 Капча Avtoformula (fallback)")
            return "NeedCaptcha", "NeedCaptcha"

        # Проверка "не найдено"
        html = await page.content()
        if (
            "К сожалению, в поставках" in html
            or "не обнаружены" in html
            or "не найдено" in html.lower()
        ):
            logger.info(f"🚫 Fallback: не найдено {brand}/{part}")
            return None, None

        # Ждём таблицу
        try:
            await page.wait_for_selector(
                SELECTORS["avtoformula"]["results_table"],
                timeout=15000,
                state="visible",
            )
        except PlaywrightTimeout:
            logger.warning(f"⏰ Fallback: таймаут таблицы {brand}/{part}")
            return None, None

        # Парсинг (копия логики из основной функции)
        table = page.locator(SELECTORS["avtoformula"]["results_table"])
        rows = table.locator("tr")
        count = await rows.count()

        if count <= 1:
            logger.info(f"❌ Fallback: нет результатов {brand}/{part}")
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
                f"✅ Fallback: {brand}/{part} = {min_price} ₽ ({min_delivery} дней)"
            )
            return min_price, f"{min_delivery} дней"
        else:
            logger.info(f"❌ Fallback: подходящих нет {brand}/{part}")
            return None, None

    except PlaywrightTimeout:
        logger.warning(f"⏰ Fallback timeout: {brand}/{part}")
        return None, None
    except Exception as e:
        logger.error(f"❌ Fallback error: {e}")
        return None, None


async def parse_avtoformula_name(page: Page, part: str, logger) -> Optional[str]:
    """
    ТОЛЬКО парсинг названия из уже открытой страницы
    Crawlee открыл главную, мы делаем поиск
    """
    try:
        # Ввод артикула
        article_field = page.locator(f"#{SELECTORS['avtoformula']['article_field']}")
        await article_field.wait_for(state="visible", timeout=10000)
        await article_field.fill(part)
        await page.locator(SELECTORS["avtoformula"]["search_button"]).click()

        # Капча (ОДИН раз)
        if await page.locator(SELECTORS["avtoformula"]["captcha_img"]).is_visible():
            logger.warning("🔒 Капча Avtoformula (name)")
            return "NeedCaptcha"

        # Ожидание таблицы
        start = time.time()
        while True:
            elapsed = time.time() - start

            table_count = await page.locator(
                SELECTORS["avtoformula"]["results_table"]
            ).count()
            if table_count > 0:
                break

            if elapsed > MAX_WAIT_SECONDS:
                logger.warning(f"⏰ Avtoformula name timeout → fallback")
                return await _fallback_avtoformula_name(page, part, logger)

            await asyncio.sleep(CHECK_INTERVAL)

        # Получение названия из первой строки результатов
        first_desc_selector = f"{SELECTORS['avtoformula']['results_table']} tr:nth-child(2) td.td_spare_info"
        first_desc = await page.locator(first_desc_selector).text_content()

        if first_desc:
            description = first_desc.strip()
            logger.info(f"✅ Avtoformula name: {description}")
            return description
        else:
            logger.info(f"❌ Avtoformula: название не найдено ({part})")
            return None

    except PlaywrightTimeout:
        logger.warning(f"⏰ Avtoformula name timeout → fallback")
        return await _fallback_avtoformula_name(page, part, logger)
    except Exception as e:
        logger.error(f"❌ Avtoformula name error: {e}")
        return None


async def _fallback_avtoformula_name(page: Page, part: str, logger) -> Optional[str]:
    """Fallback поиск названия через прямой URL"""
    try:
        fallback_url = f"https://www.avtoformula.ru/search.html?article={part}&smode=A&searchTemplate=default&delivery_time=0&sort___search_results_by=final_price"
        await page.goto(fallback_url, wait_until="networkidle", timeout=45000)
        logger.info("📍 Fallback name URL загружен")

        await page.wait_for_timeout(3000)

        # Капча
        if await page.locator(SELECTORS["avtoformula"]["captcha_img"]).is_visible():
            logger.warning("🔒 Капча Avtoformula (fallback name)")
            return "NeedCaptcha"

        # Проверка "не найдено"
        html = await page.content()
        if (
            "К сожалению, в поставках" in html
            or "не обнаружены" in html
            or "не найдено" in html.lower()
        ):
            logger.info(f"🚫 Fallback name: не найдено ({part})")
            return None

        # Ждём таблицу
        try:
            await page.wait_for_selector(
                SELECTORS["avtoformula"]["results_table"],
                timeout=15000,
                state="visible",
            )
        except PlaywrightTimeout:
            logger.warning(f"⏰ Fallback name: таймаут таблицы ({part})")
            return None

        # Название из первой строки
        first_desc_selector = f"{SELECTORS['avtoformula']['results_table']} tr:nth-child(2) td.td_spare_info"
        first_desc = await page.locator(first_desc_selector).text_content()

        if first_desc:
            description = first_desc.strip()
            logger.info(f"✅ Fallback name: {description}")
            return description
        else:
            logger.info(f"❌ Fallback name: не найдено ({part})")
            return None

    except PlaywrightTimeout:
        logger.warning(f"⏰ Fallback name timeout: {part}")
        return None
    except Exception as e:
        logger.error(f"❌ Fallback name error: {e}")
        return None

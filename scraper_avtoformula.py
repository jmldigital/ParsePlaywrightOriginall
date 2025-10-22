# scraper_avtoformula_async.py
"""
Асинхронный парсер avtoformula.ru через Playwright
С поддержкой ре-логина, ожидания результатов, проверки разлогина
и установки режима "с аналогами".
"""

import re
import time
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout
from config import SELECTORS, AVTO_LOGIN, AVTO_PASSWORD
from utils import logger, parse_price, brand_matches
from auth import is_logged_in
import asyncio
import logging
from utils import get_site_logger
logger = get_site_logger("avtoformula")


MAX_WAIT_SECONDS = 45
CHECK_INTERVAL = 0.5  # секунды
AUTH_CHECK_INTERVAL = 10  # сек




async def scrape_avtoformula_pw(page: Page, brand: str, part: str, logger: logging.Logger) -> tuple:
    """Асинхронный парсер avtoformula.ru с передачей логгера."""
    try:
        await page.goto("https://www.avtoformula.ru", wait_until="networkidle")
        logger.info(f"🌐 Страница загружена: avtoformula.ru")

        # Устанавливаем режим "с аналогами"
        try:
            mode_select = page.locator("#smode")
            await mode_select.wait_for(state="visible", timeout=5000)
            await mode_select.select_option("A0")
            logger.info("⚙️ Режим поиска установлен: без аналогов")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось установить режим 'с аналогами': {e}")

        # Ввод артикула
        article_field = page.locator(f"#{SELECTORS['avtoformula']['article_field']}")
        await article_field.wait_for(state="visible", timeout=10000)
        await article_field.fill(part)
        await page.locator(SELECTORS['avtoformula']['search_button']).click()
        logger.info(f"🔍 Поиск артикула: {part}")

        # Ожидание результатов
        start = time.time()
        while True:
            elapsed = time.time() - start


            # Проверка "не найдено"
            html = await page.content()
            if "К сожалению, в поставках" in html:
                logger.info(f"🚫 {brand}/{part} не найден")
                return None, None

            # Проверка таблицы
            table = page.locator(SELECTORS['avtoformula']['results_table'])
            rows = table.locator("tr")
            count = await rows.count()
            if count > 1:
                logger.info(f"✅ Найдено строк: {count - 1}")
                break

            if elapsed > MAX_WAIT_SECONDS:
                logger.warning(f"⏰ Таймаут ожидания результатов: {brand}/{part}")
                return None, None

            await asyncio.sleep(CHECK_INTERVAL)  # не wait_for_timeout

        # Обработка результатов
        min_price, min_delivery = None, None
        count = await rows.count()
        for i in range(1, count):
            row = rows.nth(i)
            brand_in_row = (await row.locator(SELECTORS['avtoformula']['brand_cell']).text_content() or "").strip()
            if not brand_matches(brand, brand_in_row):
                continue

            delivery_text = (await row.locator(SELECTORS['avtoformula']['delivery_cell']).text_content() or "").strip()
            price_text = (await row.locator(SELECTORS['avtoformula']['price_cell']).text_content() or "").strip()

            delivery_days_match = re.search(r'\d+', delivery_text)
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
        return None, None
    except Exception as e:
        error_msg = str(e).lower()
        if "зарегистрируйтесь" in error_msg or "авториз" in error_msg:
            logger.error(f"❗ Разлогин: {e}")
            raise
        else:
            logger.error(f"❗ Ошибка парсинга avtoformula: {e}")
            return None, None



async def scrape_avtoformula_name_async(page: Page, part: str, logger: logging.Logger) -> str:
    """
    Парсер avtoformula.ru для поиска только названия детали по номеру.
    """

    try:
        await page.goto("https://www.avtoformula.ru", wait_until="networkidle")
        logger.info(f"🌐 Страница загружена: avtoformula.ru")

        # Ввод артикула
        article_field = page.locator(f"#{SELECTORS['avtoformula']['article_field']}")
        await article_field.wait_for(state="visible", timeout=10000)
        await article_field.fill(part)
        await page.locator(SELECTORS['avtoformula']['search_button']).click()
        logger.info(f"🔍 Поиск артикула: {part}")

        # Ожидание появления результатов (одна таблица)
        await page.wait_for_selector(SELECTORS['avtoformula']['results_table'], timeout=30000)

        # Получаем первый элемент с описанием детали в колонке td_spare_info
        first_desc_cell_selector = f"{SELECTORS['avtoformula']['results_table']} tr:nth-child(2) td.td_spare_info"
        first_desc = await page.locator(first_desc_cell_selector).text_content()

        if first_desc:
            description = first_desc.strip()
            logger.info(f"Найдено название детали avtoformula: {description}")
            return description
        else:
            logger.info(f"Название детали avtoformula не найдено для артикула {part}")
            return None

    except PlaywrightTimeout:
        logger.warning(f"⏰ Таймаут ожидания результатов для {part}")
        return None
    except Exception as e:
        logger.error(f"Ошибка парсинга названия детали avtoformula для {part}: {e}")
        # await page.screenshot(path=f"screenshots/error_name_avtoformula_{part}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        return None

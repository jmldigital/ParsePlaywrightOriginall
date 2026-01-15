# scraper_stparts_async.py
"""
Асинхронный парсер stparts.ru через Playwright
С поддержкой капчи, fallback-поиска и приоритета "в наличии"
"""
import datetime
import re
import base64
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout
from twocaptcha import TwoCaptcha
from config import SELECTORS, API_KEY_2CAPTCHA
from utils import logger, parse_price, brand_matches
import asyncio
import logging
from utils import get_site_logger, solve_captcha_universal

logger = get_site_logger("stparts")


BASE_URL = "https://stparts.ru"
WAIT_TIMEOUT = 8000  # миллисекунд (8 секунд)


# async def solve_image_captcha_async(page: Page) -> bool:
#     """Решение капчи через 2Captcha"""
#     try:
#         solver = TwoCaptcha(API_KEY_2CAPTCHA)
#         captcha_img = page.locator(SELECTORS["stparts"]["captcha_img"])
#         if not await captcha_img.is_visible():
#             return False

#         # Получаем base64 из Playwright
#         captcha_bytes = await captcha_img.screenshot()
#         captcha_base64 = base64.b64encode(captcha_bytes).decode("utf-8")

#         logger.info("Отправляем капчу на распознавание в 2Captcha")
#         result = await asyncio.to_thread(solver.normal, captcha_base64)
#         captcha_text = result["code"]
#         logger.info(f"Капча распознана: {captcha_text}")

#         input_el = page.locator(SELECTORS["stparts"]["captcha_input"])
#         await input_el.fill(captcha_text)
#         await page.locator(f"#{SELECTORS['stparts']['captcha_submit']}").click()

#         await page.wait_for_timeout(5000)
#         return True
#     except Exception as e:
#         logger.error(f"Ошибка решения капчи: {e}")
#         return False


async def wait_for_results_or_no_results_async(page: Page) -> str:
    """Ожидает появления результатов или блока 'нет результатов'"""
    try:
        await page.wait_for_function(
            """
            (selector) => document.querySelector(selector) ||
                  document.querySelector('div.fr-alert.fr-alert-warning.alert-noResults')
            """,
            arg=SELECTORS["stparts"]["results_table"],
            timeout=WAIT_TIMEOUT,
        )

        if await page.locator(
            "div.fr-alert.fr-alert-warning.alert-noResults"
        ).is_visible():
            logger.info("🚫 На странице указан блок 'нет результатов'")
            return "no_results"

        return "has_results"
    except PlaywrightTimeout:
        logger.warning("⚠️ Истёк таймаут ожидания появления результатов")
        return "timeout"


async def scrape_stparts_async(
    page: Page, brand: str, part: str, logger: logging.Logger
) -> tuple:
    """Асинхронный парсер stparts.ru с передачей логгера."""
    try:
        url = f"{BASE_URL}/search/{brand}/{part}"
        await page.goto(url)
        # logger.info(f"Загружена страница: {url}")

        if await page.locator(SELECTORS["stparts"]["captcha_img"]).is_visible():
            logger.warning("Обнаружена капча на stparts.ru")
            if not await solve_captcha_universal(
                page=page,
                logger=logger,
                site_key="stparts",
                selectors={
                    "captcha_img": SELECTORS["stparts"]["captcha_img"],
                    "captcha_input": SELECTORS["stparts"]["captcha_input"],
                    "submit": SELECTORS["stparts"]["captcha_submit"],
                },
                max_attempts=3,
                scale_factor=3,
                check_changed=True,
                wait_after_submit_ms=5000,
            ):
                logger.error("Не удалось решить капчу")
                return None, None

        status = await wait_for_results_or_no_results_async(page)
        if status != "has_results":
            return None, None

        table = page.locator(SELECTORS["stparts"]["results_table"])
        await table.wait_for(state="visible", timeout=WAIT_TIMEOUT)
        rows = table.locator(SELECTORS["stparts"]["result_row"])
        row_count = await rows.count()

        if row_count == 0:
            logger.info(f"Результаты не найдены для {brand} / {part}")
            return None, None

        # logger.info(f"Найдено {row_count} строк результатов")

        async def find_best_result(priority_search: bool):
            for i in range(row_count):
                row = rows.nth(i)
                try:
                    brand_in_row = (
                        await row.locator(SELECTORS["stparts"]["brand"]).text_content()
                        or ""
                    ).strip()
                except Exception as e:
                    logger.error(f"Ошибка получения brand_in_row для строки {i}: {e}")
                    continue

                if not brand_matches(brand, brand_in_row):
                    continue
                try:
                    delivery_min = (
                        await row.locator(
                            SELECTORS["stparts"]["delivery"]
                        ).text_content()
                        or ""
                    ).strip()
                    price_text = (
                        await row.locator(SELECTORS["stparts"]["price"]).text_content()
                        or ""
                    ).strip()
                except Exception as e:
                    logger.error(f"Ошибка получения данных для строки {i}: {e}")
                    continue
                try:
                    if priority_search and not re.match(r"^1(\D|$)", delivery_min):
                        continue
                    price = parse_price(price_text)
                    if price is not None:
                        logger.info(
                            "✅ Найдено (бренд: %s, срок %s): %s ₽",
                            brand_in_row,
                            delivery_min,
                            price,
                        )

                        return price, delivery_min
                except Exception as e:
                    logger.error(f"Ошибка обработки строки {i}: {e}")
            return None, None

        result = await find_best_result(priority_search=True)
        if not result[0]:
            result = await find_best_result(priority_search=False)

        if result[0]:
            return result

        logger.info(f"❌ Подходящие результаты не найдены для {brand} / {part}")
        return None, None

    except PlaywrightTimeout:
        logger.warning(f"⏰ Таймаут при загрузке результатов для {brand} / {part}")
        return await fallback_search_async(page, brand, part)
    except Exception as e:
        logger.error(f"Ошибка парсинга стартов для {brand} / {part}: {e}")
        # Можно сделать скриншот для диагностики (опционально)
        await page.screenshot(
            path=f"screenshots/error_{brand}_{part}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        )
        return None, None


async def fallback_search_async(page: Page, brand: str, part: str) -> tuple:
    """Fallback-поиск только по номеру детали"""
    try:
        fallback_url = f"{BASE_URL}/search?pcode={part}"
        await page.goto(fallback_url)
        logger.info(f"Fallback: загружена страница без бренда: {fallback_url}")

        if await page.locator(SELECTORS["stparts"]["captcha_img"]).is_visible():
            logger.warning("Обнаружена капча на stparts.ru (fallback)")
            if not await solve_captcha_universal(
                page=page,
                logger=logger,
                site_key="stparts",
                selectors={
                    "captcha_img": SELECTORS["stparts"]["captcha_img"],
                    "captcha_input": SELECTORS["stparts"]["captcha_input"],
                    "submit": SELECTORS["stparts"]["captcha_submit"],
                },
                max_attempts=3,
                scale_factor=3,
                check_changed=True,
                wait_after_submit_ms=5000,
            ):
                logger.error("Не удалось решить капчу (fallback)")
                return None, None

        status = await wait_for_results_or_no_results_async(page)
        if status != "has_results":
            return None, None

        table = page.locator(SELECTORS["stparts"]["results_table"])
        await table.wait_for(state="visible", timeout=WAIT_TIMEOUT)
        rows = table.locator(SELECTORS["stparts"]["result_row"])
        row_count = await rows.count()

        if row_count == 0:
            logger.info(f"Fallback: результаты не найдены для {part}")
            return None, None

        logger.info(f"Fallback: найдено {row_count} строк результатов")

        async def find_best_result(priority_search: bool):
            for i in range(row_count):
                row = rows.nth(i)
                brand_in_row = (
                    await row.locator(SELECTORS["stparts"]["brand"]).text_content()
                    or ""
                ).strip()

                if not brand_matches(brand, brand_in_row):
                    continue

                delivery_min = (
                    await row.locator(SELECTORS["stparts"]["delivery"]).text_content()
                    or ""
                ).strip()
                if priority_search and not re.match(r"^1(\D|$)", delivery_min):
                    continue

                price_text = (
                    await row.locator(SELECTORS["stparts"]["price"]).text_content()
                    or ""
                ).strip()
                price = parse_price(price_text)
                if price is not None:
                    logger.info(
                        f"Fallback: найдено (бренд: {brand_in_row}, срок {delivery_min}): {price} ₽"
                    )
                    return price, delivery_min
            return None, None

        result = await find_best_result(priority_search=True)
        if not result[0]:
            result = await find_best_result(priority_search=False)

        if result[0]:
            return result
        logger.info(f"Fallback: подходящие результаты не найдены для {part}")
        return None, None

    except PlaywrightTimeout:
        logger.error(f"Fallback Timeout при загрузке результатов для {part}")
        return None, None
    except Exception as e:
        logger.error(f"Fallback ошибка парсинга stparts для {part}: {e}")
        return None, None


async def scrape_stparts_name_async(
    page: Page, part: str, logger: logging.Logger
) -> str:
    """
    Парсер stparts для поиска только названия детали по номеру.
    Проверяет два варианта таблиц с разными классами.
    """

    try:
        url = f"{BASE_URL}/search?pcode={part}"

        # ✅ FIX 1: Добавлен таймаут
        try:
            await page.goto(url, timeout=45000)
            logger.info(f"Загружена страница: {url}")
        except PlaywrightTimeout:
            logger.warning(f"⏰ Таймаут загрузки страницы для {part}")
            return None

        # Проверка капчи
        if await page.locator(SELECTORS["stparts"]["captcha_img"]).is_visible():
            logger.warning("Обнаружена капча на stparts.ru")
            if not await solve_captcha_universal(
                page=page,
                logger=logger,
                site_key="stparts",
                selectors={
                    "captcha_img": SELECTORS["stparts"]["captcha_img"],
                    "captcha_input": SELECTORS["stparts"]["captcha_input"],
                    "submit": SELECTORS["stparts"]["captcha_submit"],
                },
                max_attempts=3,
                scale_factor=3,
                check_changed=True,
                wait_after_submit_ms=5000,
            ):
                logger.error("Не удалось решить капчу")
                return None

        # ✅ FIX 2: Проверка "товар не найден"
        no_results_locator = page.locator(
            "div.fr-alert.fr-alert-warning.alert-noResults"
        )
        try:
            await no_results_locator.wait_for(state="visible", timeout=3000)
            no_results_text = await no_results_locator.text_content()
            logger.info(f"🚫 Товар не найден для {part}: {no_results_text.strip()}")
            return None
        except PlaywrightTimeout:
            # Товар найден, продолжаем
            pass

        # ✅ FIX 3: Ждём появления хотя бы одной таблицы
        try:
            await page.wait_for_selector(
                f"{SELECTORS['stparts']['case_table']}, {SELECTORS['stparts']['alt_results_table']}",
                timeout=10000,
                state="visible",
            )
        except PlaywrightTimeout:
            logger.warning(f"⏰ Таймаут ожидания таблиц результатов для {part}")
            return None

        # Проверяем таблицу globalCase
        case_table_count = await page.locator(
            SELECTORS["stparts"]["case_table"]
        ).count()
        logger.info(f"Количество таблиц globalCase: {case_table_count}")
        if case_table_count > 0:
            case_table = page.locator(SELECTORS["stparts"]["case_table"])
            desc_cells = case_table.locator(SELECTORS["stparts"]["case_description"])
            desc_count = await desc_cells.count()
            logger.info(f"Количество ячеек caseDescription: {desc_count}")
            if desc_count > 0:
                description = await desc_cells.nth(0).text_content()
                logger.info(f"Содержимое первой ячейки в globalCase: {description}")
                if description:
                    description = description.strip()
                    logger.info(
                        f"✅ Найдено название детали в globalCase: {description}"
                    )
                    return description
                else:
                    logger.info("Первая ячейка caseDescription пустая")
            else:
                logger.info("Ячейки caseDescription не найдены в globalCase")

        # Если таблицы globalCase нет, проверяем globalResult
        alt_results_count = await page.locator(
            SELECTORS["stparts"]["alt_results_table"]
        ).count()
        logger.info(f"Количество таблиц globalResult: {alt_results_count}")
        if alt_results_count > 0:
            alt_table = page.locator(SELECTORS["stparts"]["alt_results_table"])
            desc_cells = alt_table.locator(
                SELECTORS["stparts"]["alt_result_description"]
            )
            desc_count = await desc_cells.count()
            logger.info(f"Количество элементов resultDescription: {desc_count}")
            if desc_count > 0:
                description = await desc_cells.nth(0).text_content()
                logger.info(f"Содержимое первой ячейки в globalResult: {description}")
                if description:
                    description = description.strip()
                    logger.info(
                        f"✅ Найдено название детали в globalResult: {description}"
                    )
                    return description
                else:
                    logger.info("Первый элемент resultDescription пустой")
            else:
                logger.info("Элементы resultDescription не найдены в globalResult")

        logger.info(f"❌ Не удалось найти описание детали для {part}")
        return None

    # ✅ FIX 4: Отдельная обработка PlaywrightTimeout
    except PlaywrightTimeout as e:
        logger.warning(f"⏰ Таймаут для {part}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга названия детали для {part}: {e}")
        await page.screenshot(
            path=f"screenshots/error_name_stparts_{part}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        )
        return None

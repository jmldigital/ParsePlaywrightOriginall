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


# Этот работает

# async def solve_avtoformula_captcha_async(page: Page) -> bool:
#     """Решение капчи через 2Captcha для avtoformula: использование скриншота, логирование успешных и неудачных капч."""
#     captcha_text = None
#     img = None
#     original_img_bytes = None

#     try:
#         solver = TwoCaptcha(API_KEY_2CAPTCHA)
#         captcha_img = page.locator(SELECTORS["avtoformula"]["captcha_img"])
#         if not await captcha_img.is_visible():
#             logger.info("Капча не обнаружена")
#             return False

#         logger.info("📸 Делаем скриншот капчи")

#         # Сохраняем HTML для анализа
#         html = await page.content()
#         os.makedirs("screenshots/pages", exist_ok=True)
#         with open(f"screenshots/pages/captcha_page_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.html", "w", encoding="utf-8") as f:
#             f.write(html)
#         logger.info("💾 HTML страницы капчи сохранён для анализа")

#         # Делаем скриншот локатора
#         original_img_bytes = await captcha_img.screenshot()
#         logger.info(f"📸 Скриншот капчи получен, размер: {len(original_img_bytes)} байт")

#         # Проверяем, что получили валидные данные
#         if not original_img_bytes or len(original_img_bytes) < 100:
#             raise Exception("Получены пустые или слишком маленькие данные изображения")

#         # Открываем и обрабатываем изображение
#         img = Image.open(io.BytesIO(original_img_bytes))
#         logger.info(f"✅ Изображение открыто: {img.format} {img.size} {img.mode}")

#         # Только увеличение размера
#         img = img.resize((img.width * 3, img.height * 3), Image.BICUBIC)

#         buf = io.BytesIO()
#         img.save(buf, format="PNG")
#         captcha_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")

#         logger.info("Отправляем капчу avtoformula на распознавание в 2Captcha")
#         result = await asyncio.to_thread(solver.normal, captcha_base64)
#         captcha_text = result["code"]
#         logger.info(f"✅ Капча распознана: {captcha_text}")

#         # КРИТИЧНО: Проверяем, не изменилась ли капча за время распознавания
#         current_img_bytes = await captcha_img.screenshot()
#         if current_img_bytes != original_img_bytes:
#             logger.warning("⚠️ Капча изменилась во время распознавания! Начинаем заново.")
#             # Сохраняем для анализа
#             os.makedirs("screenshots/changed", exist_ok=True)
#             timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
#             Image.open(io.BytesIO(original_img_bytes)).save(f"screenshots/changed/original_{timestamp}.png")
#             Image.open(io.BytesIO(current_img_bytes)).save(f"screenshots/changed/changed_{timestamp}.png")
#             logger.info("💾 Сохранены обе версии капчи для сравнения")
#             # Рекурсивно пробуем снова (но можно добавить счётчик попыток)
#             return await solve_avtoformula_captcha_async(page)

#         input_el = page.locator(SELECTORS["avtoformula"]["captcha_input"])
#         await input_el.fill(captcha_text)
#         logger.info(f"✅ Капча введена в поле: {captcha_text}")

#         submit_button = page.locator('input[name="submit"][value="Отправить"]')
#         await submit_button.click()
#         logger.info("✅ Нажата кнопка 'Отправить'")

#         await page.wait_for_timeout(5000)
#         timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

#         if not await captcha_img.is_visible():
#             logger.info("✅ Капча успешно решена, страница обновлена")
#             # Сохраняем успешно решённую капчу
#             os.makedirs("screenshots/success", exist_ok=True)
#             success_path = f"screenshots/success/success_captcha_{captcha_text}_{timestamp}.png"
#             img.save(success_path)
#             logger.info(f"🎉 Успешная капча сохранена: {success_path}")
#             return True
#         else:
#             logger.warning("⚠️ Капча всё ещё видна после отправки")
#             screenshot_path = f"screenshots/captcha_failed_avtoformula_{captcha_text}_{timestamp}.png"
#             await page.screenshot(path=screenshot_path)
#             logger.warning(f"📸 Скриншот сохранён: {screenshot_path}")
#             os.makedirs("screenshots/capchas", exist_ok=True)
#             processed_path = f"screenshots/capchas/processed_captcha_{captcha_text}_{timestamp}.png"
#             img.save(processed_path)
#             logger.error(f"📸 Сохранена обработанная капча: {processed_path}")
#             return False

#     except Exception as e:
#         logger.error(f"❌ Ошибка решения капчи avtoformula: {e}", exc_info=True)
#         timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
#         captcha_label = captcha_text if captcha_text else "unknown"
#         screenshot_path = f"screenshots/captcha_error_avtoformula_{captcha_label}_{timestamp}.png"

#         try:
#             await page.screenshot(path=screenshot_path)
#             logger.error(f"📸 Скриншот ошибки сохранён: {screenshot_path}")
#         except Exception as screenshot_error:
#             logger.error(f"Не удалось сохранить скриншот: {screenshot_error}")

#         try:
#             if img is not None:
#                 os.makedirs("screenshots/capchas", exist_ok=True)
#                 processed_path = f"screenshots/capchas/processed_captcha_{captcha_label}_{timestamp}.png"
#                 img.save(processed_path)
#                 logger.error(f"📸 Сохранена обработанная капча: {processed_path}")
#         except Exception as save_error:
#             logger.error(f"Не удалось сохранить обработанную капчу: {save_error}")

#         return False


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
            if not await solve_captcha_universal(
                page=page,
                logger=logger,
                site_key="avtoformula",
                selectors={
                    "captcha_img": SELECTORS["avtoformula"]["captcha_img"],
                    "captcha_input": SELECTORS["avtoformula"]["captcha_input"],
                    "submit": SELECTORS["avtoformula"]["captcha_submit"],
                },
                max_attempts=3,
                scale_factor=3,
                wait_after_submit_ms=8000,
            ):
                logger.error("Не удалось решить капчу")
                return None, None

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
        if not captcha_solved:
            if await page.locator(SELECTORS["avtoformula"]["captcha_img"]).is_visible():
                logger.warning("⚠️ Обнаружена капча на avtoformula.ru (fallback)")
                if not await solve_captcha_universal(
                    page=page,
                    logger=logger,
                    site_key="avtoformula",
                    selectors={
                        "captcha_img": SELECTORS["avtoformula"]["captcha_img"],
                        "captcha_input": SELECTORS["avtoformula"]["captcha_input"],
                        "submit": 'input[name="submit"][value="Отправить"]',
                    },
                    max_attempts=3,
                    scale_factor=3,
                    wait_after_submit_ms=8000,
                ):
                    logger.error("Не удалось решить капчу (fallback)")
                    return None, None
                await page.wait_for_timeout(3000)

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
        logger.info(f"🌐 Страница загружена: avtoformula.ru")

        # Ввод артикула
        article_field = page.locator(f"#{SELECTORS['avtoformula']['article_field']}")
        await article_field.wait_for(state="visible", timeout=10000)
        await article_field.fill(part)
        await page.locator(SELECTORS["avtoformula"]["search_button"]).click()
        # logger.info(f"🔍 Поиск артикула: {part}")

        # ✅ КРИТИЧНО: Сначала проверяем капчу ОДИН РАЗ
        if await page.locator(SELECTORS["avtoformula"]["captcha_img"]).is_visible():
            logger.warning("⚠️ Обнаружена капча на avtoformula.ru")
            if not await solve_captcha_universal(
                page=page,
                logger=logger,
                site_key="avtoformula",
                selectors={
                    "captcha_img": SELECTORS["avtoformula"]["captcha_img"],
                    "captcha_input": SELECTORS["avtoformula"]["captcha_input"],
                    "submit": 'input[name="submit"][value="Отправить"]',
                },
                max_attempts=3,
                scale_factor=3,
                wait_after_submit_ms=8000,
            ):
                logger.error("Не удалось решить капчу")
                return None

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
        if not captcha_solved:
            if await page.locator(SELECTORS["avtoformula"]["captcha_img"]).is_visible():
                logger.warning("⚠️ Обнаружена капча на avtoformula.ru (fallback)")
                if not await solve_captcha_universal(
                    page=page,
                    logger=logger,
                    site_key="avtoformula",
                    selectors={
                        "captcha_img": SELECTORS["avtoformula"]["captcha_img"],
                        "captcha_input": SELECTORS["avtoformula"]["captcha_input"],
                        "submit": 'input[name="submit"][value="Отправить"]',
                    },
                    max_attempts=3,
                    scale_factor=3,
                    wait_after_submit_ms=8000,
                ):
                    logger.error("Не удалось решить капчу (fallback)")
                    return None
                await page.wait_for_timeout(3000)

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

"""
Асинхронный парсер armtek.ru для поиска ФИЗИЧЕСКОГО веса
С обработкой капчи через 2Captcha!
"""

import re
import base64
import os

from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from config import (
    SELECTORS,
    # Ключ 2Captcha из config.py
)
from utils import (
    get_site_logger,
    solve_captcha_universal,
    save_debug_info,
)  # 🆕 ИЗ utils.py!

logger = get_site_logger("armtek")  # Теперь работает!
import logging

logger = get_site_logger("armtek")

BASE_URL = "https://armtek.ru"
WAIT_TIMEOUT = 15000  # Больше для капчи
os.makedirs("debug_armtek", exist_ok=True)


# async def solve_armtek_captcha_async(page: Page, logger: logging.Logger) -> bool:
#     """2Captcha для armtek.ru с ВАШИМИ селекторами"""
#     try:
#         solver = TwoCaptcha(API_KEY_2CAPTCHA)

#         # Картинка капчи
#         captcha_img = page.locator(SELECTORS["armtek"]["captcha_img"])
#         captcha_bytes = await captcha_img.screenshot()
#         captcha_base64 = base64.b64encode(captcha_bytes).decode("utf-8")

#         logger.info("🔐 2Captcha armtek.ru...")
#         result = await asyncio.to_thread(solver.normal, captcha_base64)
#         captcha_text = result["code"]
#         logger.info(f"✅ Код: '{captcha_text}'")

#         # Вводим
#         captcha_input = page.locator(SELECTORS["armtek"]["captcha_input"])
#         await captcha_input.fill(captcha_text)

#         # Отправляем
#         submit_btn = page.locator(SELECTORS["armtek"]["captcha_submit"])
#         await submit_btn.click()

#         logger.info("✅ Капча решена!")
#         await page.wait_for_timeout(3000)
#         return True

#     except Exception as e:
#         logger.error(f"❌ Капча armtek.ru: {e}")
#         return False


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


# async def scrape_weight_armtek(
#     page: Page, part: str, logger: logging.Logger
# ) -> tuple[str, None]:
#     """
#     Armtek.ru - ДИНАМИКА + ТВОИ селекторы + fallback blob-капча!
#     """
#     try:
#         # 1. Goto поиск
#         search_url = f"{BASE_URL}/search?text={part}"
#         await page.goto(search_url, wait_until="domcontentloaded")

#         # 2. Город
#         await close_city_dialog_if_any(page, logger)

#         # 🔥 Капча = модалка С project-ui-captcha внутри + диагностика
#         captcha_modal = page.locator("sproit-ui-modal:has(project-ui-captcha)")

#         # Логируем все счетчики
#         modal_count = await page.locator("sproit-ui-modal").count()
#         captcha_modal_count = await captcha_modal.count()
#         captcha_img_count = await page.locator(
#             "sproit-ui-modal img[src*='blob']"
#         ).count()
#         input_count = await page.locator(SELECTORS["armtek"]["captcha_input"]).count()
#         submit_count = await page.locator(SELECTORS["armtek"]["captcha_submit"]).count()

#         logger.info(
#             f"{part}: modal={modal_count}, captcha_modal={captcha_modal_count}, "
#             f"img={captcha_img_count}, input={input_count}, submit={submit_count}"
#         )

#         if captcha_modal_count > 0:
#             logger.warning("🎯 Капча-модалка с project-ui-captcha — решаем!")
#             success = await solve_captcha_universal(
#                 page=page,
#                 logger=logger,
#                 site_key="armtek",
#                 selectors={
#                     "captcha_img": "sproit-ui-modal img[src*='blob']",
#                     "captcha_input": SELECTORS["armtek"]["captcha_input"],
#                     "submit": SELECTORS["armtek"]["captcha_submit"],
#                 },
#                 max_attempts=2,
#             )
#             if success:
#                 logger.info(f"{part}: ✅ Капча решена!")
#                 await page.wait_for_timeout(2000)
#             else:
#                 logger.error(f"{part}: ❌ Капча НЕ решена — пропускаем")
#                 return None, None


#         # 🔥 Ждем контейнер списка (из конфига) - просто attached, не visible
#         try:
#             await page.wait_for_selector(
#                 SELECTORS["armtek"]["product_list"],  # .results-list
#                 timeout=10000,
#                 state="attached",
#             )
#         except:
#             pass  # Продолжаем, даже если нет списка

#         # Проверяем "не найдено" с коротким таймаутом
#         try:
#             not_found = page.get_by_text("Товары не найдены")
#             if await not_found.wait_for(timeout=3000):
#                 logger.info("%s: не найдена", part)
#                 return None, None
#         except:
#             pass  # Нет "не найдено" - ищем товары

#         # Ждем первую карточку (из конфига)
#         await page.wait_for_selector(
#             f"{SELECTORS['armtek']['product_list']} {SELECTORS['armtek']['product_cards']}",  # .results-list .scroll-item
#             timeout=10000,
#             state="attached",
#         )

#         # Переходим в первую карточку
#         first_link = page.locator(f"{SELECTORS['armtek']['product_list']} a").first
#         await first_link.wait_for(timeout=5000)
#         href = await first_link.get_attribute("href")
#         if not href:
#             return None, None
#         await page.goto(BASE_URL + href, wait_until="domcontentloaded")

#         # Характеристики
#         await page.wait_for_selector("product-key-value")

#         # ТВОЙ вес-селектор
#         weight_values = page.locator(SELECTORS["armtek"]["weight_value"])
#         count = await weight_values.count()

#         for i in range(count):
#             text = await weight_values.nth(i).text_content()
#             if text and "кг" in text:
#                 import re

#                 match = re.search(r"(\d+(?:[.,]\d+)?)\s*кг", text)
#                 if match:
#                     weight = match.group(1).replace(",", ".")
#                     logger.info("%s: %s кг", part, weight)
#                     return weight, None

#         logger.warning("%s: вес не найден", part)
#         return None, None

#     except Exception as e:
#         logger.error("❌ %s: %s", part, str(e))
#         await save_debug_info(page, part, type(e).__name__, logger, "armtek")
#         return None, None


async def scrape_weight_armtek(
    page: Page, part: str, logger: logging.Logger
) -> tuple[str, None]:
    """
    Armtek.ru - БЫСТРО + капча в except!
    """
    max_retries = 2

    for attempt in range(max_retries + 1):
        try:
            # 1. Goto + город (быстро)
            search_url = f"{BASE_URL}/search?text={part}"
            await page.goto(search_url, wait_until="domcontentloaded", timeout=10000)
            await close_city_dialog_if_any(page, logger)

            # 2. Результаты (ЖЕСТКО 5s)
            await page.wait_for_selector(
                f"{SELECTORS['armtek']['product_list']} {SELECTORS['armtek']['product_cards']}",
                timeout=5000,
                state="attached",
            )

            # 3. "Не найдено"?
            try:
                not_found = page.get_by_text("Товары не найдены")
                if await not_found.wait_for(timeout=1000):
                    return None, None
            except:
                pass

            # 4. Карточка
            first_link = page.locator(f"{SELECTORS['armtek']['product_list']} a").first
            href = await first_link.get_attribute("href", timeout=2000)
            if not href:
                return None, None
            await page.goto(
                BASE_URL + href, wait_until="domcontentloaded", timeout=5000
            )

            # 5. Вес
            await page.wait_for_selector("product-key-value", timeout=3000)
            weight_values = page.locator(SELECTORS["armtek"]["weight_value"])

            for i in range(await weight_values.count()):
                text = await weight_values.nth(i).text_content(timeout=1000)
                if text and "кг" in text:
                    import re

                    match = re.search(r"(\d+(?:[.,]\d+)?)\s*кг", text)
                    if match:
                        weight = match.group(1).replace(",", ".")
                        logger.info("%s: %s кг", part, weight)
                        return weight, None

            logger.warning("%s: вес не найден", part)
            return None, None

        except Exception as e:
            logger.error("❌ %s (попытка %d): %s", part, attempt + 1, str(e))
            await save_debug_info(
                page, part, f"{type(e).__name__}_attempt{attempt}", logger, "armtek"
            )

            if attempt < max_retries:
                logger.info(f"{part}: пробуем капчу...")
                try:
                    # Капча только при ошибке!
                    captcha_modal = page.locator(
                        "sproit-ui-modal:has(project-ui-captcha)"
                    )
                    if await captcha_modal.count() > 0:
                        logger.warning("🎯 Капча в except — решаем!")
                        await solve_captcha_universal(
                            page=page,
                            logger=logger,
                            site_key="armtek",
                            selectors={
                                "captcha_img": "sproit-ui-modal img[src*='blob']",
                                "captcha_input": SELECTORS["armtek"]["captcha_input"],
                                "submit": SELECTORS["armtek"]["captcha_submit"],
                            },
                            max_attempts=1,  # Быстро
                        )
                        await page.wait_for_timeout(1500)
                    else:
                        logger.debug("Нет project-ui-captcha")
                except:
                    logger.debug("Капча-ошибка — retry")
            else:
                return None, None

    return None, None

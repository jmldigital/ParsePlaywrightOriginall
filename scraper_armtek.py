"""
Асинхронный парсер armtek.ru для поиска ФИЗИЧЕСКОГО веса
С обработкой капчи через 2Captcha!
"""

import re
import base64
import os
from datetime import datetime
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout
from twocaptcha import TwoCaptcha
import asyncio
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
#     Парсер armtek.ru:
#     1) сразу открывает https://armtek.ru/search?text=<part>
#     2) закрывает диалог выбора города, если он есть
#     3) если виден список (product_list) — заходит в первую позицию
#     4) на странице карточки (product-card-info) ищет вес по значению с 'кг'
#     """
#     # logger.info(f"🌐 armtek.ru: {part}")

#     try:
#         # 1. Переход сразу на поиск по артикулу
#         search_url = f"{BASE_URL}/search?text={part}"
#         # logger.info(f"➡️ Открываю страницу поиска: {search_url}")
#         await page.goto(search_url, timeout=WAIT_TIMEOUT)
#         # logger.info(f"📍 URL после goto: {page.url}")

#         # 2. Закрываем/подтверждаем окно выбора города, если оно всплыло
#         await close_city_dialog_if_any(page, logger)

#         # 🆕 ТОЧНЫЕ селекторы из HTML
#         # logger.info("⏳ Жду results-list + scroll-item...")

#         # 1. Контейнер списка
#         await page.wait_for_selector(".results-list", timeout=15000)
#         # logger.info("✅ .results-list НАЙДЕН!")

#         # 2. Первый товар
#         await page.wait_for_selector(".scroll-item", timeout=10000)
#         # logger.info("✅ .scroll-item НАЙДЕН!")

#         await page.wait_for_timeout(2000)

#         # 3. Селекторы
#         list_selector = SELECTORS["armtek"]["product_list"]
#         cards_selector = SELECTORS["armtek"]["product_cards"]
#         card_selector = SELECTORS["armtek"]["product_card"]

#         list_loc = page.locator(list_selector)
#         cards_loc = page.locator(cards_selector)
#         card_loc = page.locator(card_selector)

#         list_count = await list_loc.count()
#         cards_count = await cards_loc.count()
#         # logger.info(f"📊 list_count={list_count}, cards_count={cards_count}")

#         product_cards = None

#         # 4. Если есть список — переходим по первой ссылке <a>
#         if list_count > 0:
#             # logger.info("📜 Найден список товаров, ищу первую ссылку <a>")
#             first_link = list_loc.locator("a").first

#             if await first_link.count() == 0:
#                 logger.warning("ℹ️ Не найдена ни одна ссылка <a> в списке")
#                 await save_debug_info(page, part, "NO_LIST_LINK", logger)
#                 return None, None

#             href = await first_link.get_attribute("href")
#             # logger.info(f"🔗 Переход по ссылке первого товара: {href}")

#             if href and href.startswith("/"):
#                 target_url = BASE_URL + href
#             else:
#                 target_url = href or search_url

#             await page.goto(target_url, timeout=WAIT_TIMEOUT)
#             # logger.info(f"📍 URL после перехода по товару: {page.url}")

#             await page.wait_for_selector(card_selector, timeout=10000)
#             product_cards = page.locator(card_selector)

#         else:
#             if cards_count == 0:
#                 # logger.warning("ℹ️ Ни списка, ни карточек товара не найдено")
#                 await save_debug_info(page, part, "NO_LIST_NO_CARDS", logger)
#                 return None, None

#             logger.info("🧾 Найдены карточки товара на странице поиска")
#             product_cards = card_loc

#         cards_found = await product_cards.count()
#         # logger.info(f"🧾 Карточек товара (product-card-info) на странице: {cards_found}")
#         if cards_found == 0:
#             logger.warning("ℹ️ Карточка товара не найдена")
#             await save_debug_info(page, part, "NO_PRODUCT_CARD", logger)
#             return None, None

#         # logger.info("✅ Карточка товара найдена, ищем вес")
#         # 5. Капча в МОДАЛКЕ (новые селекторы)

#         await page.wait_for_timeout(4000)  # Angular рендер
#         await page.evaluate("window.scrollTo(0, 300)")  # Помогает показать список

#         # ✅ attached вместо visible!
#         try:
#             await page.wait_for_selector(list_selector, timeout=30000, state="attached")
#             # logger.info("✅ .results-list attached")
#         except:
#             pass

#         try:
#             await page.wait_for_selector(
#                 cards_selector, timeout=25000, state="attached"
#             )
#             # logger.info("✅ .scroll-item attached")
#         except:
#             pass

#         await page.wait_for_timeout(2000)

#         # 5. ✅ КАПЧА - основной + fallback blob
#         captcha_selector = SELECTORS["armtek"]["captcha_img"]  # ТОЧНЫЙ селектор
#         captcha_img = page.locator(captcha_selector)
#         captcha_count = await captcha_img.count()

#         if captcha_count > 0:
#             logger.warning("⚠️ Капча по ТОЧНОМУ селектору обнаружена")

#             # Ждём src + visible
#             try:
#                 await page.wait_for_selector(f"{captcha_selector}[src]", timeout=10000)
#                 logger.info("✅ Captcha src загружена")
#             except:
#                 logger.info("ℹ️ Captcha src не загрузилась")

#             captcha_element = captcha_img.first
#             if await captcha_element.is_visible():
#                 logger.info("🚨 Капча visible → решаем!")

#                 # ✅ ТОЧНЫЕ селекторы из config
#                 if await solve_captcha_universal(
#                     page=page,
#                     logger=logger,
#                     site_key="armtek",
#                     selectors={
#                         "captcha_img": SELECTORS["armtek"]["captcha_img"],
#                         "captcha_input": SELECTORS["armtek"]["captcha_input"],
#                         "submit": SELECTORS["armtek"]["captcha_submit"],
#                     },
#                     max_attempts=3,  # Больше попыток
#                     scale_factor=2,
#                     check_changed=False,
#                     wait_after_submit_ms=4000,  # Больше после submit
#                 ):
#                     await page.wait_for_timeout(3000)  # Ждём исчезновения модалки
#                     logger.info("✅ Капча решена!")

#                     # ✅ ПОВТОРНО ждём список после капчи
#                     await page.wait_for_selector(
#                         ".results-list", timeout=15000, state="attached"
#                     )
#                 else:
#                     logger.error("❌ Капча не решена")
#                     return None, None
#             else:
#                 logger.warning("⚠️ Капча не visible - ждём...")
#                 await page.wait_for_timeout(3000)

#         else:
#             # ✅ FALLBACK blob-капча
#             blob_captcha = page.locator("sproit-ui-modal img[src*='blob']")
#             if await blob_captcha.count() > 0:
#                 logger.warning("🔍 Blob-капча в модалке fallback!")
#                 await solve_captcha_universal(  # Повторяем с blob
#                     page=page,
#                     logger=logger,
#                     site_key="armtek",
#                     selectors={
#                         "captcha_img": "sproit-ui-modal img[src*='blob']",
#                         "captcha_input": SELECTORS["armtek"]["captcha_input"],
#                         "submit": SELECTORS["armtek"]["captcha_submit"],
#                     },
#                     max_attempts=2,
#                 )

#         # 6. Ждём блок характеристик
#         try:
#             await page.wait_for_selector("product-key-value", timeout=5000)
#         except Exception:
#             logger.warning("⚠️ Не дождались product-key-value")

#         # 7. Поиск веса по значению, содержащему "кг"
#         value_locator = product_cards.locator("span.font__body2")
#         values_count = await value_locator.count()
#         # logger.info(f"⚖️ Найдено span.font__body2 в карточке: {values_count}")

#         weight_text = None
#         for i in range(values_count):
#             v = value_locator.nth(i)
#             txt = (await v.text_content() or "").strip()
#             if "кг" in txt:
#                 logger.info("⚖️ Найден вес: %s для %s", txt, part)
#                 weight_text = txt
#                 break

#         if not weight_text:
#             logger.info("ℹ️ Вес не найден в карточке: %s", part)
#             await save_debug_info(page, part, "NO_WEIGHT_TEXT", logger)
#             return None, None

#         # 8. Парс веса из найденного текста
#         physical_match = re.search(r"(\d+(?:[.,]\d+)?)\s*кг", weight_text or "")
#         if physical_match:
#             physical_weight = physical_match.group(1).replace(",", ".")
#             # logger.info(f"✅ armtek.ru: физ. вес={physical_weight} кг")
#             return physical_weight, None
#         else:
#             logger.warning("ℹ️ Не удалось извлечь число из текста веса")
#             await save_debug_info(page, part, "PARSE_WEIGHT_FAIL", logger)
#             return None, None

#     except PlaywrightTimeout as e:
#         logger.error(f"⏰ PlaywrightTimeout: {e}")
#         await save_debug_info(page, part, f"TIMEOUT_{e.__class__.__name__}", logger)
#         return None, None
#     except Exception as e:
#         logger.error(f"❌ armtek.ru {part}: {e}")
#         await save_debug_info(page, part, f"ERROR_{type(e).__name__}", logger)
#         return None, None


async def scrape_weight_armtek(
    page: Page, part: str, logger: logging.Logger
) -> tuple[str, None]:
    """
    Armtek.ru - ДИНАМИКА + ТВОИ селекторы + fallback blob-капча!
    """
    try:
        # 1. Goto поиск
        search_url = f"{BASE_URL}/search?text={part}"
        await page.goto(search_url, wait_until="domcontentloaded")

        # 2. Город
        await close_city_dialog_if_any(page, logger)

        # 🔥 Ждем контейнер списка (из конфига) - просто attached, не visible
        try:
            await page.wait_for_selector(
                SELECTORS["armtek"]["product_list"],  # .results-list
                timeout=10000,
                state="attached",
            )
        except:
            pass  # Продолжаем, даже если нет списка

        # Проверяем "не найдено" с коротким таймаутом
        try:
            not_found = page.get_by_text("Товары не найдены")
            if await not_found.wait_for(timeout=3000):
                logger.info("%s: не найдена", part)
                return None, None
        except:
            pass  # Нет "не найдено" - ищем товары

        # Ждем первую карточку (из конфига)
        await page.wait_for_selector(
            f"{SELECTORS['armtek']['product_list']} {SELECTORS['armtek']['product_cards']}",  # .results-list .scroll-item
            timeout=10000,
            state="attached",
        )

        # Переходим в первую карточку
        first_link = page.locator(f"{SELECTORS['armtek']['product_list']} a").first
        await first_link.wait_for(timeout=5000)
        href = await first_link.get_attribute("href")
        if not href:
            return None, None
        await page.goto(BASE_URL + href, wait_until="domcontentloaded")

        # 🔥 КАПЧА (твоя логика!)
        captcha_img = page.locator(SELECTORS["armtek"]["captcha_img"])
        if await captcha_img.count() > 0:
            logger.warning("⚠️ Капча!")
            await solve_captcha_universal(
                page, logger, "armtek", SELECTORS["armtek"]  # Все твои селекторы!
            )

        # Fallback blob-капча (твоя!)
        blob_captcha = page.locator("sproit-ui-modal img[src*='blob']")
        if await blob_captcha.count() > 0:
            logger.warning("🔍 Blob-капча!")
            await solve_captcha_universal(
                page=page,
                logger=logger,
                site_key="armtek",
                selectors={
                    "captcha_img": "sproit-ui-modal img[src*='blob']",
                    "captcha_input": SELECTORS["armtek"]["captcha_input"],
                    "submit": SELECTORS["armtek"]["captcha_submit"],
                },
                max_attempts=2,
            )

        # Характеристики
        await page.wait_for_selector("product-key-value")

        # ТВОЙ вес-селектор
        weight_values = page.locator(SELECTORS["armtek"]["weight_value"])
        count = await weight_values.count()

        for i in range(count):
            text = await weight_values.nth(i).text_content()
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
        logger.error("❌ %s: %s", part, str(e))
        await save_debug_info(page, part, type(e).__name__, logger, "armtek")
        return None, None

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
from utils import get_site_logger, solve_captcha_universal  # 🆕 ИЗ utils.py!

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


async def save_debug_info(page: Page, part: str, reason: str, logger: logging.Logger):
    """DEBUG: скриншот + HTML при проблемах"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    screenshot_path = f"debug_armtek/{reason}_{part}_{timestamp}.png"
    await page.screenshot(path=screenshot_path)

    html_path = f"debug_armtek/{reason}_{part}_{timestamp}.html"
    html_content = await page.content()
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.warning(f"📸 DEBUG {reason} armtek.ru {part}:")
    logger.warning(f"   📍 URL: {page.url}")
    logger.warning(f"   🖼️  {screenshot_path}")
    logger.warning(f"   📄 {html_path}")


# async def scrape_weight_armtek(
#     page: Page, part: str, logger: logging.Logger
# ) -> tuple[str, None]:
#     """
#     Парсер armtek.ru — с ВАШИМИ селекторами из config!
#     """
#     try:
#         logger.info(f"🌐 armtek.ru: {part}")

#         # 1. Главная страница
#         await page.goto(
#             "https://armtek.ru", wait_until="networkidle", timeout=WAIT_TIMEOUT
#         )

#         if page.url == "about:blank":
#             await save_debug_info(page, part, "BLANK_PAGE", logger)
#             return None, None

#         # 2. Ждём поле поиска
#         await page.wait_for_selector(SELECTORS["armtek"]["search_input"], timeout=10000)

#         # 3. Поиск
#         search_input = page.locator(SELECTORS["armtek"]["search_input"])
#         await search_input.fill(part)
#         logger.info(f"✅ '{part}' введён")

#         search_button = page.locator(SELECTORS["armtek"]["search_button"])
#         await search_button.click()
#         logger.info("✅ Поиск отправлен")

#         await page.wait_for_load_state("networkidle", timeout=10000)

#         # 4. Капча
#         captcha_img = page.locator(SELECTORS["armtek"]["captcha_img"])
#         if await captcha_img.is_visible(timeout=2000):
#             logger.warning("⚠️  Капча armtek.ru!")
#             if await solve_armtek_captcha_async(page, logger):
#                 await page.wait_for_load_state("networkidle", timeout=10000)
#             else:
#                 return None, None

#         # 5. Ищем веса в карточках
#         product_cards = page.locator(SELECTORS["armtek"]["product_cards"])
#         weight_elements = product_cards.filter(has=page.locator(":text-is('кг')"))

#         count = await weight_elements.count()
#         logger.info(f"📊 {count} карточек с кг")

#         if count == 0:
#             logger.info(f"ℹ️  Вес не найден: {part}")
#             return None, None

#         # 6. Первый вес
#         weight_elem = weight_elements.first()
#         weight_text = await weight_elem.text_content(timeout=3000)
#         physical_match = re.search(r"(\d+[.,]\d+)\s*кг", weight_text)

#         if physical_match:
#             physical_weight = physical_match.group(1).replace(",", ".")
#             logger.info(f"✅ armtek.ru: {physical_weight}кг")
#             return physical_weight, None
#         else:
#             logger.info(f"ℹ️  Не распарсился вес")
#             return None, None

#     except PlaywrightTimeout as e:
#         await save_debug_info(page, part, f"TIMEOUT_{e.__class__.__name__}", logger)
#         logger.error(f"⏰ Таймаут armtek.ru {part}")
#         return None, None
#     except Exception as e:
#         await save_debug_info(page, part, f"ERROR_{type(e).__name__}", logger)
#         logger.error(f"❌ armtek.ru {part}: {e}")
#         return None, None


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


async def scrape_weight_armtek(
    page: Page, part: str, logger: logging.Logger
) -> tuple[str, None]:
    """
    Парсер armtek.ru:
    1) сразу открывает https://armtek.ru/search?text=<part>
    2) закрывает диалог выбора города, если он есть
    3) если виден список (product_list) — заходит в первую позицию
    4) на странице карточки (product-card-info) ищет вес по значению с 'кг'
    """
    logger.info(f"🌐 armtek.ru: {part}")

    try:
        # 1. Переход сразу на поиск по артикулу
        search_url = f"{BASE_URL}/search?text={part}"
        logger.info(f"➡️ Открываю страницу поиска: {search_url}")
        await page.goto(search_url, timeout=WAIT_TIMEOUT)
        logger.info(f"📍 URL после goto: {page.url}")

        # 2. Закрываем/подтверждаем окно выбора города, если оно всплыло
        await close_city_dialog_if_any(page, logger)

        await page.wait_for_timeout(1500)

        # 3. Селекторы
        list_selector = SELECTORS["armtek"]["product_list"]
        cards_selector = SELECTORS["armtek"]["product_cards"]

        list_loc = page.locator(list_selector)
        cards_loc = page.locator(cards_selector)

        list_count = await list_loc.count()
        cards_count = await cards_loc.count()
        logger.info(f"📊 list_count={list_count}, cards_count={cards_count}")

        product_cards = None

        # 4. Если есть список — заходим в первый элемент
        if list_count > 0 and cards_count == 0:
            logger.info("📜 Найден список товаров, ищу первый элемент")
            first_item = list_loc.locator("div.scroll-item.ng-star-inserted").first
            if await first_item.count() == 0:
                logger.warning("ℹ️ Не найден ни один scroll-item")
                await save_debug_info(page, part, "NO_SCROLL_ITEM", logger)
                return None, None

            first_link = first_item.locator("a").first
            if await first_link.count() == 0:
                logger.warning("ℹ️ В первом scroll-item нет ссылок <a>")
                await save_debug_info(page, part, "NO_LINK_IN_SCROLL_ITEM", logger)
                return None, None

            href = await first_link.get_attribute("href")
            logger.info(f"🔗 Переход по ссылке первого товара: {href}")

            if href and href.startswith("/"):
                target_url = BASE_URL + href
            else:
                target_url = href or search_url

            await page.goto(target_url, timeout=WAIT_TIMEOUT)
            logger.info(f"📍 URL после перехода по товару: {page.url}")

            await page.wait_for_selector(cards_selector, timeout=10000)
            product_cards = page.locator(cards_selector)

        else:
            if cards_count == 0:
                logger.warning("ℹ️ Ни списка, ни карточек товара не найдено")
                await save_debug_info(page, part, "NO_LIST_NO_CARDS", logger)
                return None, None

            logger.info("🧾 Найдены карточки товара на странице поиска")
            product_cards = cards_loc

        cards_found = await product_cards.count()
        logger.info(
            f"🧾 Карточек товара (product-card-info) на странице: {cards_found}"
        )
        if cards_found == 0:
            logger.warning("ℹ️ Карточка товара не найдена")
            await save_debug_info(page, part, "NO_PRODUCT_CARD", logger)
            return None, None

        logger.info("✅ Карточка товара найдена, ищем вес")

        # 5. Проверка капчи
        captcha_selector = SELECTORS["armtek"]["captcha_img"]
        captcha_img = page.locator(captcha_selector)
        captcha_count = await captcha_img.count()
        captcha_visible = (
            await captcha_img.first.is_visible() if captcha_count > 0 else False
        )
        if captcha_visible:
            logger.warning("⚠️ Капча armtek.ru обнаружена")
            if await solve_captcha_universal(
                page=page,
                logger=logger,
                site_key="armtek",
                selectors={
                    "captcha_img": SELECTORS["armtek"]["captcha_img"],
                    "captcha_input": SELECTORS["armtek"]["captcha_input"],
                    "submit": SELECTORS["armtek"]["captcha_submit"],
                },
                max_attempts=2,
                scale_factor=2,  # при желании уменьшить/увеличить
                check_changed=False,  # если капча у armtek не мигает
                wait_after_submit_ms=3000,
            ):
                await page.wait_for_timeout(2000)
            else:
                logger.error("❌ Не удалось решить капчу")
                await save_debug_info(page, part, "CAPTCHA_FAILED", logger)
                return None, None

        # 6. Ждём блок характеристик
        try:
            await page.wait_for_selector("product-key-value", timeout=5000)
        except Exception:
            logger.warning("⚠️ Не дождались product-key-value")

        # 7. Поиск веса по значению, содержащему "кг"
        value_locator = product_cards.locator("span.font__body2")
        values_count = await value_locator.count()
        logger.info(f"⚖️ Найдено span.font__body2 в карточке: {values_count}")

        weight_text = None
        for i in range(values_count):
            v = value_locator.nth(i)
            txt = (await v.text_content() or "").strip()
            if "кг" in txt:
                logger.info(f"⚖️ Найден кандидат веса по 'кг': {txt}")
                weight_text = txt
                break

        if not weight_text:
            logger.info(f"ℹ️ Вес не найден в карточке: {part}")
            await save_debug_info(page, part, "NO_WEIGHT_TEXT", logger)
            return None, None

        # 8. Парс веса из найденного текста
        physical_match = re.search(r"(\d+(?:[.,]\d+)?)\s*кг", weight_text or "")
        if physical_match:
            physical_weight = physical_match.group(1).replace(",", ".")
            logger.info(f"✅ armtek.ru: физ. вес={physical_weight} кг")
            return physical_weight, None
        else:
            logger.warning("ℹ️ Не удалось извлечь число из текста веса")
            await save_debug_info(page, part, "PARSE_WEIGHT_FAIL", logger)
            return None, None

    except PlaywrightTimeout as e:
        logger.error(f"⏰ PlaywrightTimeout: {e}")
        await save_debug_info(page, part, f"TIMEOUT_{e.__class__.__name__}", logger)
        return None, None
    except Exception as e:
        logger.error(f"❌ armtek.ru {part}: {e}")
        await save_debug_info(page, part, f"ERROR_{type(e).__name__}", logger)
        return None, None

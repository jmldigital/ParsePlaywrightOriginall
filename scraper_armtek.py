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


import logging

logger = get_site_logger("armtek")

BASE_URL = "https://armtek.ru"
WAIT_TIMEOUT = 15000  # Больше для капчи
os.makedirs("debug_armtek", exist_ok=True)


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
#     Armtek.ru - БЫСТРО + капча в except!
#     """
#     max_retries = 2

#     for attempt in range(max_retries + 1):
#         try:
#             # 1. Goto + город (быстро)
#             search_url = f"{BASE_URL}/search?text={part}"
#             await page.goto(search_url, wait_until="domcontentloaded", timeout=10000)
#             await close_city_dialog_if_any(page, logger)

#             # 2. Результаты (ЖЕСТКО 5s)
#             await page.wait_for_selector(
#                 f"{SELECTORS['armtek']['product_list']} {SELECTORS['armtek']['product_cards']}",
#                 timeout=5000,
#                 state="attached",
#             )

#             # 3. "Не найдено"?
#             try:
#                 not_found = page.get_by_text("Товары не найдены")
#                 if await not_found.wait_for(timeout=1000):
#                     return None, None
#             except:
#                 pass

#             # 4. Карточка
#             first_link = page.locator(f"{SELECTORS['armtek']['product_list']} a").first
#             href = await first_link.get_attribute("href", timeout=2000)
#             if not href:
#                 return None, None
#             await page.goto(
#                 BASE_URL + href, wait_until="domcontentloaded", timeout=5000
#             )

#             # 5. Вес
#             await page.wait_for_selector("product-key-value", timeout=3000)
#             weight_values = page.locator(SELECTORS["armtek"]["weight_value"])

#             for i in range(await weight_values.count()):
#                 text = await weight_values.nth(i).text_content(timeout=1000)
#                 if text and "кг" in text:
#                     import re

#                     match = re.search(r"(\d+(?:[.,]\d+)?)\s*кг", text)
#                     if match:
#                         weight = match.group(1).replace(",", ".")
#                         logger.info("%s: %s кг", part, weight)
#                         return weight, None

#             logger.warning("%s: вес не найден", part)
#             return None, None

#         except Exception as e:
#             logger.error("❌ %s (попытка %d): %s", part, attempt + 1, str(e))
#             await save_debug_info(
#                 page, part, f"{type(e).__name__}_attempt{attempt}", logger, "armtek"
#             )

#             if attempt < max_retries:
#                 logger.info(f"{part}: пробуем капчу...")
#                 try:
#                     # Капча только при ошибке!
#                     captcha_modal = page.locator(
#                         "sproit-ui-modal:has(project-ui-captcha)"
#                     )
#                     if await captcha_modal.count() > 0:
#                         logger.warning("🎯 Капча в except — решаем!")
#                         await solve_captcha_universal(
#                             page=page,
#                             logger=logger,
#                             site_key="armtek",
#                             selectors={
#                                 "captcha_img": "sproit-ui-modal img[src*='blob']",
#                                 "captcha_input": SELECTORS["armtek"]["captcha_input"],
#                                 "submit": SELECTORS["armtek"]["captcha_submit"],
#                             },
#                             max_attempts=1,  # Быстро
#                         )
#                         await page.wait_for_timeout(1500)
#                     else:
#                         logger.debug("Нет project-ui-captcha")
#                 except:
#                     logger.debug("Капча-ошибка — retry")
#             else:
#                 return None, None

#     return None, None


async def scrape_weight_armtek(
    page: Page, part: str, logger: logging.Logger
) -> tuple[str, None]:
    """
    Armtek.ru — упрощенная логика с project-ui-article-card
    """
    max_retries = 2
    sel = SELECTORS["armtek"]

    # Перед циклом attempt
    await page.add_init_script(
        """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
    Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en']});
    """
    )
    await page.set_extra_http_headers(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8",
            "Referer": "https://armtek.ru/",
        }
    )

    needs_captcha_check = False

    for attempt in range(max_retries + 1):
        try:
            # 1. Goto + город
            search_url = f"{BASE_URL}/search?text={part}"
            await page.goto(search_url, wait_until="domcontentloaded", timeout=10000)

            # await close_city_dialog_if_any(page, logger)

            # 🔥 БЕСКОНЕЧНЫЙ ЦИКЛ: ждём успеха капчи
            max_captcha_retries = 20  # Большой лимит на всякий
            captcha_retry = 0

            if needs_captcha_check:
                await page.wait_for_timeout(3000)

            while True:
                captcha_modal = page.locator("sproit-ui-modal:has(project-ui-captcha)")
                if await captcha_modal.count() == 0:
                    # logger.info(f"✅ No captcha for {part}, proceed")
                    break  # ✅ Нет капчи → дальше!

                if captcha_retry >= max_captcha_retries:
                    logger.error(
                        f"❌ Max captcha retries {max_captcha_retries} for {part}"
                    )
                    raise Exception(f"captcha_timeout_{part}")

                logger.warning(
                    f"🎯 Captcha attempt #{captcha_retry+1}/{max_captcha_retries}"
                )
                solved = await solve_captcha_universal(
                    page=page,
                    logger=logger,
                    site_key="armtek",
                    selectors={
                        "captcha_img": SELECTORS["armtek"]["captcha_img"],
                        "captcha_input": SELECTORS["armtek"]["captcha_input"],
                        "submit": SELECTORS["armtek"]["captcha_submit"],
                    },
                    max_attempts=2,
                )

                await page.wait_for_timeout(2000)

                if solved:
                    logger.info(f"✅ Captcha SUCCESS for {part}")
                    needs_captcha_check = False
                    break  # ✅ РЕШЕНА → дальше!
                else:
                    logger.warning(f"❌ Captcha failed, retrying...")
                    captcha_retry += 1
                    await page.wait_for_timeout(1000)  # Пауза между попытками

            # logger.info(f"🚀 Moving to cards for {part}")

            try:
                await close_city_dialog_if_any(page, logger)
                await page.wait_for_timeout(1000)  # Стабилизация
            except Exception as city_e:
                logger.debug(f"Диалог города: {city_e} — продолжаем")

            # Жестко ждем КАРТОЧКИ 5 секунд — с retry!
            max_card_wait = 3
            for card_attempt in range(max_card_wait):
                try:
                    await page.wait_for_selector(
                        "project-ui-article-card, app-article-card-tile, .scroll-item, div[data-id]",
                        timeout=5000,
                        state="attached",
                    )
                    logger.debug("✅ Карточки появились")
                    break
                except:
                    if card_attempt < max_card_wait - 1:
                        logger.debug(f"⏳ Карточки ждем... попытка {card_attempt+1}")
                        await page.wait_for_timeout(1000)
                        continue
                    else:
                        # ❌ НЕ return! Пусть идет в большой except → капча

                        logger.warning("⏰ No cards visible — retry again")
                        needs_captcha_check = True
                        raise Exception("no_cards_after_wait")  # ← ВЫКИДЫВАЕМ!

            # 🔥 3. Ищем карточки (project-ui-article-card ИЛИ app-article-card-tile)
            card_selectors = [
                "project-ui-article-card",
                "app-article-card-tile",  # 🔥 Новый селектор!
                sel["product_cards"],  # Резерв
            ]

            products = None
            for sel_name, selector in [
                ("article-card", card_selectors[0]),
                ("app-tile", card_selectors[1]),
                *[(f"backup-{i}", s) for i, s in enumerate(card_selectors[2:], 1)],
            ]:
                try:
                    count = await page.locator(selector).count()
                    if count > 0:
                        logger.debug(f"✅ {sel_name}: {count} шт по '{selector}'")
                        products = page.locator(selector)
                        break
                except Exception as e:
                    logger.debug(f"{sel_name} skip: {e}")
                    continue

            if not products or await products.count() == 0:
                logger.warning(f"❌ cards not found for {part}")
                await save_debug_info(page, part, "no_cards_all", logger, "armtek")
                return None, None

            # 4. Берем первую карточку и переходим по ссылке
            first_card = products.first
            first_link = first_card.locator("a").first
            href = await first_link.get_attribute("href", timeout=2000)
            if not href:
                logger.warning(f"❌ link not found for {part}")
                return None, None

            full_url = href if href.startswith("http") else BASE_URL + href
            await page.goto(full_url, wait_until="domcontentloaded", timeout=20000)

            # 5. Стабилизация + поиск веса в product-card-info
            await page.wait_for_load_state("domcontentloaded", timeout=5000)
            await page.evaluate("window.scrollTo(0, 0)")
            await page.wait_for_timeout(2000)

            # Ищем в product-card-info вместо product-key-value
            card_info = page.locator("product-card-info")
            if await card_info.count() == 0:
                logger.warning(f"❌ product-card-info not found for {part}")
                return "нету веса", None  # fallback

            # logger.info("✅ product-card-info найден, ищем вес")

            # 🔥 Ubuntu: клик "Все характеристики" если есть (один таймаут)
            tech_link = page.locator('a[href="#tech-info"]').first
            if await tech_link.count() > 0 and await tech_link.is_visible():
                await tech_link.click(force=True)
                # Ждем Angular + подгрузку одним wait_for_selector (5 сек)
                # await page.wait_for_selector("product-card-info", timeout=2000)
                await card_info.wait_for(state="visible", timeout=5000)

            # Перебор weight_selectors внутри product-card-info
            weight_selectors_list = sel["weight_selectors"]
            weight_found = False

            for retry in range(2):  # 2 попытки: обычная + с увеличенным таймаутом
                for selector_idx, selector in enumerate(weight_selectors_list, 1):
                    try:
                        full_selector = f"product-card-info {selector}".strip()
                        weight_values = page.locator(full_selector)
                        count = await weight_values.count()

                        if count > 0:
                            logger.debug(
                                f"🔍 #{selector_idx}: {count} elem ({selector[:30]}...)"
                            )

                        for i in range(count):
                            try:
                                # Динамический таймаут: 1000ms обычный, 3000ms на повторной попытке
                                timeout_ms = 3000 if retry > 0 else 1000
                                text = await weight_values.nth(i).text_content(
                                    timeout=timeout_ms
                                )

                                if text and "кг" in str(text).lower():
                                    import re

                                    match = re.search(
                                        r"(\d+(?:[.,]\d+)?)\s*кг",
                                        str(text),
                                        re.IGNORECASE,
                                    )
                                    if match:
                                        weight = match.group(1).replace(",", ".")
                                        logger.info(
                                            "%s: %s кг (#%d, retry=%d)",
                                            part,
                                            weight,
                                            selector_idx,
                                            retry,
                                        )
                                        return weight, None
                            except:
                                continue

                    except Exception as e:
                        logger.debug(f"Селектор #{selector_idx} skip: {e}")
                        continue

                if retry == 0 and not weight_found:
                    logger.debug(
                        f"{part}:weight not found, repeat with extended timeout..."
                    )
                    await page.wait_for_timeout(2000)  # Даём Angular доработать
                else:
                    break

            logger.warning(
                "%s: weight not found in product-card-info (after 2 attempts)", part
            )
            return None, None

        except Exception as e:
            logger.error("❌ %s (trys %d): %s", part, attempt + 1, str(e))
            # 🔥 ФИКС ФЛАГА
            if "no_cards_after_wait" in str(e):
                logger.info(f"🔄 No cards → Late captcha mode activated")
                needs_captcha_check = True  # Подтверждаем флаг
            else:
                needs_captcha_check = False  # Сброс
            await save_debug_info(
                page, part, f"{type(e).__name__}_attempt{attempt}", logger, "armtek"
            )

    return None, None

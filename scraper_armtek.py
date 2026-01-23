"""
Асинхронный парсер armtek.ru для поиска ФИЗИЧЕСКОГО веса
С обработкой капчи через 2Captcha!
"""

import re

import random
from utils import RateLimitException
import os

import asyncio
from typing import Callable, Tuple

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


async def diagnose_error_state(page: Page, part: str, logger):
    """🎯 100% ЛОВИТ КАПЧУ с правильными таймаутами"""

    # ⏰ 6 сек на полную загрузку + анимации
    await page.wait_for_timeout(6000)

    # 1️⃣ RATE LIMIT
    try:
        rate_limit_modal = page.locator(
            "sproit-ui-modal p:has-text('Превышен лимит запросов')"
        )
        await rate_limit_modal.wait_for(
            state="visible", timeout=4000
        )  # ✅ Ждём появления!
        logger.warning(f"🚫 Rate limit detected: {part}")
        await save_debug_info(page, part, "rate_limit", logger, "armtek")
        return "rate_limit"
    except Exception:
        pass  # Rate limit нет → продолжаем

    # 2️⃣ CAPTCHA — ТОЧНЫЙ СЕЛЕКТОР!
    try:
        captcha_modal = page.locator(
            "sproit-ui-modal p:has-text('Введите код с картинки')"
        )
        await captcha_modal.wait_for(
            state="visible", timeout=4000
        )  # ✅ Ждём появления!
        logger.info(f"🎯 CAPTCHA НАЙДЕНА: {part}")
        await save_debug_info(page, part, "captcha_detected", logger, "armtek")
        return "captcha_detected"
    except Exception:
        pass

    # 3️⃣ NOTHING FOUND — АНАЛОГ CAPTCHA СЕЛЕКТОРА!
    try:
        no_result_text = page.locator(
            "project-ui-search-result p:has-text('По вашему запросу ничего не найдено')"
        )
        await no_result_text.wait_for(
            state="visible", timeout=4000
        )  # ✅ Ждём появления!
        logger.info(f"❌ No results found we are cach it!!: {part}")
        # await save_debug_info(page, part, "no_search_results", logger, "armtek")
        return "no_search_results"
    except Exception:
        pass

    logger.warning(f"⏰ No cards → global timeout: {part}")
    await save_debug_info(page, part, "global_timeout", logger, "armtek")
    return "global_timeout"


async def scrape_weight_armtek_inner(
    page, part: str, logger, check_captcha: bool = False, check_rate_limit: bool = False
):
    """check_captcha=True только после no_cards"""
    sel = SELECTORS["armtek"]

    if check_rate_limit:
        logger.info(f"🔄 Retry с новым proxy внутри inner функции: {part}")
        # Меняем proxy и продолжаем

    # 🔥 БЕСКОНЕЧНЫЙ ЦИКЛ КАПЧИ
    if check_captcha:
        logger.info(f"🔍 CAPTCHA MODE {part}")

        max_captcha_retries = 2
        captcha_retry = 0

        # ✅ ЖДЁМ ЗАГРУЗКУ СТРАНИЦЫ
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=5000)
        except:
            pass

        # ⏰ УВЕЛИЧИВАЕМ ВРЕМЯ ОЖИДАНИЯ ПОЯВЛЕНИЯ КАПЧИ!
        # Капча может появиться через 3-7 секунд после загрузки
        await page.wait_for_timeout(5000)  # ✅ Было 2000, стало 5000

        # 🎯 АКТИВНОЕ ОЖИДАНИЕ КАПЧИ
        captcha_appeared = False
        for wait_attempt in range(5):  # Проверяем 5 раз с интервалом 1 сек
            captcha_modal = page.locator("sproit-ui-modal:has(project-ui-captcha)")
            count = await captcha_modal.count()

            if count > 0:
                logger.info(f"🎯 Captcha modal detected (attempt {wait_attempt+1})")
                captcha_appeared = True
                break

            logger.debug(f"⏳ Waiting for captcha modal... ({wait_attempt+1}/5)")
            await page.wait_for_timeout(1000)

        # Если капча не появилась за 10 секунд (5000 + 5*1000)
        if not captcha_appeared:
            logger.info(f"✅ No captcha appeared after waiting → cards {part}")
            # Возможно, капчи действительно нет, продолжаем
        else:
            # ♻️ ЦИКЛ РЕШЕНИЯ КАПЧИ
            while True:
                captcha_modal = page.locator("sproit-ui-modal:has(project-ui-captcha)")
                if await captcha_modal.count() == 0:
                    logger.info(f"✅ Captcha SOLVED → cards {part}")
                    break

                if captcha_retry >= max_captcha_retries:
                    logger.error(f"❌ Max captcha retries {max_captcha_retries} {part}")
                    raise Exception(f"captcha_timeout_{part}")

                logger.warning(
                    f"🎯 Solving captcha #{captcha_retry+1}/{max_captcha_retries} {part}"
                )

                try:
                    solved = await solve_captcha_universal(
                        page=page,
                        logger=logger,
                        site_key="armtek",
                        selectors={
                            "captcha_img": SELECTORS["armtek"]["captcha_img"],
                            "captcha_input": SELECTORS["armtek"]["captcha_input"],
                            "submit": SELECTORS["armtek"]["captcha_submit"],
                        },
                        max_attempts=1,
                    )
                except Exception as captcha_error:
                    logger.error(f"❌ Captcha solve error: {captcha_error}")
                    solved = False

                # ✅ Проверяем что капча действительно исчезла
                await page.wait_for_timeout(3000)  # ✅ Было 2000, стало 3000

                captcha_still_visible = await page.locator(
                    "sproit-ui-modal:has(project-ui-captcha)"
                ).count()

                if solved and captcha_still_visible == 0:
                    logger.info(f"✅ Captcha SUCCESS (disappeared) {part}")
                    break
                elif captcha_still_visible == 0:
                    logger.info(f"✅ Captcha SOLVED by itself {part}")
                    break
                else:
                    logger.warning(f"❌ Captcha still visible, retry {captcha_retry+1}")
                    captcha_retry += 1
                    await page.wait_for_timeout(2000)

        await page.wait_for_timeout(1000)

    # Город
    try:
        await close_city_dialog_if_any(page, logger)
        await page.wait_for_timeout(1000)
    except Exception as city_e:
        logger.debug(f"Город: {city_e}")

    # Карточки
    max_card_wait = 4
    for card_attempt in range(max_card_wait):
        try:
            await page.wait_for_selector(
                "project-ui-article-card, app-article-card-tile, .scroll-item, div[data-id]",
                timeout=10000,
                state="attached",
            )
            await page.wait_for_timeout(1500)  # Стабилизация

            logger.debug("✅ Карточки появились")
            break
        except:
            if card_attempt < max_card_wait - 1:
                logger.debug(f"⏳ Карточки #{card_attempt+1}")
                await page.wait_for_timeout(1000)
            else:
                # 🔥 ДИАГНОСТИКА: 4 ЧЁТКИХ СОСТОЯНИЯ!
                error_type = await diagnose_error_state(page, part, logger)
                raise Exception(error_type)  # ← ЯВНЫЙ Exception!

    # Продукты
    card_selectors = [
        "project-ui-article-card",
        "app-article-card-tile",
        sel["product_cards"],
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
                logger.debug(f"✅ {sel_name}: {count} по '{selector}'")
                products = page.locator(selector)
                break
        except Exception as e:
            logger.debug(f"{sel_name} skip: {e}")

    if not products or await products.count() == 0:
        logger.warning(f"❌ No products {part}")
        await save_debug_info(page, part, "no_products", logger, "armtek")
        return None, None

    # Первая карточка
    first_card = products.first
    first_link = first_card.locator("a").first
    href = await first_link.get_attribute("href", timeout=2000)
    if not href:
        logger.warning(f"❌ No link {part}")
        return None, None

    full_url = href if href.startswith("http") else BASE_URL + href
    await page.goto(full_url, wait_until="domcontentloaded", timeout=20000)

    # Вес
    await page.wait_for_load_state("domcontentloaded", timeout=5000)
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(2000)

    card_info = page.locator("product-card-info")
    if await card_info.count() == 0:
        return "нету веса", None

    tech_link = page.locator('a[href="#tech-info"]').first
    if await tech_link.count() > 0 and await tech_link.is_visible():
        await tech_link.click(force=True)
        await card_info.wait_for(state="visible", timeout=5000)

    # Weight selectors
    weight_selectors_list = sel["weight_selectors"]
    for retry in range(2):
        for selector_idx, selector in enumerate(weight_selectors_list, 1):
            try:
                full_selector = f"product-card-info {selector}".strip()
                weight_values = page.locator(full_selector)
                count = await weight_values.count()

                if count > 0:
                    logger.debug(f"🔍 #{selector_idx}: {count} ({selector[:30]}...)")

                for i in range(count):
                    try:
                        timeout_ms = 3000 if retry > 0 else 1000
                        text = await weight_values.nth(i).text_content(
                            timeout=timeout_ms
                        )

                        if text and "кг" in str(text).lower():
                            match = re.search(
                                r"(\d+(?:[.,]\d+)?)\s*кг", str(text), re.IGNORECASE
                            )
                            if match:
                                weight = match.group(1).replace(",", ".")
                                logger.info(
                                    f"{part}: {weight} кг (#{selector_idx}, retry={retry})"
                                )
                                return weight, None
                    except:
                        continue
            except Exception as e:
                logger.error(f"Weight error {part}: {e}")
                raise

        if retry == 0:
            logger.debug(f"{part}: retry weights...")
            await page.wait_for_timeout(2000)

    logger.warning(f"{part}: no weight")
    return None, None


async def with_timeout(timeout_ms: int, coro: Callable, *args, **kwargs):
    """Безопасный таймаут с ЯВНЫМИ ошибками."""
    try:
        task = asyncio.create_task(coro(*args, **kwargs))
        return await asyncio.wait_for(task, timeout=timeout_ms / 1000.0)
    except asyncio.TimeoutError:
        raise Exception("GLOBAL_TIMEOUT")  # ✅ scrape_weight_armtek УВИДИТ!
    except Exception as e:
        raise e  # ✅ ПЕРЕДАЁТ no_cards_after_wait!


async def scrape_weight_armtek(
    page: Page, part: str, logger: logging.Logger
) -> Tuple[str, None]:
    """
    Armtek.ru с ГЛОБАЛЬНЫМ ТАЙМАУТОМ 4 минуты на случай зависания капчи.
    """
    max_retries = 1
    sel = SELECTORS["armtek"]
    GLOBAL_TIMEOUT_MS = 300000  # 4 минуты
    check_captcha = False  # ← ИНИЦИАЛЬНО False!
    check_rate_limit = False

    # Перед циклом: антидетект
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

    for attempt in range(max_retries + 1):  # 0, 1
        try:
            search_url = f"{BASE_URL}/search?text={part}"
            await page.goto(search_url, wait_until="domcontentloaded", timeout=20000)

            await page.wait_for_timeout(
                2000 + random.randint(0, 3000)
            )  # случайная задержка
            # # 👁️ Движение мыши
            # start_x, start_y = 100, 100
            # end_x, end_y = 400, 300
            # steps = 5
            # for i in range(steps + 1):
            #     x = start_x + (end_x - start_x) * i // steps
            #     y = start_y + (end_y - start_y) * i // steps
            #     await page.mouse.move(x, y)
            #     await page.wait_for_timeout(100 + random.randint(0, 200))

            # # 🖱️ Клик в "пустое место"
            # await page.mouse.click(50, 50)
            # await page.wait_for_timeout(500)

            # # 📥 Прокрутка
            # await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 3)")
            # await page.wait_for_timeout(1000 + random.randint(0, 1000))

            result = await with_timeout(  # 4 мин на ВСЁ!
                GLOBAL_TIMEOUT_MS,
                scrape_weight_armtek_inner,
                page,
                part,
                logger,
                check_captcha,
                check_rate_limit,
            )

            if result[0]:  # Вес найден
                return result

        except Exception as e:
            logger.error(f"❌ {part} (attempt {attempt+1}): {e}")
            err = str(e).lower()

            # 🆕 RateLimit ПРВЫЙ и АБСОЛЮТНЫЙ!
            if "rate_limit" in err:
                check_rate_limit = True
                logger.warning(
                    f"🚦 RateLimit ВЫЯВЛЕН в ошибке внутри основной функции: {part}"
                )
                return "NeedProxy", "NeedProxy"

            # Только потом остальные проверки
            if "captcha_detected" in err:
                check_captcha = True
                continue

            if "no_search_results" in err:
                return None, None

            if "global_timeout" in err:
                await save_debug_info(page, part, "global_timeout", logger, "armtek")
                return None, None

            return None, None

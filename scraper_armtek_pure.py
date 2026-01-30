"""
Armtek парсер - ТОЛЬКО парсинг DOM
Навигация делается в Crawlee!
"""

import re
import asyncio
from typing import Tuple, Optional
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout

from utils import save_debug_info

from config import SELECTORS


async def close_city_dialog(page: Page):
    """Закрывает диалог города"""
    try:
        if await page.locator("button:has-text('Верно')").is_visible(timeout=1000):
            await page.locator("button:has-text('Верно')").click()
            return
        if await page.locator("div.geo-control__click-area").is_visible(timeout=500):
            await page.locator("div.geo-control__click-area").click(force=True)
    except Exception:
        pass


async def wait_skeletons_gone(page: Page, logger, timeout: int = 20000) -> bool:
    """Глобальные скелетоны = НЕ картинки/характеристики"""
    try:

        # 🔥 1. Ждём ЛОГОТИП (страница живая!)
        logo = page.locator('img[alt="armtek logo"][src*="logo-armtek"]').first
        await logo.wait_for(state="attached", timeout=10000)
        logger.debug("✅ Логотип ARMTEK OK")

        # 🔥 2. Проверяем ПРЕЛОАДЕР (sproit-ui-loading)
        preloader = page.locator("sproit-ui-loading").first
        preloader_count = await preloader.count()

        if preloader_count > 0:
            logger.info("⏳ Ждём исчезновения прелоадера...")
            try:
                await preloader.wait_for(state="hidden", timeout=timeout)
                logger.debug("✅ Прелоадер исчез")
            except PlaywrightTimeout:
                logger.warning("⚠️ Прелоадер не исчез, продолжаем")

        # 🔥 ТОЧНЫЙ селектор ГЛОБАЛЬНЫХ!
        skeletons = page.locator(
            ".product-card__skeleton_desktop sproit-ui-skeleton, .product-card__skeleton_mobile sproit-ui-skeleton"
        )

        initial_count = await skeletons.count()
        if initial_count == 0:
            logger.debug("✅ Нет глобальных скелетонов")
            return True

        logger.info(f"⏳ Ждём {initial_count} глобальных скелетонов...")

        # Ждём ключевые (первые заголовки)
        await skeletons.filter(has_text=re.compile(r"height:\s*32px")).first.wait_for(
            state="hidden", timeout=timeout
        )

        final_count = await skeletons.count()
        success = final_count < initial_count * 0.7  # 70% исчезли = OK

        logger.debug(
            f"✅ Глобальных осталось: {final_count}/{initial_count} → {success}"
        )
        return success

    except Exception as e:
        logger.warning(f"⚠️ Skip глобальных скелетонов: {e}")
        return True  # ✅ ВАЖНО: ПРОДОЛЖАЕМ ПАРСИНГ!


async def determine_state(page: Page) -> tuple[str, str]:
    """
    Определяет состояние страницы после загрузки
    Crawlee уже сделал goto(), мы только проверяем результат

    Returns:
        tuple[str, str]: (state_name, matched_selector)
    """
    selectors = {
        "cards": SELECTORS["armtek"]["product_card-list"],
        "no_results": SELECTORS["armtek"]["no_results"],
        "captcha": SELECTORS["armtek"]["captcha"],
        "rate_limit": SELECTORS["armtek"]["rate_limit"],
        "cloudflare": SELECTORS["armtek"]["rate_limit"],
        "card_direct": SELECTORS["armtek"]["specifications"],
        "product_info": SELECTORS["armtek"]["product-card-info"],
    }

    tasks = {}

    for state_name, selector_string in selectors.items():
        # Разбиваем строку селекторов по запятой
        individual_selectors = [
            s.strip() for s in selector_string.split(",") if s.strip()
        ]

        for sel in individual_selectors:
            task = asyncio.create_task(
                page.wait_for_selector(
                    f"{sel}:has(*) >> nth=0", state="visible", timeout=20000
                )
            )
            # Сохраняем tuple (state_name, конкретный_селектор)
            tasks[task] = (state_name, sel)

    done, pending = await asyncio.wait(
        tasks.keys(), return_when=asyncio.FIRST_COMPLETED
    )

    # Отменяем остальные задачи
    for task in pending:
        task.cancel()

    try:
        first_task = list(done)[0]
        await first_task  # Проверяем на ошибки
        state_name, matched_selector = tasks[first_task]
        return state_name, matched_selector

    except PlaywrightTimeout:
        return "timeout", ""
    except Exception as e:

        return "error", ""


async def parse_weight_armtek(
    page: Page, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    """
    ТОЛЬКО парсинг веса из DOM
    Страница УЖЕ загружена Crawlee на URL поиска
    """

    await close_city_dialog(page)

    # Определяем состояние
    state, container_sel = await determine_state(page)

    # ✅ КАРТОЧКА УЖЕ ГОТОВА? → ПРОПУСКАЕМ СКЕЛЕТОНЫ!
    if state == "card_direct":
        logger.info(f"🎯 [{part}] ПРЯМО НА КАРТОЧКЕ → парсим характеристики")
        # Проверяем скелетоны ТОЛЬКО на карточке
        weight = await extract_weight(page, part, logger)

        return weight or (None, None)

    logger.debug(f"✅ Состояние для {part} найдено {state}, сектор {container_sel}")

    if state == "no_results":
        await save_debug_info(page, part, "no_state", logger, "armtek")
        return None, None
    elif state == "captcha":
        return "NeedCaptcha", "NeedCaptcha"
    elif state == "rate_limit":
        return "NeedProxy", "NeedProxy"
    elif state == "cloudflare":
        return "CloudFlare", "CloudFlare"
    elif state in ("timeout", "error"):
        await save_debug_info(page, part, "state_timeout", logger, "armtek")
        return None, None

    # 🔥 1️⃣ Ждём исчезновения скелетонов (ВСЕГДА!)
    if not await wait_skeletons_gone(page, logger, timeout=30000):
        logger.debug(f"⏳ Скелетоны не исчезли: {part}")
        # await save_debug_info(
        #     page, part, "skeleton_timeout_after_block", logger, "armtek"
        # )
        return

    # Переход к карточке товара
    try:
        if state == "cards":
            # await save_debug_info(page, part, "state_cards", logger, "armtek")

            # 🔥 ТОЧНЫЕ селекторы по классам + fallback
            link_selectors = [
                f"{container_sel} a.title[href^='/product/']",
                f"{container_sel} a.suggestion-itemtitle-name[href^='/product/']",  # Из файлов
                f"{container_sel} a[href^='/product/']",  # fallback
            ]

            link = None
            for sel in link_selectors:
                all_links = page.locator(sel).all()  # 🔥 ВСЕ ссылки!
                for temp_link in await all_links:  # 🔥 Проходим по всем
                    href = await temp_link.get_attribute("href")
                    if href:  # ✅ Первая НЕПУСТАЯ!
                        link = temp_link
                        logger.debug(f"✅ Ссылка найдена: {sel} | href={href}")
                        break
                if link:  # 🔥 Выходим если нашли
                    break

            if not link:
                # Highlight ВСЕХ ссылок для отладки
                await page.add_style_tag(
                    content="""
                    a[href^='/product/'] { 
                        border: 3px solid red !important; 
                        background: yellow !important; 
                    }
                    a.title[href^='/product/'] { 
                        border: 5px solid green !important; 
                        background: lime !important; 
                    }
                """
                )
                await save_debug_info(
                    page, part, "no_valid_links_highlighted", logger, "armtek"
                )
                return None, None

            text = await link.text_content()
            logger.debug(
                f"🔗 List FOUND: href='{await link.get_attribute('href')}' | text='{text[:50]}...'"
            )

        elif state == "product_info":
            # await save_debug_info(page, part, "product_info", logger, "armtek")

            # 🔥 Используем container_sel из determine_state!
            link_selectors = [
                f"{container_sel} a[href^='/product/']",
                f"{container_sel} a.title[href^='/product/']",
                f"{container_sel} a.suggestion-itemtitle-name[href^='/product/']",  # Из файлов
            ]

            link = None
            for sel in link_selectors:
                temp_link = page.locator(sel).first
                count = await temp_link.count()
                if count > 0:
                    href = await temp_link.get_attribute("href")
                    if href:
                        link = temp_link
                        text = await link.text_content()
                        logger.debug(
                            f"🔗 product_info FOUND: {sel} | href='{href}' | text='{text[:50]}...'"
                        )
                        break

            if not link:
                await save_debug_info(page, part, "no_product_info", logger, "armtek")
                logger.debug("🔗 product_info: 0 ссылок")

        else:
            await save_debug_info(page, part, "no_state", logger, "armtek")
            return None, None

        # Финальная проверка (дублирует, но оставляем для безопасности)
        if await link.count() == 0:
            await save_debug_info(page, part, "no_link_final", logger, "armtek")
            return None, None

        href = await link.get_attribute("href", timeout=3000)
        if not href:
            await save_debug_info(page, part, "no_herf_final", logger, "armtek")
            return None, None

        full_url = href if href.startswith("http") else "https://armtek.ru" + href

        logger.debug(f"✅  {page} {state} формируем ссылку для перехода {full_url} ")

        # Переход на карточку
        await page.goto(full_url, wait_until="domcontentloaded", timeout=30000)

        # 🔥 1️⃣ Ждём исчезновения скелетонов (ВСЕГДА!)
        # if not await wait_skeletons_gone(page, logger, timeout=30000):
        #     logger.debug(f"⏳ Скелетоны не исчезли: {part}")
        #     await save_debug_info(page, part, "skeleton_timeout_after_All", logger, "armtek")
        #     return None, None

        # Ждём появления ссылки "Все характеристики" href="#tech-info"
        # tech_link_selector = 'a[href="#tech-info"]'
        # await page.locator(tech_link_selector).first.wait_for(
        #     state="visible", timeout=30000
        # )

        # 🔥 1️⃣ БЫСТРО проверяем tech-info (3 сек) - проверка н скелетоны внутри страницы после перехода
        tech_link_selector = 'a[href="#tech-info"]'
        try:
            await page.locator(tech_link_selector).first.wait_for(
                state="visible", timeout=3000
            )
            logger.debug(f"✅ [{part}] Tech-info мгновенно готова")

        except PlaywrightTimeout:
            logger.debug(f"⏳ [{part}] Tech-info не готова → ждём скелетоны")

            # 🔥 2️⃣ Fallback: ждём скелетоны (20 сек)
            if not await wait_skeletons_gone(page, logger, timeout=20000):
                logger.warning(f"⚠️ [{part}] Скелетоны не исчезли")
                await save_debug_info(
                    page, part, "skeleton_timeout_card", logger, "armtek"
                )
                return None, None

        # Проверяем, что ссылка кликабельна и имеет текст
        tech_link = page.locator(tech_link_selector).first
        link_text = await tech_link.text_content()
        logger.debug(f"🔗 Tech link найдена: '{link_text}'")

        # КЛИК по ссылке для загрузки характеристик
        await tech_link.click()
        await page.wait_for_timeout(1500)  # Даём время на рендер вкладки

    except Exception as e:
        logger.error(f"Ошибка навигации к карточке: {e}")
        await save_debug_info(page, part, "card_error", logger, "armtek")
        return None, None

    # Парсинг веса (3 попытки)
    weight = await extract_weight(page, part, logger)
    if weight:
        logger.debug(f"🎯 Вес: {weight} ({part})")
        # return "NeedProxy", None
        # await save_debug_info(page, part, "no_weigh_extract", logger, "armtek")
        return weight, None

    # Попытка 3: последний шанс
    await page.wait_for_timeout(2000)
    weight = await extract_weight(page, part, logger)

    if weight:
        logger.info(f"🎯 Вес (delayed): {weight} ({part})")
        await save_debug_info(page, part, "no_weigh_delayed", logger, "armtek")
        return weight, None

    logger.warning(f"❌ Вес не найден: {part}")
    await save_debug_info(page, part, "not_found", logger, "armtek")
    return None, None


async def extract_weight(page: Page, part: str, logger) -> Optional[str]:
    """Извлечение веса из DOM + скриншот если fail"""
    selectors = [SELECTORS["armtek"]["product-card-weight"]]

    for sel in selectors:
        try:
            elements = page.locator(sel)
            count = await elements.count()
            logger.debug(f"🔍 {sel}: {count} элементов")

            for i in range(count):
                text = await elements.nth(i).text_content()
                if text:
                    match = re.search(
                        r"(\d+(?:[.,]\d+)?)\s*(?:кг|kg)", text, re.IGNORECASE
                    )
                    if match:
                        logger.debug(f"✅ Вес найден: {match.group(1)}")
                        return match.group(1).replace(",", ".")
        except Exception as e:
            logger.debug(f"❌ Селектор {sel}: {e}")
            continue

    # 🔥 СКРИНШОТ если вес НЕТ НАЙДЕН
    try:
        await save_debug_info(page, part, "no_weight_found-2", logger, "armtek")
        logger.warning(f"📸 Скриншот сохранён: no_weight_{part}")
    except Exception as e:
        logger.error(f"❌ Скриншот failed: {e}")

    return None

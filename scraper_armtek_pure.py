"""
Armtek парсер - ГИБРИДНЫЙ (Browser + API Intercept)
Быстрее и надежнее, чем DOM-парсинг.
"""

import asyncio
from typing import Tuple, Optional
from playwright.async_api import Page

# ═══════════════════════════════════════════════════════════════
# 🔥 НОВАЯ ГЛАВНАЯ ФУНКЦИЯ
# ═══════════════════════════════════════════════════════════════


async def parse_weight_armtek(
    page: Page, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    """
    Гибридный парсинг: Навигация браузером + Перехват API JSON
    """

    # Контейнер для результатов перехвата
    intercepted_data = {"alias": None, "weight": None}

    # ═══════════════════════════════════════════════════════════
    # 🎧 1. НАСТРОЙКА ПЕРЕХВАТЧИКА (LISTENER)
    # ═══════════════════════════════════════════════════════════
    async def handle_response(response):
        try:
            # Игнорируем всё, кроме успешных JSON
            if response.status != 200:
                return
            url = response.url

            # 🅰️ Ловим SEARCH API (получаем Alias)
            if "/search-microservice/v1/search" in url:
                try:
                    data = await response.json()
                    items = data.get("data", {}).get("articlesData", [])
                    if items:
                        alias = items[0].get("ARTICLE_ALIAS")
                        if alias:
                            intercepted_data["alias"] = alias
                            logger.debug(f"⚡ API SEARCH: Alias = {alias[:30]}...")
                except:
                    pass

            # 🅱️ Ловим DETAILS API (получаем Вес)
            elif "/articles/details/alias/" in url:
                try:
                    data = await response.json()
                    w = data.get("data", {}).get("weight")
                    if w:
                        intercepted_data["weight"] = str(w)
                        logger.debug(f"⚡ API DETAILS: Вес = {w}")
                except:
                    pass
        except:
            pass

    # Подписываемся на события сети
    page.on("response", handle_response)

    try:
        # ═══════════════════════════════════════════════════════════
        # 🔍 2. ПОИСК (ТРИГГЕР API)
        # ═══════════════════════════════════════════════════════════

        # Crawlee уже мог открыть страницу, но нам важно убедиться,
        # что мы на поиске именно нужного артикула.
        current_url = page.url
        target_search_url = f"https://armtek.ru/search?text={part}"

        if target_search_url not in current_url:
            logger.debug(f"🔗 Переход на поиск: {part}")
            await page.goto(
                target_search_url, wait_until="domcontentloaded", timeout=15000
            )
        else:
            logger.debug(f"✅ Уже на странице поиска: {part}")
            # Если мы уже тут, возможно API запрос уже прошел.
            # Можно сделать page.reload() если данные не поймались,
            # но обычно Crawlee открывает свежую страницу.

        # Ждем появления Alias (макс 4 сек)
        # Это быстрее, чем ждать DOM селекторы
        for _ in range(20):
            if intercepted_data["alias"]:
                break
            await asyncio.sleep(0.2)

        if not intercepted_data["alias"]:
            logger.warning(f"❌ [{part}] Alias не пойман (нет товара или капча)")

            # Проверка на капчу/блокировку для отладки
            if await page.locator("text=Captcha").is_visible(timeout=1000):
                return "NeedCaptcha", "NeedCaptcha"

            return None, None

        # ═══════════════════════════════════════════════════════════
        # 📦 3. ПЕРЕХОД В КАРТОЧКУ (ТРИГГЕР API)
        # ═══════════════════════════════════════════════════════════

        product_url = f"https://armtek.ru/product/{intercepted_data['alias']}"
        logger.debug(f"🔗 Переход в карточку: .../{intercepted_data['alias'][:20]}")

        # Переходим. wait_until="commit" - самый быстрый, не ждем даже DOM
        await page.goto(product_url, wait_until="domcontentloaded", timeout=15000)

        # Ждем Вес (макс 4 сек)
        for _ in range(20):
            if intercepted_data["weight"]:
                break
            await asyncio.sleep(0.2)

        if intercepted_data["weight"]:
            logger.info(f"✅ [{part}] Вес найден (API): {intercepted_data['weight']}")
            return intercepted_data["weight"], None
        else:
            logger.warning(f"⚠️ [{part}] Вес не пришел в API")
            return None, None

    except Exception as e:
        logger.error(f"❌ [{part}] Ошибка гибридного парсинга: {e}")
        return None, None

    finally:
        # Важно: отписываемся, чтобы не засорять память
        try:
            page.remove_listener("response", handle_response)
        except:
            pass

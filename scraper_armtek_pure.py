"""
Armtek парсер - ГИБРИДНЫЙ v3 (Финал)
1. Перехватывает Token + Alias на поиске
2. Делает ПРЯМОЙ запрос к API за весом (обходя market-article)
"""

import asyncio
from typing import Tuple, Optional
from playwright.async_api import Page


async def parse_weight_armtek(
    page: Page, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:

    # Хранилище для пойманных данных
    context_data = {"alias": None, "token": None}

    # ═══════════════════════════════════════════════════════════
    # 1. ПЕРЕХВАТЧИК ЗАПРОСОВ (Ловим Token)
    # ═══════════════════════════════════════════════════════════
    async def handle_request(request):
        # Если токена еще нет, пытаемся найти его в заголовках любого исходящего запроса
        if not context_data["token"]:
            headers = request.headers
            if "authorization" in headers:
                token = headers["authorization"]
                if token.startswith("Bearer "):  # Простая валидация
                    context_data["token"] = token
                    # logger.debug(f"🔑 Токен перехвачен")

    # ═══════════════════════════════════════════════════════════
    # 2. ПЕРЕХВАТЧИК ОТВЕТОВ (Ловим Alias)
    # ═══════════════════════════════════════════════════════════
    async def handle_response(response):
        try:
            if response.status != 200:
                return

            # Фильтруем URL (ищем ответ поиска, но не мусорный /type)
            if (
                "/search-microservice/v1/search" in response.url
                and "/type" not in response.url
            ):
                try:
                    data = await response.json()

                    # 🔥 БЕЗУСЛОВНОЕ ЛОГИРОВАНИЕ (для отладки, потом уберете)
                    logger.debug(
                        f"🔍 [{part}] SEARCH Response перехвачен, структура: typeView={data.get('data', {}).get('typeView')}, articlesData={len(data.get('data', {}).get('articlesData', []))} товаров"
                    )

                    # Пытаемся найти данные
                    search_data = data.get("data", {})
                    items = search_data.get("articlesData", [])

                    if items:
                        alias = items[0].get("ARTICLE_ALIAS")
                        if alias:
                            context_data["alias"] = alias
                            logger.debug(f"⚡ Alias пойман: {alias[:20]}...")
                    else:
                        # Если items пустой - логируем детали
                        logger.debug(
                            f"⚠️ [{part}] articlesData ПУСТ. Полный data: {str(data)[:800]}"
                        )

                except Exception as e:
                    logger.debug(f"⚠️ JSON error [{part}]: {e}")
        except Exception as e:
            logger.debug(f"⚠️ handle_response error [{part}]: {e}")

    # Подписываемся
    page.on("request", handle_request)
    page.on("response", handle_response)

    try:
        # ═══════════════════════════════════════════════════════════
        # 3. НАВИГАЦИЯ (Триггер для API)
        # ═══════════════════════════════════════════════════════════
        target_search_url = f"https://armtek.ru/search?text={part}"

        # Если уже на поиске - релоад, иначе переход
        if target_search_url in page.url:
            await page.reload(wait_until="domcontentloaded")
        else:
            await page.goto(
                target_search_url, wait_until="domcontentloaded", timeout=15000
            )

        # Ждем появления Alias И Токена (макс 4 сек)
        # Обычно они прилетают почти мгновенно после загрузки
        for _ in range(20):
            if context_data["alias"] and context_data["token"]:
                break
            await asyncio.sleep(0.2)

        if not context_data["alias"]:
            logger.warning(f"❌ [{part}] Alias не пойман (нет товара?)")
            return None, None

        if not context_data["token"]:
            logger.warning(f"❌ [{part}] Токен не пойман (защита?)")
            return None, None

        # ═══════════════════════════════════════════════════════════
        # 4. ПРЯМОЙ ЗАПРОС К API ЗА ВЕСОМ
        # ═══════════════════════════════════════════════════════════
        # Нам не нужно переходить в карточку! Мы сами запрашиваем API

        details_url = f"https://armtek.ru/rest/ru/assortment-microservice/v1/articles/details/alias/{context_data['alias']}?weightUnitType=kg&lengthUnitType=cm&country=ru"

        logger.debug(f"⚡ Запрос API details (Direct)...")

        api_response = await page.request.get(
            details_url,
            headers={
                "Authorization": context_data[
                    "token"
                ],  # Используем перехваченный токен
                "Referer": f"https://armtek.ru/product/{context_data['alias']}",
                "x-app-version": "1.0.331",  # Можно обновить если сайт сменит версию
                "x-ca-external-system": "IM_RU",
                "x-ca-vkorg": "4000",
            },
        )

        if api_response.status != 200:
            logger.warning(f"⚠️ API Details error: {api_response.status}")
            return None, None

        data = await api_response.json()
        item_data = data.get("data", {})

        # Ищем вес
        weight = item_data.get("weight")

        # Если веса нет в основном поле, ищем в атрибутах (редкий кейс)
        if not weight:
            attrs = item_data.get("attributes", [])
            for a in attrs:
                if a.get("name") in ["Вес", "Weight"] or a.get("code") == "WEIGHT":
                    weight = a.get("value")
                    break

        if weight:
            logger.info(f"✅ [{part}] Вес: {weight}")
            return str(weight), None
        else:
            logger.warning(f"⚠️ [{part}] Вес пуст в API")
            return None, None

    except Exception as e:
        logger.error(f"❌ [{part}] Ошибка: {e}")
        return None, None

    finally:
        # Убираем слушатели, чтобы не дублировались
        try:
            page.remove_listener("request", handle_request)
            page.remove_listener("response", handle_response)
        except:
            pass

"""
Armtek парсер - TURBO HYBRID (Session Reuse)
1. Первый раз: открывает браузер, крадет Токен.
2. Дальше: работает ТОЛЬКО через API (без загрузки страниц).
3. При 401: снова открывает браузер и обновляет Токен.
"""

from utils import _save_full_page_screenshot
import asyncio
from typing import Tuple, Optional
from playwright.async_api import Page, APIRequestContext

# ⚡ ГЛОБАЛЬНЫЙ КЭШ СЕССИИ (Токен + Куки)
# Храним: {"token": "Bearer ...", "cookies_ready": False}
SESSION_CACHE = {"token": None, "last_success": 0}

TOKEN_LOCK = asyncio.Lock()


import aiohttp

# Глобальная переменная для сессии
HTTP_SESSION: aiohttp.ClientSession = None


async def get_http_session():
    global HTTP_SESSION
    if HTTP_SESSION is None or HTTP_SESSION.closed:
        # Создаем сессию с увеличенными лимитами
        connector = aiohttp.TCPConnector(limit=50)
        HTTP_SESSION = aiohttp.ClientSession(connector=connector)
    return HTTP_SESSION


async def reset_armtek_session(page: Page, logger):
    """Полный сброс авторизации и кэша"""
    async with TOKEN_LOCK:
        SESSION_CACHE["token"] = None
        # Очищаем куки в браузере, чтобы при следующем заходе сессия была чистой
        try:
            await page.context.clear_cookies()
            logger.warning(
                "🔄 [ARMTEK] Сессия сброшена из-за таймаута. Попробуем зайти заново."
            )
        except Exception as e:
            logger.error(f"Не удалось очистить куки: {e}")


async def get_or_refresh_token(page: Page, logger) -> Optional[str]:
    """
    Гарантирует наличие валидного токена.
    Использует Lock, чтобы не открывать браузер во всех потоках сразу.
    """
    # 1. Быстрая проверка вне блокировки
    if SESSION_CACHE["token"]:
        return SESSION_CACHE["token"]

    async with TOKEN_LOCK:
        # 2. Повторная проверка внутри блокировки (double-check locking)
        # Возможно, пока мы ждали очереди, другой поток уже получил токен
        if SESSION_CACHE["token"]:
            return SESSION_CACHE["token"]

        logger.info("🔄 [ARMTEK] Поток получил доступ к обновлению токена...")

        token_future = asyncio.Future()

        async def handle_request(request):
            if not token_future.done():
                headers = request.headers
                if "authorization" in headers:
                    token = headers["authorization"]
                    if token.startswith("Bearer "):
                        token_future.set_result(token)
                        # Останавливаем загрузку страницы, она нам больше не нужна
                        asyncio.create_task(page.evaluate("() => window.stop()"))

        page.on("request", handle_request)

        try:
            # Используем wait_until="commit" вместо "domcontentloaded"
            # Это еще быстрее - выходим как только сервер ответил первыми байтами
            await page.goto(
                "https://armtek.ru/search?text=BUSHING",
                wait_until="commit",
                timeout=15000,
            )
            try:
                token = await asyncio.wait_for(token_future, timeout=10.0)
                SESSION_CACHE["token"] = token
                logger.info("🔑 [ARMTEK] Токен успешно получен и сохранен в кэш!")
                return token
            except asyncio.TimeoutError:
                logger.warning("❌ [ARMTEK] Не удалось перехватить токен (Timeout)")
                return None

        except Exception as e:
            logger.error(f"❌ [ARMTEK] Ошибка получения токена: {e}")
            return None
        finally:
            try:
                page.remove_listener("request", handle_request)
            except:
                pass


async def execute_api_chain(
    session: aiohttp.ClientSession, token: str, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:

    headers = {
        "Authorization": token,
        "x-app-version": "1.0.331",
        "x-ca-external-system": "IM_RU",
        "x-ca-vkorg": "4000",
        "Referer": "https://armtek.ru/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    try:
        payload = {
            "query": part,
            "queryType": 1,
            "page": 1,
            "filters": {"text": part},
            "userInfo": {"VKORG": "4000", "VSTELS_LIST": ["ME86"]},
            "ZZSIGN": "S",
        }

        # POST запрос через aiohttp
        async with session.post(
            "https://armtek.ru/rest/ru/search-microservice/v1/search",
            headers=headers,
            json=payload,
            timeout=10,
        ) as response:

            if response.status == 429:
                return "NeedCaptcha", None
            if response.status == 401:
                return "401", None
            if response.status != 200:
                return None, None

            search_data = await response.json()
            # logger.debug(f"🔍 [ARMTEK] Response for {part}: {search_data}")
            items = search_data.get("data", {}).get("articlesData", [])
            if not items:
                return None, None
            alias = items[0].get("ARTICLE_ALIAS")
            if not alias:
                return None, None

        # GET запрос через aiohttp
        details_url = f"https://armtek.ru/rest/ru/assortment-microservice/v1/articles/details/alias/{alias}?weightUnitType=kg&lengthUnitType=cm&country=ru"

        async with session.get(details_url, headers=headers, timeout=10) as response:
            if response.status == 401:
                return "401", None
            if response.status != 200:
                return None, None

            data = await response.json()
            item_data = data.get("data", {})
            weight = item_data.get("weight") or next(
                (
                    a.get("value")
                    for a in item_data.get("attributes", [])
                    if a.get("code") == "WEIGHT"
                ),
                None,
            )

            return str(weight) if weight else None, None

    except Exception as e:
        logger.debug(f"⚠️ API Error: {e}")
        return None, None


async def parse_weight_armtek(
    page: Page, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    max_attempts = 2

    for attempt in range(max_attempts):
        try:
            # ✅ Исправлено для Python 3.10: используем wait_for вместо asyncio.timeout
            async def perform_request():
                session = await get_http_session()
                token = await get_or_refresh_token(page, logger)

                if not token:
                    return None, None

                weight, _ = await execute_api_chain(session, token, part, logger)
                return weight

            # Запускаем выполнение с таймаутом 40 секунд
            weight = await asyncio.wait_for(perform_request(), timeout=40.0)

            # Если сайт ответил, что сессия протухла (401)
            if weight == "401":
                SESSION_CACHE["token"] = None
                continue  # Идем на следующую попытку

            return (
                (weight, None)
                if weight not in ["NeedCaptcha", "401"]
                else (weight, weight)
            )

        except (asyncio.TimeoutError, Exception) as e:
            # Теперь ошибка "module 'asyncio' has no attribute 'timeout'" исчезнет
            logger.error(f"⌛ [ARMTEK] Попытка {attempt+1} не удалась: {e}")
            if attempt == 0:
                await reset_armtek_session(page, logger)
                # await asyncio.sleep(2)
            else:
                return "Timeout", None

    return None, None

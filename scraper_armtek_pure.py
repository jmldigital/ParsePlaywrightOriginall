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
                wait_until="domcontentloaded",
                timeout=30000,
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
    request_context: APIRequestContext, token: str, part: str, logger, page: Page = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Выполняет цепочку API запросов: Search -> Alias -> Details -> Weight
    Работает без рендеринга страницы!
    """
    headers = {
        "Authorization": token,
        "x-app-version": "1.0.331",
        "x-ca-external-system": "IM_RU",
        "x-ca-vkorg": "4000",
        "Referer": "https://armtek.ru/",
    }

    # 1. SEARCH API (POST)
    try:
        # Пейлоад в точности как в браузере
        payload = {
            "query": part,
            "queryType": 1,
            "page": 1,
            "filters": {"text": part},
            "userInfo": {
                "VKORG": "4000",
                "VSTELS_LIST": ["ME86"],  # Обязательный параметр для гостя
            },
            "ZZSIGN": "S",
        }

        # ВАЖНО: используем json=..., чтобы Playwright отправил application/json
        search_response = await request_context.post(
            "https://armtek.ru/rest/ru/search-microservice/v1/search",
            headers=headers,
            data=payload,
        )

        # 🚨 ОБРАБОТКА БЛОКИРОВОК
        if search_response.status == 429:
            # await page.wait_for_timeout(3000) # Даем время на отрисовку капчи
            await _save_full_page_screenshot(page, "armtek", part, "429_block")
            return "NeedCaptcha", None

        if search_response.status == 401:
            return "401", None  # Сигнал обновить токен

        if search_response.status != 200:
            # Читаем тело ошибки, чтобы понять причину
            err_text = await search_response.text()
            logger.debug(
                f"⚠️ Search API Error {search_response.status}: {err_text[:200]}"
            )
            return None, None

        search_data = await search_response.json()
        items = search_data.get("data", {}).get("articlesData", [])

        if not items:
            logger.warning(f"❌ [{part}] Не найдено в поиске (API)")
            return None, None

        alias = items[0].get("ARTICLE_ALIAS")
        if not alias:
            return None, None

    except Exception as e:
        logger.debug(f"⚠️ Ошибка Search API: {e}")
        return None, None

    # 2. DETAILS API (GET)
    try:
        details_url = f"https://armtek.ru/rest/ru/assortment-microservice/v1/articles/details/alias/{alias}?weightUnitType=kg&lengthUnitType=cm&country=ru"

        details_response = await request_context.get(
            details_url,
            headers={**headers, "Referer": f"https://armtek.ru/product/{alias}"},
        )

        if details_response.status == 401:
            return "401", None

        if details_response.status != 200:
            return None, None

        data = await details_response.json()
        item_data = data.get("data", {})

        # Поиск веса
        weight = item_data.get("weight")
        if not weight:
            # Fallback: Attributes
            attrs = item_data.get("attributes", [])
            for a in attrs:
                if a.get("name") in ["Вес", "Weight"] or a.get("code") == "WEIGHT":
                    weight = a.get("value")
                    break

        if weight:
            return str(weight), None

        return None, None

    except Exception as e:
        logger.debug(f"⚠️ Ошибка Details API: {e}")
        return None, None


async def parse_weight_armtek(
    page: Page, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    """
    Главная функция.
    Пытается использовать быстрый API. При неудаче (401) обновляет токен.
    """

    # 1. Получаем токен (если нет - идем в браузер)
    token = await get_or_refresh_token(page, logger)

    if not token:
        logger.error(f"❌ [{part}] Не удалось получить доступ к API")
        return None, None

    # 2. Пробуем выполнить API-цепочку
    # Используем page.request (он разделяет куки с page, это важно!)
    weight, _ = await execute_api_chain(page.request, token, part, logger, page=page)

    # 3. Реакция на специальные статусы
    if weight == "NeedCaptcha":
        # Сбрасываем токен, так как он больше не валиден без решения капчи
        SESSION_CACHE["token"] = None
        # Возвращаем статус в main.py, чтобы он вызвал _solve_captcha
        return "NeedCaptcha", "NeedCaptcha"

    # 3. Обработка протухшего токена (401)
    if weight == "401":
        logger.warning(f"🔄 [{part}] Токен протух (401). Обновляем...")
        SESSION_CACHE["token"] = None  # Сбрасываем

        # Получаем новый
        token = await get_or_refresh_token(page, logger)
        if not token:
            return None, None

        # Повторяем запрос
        weight, _ = await execute_api_chain(
            page.request, token, part, logger, page=page
        )

    if weight and weight != "401":
        # logger.info(f"✅ [{part}] Вес (Turbo API): {weight}")
        return weight, None
    else:
        # Если API не вернул вес (но не 401), значит товара нет или веса нет
        return None, None

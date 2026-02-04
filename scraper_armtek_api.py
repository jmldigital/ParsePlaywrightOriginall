"""
Armtek API парсер - БЫСТРЫЙ через прямые API запросы
Вместо браузерного парсинга используем REST API
Ускорение: 20-100x по сравнению с Playwright
"""

import re
import asyncio
from typing import Tuple, Optional, Dict, List
import httpx
from playwright.async_api import Browser, BrowserContext


COOKIE_STRING = "_ym_uid=1766656908325550353; _ym_d=1766656908; referrer=; _ym_isad=1; _ym_visorc=b; cf_clearance=urRPYstqN8HgZuGWs7vKtxzbjnxnGpOnmg5sMc4Xdmw-1770019445-1.2.1.1-LTUeBpxICK.RZ6ORYv9aTSV0Fi7VWZeFyzDkvuiZNNx.5td3t58pSgjQ5RnDliO29MTVyCmla3ndsq0oWC6r66vqW009fWq9peqYybTm0bV_UYGIzB7lqvs47OEZoUgWpthOdz6mRmHl4RSnbx0Sl4R6e8WGxpfW5wTn5hkdSnwpLEHec.LvB3BWmCCUPXTfDqxmn0VtTvTxvsLdNnxlmwmpOsFhNlU62.y6zAi5wRE; app_options=SlRkQ0pUSXljMlZ5ZG1WeVRHOWpZWFJwYjI1SWNtVm1KVEl5SlROQkpUSXlhSFIwY0hNbE0wRWxNa1lsTWtaaGNtMTBaV3N1Y25VbE1rWnpaV0Z5WTJnbE0wWjBaWGgwSlRORU5UZzRNekV0TUVjd01qQWxNaklsTWtNbE1qSm9iM04wSlRJeUpUTkJKVEl5YUhSMGNITWwxNTM1NDIyTTBFbE1rWWxNa1poY20xMFpXc3VjblVsTWpJbE1rTWxNakoyYTI5eVp5VXlNaVV6UVNVeU1qUXdNREFsTWpJbE1rTWxNakp6YUc5M1JYaDBjbUZOWlhOellXZGxjeVV5TWlVelFXWmhiSE5sSlRKREpUSXljMkZ3UkdsellXSnNaV1FsTWpJbE0wRm1ZV3h6WlNVM1JBJTNEJTNE1535422"


def get_raw_data():
    # Парсинг куков в словарь
    cookies = {}
    for item in COOKIE_STRING.split("; "):
        if "=" in item:
            key, val = item.split("=", 1)
            cookies[key] = val


class ArmtekApiParser:
    """
    Быстрый парсер Armtek через REST API

    Алгоритм:
    1. POST /v1/search → получаем список (typeView: list)
    2. Берём первый ARTICLE_ALIAS
    3. GET /articles/details/alias/{ARTICLE_ALIAS} → получаем weight
    """

    def __init__(self, logger):
        self.logger = logger
        self.client: Optional[httpx.AsyncClient] = None
        self.cookies: Dict[str, str] = {}
        # self.bearer_token: Optional[str] = None
        self.bearer_token: Optional[str] = (
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3OTk0MDcyMzEsImtleSI6ImE3ZmU3ZGEwNmMxMjllOTY3NTgxOTdiOTNhMjZmZDhhIiwidHlwZSI6Imc5WCIsImRhdGEiOnsibG9naW4iOiJHVUVTVF8xNzY4MzAzMjMxMjg0NDUxIiwidXVpZCI6IkdkZDAxZWY1Yjk1YmZiOGRiYWM1Y2JiMjhmNDRiYmZhYSIsInV0eXBlIjoiRyIsInVmdW5jdGlvbiI6bnVsbCwiYWNsU2NoZW1lVHlwZSI6IltcImYwOGI3YzdkLTkxMGQtNDE5MC0zMWVhLWYxOGRmNGIzMTBjMlwiXSJ9fQ==.6zGZbU6lRIYtQHrEDiiXifS5meAZ+1jnQ7xZxMJfNS8="
        )

        # 🟢 ПАРСИНГ КУК ПРЯМО ЗДЕСЬ
        self.cookies = {}
        for item in COOKIE_STRING.split("; "):
            if "=" in item:
                key, val = item.split("=", 1)
                self.cookies[key] = val

        self.logger.info(
            f"🍪 Загружено {len(self.cookies)} кук из хардкода (включая cf_clearance: {'cf_clearance' in self.cookies})"
        )

        # self.captcha_hash: Optional[str] = None
        # API endpoints
        self.search_url = "https://armtek.ru/rest/ru/search-microservice/v1/search"
        self.details_url_template = "https://armtek.ru/rest/ru/assortment-microservice/v1/articles/details/alias/{alias}"
        self.captcha_hash = "c2e18f03dc0dc7bb8d275d2cfa48fdd1"  # Свежий из DevTools

        # Статистика
        self.stats = {
            "api_calls": 0,
            "cache_hits": 0,
            "errors": 0,
        }

    async def __aenter__(self):
        """Инициализация HTTP-клиента"""
        self.client = httpx.AsyncClient(
            timeout=15.0,
            follow_redirects=True,
            limits=httpx.Limits(max_keepalive_connections=20, max_connections=100),
        )
        return self

    async def __aexit__(self, *args):
        """Закрытие клиента"""
        if self.client:
            await self.client.aclose()

    # async def refresh_session(self, browser: Browser):
    #     """
    #     Обновление куков и токенов через браузер
    #     Вызывается:
    #     - При старте парсера
    #     - При ошибке 403/401
    #     - Каждые 10-15 минут
    #     """
    #     self.logger.info("🔄 Обновление сессии Armtek через браузер...")

    #     context = await browser.new_context(
    #         ignore_https_errors=True,
    #         user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
    #     )
    #     page = await context.new_page()

    #     try:
    #         # Перехватываем API-запросы для извлечения токена
    #         captured_token = None
    #         captured_cookies = None

    #         async def capture_auth(route):
    #             nonlocal captured_token

    #             request = route.request

    #             # Извлекаем Bearer токен
    #             auth_header = request.headers.get("authorization", "")
    #             if auth_header.startswith("Bearer ") and not captured_token:
    #                 captured_token = auth_header.replace("Bearer ", "")
    #                 self.logger.debug(f"✅ Токен захвачен: {captured_token[:30]}...")

    #             await route.continue_()

    #         # Устанавливаем перехватчик на все запросы
    #         await page.route("**/*", capture_auth)

    #         # Открываем сайт (БЕЗ networkidle - быстрее!)
    #         self.logger.debug("🌐 Загрузка armtek.ru...")
    #         try:
    #             await page.goto(
    #                 "https://armtek.ru",
    #                 wait_until="domcontentloaded",  # ← ИЗМЕНЕНО с networkidle
    #                 timeout=60000,  # ← УВЕЛИЧЕНО с 30s до 60s
    #             )
    #         except Exception as e:
    #             self.logger.warning(f"⚠️ Ошибка загрузки страницы: {e}")
    #             # Пробуем продолжить

    #         # Ждём логотип (проверка что страница загрузилась)
    #         try:
    #             await page.wait_for_selector(
    #                 'img[alt="armtek logo"]', timeout=15000, state="visible"
    #             )
    #             self.logger.debug("✅ Логотип найден")
    #         except Exception as e:
    #             self.logger.warning(f"⚠️ Логотип не найден: {e}")

    #         # Делаем ПРОСТОЙ поиск для триггера API
    #         self.logger.debug("🔍 Триггер поиска для получения токена...")
    #         try:
    #             # Ищем поле поиска (несколько вариантов)
    #             search_selectors = [
    #                 'input[type="search"]',
    #                 'input[placeholder*="поиск"]',
    #                 'input[name="search"]',
    #                 "input.search-input",
    #             ]

    #             search_input = None
    #             for selector in search_selectors:
    #                 try:
    #                     search_input = page.locator(selector).first
    #                     if await search_input.is_visible(timeout=3000):
    #                         break
    #                 except:
    #                     continue

    #             if search_input:
    #                 await search_input.fill("test")
    #                 await search_input.press("Enter")

    #                 # Ждём API запрос (токен должен быть захвачен)
    #                 await asyncio.sleep(3)
    #             else:
    #                 self.logger.warning("⚠️ Поле поиска не найдено")

    #         except Exception as e:
    #             self.logger.warning(f"⚠️ Ошибка поиска: {e}")

    #         # Извлекаем ВСЕ куки
    #         cookies_list = await context.cookies()

    #         captured_cookies = {c["name"]: c["value"] for c in cookies_list}

    #         # Проверка результатов
    #         if captured_token:
    #             self.bearer_token = captured_token
    #             self.logger.info(f"✅ Bearer токен: {captured_token[:30]}...")
    #         else:
    #             self.logger.warning("⚠️ Bearer токен НЕ захвачен")
    #             # Пробуем извлечь из куков или использовать дефолтный
    #             # (некоторые сессии могут работать без явного токена в заголовках)

    #         if captured_cookies:
    #             self.cookies = captured_cookies

    #             # ✅ Делаем тестовый запрос для получения captchaHash
    #             try:
    #                 test_response = await self.client.post(
    #                     self.search_url,
    #                     headers=self._build_headers(),
    #                     cookies=self.cookies,
    #                     json={
    #                         "query": "test",
    #                         "queryType": 1,
    #                         "page": 1,
    #                         "filters": {"text": "test"},
    #                         "userInfo": {"VKORG": "4000", "VSTELS_LIST": ["ME86"]},
    #                         "ZZSIGN": "S",
    #                     },
    #                 )

    #                 if test_response.status_code == 429:
    #                     data = test_response.json()
    #                     hash_val = data.get("data", {}).get("captchaHash")
    #                     if hash_val:
    #                         self.captcha_hash = hash_val
    #                         self.logger.info(
    #                             f"✅ Начальный captchaHash: {hash_val[:16]}..."
    #                         )
    #             except Exception as e:
    #                 self.logger.warning(f"⚠️ Не удалось получить captchaHash: {e}")

    #             # Проверяем cf_clearance
    #             if "cf_clearance" in self.cookies:
    #                 cf_value = self.cookies["cf_clearance"]
    #                 self.logger.info(f"✅ CloudFlare: {cf_value[:20]}...")
    #             else:
    #                 self.logger.warning(
    #                     "⚠️ cf_clearance НЕ найден (может быть проблема)"
    #                 )

    #             self.logger.info(f"✅ Куки: {len(self.cookies)} шт")
    #         else:
    #             self.logger.warning("⚠️ Куки не получены")

    #         # Успех если есть хотя бы куки
    #         return bool(captured_cookies)

    #     except Exception as e:
    #         self.logger.error(f"❌ Критическая ошибка обновления сессии: {e}")
    #         import traceback

    #         self.logger.debug(traceback.format_exc())
    #         return False

    #     finally:
    #         await context.close()

    def _build_headers(self, referer: str = "https://armtek.ru/") -> Dict[str, str]:
        """Построение заголовков для API-запроса"""
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Accept-Language": "ru,en;q=0.9",
            "Content-Type": "application/json",
            "Origin": "https://armtek.ru",
            "Referer": referer,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            # Специальные заголовки Armtek
            "x-app-version": "1.0.330",
            "x-ca-external-system": "IM_RU",
            "x-ca-vkorg": "4000",
            # Sec-CH-UA
            "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }

        # Bearer токен
        if self.bearer_token:
            headers["Authorization"] = f"Bearer {self.bearer_token}"

        if self.captcha_hash:
            headers["x-auth-captcha-hash"] = self.captcha_hash

        return headers

    async def search_article(
        self, article: str, browser: Browser = None
    ) -> Optional[str]:
        """
        Шаг 1: Поиск детали через POST /v1/search

        Returns:
            ARTICLE_ALIAS первого результата или None
        """
        self.stats["api_calls"] += 1

        headers = self._build_headers(
            referer=f"https://armtek.ru/search?text={article}"
        )

        # ✅ ПРАВИЛЬНЫЙ Payload (из DevTools)
        payload = {
            "query": article,  # ← ИЗМЕНЕНО: было "text"
            "queryType": 1,  # ← ДОБАВЛЕНО: тип поиска (1 = по артикулу)
            "page": 1,  # ← ДОБАВЛЕНО: номер страницы
            "filters": {"text": article},  # ← ДОБАВЛЕНО: фильтры поиска
            "userInfo": {  # ← ДОБАВЛЕНО: информация о пользователе
                "VKORG": "4000",
                "VSTELS_LIST": ["ME86"],
            },
            "ZZSIGN": "S",  # ← ДОБАВЛЕНО: подпись запроса
        }

        try:
            response = await self.client.post(
                self.search_url, headers=headers, cookies=self.cookies, json=payload
            )

            # Логирование для отладки
            self.logger.debug(f"[{article}] Статус: {response.status_code}")

            if response.status_code == 429:
                self.logger.warning(
                    f"⏳ [{article}] Rate limit 429 - обновляем captcha"
                )

                try:
                    data = response.json()
                    new_hash = data.get("data", {}).get("captchaHash")

                    if new_hash:
                        self.captcha_hash = new_hash
                        self.logger.info(f"🧩 Captcha обновлён: {new_hash[:16]}...")

                        # Ждём 1-2 секунды и повторяем
                        await asyncio.sleep(1.5)

                        # ОДИН повтор с новым хешем
                        return await self.search_article(article, browser=None)
                except Exception as e:
                    self.logger.error(f"❌ [{article}] Ошибка обработки 429: {e}")

                self.stats["errors"] += 1
                return None

            # 🔥 ОБРАБОТКА ОШИБОК АВТОРИЗАЦИИ
            if response.status_code in (401, 403):
                self.logger.warning(
                    f"🔒 [{article}] Ошибка авторизации {response.status_code}"
                )
                self.logger.debug(response.text)

                # ТОЛЬКО 1 ПОПЫТКА обновления сессии
                if browser and not hasattr(self, "_session_refresh_attempted"):
                    self._session_refresh_attempted = True
                    self.logger.info("🔄 Пытаемся обновить сессию...")

                    if await self.refresh_session(browser):
                        self.logger.info("✅ Сессия обновлена, повтор запроса...")
                        # Рекурсивный вызов БЕЗ browser (чтобы не зациклиться)
                        return await self.search_article(article, browser=None)
                    else:
                        self.logger.error("❌ Обновление сессии не помогло")

                self.stats["errors"] += 1
                return None

            if response.status_code != 200:
                self.logger.warning(
                    f"❌ [{article}] API search error: {response.status_code}"
                )
                self.logger.debug(response.text)
                self.stats["errors"] += 1
                return None

            data = response.json()

            # Проверка data
            if not data.get("data"):
                self.logger.debug(f"⚠️ [{article}] Пустой ответ API (data: null)")
                return None

            # Извлекаем articlesData
            articles_data = data["data"].get("articlesData", [])

            if not articles_data:
                self.logger.debug(f"❌ [{article}] Не найдено в базе Armtek")
                return None

            # Берём ПЕРВЫЙ результат
            first_article = articles_data[0]
            article_alias = first_article.get("ARTICLE_ALIAS")

            if not article_alias:
                self.logger.warning(f"⚠️ [{article}] ARTICLE_ALIAS отсутствует")
                return None

            self.logger.debug(f"✅ [{article}] Найден alias: {article_alias}")
            return article_alias

        except httpx.RequestError as e:
            self.logger.error(f"❌ [{article}] Ошибка сети: {e}")
            self.stats["errors"] += 1
            return None
        except Exception as e:
            self.logger.error(f"❌ [{article}] Ошибка парсинга: {e}")
            self.stats["errors"] += 1
            return None

    async def get_article_details(self, alias: str, article: str) -> Optional[Dict]:
        """
        Шаг 2: Получение деталей через GET /articles/details/alias/{alias}

        Returns:
            Dict с данными детали или None
        """
        self.stats["api_calls"] += 1

        url = self.details_url_template.format(alias=alias)
        url += "?weightUnitType=kg&lengthUnitType=cm&country=ru"

        headers = self._build_headers(referer=f"https://armtek.ru/product/{alias}")

        try:
            response = await self.client.get(url, headers=headers, cookies=self.cookies)

            if response.status_code != 200:
                self.logger.warning(
                    f"❌ [{article}] API details error: {response.status_code}"
                )
                self.stats["errors"] += 1
                return None

            data = response.json()

            if not data.get("data"):
                self.logger.warning(f"⚠️ [{article}] Детали не найдены (data: null)")
                return None

            return data["data"]

        except Exception as e:
            self.logger.error(f"❌ [{article}] Ошибка получения деталей: {e}")
            self.stats["errors"] += 1
            return None

    def extract_weight(self, details: Dict, article: str) -> Optional[str]:
        """
        Извлечение веса из данных детали

        Args:
            details: Данные из API /articles/details/alias/{alias}
            article: Артикул (для логирования)

        Returns:
            Вес в кг (строка) или None
        """
        try:
            # Прямое поле weight
            weight = details.get("weight")

            if weight:
                # Убираем нули после запятой если есть
                weight_float = float(weight)

                # Форматируем
                if weight_float == int(weight_float):
                    weight_str = str(int(weight_float))
                else:
                    weight_str = str(weight_float)

                self.logger.debug(f"✅ [{article}] Вес: {weight_str} кг")
                return weight_str

            # Альтернатива: поиск в attributes
            attributes = details.get("attributes", [])
            for attr in attributes:
                name = attr.get("name", "").lower()
                if "вес" in name or "weight" in name:
                    value = attr.get("value")
                    if value:
                        # Извлекаем число
                        match = re.search(r"(\d+(?:[.,]\d+)?)", str(value))
                        if match:
                            return match.group(1).replace(",", ".")

            self.logger.debug(f"⚠️ [{article}] Вес не указан в API")
            return None

        except Exception as e:
            self.logger.error(f"❌ [{article}] Ошибка извлечения веса: {e}")
            return None

    async def parse_weight(
        self, article: str, browser: Browser = None
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        ГЛАВНАЯ ФУНКЦИЯ: Полный цикл парсинга веса

        Args:
            article: Артикул детали
            browser: Playwright Browser (для обновления сессии при ошибках)

        Returns:
            (physical_weight, volumetric_weight)
            Объёмный вес всегда None (Armtek не предоставляет)
        """
        try:
            # Шаг 1: Поиск → получение ARTICLE_ALIAS
            alias = await self.search_article(article, browser)

            if not alias:
                return None, None

            # Шаг 2: Детали → получение weight
            details = await self.get_article_details(alias, article)

            if not details:
                return None, None

            # Шаг 3: Извлечение веса
            weight = self.extract_weight(details, article)

            return weight, None  # Объёмный вес отсутствует в API

        except Exception as e:
            self.logger.error(f"❌ [{article}] Критическая ошибка: {e}")
            self.stats["errors"] += 1
            return None, None

    def get_stats(self) -> Dict:
        """Возврат статистики"""
        return self.stats.copy()


# ═══════════════════════════════════════════════════════════
# ИНТЕГРАЦИЯ В СУЩЕСТВУЮЩИЙ ПАРСЕР
# ═══════════════════════════════════════════════════════════


async def parse_weight_armtek_api(
    article: str, logger, api_parser: ArmtekApiParser, browser: Browser = None
) -> Tuple[Optional[str], Optional[str]]:
    """
    Обёртка для совместимости с существующим парсером

    Использование в ParserCrawler:

    # В __init__:
    self.armtek_api_parser = None

    # В setup():
    self.armtek_api_parser = ArmtekApiParser(logger)
    await self.armtek_api_parser.__aenter__()

    # Обновление сессии (1 раз при старте):
    await self.armtek_api_parser.refresh_session(browser)

    # В _route_to_parser для Armtek:
    if site == "armtek":
        physical, volumetric = await parse_weight_armtek_api(
            part, logger, self.armtek_api_parser, browser
        )
        return {ARMTEK_P_W: physical, ARMTEK_V_W: volumetric}
    """
    return await api_parser.parse_weight(article, browser)

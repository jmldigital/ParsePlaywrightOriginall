"""
Парсер на Crawlee - полностью оптимизированная версия
- Авторизация через Crawlee session persistence
- URL генерация вынесена из скрейперов
- Скрейперы делают только парсинг DOM
"""

from crawlee.sessions import SessionPool
import asyncio
import sys
import io
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
import aiohttp
import asyncio
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from crawlee import Request, ConcurrencySettings
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee import Request
import logging
from crawlee.proxy_configuration import ProxyConfiguration
from datetime import datetime, timedelta, timezone

# 🔥 Глобальный MSK для ВСЕХ логгеров (включая Crawlee!)
msk_tz = timezone(timedelta(hours=3))


def msk_converter(timestamp, tz_name=None):
    return datetime.now(msk_tz).timetuple()


logging.Formatter.converter = msk_converter

from config import (
    INPUT_FILE,
    MAX_ROWS,
    ARMTEK_WORKERS,
    JPARTS_WORKERS,
    INPUT_COL_BRAND,
    INPUT_COL_ARTICLE,
    ENABLE_NAME_PARSING,
    ENABLE_WEIGHT_PARSING,
    ENABLE_PRICE_PARSING,
    AVTO_LOGIN,
    AVTO_PASSWORD,
    SELECTORS,
    get_output_file,
    reload_config,
    LOG_LEVEL,
    BATCH_SIZE,
    PROXY_COUNT,
    ARMTEK_PROXY,
    SEND_TO_TELEGRAM,
)
from utils import (
    setup_root_logging,
    logger,
    preprocess_dataframe,
    consolidate_weights,
    get_2captcha_proxy_pool,
    clear_debug_folders_sync,
)
from captcha_manager import CaptchaManager
from scraper_japarts_pure import parse_weight_japarts
from armtek_pure_aiohtpp import parse_weight_armtek, reset_armtek_session
from price_adjuster import adjust_prices_and_save


# UTF-8 setup
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
os.environ["PYTHONIOENCODING"] = "utf-8"

load_dotenv()


async def skip_navigation(context: PlaywrightCrawlingContext):
    if context.request.user_data.get("site") == "armtek":
        # ❗ полностью останавливаем navigation
        await context.page.route("**/*", lambda route: route.abort())


async def block_media_requests(context: PlaywrightCrawlingContext, *args) -> None:
    """
    Блокирует тяжелые медиа-файлы (картинки, шрифты, видео),
    но ПРОПУСКАЕТ всё, что связано с капчами из ваших селекторов.

    Аргументы:
    - context: контекст PlaywrightCrawler
    - *args: для совместимости с будущими версиями Crawlee
    """
    page = context.page

    # 🔥 Белый список ключевых слов в URL.
    # Если URL содержит любое из этих слов, ресурс БУДЕТ загружен.
    whitelist_keywords = [
        # === Общие и внешние капчи ===
        "captcha",  # Для stparts и общих случаев
        "recaptcha",  # Google ReCaptcha
        "grecaptcha",  # Google ReCaptcha API
        "hcaptcha",  # hCaptcha
        "turnstile",  # Cloudflare
        "challenge",  # Cloudflare / Protection
        "verify",  # Часто используется в URL проверок
        # === Avtoformula (из селектора img[src*="/_phplib/check/img.php"]) ===
        "img.php",
        "_phplib",
        # === Armtek (из селектора img[src*='blob']) ===
        "blob",
        # === Дополнительно (иконки иногда нужны для UI капчи) ===
        "svg",
        "icon",
    ]

    async def route_handler(route):
        req = route.request
        url = req.url.lower()
        resource_type = req.resource_type

        # 1. Если это тяжелый ресурс (картинка, медиа, шрифт)
        if resource_type in ("image", "media", "font"):

            # Проверяем, есть ли в URL "разрешенные" слова
            is_whitelisted = any(keyword in url for keyword in whitelist_keywords)

            if is_whitelisted:
                # ✅ Это капча или важный элемент -> ГРУЗИМ
                await route.continue_()
            else:
                # ❌ Это реклама, фото товара или баннер -> БЛОКИРУЕМ
                await route.abort()

        # 2. Скрипты, HTML, JSON (XHR/Fetch), CSS -> ВСЕГДА ГРУЗИМ
        # (CSS нужен, чтобы капча не "поехала" версткой, скрипты нужны для логики)
        else:
            await route.continue_()

    # Применяем фильтр ко всем запросам
    await page.route("**/*", route_handler)


# ===================== URL ГЕНЕРАТОРЫ =====================
class SiteUrls:
    """Централизованное хранилище URL для всех сайтов"""

    @staticmethod
    def japarts_search(part: str) -> str:
        return f"https://www.japarts.ru/?id=price&search={part}"

    @staticmethod
    def armtek_search(part: str) -> str:
        return f"https://armtek.ru/search?text={part}"

    @staticmethod
    def stparts_search(part: str) -> str:
        return f"https://stparts.ru/search?pcode={part}"

    @staticmethod
    def avtoformula_search(brand: str, part: str) -> str:
        # Avtoformula использует форму на главной, поэтому URL = главная страница
        # Поиск будет через заполнение формы в парсере
        return "https://www.avtoformula.ru"


# # # ===================== АВТОРИЗАЦИЯ С SESSION TRACKING =====================
class SimpleAuth:
    """Авторизация с отслеживанием сессий"""

    @staticmethod
    async def is_logged_in(page) -> bool:
        """Проверка авторизации"""
        try:
            return (
                await page.locator("span:has-text('Вы авторизованы как')").count() > 0
            )
        except:
            return False

    @staticmethod
    async def login_avtoformula(page, session_id: str) -> bool:
        """Авторизация на Avtoformula"""
        try:
            # Если уже залогинены - пропускаем
            if await SimpleAuth.is_logged_in(page):
                logger.debug(f"✅ Session {session_id}: уже авторизована")
                return True

            logger.info(f"🔐 Session {session_id}: авторизация Avtoformula...")

            # Переход на главную если нужно
            if page.url != "https://www.avtoformula.ru/":
                await page.goto("https://www.avtoformula.ru")

            # Логин
            await page.fill(f"#{SELECTORS['avtoformula']['login_field']}", AVTO_LOGIN)
            await page.fill(
                f"#{SELECTORS['avtoformula']['password_field']}", AVTO_PASSWORD
            )
            await page.click(SELECTORS["avtoformula"]["login_button"])

            # Ждём завершения
            await page.wait_for_selector(
                f"#{SELECTORS['avtoformula']['login_field']}",
                state="hidden",
                timeout=10000,
            )

            # Режим A0 (без аналогов)
            await page.select_option(
                f"#{SELECTORS['avtoformula']['smode_select']}", "A0"
            )

            logger.info(f"✅ Session {session_id}: авторизация успешна")
            return True

        except Exception as e:
            logger.error(f"❌ Session {session_id}: ошибка авторизации: {e}")
            return False


# ===================== ГЛАВНЫЙ КЛАСС =====================
class ParserCrawler:
    """Оптимизированный парсер на Crawlee"""

    def __init__(self):
        self.df = None
        self.mode = None
        self.captcha_manager = CaptchaManager()
        self.results_lock = asyncio.Lock()
        self.processed_count = 0
        self.total_tasks = 0

        self.jparts_crawler: PlaywrightCrawler | None = None
        self.proxy_crawler: PlaywrightCrawler | None = None

        # 🔥 ГЛОБАЛЬНАЯ ПАУЗА при RateLimit
        self.rate_limit_pause = False
        self.pause_event = asyncio.Event()
        self.pause_event.set()  # ✅ Разблокировано при старте
        self.pause_lock = asyncio.Lock()

        # 🔥 Трекинг авторизованных сессий
        self.authorized_sessions = set()
        self.session_lock = asyncio.Lock()

        # 🆕 Статистика по сайтам
        self.stats = {
            "japarts": {"total": 0, "success": 0, "empty": 0},
            "armtek": {"total": 0, "success": 0, "empty": 0},
        }

    async def setup(self):
        """Инициализация"""
        reload_config()

        # 🔥 Импортируем ПОСЛЕ reload_config()
        from config import (
            ENABLE_WEIGHT_PARSING,
            ENABLE_NAME_PARSING,
            ENABLE_PRICE_PARSING,
        )

        # Режим
        active = sum([ENABLE_WEIGHT_PARSING, ENABLE_NAME_PARSING, ENABLE_PRICE_PARSING])
        if active != 1:
            raise ValueError("❌ Только 1 режим!")

        self.mode = (
            "ВЕСА"
            if ENABLE_WEIGHT_PARSING
            else "ИМЕНА" if ENABLE_NAME_PARSING else "ЦЕНЫ"
        )

        logger.info(f"✅ Режим: {self.mode}")

        # Загрузка данных
        self.df = pd.read_excel(INPUT_FILE)
        self.df = preprocess_dataframe(self.df)
        self._init_columns()

        logger.info(f"📊 Загружено {len(self.df)} строк")
        self.total_tasks = len(self.df)
        logger.info(f"📊 Цель: {self.total_tasks} строк")

    def _init_columns(self):
        """Инициализация колонок"""
        from config import (
            stparts_price,
            stparts_delivery,
            avtoformula_price,
            avtoformula_delivery,
            JPARTS_P_W,
            JPARTS_V_W,
            ARMTEK_P_W,
            ARMTEK_V_W,
        )

        cols = [
            stparts_price,
            stparts_delivery,
            avtoformula_price,
            avtoformula_delivery,
        ]
        for col in cols:
            if col not in self.df.columns:
                self.df[col] = None

        if ENABLE_NAME_PARSING and "finde_name" not in self.df.columns:
            self.df["finde_name"] = None

        if ENABLE_WEIGHT_PARSING:
            for col in [JPARTS_P_W, JPARTS_V_W, ARMTEK_P_W, ARMTEK_V_W]:
                if col not in self.df.columns:
                    self.df[col] = None

    async def request_handler(self, context: PlaywrightCrawlingContext):
        """Обработчик Crawlee - ТОЛЬКО парсинг"""
        page = context.page
        request = context.request
        session = context.session

        # 🔥 ГЛОБАЛЬНАЯ ПАУЗА - ждём разблокировки
        if not self.pause_event.is_set():
            await self.pause_event.wait()

        # 🛑 Быстрый стоп
        if Path("input/STOP.flag").exists():
            logger.warning(
                "🛑 STOP.flag найден внутри request_handler → прерываем задачу"
            )
            return  # или raise Exception("STOP") если хочешь, чтобы Crawlee не ретраил

        idx = request.user_data["idx"]
        brand = request.user_data["brand"]
        part = request.user_data["part"]
        site = request.user_data["site"]  # 🔥 Объявляем здесь
        task_type = request.user_data["task_type"]

        session_id = session.id if session else "no-session"

        try:
            # 🔥 АВТОРИЗАЦИЯ ДЛЯ AVTOFORMULA
            if site == "avtoformula":
                async with self.session_lock:
                    if session_id not in self.authorized_sessions:
                        logger.debug(f"🔐 [{idx}] Session {session_id}: авторизация")
                        success = await SimpleAuth.login_avtoformula(page, session_id)

                        if success:
                            self.authorized_sessions.add(session_id)
                            logger.debug(f"✅ [{idx}] Session {session_id}: сохранена")
                        else:
                            raise Exception("Авторизация не удалась")

            # 🔥 ТОЛЬКО ПАРСИНГ
            result = await self._route_to_parser(
                page, idx, brand, part, site, task_type
            )

            if result:
                await self._save_result(idx, result)

        except Exception as e:
            logger.error(f"❌ [{idx}] {site}: {e}")
            raise

    async def _route_to_parser(self, page, idx, brand, part, site, task_type):
        """Роутинг к нужному парсеру"""

        # 🔥 ОБНОВЛЯЕМ СТАТИСТИКУ
        if site in self.stats:
            async with self.results_lock:
                self.stats[site]["total"] += 1

        # ======== ВЕСА ========
        if task_type == "weight":
            if site == "japarts":
                physical, volumetric = await parse_weight_japarts(page, part, logger)

                if physical == "NeedCaptcha":
                    if await self._solve_captcha(page, "japarts", numeric_only=False):
                        physical, volumetric = await parse_weight_japarts(
                            page, part, logger
                        )

                from config import JPARTS_P_W, JPARTS_V_W

                # 🆕 Логирование результата
                if physical or volumetric:
                    self.stats["japarts"]["success"] += 1
                    logger.info(f"[JAPARTS] ✅ {part} | P={physical} | V={volumetric}")
                else:
                    self.stats["japarts"]["empty"] += 1
                    logger.info(f"[JAPARTS] ⚠️ {part} | Не найдено")

                # 🆕 ДОБАВИТЬ лог ДО return:
                # logger.info(
                #     f"🔍 [{idx}] Japarts RESULT → {JPARTS_P_W}={physical}, {JPARTS_V_W}={volumetric}"
                # )

                return {JPARTS_P_W: physical, JPARTS_V_W: volumetric}

            if site == "armtek":
                max_retries = 2  # Локальные ретраи для transient ошибок

                for retry in range(max_retries + 1):
                    physical, volumetric = await parse_weight_armtek(page, part, logger)

                    # 🔥 PROXY РЕЖИМ: все блокировки → новый proxy
                    if ARMTEK_PROXY:
                        if physical in [
                            "CloudFlare",
                            "NeedProxy",
                            "PreloaderState",
                            "DeadPage",
                            "NeedCaptcha",
                        ]:
                            logger.warning(
                                f"🔄 [ARMTEK] Proxy retry: {physical} | {part}"
                            )
                            raise Exception(f"proxy block: {physical}")

                    # 🔥 NORMAL РЕЖИМ:
                    else:
                        if physical == "NeedCaptcha":
                            # if await self._solve_captcha(
                            #     page, "armtek", numeric_only=False
                            # ):
                            #     continue  # Перепарсим после капчи
                            # await asyncio.sleep(180)
                            # continue  # Пробуем еще раз после паузы

                            logger.warning(
                                f"⚠️ [ARMTEK] Block detected for {part}. Retrying later..."
                            )

                            await reset_armtek_session(page, logger)
                            raise Exception(
                                "RateLimit: Need pause"
                            )  # Crawlee сделает retry позже

                    # ✅ Если дошли сюда = данные готовы
                    break

                # Логи + return
                from config import ARMTEK_P_W, ARMTEK_V_W

                if physical or volumetric:
                    self.stats["armtek"]["success"] += 1
                    logger.info(f"[ARMTEK] ✅ {part} | P={physical} | V={volumetric}")
                else:
                    self.stats["armtek"]["empty"] += 1
                    logger.info(f"[ARMTEK] ⚠️ {part} | Не найдено")

                return {ARMTEK_P_W: physical, ARMTEK_V_W: volumetric}

        return None

    async def _solve_captcha(self, page, site_key, numeric_only):
        """Решение капчи"""
        logger.info(f"🔒 Капча {site_key}")

        success = await self.captcha_manager.solve_captcha(
            page=page,
            logger=logger,
            site_key=site_key,
            selectors=SELECTORS.get(site_key, {}),
        )

        logger.info(f"{'✅' if success else '❌'} Капча {site_key}")
        return success

    async def _save_result(self, idx, result):
        """Потокобезопасное сохранение"""
        async with self.results_lock:
            for col, val in result.items():
                if pd.notna(val):
                    self.df.at[idx, col] = val

    async def _failed_handler(self, context):
        """Логирует ошибки, если страница ВООБЩЕ не открылась"""
        req = context.request
        # Логируем красным цветом или ERROR
        logger.error(
            f"💀 FATAL FAIL [{req.user_data.get('site')}]: {req.url} | {context.error}"
        )

    async def run(self):
        """Главный метод запуска"""
        await self.setup()

        logger.info(
            f"DEBUG CONFIG: Weight={ENABLE_WEIGHT_PARSING}, Price={ENABLE_PRICE_PARSING}, Name={ENABLE_NAME_PARSING}"
        )
        logger.info(f"CURRENT MODE: {self.mode}")

        # proxy_list = await asyncio.to_thread(get_2captcha_proxy_pool, count=PROXY_COUNT)
        proxy_list = []

        self.proxy_crawler = None
        logger.info("🌐 Загрузка прокси для Armtek...")
        if proxy_list:
            # self.proxy_crawler = PlaywrightCrawler(
            #     request_handler=self.request_handler,
            #     proxy_configuration=ProxyConfiguration(proxy_urls=proxy_list),
            #     use_session_pool=False,
            #     max_request_retries=3,
            #     concurrency_settings=ConcurrencySettings(
            #         max_concurrency=ARMTEK_WORKERS,
            #         desired_concurrency=ARMTEK_WORKERS,
            #         min_concurrency=2,
            #     ),
            #     browser_new_context_options={
            #         "ignore_https_errors": True,
            #         "args": [
            #             "--no-sandbox",
            #             "--disable-setuid-sandbox",
            #             "--disable-dev-shm-usage",
            #             "--disable-accelerated-2d-canvas",
            #             "--no-first-run",
            #             "--no-zygote",
            #             "--disable-gpu",
            #         ],
            #     },
            #     headless=True,
            # )
            self.proxy_crawler = None
            logger.info("🚫 Proxy отключены (работаем напрямую)")
            # 2. Добавляем хук ПОСЛЕ создания
            self.proxy_crawler._pre_navigation_hooks.append(block_media_requests)
            # proxy_crawler = None  # ← ДОБАВИТЬ!
            # logger.info(f"✅ Proxy отключены армтек на нормально мпрокси)")
            logger.info(f"✅ Proxy crawler создан ({len(proxy_list)} прокси)")
        else:
            logger.warning("⚠️ Прокси не получены → Armtek БЕЗ прокси")

        # Normal crawler (БЕЗ прокси)
        self.jparts_crawler = PlaywrightCrawler(
            request_handler=self.request_handler,
            max_request_retries=3,
            use_session_pool=True,
            request_handler_timeout=timedelta(
                seconds=90
            ),  # ✅ Сохранение сессии для Avtoformula
            session_pool=SessionPool(
                create_session_settings={
                    "blocked_status_codes": [403, 407],  # без 429
                }
            ),
            concurrency_settings=ConcurrencySettings(
                max_concurrency=JPARTS_WORKERS,
                desired_concurrency=JPARTS_WORKERS,
                min_concurrency=2,
            ),
            browser_new_context_options={
                "ignore_https_errors": True,
                "args": [
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-accelerated-2d-canvas",
                    "--no-first-run",
                    "--no-zygote",
                    "--disable-gpu",
                ],
            },
            headless=True,
        )

        # 2. Добавляем хук ПОСЛЕ создания
        from crawlee.browsers import BrowserPool, PlaywrightBrowserPlugin

        armtek_browser_pool = BrowserPool(
            plugins=[
                PlaywrightBrowserPlugin(
                    max_open_pages_per_browser=5,
                    browser_launch_options={
                        "headless": True,
                        "args": [
                            "--no-sandbox",
                            "--disable-setuid-sandbox",
                            "--disable-dev-shm-usage",
                            "--disable-gpu",
                            "--no-zygote",
                            "--disable-extensions",
                            # "--single-process",
                        ],
                    },
                    browser_new_context_options={
                        "ignore_https_errors": True,
                    },
                )
            ]
        )

        # Normal crawler (БЕЗ прокси)
        self.armtek_crawler = PlaywrightCrawler(
            request_handler=self.request_handler,
            max_request_retries=3,
            request_handler_timeout=timedelta(seconds=30),
            use_session_pool=True,
            session_pool=SessionPool(
                create_session_settings={
                    "blocked_status_codes": [403, 407],
                }
            ),
            concurrency_settings=ConcurrencySettings(
                max_concurrency=ARMTEK_WORKERS,
                desired_concurrency=ARMTEK_WORKERS,
                min_concurrency=2,
            ),
            browser_pool=armtek_browser_pool,
            ignore_http_error_status_codes=[400, 401, 403, 404, 429],
        )

        # 2. Добавляем хук ПОСЛЕ создания
        self.armtek_crawler._pre_navigation_hooks.append(block_media_requests)

        # Proxy crawler (только для Armtek в режиме ВЕСОВ)

        # 🔥 БАТЧ-ОБРАБОТКА
        BATCH_SIZE
        total_rows = min(len(self.df), MAX_ROWS)
        stop_flag = Path("input/STOP.flag")

        for batch_start in range(0, total_rows, BATCH_SIZE):
            # 🛑 Проверка стоп-флага перед каждым батчем
            if stop_flag.exists():
                logger.warning(
                    "🛑 STOP.flag найден, останавливаем парсер после текущего состояния"
                )
                break

            batch_end = min(batch_start + BATCH_SIZE, total_rows)
            batch_num = batch_start // BATCH_SIZE + 1

            logger.info(f"📦 БАТЧ #{batch_num}: строки {batch_start}-{batch_end}")

            current_mode = self.mode.lower().strip()

            # 🔥 ТЕПЕРЬ МЫ ВЫБИРАЕМ СТРОГО ПО CURRENT MODE
            if current_mode in ["веса", "weight"]:
                await self._process_weight_batch(batch_start, batch_end, batch_num)
            elif current_mode in ["имена", "name"]:
                await self._process_name_batch(batch_start, batch_end, batch_num)
            elif current_mode in ["цены", "price"]:
                await self._process_price_batch(batch_start, batch_end, batch_num)
            else:
                logger.error(f"❌ РЕЖИМ НЕ ОПРЕДЕЛЕН: {self.mode}")

            # 💾 ПРОМЕЖУТОЧНОЕ СОХРАНЕНИЕ
            output_file = get_output_file(self.mode)
            await asyncio.to_thread(self.df.to_excel, output_file, index=False)
            rows_processed = batch_end
            logger.info(f"💾 Батч #{batch_num} сохранён ({batch_end} строк)")

            # После сохранения сырых данных
            await self.finalize_saved_file(
                output_file, batch_num
            )  # output_file → input_file

        logger.info(f"📊 Всего обработано: {self.processed_count} строк")
        if ENABLE_NAME_PARSING or ENABLE_PRICE_PARSING:
            logger.info(
                f"📊 Всего сессий авторизовано: {len(self.authorized_sessions)}"
            )

        await self._finalize()

    async def _finalize(self):
        """Финальная обработка"""
        logger.info(f"🔄 Финализация ({self.mode})...")

        if ENABLE_WEIGHT_PARSING:
            self.df = await asyncio.to_thread(consolidate_weights, self.df)
            logger.info("✅ Веса консолидированы")

        output_file = get_output_file(self.mode)

        if ENABLE_PRICE_PARSING:
            await asyncio.to_thread(adjust_prices_and_save, self.df, output_file)
        else:
            await asyncio.to_thread(self.df.to_excel, output_file, index=False)

        logger.info(f"✅ Сохранено: {output_file}")
        logger.info(f"📊 Обработано: {self.processed_count}/{self.total_tasks}")

    async def finalize_saved_file(self, input_file: str, batch_num: int):
        """Асинхронно финализирует уже сохранённый файл"""

        logger.info(f"🔄 Финализация batch_finalize.xlsx (батч #{batch_num})...")

        # Загружаем сохранённый файл
        df_final = pd.read_excel(input_file)

        if ENABLE_WEIGHT_PARSING:
            df_final = await asyncio.to_thread(consolidate_weights, df_final)
            logger.info("✅ Веса консолидированы")

        # 🆕 ОДИН файл для финализированных батчей
        batch_final_file = "output/batch_finalize.xlsx"

        if ENABLE_PRICE_PARSING:
            await asyncio.to_thread(adjust_prices_and_save, df_final, batch_final_file)
        else:
            await asyncio.to_thread(df_final.to_excel, batch_final_file, index=False)

        logger.info(f"💾 batch_finalize.xlsx готов ({len(df_final)} строк)")

    async def _process_weight_batch(self, batch_start, batch_end, batch_num):
        """
        Двухстадийная обработка:
        1. Japarts для всех деталей батча.
        2. Armtek только для тех, что не найдены на Japarts.
        """
        from config import JPARTS_P_W

        # --- СТАДИЯ 1: JAPARTS ---
        jparts_requests = []
        for idx in range(batch_start, batch_end):
            row = self.df.iloc[idx]
            article = str(row[INPUT_COL_ARTICLE]).strip()
            brand = str(row[INPUT_COL_BRAND]).strip()
            if not article:
                continue

            jparts_requests.append(
                Request.from_url(
                    url=SiteUrls.japarts_search(article),
                    user_data={
                        "idx": idx,
                        "brand": brand,
                        "part": article,
                        "site": "japarts",
                        "task_type": "weight",
                    },
                )
            )

        if jparts_requests:
            logger.info(f"🚀 Стадия 1: Japarts ({len(jparts_requests)} задач)")
            await self.jparts_crawler.run(jparts_requests)

        # --- СТАДИЯ 2: ARMTEK (FALLBACK) ---
        armtek_fallback_requests = []

        # Перепроверяем DataFrame после работы Japarts
        async with self.results_lock:
            for idx in range(batch_start, batch_end):
                # Проверяем, записался ли физический вес от Japarts
                if pd.isna(self.df.at[idx, JPARTS_P_W]):
                    row = self.df.iloc[idx]
                    article = str(row[INPUT_COL_ARTICLE]).strip()
                    brand = str(row[INPUT_COL_BRAND]).strip()

                    if article:
                        armtek_fallback_requests.append(
                            Request.from_url(
                                # url=SiteUrls.armtek_search(article),
                                url="https://httpbin.org/status/200",
                                user_data={
                                    "idx": idx,
                                    "brand": brand,
                                    "part": article,
                                    "site": "armtek",
                                    "task_type": "weight",
                                },
                                unique_key=f"armtek_{batch_num}_{idx}",
                            )
                        )

        if armtek_fallback_requests:
            logger.info(
                f"🚀 Стадия 2: Armtek Fallback ({len(armtek_fallback_requests)} задач)"
            )
            # Используем второй краулер (или тот же, если он свободен)
            await self.armtek_crawler.run(armtek_fallback_requests)
        else:
            logger.info("✅ Все веса найдены на Japarts, Armtek не требуется.")

    async def _trigger_global_pause(self):
        """Глобальная пауза ВСЕГО краулера на 10 минут"""
        logger.critical("⏸️  ГЛОБАЛЬНАЯ ПАУЗА 10 МИНУТ (RateLimit)")

        # 🔥 Сохраняем текущий прогресс
        temp_file = f"output/temp_progress_{int(asyncio.get_event_loop().time())}.xlsx"
        await asyncio.to_thread(self.df.to_excel, temp_file, index=False)
        logger.info(f"💾 Прогресс сохранён: {temp_file}")

        # Ждём 10 минут
        await asyncio.sleep(600)  # 10 минут

        logger.critical("▶️  ВОЗОБНОВЛЕНИЕ после RateLimit")

        # Разблокируем все воркеры
        self.pause_event.set()


async def main():
    logger.setLevel(getattr(logging, LOG_LEVEL.upper()))
    clear_debug_folders_sync(logger)
    reload_config()
    logger.info("🚀 START: Config reloaded!")  # Дебаг
    parser = ParserCrawler()
    logger.debug("🔍 Детальная информация (видна только при LOG_LEVEL=DEBUG)")
    logger.info("🔍  информация (видна только при LOG_LEVEL=INFO)")
    await parser.run()


if __name__ == "__main__":
    setup_root_logging()
    asyncio.run(main())

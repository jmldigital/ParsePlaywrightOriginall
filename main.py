"""
Парсер на Crawlee - полностью оптимизированная версия
- Авторизация через Crawlee session persistence
- URL генерация вынесена из скрейперов
- Скрейперы делают только парсинг DOM
"""

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

# ✅ ПРАВИЛЬНО:
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee import Request
import logging
from crawlee.proxy_configuration import ProxyConfiguration

# UTF-8 setup
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
os.environ["PYTHONIOENCODING"] = "utf-8"

load_dotenv()


async def block_media_requests(context: PlaywrightCrawlingContext) -> None:
    """Блокирует изображения, шрифты и медиафайлы"""
    await context.page.route(
        "**/*",
        lambda route: (
            route.abort()
            if route.request.resource_type in ("image", "media", "font")
            else route.continue_()
        ),
    )


from config import (
    INPUT_FILE,
    MAX_ROWS,
    MAX_WORKERS,
    INPUT_COL_BRAND,
    INPUT_COL_ARTICLE,
    ENABLE_NAME_PARSING,
    ENABLE_WEIGHT_PARSING,
    ENABLE_PRICE_PARSING,
    AVTO_LOGIN,
    AVTO_PASSWORD,
    BAD_DETAIL_NAMES,
    SELECTORS,
    get_output_file,
    reload_config,
    TEMP_RAW,
    LOG_LEVEL,
    BATCH_SIZE,
    PROXY_COUNT,
    MAX_WORKERS_PROXY,
    ARMTEK_PROXY,
    STPARTS_PROXY,
    NOTIFY_PROGRESS,
)
from utils import (
    logger,
    preprocess_dataframe,
    consolidate_weights,
    get_2captcha_proxy_pool,
    clear_debug_folders_sync,
)
from captcha_manager import CaptchaManager

# Импорт ТОЛЬКО парсеров (без навигации)
from scraper_japarts_pure import parse_weight_japarts
from scraper_armtek_pure import parse_weight_armtek
from scraper_stparts_pure import parse_stparts_name, parse_stparts_price
from scraper_avtoformula_pure import parse_avtoformula_name, parse_avtoformula_price
from price_adjuster import adjust_prices_and_save


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


# # # ===================== УПРОЩЕННАЯ АВТОРИЗАЦИЯ =====================
# class SimpleAuth:
#     """Упрощенная авторизация через Crawlee session"""

#     @staticmethod
#     async def login_avtoformula(page) -> bool:
#         """Минимальная логика логина - Crawlee сам сохранит сессию"""
#         try:
#             await page.goto("https://www.avtoformula.ru")

#             # Проверка: уже залогинены?
#             if await page.locator("span:has-text('Вы авторизованы как')").count() > 0:
#                 logger.info("✅ Уже авторизованы")
#                 return True

#             # Логин
#             await page.fill(f"#{SELECTORS['avtoformula']['login_field']}", AVTO_LOGIN)
#             await page.fill(
#                 f"#{SELECTORS['avtoformula']['password_field']}", AVTO_PASSWORD
#             )
#             await page.click(SELECTORS["avtoformula"]["login_button"])

#             # Ждём завершения
#             await page.wait_for_selector(
#                 f"#{SELECTORS['avtoformula']['login_field']}",
#                 state="hidden",
#                 timeout=10000,
#             )

#             # Режим A0 (без аналогов)
#             await page.select_option(
#                 f"#{SELECTORS['avtoformula']['smode_select']}", "A0"
#             )

#             logger.info("✅ Авторизация успешна")
#             return True

#         except Exception as e:
#             logger.error(f"❌ Ошибка авторизации: {e}")
#             return False


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

        self.telegram_chat_id = os.getenv("ADMIN_CHAT_ID")
        self.telegram_bot_token = os.getenv("BOT_TOKEN")

        self.normal_crawler: PlaywrightCrawler | None = None
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

            # 🆕 ПРОВЕРКА IP (первые 3 запроса)
            if not hasattr(self, "_ip_check_count"):
                self._ip_check_count = 0

            if self._ip_check_count < 3:
                try:
                    actual_ip = await page.evaluate(
                        "() => fetch('https://api.ipify.org?format=json', {timeout: 5000}).then(r => r.json()).then(d => d.ip).catch(() => 'N/A')"
                    )
                    # logger.debug(f"🌍 [{idx}] IP: {actual_ip}")
                    self._ip_check_count += 1
                except:
                    pass

            # 🔥 ТОЛЬКО ПАРСИНГ
            result = await self._route_to_parser(
                page, idx, brand, part, site, task_type
            )

            if result:
                await self._save_result(idx, result)

            # 🔥 ПРОГРЕСС (обновляется после каждого запроса)
            async with self.results_lock:
                self.processed_count += 1

                # Лог + Telegram каждые NOTIFY_PROGRESS задач
                if self.processed_count % NOTIFY_PROGRESS == 0:
                    logger.info(
                        f"📊 Прогресс: {self.processed_count}/{self.total_tasks}"
                    )
                    await self._send_telegram_notification(self.processed_count)

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
            # if site == "japarts":
            #     physical, volumetric = await parse_weight_japarts(page, part, logger)

            #     if physical == "NeedCaptcha":
            #         if await self._solve_captcha(page, "japarts", numeric_only=False):
            #             physical, volumetric = await parse_weight_japarts(
            #                 page, part, logger
            #             )

            #     from config import JPARTS_P_W, JPARTS_V_W

            #     # 🆕 Логирование результата
            #     if physical or volumetric:
            #         self.stats["japarts"]["success"] += 1
            #         logger.info(f"[JAPARTS] ✅ {part} | P={physical} | V={volumetric}")
            #     else:
            #         self.stats["japarts"]["empty"] += 1
            #         logger.info(f"[JAPARTS] ⚠️ {part} | Не найдено")

            #     # 🆕 ДОБАВИТЬ лог ДО return:
            #     # logger.info(
            #     #     f"🔍 [{idx}] Japarts RESULT → {JPARTS_P_W}={physical}, {JPARTS_V_W}={volumetric}"
            #     # )

            #     return {JPARTS_P_W: physical, JPARTS_V_W: volumetric}

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
                            if await self._solve_captcha(
                                page, "armtek", numeric_only=False
                            ):
                                continue  # Перепарсим после капчи

                        elif physical in ["PreloaderState", "DeadPage"]:
                            logger.warning(
                                f"🔄 [ARMTEK] Transient retry {retry+1}/{max_retries}: {physical}"
                            )
                            if retry < max_retries:
                                await asyncio.sleep(2**retry)  # 2s, 4s backoff
                                continue
                            # Если ретраи исчерпаны → как CloudFlare (пауза)

                        elif physical in ["CloudFlare", "NeedProxy"]:
                            # Global pause 10мин
                            logger.warning(f"⏸️ [ARMTEK] Rate limit: {physical}")
                            async with self.pause_lock:
                                if not self.rate_limit_pause:
                                    self.rate_limit_pause = True
                                    await self._trigger_global_pause()
                            await self.pause_event.wait()
                            self.rate_limit_pause = False
                            continue  # Перепарсим после паузы

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

        # ======== ИМЕНА ========
        elif task_type == "name":
            if site == "stparts":
                name = await parse_stparts_name(page, part, logger)

                if name == "NeedCaptcha":
                    if STPARTS_PROXY:
                        # Proxy: быстрый retry на новом IP
                        raise Exception("NeedCaptcha: proxy rotate")
                    else:
                        # Normal: решаем капчу
                        if await self._solve_captcha(
                            page, "stparts", numeric_only=True
                        ):
                            name = await parse_stparts_name(page, part, logger)
                        # Если solve FAIL → name остается "NeedCaptcha" → ниже None

                # ✅ ЕДИНЫЙ return в конце (все проверки)
                return (
                    {"finde_name": name}
                    if name and name not in BAD_DETAIL_NAMES
                    else None
                )

            elif site == "avtoformula":
                name = await parse_avtoformula_name(page, part, logger)

                if name == "NeedCaptcha":
                    if await self._solve_captcha(
                        page, "avtoformula", numeric_only=False
                    ):
                        name = await parse_avtoformula_name(page, part, logger)

                return {
                    "finde_name": (
                        name if name and name not in BAD_DETAIL_NAMES else "Detail"
                    )
                }

        # ======== ЦЕНЫ ========
        elif task_type == "price":
            from config import (
                stparts_price,
                stparts_delivery,
                avtoformula_price,
                avtoformula_delivery,
            )

            # if site == "stparts":
            #     price, delivery = await parse_stparts_price(page, brand, part, logger)

            #     if price == "NeedCaptcha":
            #         if STPARTS_PROXY:
            #             # Proxy: быстрый retry на новом IP
            #             raise Exception("NeedCaptcha: proxy rotate")
            #         else:
            #             # Normal: решаем капчу (numeric)
            #             if await self._solve_captcha(
            #                 page, "stparts", numeric_only=True
            #             ):
            #                 price, delivery = await parse_stparts_price(
            #                     page, brand, part, logger
            #                 )

            #     return {stparts_price: price, stparts_delivery: delivery}

            if site == "avtoformula":
                price, delivery = await parse_avtoformula_price(
                    page, brand, part, logger
                )

                if price == "NeedCaptcha":
                    if await self._solve_captcha(
                        page, "avtoformula", numeric_only=False
                    ):
                        price, delivery = await parse_avtoformula_price(
                            page, brand, part, logger
                        )

                return {avtoformula_price: price, avtoformula_delivery: delivery}

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

            # 🔥 ПРОМЕЖУТОЧНОЕ СОХРАНЕНИЕ каждые 100 строк
            if (self.processed_count + 1) % TEMP_RAW == 0:
                temp_file = f"output/temp_progress.xlsx"
                await asyncio.to_thread(self.df.to_excel, temp_file, index=False)
                logger.info(f"💾 Промежуточное сохранение {self.processed_count} строк")

    async def _send_telegram_notification(self, processed_count: int):
        """Отправка уведомления в Telegram"""
        if not self.telegram_chat_id or not self.telegram_bot_token:
            return

        try:
            message = f"📊 Парсер: обработано {processed_count}/{self.total_tasks} строк ({self.mode})"
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"

            async with aiohttp.ClientSession() as session:
                await session.post(
                    url, json={"chat_id": self.telegram_chat_id, "text": message}
                )
            logger.info(f"📱 Telegram: {message}")
        except Exception as e:
            logger.error(f"❌ Telegram error: {e}")

    async def run(self):
        """Главный метод запуска"""
        await self.setup()

        # 🔥 СОЗДАЁМ CRAWLERS ОДИН РАЗ
        WORKERS = MAX_WORKERS

        if ENABLE_WEIGHT_PARSING:
            WORKERS = MAX_WORKERS_PROXY
        else:
            WORKERS = MAX_WORKERS

        proxy_list = await asyncio.to_thread(get_2captcha_proxy_pool, count=PROXY_COUNT)
        self.proxy_crawler = None
        logger.info("🌐 Загрузка прокси для Armtek...")
        if proxy_list:
            self.proxy_crawler = PlaywrightCrawler(
                request_handler=self.request_handler,
                proxy_configuration=ProxyConfiguration(proxy_urls=proxy_list),
                use_session_pool=False,
                max_request_retries=3,
                concurrency_settings=ConcurrencySettings(
                    max_concurrency=WORKERS,
                    desired_concurrency=WORKERS,
                    min_concurrency=2,
                ),
                browser_new_context_options={"ignore_https_errors": True},
                headless=True,
            )
            # 2. Добавляем хук ПОСЛЕ создания
            self.proxy_crawler._pre_navigation_hooks.append(block_media_requests)
            # proxy_crawler = None  # ← ДОБАВИТЬ!
            # logger.info(f"✅ Proxy отключены армтек на нормально мпрокси)")
            logger.info(f"✅ Proxy crawler создан ({len(proxy_list)} прокси)")
        else:
            logger.warning("⚠️ Прокси не получены → Armtek БЕЗ прокси")

        # Normal crawler (БЕЗ прокси)
        self.normal_crawler = PlaywrightCrawler(
            request_handler=self.request_handler,
            max_request_retries=3,
            use_session_pool=True,  # ✅ Сохранение сессии для Avtoformula
            concurrency_settings=ConcurrencySettings(
                max_concurrency=WORKERS,
                desired_concurrency=WORKERS,
                min_concurrency=2,
            ),
            headless=True,
        )

        # 2. Добавляем хук ПОСЛЕ создания
        self.normal_crawler._pre_navigation_hooks.append(block_media_requests)

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

            # 🔥 ВЫБОР МЕТОДА ПО РЕЖИМУ
            if ENABLE_WEIGHT_PARSING:
                await self._process_weight_batch(batch_start, batch_end, batch_num)
            elif ENABLE_NAME_PARSING:
                await self._process_name_batch(batch_start, batch_end, batch_num)
            elif ENABLE_PRICE_PARSING:
                await self._process_price_batch(batch_start, batch_end, batch_num)

            # 💾 ПРОМЕЖУТОЧНОЕ СОХРАНЕНИЕ
            output_file = get_output_file(self.mode)
            await asyncio.to_thread(self.df.to_excel, output_file, index=False)
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
        """Обработка батча для ВЕСОВ: Japarts (обычный) → Armtek (прокси)"""

        # 1️⃣ JAPARTS (без прокси)
        japarts_requests = []
        for idx in range(batch_start, batch_end):
            row = self.df.iloc[idx]
            article = str(row[INPUT_COL_ARTICLE]).strip()

            if not article:
                continue

            brand = str(row[INPUT_COL_BRAND]).strip()

            japarts_requests.append(
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

        if japarts_requests:
            logger.info(f"  🚀 Japarts (normal): {len(japarts_requests)} задач")
            await self.normal_crawler.run(japarts_requests)

        # 2️⃣ ARMTEK FALLBACK (С ПРОКСИ, только если физический вес НЕ найден)
        from config import JPARTS_P_W

        armtek_fallback = []
        for idx in range(batch_start, batch_end):
            row = self.df.iloc[idx]

            # Проверяем: есть ли физический вес с Japarts?
            if pd.isna(row.get(JPARTS_P_W)):
                article = str(row[INPUT_COL_ARTICLE]).strip()
                brand = str(row[INPUT_COL_BRAND]).strip()

                if article:
                    armtek_fallback.append(
                        Request.from_url(
                            url=SiteUrls.armtek_search(article),
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

        if armtek_fallback:
            if ARMTEK_PROXY and self.proxy_crawler:
                logger.info(f"🚀 Armtek (proxy): {len(armtek_fallback)} fallback")
                await self.proxy_crawler.run(armtek_fallback)
            else:
                # Normal режим (или proxy недоступен)
                proxy_status = "proxy недоступен" if ARMTEK_PROXY else "normal"
                logger.info(
                    f"🚀 Armtek ({proxy_status}): {len(armtek_fallback)} fallback"
                )
                await self.normal_crawler.run(armtek_fallback)
        else:
            logger.info(f"  ✅ Все физ. веса найдены на Japarts")

    async def _process_name_batch(self, batch_start, batch_end, batch_num):
        """Обработка батча для ИМЁН: Stparts → Avtoformula fallback"""

        # 1️⃣ STPARTS (приоритет)
        stparts_requests = []
        for idx in range(batch_start, batch_end):
            row = self.df.iloc[idx]
            article = str(row[INPUT_COL_ARTICLE]).strip()

            if not article:
                continue

            brand = str(row[INPUT_COL_BRAND]).strip()

            stparts_requests.append(
                Request.from_url(
                    url=SiteUrls.stparts_search(article),
                    user_data={
                        "idx": idx,
                        "brand": brand,
                        "part": article,
                        "site": "stparts",
                        "task_type": "name",
                    },
                )
            )

        if stparts_requests:
            if STPARTS_PROXY and self.proxy_crawler:
                # Proxy режим
                logger.info(f"🚀 Stparts (proxy): {len(stparts_requests)} задач")
                await self.proxy_crawler.run(stparts_requests)
            else:
                # Normal режим (или proxy недоступен)
                status = "proxy недоступен" if STPARTS_PROXY else "normal"
                logger.info(f"🚀 Stparts ({status}): {len(stparts_requests)} задач")
                await self.normal_crawler.run(stparts_requests)

        # 2️⃣ AVTOFORMULA FALLBACK
        avtoformula_fallback = []
        for idx in range(batch_start, batch_end):
            row = self.df.iloc[idx]

            if (
                pd.isna(row.get("finde_name"))
                or row.get("finde_name") in BAD_DETAIL_NAMES
            ):
                article = str(row[INPUT_COL_ARTICLE]).strip()
                brand = str(row[INPUT_COL_BRAND]).strip()

                if article:
                    avtoformula_fallback.append(
                        Request.from_url(
                            url=SiteUrls.avtoformula_search(brand, article),
                            user_data={
                                "idx": idx,
                                "brand": brand,
                                "part": article,
                                "site": "avtoformula",
                                "task_type": "name",
                            },
                            unique_key=f"avtoformula_{batch_num}_{idx}",
                        )
                    )

        if avtoformula_fallback:
            logger.info(
                f"  🚀 Avtoformula fallback: {len(avtoformula_fallback)} пустых"
            )
            await self.normal_crawler.run(avtoformula_fallback)
        else:
            logger.info(f"  ✅ Все имена найдены на Stparts")

    async def _process_price_batch(self, batch_start, batch_end, batch_num):
        """Обработка батча для ЦЕН: Stparts + Avtoformula ПАРАЛЛЕЛЬНО"""

        stparts_requests = []
        avto_requests = []

        for idx in range(batch_start, batch_end):
            row = self.df.iloc[idx]
            article = str(row[INPUT_COL_ARTICLE]).strip()

            if not article:
                continue

            brand = str(row[INPUT_COL_BRAND]).strip()

            # Stparts
            stparts_requests.append(
                Request.from_url(
                    url=SiteUrls.stparts_search(article),
                    user_data={
                        "idx": idx,
                        "brand": brand,
                        "part": article,
                        "site": "stparts",
                        "task_type": "price",
                    },
                )
            )

            # # Avtoformula
            avto_requests.append(
                Request.from_url(
                    url=SiteUrls.avtoformula_search(brand, article),
                    user_data={
                        "idx": idx,
                        "brand": brand,
                        "part": article,
                        "site": "avtoformula",
                        "task_type": "price",
                    },
                    unique_key=f"avtoformula_price_{batch_num}_{idx}",
                )
            )

        if stparts_requests:
            if STPARTS_PROXY and self.proxy_crawler:
                # Proxy режим
                logger.info(f"🚀 Stparts (proxy): {len(stparts_requests)} задач")
                await self.proxy_crawler.run(stparts_requests)
            else:
                # Normal режим (или proxy недоступен)
                status = "proxy недоступен" if STPARTS_PROXY else "normal"
                logger.info(f"🚀 Stparts ({status}): {len(stparts_requests)} задач")
                await self.normal_crawler.run(stparts_requests)

        if avto_requests:
            logger.info(f"  🚀 Avtoformula: {len(avto_requests)} задач")
            await self.normal_crawler.run(avto_requests)

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
    asyncio.run(main())

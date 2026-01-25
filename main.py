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

import asyncio
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from crawlee import Request, ConcurrencySettings

# ✅ ПРАВИЛЬНО:
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee import Request

# UTF-8 setup
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
os.environ["PYTHONIOENCODING"] = "utf-8"

load_dotenv()

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
)
from utils import logger, preprocess_dataframe, consolidate_weights
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
        return f"https://stparts.ru/search/?text={part}"

    @staticmethod
    def avtoformula_search(brand: str, part: str) -> str:
        # Avtoformula использует форму на главной, поэтому URL = главная страница
        # Поиск будет через заполнение формы в парсере
        return "https://www.avtoformula.ru"


# ===================== УПРОЩЕННАЯ АВТОРИЗАЦИЯ =====================
class SimpleAuth:
    """Упрощенная авторизация через Crawlee session"""

    @staticmethod
    async def login_avtoformula(page) -> bool:
        """Минимальная логика логина - Crawlee сам сохранит сессию"""
        try:
            await page.goto("https://www.avtoformula.ru")

            # Проверка: уже залогинены?
            if await page.locator("span:has-text('Вы авторизованы как')").count() > 0:
                logger.info("✅ Уже авторизованы")
                return True

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

            logger.info("✅ Авторизация успешна")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка авторизации: {e}")
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
        """
        Обработчик Crawlee - вызывается для каждого Request
        Получает УЖЕ открытую страницу с нужным URL
        """
        page = context.page
        request = context.request

        idx = request.user_data["idx"]
        brand = request.user_data["brand"]
        part = request.user_data["part"]
        site = request.user_data["site"]
        task_type = request.user_data["task_type"]  # "weight"/"name"/"price"

        # 🔥 АВТОРИЗАЦИЯ (если Avtoformula)
        if site == "avtoformula" and not hasattr(self, "_avtoformula_logged_in"):
            logger.info("🔐 Авторизация Avtoformula...")
            success = await SimpleAuth.login_avtoformula(page)
            if success:
                self._avtoformula_logged_in = True
            else:
                raise Exception("Авторизация не удалась")

        # logger.info(f"🔍 [{idx}] {site}: {part}")

        try:
            # Выбор парсера в зависимости от сайта и задачи
            result = await self._route_to_parser(
                page, idx, brand, part, site, task_type
            )

            if result:
                await self._save_result(idx, result)

            # Прогресс
            async with self.results_lock:
                self.processed_count += 1
                if self.processed_count % TEMP_RAW / 2 == 0:
                    logger.info(f"📊 {self.processed_count}/{self.total_tasks}")

        except Exception as e:
            logger.error(f"❌ [{idx}] {site}: {e}")
            # Crawlee автоматически повторит

    async def _route_to_parser(self, page, idx, brand, part, site, task_type):
        """Роутинг к нужному парсеру"""

        # ======== ВЕСА ========
        if task_type == "weight":
            if site == "japarts":
                physical, volumetric = await parse_weight_japarts(page, part, logger)

                if physical == "NeedCaptcha":
                    if await self._solve_captcha(page, "japarts"):
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

            elif site == "armtek":
                physical, volumetric = await parse_weight_armtek(page, part, logger)

                # 🔥 RateLimit обработка
                if physical == "NeedProxy":
                    logger.warning(f"🚦 [{idx}] RateLimit на Armtek → прокси retry")
                    return await self._retry_with_proxy(
                        idx, brand, part, site, task_type
                    )

                if physical == "NeedCaptcha":
                    if await self._solve_captcha(page, "armtek"):
                        physical, volumetric = await parse_weight_armtek(
                            page, part, logger
                        )

                # 🔥 1. CLOUDFLARE - сбросить прокси, retry без прокси
                if physical == "CloudFlare":
                    logger.warning(f"☁️ [{idx}] CloudFlare на Armtek → retry без прокси")

                    # Перезагрузка страницы (Crawlee уже без прокси)
                    try:
                        await page.reload(wait_until="domcontentloaded", timeout=30000)
                        await page.wait_for_timeout(3000)  # Ждём CloudFlare check

                        # Повторный парсинг
                        physical, volumetric = await parse_weight_armtek(
                            page, part, logger
                        )

                        # Если снова CloudFlare - пропускаем
                        if physical == "CloudFlare":
                            logger.error(f"☁️ [{idx}] CloudFlare персистентен → пропуск")
                            self.stats["armtek"]["empty"] += 1
                            from config import ARMTEK_P_W, ARMTEK_V_W

                            return {ARMTEK_P_W: None, ARMTEK_V_W: None}

                    except Exception as e:
                        logger.error(f"❌ [{idx}] Ошибка retry CloudFlare: {e}")
                        self.stats["armtek"]["empty"] += 1
                        from config import ARMTEK_P_W, ARMTEK_V_W

                        return {ARMTEK_P_W: None, ARMTEK_V_W: None}

                from config import ARMTEK_P_W, ARMTEK_V_W

                # 🆕 Логирование результата
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
                    if await self._solve_captcha(page, "stparts"):
                        name = await parse_stparts_name(page, part, logger)

                return (
                    {"finde_name": name}
                    if name and name not in BAD_DETAIL_NAMES
                    else None
                )

            elif site == "avtoformula":
                name = await parse_avtoformula_name(page, part, logger)

                if name == "NeedCaptcha":
                    if await self._solve_captcha(page, "avtoformula"):
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

            if site == "stparts":
                price, delivery = await parse_stparts_price(page, brand, part, logger)
                return {stparts_price: price, stparts_delivery: delivery}

            elif site == "avtoformula":
                price, delivery = await parse_avtoformula_price(
                    page, brand, part, logger
                )
                return {avtoformula_price: price, avtoformula_delivery: delivery}

        return None

    async def _solve_captcha(self, page, site_key):
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

    # async def _retry_with_proxy(self, idx, brand, part, site, task_type):
    #     """
    #     🆕 Retry через прокси при RateLimit
    #     Создаёт НОВЫЙ контекст с прокси для ОДНОГО запроса
    #     """
    #     from utils import get_2captcha_proxy

    #     try:
    #         # Получаем прокси от 2Captcha
    #         proxy_config = get_2captcha_proxy()
    #         logger.info(f"🔄 [{idx}] Retry с прокси: {proxy_config['server'][:30]}...")

    #         # 🔥 Crawlee позволяет создавать временные контексты!
    #         # Используем browser из crawler
    #         temp_browser = self.crawler.browser_pool._browser  # Внутренний доступ

    #         # Создаём временный контекст с прокси
    #         proxy_context = await temp_browser.new_context(
    #             proxy=proxy_config,
    #             viewport={"width": 1920, "height": 1080},
    #             user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    #         )

    #         # Новая страница
    #         proxy_page = await proxy_context.new_page()

    #         try:
    #             # Переход на URL
    #             url = SiteUrls.armtek_search(part)  # Только Armtek имеет RateLimit
    #             await proxy_page.goto(url, wait_until="domcontentloaded", timeout=60000)

    #             # Парсинг через прокси
    #             from scraper_armtek_pure import parse_weight_armtek

    #             physical, volumetric = await parse_weight_armtek(
    #                 proxy_page, part, logger
    #             )

    #             logger.info(f"✅ [{idx}] Proxy retry успешен: {physical}/{volumetric}")

    #             from config import ARMTEK_P_W, ARMTEK_V_W

    #             return {ARMTEK_P_W: physical, ARMTEK_V_W: volumetric}

    #         finally:
    #             # Cleanup
    #             await proxy_page.close()
    #             await proxy_context.close()

    #     except Exception as e:
    #         logger.error(f"❌ [{idx}] Proxy retry failed: {e}")
    #         from config import ARMTEK_P_W, ARMTEK_V_W

    #         return {ARMTEK_P_W: None, ARMTEK_V_W: None}

    async def _retry_with_proxy(self, idx, brand, part, site, task_type):
        """Retry через прокси при RateLimit"""
        from utils import get_2captcha_proxy
        from playwright.async_api import async_playwright  # 🆕 Добавить импорт

        try:
            proxy_config = get_2captcha_proxy()
            logger.info(f"🔄 [{idx}] Retry с прокси: {proxy_config['server'][:30]}...")

            # 🆕 Создаём независимый Playwright контекст
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True, proxy=proxy_config  # ✅ Переместить сюда!
                )
                context = await browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                )
                page = await context.new_page()

                try:
                    url = SiteUrls.armtek_search(part)
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)

                    from scraper_armtek_pure import parse_weight_armtek

                    physical, volumetric = await parse_weight_armtek(page, part, logger)

                    logger.info(f"✅ [{idx}] Proxy retry: {physical}/{volumetric}")

                    from config import ARMTEK_P_W, ARMTEK_V_W

                    return {ARMTEK_P_W: physical, ARMTEK_V_W: volumetric}

                finally:
                    await page.close()
                    await context.close()
                    await browser.close()

        except Exception as e:
            logger.error(f"❌ [{idx}] Proxy retry failed: {e}")
            from config import ARMTEK_P_W, ARMTEK_V_W

            return {ARMTEK_P_W: None, ARMTEK_V_W: None}

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

    def _build_requests(self):
        """
        Построение Request-ов для Crawlee
        КЛЮЧЕВОЕ ОТЛИЧИЕ: URL теперь реальные!
        """
        requests = []
        logger.info(
            f"🔧 _build_requests: MAX_ROWS={MAX_ROWS}, df.shape={self.df.shape}"
        )

        for idx, row in self.df.head(MAX_ROWS).iterrows():
            article = str(row[INPUT_COL_ARTICLE]).strip()

            # 🆕 ЛОГ КАЖДОЙ ИТЕРАЦИИ
            # logger.debug(f"  Loop: idx={idx}, article={article}")

            if not article:
                # logger.warning(f"  ⚠️ Пропуск idx={idx}: пустой артикул")
                continue

            brand = str(row[INPUT_COL_BRAND]).strip()

            # ======== ВЕСА ========
            if ENABLE_WEIGHT_PARSING:
                # Japarts (приоритет)
                requests.append(
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
                # 🆕 ЛОГ ДОБАВЛЕНИЯ
                # logger.info(
                #     f"  ✅ Request #{len(requests)}: idx={idx}, site=japarts, part={article}"
                # )

                # Armtek (fallback - будет обработано если Japarts вернет None)
                requests.append(
                    Request.from_url(
                        url=SiteUrls.armtek_search(article),
                        user_data={
                            "idx": idx,
                            "brand": brand,
                            "part": article,
                            "site": "armtek",
                            "task_type": "weight",
                        },
                    )
                )

            # ======== ИМЕНА ========
            elif ENABLE_NAME_PARSING:
                # Stparts (приоритет)
                requests.append(
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

                # Avtoformula (fallback)
                requests.append(
                    Request.from_url(
                        url=SiteUrls.avtoformula_search(brand, article),
                        user_data={
                            "idx": idx,
                            "brand": brand,
                            "part": article,
                            "site": "avtoformula",
                            "task_type": "name",
                        },
                    )
                )

            # ======== ЦЕНЫ ========
            elif ENABLE_PRICE_PARSING:
                # Параллельно оба сайта
                requests.append(
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
                requests.append(
                    Request.from_url(
                        url=SiteUrls.avtoformula_search(brand, article),
                        user_data={
                            "idx": idx,
                            "brand": brand,
                            "part": article,
                            "site": "avtoformula",
                            "task_type": "price",
                        },
                    )
                )

        return requests

    async def run(self):
        """Главный метод запуска"""
        await self.setup()

        # 🆕 ОЧИСТКА КЕША CRAWLEE
        import shutil

        storage_dir = Path("storage")
        if storage_dir.exists():
            shutil.rmtree(storage_dir)
            logger.info("🗑️ Очищен кеш Crawlee")

        # Настройка Crawler
        self.crawler = PlaywrightCrawler(
            request_handler=self.request_handler,
            max_requests_per_crawl=None,  # Без лимита
            max_request_retries=3,
            # 🔥 ПРОСТОЙ ВАРИАНТ (если MAX_WORKERS=5):
            concurrency_settings=ConcurrencySettings(
                max_concurrency=MAX_WORKERS,
                desired_concurrency=MAX_WORKERS,
            ),
            headless=True,
            browser_type="chromium",
        )

        # Построение очереди
        requests = self._build_requests()
        self.total_tasks = len(requests)
        logger.info(f"📋 Задач: {self.total_tasks}")

        # Запуск
        await self.crawler.run(requests)

        # Финализация
        await self._finalize()

    async def _pre_navigation_hook(self, context: PlaywrightCrawlingContext):
        """
        Выполняется ДО навигации на каждый URL
        Здесь делаем авторизацию ОДИН раз для Avtoformula
        """
        if context.request.user_data.get("site") == "avtoformula":
            # Crawlee сам управляет сессией, поэтому логин нужен только 1 раз
            if not hasattr(self, "_avtoformula_logged_in"):
                logger.info("🔐 Авторизация Avtoformula...")
                success = await SimpleAuth.login_avtoformula(context.page)
                if success:
                    self._avtoformula_logged_in = True
                else:
                    raise Exception("Авторизация не удалась")

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


async def main():
    parser = ParserCrawler()
    await parser.run()


if __name__ == "__main__":
    asyncio.run(main())

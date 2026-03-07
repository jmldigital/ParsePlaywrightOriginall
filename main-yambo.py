"""
Парсер на Crawlee - оптимизированная версия для YUMBO-JP.COM
- Только поиск цен
- Только один сайт: https://yumbo-jp.com/
- Без авторизации
- Без прокси
- URL: https://yumbo-jp.com/parts.html?partNo={part}
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
from crawlee import Request, ConcurrencySettings
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee import Request
import logging
from telegram import Bot
from datetime import datetime, timedelta, timezone

from yumbo_parse_price import yumbo_parse_price

# 🔥 Глобальный MSK для ВСЕХ логгеров
msk_tz = timezone(timedelta(hours=3))


def msk_converter(timestamp, tz_name=None):
    return datetime.now(msk_tz).timetuple()


logging.Formatter.converter = msk_converter

from config import (
    INPUT_FILE,
    MAX_ROWS,
    JPARTS_WORKERS,  # Переиспользуем для Yumbo
    INPUT_COL_BRAND,
    INPUT_COL_ARTICLE,
    ENABLE_PRICE_PARSING,  # Только этот режим
    SELECTORS,
    get_output_file,
    reload_config,
    LOG_LEVEL,
    BATCH_SIZE,
    SEND_TO_TELEGRAM,
)
from utils import (
    logger,
    preprocess_dataframe,
    clear_debug_folders_sync,
)


# UTF-8 setup
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")
if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
os.environ["PYTHONIOENCODING"] = "utf-8"

load_dotenv()


async def send_telegram_file(file_path: str, caption: str | None = None):
    """Отправка файла в Telegram"""
    if not SEND_TO_TELEGRAM:
        return
    try:
        bot = Bot(token=os.getenv("BOT_TOKEN"))
        async with bot:
            with open(file_path, "rb") as f:
                await bot.send_document(
                    chat_id=os.getenv("ADMIN_CHAT_ID"),
                    document=f,
                    caption=caption,
                )
        logger.info("✅ Финальный файл отправлен в Telegram")
    except Exception as e:
        logger.info(f"❌ Ошибка отправки файла в Telegram: {e}")


async def block_media_requests(context: PlaywrightCrawlingContext, *args) -> None:
    """Блокирует тяжелые медиа-файлы"""
    page = context.page

    whitelist_keywords = [
        "captcha",
        "recaptcha",
        "grecaptcha",
        "hcaptcha",
        "turnstile",
        "challenge",
        "verify",
        "svg",
        "icon",
    ]

    async def route_handler(route):
        req = route.request
        url = req.url.lower()
        resource_type = req.resource_type

        if resource_type in ("image", "media", "font"):
            is_whitelisted = any(keyword in url for keyword in whitelist_keywords)
            if is_whitelisted:
                await route.continue_()
            else:
                await route.abort()
        else:
            await route.continue_()

    await page.route("**/*", route_handler)


# ===================== URL ГЕНЕРАТОР =====================
class SiteUrls:
    """URL для Yumbo-JP"""

    @staticmethod
    def yumbo_search(part: str) -> str:
        return f"https://yumbo-jp.com/parts.html?partNo={part}"


# ===================== ГЛАВНЫЙ КЛАСС =====================
class ParserCrawler:
    """Оптимизированный парсер для Yumbo-JP (только цены)"""

    def __init__(self):
        self.df = None
        self.crawler: PlaywrightCrawler | None = None
        self.results_lock = asyncio.Lock()
        self.processed_count = 0
        self.total_tasks = 0

        self.telegram_chat_id = os.getenv("ADMIN_CHAT_ID")
        self.telegram_bot_token = os.getenv("BOT_TOKEN")

        # 🔥 Глобальная пауза при RateLimit
        self.rate_limit_pause = False
        self.pause_event = asyncio.Event()
        self.pause_event.set()
        self.pause_lock = asyncio.Lock()

        # 🆕 Статистика
        self.stats = {"yumbo": {"total": 0, "success": 0, "empty": 0, "ratelimit": 0}}

    async def setup(self):
        """Инициализация"""
        reload_config()

        # Проверяем режим
        if not ENABLE_PRICE_PARSING:
            raise ValueError("❌ Только режим ЦЕНЫ!")

        self.mode = "ЦЕНЫ (Yumbo-JP)"
        logger.info(f"✅ Режим: {self.mode}")

        # Загрузка данных
        self.df = pd.read_excel(INPUT_FILE)
        self.df = preprocess_dataframe(self.df)
        self._init_columns()

        logger.info(f"📊 Загружено {len(self.df)} строк")
        self.total_tasks = len(self.df)
        logger.info(f"📊 Цель: {self.total_tasks} строк")

    def _init_columns(self):
        """Инициализация колонок для цен"""
        # Колонка для Yumbo цены (добавим в config позже)
        yumbo_price_col = "yumbo_price"
        if yumbo_price_col not in self.df.columns:
            self.df[yumbo_price_col] = None

    async def request_handler(self, context: PlaywrightCrawlingContext):
        """Обработчик Crawlee - только парсинг Yumbo"""
        page = context.page
        request = context.request
        session = context.session

        # 🔥 Глобальная пауза
        if not self.pause_event.is_set():
            await self.pause_event.wait()

        # 🛑 Быстрый стоп
        if Path("input/STOP.flag").exists():
            logger.warning("🛑 STOP.flag найден → прерываем задачу")
            return

        idx = request.user_data["idx"]
        brand = request.user_data["brand"]
        part = request.user_data["part"]
        site = request.user_data["site"]
        session_id = session.id if session else "no-session"

        try:
            # 🔥 Обновляем статистику
            async with self.results_lock:
                self.stats[site]["total"] += 1

            # 🔥 Парсинг Yumbo цен
            price = await yumbo_parse_price(part, brand, logger, page)

            # Логирование результата
            if price:
                async with self.results_lock:
                    self.stats[site]["success"] += 1
                logger.info(f"[YUMBO] ✅ [{idx}] {part} | Цена: {price}")
                await self._save_result(idx, {"yumbo_price": price})
            else:
                async with self.results_lock:
                    self.stats[site]["empty"] += 1
                logger.info(f"[YUMBO] ⚠️ [{idx}] {part} | Цена не найдена")

        except Exception as e:
            if "RateLimit" in str(e) or "429" in str(e):
                async with self.results_lock:
                    self.stats[site]["ratelimit"] += 1
                logger.warning(f"⏳ [{idx}] RateLimit {part} → пропускаем")
            else:
                logger.error(f"❌ [{idx}] {site}: {e}")
            raise

    async def _save_result(self, idx, result):
        """Потокобезопасное сохранение"""
        async with self.results_lock:
            for col, val in result.items():
                if pd.notna(val):
                    self.df.at[idx, col] = val

    async def _send_telegram_notification(self, message: str):
        """Отправка уведомления в Telegram"""
        if not self.telegram_chat_id or not self.telegram_bot_token:
            return
        try:
            url = f"https://api.telegram.org/bot{self.telegram_bot_token}/sendMessage"
            async with aiohttp.ClientSession() as session:
                await session.post(
                    url, json={"chat_id": self.telegram_chat_id, "text": message}
                )
            logger.info(f"📱 Telegram: {message}")
        except Exception as e:
            logger.error(f"❌ Telegram error: {e}")

    async def _failed_handler(self, context):
        """Логирует фатальные ошибки"""
        req = context.request
        logger.error(
            f"💀 FATAL FAIL [{req.user_data.get('site')}]: {req.url} | {context.error}"
        )

    async def run(self):
        """Главный метод запуска"""
        await self.setup()

        logger.info(f"DEBUG CONFIG: Price={ENABLE_PRICE_PARSING}")
        logger.info(f"CURRENT MODE: {self.mode}")

        # 🔥 ОДИН crawler без прокси
        self.crawler = PlaywrightCrawler(
            request_handler=self.request_handler,
            max_request_retries=2,  # Меньше ретраев при RateLimit
            use_session_pool=True,
            session_pool=SessionPool(
                create_session_settings={
                    "blocked_status_codes": [403, 407],
                }
            ),
            concurrency_settings=ConcurrencySettings(
                max_concurrency=JPARTS_WORKERS,  # Переиспользуем настройку
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

        # Добавляем блокировку медиа
        self.crawler._pre_navigation_hooks.append(block_media_requests)

        # 🔥 БАТЧ-ОБРАБОТКА
        total_rows = min(len(self.df), MAX_ROWS)
        stop_flag = Path("input/STOP.flag")

        for batch_start in range(0, total_rows, BATCH_SIZE):
            # 🛑 Проверка стоп-флага
            if stop_flag.exists():
                logger.warning("🛑 STOP.flag найден, останавливаем парсер")
                break

            batch_end = min(batch_start + BATCH_SIZE, total_rows)
            batch_num = batch_start // BATCH_SIZE + 1

            logger.info(f"📦 БАТЧ #{batch_num}: строки {batch_start}-{batch_end}")

            # 🔥 Обработка батча Yumbo
            await self._process_price_batch(batch_start, batch_end, batch_num)

            # 💾 ПРОМЕЖУТОЧНОЕ СОХРАНЕНИЕ
            output_file = get_output_file("ЦЕНЫ")
            await asyncio.to_thread(self.df.to_excel, output_file, index=False)
            logger.info(f"💾 Батч #{batch_num} сохранён ({batch_end} строк)")

            # Telegram уведомление
            if batch_num % 2 == 0:
                message = f"📊 Yumbo парсер: обработано <b>{batch_end}</b> строк из {total_rows}"
                await self._send_telegram_notification(message)

            await self.finalize_saved_file(output_file, batch_num)

        # Финальная статистика
        logger.info(f"📊 ИТОГО Yumbo: {self.stats['yumbo']}")
        logger.info(f"📊 Обработано: {self.processed_count}/{self.total_tasks}")
        await self._finalize()

    async def _process_price_batch(self, batch_start, batch_end, batch_num):
        """Обработка батча цен Yumbo"""
        yumbo_requests = []

        for idx in range(batch_start, batch_end):
            row = self.df.iloc[idx]
            article = str(row[INPUT_COL_ARTICLE]).strip()
            brand = str(row[INPUT_COL_BRAND]).strip()
            if not article:
                continue

            yumbo_requests.append(
                Request.from_url(
                    url=SiteUrls.yumbo_search(article),
                    user_data={
                        "idx": idx,
                        "brand": brand,
                        "part": article,
                        "site": "yumbo",
                        "task_type": "price",
                    },
                )
            )

        if yumbo_requests:
            logger.info(f"🚀 Yumbo батч #{batch_num}: {len(yumbo_requests)} задач")
            await self.crawler.run(yumbo_requests)
        else:
            logger.info(f"✅ Батч #{batch_num}: нет данных для обработки")

    async def finalize_saved_file(self, input_file: str, batch_num: int):
        """Финализация батча"""
        logger.info(f"🔄 Финализация batch_finalize.xlsx (батч #{batch_num})")

        df_final = pd.read_excel(input_file)

        batch_final_file = "output/batch_finalize.xlsx"
        await asyncio.to_thread(df_final.to_excel, batch_final_file, index=False)
        logger.info(f"💾 batch_finalize.xlsx готов ({len(df_final)} строк)")

    async def _finalize(self):
        """Финальная обработка"""
        logger.info("🔄 Финализация...")

        output_file = get_output_file("ЦЕНЫ")

        logger.info(f"✅ Сохранено: {output_file}")
        logger.info(f"📊 Обработано: {self.processed_count}/{self.total_tasks}")
        await send_telegram_file(output_file, "✅ Yumbo цены завершены!")


async def main():
    logger.setLevel(getattr(logging, LOG_LEVEL.upper()))
    clear_debug_folders_sync(logger)
    reload_config()
    logger.info("🚀 YUMBO-JP ПАРСЕР: START!")
    parser = ParserCrawler()
    await parser.run()


if __name__ == "__main__":
    asyncio.run(main())

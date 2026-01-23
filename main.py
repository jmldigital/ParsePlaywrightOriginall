# main.py
"""
Асинхронный парсер с Playwright.
- Общие куки для avtoformula
- Автоматический re-login при разлогине
- Разделённые логи по сайтам
"""
import random
from telegram import Bot
import asyncio
import sys  # 🆕 №1 — ПЕРВЫЙ!
import io  # 🆕 №2
import os  # 🆕 №3
import pandas as pd
import signal
import math
import multiprocessing
from pathlib import Path
from tqdm.asyncio import tqdm
from dotenv import load_dotenv


# 🔥 ГЛОБАЛЬНЫЙ UTF-8 для ВСЕГО
sys.stdout.reconfigure(encoding="utf-8")
sys.stderr.reconfigure(encoding="utf-8")

if os.name == "nt":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

os.environ["PYTHONIOENCODING"] = "utf-8"

print("🟢 Глобальный UTF-8: 🚀 Тест прошел!")


from scraper_japarts import scrape_weight_japarts
from scraper_armtek import scrape_weight_armtek

load_dotenv()
from config import BAD_DETAIL_NAMES

from playwright.async_api import async_playwright, Browser, BrowserContext
from config import (
    ENABLE_NAME_PARSING,
    ENABLE_WEIGHT_PARSING,
    ENABLE_PRICE_PARSING,
    COOKIE_FILE,
    AVTO_LOGIN,
    AVTO_PASSWORD,
    BOT_TOKEN,
    ADMIN_CHAT_ID,
    SEND_TO_TELEGRAM,
    TASK_TIMEOUT,
    PROXY_TIMOUT,
    get_output_file,
    TEMP_RAW,
    TEMP_FILES_DIR,
    reload_config,
)

from utils import (
    logger,
    preprocess_dataframe,
    consolidate_weights,
    clear_debug_folders_sync,
    get_2captcha_proxy,
)
from state_manager import load_state, save_state
from price_adjuster import adjust_prices_and_save
import requests

# Импортируем асинхронные скрапперы
from scraper_avtoformula import scrape_avtoformula_pw, scrape_avtoformula_name_async
from scraper_stparts import scrape_stparts_async, scrape_stparts_name_async
from auth import ensure_logged_in


async def safe_close_page(page):
    """Улучшенное закрытие"""
    if page:
        try:
            if not page.is_closed():
                await page.close()
        except Exception as e:
            logger.debug(f"Page close ignored: {e}")


# ENABLE_NAME_PARSING = os.getenv("ENABLE_NAME_PARSING", "False").lower() == "true"
COOKIE_PATH = Path(COOKIE_FILE)
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# === Разделение логов ===
from utils import get_site_logger

logger_avto = get_site_logger("avtoformula")
logger_st = get_site_logger("stparts")
logger_jp = get_site_logger("japarts")
logger_armtek = get_site_logger("armtek")

stop_parsing = multiprocessing.Event()
stop_parsing.clear()

sites = ["avtoformula", "stparts", "japarts", "armtek"]

INPUT_DIR = Path("input")

stop_files = ["STOP", "STOP.flag", "AIL_STOP"]
for name in stop_files:
    path = INPUT_DIR / name
    if path.exists():
        path.unlink()
        logger.info("🧹 Удален %s", path)

logger.info("🚀 Старт без STOP флагов в input/")

TEMP_FILES_DIR.mkdir(exist_ok=True)


def get_temp_file(worker_id: int):
    """Файл для конкретного worker"""
    return TEMP_FILES_DIR / f"temp_worker_{worker_id:02d}.xlsx"


async def merge_temp_files(df_template: pd.DataFrame, mode: str):
    """
    Merge ВСЕХ файлов из TEMP_FILES_DIR в df_template
    """

    logger.info(f"🔄 Merge из {TEMP_FILES_DIR}...")

    df_merged = df_template.copy()
    merged_count = 0

    # Загружаем ВСЕ temp_worker_*.xlsx
    if TEMP_FILES_DIR.exists():
        for temp_file in TEMP_FILES_DIR.glob("temp_worker_*.xlsx"):
            try:
                temp_df = pd.read_excel(temp_file)
                logger.info(f"📂 [{merged_count+1}] {temp_file.name}")

                # Merge по индексу
                for idx in temp_df.index:
                    if idx in df_merged.index:
                        for col in temp_df.columns:
                            if col != "idx":  # Игнорируем служебные
                                df_merged.at[idx, col] = temp_df.at[idx, col]

                temp_file.unlink()  # Cleanup
                merged_count += 1

            except Exception as e:
                logger.warning(f"⚠️ Пропуск {temp_file}: {e}")

        # Полная очистка директории
        # try:
        #     for leftover in TEMP_FILES_DIR.glob("*"):
        #         leftover.unlink()
        #     TEMP_FILES_DIR.rmdir()
        #     logger.info("🧹 TEMP_FILES_DIR очищена")
        # except:
        #     logger.warning("⚠️ Не удалось полностью очистить TEMP_FILES_DIR")

    else:
        logger.warning("⚠️ TEMP_FILES_DIR не существует")

    logger.info(f"✅ Merge завершён! Объедено файлов: {merged_count}")
    return df_merged


def setup_event_loop_policy():
    if sys.platform.startswith("win"):
        if hasattr(asyncio, "WindowsProactorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            print("Установлена WindowsProactorEventLoopPolicy для Windows")
    else:
        print("Не Windows — политика событийного цикла не меняется")


def send_telegram_process(msg):
    """Отправка прогресса в Telegram"""
    if not SEND_TO_TELEGRAM:
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(
            url, data={"chat_id": ADMIN_CHAT_ID, "text": f"🕐 Прогресс:\n{msg}"}
        )
    except Exception as e:
        logger.error("Ошибка отправки прогресса в Telegram: %s", e)


# === Telegram ===
def send_telegram_error(msg):
    if not SEND_TO_TELEGRAM:
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(
            url, data={"chat_id": ADMIN_CHAT_ID, "text": f"❌ Parser Error:\n{msg}"}
        )
    except Exception as e:
        logger.error("Ошибка Telegram: %s", e)


async def send_telegram_file(file_path, caption=None):
    if not SEND_TO_TELEGRAM:
        return
    try:
        bot = Bot(token=BOT_TOKEN)
        async with bot:
            with open(file_path, "rb") as f:  # ← теперь файл закрывается
                await bot.send_document(
                    chat_id=ADMIN_CHAT_ID, document=f, caption=caption
                )
        logger.info("Файл отправлен в Telegram")
    except Exception as e:
        logger.error("Ошибка отправки в Telegram: %s", e)


async def finalize_processing(df: pd.DataFrame, mode: str, output_file: str = None):
    """Финальная обработка + сохранение (для normal/extreme stop)"""
    logger.info(f"🔄 Финализация ({mode})...")

    # 🆕 Проверяем DataFrame
    if df is None or df.empty:
        logger.error("❌ DataFrame пустой или None!")
        return

    try:
        # 🆕 Инициализируем недостающие колонки (как в main)
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

        for col in [
            stparts_price,
            stparts_delivery,
            avtoformula_price,
            avtoformula_delivery,
        ]:
            if col not in df.columns:
                df[col] = None

        if ENABLE_WEIGHT_PARSING:
            for col in [JPARTS_P_W, JPARTS_V_W, ARMTEK_P_W, ARMTEK_V_W]:
                if col not in df.columns:
                    df[col] = None

        if ENABLE_NAME_PARSING and "finde_name" not in df.columns:
            df["finde_name"] = None

        if ENABLE_WEIGHT_PARSING:  # Режим весов
            df = await asyncio.to_thread(consolidate_weights, df)
            logger.info("✅ Веса консолидированы")

        # 🆕 Гарантированно получаем output_file
        if not output_file:
            output_file = get_output_file(mode)
            if not output_file:
                raise ValueError(f"Не удалось определить output_file для режима {mode}")

        logger.info(f"💾 Сохраняем в: {output_file}")

        if ENABLE_PRICE_PARSING:
            await asyncio.to_thread(adjust_prices_and_save, df, output_file)
        else:
            await asyncio.to_thread(df.to_excel, output_file, index=False)

        logger.info(f"✅ Сохранено: {output_file}")
        await send_telegram_file(output_file, f"✅ {mode} завершены!")

    except Exception as e:
        logger.error(
            f"❌ Ошибка при сохранении Excel с форматированием: {e}", exc_info=True
        )
        # 🆕 Emergency save без форматирования
        emergency_file = output_file.replace(".xlsx", "_emergency.xlsx")
        try:
            await asyncio.to_thread(df.to_excel, emergency_file, index=False)
            logger.info(f"💾 Emergency save: {emergency_file}")
            await send_telegram_file(emergency_file, f"⚠️ {mode} (emergency)")
        except Exception as e2:
            logger.error(f"❌ Даже emergency save failed: {e2}")


# === Пул контекстов ===
class ContextPool:

    def __init__(
        self, browser: Browser, pool_size: int = 5, auth_avtoformula: bool = True
    ):
        self.browser = browser
        self.pool_size = pool_size
        self.contexts = []
        self.semaphore = asyncio.Semaphore(pool_size)
        self.initialized = False
        self.cookies = None  # общие куки
        self.auth_avtoformula = auth_avtoformula  # 🆕 ПАРАМЕТР!

    async def initialize(self):
        if self.auth_avtoformula:
            """Создание пула контекстов с общей авторизацией. Страницы создаются при обработке задач."""
            logger.info("🔧 Авторизация на Avtoformula для получения кук...")

            # Временный контекст для логина
            temp_context = await self.browser.new_context()
            temp_page = await temp_context.new_page()

            try:
                if not await ensure_logged_in(temp_page, AVTO_LOGIN, AVTO_PASSWORD):
                    logger.error("❌ Не удалось авторизоваться на Avtoformula")
                    raise RuntimeError("Авторизация не удалась")

                # Сохраняем состояние авторизации (куки + localStorage и т.д.)
                await temp_context.storage_state(path=COOKIE_PATH)
                logger.info(
                    "✅ Авторизация успешна, состояние сохранено в storage_state.json"
                )

            finally:
                await temp_context.close()

            # Создаём пул контекстов, загружая состояние
            logger.info("Создаём %d контекстов...", self.pool_size)
            self.contexts = []  # очищаем на всякий случай

            for i in range(self.pool_size):
                ctx = await self.browser.new_context(
                    storage_state=COOKIE_PATH,  # ← авторизованное состояние
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                )
                self.contexts.append(ctx)
                logger.info(
                    f"✅ Контекст {i + 1}/{self.pool_size} создан и авторизован"
                )
        else:
            # ✅ ПРОСТАЯ инициализация
            logger.info(f"Создаём {self.pool_size} контекстов БЕЗ авторизации...")
            for i in range(self.pool_size):
                ctx = await self.browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    user_agent="Mozilla/5.0...",
                )
                self.contexts.append(ctx)

        self.initialized = True

    async def refresh_cookies(self):
        """Переавторизация и обновление куков для всех контекстов"""
        logger.warning("🔄 Обнаружен разлогин — повторная авторизация...")
        temp_context = await self.browser.new_context()
        temp_page = await temp_context.new_page()

        try:
            if await ensure_logged_in(temp_page, AVTO_LOGIN, AVTO_PASSWORD):
                # Получаем куки
                cookies = await temp_context.cookies()
                await temp_context.storage_state(path=COOKIE_PATH)
                logger.info("✅ Авторизация успешна, куки обновлены и сохранены")

                # Обновляем куки во всех активных контекстах
                for ctx in self.contexts:
                    await ctx.add_cookies(cookies)
                logger.info(f"✅ Куки обновлены для {len(self.contexts)} контекстов")
            else:
                logger.error("❌ Повторная авторизация не удалась")
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении кук: {e}")
        finally:
            await temp_context.close()

    async def get_context(self):
        """Получить один контекст из пула (без страницы)"""
        await self.semaphore.acquire()
        if not self.contexts:
            raise RuntimeError("Нет свободных контекстов")
        return self.contexts.pop()  # ← возвращаем только контекст

    def release_context(self, ctx):
        """Вернуть контекст в пул"""
        self.contexts.append(ctx)
        self.semaphore.release()

    async def close_all(self):
        for ctx in self.contexts:
            await ctx.close()
        self.contexts.clear()
        logger.info("🛑 Все контексты закрыты")


# class SimpleContextPool(ContextPool):
#     """Пул БЕЗ авторизации — для весов/имен"""

#     async def initialize(self):
#         """ПРОСТАЯ инициализация БЕЗ авторизации"""
#         logger.info(f"Создаём {self.pool_size} простых контекстов...")

#         for i in range(self.pool_size):
#             ctx = await self.browser.new_context(
#                 viewport={"width": 1920, "height": 1080},
#                 user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
#             )
#             self.contexts.append(ctx)
#             logger.debug(f"✅ Контекст {i + 1}/{self.pool_size} создан")

#         self.initialized = True
#         logger.info(f"✅ {self.pool_size} простых контекстов готово")


async def process_single_item(page1, idx: int, brand: str, part: str):
    """
    Только логика парсинга БЕЗ создания контекстов!
    Поддерживает WEIGHT/NAME/PRICE режимы.
    Возвращает результат или "NeedProxy" при RateLimit.
    """
    from config import (
        ENABLE_WEIGHT_PARSING as WEIGHT,
        ENABLE_NAME_PARSING as NAME,
        ENABLE_PRICE_PARSING as PRICE,
        JPARTS_P_W,
        JPARTS_V_W,
        ARMTEK_P_W,
        ARMTEK_V_W,
        stparts_price,
        stparts_delivery,
        avtoformula_price,
        avtoformula_delivery,
    )

    # Инициализация результатов
    result = {}
    # Дя теста----------------------
    # if WEIGHT:
    #     # ✅ ОТКЛЮЧЕНО JAPARTS ДЛЯ ТЕСТА!
    #     jp_physical, jp_volumetric = None, None  # ← Принудительно None!

    #     logger.info(f"🚀 [{idx}] ТЕСТ: ТОЛЬКО ARMTEK: {part}")

    #     # ПРЯМО к Armtek!
    #     # 🔥 ПРЯМО ЗДЕСЬ — добавьте/измените:
    #     try:
    #         armtek_physical, armtek_volumetric = await asyncio.wait_for(
    #             scrape_weight_armtek(page, part, logger_armtek),
    #             timeout=90.0,  # ← Было 15.0 → 90.0!
    #         )
    #         logger.info(
    #             f"🔍 [{idx}] Armtek result внутри process_raw: {armtek_physical=}, {armtek_volumetric=}"
    #         )
    #     except asyncio.TimeoutError:
    #         logger.error(f"⚠️ [{idx}] ARMTEK TIMEOUT!")
    #         armtek_physical, armtek_volumetric = None, None

    #     # 🧪 ДИАГНОСТИКА:
    #     logger.info(
    #         f"🧪 [{idx}] FINAL CHECK: physical='{armtek_physical}', vol='{armtek_volumetric}'"
    #     )

    #     # 🆕 ИСПРАВЛЕНИЕ RateLimit!
    #     # if armtek_physical == "NeedProxy" or armtek_volumetric == "NeedProxy":
    #     if random.random() < 0.3:
    #         logger.warning(
    #             f"🚦 [{idx}] RateLimit → NeedProxy! внутри Process_single_item ловит"
    #         )
    #         return "NeedProxy"  # ← Worker поймает!

    #     result.update(
    #         {
    #             JPARTS_P_W: None,  # ← Japarts отключён
    #             JPARTS_V_W: None,  # ← Japarts отключён
    #             ARMTEK_P_W: armtek_physical,
    #             ARMTEK_V_W: armtek_volumetric,
    #         }
    #     )

    # ======================= WEIGHT =======================

    if WEIGHT:
        jp_physical, jp_volumetric = None, None
        armtek_physical, armtek_volumetric = None, None

        try:
            # Japarts
            logger.info(f"🔍 [{idx}] Japarts: {part}")
            jp_physical, jp_volumetric = await scrape_weight_japarts(
                page1, part, logger_jp
            )

            # Armtek — ТОЛЬКО при Japarts fail
            if not jp_physical or not jp_volumetric:
                logger.info(f"🚀 [{idx}] Japarts fail → ARMTEK: {part}")

                armtek_physical, armtek_volumetric = await scrape_weight_armtek(
                    page1, part, logger_armtek
                )

                # 🚨 RateLimit детектор!
                if armtek_physical == "NeedProxy":
                    logger.info(f"🎯 [{idx}] RateLimit → NeedProxy!")
                    return "NeedProxy"  # ← ПРОКИДЫВАЕМ НАВЕРХ!

                # Сохраняем Armtek результат
                result.update(
                    {
                        JPARTS_P_W: jp_physical,
                        JPARTS_V_W: jp_volumetric,
                        ARMTEK_P_W: armtek_physical,
                        ARMTEK_V_W: armtek_volumetric,
                    }
                )

            else:
                # Только Japarts
                result.update(
                    {
                        JPARTS_P_W: jp_physical,
                        JPARTS_V_W: jp_volumetric,
                        ARMTEK_P_W: None,
                        ARMTEK_V_W: None,
                    }
                )

        except Exception as e:
            logger.error(f"❌ [{idx}] Weight parse error: {e}")
            result.update(
                {JPARTS_P_W: None, JPARTS_V_W: None, ARMTEK_P_W: None, ARMTEK_V_W: None}
            )
        logger.info(f"📊 [{idx}] WEIGHT result: {result}")
        return result  # 🔥 🔥 ДОБАВИТЬ ЭТУ СТРОКУ! 🔥 🔥

    # ======================= NAME =======================
    if NAME:
        try:
            detail_name = await scrape_stparts_name_async(page1, part, logger_st)

            if not detail_name or detail_name.lower().strip() in BAD_DETAIL_NAMES:
                if detail_name:
                    logger.info(f"⚠️ [{idx}] stparts '{detail_name}' → avtoformula")
                detail_name = await scrape_avtoformula_name_async(
                    page1, part, logger_avto
                )

                if not detail_name or detail_name.lower().strip() in BAD_DETAIL_NAMES:
                    detail_name = "Detail"
                    logger.info(f"❌ [{idx}] Название не найдено: {part}")

            result["finde_name"] = detail_name

        except Exception as e:
            logger.error(f"❌ [{idx}] Name parse error: {e}")
            result["finde_name"] = "Detail"

    # ======================= PRICE =======================
    if PRICE:  # PRICE — 🔥 ИМЕННО КАК В СТАРОМ!
        try:
            page2 = await page1.context.new_page()  # 🆕 ИЗ ТОГО ЖЕ CONTEXT!

            result_price_st, result_price_avto = await asyncio.gather(
                scrape_stparts_async(page1, brand, part, logger_st),
                scrape_avtoformula_pw(page2, brand, part, logger_avto),
                return_exceptions=True,
            )

            await safe_close_page(page2)  # Закрываем ВТОРУЮ

            # Проверка разлогина (адаптируйте под worker)
            if (
                isinstance(result_price_avto, Exception)
                and "зарегистрируйтесь" in str(result_price_avto).lower()
            ):
                return "ReauthNeeded"  # Worker: pool.refresh_cookies() + retry

            # Возврат как в старом
            price_st, delivery_st = result_price_st if result_price_st else (None, None)
            price_avto, delivery_avto = (
                result_price_avto if result_price_avto else (None, None)
            )
            return idx, {
                stparts_price: price_st,
                stparts_delivery: delivery_st,
                avtoformula_price: price_avto,
                avtoformula_delivery: delivery_avto,
            }

        except Exception as e:
            logger.error(f"Ошибка [{idx}]: {e}")
            return None  # ← Нормальный результат

    return result  # Общий return в конце


# async def worker(
#     worker_id: int,
#     queue: asyncio.Queue,
#     pool: ContextPool,
#     normal_browser: Browser,  # 🆕 Browser #1: ContextPool (Обычный)
#     proxy_browser: Browser,  # 🆕 Browser #2: Proxy задачи (С флагом proxy="per-context")
#     df: pd.DataFrame,
#     pbar,
#     total_tasks: int,
#     progress_checkpoints: set,
#     sent_progress: set,
#     counter: dict,
#     counter_lock: asyncio.Lock,
# ):
#     """
#     Worker с 2 БРАУЗЕРАМИ:
#     1. Пытается взять контекст из пула (normal_browser).
#     2. При RateLimit переключается на proxy_browser и СОХРАНЯЕТ этот контекст для следующих задач.
#     """
#     proxy_context = None  # Личный proxy_context, живет между итерациями цикла

#     try:
#         while not queue.empty():
#             try:
#                 # Получаем задачу
#                 idx_brand_part = await queue.get()
#                 idx, brand, part = idx_brand_part

#                 # В worker() блок STOP.flag:
#                 if Path("input/STOP.flag").exists():
#                     logger.info(f"👷 Worker-{worker_id}: STOP.flag → graceful stop")

#                     # ✅ НИЧЕГО НЕ САЛАЕМ! Просто выходим
#                     queue.task_done()
#                     break  # ← Graceful exit, df уже обновлён в памяти!

#                 # Инициализация переменных для текущей итерации
#                 using_proxy = proxy_context is not None
#                 pool_ctx_obj = None
#                 page = None

#                 # Результат обработки
#                 result = None

#                 try:
#                     # 🚦 ШАГ 1: ВЫБОР РЕЖИМА
#                     if not using_proxy:
#                         # НОРМАЛЬНЫЙ РЕЖИМ (берем из пула)
#                         pool_ctx_obj = await pool.get_context()
#                         context = pool_ctx_obj
#                         page1 = await context.new_page()
#                         # logger.debug(f"👷 Worker-{worker_id}: Pool context")
#                     else:
#                         # PROXY РЕЖИМ (используем свой личный контекст)
#                         context = proxy_context
#                         page1 = await context.new_page()
#                         logger.debug(f"👷 Worker-{worker_id}: Proxy context (Reuse)")

#                     # Основной парсинг
#                     try:
#                         result = await asyncio.wait_for(
#                             process_single_item(page1, idx, brand, part),
#                             timeout=TASK_TIMEOUT,
#                         )
#                     except Exception as e:
#                         logger.warning(
#                             f"👷 Worker-{worker_id}: Ошибка парсинга {part}: {e}"
#                         )
#                         result = None

#                     if result == "ReauthNeeded":
#                         await pool.refresh_cookies()  # Как в старом!
#                         await queue.put((idx, brand, part))  # Вернуть задачу в очередь
#                         continue

#                     # 🚦 ШАГ 2: ОБРАБОТКА RateLimit (ПЕРЕКЛЮЧЕНИЕ / РОТАЦИЯ)
#                     if result == "NeedProxy":
#                         logger.warning(
#                             f"👷 Worker-{worker_id}: 🚦 RateLimit на {part}. Переключение на прокси..."
#                         )

#                         # 1. Закрываем текущую страницу и освобождаем пул
#                         await safe_close_page(page1)
#                         page = None  # Чтобы finally не пытался закрыть дважды

#                         if pool_ctx_obj:
#                             pool.release_context(pool_ctx_obj)
#                             pool_ctx_obj = None  # Сброс ссылки

#                         # 2. Если у нас УЖЕ был прокси, но он словил бан — закрываем его (Ротация)
#                         if proxy_context:
#                             logger.info(
#                                 f"👷 Worker-{worker_id}: ♻️ Старый прокси забанен, меняем IP..."
#                             )
#                             await proxy_context.close()
#                             proxy_context = None

#                         # 3. Получаем конфиг прокси
#                         # ⚠️ ВАЖНО: get_2captcha_proxy должен возвращать словарь с 'server', 'username', 'password'!
#                         proxy_cfg = get_2captcha_proxy()

#                         if not proxy_cfg or "server" not in proxy_cfg:
#                             logger.error("❌ Прокси не получен или неверный формат")
#                         else:
#                             logger.debug(f"✅ Прокси‑конфиг: {proxy_cfg['server']}")

#                         if proxy_cfg and "server" in proxy_cfg:
#                             try:
#                                 # 4. Создаем НОВЫЙ proxy_context
#                                 proxy_context = await asyncio.wait_for(
#                                     proxy_browser.new_context(
#                                         proxy=proxy_cfg,  # ← Ваша get_2captcha_proxy() остается!
#                                         viewport={"width": 1920, "height": 1080},
#                                         device_scale_factor=1.0,
#                                         is_mobile=False,
#                                         has_touch=False,
#                                         locale="ru-RU",
#                                         timezone_id="Europe/Moscow",
#                                         user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
#                                         ignore_https_errors=True,
#                                         extra_http_headers={
#                                             "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
#                                         },
#                                     ),
#                                     timeout=60.0,
#                                 )
#                                 logger.info(
#                                     f"👷 Worker-{worker_id}: ✅ Proxy подключен!"
#                                 )

#                                 # 5. RETRY (Повторная попытка с тем же товаром)
#                                 page_retry = await proxy_context.new_page()

#                                 # (Опционально) Проверка IP
#                                 # await page_retry.goto("https://api.ipify.org", timeout=10000)

#                                 result = await asyncio.wait_for(
#                                     process_single_item(page_retry, idx, brand, part),
#                                     timeout=PROXY_TIMOUT,  # Увеличенный таймаут для прокси
#                                 )
#                                 await safe_close_page(page_retry)

#                             except asyncio.TimeoutError:
#                                 logger.error(
#                                     f"👷 Worker-{worker_id}: ❌ Proxy connection timeout!"
#                                 )
#                                 if proxy_context:
#                                     await proxy_context.close()
#                                     proxy_context = None
#                                 result = None
#                             except Exception as e:
#                                 logger.error(
#                                     f"👷 Worker-{worker_id}: ❌ Ошибка прокси: {e}"
#                                 )
#                                 if proxy_context:
#                                     await proxy_context.close()
#                                     proxy_context = None
#                                 result = None
#                         else:
#                             logger.error(
#                                 f"👷 Worker-{worker_id}: ❌ Не удалось получить конфиг прокси!"
#                             )
#                             result = None

#                     # ✅ СОХРАНЕНИЕ РЕЗУЛЬТАТА
#                     if result and result != "NeedProxy" and isinstance(result, dict):
#                         for col, val in result.items():
#                             df.at[idx, col] = val
#                         # logger.info(f"✅ [{idx}] {part} OK")

#                 finally:
#                     # Очистка ресурсов ТЕКУЩЕЙ итерации
#                     if page:
#                         await safe_close_page(page1)

#                     # Если использовали пул — возвращаем контекст
#                     if not using_proxy and pool_ctx_obj:
#                         pool.release_context(pool_ctx_obj)

#                     # ⚠️ ВАЖНО: Мы НЕ закрываем proxy_context здесь,
#                     # чтобы использовать его на следующей итерации while!

#                 pbar.update(1)

#                 # Обновление общего прогресса и отправка в Telegram
#                 async with counter_lock:
#                     counter["processed"] += 1
#                     processed_count = counter["processed"]

#                     # 🆕 ПРОМЕЖУТОЧНОЕ СОХРАНЕНИЕ каждые TEMP_RAW строк!
#                     if processed_count % TEMP_RAW == 0:
#                         try:
#                             # Атомарное + engine fix
#                             import tempfile, shutil

#                             fd, temp_path = tempfile.mkstemp(suffix=".xlsx")
#                             os.close(fd)

#                             with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
#                                 df.to_excel(writer, index=False)

#                             shutil.move(temp_path, TEMP_FILE)
#                             logger.info(f"💾 Atomic: {processed_count}/{total_tasks}")
#                         except Exception as e:
#                             logger.error(f"❌ Atomic save: {e}")

#                     # 🆕 Отправка прогресса!
#                     if (
#                         processed_count in progress_checkpoints
#                         and processed_count not in sent_progress
#                     ):
#                         percent = int(processed_count / total_tasks * 100)
#                         send_telegram_process(
#                             f"Прогресс: {percent}% ({processed_count} из {total_tasks})"
#                         )
#                         sent_progress.add(processed_count)
#                 queue.task_done()

#             except asyncio.TimeoutError:
#                 logger.error(f"👷 Worker-{worker_id}: Task wait timeout!")
#                 queue.task_done()
#             except Exception as e:
#                 logger.error(f"👷 Worker-{worker_id}: Critical Worker Exception: {e}")
#                 queue.task_done()

#     finally:
#         # 🆕 ПОЛНАЯ очистка!
#         try:
#             # Страницы
#             if "page1" in locals():
#                 await safe_close_page(page1)
#             if "page_retry" in locals():
#                 await safe_close_page(page_retry)

#             # Контексты
#             if proxy_context:
#                 await proxy_context.close()
#                 logger.info(f"👷 Worker-{worker_id}: Proxy closed")

#             # Pool context
#             if "pool_ctx_obj" in locals() and pool_ctx_obj:
#                 pool.release_context(pool_ctx_obj)
#                 logger.info(f"👷 Worker-{worker_id}: Pool released")

#         except Exception as e:
#             logger.error(f"👷 Worker-{worker_id} cleanup error: {e}")


async def worker(
    worker_id: int,
    queue: asyncio.Queue,
    pool: ContextPool,
    normal_browser: Browser,
    proxy_browser: Browser,
    df: pd.DataFrame,
    pbar,
    total_tasks: int,
    progress_checkpoints: set,
    sent_progress: set,
    counter: dict,
    counter_lock: asyncio.Lock,
):
    """
    Worker с 2 БРАУЗЕРАМИ:
    1. Пытается взять контекст из пула (normal_browser).
    2. При RateLimit переключается на proxy_browser и СОХРАНЯЕТ этот контекст.
    """
    proxy_context = None

    my_temp_file = get_temp_file(worker_id)

    while True:  # ← Изменено: while True вместо queue.empty()
        idx_brand_part = None
        page1 = None
        page_retry = None
        pool_ctx_obj = None

        try:
            # Получаем задачу (блокируется до получения)
            idx_brand_part = await queue.get()

            # Если None — poison pill (graceful exit)
            if idx_brand_part is None:
                logger.info(f"👷 Worker-{worker_id}: Получен poison pill → exit")
                break

            idx, brand, part = idx_brand_part

            # Блок STOP.flag
            if Path("input/STOP.flag").exists():
                logger.info(f"👷 Worker-{worker_id}: STOP.flag → graceful stop")
                break

            # Инициализация
            using_proxy = proxy_context is not None
            result = None

            # 🚦 ШАГ 1: ВЫБОР РЕЖИМА
            if not using_proxy:
                pool_ctx_obj = await pool.get_context()
                context = pool_ctx_obj
                page1 = await context.new_page()
            else:
                context = proxy_context
                page1 = await context.new_page()
                logger.debug(f"👷 Worker-{worker_id}: Proxy context (Reuse)")

            # Основной парсинг
            result = await asyncio.wait_for(
                process_single_item(page1, idx, brand, part),
                timeout=TASK_TIMEOUT,
            )

            if result == "ReauthNeeded":
                await pool.refresh_cookies()
                await queue.put((idx, brand, part))  # Retry
                continue

            # 🚦 ШАГ 2: RateLimit обработка
            if result == "NeedProxy":
                logger.warning(
                    f"👷 Worker-{worker_id}: 🚦 RateLimit на {part}. Переключение..."
                )

                # Cleanup текущего
                await safe_close_page(page1)
                page1 = None
                if pool_ctx_obj:
                    pool.release_context(pool_ctx_obj)
                    pool_ctx_obj = None

                # Ротация прокси если был
                if proxy_context:
                    logger.info(f"👷 Worker-{worker_id}: ♻️ Меняем IP...")
                    await proxy_context.close()
                    proxy_context = None

                # Новый прокси
                proxy_cfg = get_2captcha_proxy()
                if not proxy_cfg or "server" not in proxy_cfg:
                    logger.error("❌ Нет прокси конфига")
                    result = None
                else:
                    try:
                        proxy_context = await asyncio.wait_for(
                            proxy_browser.new_context(
                                proxy=proxy_cfg,
                                viewport={"width": 1920, "height": 1080},
                                device_scale_factor=1.0,
                                is_mobile=False,
                                has_touch=False,
                                locale="ru-RU",
                                timezone_id="Europe/Moscow",
                                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                                ignore_https_errors=True,
                                extra_http_headers={
                                    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                                },
                            ),
                            timeout=60.0,
                        )
                        logger.info(f"👷 Worker-{worker_id}: ✅ Proxy подключен!")

                        # Retry с прокси
                        page_retry = await proxy_context.new_page()
                        result = await asyncio.wait_for(
                            process_single_item(page_retry, idx, brand, part),
                            timeout=PROXY_TIMOUT,
                        )
                        await safe_close_page(page_retry)
                        page_retry = None

                    except asyncio.TimeoutError:
                        logger.error(f"👷 Worker-{worker_id}: ❌ Proxy timeout!")
                        if proxy_context:
                            await proxy_context.close()
                            proxy_context = None
                        result = None
                    except Exception as e:
                        logger.error(f"👷 Worker-{worker_id}: ❌ Proxy error: {e}")
                        if proxy_context:
                            await proxy_context.close()
                            proxy_context = None
                        result = None
            # else:
            #     logger.error(f"👷 Worker-{worker_id}: ❌ Нет прокси!")
            #     result = None

            # ✅ СОХРАНЕНИЕ РЕЗУЛЬТАТА (финальная версия)
            if result and isinstance(result, dict):
                # logger.info(f"🔍 [{idx}] SAVE DEBUG: result={result}")

                # Инициализация колонок
                for col in result.keys():
                    if col not in df.columns:
                        # logger.info(f"➕ [{idx}] Колонка '{col}'")
                        df[col] = pd.NA

                first_col = next(iter(result))
                before_val = df.at[idx, first_col]

                # 🔧 ОЧИСТКА None → pd.NA
                for col, val in result.items():
                    clean_val = pd.NA if val is None else val
                    df.at[idx, col] = clean_val
                    logger.debug(f"📝 [{idx}] {col} ← {repr(clean_val)}")

                after_val = df.at[idx, first_col]

                # logger.info(
                #     f"✅ [{idx}] СТАТУС: {repr(before_val)} → {repr(after_val)} | "
                #     f"non-null +1"
                # )
            pbar.update(1)

            # ПРОМЕЖУТОЧНОЕ СОХРАНЕНИЕ + DEBUG
            async with counter_lock:
                counter["processed"] += 1
                processed_count = counter["processed"]

                logger.debug(
                    f"📊 Progress: {processed_count}/{total_tasks}, df.shape={df.shape}"
                )

                # 🔥 КАЖДЫЙ WORKER ПИШЕТ В СВОЙ ФАЙЛ
                if processed_count % TEMP_RAW == 0:
                    my_temp_file = get_temp_file(worker_id)
                    my_temp_file.parent.mkdir(parents=True, exist_ok=True)

                    try:
                        # DEBUG: состояние df перед save
                        null_count = df.isna().sum().sum()
                        logger.info(
                            f"💾 PRE-SAVE: processed={processed_count}, "
                            f"shape={df.shape}, nulls={null_count}"
                        )

                        df.to_excel(my_temp_file, index=False, engine="openpyxl")

                        # ✅ ПРОВЕРЯЕМ записанный файл!
                        test_df = pd.read_excel(my_temp_file)
                        test_nulls = test_df.isna().sum().sum()
                        logger.info(
                            f"💾 ✅ {my_temp_file.name}: "
                            f"shape={test_df.shape}, nulls={test_nulls}, "
                            f"rows_saved={len(test_df)} [{processed_count}/{total_tasks}]"
                        )

                    except Exception as e:
                        logger.error(
                            f"❌ Worker-{worker_id} save FAILED: {e}", exc_info=True
                        )

                # Telegram прогресс
                if (
                    processed_count in progress_checkpoints
                    and processed_count not in sent_progress
                ):
                    percent = int(processed_count / total_tasks * 100)
                    send_telegram_process(
                        f"Прогресс: {percent}% ({processed_count}/{total_tasks})"
                    )
                    sent_progress.add(processed_count)

        except asyncio.CancelledError:
            logger.info(f"👷 Worker-{worker_id}: Cancelled")
            break
        except asyncio.TimeoutError:
            logger.error(f"👷 Worker-{worker_id}: Task timeout!")
        except Exception as e:
            logger.error(f"👷 Worker-{worker_id}: Unexpected error: {e}")
        finally:
            # Cleanup текущей итерации
            if page1:
                await safe_close_page(page1)
            if page_retry:
                await safe_close_page(page_retry)
            if pool_ctx_obj:
                pool.release_context(pool_ctx_obj)

            # ✅ ГАРАНТИРОВАННЫЙ task_done()
            if idx_brand_part is not None:
                queue.task_done()
                logger.debug(
                    f"👷 Worker-{worker_id}: task_done() для {idx if idx_brand_part else 'None'}"
                )

    # Final cleanup при выходе из while
    try:
        if proxy_context:
            await proxy_context.close()
            logger.info(f"👷 Worker-{worker_id}: Proxy closed")
    except Exception as e:
        logger.error(f"👷 Worker-{worker_id} final cleanup error: {e}")


# async def main_async():
#     print("🚀 main.py ЗАПУЩЕН!")
#     print(
#         f"🔍 .env ДО reload: NAME={os.getenv('ENABLE_NAME_PARSING')}, WEIGHT={os.getenv('ENABLE_WEIGHT_PARSING')}"
#     )

#     reload_config()

#     # 🆕 ЛОКАЛЬНЫЕ КОПИИ — работают ВЕЗДЕ!
#     from config import (
#         INPUT_FILE,
#         TEMP_FILE,
#         TEMP_RAW,
#         MAX_ROWS,
#         MAX_WORKERS,
#         INPUT_COL_BRAND,
#         INPUT_COL_ARTICLE,
#         get_output_file,
#         stparts_price,
#         stparts_delivery,
#         avtoformula_price,
#         avtoformula_delivery,
#         ENABLE_WEIGHT_PARSING as LOCAL_WEIGHT,
#         ENABLE_NAME_PARSING as LOCAL_NAME,
#         ENABLE_PRICE_PARSING as LOCAL_PRICE,
#         JPARTS_P_W,
#         JPARTS_V_W,
#         ARMTEK_P_W,
#         ARMTEK_V_W,
#         BAD_DETAIL_NAMES,
#     )

#     # Проверка: только 1 режим активен
#     active_modes = sum([LOCAL_WEIGHT, LOCAL_NAME, LOCAL_PRICE])
#     if active_modes != 1:
#         error_msg = f"❌ Ошибка: 1 режим! ИМЕНА={LOCAL_NAME}, ВЕСА={LOCAL_WEIGHT}, ЦЕНЫ={LOCAL_PRICE}"
#         logger.error(error_msg)
#         return

#     # Режим
#     if LOCAL_WEIGHT:
#         mode = "ВЕСА"
#     elif LOCAL_NAME:
#         mode = "ИМЕНА"
#     else:
#         mode = "ЦЕНЫ"

#     logger.info(f"✅ Режим: {mode}")
#     logger.info("=" * 60)

#     # 📊 Загрузка и подготовка DataFrame
#     df = pd.read_excel(INPUT_FILE)
#     df = preprocess_dataframe(df)

#     # 🆕 Инициализация колонок
#     for col in [
#         stparts_price,
#         stparts_delivery,
#         avtoformula_price,
#         avtoformula_delivery,
#     ]:
#         if col not in df.columns:
#             df[col] = None

#     if LOCAL_NAME and "finde_name" not in df.columns:
#         df["finde_name"] = None

#     if LOCAL_WEIGHT:
#         for col in [JPARTS_P_W, JPARTS_V_W, ARMTEK_P_W, ARMTEK_V_W]:
#             if col not in df.columns:
#                 df[col] = None

#     # 🆕 Создание очереди задач
#     queue = asyncio.Queue()
#     total_tasks = 0

#     for idx, row in df.head(MAX_ROWS).iterrows():
#         article = str(row[INPUT_COL_ARTICLE]).strip()
#         if article:
#             task = (idx, str(row[INPUT_COL_BRAND]).strip(), article)
#             queue.put_nowait(task)
#             total_tasks += 1

#     logger.info(f"📋 Задач в очереди: {total_tasks}")

#     # 🆕 Контрольные точки прогресса
#     progress_checkpoints = {
#         math.ceil(total_tasks * 0.25),
#         math.ceil(total_tasks * 0.50),
#         math.ceil(total_tasks * 0.75),
#         total_tasks,
#     }
#     sent_progress = set()
#     counter = {"processed": 0}
#     counter_lock = asyncio.Lock()

#     async with async_playwright() as p:
#         # 🆕 BROWSER #1: ContextPool (БЕЗ proxy) — ОСТАЕТСЯ
#         normal_browser = await p.chromium.launch(
#             headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
#         )

#         # 2️⃣ PROXY browser (Firefox + per-context)
#         proxy_browser = await p.chromium.launch(
#             headless=True,
#             args=["--no-sandbox", "--disable-dev-shm-usage"],
#             proxy={"server": "http://per-context"},
#         )

#         # ContextPool использует normal_browser (БЕЗ proxy ошибок!)
#         pool = ContextPool(
#             normal_browser,  # ← КРИТИЧНО!
#             pool_size=MAX_WORKERS,
#             auth_avtoformula=LOCAL_NAME or LOCAL_PRICE,
#         )
#         await pool.initialize()

#         with tqdm(total=total_tasks, desc="Парсинг") as pbar:
#             # 🆕 Workers получают ОБОИХ браузеров!
#             workers = [
#                 asyncio.create_task(
#                     worker(
#                         i,
#                         queue,
#                         pool,
#                         normal_browser,
#                         proxy_browser,
#                         df,
#                         pbar,
#                         total_tasks,
#                         progress_checkpoints,
#                         sent_progress,
#                         counter,
#                         counter_lock,
#                     )
#                 )
#                 for i in range(MAX_WORKERS)
#             ]

#             # Вместо await queue.join():
#             while not queue.empty():
#                 if Path("input/STOP.flag").exists():
#                     logger.warning("🛑 GLOBAL STOP!")
#                     for w in workers:
#                         w.cancel()
#                     await asyncio.gather(*workers, return_exceptions=True)

#                     # 🆕 Исправленная финализация при STOP
#                     try:
#                         # Загружаем TEMP с preprocess
#                         if Path(TEMP_FILE).exists():
#                             df_final = pd.read_excel(TEMP_FILE)
#                             df_final = preprocess_dataframe(df_final)  # 🆕 КРИТИЧНО!
#                             await finalize_processing(df_final, mode)
#                         else:
#                             logger.warning("⚠️ TEMP_FILE не найден при STOP")
#                     except Exception as e:
#                         logger.error(f"❌ Финализация при STOP failed: {e}")
#                     return  # ← Выход!

#                 await asyncio.sleep(0.1)  # Лёгкая проверка

#             # Ждём завершения ВСЕХ задач
#             await queue.join()
#             logger.info("✅ Очередь задач завершена!")

#             # Graceful shutdown воркеров
#             for w in workers:
#                 w.cancel()
#             await asyncio.gather(*workers, return_exceptions=True)

#         # 🆕 ЕДИНАЯ ФИНАЛИЗАЦИЯ через функцию!
#     try:
#         await finalize_processing(df, mode)  # Автоматически определит режим и сохранит!
#     except Exception as e:
#         logger.error(f"❌ Финальная обработка: {e}")

#         # 🧹 Cleanup ОБОИХ браузеров
#         await pool.close_all()
#         await normal_browser.close()
#         await proxy_browser.close()
#         logger.info("🎉 Завершено!")


# async def main_async():
#     print("🚀 main.py ЗАПУЩЕН!")
#     print(
#         f"🔍 .env ДО reload: NAME={os.getenv('ENABLE_NAME_PARSING')}, WEIGHT={os.getenv('ENABLE_WEIGHT_PARSING')}"
#     )

#     reload_config()
#     TEMP_FILES_DIR.mkdir(parents=True, exist_ok=True)

#     # 🆕 ЛОКАЛЬНЫЕ КОПИИ — работают ВЕЗДЕ!
#     from config import (
#         INPUT_FILE,
#         MAX_ROWS,
#         MAX_WORKERS,
#         INPUT_COL_BRAND,
#         INPUT_COL_ARTICLE,
#         get_output_file,
#         stparts_price,
#         stparts_delivery,
#         avtoformula_price,
#         avtoformula_delivery,
#         ENABLE_WEIGHT_PARSING as LOCAL_WEIGHT,
#         ENABLE_NAME_PARSING as LOCAL_NAME,
#         ENABLE_PRICE_PARSING as LOCAL_PRICE,
#         JPARTS_P_W,
#         JPARTS_V_W,
#         ARMTEK_P_W,
#         ARMTEK_V_W,
#         BAD_DETAIL_NAMES,
#     )

#     # Проверка: только 1 режим активен
#     active_modes = sum([LOCAL_WEIGHT, LOCAL_NAME, LOCAL_PRICE])
#     if active_modes != 1:
#         error_msg = f"❌ Ошибка: 1 режим! ИМЕНА={LOCAL_NAME}, ВЕСА={LOCAL_WEIGHT}, ЦЕНЫ={LOCAL_PRICE}"
#         logger.error(error_msg)
#         return

#     # Режим
#     if LOCAL_WEIGHT:
#         mode = "ВЕСА"
#     elif LOCAL_NAME:
#         mode = "ИМЕНА"
#     else:
#         mode = "ЦЕНЫ"

#     logger.info(f"✅ Режим: {mode}")
#     logger.info("=" * 60)

#     # 📊 Загрузка и подготовка DataFrame
#     df = pd.read_excel(INPUT_FILE)
#     df = preprocess_dataframe(df)

#     # 🆕 Инициализация колонок
#     for col in [
#         stparts_price,
#         stparts_delivery,
#         avtoformula_price,
#         avtoformula_delivery,
#     ]:
#         if col not in df.columns:
#             df[col] = None

#     if LOCAL_NAME and "finde_name" not in df.columns:
#         df["finde_name"] = None

#     if LOCAL_WEIGHT:
#         for col in [JPARTS_P_W, JPARTS_V_W, ARMTEK_P_W, ARMTEK_V_W]:
#             if col not in df.columns:
#                 df[col] = None

#     # 🆕 Создание очереди задач
#     queue = asyncio.Queue()
#     total_tasks = 0

#     for idx, row in df.head(MAX_ROWS).iterrows():
#         article = str(row[INPUT_COL_ARTICLE]).strip()
#         if article:
#             task = (idx, str(row[INPUT_COL_BRAND]).strip(), article)
#             queue.put_nowait(task)
#             total_tasks += 1

#     logger.info(f"📋 Задач в очереди: {total_tasks}")

#     # 🆕 Контрольные точки прогресса
#     progress_checkpoints = {
#         math.ceil(total_tasks * 0.25),
#         math.ceil(total_tasks * 0.50),
#         math.ceil(total_tasks * 0.75),
#         total_tasks,
#     }
#     sent_progress = set()
#     counter = {"processed": 0}
#     counter_lock = asyncio.Lock()

#     async with async_playwright() as p:
#         # 🆕 BROWSER #1: ContextPool (БЕЗ proxy)
#         normal_browser = await p.chromium.launch(
#             headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
#         )

#         # 2️⃣ PROXY browser
#         proxy_browser = await p.chromium.launch(
#             headless=True,
#             args=["--no-sandbox", "--disable-dev-shm-usage"],
#             proxy={"server": "http://per-context"},
#         )

#         # ContextPool
#         pool = ContextPool(
#             normal_browser,
#             pool_size=MAX_WORKERS,
#             auth_avtoformula=LOCAL_NAME or LOCAL_PRICE,
#         )
#         await pool.initialize()

#         with tqdm(total=total_tasks, desc="Парсинг") as pbar:
#             workers = [
#                 asyncio.create_task(
#                     worker(
#                         i,
#                         queue,
#                         pool,
#                         normal_browser,
#                         proxy_browser,
#                         df,
#                         pbar,
#                         total_tasks,
#                         progress_checkpoints,
#                         sent_progress,
#                         counter,
#                         counter_lock,
#                     )
#                 )
#                 for i in range(MAX_WORKERS)
#             ]

#             # 🔥 ✅ ОСНОВНОЙ ЦИКЛ: проверка STOP + join()
#             while True:
#                 # Проверяем STOP каждые 0.5с
#                 if Path("input/STOP.flag").exists():
#                     logger.warning("🛑 GLOBAL STOP!")
#                     for w in workers:
#                         w.cancel()
#                     await asyncio.gather(*workers, return_exceptions=True)

#                     # 🔥 ПРОСТО: merge + finalize_processing (всё сделает сама!)
#                     try:
#                         df_final = await merge_temp_files(
#                             df, mode
#                         )  # Merge всех worker файлов
#                         await finalize_processing(df_final, mode)  # ✅ Автоматически:
#                         # - Инициализирует колонки
#                         # - consolidate_weights() если WEIGHT
#                         # - adjust_prices_and_save() если PRICE
#                         # - Сохранит + Telegram
#                     except Exception as e:
#                         logger.error(f"❌ STOP merge/финал failed: {e}")
#                     break

#                 # Проверяем очередь: если пуста — join()
#                 if queue.empty():
#                     logger.info("Очередь пуста, ждём queue.join()...")
#                     try:
#                         await asyncio.wait_for(
#                             queue.join(), timeout=30.0
#                         )  # Timeout на всякий случай
#                         logger.info("✅ queue.join() завершён!")
#                         break
#                     except asyncio.TimeoutError:
#                         logger.warning("⚠️ queue.join() timeout — проверяем workers...")
#                         # Если join() не сработал — форсируем выход
#                         break
#                 else:
#                     await asyncio.sleep(0.5)  # Лёгкая проверка

#             # Graceful shutdown workers (poison pills)
#             logger.info("🛑 Отправляем poison pills...")
#             for _ in range(len(workers)):
#                 await queue.put(None)

#             # Ждём завершения workers
#             for w in workers:
#                 w.cancel()
#             await asyncio.gather(*workers, return_exceptions=True)
#             logger.info("✅ Все workers завершены!")

#         # 🧹 Cleanup браузеров
#         await pool.close_all()
#         await normal_browser.close()
#         await proxy_browser.close()

#     # 🔥 ✅ ФИНАЛИЗАЦИЯ (всегда выполняется!)
#     try:
#         logger.info(f"🔄 Финализация ({mode})...")
#         df_final = await merge_temp_files(df, mode)  # Автоматически всё merge'ит!
#         await finalize_processing(df, mode)
#         logger.info("🎉 Парсинг завершён успешно!")
#     except Exception as e:
#         logger.error(f"❌ Финальная обработка failed: {e}")
#         # Emergency save
#         emergency_file = get_output_file(mode).replace(".xlsx", "_emergency.xlsx")
#         await asyncio.to_thread(df.to_excel, emergency_file, index=False)
#         logger.info(f"💾 Emergency save: {emergency_file}")


async def main_async():
    print("🚀 main.py ЗАПУЩЕН!")
    print(
        f"🔍 .env ДО reload: NAME={os.getenv('ENABLE_NAME_PARSING')}, WEIGHT={os.getenv('ENABLE_WEIGHT_PARSING')}"
    )

    reload_config()
    TEMP_FILES_DIR.mkdir(parents=True, exist_ok=True)

    # 🆕 ЛОКАЛЬНЫЕ КОПИИ — работают ВЕЗДЕ!
    from config import (
        INPUT_FILE,
        MAX_ROWS,
        MAX_WORKERS,
        INPUT_COL_BRAND,
        INPUT_COL_ARTICLE,
        get_output_file,
        stparts_price,
        stparts_delivery,
        avtoformula_price,
        avtoformula_delivery,
        ENABLE_WEIGHT_PARSING as LOCAL_WEIGHT,
        ENABLE_NAME_PARSING as LOCAL_NAME,
        ENABLE_PRICE_PARSING as LOCAL_PRICE,
        JPARTS_P_W,
        JPARTS_V_W,
        ARMTEK_P_W,
        ARMTEK_V_W,
        BAD_DETAIL_NAMES,
    )

    # Проверка: только 1 режим активен
    active_modes = sum([LOCAL_WEIGHT, LOCAL_NAME, LOCAL_PRICE])
    if active_modes != 1:
        error_msg = f"❌ Ошибка: 1 режим! ИМЕНА={LOCAL_NAME}, ВЕСА={LOCAL_WEIGHT}, ЦЕНЫ={LOCAL_PRICE}"
        logger.error(error_msg)
        return

    # Режим
    if LOCAL_WEIGHT:
        mode = "ВЕСА"
    elif LOCAL_NAME:
        mode = "ИМЕНА"
    else:
        mode = "ЦЕНЫ"

    logger.info(f"✅ Режим: {mode}")
    logger.info("=" * 60)

    # 📊 Загрузка и подготовка DataFrame
    df = pd.read_excel(INPUT_FILE)
    df = preprocess_dataframe(df)

    # 🆕 Инициализация колонок
    for col in [
        stparts_price,
        stparts_delivery,
        avtoformula_price,
        avtoformula_delivery,
    ]:
        if col not in df.columns:
            df[col] = None

    if LOCAL_NAME and "finde_name" not in df.columns:
        df["finde_name"] = None

    if LOCAL_WEIGHT:
        for col in [JPARTS_P_W, JPARTS_V_W, ARMTEK_P_W, ARMTEK_V_W]:
            if col not in df.columns:
                df[col] = None

    # 🆕 Создание очереди задач
    queue = asyncio.Queue()
    total_tasks = 0

    for idx, row in df.head(MAX_ROWS).iterrows():
        article = str(row[INPUT_COL_ARTICLE]).strip()
        if article:
            task = (idx, str(row[INPUT_COL_BRAND]).strip(), article)
            queue.put_nowait(task)
            total_tasks += 1

    logger.info(f"📋 Задач в очереди: {total_tasks}")

    # 🆕 Контрольные точки прогресса
    progress_checkpoints = {
        math.ceil(total_tasks * 0.25),
        math.ceil(total_tasks * 0.50),
        math.ceil(total_tasks * 0.75),
        total_tasks,
    }
    sent_progress = set()
    counter = {"processed": 0}
    counter_lock = asyncio.Lock()

    # 🔥 🆕 ИСПРАВЛЕННЫЙ БЛОК: try-finally вместо async with
    playwright = None
    normal_browser = None
    proxy_browser = None
    pool = None

    try:
        playwright = await async_playwright().start()

        # 🆕 BROWSER #1: ContextPool (БЕЗ proxy)
        normal_browser = await playwright.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        # 2️⃣ PROXY browser
        proxy_browser = await playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
            proxy={"server": "http://per-context"},
        )

        # ContextPool
        pool = ContextPool(
            normal_browser,
            pool_size=MAX_WORKERS,
            auth_avtoformula=LOCAL_NAME or LOCAL_PRICE,
        )
        await pool.initialize()

        with tqdm(total=total_tasks, desc="Парсинг") as pbar:
            workers = [
                asyncio.create_task(
                    worker(
                        i,
                        queue,
                        pool,
                        normal_browser,
                        proxy_browser,
                        df,
                        pbar,
                        total_tasks,
                        progress_checkpoints,
                        sent_progress,
                        counter,
                        counter_lock,
                    )
                )
                for i in range(MAX_WORKERS)
            ]

            # 🔥 ✅ ОСНОВНОЙ ЦИКЛ: проверка STOP + join()
            while True:
                # Проверяем STOP каждые 0.5с
                if Path("input/STOP.flag").exists():
                    logger.warning("🛑 GLOBAL STOP!")
                    for w in workers:
                        w.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)

                    # 🔥 ПРОСТО: merge + finalize_processing (всё сделает сама!)
                    try:
                        df_final = await merge_temp_files(
                            df, mode
                        )  # Merge всех worker файлов
                        await finalize_processing(df_final, mode)  # ✅ Автоматически:
                        # - Инициализирует колонки
                        # - consolidate_weights() если WEIGHT
                        # - adjust_prices_and_save() если PRICE
                        # - Сохранит + Telegram
                    except Exception as e:
                        logger.error(f"❌ STOP merge/финал failed: {e}")
                    break

                # Проверяем очередь: если пуста — join()
                if queue.empty():
                    logger.info("Очередь пуста, ждём queue.join()...")
                    try:
                        await asyncio.wait_for(
                            queue.join(), timeout=30.0
                        )  # Timeout на всякий случай
                        logger.info("✅ queue.join() завершён!")
                        break
                    except asyncio.TimeoutError:
                        logger.warning("⚠️ queue.join() timeout — проверяем workers...")
                        # Если join() не сработал — форсируем выход
                        break
                else:
                    await asyncio.sleep(0.5)  # Лёгкая проверка

            # Graceful shutdown workers (poison pills)
            logger.info("🛑 Отправляем poison pills...")
            for _ in range(len(workers)):
                await queue.put(None)

            # Ждём завершения workers
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            logger.info("✅ Все workers завершены!")

    except Exception as e:
        logger.error(f"❌ Критическая ошибка в main: {e}")
        raise
    finally:
        # 🧹 Graceful cleanup ВСЕГДА
        logger.info("🧹 Cleanup браузеров...")
        try:
            if pool:
                await pool.close_all()
                logger.info("✅ Pool закрыт")
        except Exception as e:
            logger.warning(f"⚠️ Pool close error: {e}")

        try:
            if normal_browser:
                await normal_browser.close()
                logger.info("✅ Normal browser закрыт")
        except Exception as e:
            logger.warning(f"⚠️ Normal browser close error: {e}")

        try:
            if proxy_browser:
                await proxy_browser.close()
                logger.info("✅ Proxy browser закрыт")
        except Exception as e:
            logger.warning(f"⚠️ Proxy browser close error: {e}")

        try:
            if playwright:
                await playwright.stop()
                logger.info("✅ Playwright остановлен")
        except Exception as e:
            logger.warning(f"⚠️ Playwright stop error: {e}")

    # 🔥 ✅ ФИНАЛИЗАЦИЯ (всегда выполняется!)
    try:
        logger.info(f"🔄 Финализация ({mode})...")
        df_final = await merge_temp_files(df, mode)  # Автоматически всё merge'ит!
        await finalize_processing(df, mode)
        logger.info("🎉 Парсинг завершён успешно!")
    except Exception as e:
        logger.error(f"❌ Финальная обработка failed: {e}")
        # Emergency save
        emergency_file = get_output_file(mode).replace(".xlsx", "_emergency.xlsx")
        await asyncio.to_thread(df.to_excel, emergency_file, index=False)
        logger.info(f"💾 Emergency save: {emergency_file}")


def main():
    setup_event_loop_policy()
    clear_debug_folders_sync(sites, logger)

    def stop_handler(signum, frame):
        stop_parsing.set()

    signal.signal(signal.SIGTERM, stop_handler)

    asyncio.run(main_async())


if __name__ == "__main__":
    main()

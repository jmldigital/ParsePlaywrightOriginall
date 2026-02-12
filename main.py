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
from captcha_manager import CaptchaManager

captcha_manager = CaptchaManager()

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
    SELECTORS,
    DELAY_EXIST,
)

from utils import (
    logger,
    preprocess_dataframe,
    consolidate_weights,
    clear_debug_folders_sync,
    get_2captcha_proxy,
    get_site_logger,
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
    """Финальная обработка + сохранение (normal/extreme stop)"""
    logger.info(f"🔄 Финализация ({mode})...")

    # 🔥 Импорт констант ОДИН РАЗ!
    from config import (
        ENABLE_WEIGHT_PARSING,
        ENABLE_PRICE_PARSING,
        ENABLE_NAME_PARSING,
        stparts_price,
        stparts_delivery,
        avtoformula_price,
        avtoformula_delivery,
    )

    # Локальные копии
    local_weight = ENABLE_WEIGHT_PARSING
    local_price = ENABLE_PRICE_PARSING
    local_name = ENABLE_NAME_PARSING

    logger.info(
        f"🔧 Режимы: weight={local_weight}, price={local_price}, name={local_name}"
    )

    if df is None or df.empty:
        logger.error("❌ DataFrame пустой или None!")
        return

    try:
        # Инициализация колонок ПО РЕЖИМУ
        price_cols = [
            stparts_price,
            stparts_delivery,
            avtoformula_price,
            avtoformula_delivery,
        ]
        for col in price_cols:
            if col not in df.columns:
                df[col] = None

        if local_name and "finde_name" not in df.columns:
            df["finde_name"] = None

        # 🧹 Drop лишнего для ИМЕНА
        if local_name:
            cols_to_drop = price_cols
            existing_cols = [col for col in cols_to_drop if col in df.columns]
            if existing_cols:
                logger.info(f"🧹 ИМЕНА: удаляем {len(existing_cols)} лишних колонок")
                df.drop(columns=existing_cols, inplace=True)

        # Получаем output_file
        if not output_file:
            output_file = get_output_file(mode)
            if not output_file:
                raise ValueError(f"❌ output_file не найден для {mode}")

        logger.info(f"💾 Финальный файл: {output_file}")

        # Сохранение ПО РЕЖИМУ
        if local_price:
            await asyncio.to_thread(adjust_prices_and_save, df, output_file)
            logger.info("✅ Цены скорректированы и сохранены")
        else:
            await asyncio.to_thread(df.to_excel, output_file, index=False)
            logger.info("✅ Обычное сохранение Excel")

        # ✅ Отправка нормального файла
        await send_telegram_file(output_file, f"✅ {mode} готово! ({len(df)} строк)")
        logger.info("🎉 Финализация завершена!")

    except Exception as e:
        logger.error(f"❌ Финальная обработка FAILED: {e}", exc_info=True)

        # 🆕 Emergency save (ИСПРАВЛЕНО .xlsx → _emergency.xlsx)
        emergency_file = output_file.replace(".xlsx", "_emergency.xlsx")
        try:
            await asyncio.to_thread(df.to_excel, emergency_file, index=False)
            logger.info(f"💾 Emergency: {emergency_file} ({len(df)} строк)")
            await send_telegram_file(
                emergency_file, f"⚠️ {mode} EMERGENCY ({len(df)} строк)"
            )
        except Exception as e2:
            logger.error(f"❌ Emergency save тоже упал: {e2}")


# async def finalize_processing(df: pd.DataFrame, mode: str, output_file: str = None):
#     """Финальная обработка + сохранение с ПОЛНОЙ отладкой"""
#     logger.info(f"🔄 Финализация ({mode})...")
#     logger.info(f"📊 df.shape ВХОД: {df.shape}")

#     # СКРИН 1 — ВХОД
#     from pathlib import Path

#     output_dir = Path("output")
#     output_dir.mkdir(exist_ok=True)
#     debug1 = output_dir / f"finalize_1_input_{mode}.xlsx"
#     await asyncio.to_thread(df.to_excel, debug1)
#     logger.info(f"💾 Шаг1: {debug1}")

#     # 🆕 ЛОКАЛЬНЫЕ КОПИИ!
#     local_weight = ENABLE_WEIGHT_PARSING
#     local_price = ENABLE_PRICE_PARSING
#     local_name = ENABLE_NAME_PARSING

#     logger.info(
#         f"🔧 Режимы: weight={local_weight}, price={local_price}, name={local_name}"
#     )

#     if df is None or df.empty:
#         logger.error("❌ DataFrame пустой!")
#         return

#     try:
#         from config import (
#             stparts_price,
#             stparts_delivery,
#             avtoformula_price,
#             avtoformula_delivery,
#             JPARTS_P_W,
#             JPARTS_V_W,
#             ARMTEK_P_W,
#             ARMTEK_V_W,
#         )

#         # Статистика ДО
#         logger.info(f"📊 ДО init колонок:")
#         logger.info(f"  JP_Phys: {df[JPARTS_P_W].notna().sum()}")
#         logger.info(f"  ARM_Phys: {df[ARMTEK_P_W].notna().sum()}")

#         # Инициализация колонок
#         for col in [
#             stparts_price,
#             stparts_delivery,
#             avtoformula_price,
#             avtoformula_delivery,
#         ]:
#             if col not in df.columns:
#                 df[col] = None

#         if local_weight:
#             for col in [JPARTS_P_W, JPARTS_V_W, ARMTEK_P_W, ARMTEK_V_W]:
#                 if col not in df.columns:
#                     df[col] = None

#         if local_name and "finde_name" not in df.columns:
#             df["finde_name"] = None

#         # # СКРИН 2 — ПОСЛЕ init колонок
#         # debug2 = output_dir / f"finalize_2_init_cols_{mode}.xlsx"
#         # await asyncio.to_thread(df.to_excel, debug2)
#         # logger.info(f"💾 Шаг2: {debug2}")

#         if local_weight:
#             logger.info("🔄 consolidate_weights...")
#             df = await asyncio.to_thread(consolidate_weights, df)
#             logger.info("✅ Веса консолидированы")

#             # # СКРИН 3 — ПОСЛЕ consolidate
#             # debug3 = output_dir / f"finalize_3_consolidate_{mode}.xlsx"
#             # await asyncio.to_thread(df.to_excel, debug3)
#             # logger.info(f"💾 Шаг3: {debug3}")

#         # output_file
#         if not output_file:
#             output_file = get_output_file(mode)
#             if not output_file:
#                 raise ValueError(f"Нет output_file для {mode}")

#         logger.info(f"💾 Финал: {output_file}")

#         # # СКРИН 4 — ПЕРЕД сохранением
#         # debug4 = output_dir / f"finalize_4_pre_save_{mode}.xlsx"
#         # await asyncio.to_thread(df.to_excel, debug4)
#         # logger.info(f"💾 Шаг4: {debug4}")

#         if local_price:
#             logger.info("🔄 adjust_prices_and_save...")
#             await asyncio.to_thread(
#                 adjust_prices_and_save, df.copy(), output_file
#             )  # copy!
#         else:
#             logger.info("🔄 to_excel...")
#             await asyncio.to_thread(df.to_excel, output_file, index=False)

#         # # СКРИН 5 — ПОСЛЕ сохранения (проверка)
#         # logger.info(f"✅ Сохранено: {output_file}")
#         # await send_telegram_file(output_file, f"✅ {mode} завершены!")

#     except Exception as e:
#         logger.error(f"❌ Ошибка финализации: {e}", exc_info=True)
#         emergency_file = str(output_file).replace(".xlsx", "_emergency.xlsx")
#         try:
#             await asyncio.to_thread(df.to_excel, emergency_file, index=False)
#             logger.info(f"💾 Emergency: {emergency_file}")
#             await send_telegram_file(emergency_file, f"⚠️ {mode} emergency")
#         except Exception as e2:
#             logger.error(f"❌ Emergency failed: {e2}")


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


async def process_single_item(
    context,
    idx: int,
    brand: str,
    part: str,
):
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
    #     # 🧪 Симуляция прокси-циклов
    #     if not hasattr(process_single_item, "proxy_cycle"):
    #         process_single_item.proxy_cycle = {"count": 0, "phase": 0}

    #     cycle = process_single_item.proxy_cycle
    #     cycle["count"] += 1

    #     logger.info(
    #         f"🚀 [{idx}] ТЕСТ цикл {cycle['count']}/phase{cycle['phase']}: {part}"
    #     )

    #     # ✅ ТОЛЬКО new_page() БЕЗ параметров:
    #     page1 = await context.new_page()  # ✅

    #     # Human-like!
    #     await page1.add_init_script(
    #         """
    #         Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    #         Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru']});
    #     """
    #     )
    #     await page1.goto("about:blank")
    #     await page1.wait_for_timeout(2000)  # "Просмотр главной"

    #     try:
    #         # ARMTEK
    #         armtek_physical, armtek_volumetric = await asyncio.wait_for(
    #             scrape_weight_armtek(page1, part, logger_armtek),
    #             timeout=120.0,  # 2 минуты на Cloudflare!
    #         )
    #         logger.info(
    #             f"🔍 [{idx}] ARMTEK: phys={armtek_physical}, vol={armtek_volumetric}"
    #         )

    #     except Exception as e:
    #         logger.error(f"❌ [{idx}] ARMTEK: {e}")
    #         armtek_physical = armtek_volumetric = None

    #     # 🔥 КАПЧА!
    #     if armtek_physical == "NeedCaptcha":
    #         logger.info(f"🔒 [{idx}] Капча ARMTEK")
    #         success = await captcha_manager.solve_captcha(
    #             page=page1,
    #             logger=logger_armtek,
    #             site_key="armtek",
    #             selectors=SELECTORS.get("armtek", {}),
    #         )
    #         await safe_close_page(page1)

    #         if success:
    #             logger.info(f"🔓 [{idx}] Капча OK")
    #             # Можно retry, но для теста продолжаем
    #         else:
    #             logger.warning(f"❌ [{idx}] Капча fail")
    #             result.update({ARMTEK_P_W: None, ARMTEK_V_W: None})
    #     else:
    #         await safe_close_page(page1)

    #     # 🔥 ПРОКСИ-ЦИКЛЫ (по 10)
    #     if cycle["count"] <= 10:  # 1: без прокси
    #         logger.info(f"📡 [{idx}] ФАЗА 1: без прокси")

    #     elif cycle["count"] <= 20:  # 2: прокси 1
    #         logger.warning(f"🚦 [{idx}] ФАЗА 2: NeedProxy 1")
    #         cycle["phase"] = 1
    #         await safe_close_page(page1)
    #         return "NeedProxy"  # Worker → proxy!

    #     elif cycle["count"] <= 30:  # 3: прокси 2
    #         logger.warning(f"🚦 [{idx}] ФАЗА 3: NeedProxy 2")
    #         cycle["phase"] = 2
    #         await safe_close_page(page1)
    #         return "NeedProxy"

    #     else:  # 4+: прокси 2
    #         logger.info(f"📡 [{idx}] ФАЗА 4: прокси 2")

    #     # ✅ Запись в result (как оригинал)
    #     result.update(
    #         {
    #             JPARTS_P_W: None,
    #             JPARTS_V_W: None,
    #             ARMTEK_P_W: armtek_physical,
    #             ARMTEK_V_W: armtek_volumetric,
    #         }
    #     )

    #     logger.info(f"📊 [{idx}] Записано: ARMTEK_P_W={armtek_physical}")
    #     # НЕ return — result глобальный!

    # ======================= WEIGHT =======================

    if WEIGHT:
        max_retries = 2

        for attempt in range(max_retries + 1):
            # page1 = None

            try:
                # 🆕 Новая страница каждый retry
                page1 = await context.new_page()

                jp_physical, jp_volumetric = None, None
                armtek_physical, armtek_volumetric = None, None

                # 1️⃣ Japarts (первый приоритет)
                jp_physical, jp_volumetric = await scrape_weight_japarts(
                    page1, part, logger_jp
                )

                # 🆕 Проверка капчи Japarts
                if jp_physical == "NeedCaptcha" or jp_volumetric == "NeedCaptcha":
                    logger.info(
                        f"🔒 [{idx}] Капча на japarts (попытка {attempt+1}/{max_retries+1})"
                    )
                    success = await captcha_manager.solve_captcha(
                        page=page1,
                        logger=logger_jp,
                        site_key="japarts",
                        selectors={
                            "captcha_img": SELECTORS.get("japarts", {}).get(
                                "captcha_img"
                            ),
                            "captcha_input": SELECTORS.get("japarts", {}).get(
                                "captcha_input"
                            ),
                            "captcha_submit": SELECTORS.get("japarts", {}).get(
                                "captcha_submit"
                            ),
                        },
                    )

                    await safe_close_page(page1)
                    page1 = None

                    if success:
                        continue  # Retry
                    else:
                        return "CaptchaFailed"

                # 2️⃣ Armtek ТОЛЬКО при Japarts fail
                if not jp_physical or not jp_volumetric:
                    logger.info(f"🚀 [{idx}] Japarts fail → ARMTEK: {part}")

                    armtek_physical, armtek_volumetric = await scrape_weight_armtek(
                        page1, part, logger_armtek
                    )

                    # 🆕 Проверка капчи Armtek
                    if (
                        armtek_physical == "NeedCaptcha"
                        or armtek_volumetric == "NeedCaptcha"
                    ):
                        logger.info(
                            f"🔒 [{idx}] Капча на armtek (попытка {attempt+1}/{max_retries+1})"
                        )
                        success = await captcha_manager.solve_captcha(
                            page=page1,
                            logger=logger_armtek,
                            site_key="armtek",
                            selectors={
                                "captcha_img": SELECTORS.get("armtek", {}).get(
                                    "captcha_img"
                                ),
                                "captcha_input": SELECTORS.get("armtek", {}).get(
                                    "captcha_input"
                                ),
                                "captcha_submit": SELECTORS.get("armtek", {}).get(
                                    "captcha_submit"
                                ),
                            },
                        )

                        await safe_close_page(page1)
                        page1 = None

                        if success:
                            continue  # Retry
                        else:
                            return "CaptchaFailed"

                    # 🚨 RateLimit (остается как есть)
                    if armtek_physical == "NeedProxy":
                        logger.info(f"🎯 [{idx}] RateLimit → NeedProxy!")
                        await safe_close_page(page1)
                        return "NeedProxy"

                    # ✅ Armtek результат
                    result.update(
                        {
                            JPARTS_P_W: jp_physical,
                            JPARTS_V_W: jp_volumetric,
                            ARMTEK_P_W: armtek_physical,
                            ARMTEK_V_W: armtek_volumetric,
                        }
                    )

                else:
                    # ✅ Только Japarts
                    result.update(
                        {
                            JPARTS_P_W: jp_physical,
                            JPARTS_V_W: jp_volumetric,
                            ARMTEK_P_W: None,
                            ARMTEK_V_W: None,
                        }
                    )

                await safe_close_page(page1)
                break  # ✅ Успех!

            except Exception as e:
                logger.error(
                    f"❌ [{idx}] Weight parse error (попытка {attempt+1}): {e}"
                )
                await safe_close_page(page1)
                if attempt < max_retries:
                    continue

            # Если все попытки исчерпаны
            result.update(
                {JPARTS_P_W: None, JPARTS_V_W: None, ARMTEK_P_W: None, ARMTEK_V_W: None}
            )

    # ======================= NAME =======================
    if NAME:
        max_retries = 2

        for attempt in range(max_retries + 1):
            # page1 = None

            try:
                page1 = await context.new_page()

                # 1) stparts
                detail_name = await scrape_stparts_name_async(page1, part, logger_st)

                if detail_name == "NeedCaptcha":
                    logger.info(
                        f"🔒 [{idx}] Капча на stparts (попытка {attempt+1}/{max_retries+1})"
                    )
                    success = await captcha_manager.solve_captcha(
                        page=page1,
                        logger=logger_st,
                        site_key="stparts",
                        selectors={
                            "captcha_img": SELECTORS["stparts"]["captcha_img"],
                            "captcha_input": SELECTORS["stparts"]["captcha_input"],
                            "captcha_submit": SELECTORS["stparts"]["captcha_submit"],
                        },
                    )

                    await safe_close_page(page1)
                    page1 = None

                    if success:
                        continue  # retry
                    else:
                        return "CaptchaFailed"

                # Если имя плохое → avtoformula
                if not detail_name or detail_name.lower().strip() in BAD_DETAIL_NAMES:
                    if detail_name:
                        logger.info(f"⚠️ [{idx}] stparts '{detail_name}' → avtoformula")

                    detail_name = await scrape_avtoformula_name_async(
                        page1, part, logger_avto
                    )

                    if detail_name == "NeedCaptcha":
                        logger.info(
                            f"🔒 [{idx}] Капча на avtoformula (попытка {attempt+1}/{max_retries+1})"
                        )
                        success = await captcha_manager.solve_captcha(
                            page=page1,
                            logger=logger_avto,
                            site_key="avtoformula",
                            selectors={
                                "captcha_img": SELECTORS["avtoformula"]["captcha_img"],
                                "captcha_input": SELECTORS["avtoformula"][
                                    "captcha_input"
                                ],
                                "captcha_submit": SELECTORS["avtoformula"][
                                    "captcha_submit"
                                ],
                            },
                        )

                        await safe_close_page(page1)
                        page1 = None

                        if success:
                            continue
                        else:
                            return "CaptchaFailed"

                if not detail_name or detail_name.lower().strip() in BAD_DETAIL_NAMES:
                    detail_name = "Detail"
                    logger.info(f"❌ [{idx}] Название не найдено: {part}")

                await safe_close_page(page1)

                result["finde_name"] = detail_name
                break

            except Exception as e:
                logger.error(f"❌ [{idx}] Name parse error (попытка {attempt+1}): {e}")
                await safe_close_page(page1)
                if attempt < max_retries:
                    continue
                result["finde_name"] = "Detail"

    # ======================= PRICE =======================
    if PRICE:
        max_retries = 2

        for attempt in range(max_retries + 1):
            page1 = None
            page2 = None

            try:
                # 🆕 свежие страницы каждый retry из контекста
                page1 = await context.new_page()  # Stparts
                page2 = await context.new_page()  # Avtoformula

                result_st, result_avto = await asyncio.gather(
                    scrape_stparts_async(page1, brand, part, logger_st),
                    scrape_avtoformula_pw(page2, brand, part, logger_avto),
                    return_exceptions=True,
                )

                # Капча
                if result_st == "NeedCaptcha" or result_avto == "NeedCaptcha":
                    site_to_solve = (
                        "stparts" if result_st == "NeedCaptcha" else "avtoformula"
                    )
                    logger_to_use = (
                        logger_st if site_to_solve == "stparts" else logger_avto
                    )
                    page_to_solve = page1 if site_to_solve == "stparts" else page2

                    logger.info(
                        f"🔒 [{idx}] Капча на {site_to_solve} (попытка {attempt+1}/{max_retries+1})"
                    )

                    success = await captcha_manager.solve_captcha(
                        page=page_to_solve,
                        logger=logger_to_use,
                        site_key=site_to_solve,
                        selectors={
                            "captcha_img": SELECTORS[site_to_solve]["captcha_img"],
                            "captcha_input": SELECTORS[site_to_solve]["captcha_input"],
                            "captcha_submit": SELECTORS[site_to_solve][
                                "captcha_submit"
                            ],
                        },
                    )

                    await safe_close_page(page1)
                    await safe_close_page(page2)
                    page1 = page2 = None

                    if success:
                        continue  # retry с новыми страницами из того же контекста
                    else:
                        return "CaptchaFailed"

                # Разлогин
                if (
                    isinstance(result_avto, Exception)
                    and "зарегистрируйтесь" in str(result_avto).lower()
                ):
                    await safe_close_page(page1)
                    await safe_close_page(page2)
                    return "ReauthNeeded"

                # Нормализация результатов
                price_st, delivery_st = (
                    result_st
                    if result_st and result_st != "NeedCaptcha"
                    else (None, None)
                )
                price_avto, delivery_avto = (
                    result_avto
                    if result_avto and result_avto != "NeedCaptcha"
                    else (None, None)
                )

                await safe_close_page(page1)
                await safe_close_page(page2)

                return idx, {
                    stparts_price: price_st,
                    stparts_delivery: delivery_st,
                    avtoformula_price: price_avto,
                    avtoformula_delivery: delivery_avto,
                }

            except Exception as e:
                logger.error(f"[{idx}] PRICE ошибка попытка {attempt+1}: {e}")
                await safe_close_page(page1)
                await safe_close_page(page2)
                if attempt < max_retries:
                    continue
                return None

    return result  # Общий return в конце


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
    retry_counts = {}

    # my_temp_file = get_temp_file(worker_id)

    while True:  # ← Изменено: while True вместо queue.empty()
        idx_brand_part = None
        # page1 = None
        # page_retry = None
        pool_ctx_obj = None

        try:
            # Получаем задачу (блокируется до получения)
            idx_brand_part = await queue.get()
            if DELAY_EXIST:
                await asyncio.sleep(random.uniform(1.5, 3.0))

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

            else:
                context = proxy_context

                logger.debug(f"👷 Worker-{worker_id}: Proxy context (Reuse)")
            # В цикле:
            try:
                # Основной парсинг
                result = await asyncio.wait_for(
                    process_single_item(context, idx, brand, part),
                    timeout=TASK_TIMEOUT,
                )

            # Если тамаут повторяем до успеха
            except asyncio.TimeoutError:
                async with counter_lock:
                    retries = retry_counts.get(idx, 0) + 1
                    retry_counts[idx] = retries

                    if retries >= 3:  # ✅ Макс 3 попытки!
                        logger.error(
                            f"👷 Worker-{worker_id}: ❌ {idx} {retries}/3 FAIL!"
                        )
                        del retry_counts[idx]
                        pbar.update(1)  # Только после FAIL
                    else:
                        delay = 2**retries * 5  # 10s, 20s, 40s
                        logger.warning(
                            f"👷 ⏰ Timeout {idx} ({retries}/3) → {delay}s retry!"
                        )
                        await asyncio.sleep(delay)
                        await queue.put((idx, brand, part))  # 🔄 Retry!
                        continue

            # Нормальный успех

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
                # await safe_close_page(page1)
                # page1 = None
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
                        # page_retry = await proxy_context.new_page()
                        result = await asyncio.wait_for(
                            process_single_item(
                                proxy_context,
                                idx,
                                brand,
                                part,
                            ),
                            timeout=PROXY_TIMOUT,
                        )
                        # await safe_close_page(page_retry)
                        # page_retry = None

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
            elif result == "CloudFlare":  # 🔥 НОВОЕ!
                logger.warning(
                    f"👷 Worker-{worker_id}: ☁️ CloudFlare на {part}. 30 сек cooldown → БЕЗ прокси!"
                )

                # Cleanup прокси
                if pool_ctx_obj:
                    pool.release_context(pool_ctx_obj)
                    pool_ctx_obj = None
                if proxy_context:
                    await proxy_context.close()
                    proxy_context = None

                # ⏳ 30 СЕКУНД PAUZA
                await asyncio.sleep(30)

                # 🔄 НОРМАЛЬНЫЙ КОНТЕКСТ (БЕЗ ПРОКСИ)
                normal_context = (
                    await normal_browser.new_context(  # Твой normal_browser
                        viewport={"width": 1920, "height": 1080},
                        locale="ru-RU",
                        timezone_id="Europe/Moscow",
                        user_agent="Mozilla/5.0...",
                    )
                )

                # Retry БЕЗ прокси
                result = await asyncio.wait_for(
                    process_single_item(normal_context, idx, brand, part),
                    timeout=120.0,  # 2 мин на нормальный
                )

                await normal_context.close()
                normal_context = None

            pbar.update(1)

            # 🆕 🔥 ПРОМЕЖУТОЧНОЕ СОХРАНЕНИЕ + DEBUG
            if result and not isinstance(result, (str, Exception)):
                async with counter_lock:
                    if isinstance(result, dict):
                        for col, val in result.items():
                            if pd.notna(val):
                                df.at[idx, col] = val
                    elif isinstance(result, tuple) and len(result) == 2:
                        real_idx, data = result
                        for col, val in data.items():
                            if pd.notna(val):
                                df.at[real_idx, col] = val

            # прогресс в телеграм
            async with counter_lock:
                counter["processed"] += 1
                processed_count = counter["processed"]

                logger.debug(
                    f"📊 Progress: {processed_count}/{total_tasks}, df.shape={df.shape}"
                )

                # Telegram прогресс (без изменений)
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
            # if page1:
            #     await safe_close_page(page1)
            # if page_retry:
            #     await safe_close_page(page_retry)
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


async def main_async():
    print("🚀 main.py ЗАПУЩЕН!")
    print(
        f"🔍 .env ДО reload: NAME={os.getenv('ENABLE_NAME_PARSING')}, WEIGHT={os.getenv('ENABLE_WEIGHT_PARSING')}"
    )

    reload_config()
    # TEMP_FILES_DIR.mkdir(parents=True, exist_ok=True)

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

    # 🔥 🔥 🔥 ВСТАВЬ ЗДЕСЬ 🔥 🔥 🔥
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

    all_possible_cols = [
        stparts_price,
        stparts_delivery,
        avtoformula_price,
        avtoformula_delivery,
        JPARTS_P_W,
        JPARTS_V_W,
        ARMTEK_P_W,
        ARMTEK_V_W,
        "finde_name",
    ]

    for col in all_possible_cols:
        if col not in df.columns:
            df[col] = None
            logger.info(f"✅ Создана колонка: {col}")

    logger.info(f"📊 DataFrame готов: {df.shape} | колонки: {list(df.columns)}")
    # 🔥 🔥 🔥 КОНЕЦ ВСТАВКИ 🔥 🔥 🔥

    # 🆕 Инициализация колонок
    # for col in [
    #     stparts_price,
    #     stparts_delivery,
    #     avtoformula_price,
    #     avtoformula_delivery,
    # ]:
    #     if col not in df.columns:
    #         df[col] = None

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

            # 🔥 ОСНОВНОЙ ЦИКЛ с промежуточным сохранением КАЖДЫЕ 10 строк!
            while True:
                async with counter_lock:
                    processed_count = counter["processed"]

                    # 🆕 ПРОВЕРКА: сохраняем только если новая отметка!
                    if (
                        processed_count % TEMP_RAW == 0
                        and processed_count > 0
                        and counter.get("last_saved", -1) != processed_count
                    ):

                        try:
                            df_current = preprocess_dataframe(df)
                            await asyncio.to_thread(
                                df_current.to_excel, TEMP_FILES_DIR, index=False
                            )
                            logger.info(
                                f"💾 Промежуточное: {processed_count}/{total_tasks} → {TEMP_FILES_DIR}"
                            )

                            # 🆕 ОТМЕЧАЕМ: эта отметка сохранена!
                            counter["last_saved"] = processed_count

                        except Exception as e:
                            logger.error(f"❌ Промежуточное: {e}")

                # Проверки
                if Path("input/STOP.flag").exists():
                    logger.warning("🛑 GLOBAL STOP!")
                    for w in workers:
                        w.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)
                    await finalize_processing(df, mode)  # ← Только 1 раз!
                    break

                if queue.empty():
                    logger.info("Очередь пуста, ждём queue.join()...")
                    try:
                        await asyncio.wait_for(queue.join(), timeout=30.0)
                        logger.info("✅ queue.join() завершён!")
                        break
                    except asyncio.TimeoutError:
                        logger.warning("⚠️ queue.join() timeout")
                        break
                else:
                    await asyncio.sleep(1.0)

            # Graceful shutdown workers (poison pills)
            # logger.info("🛑 Отправляем poison pills...")
            # for _ in range(len(workers)):
            #     await queue.put(None)

            # ✅ ДОБАВИТЬ:
            logger.info("⏳ Буфер записи df...")
            await asyncio.sleep(8)

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

    # 🔥 ФИНАЛИЗАЦИЯ ТОЛЬКО при нормальном завершении!
    if not Path("input/STOP.flag").exists():
        try:
            logger.info(f"🔄 Финализация ({mode})...")
            # 🔥 ЖЁСТКИЙ БУФЕР!
            logger.info("⏳ Ждём записи df...")
            await asyncio.sleep(5)  # Workers допишут df.at[]!

            async with counter_lock:
                logger.info(f"✅ Processed: {counter['processed']}/{total_tasks}")

            print("🔍 Последние 3 строки df:")
            print(df.tail(3)[[INPUT_COL_ARTICLE, JPARTS_P_W, ARMTEK_P_W]])

            # Перед finalize:
            logger.info(f"df.shape={df.shape}")
            logger.info(f"Веса JP: {df[JPARTS_P_W].notna().sum()}")
            logger.info(f"Веса ARM: {df[ARMTEK_P_W].notna().sum()}")

            # Сохрани debug
            # main_async перед finalize:
            output_dir = Path("output")
            output_dir.mkdir(exist_ok=True)
            debug_file = output_dir / "debug_pre_final.xlsx"
            logger.info(f"🔍 Debug в output: {debug_file}")
            await asyncio.to_thread(df.to_excel, debug_file)

            await finalize_processing(df, mode)
            logger.info("🎉 Парсинг завершён успешно!")
        except Exception as e:
            logger.error(f"❌ Финальная обработка failed: {e}")
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

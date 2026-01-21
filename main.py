# main.py
"""
Асинхронный парсер с Playwright.
- Общие куки для avtoformula
- Автоматический re-login при разлогине
- Разделённые логи по сайтам
"""
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
from config import reload_config  # ← импорт

from utils import RateLimitException, get_2captcha_proxy

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
    stparts_price,
    stparts_delivery,
    avtoformula_price,
    avtoformula_delivery,
    COOKIE_FILE,
    AVTO_LOGIN,
    AVTO_PASSWORD,
    BOT_TOKEN,
    ADMIN_CHAT_ID,
    SEND_TO_TELEGRAM,
    ARMTEK_P_W,
    ARMTEK_V_W,
    JPARTS_P_W,
    JPARTS_V_W,
    TASK_TIMEOUT,
)
from utils import (
    logger,
    preprocess_dataframe,
    consolidate_weights,
    clear_debug_folders_sync,
)
from state_manager import load_state, save_state
from price_adjuster import adjust_prices_and_save
import requests

# Импортируем асинхронные скрапперы
from scraper_avtoformula import scrape_avtoformula_pw, scrape_avtoformula_name_async
from scraper_stparts import scrape_stparts_async, scrape_stparts_name_async
from auth import ensure_logged_in


async def safe_close_page(page):
    """Безопасное закрытие страницы без ошибок"""
    if page:
        try:
            await page.close()
        except Exception:
            pass  # игнорируем ошибки закрытия


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

stop_files = ["STOP", "STOP.FLAG", "AIL_STOP"]
for f in stop_files:
    if os.path.exists(f):
        os.remove(f)
        logger.info("🧹 Удален %s", f)

logger.info("🚀 Старт без STOP флагов!")


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


async def process_row_async(
    pool: ContextPool, browser: Browser, idx: int, brand: str, part: str
):
    """
    Обрабатывает одну строку входного файла.
    Поддерживает три режима (WEIGHT / NAME / PRICE) и умеет
    переключать прокси только для armtek при Rate‑limit.
    """
    from config import (
        ENABLE_WEIGHT_PARSING as WEIGHT,
        ENABLE_NAME_PARSING as NAME,
        ENABLE_PRICE_PARSING as PRICE,
    )

    # ------------------- STOP‑флаг -------------------
    if Path("input/STOP.flag").exists():
        logger.info(f"🛑 [{idx}] STOP.flag → пропуск")
        return idx, None

    # ======================= WEIGHT =======================
    if WEIGHT:
        try:
            context = await pool.get_context()
            page = await context.new_page()

            jp_physical, jp_volumetric = None, None
            armtek_physical, armtek_volumetric = None, None
            proxy_used = False

            # Japarts
            logger.info(f"🔍 [{idx}] Japarts: {part}")
            jp_physical, jp_volumetric = await scrape_weight_japarts(
                page, part, logger_jp
            )

            # Armtek только если Japarts fail
            if not jp_physical or not jp_volumetric:
                logger.info(f"🚀 [{idx}] Japarts fail → ARMTEK: {part}")

                try:
                    armtek_physical, armtek_volumetric = await scrape_weight_armtek(
                        page, part, logger_armtek
                    )
                    logger.info(f"✅ [{idx}] Armtek OK: {part}")

                except RateLimitException:
                    logger.critical(
                        f"🎯 [{idx}] MAIN.PY ЛОВИТ RateLimitException: {part}"
                    )

                    # Закрываем старый
                    await safe_close_page(page)
                    try:
                        await context.close()
                    except:
                        pass
                    proxy_used = True

                    # ПРОКСИ
                    proxy_cfg = get_2captcha_proxy()
                    if not proxy_cfg or "server" not in proxy_cfg:
                        logger.error(f"❌ [{idx}] Нет прокси для {part}")
                    else:
                        logger.info(f"🔌 [{idx}] New proxy: {proxy_cfg['server']}")

                        proxy_ctx = await browser.new_context(
                            proxy=proxy_cfg,
                            viewport={"width": 1920, "height": 1080},
                            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                        )
                        proxy_page = await proxy_ctx.new_page()

                        try:
                            logger.info(f"🌐 [{idx}] Proxy retry: {part}")
                            armtek_physical, armtek_volumetric = (
                                await scrape_weight_armtek(
                                    proxy_page, part, logger_armtek
                                )
                            )
                            logger.info(
                                f"✅ [{idx}] PROXY SUCCESS {part}: {armtek_physical}"
                            )
                        finally:
                            await safe_close_page(proxy_page)
                            await proxy_ctx.close()

            # Результат
            result = {
                JPARTS_P_W: jp_physical,
                JPARTS_V_W: jp_volumetric,
                ARMTEK_P_W: armtek_physical,
                ARMTEK_V_W: armtek_volumetric,
            }
            logger.info(f"⚖️ [{idx}] Total {part} → {result}")
            return idx, result

        finally:
            if not proxy_used:
                await safe_close_page(page)
                try:
                    await context.new_page()
                    pool.release_context(context)
                    logger.debug(f"🔄 [{idx}] Context OK")
                except:
                    logger.debug(f"💀 [{idx}] Context dead")

    # ======================= NAME =======================
    if NAME:
        # Для названий нужен один контекст, два окна
        context = await pool.get_context()
        page1 = await context.new_page()
        page2 = await context.new_page()

        try:
            detail_name = await scrape_stparts_name_async(page1, part, logger_st)

            if not detail_name or detail_name.lower().strip() in BAD_DETAIL_NAMES:
                if detail_name:
                    logger.info(
                        f"⚠️ stparts вернул '{detail_name}' → пробуем avtoformula"
                    )
                detail_name_avto = await scrape_avtoformula_name_async(
                    page2, part, logger_avto
                )
                if (
                    detail_name_avto
                    and detail_name_avto.lower().strip() not in BAD_DETAIL_NAMES
                ):
                    detail_name = detail_name_avto
                else:
                    detail_name = "Detail"
                    logger.info(f"❌ Не удалось найти нормальное название для {part}")

            return idx, {"finde_name": detail_name}
        finally:
            await safe_close_page(page1)
            await safe_close_page(page2)
            pool.release_context(context)

    # ======================= PRICE =======================
    # Два окна (stparts + avtoformula)
    context = await pool.get_context()
    page1 = await context.new_page()
    page2 = await context.new_page()

    try:
        result_price_st, result_price_avto = await asyncio.gather(
            scrape_stparts_async(page1, brand, part, logger_st),
            scrape_avtoformula_pw(page2, brand, part, logger_avto),
            return_exceptions=True,
        )
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
    finally:
        await safe_close_page(page1)
        await safe_close_page(page2)
        pool.release_context(context)


async def main_async():
    # global ENABLE_NAME_PARSING
    # # Перечитываем .env, чтобы подхватить изменения
    # load_dotenv(override=True)

    # Считываем переменную заново
    # ENABLE_NAME_PARSING = os.getenv("ENABLE_NAME_PARSING", "False").lower() == "true"
    print("🚀 main.py ЗАПУЩЕН!")
    print(
        f"🔍 .env ДО reload: NAME={os.getenv('ENABLE_NAME_PARSING')}, WEIGHT={os.getenv('ENABLE_WEIGHT_PARSING')}"
    )

    reload_config()

    # 🆕 ЛОКАЛЬНЫЕ КОПИИ — работают ВЕЗДЕ!
    from config import (
        INPUT_FILE,
        TEMP_FILE,
        TEMP_RAW,
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
        BAD_DETAIL_NAMES,
    )

    # Проверка ЛОКАЛЬНЫХ
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

    df = pd.read_excel(INPUT_FILE)
    df = preprocess_dataframe(df)

    for col in [
        stparts_price,
        stparts_delivery,
        avtoformula_price,
        avtoformula_delivery,
    ]:
        if col not in df.columns:
            df[col] = None

    if LOCAL_NAME:
        if "finde_name" not in df.columns:
            df["finde_name"] = None

    if LOCAL_WEIGHT:
        df[JPARTS_P_W] = None
        df[JPARTS_V_W] = None
        df[ARMTEK_P_W] = None
        df[ARMTEK_V_W] = None

    tasks = [
        (idx, str(row[INPUT_COL_BRAND]).strip(), str(row[INPUT_COL_ARTICLE]).strip())
        for idx, row in df.head(MAX_ROWS).iterrows()
        if str(row[INPUT_COL_ARTICLE]).strip()
    ]

    # Вычисляем контрольные точки прогресса
    total_tasks = len(tasks)
    progress_checkpoints = {
        math.ceil(total_tasks * 0.25),  # 25%
        math.ceil(total_tasks * 0.50),  # 50%
        math.ceil(total_tasks * 0.75),  # 75%
        total_tasks,  # 100%
    }
    sent_progress = set()  # Чтобы не отправлять дважды

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        if LOCAL_WEIGHT:
            pool = ContextPool(
                browser, pool_size=MAX_WORKERS, auth_avtoformula=False
            )  # 🆕
        else:
            pool = ContextPool(browser, pool_size=MAX_WORKERS, auth_avtoformula=True)

        # pool = ContextPool(browser, pool_size=MAX_WORKERS)
        await pool.initialize()

        results = []
        processed_count = 0

        with tqdm(total=total_tasks, desc="Парсинг") as pbar:

            for coro in asyncio.as_completed(
                [process_row_async(pool, browser, *t) for t in tasks]
            ):
                # if stop_parsing.is_set():
                #     break
                idx, result = await coro
                if result:
                    for col, val in result.items():
                        df.at[idx, col] = val
                    # logger.info(f"✅ [{idx}] Записаны значения в df: {result}")

                pbar.update(1)
                results.append((idx, result))
                processed_count += 1

                # Проверка файла-флага каждые 10 задач или после каждой
                # if processed_count % 10 == 0 and Path("input/STOP.flag").exists():
                if Path("input/STOP.flag").exists():
                    logger.info("🛑 STOP.flag detected → graceful exit!")
                    break

                # Промежуточное сохранение каждые 100 строк
                if processed_count % TEMP_RAW == 0:
                    try:
                        # df = preprocess_dataframe(df)
                        await asyncio.to_thread(df.to_excel, TEMP_FILE, index=False)
                        logger.info(
                            f"💾 Промежуточное сохранение: {processed_count} строк обработано → {TEMP_FILE}"
                        )
                    except Exception as e:
                        logger.error(
                            f"❌ Ошибка при промежуточном сохранении Excel, но мы продолжаем: {e}"
                        )
                        # raise -убрали чтобы не вываливалось все

                # Отправка прогресса в Telegram при достижении контрольных точек
                if (
                    processed_count in progress_checkpoints
                    and processed_count not in sent_progress
                ):
                    percent = int(processed_count / total_tasks * 100)
                    send_telegram_process(
                        f"Прогресс: {percent}% ({processed_count} из {total_tasks})"
                    )
                    sent_progress.add(processed_count)

        # Финальное сохранение
        try:
            # df = preprocess_dataframe(df)
            output_file = get_output_file(mode)  # 🆕 + mode!

            if LOCAL_PRICE:  # Только для цен
                await asyncio.to_thread(adjust_prices_and_save, df, output_file)
            elif LOCAL_WEIGHT:
                pd.set_option("display.max_columns", None)
                pd.set_option("display.width", 200)

                logger.info(
                    f"📊 Перед консолидацией:\n"
                    f"{df[[INPUT_COL_ARTICLE, JPARTS_P_W, JPARTS_V_W, ARMTEK_P_W, ARMTEK_V_W]].head(20)}"
                )
                df = await asyncio.to_thread(consolidate_weights, df)
                await asyncio.to_thread(df.to_excel, output_file, index=False)
                logger.info(f"💾 Веса сохранены: {output_file}")
            elif LOCAL_NAME:
                await asyncio.to_thread(df.to_excel, output_file, index=False)
                logger.info(f"💾 Имена сохранены: {output_file}")

            # await asyncio.to_thread(adjust_prices_and_save, df, output_file)
        except Exception as e:
            logger.error(f"❌ Ошибка при финальном сохранении Excel: {e}")
        # await send_telegram_file(output_file) дулировалась отсылка файла
        await pool.close_all()
        await browser.close()
        logger.info("🎉 Завершено")


def main():
    setup_event_loop_policy()
    clear_debug_folders_sync(sites, logger)

    def stop_handler(signum, frame):
        stop_parsing.set()

    signal.signal(signal.SIGTERM, stop_handler)

    asyncio.run(main_async())


if __name__ == "__main__":
    main()

# main.py
"""
Асинхронный парсер с Playwright.
- Общие куки для avtoformula
- Автоматический re-login при разлогине
- Разделённые логи по сайтам
"""
from telegram import Bot
import asyncio
import time
import pandas as pd
import json
import math
import random
from pathlib import Path
from tqdm.asyncio import tqdm
import logging
from dotenv import load_dotenv
load_dotenv()

from playwright.async_api import async_playwright, Browser, BrowserContext
from config import (
    INPUT_FILE, OUTPUT_FILE, TEMP_FILE, MAX_ROWS, SAVE_INTERVAL,
    competitor1, competitor1_delivery, competitor2, competitor2_delivery,
    MAX_WORKERS, COOKIE_FILE,
    INPUT_COL_ARTICLE, INPUT_COL_BRAND,  
    AVTO_LOGIN, AVTO_PASSWORD, BOT_TOKEN, ADMIN_CHAT_ID, SEND_TO_TELEGRAM,ENABLE_AVTOFORMULA
)
from utils import logger, preprocess_dataframe
from state_manager import load_state, save_state
from price_adjuster import adjust_prices_and_save
import requests

# Импортируем асинхронные скрапперы
from scraper_avtoformula import scrape_avtoformula_pw
from scraper_stparts import scrape_stparts_async
from auth import ensure_logged_in


import sys



COOKIE_PATH = Path(COOKIE_FILE)
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# === Разделение логов ===
from utils import get_site_logger

logger_avto = get_site_logger("avtoformula")
logger_st = get_site_logger("stparts")



def setup_event_loop_policy():
    if sys.platform.startswith('win'):
        if hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
            asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            print("Установлена WindowsProactorEventLoopPolicy для Windows")
    else:
        print("Не Windows — политика событийного цикла не меняется")




# === Telegram ===
def send_telegram_error(msg):
    if not SEND_TO_TELEGRAM:
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, data={'chat_id': ADMIN_CHAT_ID, 'text': f"❌ Parser Error:\n{msg}"})
    except Exception as e:
        logger.error(f"Ошибка Telegram: {e}")


async def send_telegram_file(file_path, caption=None):
    if not SEND_TO_TELEGRAM:
        return
    try:
        bot = Bot(token=BOT_TOKEN)
        async with bot:
            with open(file_path, 'rb') as f:  # ← теперь файл закрывается
                await bot.send_document(
                    chat_id=ADMIN_CHAT_ID,
                    document=f,
                    caption=caption
                )
        logger.info("Файл отправлен в Telegram")
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")


# === Пул контекстов ===
class ContextPool:
    def __init__(self, browser: Browser, pool_size: int = 5):
        self.browser = browser
        self.pool_size = pool_size
        self.contexts = []
        self.semaphore = asyncio.Semaphore(pool_size)
        self.initialized = False
        self.cookies = None  # общие куки


    async def initialize(self):
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
            logger.info("✅ Авторизация успешна, состояние сохранено в storage_state.json")

        finally:
            await temp_context.close()

        # Создаём пул контекстов, загружая состояние
        logger.info(f"Создаём {self.pool_size} контекстов с общей авторизацией...")
        self.contexts = []  # очищаем на всякий случай

        for i in range(self.pool_size):
            ctx = await self.browser.new_context(
                storage_state=COOKIE_PATH,  # ← авторизованное состояние
                viewport={'width': 1920, 'height': 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            )
            self.contexts.append(ctx)
            logger.info(f"✅ Контекст {i + 1}/{self.pool_size} создан и авторизован")

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



async def process_row_async(pool: ContextPool, idx: int, brand: str, part: str):
    context = None
    page_st = None
    page_avto = None
    result_st = None
    result_avto = None

    for attempt in range(2):  # максимум 2 попытки
        try:
            context = await pool.get_context()
            page_st = await context.new_page()
            page_avto = await context.new_page()

            if ENABLE_AVTOFORMULA:
                result_st, result_avto = await asyncio.gather(
                    scrape_stparts_async(page_st, brand, part, logger_st),
                    scrape_avtoformula_pw(page_avto, brand, part, logger_avto),
                    return_exceptions=True
                )
            else:
                result_st = await scrape_stparts_async(page_st, brand, part, logger_st)
                result_avto = (None, None)

            # Проверка: если avtoformula упал из-за разлогина
            if isinstance(result_avto, Exception) and "зарегистрируйтесь" in str(result_avto).lower():
                logger.warning(f"🔁 Разлогин обнаружен для {brand}/{part}. Обновляем куки...")
                await pool.refresh_cookies()
                # Закрываем текущие страницы и контекст
                await page_st.close()
                await page_avto.close()
                pool.release_context(context)
                context = page_st = page_avto = None
                continue  # повторим попытку
            else:
                break  # всё ок, выходим из цикла

        except Exception as e:
            logger.error(f"Ошибка [{idx}] {brand}/{part}: {e}")
            send_telegram_error(f"{brand}/{part}: {e}")
            break  # выходим при фатальной ошибке
        finally:
            if page_st:
                await page_st.close()
            if page_avto:
                await page_avto.close()
            if context:
                pool.release_context(context)

    # Обработка результатов
    if isinstance(result_st, Exception):
        price_st, delivery_st = None, None
    else:
        price_st, delivery_st = result_st

    if isinstance(result_avto, Exception):
        price_avto, delivery_avto = None, None
    else:
        price_avto, delivery_avto = result_avto

    return idx, {
        competitor1: price_st,
        competitor1_delivery: delivery_st,
        competitor2: price_avto,
        competitor2_delivery: delivery_avto
    }

# === Основная функция ===
async def main_async():
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК PLAYWRIGHT ПАРСЕРА")
    logger.info("=" * 60)

    df = pd.read_excel(INPUT_FILE)
    
    # logger.info(f" датафрейм перед препроцесом {df}")  

    df = preprocess_dataframe(df)
    for col in [competitor1, competitor1_delivery, competitor2, competitor2_delivery]:
        if col not in df.columns:
            df[col] = None

    # logger.info(f" датафрейм после препроцесса {df}")   

    tasks = [
        (idx, str(row[INPUT_COL_BRAND]).strip(), str(row[INPUT_COL_ARTICLE]).strip())
        for idx, row in df.head(MAX_ROWS).iterrows()
        if str(row[INPUT_COL_ARTICLE]).strip()
    ]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        pool = ContextPool(browser, pool_size=MAX_WORKERS)
        await pool.initialize()

        results = []
        with tqdm(total=len(tasks), desc="Парсинг") as pbar:
            for coro in asyncio.as_completed([process_row_async(pool, *t) for t in tasks]):
                idx, result = await coro
                if result:
                    for col, val in result.items():
                        df.at[idx, col] = val
                pbar.update(1)
                results.append((idx, result))

        await asyncio.to_thread(adjust_prices_and_save, df, OUTPUT_FILE)
        await send_telegram_file(OUTPUT_FILE)
        await pool.close_all()
        await browser.close()
        logger.info("🎉 Завершено")


def main():
    setup_event_loop_policy()
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
    
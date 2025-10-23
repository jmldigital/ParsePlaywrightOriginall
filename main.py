# main.py
"""
Асинхронный парсер с Playwright.
- Общие куки для avtoformula
- Автоматический re-login при разлогине
- Разделённые логи по сайтам
"""
from telegram import Bot
import asyncio
import os
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
    INPUT_COL_ARTICLE, INPUT_COL_BRAND,TEMP_RAW,
    AVTO_LOGIN, AVTO_PASSWORD, BOT_TOKEN, ADMIN_CHAT_ID, SEND_TO_TELEGRAM,ENABLE_AVTOFORMULA
)
from utils import logger, preprocess_dataframe
from state_manager import load_state, save_state
from price_adjuster import adjust_prices_and_save
import requests

# Импортируем асинхронные скрапперы
from scraper_avtoformula import scrape_avtoformula_pw,scrape_avtoformula_name_async
from scraper_stparts import scrape_stparts_async, scrape_stparts_name_async
from auth import ensure_logged_in


import sys


ENABLE_NAME_PARSING = os.getenv('ENABLE_NAME_PARSING', 'False').lower() == 'true'
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


def send_telegram_process(msg):
    """Отправка прогресса в Telegram"""
    if not SEND_TO_TELEGRAM:
        return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, data={'chat_id': ADMIN_CHAT_ID, 'text': f"🕐 Прогресс:\n{msg}"})
    except Exception as e:
        logger.error(f"Ошибка отправки прогресса в Telegram: {e}")




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

            if ENABLE_NAME_PARSING:
                # Сначала пытаемся получить имя с stparts
                detail_name = await scrape_stparts_name_async(page_st, part, logger_st)
                
                # ✅ Проверяем, не является ли название "Деталь" или пустым
                if not detail_name or detail_name.lower().strip() == "деталь":
                    if detail_name:
                        logger.info(f"⚠️ stparts вернул 'Деталь' для {part}, пробуем avtoformula")
                    
                    # Пытаемся на avtoformula
                    detail_name_avto = await scrape_avtoformula_name_async(page_avto, part, logger_avto)
                    
                    # Используем название из avtoformula только если оно НЕ "Деталь" и НЕ пустое
                    if detail_name_avto and detail_name_avto.lower().strip() != "деталь":
                        detail_name = detail_name_avto
                    else:
                        detail_name = "Detail"
                        logger.info(f"❌ Не удалось найти нормальное название для {part}")
                
                # ✅ Сохраняем как строку, а не кортеж
                result_st = detail_name
                result_avto = None

            else:
                # Функции парсинга цены как раньше
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
                # ✅ Не закрываем здесь — будет в finally
                pool.release_context(context)
                context = None  # чтобы finally не освободил дважды
                # Помечаем страницы для закрытия
                continue  # повторим попытку
            else:
                break  # всё ок, выходим из цикла

        except asyncio.CancelledError:
            logger.warning(f"⚠️ Задача отменена для {brand}/{part}")
            break
        except Exception as e:
            logger.error(f"Ошибка [{idx}] {brand}/{part}: {e}")
            send_telegram_error(f"{brand}/{part}: {e}")
            break
        finally:
            # ✅ Безопасное закрытие страниц
            await safe_close_page(page_st)
            await safe_close_page(page_avto)
            if context:
                pool.release_context(context)

    # ✅ Правильная обработка результатов
    if ENABLE_NAME_PARSING:
        # result_st теперь просто строка (или None)
        name = result_st if isinstance(result_st, str) else None
        return idx, {'finde_name': name}
    else:
        if isinstance(result_st, Exception):
            price_st, delivery_st = None, None
        else:
            price_st, delivery_st = result_st if result_st else (None, None)

        if isinstance(result_avto, Exception):
            price_avto, delivery_avto = None, None
        else:
            price_avto, delivery_avto = result_avto if result_avto else (None, None)

        return idx, {
            competitor1: price_st,
            competitor1_delivery: delivery_st,
            competitor2: price_avto,
            competitor2_delivery: delivery_avto
        }



async def safe_close_page(page):
    """Безопасное закрытие страницы без ошибок"""
    if page:
        try:
            await page.close()
        except Exception:
            pass  # игнорируем ошибки закрытия




async def main_async():
    global ENABLE_NAME_PARSING
    # Перечитываем .env, чтобы подхватить изменения
    load_dotenv(override=True)
    
    # Считываем переменную заново
    ENABLE_NAME_PARSING = os.getenv('ENABLE_NAME_PARSING', 'False').lower() == 'true'

    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК PLAYWRIGHT ПАРСЕРА")
    logger.info(f"режим {'Поиск имён' if ENABLE_NAME_PARSING else 'Поиск цен'}")
    logger.info("=" * 60)

    df = pd.read_excel(INPUT_FILE)
    df = preprocess_dataframe(df)
    
    for col in [competitor1, competitor1_delivery, competitor2, competitor2_delivery]:
        if col not in df.columns:
            df[col] = None
    
    if ENABLE_NAME_PARSING:
        if 'finde_name' not in df.columns:
            df['finde_name'] = None

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
        total_tasks                      # 100%
    }
    sent_progress = set()  # Чтобы не отправлять дважды

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"])
        pool = ContextPool(browser, pool_size=MAX_WORKERS)
        await pool.initialize()

        results = []
        processed_count = 0
        
        with tqdm(total=total_tasks, desc="Парсинг") as pbar:
            for coro in asyncio.as_completed([process_row_async(pool, *t) for t in tasks]):
                idx, result = await coro
                if result:
                    for col, val in result.items():
                        df.at[idx, col] = val
                pbar.update(1)
                results.append((idx, result))
                processed_count += 1

                # Промежуточное сохранение каждые 100 строк
                if processed_count % TEMP_RAW == 0:
                    await asyncio.to_thread(df.to_excel, TEMP_FILE, index=False)
                    logger.info(f"💾 Промежуточное сохранение: {processed_count} строк обработано → {TEMP_FILE}")

                # Отправка прогресса в Telegram при достижении контрольных точек
                if processed_count in progress_checkpoints and processed_count not in sent_progress:
                    percent = int(processed_count / total_tasks * 100)
                    send_telegram_process(f"Прогресс: {percent}% ({processed_count} из {total_tasks})")
                    sent_progress.add(processed_count)

        # Финальное сохранение
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
    
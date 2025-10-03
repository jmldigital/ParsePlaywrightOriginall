# main.py
import time
import pandas as pd
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import random
import logging
from dotenv import load_dotenv
load_dotenv()  # ← ДО импорта config.py

import selenium.webdriver as webdriver
import json
import asyncio  # ← нужен для asyncio.run()

from config import (
    INPUT_FILE, OUTPUT_FILE, TEMP_FILE, MAX_ROWS, SAVE_INTERVAL,
    competitor1, competitor1_delivery, competitor2, competitor2_delivery,
    input_price, corrected_price,
    AVTO_LOGIN, AVTO_PASSWORD, BOT_TOKEN, ADMIN_CHAT_ID, SEND_TO_TELEGRAM
)
from utils import logger, preprocess_dataframe
from state_manager import load_state, save_state
from cache_manager import load_cache, save_cache, get_cache_key
from auth import load_cookies, is_logged_in
from scraper_stparts import scrape_stparts
from scraper_avtoformula import scrape_avtoformula
from price_adjuster import adjust_prices_and_save
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import asyncio

def setup_driver():
    """Настройка и создание WebDriver"""
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    from config import PAGE_LOAD_TIMEOUT

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def login_manually(driver, login, password):
    """Ручная авторизация на avtoformula.ru"""
    from config import SELECTORS
    try:
        driver.get("https://www.avtoformula.ru")
        wait = WebDriverWait(driver, 15)

        login_el = wait.until(EC.element_to_be_clickable((By.ID, SELECTORS['avtoformula']['login_field'])))
        login_el.clear()
        login_el.send_keys(login)

        password_el = driver.find_element(By.ID, SELECTORS['avtoformula']['password_field'])
        password_el.clear()
        password_el.send_keys(password)

        submit_btn = driver.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['login_button'])
        submit_btn.click()

        wait.until(EC.invisibility_of_element_located((By.ID, SELECTORS['avtoformula']['login_field'])))
        time.sleep(2)

        smode_select = wait.until(EC.element_to_be_clickable((By.ID, SELECTORS['avtoformula']['smode_select'])))
        for option in smode_select.find_elements(By.TAG_NAME, "option"):
            if option.get_attribute("value") == "A0":
                option.click()
                break

        from auth import save_cookies
        save_cookies(driver)
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка ручного входа: {e}")
        return False


def process_row(args):
    """
    Обрабатывает одну строку: (idx, brand, part, cache)
    Возвращает: (idx, result_dict)
    """
    idx, brand, part, cache = args

    # Проверка кэша
    # cache_key = get_cache_key(brand, part)
    # if cache_key in cache:
    #     logger.info(f"✅ Кэш: {brand}/{part} — пропускаем")
    #     return idx, cache[cache_key]

    driver = None
    try:
        driver = setup_driver()

        # Попытка входа по кукам
        if not load_cookies(driver) or not is_logged_in(driver):
            logger.info(f"→ Авторизация (ручная) для {brand}/{part}")
            if not login_manually(driver, AVTO_LOGIN, AVTO_PASSWORD):
                logger.warning(f"❌ Не удалось авторизоваться для {brand}/{part}")
                return idx, None

        logger.info(f"→ Парсинг stparts: {brand}/{part}")
        price_st, delivery_st = scrape_stparts(driver, brand, part)

        logger.info(f"→ Парсинг avtoformula: {brand}/{part}")
        price_avto, delivery_avto = scrape_avtoformula(driver, brand, part)

        # Формируем результат с именами из config
        result = {
            competitor1: round(price_st, 2) if price_st else None,
            competitor1_delivery: delivery_st,
            competitor2: round(price_avto, 2) if price_avto else None,
            competitor2_delivery: delivery_avto
        }

        time.sleep(random.uniform(1.0, 2.5))
        return idx, result

    except Exception as e:
        logger.error(f"Ошибка в потоке {brand}/{part}: {e}")
        return idx, None
    finally:
        if driver:
            driver.quit()


# === Telegram отправка (если нужна) ===
async def send_telegram_file(file_path, caption=None):
    from telegram import Bot
    try:
        bot = Bot(token=BOT_TOKEN)
        file_size = Path(file_path).stat().st_size / 1024
        default_caption = (
            f"✅ Обработка завершена!\n\n"
            f"📄 {Path(file_path).name}\n"
            f"📊 Размер: {file_size:.2f} KB"
        )
        with open(file_path, 'rb') as f:
            await bot.send_document(
                chat_id=ADMIN_CHAT_ID,
                document=f,
                caption=caption or default_caption,
                filename=Path(file_path).name
            )
        logger.info("✅ Файл отправлен в Telegram")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка отправки в Telegram: {e}")
        return False


# def send_result_to_telegram(file_path, processed_count=0, total_count=0):
#     if not SEND_TO_TELEGRAM:
#         logger.info("📤 Отправка в Telegram отключена")
#         return
#     caption = (
#         f"✅ Обработка завершена!\n\n"
#         f"📄 {Path(file_path).name}\n"
#         f"📊 Обработано: {processed_count}/{total_count}\n"
#         f"📦 Размер: {Path(file_path).stat().st_size / 1024:.2f} KB"
#     )
#     try:
#         asyncio.run(send_telegram_file(file_path, caption))
#     except Exception as e:
#         logger.error(f"Ошибка при отправке в Telegram: {e}")


def main():
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК ПАРСЕРА (ПАРАЛЛЕЛЬНЫЙ + STATE + CACHE)")
    logger.info("=" * 60)

    if not Path(INPUT_FILE).exists():
        logger.error(f"❌ Файл {INPUT_FILE} не найден!")
        return

    try:
        df = pd.read_excel(INPUT_FILE)
        logger.info(f"📥 Загружено {len(df)} строк")
        df = preprocess_dataframe(df)
    except Exception as e:
        logger.error(f"❌ Ошибка чтения файла: {e}")
        return

    # Добавляем колонки, если нужно
    for col in [competitor1, competitor1_delivery, competitor2, competitor2_delivery]:
        if col not in df.columns:
            df[col] = None

    # Загружаем состояние и кэш
    state = load_state()
    # cache = load_cache()

    cache = {}

    last_index = state['last_index']
    processed_count = state['processed_count']

    # Подготовка задач
    rows_to_process = df.head(MAX_ROWS).copy()
    tasks = []
    skipped = 0

    for idx, row in rows_to_process.iterrows():
        if idx <= last_index:
            skipped += 1
            continue
        part = str(row[1]).strip()
        brand = str(row[3]).strip()
        if pd.isna(row[1]) or pd.isna(row[3]) or not part or not brand or part == 'nan':
            continue
        tasks.append((idx, brand, part, cache))

    logger.info(f"✅ Пропущено {skipped} строк (уже обработано)")
    logger.info(f"📦 К обработке: {len(tasks)} позиций")

    if not tasks:
        logger.info("✅ Нет данных для обработки — завершаем")
        adjust_prices_and_save(df, OUTPUT_FILE)
        # send_result_to_telegram(OUTPUT_FILE, processed_count, processed_count)
        return

    # Параллельная обработка
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_row, task) for task in tasks]
        for future in tqdm(as_completed(futures), total=len(futures), desc="🔍 Парсинг"):
            try:
                idx, result = future.result()
                if result:
                    for col, value in result.items():
                        df.at[idx, col] = value
                    # Обновляем кэш
                    # cache[get_cache_key(df.iloc[idx, 3], df.iloc[idx, 1])] = result

                processed_count += 1
                if processed_count % SAVE_INTERVAL == 0:
                    df.to_excel(TEMP_FILE, index=False)
                    save_state(idx, processed_count)
                    # save_cache(cache)
                    logger.info(f"💾 Сохранён прогресс: строка {idx}")

            except Exception as e:
                logger.error(f"❌ Ошибка при обработке результата: {e}")

    # Финальное сохранение
    try:
        df.to_excel(OUTPUT_FILE, index=False)
        logger.info(f"✅ Результат сохранён: {OUTPUT_FILE}")

        adjust_prices_and_save(df, OUTPUT_FILE)

        if Path(OUTPUT_FILE).exists():
            logger.info("📤 Отправка результата в Telegram...")
            # send_result_to_telegram(OUTPUT_FILE, processed_count, processed_count)

        # Очистка
        if Path(TEMP_FILE).exists():
            Path(TEMP_FILE).unlink()
            logger.info(f"🧹 Временный файл {TEMP_FILE} удалён")

        logger.info(f"🎉 Обработка завершена: {processed_count} строк")

    except Exception as e:
        logger.error(f"❌ Ошибка при финальном сохранении: {e}")


if __name__ == "__main__":
    main()
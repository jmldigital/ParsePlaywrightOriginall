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
import atexit
import selenium.webdriver as webdriver
import json
import asyncio  # ← нужен для asyncio.run()
import platform

from config import (
    INPUT_FILE, OUTPUT_FILE, TEMP_FILE, MAX_ROWS, SAVE_INTERVAL,
    competitor1, competitor1_delivery, competitor2, competitor2_delivery,
    MAX_WORKERS,
    INPUT_COL_ARTICLE, INPUT_COL_BRAND,  
    AVTO_LOGIN, AVTO_PASSWORD, BOT_TOKEN, ADMIN_CHAT_ID, SEND_TO_TELEGRAM
)
from utils import logger, preprocess_dataframe
from state_manager import load_state, save_state
from cache_manager import load_cache, save_cache, get_cache_key
from auth import load_cookies, is_logged_in,save_cookies
from scraper_stparts import scrape_stparts
from scraper_avtoformula import scrape_avtoformula
from price_adjuster import adjust_prices_and_save
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import asyncio
import requests
import threading



from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import os
import stat


# Локальное хранилище драйвера для каждого потока
thread_local = threading.local()
# Глобальная переменная — путь к chromedriver
CHROMEDRIVER_PATH = None

def get_driver():
    """Возвращает драйвер для текущего потока. Создаёт, если его нет."""
    if not hasattr(thread_local, "driver"):
        logger.info(f"🧵 Создаём драйвер для потока {threading.current_thread().name}")
        driver = setup_driver()
        thread_local.driver = driver
        thread_local.logged_in = False  # флаг авторизации
    else:
        driver = thread_local.driver

    # Проверяем, залогинены ли мы
    if not thread_local.logged_in:
        if not load_cookies(driver) or not is_logged_in(driver):
            logger.info("🔐 Куки не сработали — делаем ручной логин")
            if login_manually(driver, AVTO_LOGIN, AVTO_PASSWORD):
                save_cookies(driver)
                thread_local.logged_in = True
            else:
                logger.error("❌ Не удалось залогиниться")
                return None
        else:
            logger.info("✅ Авторизован по кукам")
            thread_local.logged_in = True

    return driver


def quit_driver():
    """Закрывает драйвер текущего потока"""
    if hasattr(thread_local, "driver"):
        logger.info(f"🛑 Закрываем драйвер потока {threading.current_thread().name}")
        thread_local.driver.quit()
        delattr(thread_local, "driver")
        thread_local.logged_in = False





# универсальный
def setup_driver():
    """Универсальная настройка WebDriver — для Windows и Linux"""
    from selenium.webdriver.chrome.service import Service
    from config import PAGE_LOAD_TIMEOUT
    import logging

    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    )

    global CHROMEDRIVER_PATH

    if CHROMEDRIVER_PATH is None:
        raise RuntimeError("❌ CHROMEDRIVER_PATH не инициализирован")

    try:
        service = Service(CHROMEDRIVER_PATH)
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        logger.info(f"✅ WebDriver создан: {CHROMEDRIVER_PATH}")
        return driver
    except Exception as e:
        logger.critical(f"❌ Ошибка создания WebDriver: {e}", exc_info=True)
        send_telegram_error(f"💥 Ошибка драйвера: {e}")
        raise






# ддля докера
# def setup_driver():
#     """Настройка WebDriver — только системный chromedriver (Docker)"""
#     from selenium import webdriver
#     from selenium.webdriver.chrome.service import Service
#     import shutil
#     import os

#     # Ищем chromedriver
#     chromedriver_path = shutil.which('chromedriver')
    
#     if not chromedriver_path:
#         # Резервный путь (на случай, если which не нашёл)
#         fallback_path = '/usr/local/bin/chromedriver'
#         if os.path.isfile(fallback_path) and os.access(fallback_path, os.X_OK):
#             chromedriver_path = fallback_path
#         else:
#             raise FileNotFoundError(
#                 "❌ chromedriver не найден ни через 'which', ни по пути /usr/local/bin/chromedriver"
#             )

#     logger.info(f"✅ Используем chromedriver: {chromedriver_path}")

#     # Настройки Chrome
#     options = webdriver.ChromeOptions()
#     options.add_argument("--headless=new")
#     options.add_argument("--no-sandbox")
#     options.add_argument("--disable-dev-shm-usage")
#     options.add_argument("--disable-gpu")
#     options.add_argument("--disable-software-rasterizer")
#     options.add_argument("--disable-extensions")
#     options.add_argument("--disable-setuid-sandbox")
#     options.add_argument("--window-size=1920,1080")
#     options.add_experimental_option("excludeSwitches", ["enable-automation"])
#     options.add_experimental_option('useAutomationExtension', False)
#     options.add_argument(
#         "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
#     )

#     try:
#         service = Service(chromedriver_path)
#         driver = webdriver.Chrome(service=service, options=options)
#         logger.info("✅ WebDriver успешно запущен в Docker")
#         return driver
#     except Exception as e:
#         logger.critical(f"❌ Ошибка запуска WebDriver: {e}", exc_info=True)
#         send_telegram_error(f"💥 Ошибка драйвера: {e}")
#         raise




def send_telegram_error(msg):
    """Отправка текстовой ошибки в Telegram"""
    token = BOT_TOKEN
    chat_id = ADMIN_CHAT_ID
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': f"❌ Parser Error:\n{msg}",
        'parse_mode': 'html'
    }
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        logger.error(f"Ошибка отправки ошибки в Telegram: {e}")


def send_telegram_process(msg):
    """Отправка текстовой ошибки в Telegram"""
    token = BOT_TOKEN
    chat_id = ADMIN_CHAT_ID
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        'chat_id': chat_id,
        'text': f"🕐 Прогресс:\n{msg}",
        'parse_mode': 'html'
    }
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        logger.error(f"Ошибка отправки ошибки в Telegram: {e}")




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

        # from auth import save_cookies
        # save_cookies(driver)
        return True
    except Exception as e:
        send_telegram_error(f"Ошибка ручного входа: {e}")
        logger.error(f"❌ Ошибка ручного входа: {e}")
        return False




def process_row(args):
    """
    Обрабатывает одну строку: (idx, brand, part)
    Возвращает: (idx, result_dict)
    """
    idx, brand, part = args  # cache больше не передаём

    try:
        driver = get_driver()
        if driver is None:
            return idx, None

        logger.info(f"→ Парсинг stparts: {brand}/{part}")
        price_st, delivery_st = scrape_stparts(driver, brand, part)

        logger.info(f"→ Парсинг avtoformula: {brand}/{part}")
        price_avto, delivery_avto = scrape_avtoformula(driver, brand, part)

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
        send_telegram_error(f"Ошибка в потоке {brand}/{part}: {e}")
        return idx, None


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
        send_telegram_error(f"Ошибка отправки в Telegram: {e}")
        return False




def thread_initializer():
    """Инициализирует драйвер при старте потока"""
    get_driver()  # запустит setup_driver и login при первом вызове
    # Регистрируем закрытие драйвера при выходе потока

    atexit.register(quit_driver)

def main():
    global CHROMEDRIVER_PATH  # ← Обязательно!
    logger.info("=" * 60)
    logger.info("🚀 ЗАПУСК ПАРСЕРА (ПАРАЛЛЕЛЬНЫЙ + STATE + CACHE)")
    logger.info("=" * 60)

 # 🔍 Определяем ОС
    current_os = platform.system()
    logger.info(f"🖥️  ОС: {current_os} ({platform.platform()})")

    # 🌐 Только на Windows — скачиваем chromedriver
    if current_os == "Windows":
        logger.info("⏬ Инициализация: скачиваем chromedriver (Windows)...")
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            CHROMEDRIVER_PATH = ChromeDriverManager().install()
            logger.info(f"✅ chromedriver установлен: {CHROMEDRIVER_PATH}")
        except Exception as e:
            logger.critical(f"❌ Ошибка при установке chromedriver: {e}", exc_info=True)
            send_telegram_error(f"💥 Критическая ошибка: не удалось установить chromedriver\n{e}")
            return
    else:
        # Linux / Docker: предполагаем, что chromedriver уже установлен
        logger.info("🟢 OS: Linux/Docker — используем системный chromedriver")
        # Можно дополнительно проверить, доступен ли chromedriver в PATH
        import shutil
        chromedriver_path = shutil.which('chromedriver')
        if chromedriver_path:
            CHROMEDRIVER_PATH = chromedriver_path
            logger.info(f"✅ Найден системный chromedriver: {CHROMEDRIVER_PATH}")
        else:
            logger.critical("❌ chromedriver не найден в PATH (Linux/Docker)")
            send_telegram_error("💥 Ошибка: chromedriver не найден в PATH")
            return

    if not Path(INPUT_FILE).exists():
        logger.error(f"❌ Файл {INPUT_FILE} не найден!")
        return

    try:
        df = pd.read_excel(INPUT_FILE)
        # 🔥 Конвертируем имена столбцов: любые числа → строки без .0
        df.columns = df.columns.astype(str).str.replace('.0', '', regex=False).str.strip()
        logger.info(f"📥 Загружено {len(df)} строк")
        df = preprocess_dataframe(df)
    except Exception as e:
        send_telegram_error(f"Ошибка чтения файла: {e}")
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
        # part = str(row[1]).strip()
        # brand = str(row[3]).strip()

        part = str(row[INPUT_COL_ARTICLE]).strip()
        brand = str(row[INPUT_COL_BRAND]).strip()
        if (
            pd.isna(row[INPUT_COL_ARTICLE])
            or pd.isna(row[INPUT_COL_BRAND])
            or not part
            or not brand
            or part == 'nan'
        ):
            continue

        tasks.append((idx, brand, part))

    logger.info(f"✅ Пропущено {skipped} строк (уже обработано)")
    logger.info(f"📦 К обработке: {len(tasks)} позиций")

    progress_checkpoints = [
    int(len(tasks) * 0.25),
    int(len(tasks) * 0.50),
    int(len(tasks) * 0.75),
    len(tasks)
    ]
    sent_progress = set()

    if not tasks:
        logger.info("✅ Нет данных для обработки — завершаем")
        adjust_prices_and_save(df, OUTPUT_FILE)
        # send_result_to_telegram(OUTPUT_FILE, processed_count, processed_count)
        return

    # Параллельная обработка
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, initializer=thread_initializer) as executor:
        futures = [executor.submit(process_row, task) for task in tasks]
        for future in tqdm(as_completed(futures), total=len(futures), desc="🔍 Парсинг"):
            try:
                idx, result = future.result()
                if result:
                    for col, value in result.items():
                        df.at[idx, col] = value
                    # Обновляем кэш
                    # cache[get_cache_key(df.iloc[idx, 3], df.iloc[idx, 1])] = result

                        # Проверяем, не достигли ли очередной четверти
                processed_count += 1
                if processed_count in progress_checkpoints and processed_count not in sent_progress:
                    percent = int(processed_count / len(tasks) * 100)
                    send_telegram_process(f"Прогресс: {percent}% ({processed_count} из {len(tasks)})")
                    sent_progress.add(processed_count)    

                
                if processed_count % SAVE_INTERVAL == 0:
                    df.to_excel(TEMP_FILE, index=False)
                    save_state(idx, processed_count)
                    # save_cache(cache)
                    logger.info(f"💾 Сохранён прогресс: строка {idx}")

            except Exception as e:
                send_telegram_error(f"❌ Ошибка при обработке результата: {e}")
                logger.error(f"❌ Ошибка при обработке результата: {e}")

    # Финальное сохранение
    try:
        adjust_prices_and_save(df, OUTPUT_FILE)
        logger.info(f"✅ Результат сохранён: {OUTPUT_FILE}")

        

        if Path(OUTPUT_FILE).exists():
            logger.info("📤 Отправка результата в Telegram...")
            # send_result_to_telegram(OUTPUT_FILE, processed_count, processed_count)

        # Очистка
        if Path(TEMP_FILE).exists():
            Path(TEMP_FILE).unlink()
            logger.info(f"🧹 Временный файл {TEMP_FILE} удалён")



        # logger.info(f"🔄 rows_to_process - {len(tasks)} --------processed_count - {processed_count}")

        new_tasks_count = len(tasks)
        new_processed = processed_count - state['processed_count']

        if new_tasks_count > 0 and new_processed >= new_tasks_count:
            logger.info("🔄 Все новые строки обработаны. Сбрасываем state для следующего запуска.")
            save_state(-1, 0)

        logger.info(f"🎉 Обработка завершена: {processed_count} строк")

    except Exception as e:
        logger.error(f"❌ Ошибка при финальном сохранении: {e}")
        send_telegram_error(f"Ошибка при финальном сохранении: {e}")

if __name__ == "__main__":
    main()
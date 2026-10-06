import time
import re
import logging
from pathlib import Path
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from twocaptcha import TwoCaptcha
from dotenv import load_dotenv
import os

# Загрузка переменных окружения
load_dotenv()

# ==================== КОНФИГУРАЦИЯ ====================
API_KEY_2CAPTCHA = os.getenv('API_KEY_2CAPTCHA', 'your_api_key_here')
AVTO_LOGIN = os.getenv('AVTO_LOGIN', 'your_login_here')
AVTO_PASSWORD = os.getenv('AVTO_PASSWORD', 'your_password_here')

INPUT_FILE = 'input/наличие.xls'
OUTPUT_FILE = 'output/наличие_with_competitors.xlsx'
TEMP_FILE = 'output/наличие_temp.xlsx'
MAX_ROWS = 5
SAVE_INTERVAL = 10  # Сохранять каждые N строк

# Таймауты и задержки
PAGE_LOAD_TIMEOUT = 60
DEFAULT_WAIT = 15
REQUEST_DELAY = 1.5
CAPTCHA_WAIT = 5

# Retry настройки
MAX_RETRIES = 3
RETRY_DELAY = 2

# CSS селекторы
SELECTORS = {
    'stparts': {
        'captcha_img': 'img.captchaImg',
        'captcha_input': "input[name='captcha']",
        'captcha_submit': 'captchaSubmitBtn',
        'results_table': 'table.globalResult.searchResultsSecondStep',
        'result_row': 'tr.resultTr2',
        'brand': 'td.resultBrand',
        'delivery': 'td.resultDeadline',
        'price': 'td.resultPrice'
    },
    'avtoformula': {
        'login_field': 'userlogin',
        'password_field': 'userpassword',
        'login_button': "input[type='submit'][name='login']",
        'article_field': 'article',
        'search_button': 'input[name="search"][data-action="ajaxSearch"]',
        'smode_select': 'smode',
        'results_table': 'table.web_ar_datagrid.search_results',
        'brand_cell': 'td.td_prd_info_link',
        'delivery_cell': 'td.td_term',
        'price_cell': 'td.td_final_price'
    }
}

# ==================== НАСТРОЙКА ЛОГИРОВАНИЯ ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def setup_driver():
    """Настройка и создание WebDriver"""
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=False")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        logger.info("WebDriver успешно инициализирован")
        return driver
    except Exception as e:
        logger.error(f"Ошибка при создании WebDriver: {e}")
        raise


def parse_price(text):
    """Парсинг цены из текста"""
    if not text or not isinstance(text, str):
        return None
    
    # Удаляем всё кроме цифр, точек, запятых и пробелов
    clean = re.sub(r"[^\d,\.\s]", "", text).strip()
    
    # Удаляем пробелы
    clean = clean.replace(" ", "")
    
    # Обработка запятых как разделителей
    if clean.count(",") == 1 and clean.count(".") == 0:
        clean = clean.replace(",", ".")
    elif clean.count(",") > 1:
        clean = clean.replace(",", "")
    
    try:
        return float(clean)
    except (ValueError, AttributeError):
        logger.warning(f"Не удалось распарсить цену: {text}")
        return None


def preprocess_dataframe(df):
    """
    Удаляет любые слеши из колонки бренда (индекс 3)
    """
    try:
        brand_col_idx = 2
        if len(df.columns) > brand_col_idx:
            df.iloc[:, brand_col_idx] = (
                df.iloc[:, brand_col_idx]
                  .astype(str)
                  .str.replace('/', '', regex=False)
                  .str.replace('\\', '', regex=False)
                  .str.strip()
            )
    except Exception as e:
        logger.error(f"Ошибка при предобработке данных: {e}")
    return df


    

def normalize_brand(brand_str):
    """Нормализация названия бренда для сравнения"""
    if not brand_str:
        return ""
    return re.sub(r'[^a-z0-9]', '', str(brand_str).lower())


def brand_matches(search_brand, result_brand):
    """
    Проверка соответствия бренда (двусторонняя).
    Возвращает True если один бренд является подстрокой другого.
    Примеры:
    - DAEWOO соответствует DAEWOO BUS ✓
    - N-ROCKY соответствует ROCKY ✓
    - VAG соответствует VAG GROUP ✓
    - MANN соответствует MANN FILTER ✓
    """
    if not search_brand or not result_brand:
        return False
    
    norm_search = normalize_brand(search_brand)
    norm_result = normalize_brand(result_brand)
    
    # Проверяем в обе стороны: один бренд содержится в другом
    return norm_search in norm_result or norm_result in norm_search


def save_progress(df, filename=TEMP_FILE):
    """Сохранение промежуточного прогресса"""
    try:
        df.to_excel(filename, index=False)
        logger.info(f"Промежуточный прогресс сохранён в {filename}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении прогресса: {e}")


def retry_on_failure(func, *args, max_retries=MAX_RETRIES, **kwargs):
    """Декоратор для повторных попыток при ошибке"""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Попытка {attempt + 1}/{max_retries} не удалась: {e}. Повтор через {RETRY_DELAY}с")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"Все {max_retries} попытки исчерпаны для {func.__name__}")
                raise


# ==================== РАБОТА С КАПЧЕЙ ====================
def solve_image_captcha(driver):
    """Решение капчи через 2Captcha"""
    try:
        solver = TwoCaptcha(API_KEY_2CAPTCHA)
        
        img_el = driver.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['captcha_img'])
        captcha_base64 = img_el.screenshot_as_base64

        logger.info("Отправляем капчу на распознавание в 2Captcha")
        result = solver.normal(captcha_base64)
        captcha_text = result['code']
        logger.info(f"Капча распознана: {captcha_text}")

        # Вводим ответ
        input_el = driver.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['captcha_input'])
        input_el.clear()
        input_el.send_keys(captcha_text)

        submit_btn = driver.find_element(By.ID, SELECTORS['stparts']['captcha_submit'])
        submit_btn.click()
        time.sleep(CAPTCHA_WAIT)

        return True
    except Exception as e:
        logger.error(f"Ошибка решения капчи: {e}")
        return False


# ==================== ПАРСИНГ STPARTS.RU ====================
def scrape_stparts(driver, brand, part):
    """Парсинг данных с stparts.ru"""
    url = f"https://stparts.ru/search/{brand}/{part}"
    
    try:
        driver.get(url)
        logger.info(f"Загружена страница: {url}")
        
        # Проверка на капчу
        if len(driver.find_elements(By.CSS_SELECTOR, SELECTORS['stparts']['captcha_img'])) > 0:
            logger.warning("Обнаружена капча на stparts.ru")
            if not solve_image_captcha(driver):
                raise Exception("Не удалось решить капчу")

        wait = WebDriverWait(driver, DEFAULT_WAIT)

        # Ожидаем загрузку таблицы результатов
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, SELECTORS['stparts']['results_table'])))
        
        table = driver.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['results_table'])
        wait.until(lambda d: len(d.find_elements(
            By.CSS_SELECTOR, SELECTORS['stparts']['result_row'])) > 0)
        
        rows = table.find_elements(By.CSS_SELECTOR, SELECTORS['stparts']['result_row'])
        
        if not rows:
            logger.info(f"Результаты не найдены для {brand} / {part}")
            return None, None

        logger.info(f"Найдено {len(rows)} строк результатов")

        # Приоритетный поиск: сначала ищем с доставкой от 1 дня
        for priority_search in [True, False]:
            for row in rows:
                try:
                    brand_td = row.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['brand'])
                    brand_in_row = brand_td.text.strip()
                    
                    # Используем новую функцию проверки бренда
                    if not brand_matches(brand, brand_in_row):
                        # print('brand-',brand,'brand_in_row',brand_in_row)
                        # print(brand_matches(brand, brand_in_row))
                        continue

                    delivery_td = row.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['delivery'])
                    delivery_min = delivery_td.text.strip()

                    # Первый проход - только срок от 1 дня
                    if priority_search and not re.match(r"^1(\D|$)", delivery_min):
                        continue

                    price_text = row.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['price']).text
                    price = parse_price(price_text)
                    
                    if price is not None:
                        if priority_search:
                            logger.info(f"Найдено в наличии (бренд: {brand_in_row}, срок {delivery_min}): {price} ₽")
                        else:
                            logger.info(f"Найдено (бренд: {brand_in_row}, срок {delivery_min}): {price} ₽")
                        return price, delivery_min
                        
                except NoSuchElementException:
                    continue

        logger.info(f"Подходящие результаты не найдены для {brand} / {part}")
        return None, None

    except TimeoutException:
        logger.error(f"Timeout при загрузке результатов для {brand} / {part}")
        return None, None
    except Exception as e:
        logger.error(f"Ошибка парсинга stparts для {brand} / {part}: {e}")
        return None, None


# ==================== АВТОРИЗАЦИЯ AVTOFORMULA ====================
def login_avtoformula(driver, login, password):
    """Авторизация на avtoformula.ru"""
    login_url = "https://www.avtoformula.ru"
    
    try:
        driver.get(login_url)
        wait = WebDriverWait(driver, DEFAULT_WAIT)

        logger.info("Начинаем авторизацию на avtoformula.ru")
        
        login_el = wait.until(EC.element_to_be_clickable(
            (By.ID, SELECTORS['avtoformula']['login_field'])))
        login_el.clear()
        login_el.send_keys(login)

        password_el = driver.find_element(By.ID, SELECTORS['avtoformula']['password_field'])
        password_el.clear()
        password_el.send_keys(password)

        submit_btn = driver.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['login_button'])
        submit_btn.click()

        # Проверка успешной авторизации
        wait.until(EC.invisibility_of_element((By.ID, SELECTORS['avtoformula']['login_field'])))
        logger.info("Авторизация успешна")

        # Настройка опции "без аналогов"
        time.sleep(2)
        wait.until(EC.element_to_be_clickable((By.ID, SELECTORS['avtoformula']['smode_select'])))
        smode_select = driver.find_element(By.ID, SELECTORS['avtoformula']['smode_select'])
        
        for option in smode_select.find_elements(By.TAG_NAME, "option"):
            if option.get_attribute("value") == "A0":
                option.click()
                logger.info("Опция 'без аналогов' установлена")
                break

        return True
        
    except TimeoutException:
        logger.error("Timeout при авторизации на avtoformula.ru")
        return False
    except Exception as e:
        logger.error(f"Ошибка авторизации на avtoformula.ru: {e}")
        return False


# ==================== ПАРСИНГ AVTOFORMULA.RU ====================
def scrape_avtoformula(driver, brand, part):
    """Парсинг данных с avtoformula.ru"""
    try:
        wait = WebDriverWait(driver, DEFAULT_WAIT)
        driver.get("https://www.avtoformula.ru")

        # Вводим артикул
        article_field = wait.until(EC.element_to_be_clickable(
            (By.ID, SELECTORS['avtoformula']['article_field'])))
        article_field.clear()
        article_field.send_keys(part)
        logger.info(f"Введён артикул: {part}")

        # Клик по кнопке поиска
        search_btn = driver.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['search_button'])
        search_btn.click()

        # Ожидаем результаты
        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, SELECTORS['avtoformula']['results_table'])))
        
        rows = driver.find_elements(By.CSS_SELECTOR, 
            f"{SELECTORS['avtoformula']['results_table']} tr")
        
        if not rows or len(rows) < 2:
            logger.info(f"Результаты не найдены для {brand} / {part}")
            return None, None

        min_delivery = None
        min_price = None

        # Пропускаем заголовок таблицы
        for row in rows[1:]:
            try:
                brand_td = row.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['brand_cell'])
                brand_in_row = brand_td.text.strip()
                
                # Используем новую функцию проверки бренда
                if not brand_matches(brand, brand_in_row):
                    continue

                delivery_td = row.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['delivery_cell'])
                delivery_text = delivery_td.text.strip().split('/')[0].strip()
                
                try:
                    delivery_days = int(delivery_text)
                except (ValueError, IndexError):
                    continue

                price_td = row.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['price_cell'])
                price = parse_price(price_td.text.strip())
                
                if price is None:
                    continue

                # Ищем минимальный срок доставки
                if min_delivery is None or delivery_days < min_delivery:
                    min_delivery = delivery_days
                    min_price = price
                    
            except NoSuchElementException:
                continue

        if min_delivery is not None:
            logger.info(f"Найдено: {min_delivery} дней, цена {min_price} ₽")
            return min_price, f"{min_delivery} дней"
        
        logger.info(f"Подходящие результаты не найдены для {brand} / {part}")
        return None, None

    except TimeoutException:
        logger.error(f"Timeout при поиске на avtoformula для {brand} / {part}")
        return None, None
    except Exception as e:
        logger.error(f"Ошибка парсинга avtoformula для {brand} / {part}: {e}")
        return None, None


# ==================== ОСНОВНАЯ ЛОГИКА ====================
def main():
    """Главная функция"""
    logger.info("=" * 60)
    logger.info("ЗАПУСК ПАРСЕРА АВТОЗАПЧАСТЕЙ")
    logger.info("=" * 60)
    
    # Проверка наличия входного файла
    if not Path(INPUT_FILE).exists():
        logger.error(f"Файл {INPUT_FILE} не найден!")
        return
    
    # Проверка credentials
    if API_KEY_2CAPTCHA == 'your_api_key_here':
        logger.warning("⚠️ API ключ 2Captcha не настроен!")
    if AVTO_LOGIN == 'your_login_here':
        logger.warning("⚠️ Логин Avtoformula не настроен!")
    
    # Загрузка данных
    try:
        df = pd.read_excel(INPUT_FILE)
        logger.info(f"Загружено {len(df)} строк из {INPUT_FILE}")

        # Предобработка данных
        df = preprocess_dataframe(df)
        print('колонка бренда ',df[3])

    except Exception as e:
        logger.error(f"Ошибка при чтении файла: {e}")
        return
    
    # Добавление новых колонок
    for col in ['stparts', 'stparts_delivery', 'avtoformula_price', 'avtoformula_delivery']:
        if col not in df.columns:
            df[col] = ""
    
    driver = None
    processed_count = 0
    
    try:
        driver = setup_driver()
        
        # Авторизация на avtoformula
        if not login_avtoformula(driver, AVTO_LOGIN, AVTO_PASSWORD):
            logger.error("Не удалось авторизоваться на avtoformula.ru")
            return
        
        # Основной цикл обработки
        rows_to_process = df.head(MAX_ROWS)
        total_rows = len(rows_to_process)
        
        for idx, row in rows_to_process.iterrows():
            try:
                part = str(row[1]).strip()
                brand = str(row[3]).strip()
                
                if not part or not brand or part == 'nan' or brand == 'nan':
                    logger.warning(f"Строка {idx + 1}: пропущена (пустые данные)")
                    continue
                
                logger.info(f"\n{'=' * 60}")
                logger.info(f"Обработка {processed_count + 1}/{total_rows}: {brand} / {part}")
                logger.info(f"{'=' * 60}")
                
                # Парсинг stparts.ru
                logger.info("→ Поиск на stparts.ru")
                price_st, delivery_st = retry_on_failure(scrape_stparts, driver, brand, part)
                
                if price_st is not None:
                    df.at[idx, 'stparts'] = round(price_st, 2)
                if delivery_st is not None:
                    df.at[idx, 'stparts_delivery'] = delivery_st
                
                # Парсинг avtoformula.ru
                logger.info("→ Поиск на avtoformula.ru")
                price_avto, delivery_avto = retry_on_failure(scrape_avtoformula, driver, brand, part)
                
                if price_avto is not None:
                    df.at[idx, 'avtoformula_price'] = round(price_avto, 2)
                if delivery_avto is not None:
                    df.at[idx, 'avtoformula_delivery'] = delivery_avto
                
                processed_count += 1
                
                # Периодическое сохранение прогресса
                if processed_count % SAVE_INTERVAL == 0:
                    save_progress(df)
                
                time.sleep(REQUEST_DELAY)
                
            except Exception as e:
                logger.error(f"Критическая ошибка при обработке строки {idx + 1}: {e}")
                save_progress(df)
                continue
        
        logger.info(f"\n{'=' * 60}")
        logger.info(f"Обработка завершена: {processed_count}/{total_rows} строк")
        logger.info(f"{'=' * 60}")
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ Прервано пользователем")
        save_progress(df)
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        save_progress(df)
        
    finally:
        if driver:
            driver.quit()
            logger.info("WebDriver закрыт")
        
        # Финальное сохранение
        try:
            df.to_excel(OUTPUT_FILE, index=False)
            logger.info(f"✅ Результаты сохранены в {OUTPUT_FILE}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении финального файла: {e}")


if __name__ == "__main__":
    main()
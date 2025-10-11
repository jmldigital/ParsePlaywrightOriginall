# scraper_stparts.py
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from config import SELECTORS, API_KEY_2CAPTCHA
from utils import logger, parse_price, brand_matches
from twocaptcha import TwoCaptcha
import time
import re
from selenium.webdriver.support import expected_conditions as EC

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

        input_el = driver.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['captcha_input'])
        input_el.clear()
        input_el.send_keys(captcha_text)

        submit_btn = driver.find_element(By.ID, SELECTORS['stparts']['captcha_submit'])
        submit_btn.click()
        time.sleep(5)  # Ждём реакции после отправки капчи
        return True
    except Exception as e:
        logger.error(f"Ошибка решения капчи: {e}")
        return False




# 🕒 Глобальная настройка таймаута ожидания элементов
WAIT_TIMEOUT = 8  # например, 8 секунд — можно изменить в одном месте


def wait_for_results_or_no_results(driver, timeout=WAIT_TIMEOUT):
    """Ждёт появления результатов или блока 'нет результатов'"""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: (
                d.find_elements(By.CSS_SELECTOR, SELECTORS['stparts']['results_table'])
                or d.find_elements(By.CSS_SELECTOR, "div.fr-alert.fr-alert-warning.alert-noResults")
            )
        )

        if driver.find_elements(By.CSS_SELECTOR, "div.fr-alert.fr-alert-warning.alert-noResults"):
            logger.info("🚫 На странице указан блок 'нет результатов'")
            return "no_results"
        return "has_results"

    except TimeoutException:
        logger.warning("⚠️ Истёк таймаут ожидания появления результатов")
        return "timeout"


def scrape_stparts(driver, brand, part):
    """Парсинг данных с stparts.ru с fallback-поиском только по номеру детали"""
    base_url = "https://stparts.ru"
    try:
        # Основной поиск бренд + деталь
        url = f"{base_url}/search/{brand}/{part}"
        driver.get(url)
        logger.info(f"Загружена страница: {url}")

        # Проверка капчи
        try:
            WebDriverWait(driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, SELECTORS['stparts']['captcha_img']))
            )
            logger.warning("Обнаружена капча на stparts.ru")
            if not solve_image_captcha(driver):
                raise Exception("Не удалось решить капчу")
        except TimeoutException:
            pass  # капчи нет - продолжаем

        status = wait_for_results_or_no_results(driver)
        if status == "no_results":
            return None, None
        elif status == "timeout":
            logger.error(f"Timeout при загрузке результатов для {brand} / {part}")
            return None, None

        # Ожидание таблицы результатов
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, SELECTORS['stparts']['results_table']))
        )
        table = driver.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['results_table'])

        WebDriverWait(driver, WAIT_TIMEOUT).until(
            lambda d: len(d.find_elements(By.CSS_SELECTOR, SELECTORS['stparts']['result_row'])) > 0
        )

        rows = table.find_elements(By.CSS_SELECTOR, SELECTORS['stparts']['result_row'])
        if not rows:
            logger.info(f"Результаты не найдены для {brand} / {part}")
            return None, None

        logger.info(f"Найдено {len(rows)} строк результатов")

        # Приоритет: сначала "в наличии" с доставкой 1 день
        for priority_search in [True, False]:
            for row in rows:
                try:
                    brand_td = row.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['brand'])
                    brand_in_row = brand_td.text.strip()

                    if not brand_matches(brand, brand_in_row):
                        continue

                    delivery_td = row.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['delivery'])
                    delivery_min = delivery_td.text.strip()

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
        # Попытка fallback поиска только по номеру детали
        try:
            fallback_url = f"{base_url}/search?pcode={part}"
            driver.get(fallback_url)
            logger.info(f"Fallback: загружена страница без бренда: {fallback_url}")

            try:
                WebDriverWait(driver, WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, SELECTORS['stparts']['captcha_img']))
                )
                logger.warning("Обнаружена капча на stparts.ru (fallback)")
                if not solve_image_captcha(driver):
                    raise Exception("Не удалось решить капчу (fallback)")
            except TimeoutException:
                pass

            WebDriverWait(driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, SELECTORS['stparts']['results_table']))
            )
            table = driver.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['results_table'])

            WebDriverWait(driver, WAIT_TIMEOUT).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, SELECTORS['stparts']['result_row'])) > 0
            )

            rows = table.find_elements(By.CSS_SELECTOR, SELECTORS['stparts']['result_row'])
            if not rows:
                logger.info(f"Fallback: результаты не найдены для {part}")
                return None, None

            logger.info(f"Fallback: найдено {len(rows)} строк результатов")

            for priority_search in [True, False]:
                for row in rows:
                    try:
                        brand_td = row.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['brand'])
                        brand_in_row = brand_td.text.strip()

                        if not brand_matches(brand, brand_in_row):
                            continue

                        delivery_td = row.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['delivery'])
                        delivery_min = delivery_td.text.strip()

                        if priority_search and not re.match(r"^1(\D|$)", delivery_min):
                            continue

                        price_text = row.find_element(By.CSS_SELECTOR, SELECTORS['stparts']['price']).text
                        price = parse_price(price_text)

                        if price is not None:
                            if priority_search:
                                logger.info(f"Fallback: найдено в наличии (бренд: {brand_in_row}, срок {delivery_min}): {price} ₽")
                            else:
                                logger.info(f"Fallback: найдено (бренд: {brand_in_row}, срок {delivery_min}): {price} ₽")
                            return price, delivery_min
                    except NoSuchElementException:
                        continue

            logger.info(f"Fallback: подходящие результаты не найдены для {part}")
            return None, None

        except TimeoutException:
            logger.error(f"Fallback Timeout при загрузке результатов для {part}")
            return None, None
        except Exception as e:
            logger.error(f"Fallback ошибка парсинга stparts для {part}: {e}")
            return None, None

    except Exception as e:
        logger.error(f"Ошибка парсинга stparts для {brand} / {part}: {e}")
        return None, None

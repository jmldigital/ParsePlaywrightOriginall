
# scraper_avtoformula.py
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from config import SELECTORS
from utils import logger, parse_price, brand_matches
import re

def scrape_avtoformula(driver, brand, part):
    """Парсинг данных с avtoformula.ru"""
    try:
        wait = WebDriverWait(driver, 15)
        driver.get("https://www.avtoformula.ru")

        article_field = wait.until(EC.element_to_be_clickable(
            (By.ID, SELECTORS['avtoformula']['article_field'])))
        article_field.clear()
        article_field.send_keys(part)
        logger.info(f"Введён артикул: {part}")

        search_btn = driver.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['search_button'])
        search_btn.click()

        wait.until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, SELECTORS['avtoformula']['results_table'])))
        
        rows = driver.find_elements(By.CSS_SELECTOR, 
            f"{SELECTORS['avtoformula']['results_table']} tr")
        
        if not rows or len(rows) < 2:
            logger.info(f"Результаты не найдены для {brand} / {part}")
            return None, None

        min_delivery = None
        min_price = None

        for row in rows[1:]:
            try:
                brand_td = row.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['brand_cell'])
                brand_in_row = brand_td.text.strip()
                
                if not brand_matches(brand, brand_in_row):
                    continue

                delivery_td = row.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['delivery_cell'])
                delivery_text = delivery_td.text.strip().split('/')[0].strip()
                
                try:
                    delivery_days = int(re.search(r'\d+', delivery_text).group())
                except (ValueError, AttributeError, TypeError):
                    continue

                price_td = row.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['price_cell'])
                price = parse_price(price_td.text.strip())
                
                if price is None:
                    continue

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
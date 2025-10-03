
# auth.py
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from config import SELECTORS, COOKIE_FILE
from utils import logger
import time

def save_cookies(driver, filepath=COOKIE_FILE):
    import json
    try:
        cookies = driver.get_cookies()
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(cookies, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ Куки сохранены в {filepath}")
    except Exception as e:
        logger.warning(f"❌ Не удалось сохранить куки: {e}")

def load_cookies(driver, filepath=COOKIE_FILE):
    import json
    from pathlib import Path
    if not Path(filepath).exists():
        return False
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            cookies = json.load(f)
        driver.get("https://www.avtoformula.ru")
        time.sleep(1)
        for cookie in cookies:
            if 'sameSite' in cookie and cookie['sameSite'] not in ['Strict', 'Lax', 'None']:
                cookie['sameSite'] = 'Lax'
            try:
                driver.add_cookie(cookie)
            except Exception as e:
                logger.debug(f"⚠️ Не удалось добавить куку {cookie.get('name')}: {e}")
        driver.refresh()
        logger.info(f"✅ Куки загружены из {filepath}")
        return True
    except Exception as e:
        logger.warning(f"❌ Ошибка при загрузке кук: {e}")
        return False

def is_logged_in(driver):
    """Проверяет, что пользователь внутри (поле артикула или 'выход' в странице)"""
    try:
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.ID, SELECTORS['avtoformula']['article_field']))
        )
        return True
    except:
        return False
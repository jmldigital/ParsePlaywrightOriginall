
# auth.py
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from config import SELECTORS, COOKIE_FILE
from utils import logger
import time
import json
from pathlib import Path
import threading

# --- Глобальные переменные для синхронизации ---
_login_lock = threading.Lock()
_global_login_done = False  # Флаг: кто-то уже успешно залогинился

def save_cookies(driver, filepath=COOKIE_FILE):
    """Сохраняет куки. Должно вызываться только один раз после успешного логина."""
    try:
        cookies = driver.get_cookies()
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(cookies, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ Куки сохранены в {filepath}")
    except Exception as e:
        logger.warning(f"❌ Не удалось сохранить куки: {e}")

def load_cookies(driver, filepath=COOKIE_FILE):
    """
    Загружает куки без ожидания полной загрузки страницы.
    Важно: не используем WebDriverWait здесь.
    """
    if not Path(filepath).exists():
        logger.debug(f"❌ Файл кук не найден: {filepath}")
        return False

    try:
        # Открываем базовый URL, чтобы можно было добавить куки
        driver.get("https://www.avtoformula.ru")

        # Удаляем текущие куки и добавляем свои
        driver.delete_all_cookies()

        with open(filepath, 'r', encoding='utf-8') as f:
            cookies = json.load(f)

        for cookie in cookies:
            # Удаляем problematic поля
            cookie.pop('sameSite', None)  # ← Удаляем sameSite — Chrome сам решит
            cookie.pop('httpOnly', None)  # ← Необязательно, но может помочь
            cookie.pop('expiry', None)    # ← Иногда мешает, особенно если просрочены

            try:
                driver.add_cookie(cookie)
            except Exception as e:
                # Логируем только если критично
                if 'Secure' in str(e) and cookie.get('secure'):
                    # Проблема: кука secure, но мы на http? (но у нас https)
                    pass
                logger.debug(f"⚠️ Пропущена кука {cookie.get('name')}: {e}")

        # Перезагружаем — теперь с куками
        driver.refresh()
        time.sleep(1)  # Даем время на обработку

        logger.info(f"✅ Куки загружены из {filepath}")
        return True
    except Exception as e:
        logger.warning(f"❌ Ошибка при загрузке кук: {e}")
        return False


def is_logged_in(driver):
    """
    Проверяет авторизацию по наличию фразы 'Вы авторизованы как'.
    Выводит в лог точный текст для уверенности.
    """
    try:
        # Ищем элемент по части текста
        element = driver.find_element(By.XPATH, "//span[contains(text(), 'Вы авторизованы как')]")
        
        if not element.is_displayed():
            logger.debug("❌ Элемент 'Вы авторизованы как' найден, но скрыт")
            return False

        # Получаем полный текст
        full_text = element.text.strip()
        logger.info(f"🟢 Найдена надпись об авторизации: '{full_text}'")

        # Можно дополнительно проверить, что внутри есть имя
        try:
            username_span = element.find_element(By.XPATH, ".//span")
            username = username_span.text.strip()
            if username:
                logger.info(f"🟢 Авторизован как: {username}")
            else:
                logger.warning("🟡 Нет имени пользователя внутри надписи")
        except Exception as e:
            logger.debug(f"⚠️ Не удалось найти имя пользователя в надписи: {e}")

        return True

    except Exception as e:
        logger.debug(f"❌ Элемент 'Вы авторизованы как' не найден: {e}")
        return False

def ensure_logged_in(driver, login, password):
    """
    Гарантирует, что драйвер залогинен.
    Выполняется только один раз для всех потоков.
    """
    global _global_login_done

    # Проверяем, не залогинились ли уже
    if _global_login_done:
        return True

    with _login_lock:
        # Двойная проверка внутри блока (защита от race condition)
        if _global_login_done:
            return True

        logger.info("🔐 Проверка авторизации...")

        # Пытаемся загрузить куки
        if load_cookies(driver) and is_logged_in(driver):
            logger.info("✅ Авторизация по кукам успешна")
            _global_login_done = True
            return True

        # Если не вышло — делаем ручной логин
        logger.info("🔄 Куки не помогли — выполняем ручной логин")
        if login_manually(driver, login, password):
            save_cookies(driver)  # Сохраняем куки один раз!
            _global_login_done = True
            logger.info("✅ Ручной логин успешен, куки сохранены")
            return True
        else:
            logger.error("❌ Не удалось авторизоваться")
            return False

# --- Отдельно выносим login_manually, чтобы избежать циклических импортов ---
def login_manually(driver, login, password):
    """Ручная авторизация на avtoformula.ru"""
    from config import SELECTORS
    try:
        driver.get("https://www.avtoformula.ru")
        wait = WebDriverWait(driver, 15)

        # Поле логина
        login_el = wait.until(EC.element_to_be_clickable((By.ID, SELECTORS['avtoformula']['login_field'])))
        login_el.clear()
        login_el.send_keys(login)

        # Поле пароля
        password_el = driver.find_element(By.ID, SELECTORS['avtoformula']['password_field'])
        password_el.clear()
        password_el.send_keys(password)

        # Кнопка входа
        submit_btn = driver.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['login_button'])
        submit_btn.click()

        # Ждём, пока форма логина исчезнет
        wait.until(EC.invisibility_of_element_located((By.ID, SELECTORS['avtoformula']['login_field'])))
        time.sleep(2)

        # Выбор режима A0
        smode_select = wait.until(EC.element_to_be_clickable((By.ID, SELECTORS['avtoformula']['smode_select'])))
        for option in smode_select.find_elements(By.TAG_NAME, "option"):
            if option.get_attribute("value") == "A0":
                option.click()
                break

        logger.info("✅ Ручной логин выполнен")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка ручного входа: {e}")
        return False
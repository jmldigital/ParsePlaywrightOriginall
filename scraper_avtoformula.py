import time
import re
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, WebDriverException, TimeoutException
from config import SELECTORS, AVTO_LOGIN, AVTO_PASSWORD
from utils import logger, parse_price, brand_matches
from auth import ensure_logged_in, is_logged_in
import os
from datetime import datetime

# ==================== РЕЖИМ ОТЛАДКИ ====================
# Для включения сохранения HTML и скриншотов при ошибках:
# 1. Раскомментируйте строку: os.makedirs(DEBUG_HTML_DIR, exist_ok=True)
# 2. Раскомментируйте код внутри функции save_page_source()
# 3. Закомментируйте строку: return None (в конце save_page_source)
# =======================================================

# Максимальное время ожидания (в секундах)
MAX_WAIT_SECONDS = 120
CHECK_INTERVAL = 0.5  # шаг проверки в секундах
AUTH_CHECK_INTERVAL = 10  # проверка авторизации каждые 10 секунд

# Папка для сохранения HTML при проблемах (используется только при отладке)
DEBUG_HTML_DIR = "debug_html"
# os.makedirs(DEBUG_HTML_DIR, exist_ok=True)  # Закомментировано - раскомментируйте вместе с save_page_source


def save_page_source(driver, brand, part, reason="timeout"):
    """Сохранить HTML страницы для отладки"""
    # try:
    #     timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #     filename = f"{DEBUG_HTML_DIR}/{reason}_{brand}_{part}_{timestamp}.html"
    #     
    #     with open(filename, "w", encoding="utf-8") as f:
    #         f.write(driver.page_source)
    #     
    #     logger.info(f"💾 HTML сохранён: {filename}")
    #     
    #     # Также сохраним скриншот
    #     screenshot_filename = filename.replace(".html", ".png")
    #     driver.save_screenshot(screenshot_filename)
    #     logger.info(f"📸 Скриншот сохранён: {screenshot_filename}")
    #     
    #     return filename
    # except Exception as e:
    #     logger.error(f"Ошибка сохранения HTML: {e}")
    #     return None
    
    # Закомментировано для продакшена - раскомментируйте при отладке
    return None


def check_if_logged_out(driver):
    """
    Быстрая проверка: не разлогинились ли мы?
    Ищет элемент регистрации как индикатор разлогина.
    """
    try:
        # Ищем ссылку на регистрацию
        reg_link = driver.find_elements(By.XPATH, "//a[@href='/registration.html']")
        if reg_link and any(link.is_displayed() for link in reg_link):
            logger.warning("🚪 Обнаружен разлогин - найдена ссылка 'зарегистрируйтесь'")
            return True
        
        # Дополнительная проверка: нет ли текста "зарегистрируйтесь"
        if "зарегистрируйтесь" in driver.page_source.lower():
            logger.warning("🚪 Обнаружен разлогин - найден текст 'зарегистрируйтесь'")
            return True
            
        return False
    except Exception as e:
        logger.debug(f"Ошибка проверки разлогина: {e}")
        return False


def check_page_state(driver):
    """Проверить состояние страницы и вернуть диагностическую информацию"""
    info = {
        "url": driver.current_url,
        "title": driver.title,
        "body_text_length": len(driver.find_element(By.TAG_NAME, "body").text),
    }
    
    # Проверка на загрузку/спиннеры
    try:
        loaders = driver.find_elements(By.CSS_SELECTOR, ".loader, .loading, .spinner, [class*='load']")
        info["loaders_found"] = len(loaders)
        info["loaders_visible"] = sum(1 for l in loaders if l.is_displayed())
    except:
        info["loaders_found"] = 0
        info["loaders_visible"] = 0
    
    # Проверка на модальные окна
    try:
        modals = driver.find_elements(By.CSS_SELECTOR, ".modal, .popup, [class*='modal'], [class*='popup']")
        info["modals_found"] = len(modals)
        info["modals_visible"] = sum(1 for m in modals if m.is_displayed())
    except:
        info["modals_found"] = 0
        info["modals_visible"] = 0
    
    # Проверка на наличие таблицы
    try:
        tables = driver.find_elements(By.TAG_NAME, "table")
        info["tables_count"] = len(tables)
    except:
        info["tables_count"] = 0
    
    # Проверка разлогина
    info["logged_out"] = check_if_logged_out(driver)
    
    return info


def handle_relogin(driver, brand, part, login, password):
    """
    Обработка разлогина: сохраняет состояние, пытается залогиниться и повторить поиск
    """
    logger.warning(f"🔄 Попытка повторного логина для {brand} / {part}")
    
    if not login or not password:
        logger.error("❌ Нет данных для ре-логина (проверьте AVTO_LOGIN и AVTO_PASSWORD в config.py)")
        return False
    
    # Сохраняем состояние до ре-логина (только при отладке)
    # save_page_source(driver, brand, part, "before_relogin")
    
    # Пытаемся залогиниться
    try:
        # Используем функцию из auth модуля
        from auth import load_cookies
        
        # Сначала пробуем куки
        if load_cookies(driver) and is_logged_in(driver):
            logger.info("✅ Ре-логин через куки успешен")
            return True
        
        # Если куки не помогли - полный логин
        if ensure_logged_in(driver, login, password):
            logger.info("✅ Ре-логин успешен")
            return True
        
        logger.error("❌ Ре-логин не удался")
        return False
        
    except Exception as e:
        logger.error(f"❌ Ошибка при ре-логине: {e}")
        return False


def repeat_search(driver, part):
    """Повторяет поиск после ре-логина"""
    try:
        # Возвращаемся на главную
        driver.get("https://www.avtoformula.ru")
        time.sleep(1)
        
        # Вводим артикул заново
        article_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, SELECTORS['avtoformula']['article_field']))
        )
        article_field.clear()
        article_field.send_keys(part)
        
        # Кликаем по поиску
        search_button = driver.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['search_button'])
        search_button.click()
        logger.info(f"🔄 Поиск повторён для артикула: {part}")
        
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка повтора поиска: {e}")
        return False


def scrape_avtoformula(driver, brand, part):
    """Парсинг данных с avtoformula.ru — ждёт до 2 минут, пока появятся результаты
    Автоматически использует логин/пароль из config.py при необходимости ре-логина"""
    
    # Получаем credentials из config
    login = AVTO_LOGIN
    password = AVTO_PASSWORD
    
    # Проверяем, что credentials настроены
    if not login or login == 'your_login_here':
        logger.warning("⚠️ AVTO_LOGIN не настроен в config.py - ре-логин будет недоступен")
        login = None
        password = None
    
    try:
        driver.get("https://www.avtoformula.ru")
        time.sleep(1)

        # Ввод артикула
        try:
            article_field = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, SELECTORS['avtoformula']['article_field']))
            )
            article_field.clear()
            article_field.send_keys(part)
            logger.info(f"Введён артикул: {part}")
        except TimeoutException:
            logger.error("❌ Не найдено поле для ввода артикула")
            save_page_source(driver, brand, part, "no_input_field")
            return None, None

        # Поиск
        try:
            search_button = driver.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['search_button'])
            search_button.click()
            logger.debug("Клик по кнопке поиска")
            time.sleep(1)  # Небольшая задержка после клика
        except NoSuchElementException:
            logger.error("❌ Не найдена кнопка поиска")
            save_page_source(driver, brand, part, "no_search_button")
            return None, None

        # --- Цикл ожидания результата ---
        start_time = time.time()
        status = None
        last_log_time = start_time
        last_auth_check_time = start_time
        check_count = 0
        relogin_attempted = False

        while True:
            check_count += 1
            elapsed = time.time() - start_time
            
            # Проверка авторизации каждые AUTH_CHECK_INTERVAL секунд
            if elapsed - (last_auth_check_time - start_time) >= AUTH_CHECK_INTERVAL:
                if check_if_logged_out(driver):
                    logger.warning(f"⚠️ Обнаружен разлогин на {elapsed:.1f}s")
                    
                    if not relogin_attempted and login and password:
                        relogin_attempted = True
                        
                        if handle_relogin(driver, brand, part, login, password):
                            # Повторяем поиск после успешного ре-логина
                            if repeat_search(driver, part):
                                # Сбрасываем таймеры
                                start_time = time.time()
                                last_log_time = start_time
                                last_auth_check_time = start_time
                                check_count = 0
                                logger.info("🔄 Поиск перезапущен после ре-логина")
                                continue
                            else:
                                logger.error("❌ Не удалось повторить поиск после ре-логина")
                                save_page_source(driver, brand, part, "relogin_search_failed")
                                return None, None
                        else:
                            logger.error("❌ Ре-логин не удался")
                            save_page_source(driver, brand, part, "relogin_failed")
                            return None, None
                    else:
                        if relogin_attempted:
                            logger.error("❌ Повторный разлогин после ре-логина")
                        else:
                            logger.error("❌ Разлогин обнаружен, но нет данных для логина")
                        save_page_source(driver, brand, part, "logged_out")
                        return None, None
                
                last_auth_check_time = time.time()
            
            # Логируем состояние каждые 10 секунд
            if elapsed - (last_log_time - start_time) >= 10:
                page_info = check_page_state(driver)
                logger.debug(f"⏱️ {elapsed:.1f}s | Проверок: {check_count} | Состояние: {page_info}")
                last_log_time = time.time()
            
            try:
                # Проверяем наличие сообщения "не найдено"
                if "К сожалению, в поставках данная деталь" in driver.page_source:
                    status = "no_results"
                    logger.info(f"🚫 Деталь не найдена на avtoformula для {brand} / {part}")
                    break
                
                # Ищем таблицу с результатами
                rows = driver.find_elements(By.CSS_SELECTOR, f"{SELECTORS['avtoformula']['results_table']} tr")
                
                if check_count % 20 == 0:  # Каждые 10 секунд (20 проверок * 0.5s)
                    logger.debug(f"Найдено строк в таблице: {len(rows)}")
                
                # Проверяем, что есть данные (больше 1 строки = есть заголовок + данные)
                if len(rows) > 1:
                    # Дополнительная проверка: есть ли реальные данные в строках
                    data_rows = [r for r in rows[1:] if r.text.strip()]
                    if data_rows:
                        status = "results"
                        logger.info(f"✅ Найдено строк с данными: {len(data_rows)}")
                        break
                    else:
                        logger.debug(f"Таблица найдена ({len(rows)} строк), но данные пустые")

            except WebDriverException as e:
                logger.debug(f"WebDriverException в ожидании (проверка {check_count}): {e}")

            # Проверка таймаута
            if elapsed > MAX_WAIT_SECONDS:
                logger.warning(f"⏰ Превышено время ожидания ({MAX_WAIT_SECONDS}s) для {brand} / {part}")
                logger.warning(f"Всего выполнено проверок: {check_count}")
                
                # Сохраняем детальную диагностику
                page_info = check_page_state(driver)
                logger.warning(f"Финальное состояние страницы: {page_info}")
                
                # Сохраняем HTML и скриншот
                save_page_source(driver, brand, part, "timeout")
                
                status = "timeout"
                break

            time.sleep(CHECK_INTERVAL)

        # --- Обработка статусов ---
        if status == "no_results":
            return None, None

        if status == "timeout" or status is None:
            logger.warning(f"⚠️ Истекло ожидание без результата для {brand} / {part}")
            return None, None

        # --- Обработка таблицы ---
        rows = driver.find_elements(By.CSS_SELECTOR, f"{SELECTORS['avtoformula']['results_table']} tr")
        if not rows or len(rows) < 2:
            logger.info(f"Результаты не найдены для {brand} / {part}")
            save_page_source(driver, brand, part, "no_data_in_table")
            return None, None

        min_delivery = None
        min_price = None
        rows_processed = 0
        rows_matched = 0

        for row in rows[1:]:
            rows_processed += 1
            try:
                brand_td = row.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['brand_cell'])
                brand_in_row = brand_td.text.strip()
                
                if not brand_matches(brand, brand_in_row):
                    logger.debug(f"Бренд не совпадает: '{brand}' != '{brand_in_row}'")
                    continue
                
                rows_matched += 1

                delivery_td = row.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['delivery_cell'])
                delivery_text = delivery_td.text.strip().split('/')[0].strip()
                try:
                    delivery_days = int(re.search(r'\d+', delivery_text).group())
                except (ValueError, AttributeError, TypeError):
                    logger.debug(f"Не удалось извлечь дни доставки из: '{delivery_text}'")
                    continue

                price_td = row.find_element(By.CSS_SELECTOR, SELECTORS['avtoformula']['price_cell'])
                price = parse_price(price_td.text.strip())
                if price is None:
                    logger.debug(f"Не удалось извлечь цену из: '{price_td.text.strip()}'")
                    continue

                if (
                    min_delivery is None or
                    delivery_days < min_delivery or
                    (delivery_days == min_delivery and price < min_price)
                ):
                    min_delivery = delivery_days
                    min_price = price

            except NoSuchElementException as e:
                logger.debug(f"Элемент не найден в строке: {e}")
                continue

        logger.info(f"Обработано строк: {rows_processed}, совпало по бренду: {rows_matched}")

        if min_delivery is not None:
            logger.info(f"✅ Найдено: {min_delivery} дней, цена {min_price} ₽")
            return min_price, f"{min_delivery} дней"

        logger.info(f"Подходящие результаты не найдены для {brand} / {part}")
        save_page_source(driver, brand, part, "no_matching_results")
        return None, None

    except Exception as e:
        logger.error(f"❗ Ошибка парсинга avtoformula для {brand} / {part}: {e}")
        save_page_source(driver, brand, part, "exception")
        return None, None
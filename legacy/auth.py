# auth_playwright_async.py
"""
Асинхронная авторизация для Playwright
Аналог auth_playwright.py, но с async/await
"""

from playwright.async_api import Page
from config import SELECTORS, COOKIE_FILE
from utils import logger
import json
from pathlib import Path
import asyncio
import threading

_login_lock = asyncio.Lock()
_global_login_done = False


async def save_cookies(page: Page, filepath: str = COOKIE_FILE):
    """Сохраняет куки после успешного логина"""
    try:
        cookies = await page.context.cookies()
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(cookies, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ Куки сохранены в {filepath}")
    except Exception as e:
        logger.warning(f"❌ Не удалось сохранить куки: {e}")


async def load_cookies(page: Page, filepath: str = COOKIE_FILE) -> bool:
    """Загружает куки из файла"""
    if not Path(filepath).exists():
        logger.debug(f"❌ Файл кук не найден: {filepath}")
        return False
    try:
        await page.goto("https://www.avtoformula.ru")
        with open(filepath, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        await page.context.add_cookies(cookies)
        await page.reload()
        await page.wait_for_timeout(1000)
        logger.info(f"✅ Куки загружены из {filepath}")
        return True
    except Exception as e:
        logger.warning(f"❌ Ошибка при загрузке кук: {e}")
        return False


async def is_logged_in(page: Page) -> bool:
    """Проверяет наличие надписи 'Вы авторизованы как'"""
    try:
        element = page.locator("xpath=//span[contains(text(), 'Вы авторизованы как')]")
        if await element.count() == 0:
            return False
        if not await element.is_visible():
            return False
        text = (await element.text_content() or "").strip()
        logger.info(f"🟢 Найдена надпись об авторизации: '{text}'")
        return True
    except Exception as e:
        logger.debug(f"Ошибка проверки авторизации: {e}")
        return False


async def login_manually(page: Page, login: str, password: str) -> bool:
    """Ручной логин на сайте AvtoFormula"""
    try:
        await page.goto("https://www.avtoformula.ru")

        login_field = page.locator(f"#{SELECTORS['avtoformula']['login_field']}")
        await login_field.wait_for(state="visible", timeout=15000)
        await login_field.fill(login)

        password_field = page.locator(f"#{SELECTORS['avtoformula']['password_field']}")
        await password_field.fill(password)

        submit_btn = page.locator(SELECTORS['avtoformula']['login_button'])
        await submit_btn.click()

        # Ждём исчезновения формы логина
        await login_field.wait_for(state="hidden", timeout=15000)
        await page.wait_for_timeout(2000)

        # Выбор режима без аналогов (A0)
        smode_select = page.locator(f"#{SELECTORS['avtoformula']['smode_select']}")
        await smode_select.wait_for(state="visible", timeout=15000)
        await smode_select.select_option("A0")

        logger.info("✅ Ручной логин выполнен")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка ручного входа: {e}")
        return False


async def ensure_logged_in(page: Page, login: str, password: str) -> bool:
    """Гарантирует, что мы авторизованы"""
    global _global_login_done
    if _global_login_done:
        return True

    async with _login_lock:
        if _global_login_done:
            return True
        logger.info("🔐 Проверка авторизации...")

        # Пробуем куки
        if await load_cookies(page) and await is_logged_in(page):
            logger.info("✅ Авторизация по кукам успешна")
            _global_login_done = True
            return True

        # Если не вышло — делаем ручной вход
        if await login_manually(page, login, password):
            await save_cookies(page)
            _global_login_done = True
            logger.info("✅ Авторизация успешна и куки сохранены")
            return True
        else:
            logger.error("❌ Не удалось авторизоваться")
            return False


async def check_if_logged_out(page: Page) -> bool:
    """Проверяет, не разлогинились ли мы"""
    try:
        reg_link = page.locator("a[href='/registration.html']")
        if await reg_link.count() > 0 and await reg_link.is_visible():
            logger.warning("🚪 Обнаружен разлогин (ссылка регистрации)")
            return True
        if "зарегистрируйтесь" in (await page.content()).lower():
            logger.warning("🚪 Обнаружен разлогин (по тексту)")
            return True
        return False
    except Exception as e:
        logger.debug(f"Ошибка проверки разлогина: {e}")
        return False


async def handle_relogin(page: Page,  login: str, password: str) -> bool:
    """Повторный логин при разлогине"""
    logger.warning(f"🔄 Попытка повторного логина для ")
    try:
        if await load_cookies(page) and await is_logged_in(page):
            logger.info("✅ Ре-логин через куки успешен")
            return True
        if await login_manually(page, login, password):
            await save_cookies(page)
            logger.info("✅ Ре-логин успешен")
            return True
        logger.error("❌ Ре-логин не удался")
        return False
    except Exception as e:
        logger.error(f"Ошибка при ре-логине: {e}")
        return False

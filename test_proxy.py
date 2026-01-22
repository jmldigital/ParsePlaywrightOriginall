import asyncio
import random
from playwright.async_api import async_playwright

# =========================================================
# НАСТРОЙКИ
# =========================================================

API_KEY_2CAPTCHA = "fedd764a51201126949cdb8800a9f6bb"
PROXY_USERNAME = "u038f310456a605c8"
PROXY_PASSWORD = "u038f310456a605c1"

# HTTP прокси (уже работают!)
HTTP_PROXIES = [
    "118.193.59.87:11477",
    "107.150.117.248:11446",
    "118.193.59.17:11151",
    "118.193.59.92:11329",
    "118.193.59.165:11196",
]


# =========================================================
# ОСНОВНАЯ ФУНКЦИЯ ПАРСИНГА ARMTEK
# =========================================================


async def scrape_armtek(search_query: str, headless: bool = False):
    """
    Парсинг Armtek через HTTP прокси

    Args:
        search_query: Артикул для поиска (например "15163-25010")
        headless: True - без окна, False - с окном браузера
    """

    proxy = random.choice(HTTP_PROXIES)

    print("=" * 60)
    print(f"🔍 Поиск на Armtek: {search_query}")
    print(f"📡 Прокси: {proxy}")
    print("=" * 60)

    async with async_playwright() as p:

        # Запуск браузера с HTTP прокси
        browser = await p.chromium.launch(
            headless=headless,
            proxy={
                "server": f"http://{proxy}",  # HTTP прокси
                "username": PROXY_USERNAME,
                "password": PROXY_PASSWORD,
            },
        )

        # Создаём контекст с реалистичными настройками
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="ru-RU",
            timezone_id="Europe/Moscow",
        )

        page = await context.new_page()

        # =============================================
        # 1. Проверяем IP через прокси
        # =============================================
        print("\n1️⃣ Проверка IP...")
        await page.goto("http://ip-api.com/json", timeout=30000)
        ip_content = await page.inner_text("body")
        print(f"   {ip_content}")

        # =============================================
        # 2. Заходим на Armtek
        # =============================================
        print("\n2️⃣ Загрузка Armtek...")
        url = f"https://armtek.ru/search?text={search_query}"

        await page.goto(url, timeout=60000, wait_until="domcontentloaded")
        print(f"   ✅ Страница загружена")

        # Ждём появления контента
        await asyncio.sleep(3)

        # =============================================
        # 3. Скриншот результатов
        # =============================================
        screenshot_path = f"armtek_{search_query}.png"
        await page.screenshot(path=screenshot_path, full_page=True)
        print(f"   📸 Скриншот: {screenshot_path}")

        # =============================================
        # 4. Получаем заголовок и HTML
        # =============================================
        title = await page.title()
        print(f"   📄 Заголовок: {title}")

        html = await page.content()
        print(f"   📊 HTML: {len(html)} символов")

        # =============================================
        # 5. Пробуем найти результаты поиска
        # =============================================
        print("\n3️⃣ Анализ результатов...")

        # Ищем карточки товаров (примерные селекторы)
        try:
            # Ждём появления результатов
            await page.wait_for_selector(
                ".search-results, .product-card, .catalog-item", timeout=10000
            )

            # Пробуем найти товары
            items = await page.query_selector_all(
                ".product-card, .catalog-item, [data-product]"
            )
            print(f"   📦 Найдено товаров: {len(items)}")

        except Exception as e:
            print(f"   ⚠️ Селекторы не найдены: {e}")
            # Возможно, нужна капча или другая структура страницы

        # =============================================
        # 6. Сохраняем HTML для анализа
        # =============================================
        html_path = f"armtek_{search_query}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"   💾 HTML сохранён: {html_path}")

        # Держим браузер открытым для проверки
        if not headless:
            print("\n⏳ Браузер открыт 30 секунд для просмотра...")
            await asyncio.sleep(30)

        await browser.close()

        print("\n" + "=" * 60)
        print("✅ ГОТОВО!")
        print("=" * 60)

        return html


# =========================================================
# ФУНКЦИЯ ДЛЯ МНОЖЕСТВЕННОГО ПОИСКА
# =========================================================


async def scrape_multiple(articles: list):
    """Поиск нескольких артикулов"""

    results = {}

    for article in articles:
        try:
            html = await scrape_armtek(article, headless=True)
            results[article] = {"status": "ok", "html_length": len(html)}
        except Exception as e:
            results[article] = {"status": "error", "error": str(e)}

        # Пауза между запросами
        await asyncio.sleep(2)

    return results


# =========================================================
# ЗАПУСК
# =========================================================


async def main():
    # Один артикул с видимым браузером
    await scrape_armtek("15163-25010", headless=False)

    # # Или несколько артикулов в фоне:
    # articles = ["15163-25010", "90915-YZZD2", "04152-YZZA1"]
    # results = await scrape_multiple(articles)
    # print(results)


if __name__ == "__main__":
    asyncio.run(main())

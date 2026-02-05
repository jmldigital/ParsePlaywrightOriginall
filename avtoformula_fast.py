import asyncio
import re
from typing import Tuple, Optional
from bs4 import BeautifulSoup
from playwright.async_api import Page
from config import SELECTORS, AVTO_LOGIN, AVTO_PASSWORD
from utils import parse_price, brand_matches


async def parse_avtoformula_price(
    page: Page, brand: str, part: str, logger
) -> Tuple[Optional[float], Optional[str]]:
    """
    TURBO HYBRID (Browser-Fetch): Выполняет HTTP-запрос через сетевой стек браузера.
    Это обходит блокировку 423 (Locked) и TLS-фингерпринтинг.
    """
    try:
        # 1. Проверяем, что браузер находится на домене (нужно для работы fetch)
        if "avtoformula.ru" not in page.url:
            await page.goto(
                "https://www.avtoformula.ru/", wait_until="networkidle", timeout=30000
            )

        # 2. Формируем URL поиска
        search_url = (
            f"https://www.avtoformula.ru/search.html?article={part}"
            f"&smode=A0&searchTemplate=default&delivery_time=0"
            f"&sort___search_results_by=final_price"
        )

        logger.debug(f"🔍 [AVTOFORMULA] Browser-Fetch: {part}")

        # 3. Выполняем асинхронный запрос внутри браузера
        # Это позволяет использовать куки и TLS-отпечаток реального браузера
        html = await page.evaluate(
            f"""
            async () => {{
                try {{
                    const response = await fetch('{search_url}');
                    if (!response.ok) return 'Error: ' + response.status;
                    return await response.text();
                }} catch (e) {{
                    return 'Error: ' + e.message;
                }}
            }}
        """
        )

        # Проверка на технические ошибки fetch
        if not html or html.startswith("Error:"):
            logger.error(f"❌ [AVTOFORMULA] Fetch failed for {part}: {html}")
            return None, None

        # 4. Проверка на капчу или блокировку
        if "ban_hc_code" in html or "captcha" in html.lower():
            logger.warning(f"🔒 [AVTOFORMULA] Капча на артикуле {part}")

            # Сохраняем дамп для отладки, если нужно
            # with open(f"debug_avto_{part}.html", "w", encoding="utf-8") as f: f.write(html)

            return "NeedCaptcha", "NeedCaptcha"

        # 5. Проверка на отсутствие в наличии
        if "К сожалению, в поставках" in html or "не найдено" in html.lower():
            logger.info(f"🚫 [AVTOFORMULA] {brand}/{part}: нет в наличии")
            return None, None

        # 6. Быстрый парсинг полученного HTML текста
        price, delivery = _parse_html_with_bs4(html, brand, part, logger)

        if price:
            logger.info(f"✅ [AVTOFORMULA] {brand}/{part}: {price} ₽ ({delivery})")
            return price, delivery
        else:
            logger.info(f"❌ [AVTOFORMULA] {brand}/{part}: не найдено в результатах")
            return None, None

    except Exception as e:
        logger.error(f"⚠️ [AVTOFORMULA] Browser-Fetch Error: {e}")
        return None, None


def _parse_html_with_bs4(
    html: str, brand: str, part: str, logger
) -> Tuple[Optional[float], Optional[str]]:
    """Молниеносный парсинг HTML с помощью BeautifulSoup"""
    try:
        soup = BeautifulSoup(html, "lxml")

        # Получаем таблицу результатов
        table_selector = SELECTORS["avtoformula"]["results_table"]
        table = soup.select_one(table_selector)

        if not table:
            return None, None

        rows = table.find_all("tr")
        min_price, min_delivery = None, None

        # Пропускаем заголовок (обычно первый tr)
        for row in rows:
            # Ищем ячейку бренда
            brand_td = row.select_one(SELECTORS["avtoformula"]["brand_cell"])
            if not brand_td:
                continue

            brand_text = brand_td.get_text(strip=True)
            if not brand_matches(brand, brand_text):
                continue

            # Ищем ячейки цены и срока
            price_td = row.select_one(SELECTORS["avtoformula"]["price_cell"])
            delivery_td = row.select_one(SELECTORS["avtoformula"]["delivery_cell"])

            if price_td and delivery_td:
                # Очистка цены
                price = parse_price(price_td.get_text(strip=True))
                # Очистка срока доставки
                delivery_match = re.search(r"\d+", delivery_td.get_text(strip=True))

                if price and delivery_match:
                    days = int(delivery_match.group())
                    # Логика выбора лучшего предложения (мин. срок, затем мин. цена)
                    if (
                        min_delivery is None
                        or days < min_delivery
                        or (days == min_delivery and price < min_price)
                    ):
                        min_delivery, min_price = days, price

        if min_price:
            return min_price, f"{min_delivery} дн."

    except Exception as e:
        logger.error(f"❌ [AVTOFORMULA] BS4 parsing error: {e}")

    return None, None

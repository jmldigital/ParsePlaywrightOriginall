import asyncio
import json
from typing import Optional, Tuple
from playwright.async_api import Page

REQUEST_DELAY = 2.0
_last_request_time = 0.0


def _parse_emex_response(
    body: str, brand: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    """Парсит emex.ru API, берет минимальную доставку."""
    if not body or len(body.strip()) < 2:
        logger.warning("[emex] Пустой body")
        return None, None

    try:
        data = json.loads(body.strip())
    except Exception as e:
        logger.error(f"[emex] JSON parse error: {e}")
        logger.error(f"[emex] RAW: {repr(body[:200])}")
        return None, None

    points = data.get("searchResult", {}).get("points", {}).get("list", [])
    if not points:
        logger.warning("[emex] Нет точек доставки")
        return None, None

    # 🔥 Берем точку с минимальной доставкой
    best_point = min(
        points,
        key=lambda p: int(
            p.get("bestDelivery", {}).get("delivery", {}).get("value", 999)
        ),
    )

    delivery_days = best_point.get("bestDelivery", {}).get("delivery", {}).get("value")
    price = best_point.get("bestDelivery", {}).get("price", {}).get("value")

    if price:
        logger.info(f"[emex] 💰 {price}₽, {delivery_days}дн (bestDelivery)")
        return str(price), str(delivery_days)

    logger.warning("[emex] Нет цены в bestDelivery")
    return None, None


async def scrape_emex(
    page: Page, brand: str, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    global _last_request_time

    try:
        logger.info(f"🔍 [emex] {brand} {part}")

        # Rate limit
        now = asyncio.get_event_loop().time()
        wait = REQUEST_DELAY - (now - _last_request_time)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_request_time = now

        result_data = {"status": None, "body": None}
        got_result = asyncio.Event()

        # 🔥 НАДЕЖНЫЙ АСИНХРОННЫЙ listener (как в adeo!)
        async def on_response(response):
            if "api/search/search" not in response.url:
                return
            status = response.status
            try:
                body = await response.text()
            except Exception:
                body = ""
            # logger.info(f"[emex] 📡 {status} len={len(body)}: {body[:100]}")
            if status == 200 and not got_result.is_set():
                result_data["status"] = status
                result_data["body"] = body
                got_result.set()

        # Регистрируем listener
        page.on("response", on_response)

        # Переходим на страницу
        url = f"https://emex.ru/products/{part}/{brand}"
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)  # Даем API загрузиться

        # Ждем API или капчу (ТОЧНО как adeo!)
        try:
            await asyncio.wait_for(got_result.wait(), timeout=10.0)
            # logger.info("[emex] ✅ API получен")
        except asyncio.TimeoutError:
            # Проверяем капчу
            has_captcha = await page.evaluate(
                '() => !!document.querySelector(\'iframe[src*="recaptcha"], .captcha, [class*="captcha"]\')'
            )
            logger.info(f"[emex] Капча: {has_captcha}")

            if has_captcha:
                logger.info("[emex] 🔒 NeedCaptcha")
                return "NeedCaptcha"
            else:
                logger.warning("[emex] ❌ API timeout")
                return None, None

        return _parse_emex_response(result_data["body"] or "", brand, logger)

    except Exception as e:
        logger.error(f"[emex] Ошибка: {e}")
        return None, None

"""
Adeo.pro — два варианта ответа API:
1. {"items": [...]} — берём best по delivery+price
2. {"brands": [...]} — берём по group_name == brand, цена из "cost"
"""

import asyncio
import json
from typing import Optional, Tuple
from playwright.async_api import Page

REQUEST_DELAY = 5.0
_last_request_time = 0.0


def _parse_response(
    body: str, brand: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    try:
        data = json.loads(body)
    except Exception as e:
        logger.error(f"[adeo] JSON parse error: {e}")
        return None, None

    # Вариант 1: {"items": [...]}
    if "items" in data:
        items = data.get("items") or []
        visible = [i for i in items if not i.get("hide")]
        if not visible:
            logger.warning(f"[adeo] items пустые")
            return None, None
        best = min(
            visible,
            key=lambda i: (
                int(i.get("due_date_avr") or i.get("due_date_true") or 9999)
                + int(i.get("additional_delivery_days") or 0),
                float(i.get("cost_client") or i.get("cost_sale") or 999999),
            ),
        )
        price = best.get("cost_client") or best.get("cost_sale")
        days = int(best.get("due_date_avr") or best.get("due_date_true") or 0) + int(
            best.get("additional_delivery_days") or 0
        )
        logger.info(f"[adeo] 💰 [items] {price} руб, {days} дн.")
        return str(price), str(days)

    # Вариант 2: {"brands": [...]}
    if "brands" in data:
        brands = data.get("brands") or []
        # Ищем совпадение по brand (case-insensitive)
        match = next(
            (b for b in brands if b.get("group_name", "").upper() == brand.upper()),
            None,
        )
        if not match:
            # Если нет точного совпадения — берём первый
            match = brands[0] if brands else None
            if match:
                logger.warning(
                    f"[adeo] Бренд {brand} не найден, берём {match.get('group_name')}"
                )
        if match:
            price = match.get("cost")
            logger.info(f"[adeo] 💰 [brands] {price} руб (нет данных о доставке)")
            return str(price), None
        logger.warning("[adeo] brands пустые")
        return None, None

    logger.warning(f"[adeo] Неизвестный формат. keys={list(data.keys())}")
    return None, None


# async def scrape_adeo(
#     page: Page, brand: str, part: str, logger
# ) -> Tuple[Optional[str], Optional[str]]:
#     global _last_request_time


#     try:
#         logger.info(f"🔍 [adeo] {brand} {part}")

#         now = asyncio.get_event_loop().time()
#         wait = REQUEST_DELAY - (now - _last_request_time)
#         if wait > 0:
#             await asyncio.sleep(wait)
#         _last_request_time = asyncio.get_event_loop().time()

#         result_data = {"status": None, "body": None}
#         got_result = asyncio.Event()

#         async def on_response(response):
#             if "papi/price_part" not in response.url:
#                 return
#             status = response.status
#             try:
#                 body = await response.text()
#             except Exception:
#                 body = ""
#             logger.info(f"[adeo] 📡 {status}")
#             if status == 200 and not got_result.is_set():
#                 result_data["status"] = status
#                 result_data["body"] = body
#                 got_result.set()

#         page.on("response", on_response)

#         url = f"https://adeo.pro/pn?pn={part}&brand={brand}"
#         await page.goto(url, wait_until="domcontentloaded", timeout=30000)
#         await asyncio.sleep(2)

#         if got_result.is_set():
#             logger.info("[adeo] ✅ 200 без капчи")
#         else:
#             has_captcha = await page.evaluate(
#                 """
#                 () => !!document.querySelector('iframe[src*="recaptcha/api2/anchor"]')
#             """
#             )
#             logger.info(f"[adeo] Капча: {has_captcha}")

#             if has_captcha:
#                 logger.info("[adeo] 🔒 Обнаружена reCAPTCHA → возвращаем NeedCaptcha")
#                 return "NeedCaptcha"  # ← ВОТ ЭТО!

#         return _parse_response(result_data["body"] or "", brand, logger)

#     except Exception as e:
#         logger.error(f"[adeo] Ошибка: {e}")
#         return None, None


async def scrape_adeo(page: Page, brand: str, part: str, logger):
    global _last_request_time

    try:
        logger.info(f"🔍 [adeo] {brand} {part}")

        # 1. 🔥 RATE LIMIT ПЕРЕД ВСЕМ
        now = asyncio.get_event_loop().time()
        wait = REQUEST_DELAY - (now - _last_request_time)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_request_time = now

        # 2. Инициализация
        result_data = {"status": None, "body": None}
        got_result = asyncio.Event()

        # 3. Listener
        async def on_response(response):
            if "papi/price_part" not in response.url:
                return
            status = response.status
            try:
                body = await response.text()
            except:
                body = ""
            logger.info(f"[adeo] 📡 {status} len={len(body)}")
            if status == 200 and not got_result.is_set():
                result_data["status"] = status
                result_data["body"] = body
                got_result.set()

        page.on("response", on_response)

        # 4. Запрос
        url = f"https://adeo.pro/pn?pn={part}&brand={brand}"
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)  # Дать API время

        # 5. 🔥 Ждем API или капчу
        try:
            await asyncio.wait_for(got_result.wait(), timeout=10.0)
            logger.info("[adeo] ✅ 200 без капчи")
        except asyncio.TimeoutError:
            has_captcha = await page.evaluate(
                "() => !!document.querySelector('iframe[src*=\"recaptcha/api2/anchor\"]')"
            )
            logger.info(f"[adeo] Капча: {has_captcha}")

            if has_captcha:
                logger.info("[adeo] 🔒 NeedCaptcha")
                return "NeedCaptcha"
            else:
                logger.warning(
                    f"[adeo] ❌ API timeout. Body: '{result_data['body'][:100]}'"
                )
                return None, None

        # 6. Парсим
        return _parse_response(result_data["body"] or "", brand, logger)

    except Exception as e:
        logger.error(f"[adeo] Ошибка: {e}")
        return None, None

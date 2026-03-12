"""
Adeo.pro — два варианта ответа API:
1. {"items": [...]} — берём best по delivery+price
2. {"brands": [...]} — берём по group_name == brand, цена из "cost"
"""

import asyncio
import json
from typing import Optional, Tuple
from playwright.async_api import Page

from config import API_KEY_2CAPTCHA

RECAPTCHA_SITEKEY = "6Ld0qCkTAAAAABsB8vqojZWYD9o_KLZvt6xF3x-l"
_captcha_semaphore = asyncio.Semaphore(1)
_last_request_time = 0.0
REQUEST_DELAY = 5.0


async def _get_captcha_token(logger) -> Optional[str]:
    async with _captcha_semaphore:
        try:
            from twocaptcha import TwoCaptcha

            logger.info("[adeo] 🔒 2captcha решает reCAPTCHA...")
            solver = TwoCaptcha(API_KEY_2CAPTCHA)
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: solver.recaptcha(
                    sitekey=RECAPTCHA_SITEKEY,
                    url="https://adeo.pro",
                ),
            )
            return result["code"]
        except Exception as e:
            logger.error(f"[adeo] ❌ {e}")
            return None


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


async def scrape_adeo(
    page: Page, brand: str, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    global _last_request_time

    try:
        logger.info(f"🔍 [adeo] {brand} {part}")

        now = asyncio.get_event_loop().time()
        wait = REQUEST_DELAY - (now - _last_request_time)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_request_time = asyncio.get_event_loop().time()

        result_data = {"status": None, "body": None}
        got_result = asyncio.Event()

        async def on_response(response):
            if "papi/price_part" not in response.url:
                return
            status = response.status
            try:
                body = await response.text()
            except Exception:
                body = ""
            logger.info(f"[adeo] 📡 {status}")
            if status == 200 and not got_result.is_set():
                result_data["status"] = status
                result_data["body"] = body
                got_result.set()

        page.on("response", on_response)

        url = f"https://adeo.pro/pn?pn={part}&brand={brand}"
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(1)

        if got_result.is_set():
            logger.info("[adeo] ✅ 200 без капчи")
        else:
            has_captcha = await page.evaluate(
                """
                () => !!document.querySelector('iframe[src*="recaptcha/api2/anchor"]')
            """
            )
            logger.info(f"[adeo] Капча: {has_captcha}")

            if has_captcha:

                async def wait_iframe():
                    await page.wait_for_selector(
                        'iframe[src*="recaptcha/api2/anchor"]', timeout=10000
                    )
                    await asyncio.sleep(0.5)

                _, captcha_token = await asyncio.gather(
                    wait_iframe(),
                    _get_captcha_token(logger),
                )
                if not captcha_token:
                    return None, None

                inject = await page.evaluate(
                    f"""
                    (token) => {{
                        const steps = [];
                        const ta = document.querySelector('textarea[name="g-recaptcha-response"]');
                        if (ta) {{
                            Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value')
                                .set.call(ta, token);
                            ta.dispatchEvent(new Event('input', {{bubbles: true}}));
                            ta.dispatchEvent(new Event('change', {{bubbles: true}}));
                            steps.push('textarea ok');
                        }}
                        const client = window.___grecaptcha_cfg?.clients?.[0];
                        if (client) {{
                            for (const key of ['bq', 'mc']) {{
                                const vue = client[key]?.__vue__;
                                if (vue?.verify) {{
                                    try {{ vue.verify(token); steps.push(key + ' ok'); }}
                                    catch(e) {{ steps.push(key + ' err: ' + e.message.substring(0,40)); }}
                                }}
                            }}
                        }}
                        return steps;
                    }}
                """,
                    captcha_token,
                )
                logger.info(f"[adeo] Inject: {inject}")

                try:
                    await asyncio.wait_for(got_result.wait(), timeout=20.0)
                except asyncio.TimeoutError:
                    logger.warning("[adeo] Таймаут после капчи")
                    return None, None
            else:
                try:
                    await asyncio.wait_for(got_result.wait(), timeout=10.0)
                except asyncio.TimeoutError:
                    logger.warning("[adeo] Нет капчи и нет 200")
                    return None, None

        return _parse_response(result_data["body"] or "", brand, logger)

    except Exception as e:
        logger.error(f"[adeo] Ошибка: {e}")
        return None, None

"""Stparts search extracted from the main.py / scraper_stparts.py workflow.

Preserves the source's routes, DOM selectors, brand token matching, OEM-only
fallback and image captcha submission. No Excel or price-adjuster dependencies.
"""

import asyncio
import base64
import re
from decimal import Decimal, InvalidOperation
from urllib.parse import quote, urlencode

import httpx
from playwright.async_api import TimeoutError as PlaywrightTimeout
from playwright.async_api import async_playwright

from .models import ItemError, SearchResult

TABLE = "table.globalResult.searchResultsSecondStep"
ROW = "tr.resultTr2"
NO_RESULTS = "div.fr-alert.fr-alert-warning.alert-noResults"
CAPTCHA = "img.captchaImg"


class SourceError(Exception):
    def __init__(self, code, message):
        self.code, self.message = code, message
        super().__init__(message)


def brand_matches(search, actual):
    # Existing matching rule; discard empty tokens to avoid false positives.
    def tokens(value):
        return set(filter(None, re.split(r"[\s\-/_,]+", value.casefold().strip())))

    return bool(tokens(search) & tokens(actual))


def delivery_days(text):
    value = text.strip().casefold()
    if value in {"в наличии", "сегодня", "наличие"}:
        return 0
    # A range uses its earliest day, as required by the price-selection contract. Unknown dates/hours are never interpreted as days.
    match = re.fullmatch(
        r"(\d+)(?:\s*[-–—]\s*(\d+))?(?:\s*(?:д|дн|дня|дней|день)\.?)?", value
    )
    if not match:
        return None
    return int(match.group(1))


def price_value(text):
    clean = re.sub(r"(?:руб\.?|₽|RUB)", "", text, flags=re.IGNORECASE)
    clean = re.sub(r"\s+", "", clean).replace(",", ".")
    if not re.fullmatch(r"\d+(?:\.\d{1,2})?", clean):
        return None
    try:
        price = Decimal(clean)
        return price.quantize(Decimal("0.01")) if price > 0 else None
    except InvalidOperation:
        return None


def select_offer(rows, brand, max_days):
    offers = []
    malformed = False
    for row in rows:
        if not brand_matches(brand, row["brand"]):
            continue
        days, price = delivery_days(row["delivery"]), price_value(row["price"])
        if days is None or price is None:
            malformed = True
            continue
        if days <= max_days:
            offers.append((price, days))
    # Do not claim a minimum price if an unparsed matching row could be cheaper.
    if malformed:
        raise SourceError(
            "SOURCE_FORMAT", "Source price or delivery format is not recognized"
        )
    if not offers:
        return SearchResult(status="not_found")
    price, days = min(offers)
    return SearchResult(status="found", price=format(price, ".2f"), delivery_days=days)


class StpartsScraper:
    def __init__(self, settings):
        self.settings = settings
        self.playwright = None
        self.browser = None
        self.lock = asyncio.Lock()

    async def get_browser(self):
        async with self.lock:
            if self.playwright is None:
                self.playwright = await async_playwright().start()
            if self.browser is None or not self.browser.is_connected():
                self.browser = await self.playwright.chromium.launch(
                    headless=self.settings.headless
                )
            return self.browser

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def solve_captcha(self, page):
        key = self.settings.captcha_api_key.get_secret_value()
        if not key:
            raise SourceError("CAPTCHA_REQUIRED", "Captcha solver is not configured")
        async with httpx.AsyncClient(timeout=20) as client:
            for _ in range(3):
                if not await page.locator(CAPTCHA).is_visible():
                    return
                image = await page.locator(CAPTCHA).screenshot()
                response = await client.post(
                    "https://2captcha.com/in.php",
                    data={
                        "key": key,
                        "method": "base64",
                        "body": base64.b64encode(image).decode(),
                        "json": 1,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                if payload.get("status") != 1:
                    raise SourceError(
                        "CAPTCHA_FAILED", "Captcha provider rejected the request"
                    )
                captcha_id = payload["request"]
                for _ in range(12):
                    await asyncio.sleep(5)
                    response = await client.get(
                        "https://2captcha.com/res.php",
                        params={
                            "key": key,
                            "action": "get",
                            "id": captcha_id,
                            "json": 1,
                        },
                    )
                    response.raise_for_status()
                    solved = response.json()
                    if solved.get("status") == 1:
                        await page.locator("input[name='captcha']").fill(
                            str(solved["request"]).upper().strip()
                        )
                        await page.locator("#captchaSubmitBtn").click()
                        await page.wait_for_timeout(2000)
                        break
                    if solved.get("request") != "CAPCHA_NOT_READY":
                        raise SourceError(
                            "CAPTCHA_FAILED",
                            "Captcha provider could not solve the challenge",
                        )
                else:
                    raise SourceError("CAPTCHA_TIMEOUT", "Captcha solution timed out")
            if await page.locator(CAPTCHA).is_visible():
                raise SourceError(
                    "CAPTCHA_FAILED", "Source did not accept the captcha solution"
                )

    async def read_page(self, page, url):
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        if response is not None and response.status >= 400:
            # The source may return its automatic browser-check page with HTTP 403.
            checking = await page.get_by_text(
                "Мы проверяем ваш браузер", exact=False
            ).is_visible()
            if response.status != 403 or not checking:
                raise SourceError(
                    "SOURCE_HTTP_ERROR", f"Source returned HTTP {response.status}"
                )
        await page.locator(
            f"{TABLE} {ROW}:visible, {NO_RESULTS}:visible, {CAPTCHA}:visible"
        ).first.wait_for(state="visible", timeout=15000)
        if await page.locator(CAPTCHA).is_visible():
            await self.solve_captcha(page)
            await page.locator(
                f"{TABLE} {ROW}:visible, {NO_RESULTS}:visible"
            ).first.wait_for(state="visible", timeout=15000)
        if await page.locator(NO_RESULTS).is_visible():
            return []
        await page.locator(TABLE).wait_for(state="visible", timeout=15000)
        # Read a single DOM snapshot, avoiding hundreds of separate browser calls.
        rows = await page.locator(f"{TABLE} {ROW}").evaluate_all("""rows => rows.map(row => ({
            brand: row.querySelector('td.resultBrand')?.textContent?.trim() ?? '',
            delivery: (row.querySelector('td.resultDeadline .resultDeadlineBlock .info')
                ?? row.querySelector('td.resultDeadline'))?.textContent?.trim() ?? '',
            price: row.querySelector('td.resultPrice')?.textContent?.trim() ?? ''
        }))""")
        if not rows:
            raise SourceError(
                "SOURCE_FORMAT", "Source table contains no recognizable offer rows"
            )
        return rows

    async def search(self, brand: str, oem: str, max_delivery_days: int = 2):
        """Search one position. Infrastructure exceptions are isolated by Runner."""
        browser = await self.get_browser()
        options = {}
        if self.settings.stparts_storage_state:
            options["storage_state"] = str(self.settings.stparts_storage_state)
        context = await browser.new_context(**options)
        try:
            page = await context.new_page()
            first = re.match(r"^[A-ZА-Я0-9]+", brand, re.IGNORECASE)
            first_brand = first.group() if first else brand
            urls = [
                f"https://stparts.ru/search/{quote(first_brand, safe='')}/{quote(oem, safe='')}",
                "https://stparts.ru/search?" + urlencode({"pcode": oem}),
            ]
            for index, url in enumerate(urls):
                try:
                    rows = await self.read_page(page, url)
                except PlaywrightTimeout:
                    if index == 0:
                        continue
                    raise SourceError(
                        "SOURCE_TIMEOUT", "Source results did not load"
                    ) from None
                result = select_offer(rows, brand, max_delivery_days)
                if result.status == "found" or index == 1:
                    return result
            raise SourceError("SOURCE_TIMEOUT", "Source results did not load")
        except httpx.HTTPError:
            # Provider request URLs contain the API key; never log those exceptions.
            return SearchResult(
                status="error",
                error=ItemError(
                    code="CAPTCHA_PROVIDER_ERROR",
                    message="Captcha provider is unavailable",
                ),
            )
        except SourceError as exc:
            return SearchResult(
                status="error", error=ItemError(code=exc.code, message=exc.message)
            )
        finally:
            await context.close()

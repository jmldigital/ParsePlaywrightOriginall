"""Stparts search extracted from the main.py / scraper_stparts.py workflow.

Preserves the source's routes, DOM selectors, brand token matching, OEM-only
fallback and image captcha submission. No Excel or price-adjuster dependencies.
"""

import asyncio
import base64
import json
import logging
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import quote, urlencode
from uuid import uuid4

import httpx
from playwright.async_api import TimeoutError as PlaywrightTimeout
from playwright.async_api import async_playwright

from .models import ItemError, SearchResult

log = logging.getLogger("uvicorn.error.price_api.source")

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
        self.diagnostics_lock = asyncio.Lock()

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
        log.info("captcha_detected")
        key = self.settings.captcha_api_key.get_secret_value()
        if not key:
            log.warning("captcha_missing_key")
            raise SourceError("CAPTCHA_REQUIRED", "Captcha solver is not configured")
        async with httpx.AsyncClient(timeout=20) as client:
            for _ in range(3):
                if not await page.locator(CAPTCHA).is_visible():
                    log.info("captcha_accepted")
                    return
                log.info("captcha_submit attempt=%s", _ + 1)
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
                    self.log_provider_error(payload.get("request"))
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
                        log.info("captcha_solution_received")
                        await page.locator("input[name='captcha']").fill(
                            str(solved["request"]).upper().strip()
                        )
                        await page.locator("#captchaSubmitBtn").click()
                        await page.wait_for_timeout(2000)
                        break
                    if solved.get("request") != "CAPCHA_NOT_READY":
                        self.log_provider_error(solved.get("request"))
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

    @staticmethod
    def log_provider_error(value):
        # Never print arbitrary provider payloads (they may include credentials).
        code = (
            value
            if isinstance(value, str) and re.fullmatch(r"ERROR_[A-Z0-9_]{1,80}", value)
            else "UNKNOWN"
        )
        log.warning("captcha_provider_rejected code=%s", code)

    async def save_diagnostic(self, page, url, error, status):
        if not self.settings.diagnostics_enabled:
            return
        try:
            async with asyncio.timeout(5):
                async with self.diagnostics_lock:
                    folder = self.settings.database_path.parent / "diagnostics"
                    await asyncio.to_thread(folder.mkdir, parents=True, exist_ok=True)
                    await asyncio.to_thread(self.prune_diagnostics, folder)
                    stem = (
                        datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
                        + "-"
                        + uuid4().hex[:12]
                    )
                    metadata = {
                        "time": datetime.now(UTC).isoformat(),
                        "url": url,
                        "final_url": page.url,
                        "http_status": status,
                        "error": error,
                    }
                    await asyncio.to_thread(
                        (folder / (stem + ".json")).write_text,
                        json.dumps(metadata, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    # Export rendered markup without scripts or entered form values.
                    html = await page.locator("html").evaluate("""element => {
                        const copy = element.cloneNode(true);
                        copy.querySelectorAll('script').forEach(el => el.remove());
                        copy.querySelectorAll('input').forEach(el => el.removeAttribute('value'));
                        copy.querySelectorAll('textarea').forEach(el => el.textContent = '');
                        return copy.outerHTML;
                    }""")
                    html = html[:2000000]
                    await asyncio.to_thread(
                        (folder / (stem + ".html")).write_text, html, encoding="utf-8"
                    )
                    try:
                        await page.screenshot(
                            path=str(folder / (stem + ".png")),
                            timeout=2000,
                            mask=[page.locator("input, textarea")],
                        )
                    except Exception as exc:
                        metadata["screenshot_error"] = type(exc).__name__
                    await asyncio.to_thread(
                        (folder / (stem + ".json")).write_text,
                        json.dumps(metadata, ensure_ascii=False),
                        encoding="utf-8",
                    )
                    await asyncio.to_thread(self.prune_diagnostics, folder)
                    log.warning(
                        "source_diagnostic sample=%s error=%s http=%s",
                        folder / stem,
                        error,
                        status,
                    )
        except Exception as exc:
            # Diagnostics must not replace the original source error.
            log.warning("source_diagnostic_failed type=%s", type(exc).__name__)

    def prune_diagnostics(self, folder):
        # Only our generated bundles; bounded storage, including partial captures.
        stems = sorted(
            {
                f.stem
                for f in folder.iterdir()
                if re.fullmatch(r"\d{8}T\d{6}(?:\d{6})?-[a-f0-9]{12}", f.stem)
            }
        )
        for stem in stems[: -self.settings.diagnostics_max_samples]:
            for extension in (".html", ".png", ".json"):
                (folder / (stem + extension)).unlink(missing_ok=True)

    async def read_page(self, page, url):
        status = None

        def response_received(response):
            nonlocal status
            if (
                response.request.is_navigation_request()
                and response.frame == page.main_frame
            ):
                status = response.status
                log.info("source_navigation http=%s url=%r", status, response.url)

        page.on("response", response_received)
        try:
            return await self._read_page(page, url)
        except asyncio.CancelledError:
            await self.save_diagnostic(page, url, "CANCELLED_OR_ITEM_TIMEOUT", status)
            raise
        except Exception as exc:
            await self.save_diagnostic(
                page, url, getattr(exc, "code", type(exc).__name__), status
            )
            raise
        finally:
            page.remove_listener("response", response_received)

    async def _read_page(self, page, url):
        response = await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        if response is not None and response.status >= 400:
            # The source may return its automatic browser-check page with HTTP 403.
            checking = await page.get_by_text(
                "Мы проверяем ваш браузер", exact=False
            ).is_visible()
            captcha = await page.locator(CAPTCHA).is_visible()
            ready = await page.locator(
                f"{TABLE} {ROW}:visible, {NO_RESULTS}:visible"
            ).count()
            if response.status != 403 or not (checking or captcha or ready):
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

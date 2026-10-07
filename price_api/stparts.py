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
from urllib.parse import parse_qs, quote, urlencode, urlparse
from uuid import uuid4

import httpx
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeout
from playwright.async_api import async_playwright

from .models import ItemError, SearchResult

log = logging.getLogger("uvicorn.error.price_api.source")

TABLE = "table.globalResult.searchResultsSecondStep"
ROW = "tr.resultTr2"
NO_RESULTS = "div.fr-alert.fr-alert-warning.alert-noResults"
RESULTS = f"{TABLE} {ROW}:visible, {NO_RESULTS}:visible"
CAPTCHA = "img.captchaImg"
CAPTCHA_PANEL = "#captcha"
CAPTCHA_INPUT = "input[name='captcha']"
CAPTCHA_SUBMIT = "#captchaSubmitBtn"
CAPTCHA_RELOAD = "a.captchaReload"
# The image itself gets its src from the site's browser check, so the panel and the
# input field are the reliable signals that the source is asking for a code.
CAPTCHA_STATE = f"{CAPTCHA}:visible, {CAPTCHA_PANEL}:visible, {CAPTCHA_INPUT}:visible"
RECAPTCHA_FORM = "form#formSearchLimitCaptcha"
RECAPTCHA_FRAME = (
    "iframe[src*='recaptcha/api2/anchor'], iframe[src*='recaptcha/enterprise/anchor']"
)
RECAPTCHA = f"{RECAPTCHA_FORM}:visible, {RECAPTCHA_FRAME}:visible"
# The limit form can be in the DOM before Google renders its widget, so presence is
# checked separately from visibility.
RECAPTCHA_PRESENT = f"{RECAPTCHA_FORM}, {RECAPTCHA_FRAME}, .g-recaptcha[data-sitekey]"
BROWSER_CHECK = "Мы проверяем ваш браузер"
# The source's defence service replaces the page with this interstitial for a flagged client.
BLOCKED = "text=Access Restricted"
BLOCKED_HOST = "nodacdn.net"


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
        # One paid challenge at a time: the old project solved captchas through a single slot.
        self.captcha_lock = asyncio.Lock()
        self.source_state = None
        # Bumped whenever the passed-check session is refreshed, so a waiting worker can
        # reuse it instead of paying for the same challenge again.
        self.source_state_version = 0

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

    def captcha_key(self):
        key = self.settings.captcha_api_key.get_secret_value()
        if not key:
            log.warning("captcha_missing_key")
            raise SourceError("CAPTCHA_REQUIRED", "Captcha solver is not configured")
        # RuCaptcha issues 32-character keys; a different length can only waste the item timeout.
        if len(key) != 32:
            log.warning("captcha_key_length_unexpected length=%s", len(key))
        return key

    def captcha_endpoint(self, name):
        base = self.settings.captcha_api_url.strip().rstrip("/")
        if not base:
            raise SourceError(
                "CAPTCHA_REQUIRED", "Captcha provider URL is not configured"
            )
        return f"{base}/{name}"

    def captcha_deadline(self):
        return (
            asyncio.get_running_loop().time()
            + self.settings.captcha_solve_timeout_seconds
        )

    async def provider_solution(self, client, key, request_data, deadline):
        """Run one in.php/res.php task; the flow is the provider's documented one."""
        while True:
            response = await client.post(
                self.captcha_endpoint("in.php"),
                data={**request_data, "key": key, "json": 1},
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("status") == 1:
                break
            error = payload.get("request")
            # The provider queues tasks when all its workers are busy.
            if error == "ERROR_NO_SLOT_AVAILABLE" and self.before(deadline):
                await asyncio.sleep(self.settings.captcha_poll_interval_seconds)
                continue
            self.log_provider_error(error)
            raise SourceError(
                "CAPTCHA_FAILED", "Captcha provider rejected the challenge"
            )
        task_id = payload["request"]
        while self.before(deadline):
            await asyncio.sleep(self.settings.captcha_poll_interval_seconds)
            # POST keeps the API key out of the request line, which clients log.
            response = await client.post(
                self.captcha_endpoint("res.php"),
                data={"key": key, "action": "get", "id": task_id, "json": 1},
            )
            response.raise_for_status()
            solved = response.json()
            if solved.get("status") == 1:
                return str(solved["request"])
            if solved.get("request") != "CAPCHA_NOT_READY":
                self.log_provider_error(solved.get("request"))
                raise SourceError(
                    "CAPTCHA_FAILED", "Captcha provider could not solve the challenge"
                )
        raise SourceError("CAPTCHA_TIMEOUT", "Captcha solution timed out")

    @staticmethod
    def before(deadline):
        return asyncio.get_running_loop().time() < deadline

    async def recaptcha_sitekey(self, page):
        """Wait for the widget, then read the site key the provider needs as googlekey.

        The source serves the search-limit form before Google's script renders the
        widget, and its own browser check can reload the page meanwhile.
        """
        deadline = (
            asyncio.get_running_loop().time() + self.settings.captcha_wait_seconds
        )
        while self.before(deadline):
            for selector, attribute in (
                (".g-recaptcha[data-sitekey]", "data-sitekey"),
                (f"{RECAPTCHA_FORM} [data-sitekey]", "data-sitekey"),
                (RECAPTCHA_FRAME, "src"),
            ):
                try:
                    locator = page.locator(selector).first
                    if not await locator.count():
                        continue
                    value = await locator.get_attribute(attribute, timeout=2000)
                except PlaywrightError:
                    continue
                if attribute == "src":
                    value = parse_qs(urlparse(value or "").query).get("k", [None])[0]
                if value:
                    return value
            await asyncio.sleep(0.3)
        return None

    async def submit_recaptcha_token(self, page, token):
        """Fill the site's g-recaptcha-response field and submit its search-limit form."""
        form = page.locator(RECAPTCHA_FORM).first
        if not await form.count():
            raise SourceError("CAPTCHA_FORMAT", "reCAPTCHA form is missing")
        await form.evaluate(
            """(form, token) => {
                const field = form.querySelector('[name="g-recaptcha-response"]')
                    || document.querySelector('[name="g-recaptcha-response"]');
                if (field) {
                    const setter = Object.getOwnPropertyDescriptor(
                        HTMLTextAreaElement.prototype, 'value').set;
                    setter.call(field, token);
                    field.dispatchEvent(new Event('input', {bubbles: true}));
                    field.dispatchEvent(new Event('change', {bubbles: true}));
                }
                const button = form.querySelector(
                    'button[type="submit"], input[type="submit"], button');
                if (button) { button.click(); } else { form.requestSubmit(); }
            }""",
            token,
        )

    async def recaptcha_present(self, page):
        return await page.locator(RECAPTCHA_PRESENT).count() > 0

    async def image_captcha_present(self, page):
        return await page.locator(CAPTCHA_STATE).count() > 0

    async def captcha_present(self, page):
        return await self.recaptcha_present(page) or await self.image_captcha_present(
            page
        )

    async def results_present(self, page):
        return await page.locator(RESULTS).count() > 0

    async def blocked(self, page):
        """The defence service answers a flagged client with an interstitial, not results."""
        try:
            if BLOCKED_HOST in urlparse(page.url).netloc:
                return True
            return await page.get_by_text("Access Restricted", exact=False).is_visible()
        except PlaywrightTimeout:
            return False

    async def wait_for_results(self, page, timeout=None):
        """Wait until the page shows offers or the explicit no-results alert."""
        try:
            await page.locator(RESULTS).first.wait_for(
                state="visible",
                timeout=timeout or self.settings.captcha_wait_seconds * 1000,
            )
            return True
        except PlaywrightTimeout:
            return False

    async def wait_for_captcha_image(self, page):
        """The site fills img.captchaImg.src from JS after its browser check; wait for it."""
        timeout = self.settings.captcha_wait_seconds * 1000
        image = page.locator(CAPTCHA).first
        try:
            await image.wait_for(state="visible", timeout=timeout)
            await page.wait_for_function(
                """selector => {
                    const image = document.querySelector(selector);
                    return Boolean(image) && image.complete && image.naturalWidth > 0;
                }""",
                arg=CAPTCHA,
                timeout=timeout,
            )
            return True
        except PlaywrightTimeout:
            return False

    async def store_source_state(self, page):
        """Keep the session that passed the check, so later positions reuse it."""
        self.source_state = await page.context.storage_state()
        self.source_state_version += 1

    async def adopt_source_state(self, page, state_version):
        """Reuse the session another worker already passed the check with."""
        if self.source_state is None or self.source_state_version <= state_version:
            return False
        cookies = self.source_state.get("cookies") or []
        if not cookies:
            return False
        try:
            await page.context.clear_cookies()
            await page.context.add_cookies(cookies)
            await page.reload(wait_until="domcontentloaded", timeout=30000)
        except PlaywrightError:
            return False
        if await self.wait_for_results(page, timeout=10000):
            log.info("captcha_session_reused")
            return True
        return False

    async def solve_captcha(self, page):
        """Solve the classic image CAPTCHA with the provider's base64 method."""
        log.info("captcha_detected type=image")
        key = self.captcha_key()
        async with self.captcha_lock, httpx.AsyncClient(timeout=30) as client:
            for attempt in range(1, self.settings.captcha_max_attempts + 1):
                if not await self.image_captcha_present(page):
                    log.info("captcha_accepted type=image")
                    return
                if not await self.wait_for_captcha_image(page):
                    raise SourceError(
                        "CAPTCHA_FORMAT", "Captcha image did not load in time"
                    )
                log.info("captcha_submit type=image attempt=%s", attempt)
                image = await page.locator(CAPTCHA).screenshot()
                solution = await self.provider_solution(
                    client,
                    key,
                    {"method": "base64", "body": base64.b64encode(image).decode()},
                    self.captcha_deadline(),
                )
                log.info("captcha_solution_received type=image attempt=%s", attempt)
                await page.locator(CAPTCHA_INPUT).fill(solution.upper().strip())
                await page.locator(CAPTCHA_SUBMIT).click()
                if await self.wait_for_results(page):
                    await self.store_source_state(page)
                    log.info("captcha_accepted type=image")
                    return
                log.warning("captcha_rejected type=image attempt=%s", attempt)
                reload_link = page.locator(CAPTCHA_RELOAD).first
                if await reload_link.count():
                    # Ask the source for a fresh code instead of resubmitting the old one.
                    await reload_link.click()
                    await self.wait_for_captcha_image(page)
            raise SourceError(
                "CAPTCHA_FAILED", "Source did not accept the captcha solution"
            )

    async def solve_recaptcha(self, page, state_version=0):
        """Solve the search-limit reCAPTCHA v2 with the provider's userrecaptcha method."""
        log.info("captcha_detected type=recaptcha_v2")
        await self.save_diagnostic(page, page.url, "RECAPTCHA_DETECTED", 403)
        key = self.captcha_key()
        async with self.captcha_lock:
            if await self.adopt_source_state(page, state_version):
                return
        sitekey = await self.recaptcha_sitekey(page)
        if not sitekey:
            if await self.results_present(page):
                # The form was in the DOM but the source rendered results instead.
                log.info("captcha_not_required type=recaptcha_v2")
                return
            raise SourceError("CAPTCHA_FORMAT", "reCAPTCHA site key not found")
        pageurl = page.url
        user_agent = await page.evaluate("navigator.userAgent")
        async with self.captcha_lock, httpx.AsyncClient(timeout=30) as client:
            deadline = self.captcha_deadline()
            for attempt in range(1, self.settings.captcha_max_attempts + 1):
                log.info("captcha_submit type=recaptcha_v2 attempt=%s", attempt)
                token = await self.provider_solution(
                    client,
                    key,
                    {
                        "method": "userrecaptcha",
                        "googlekey": sitekey,
                        "pageurl": pageurl,
                        "userAgent": user_agent,
                    },
                    deadline,
                )
                log.info(
                    "captcha_solution_received type=recaptcha_v2 attempt=%s", attempt
                )
                await self.submit_recaptcha_token(page, token)
                if await self.wait_for_results(page):
                    await self.store_source_state(page)
                    log.info("captcha_accepted type=recaptcha_v2 attempt=%s", attempt)
                    return
                if not await self.recaptcha_present(page) or not self.before(deadline):
                    break
                log.warning("captcha_rejected type=recaptcha_v2 attempt=%s", attempt)
            raise SourceError(
                "CAPTCHA_FAILED", "Source did not accept the reCAPTCHA solution"
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

    async def read_page(self, page, url, state_version=0):
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
            return await self._read_page(page, url, state_version)
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

    async def _read_page(self, page, url, state_version=0):
        try:
            response = await page.goto(
                url, wait_until="domcontentloaded", timeout=30000
            )
        except PlaywrightTimeout:
            raise
        except PlaywrightError:
            # The defence service drops the connection instead of answering.
            raise SourceError(
                "SOURCE_CONNECTION_ERROR", "Source refused the connection"
            ) from None
        if response is not None and response.status >= 400:
            # The source may return its automatic browser-check page with HTTP 403, and
            # its search-limit form can be in the DOM before Google renders the widget.
            checking = await page.get_by_text(BROWSER_CHECK, exact=False).is_visible()
            captcha = await self.captcha_present(page)
            ready = await page.locator(RESULTS).count()
            blocked = await self.blocked(page)
            if response.status != 403 or not (checking or captcha or ready or blocked):
                raise SourceError(
                    "SOURCE_HTTP_ERROR", f"Source returned HTTP {response.status}"
                )
        if await self.blocked(page):
            # A flagged client gets this page instead of the source's own CAPTCHA.
            raise SourceError(
                "SOURCE_BLOCKED", "Source answered with its access-restriction page"
            )
        # One of four states: offers, no results, image CAPTCHA or the reCAPTCHA form.
        try:
            await page.locator(
                f"{RESULTS}, {CAPTCHA_STATE}, {RECAPTCHA}"
            ).first.wait_for(state="visible", timeout=15000)
        except PlaywrightTimeout:
            if await self.blocked(page):
                raise SourceError(
                    "SOURCE_BLOCKED",
                    "Source answered with its access-restriction page",
                ) from None
            if not await self.recaptcha_present(page):
                raise
            # The limit form is there; solve_recaptcha waits for the widget itself.
        for _round in range(3):
            # Both challenges are page states rather than separate events, and the page
            # can move from the browser check to either one of them.
            if await self.results_present(page):
                break
            if await self.image_captcha_present(page):
                await self.solve_captcha(page)
                continue
            if await self.recaptcha_present(page):
                await self.solve_recaptcha(page, state_version)
                continue
            await asyncio.sleep(0.5)
        await page.locator(RESULTS).first.wait_for(state="visible", timeout=15000)
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
        if self.source_state is not None:
            options["storage_state"] = self.source_state
        elif self.settings.stparts_storage_state:
            options["storage_state"] = str(self.settings.stparts_storage_state)
        context = await browser.new_context(**options)
        try:
            page = await context.new_page()
            state_version = self.source_state_version
            first = re.match(r"^[A-ZА-Я0-9]+", brand, re.IGNORECASE)
            first_brand = first.group() if first else brand
            urls = [
                f"https://stparts.ru/search/{quote(first_brand, safe='')}/{quote(oem, safe='')}",
                "https://stparts.ru/search?" + urlencode({"pcode": oem}),
            ]
            for index, url in enumerate(urls):
                try:
                    rows = await self.read_page(page, url, state_version)
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

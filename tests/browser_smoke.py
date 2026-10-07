"""Run inside the built image with Python. All source requests are intercepted."""

import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import httpx
from pydantic import SecretStr

from price_api.settings import Settings
from price_api.stparts import StpartsScraper

TABLE = """<table class="globalResult searchResultsSecondStep">
<tr class="resultTr2"><td class="resultBrand">Toyota</td><td class="resultDeadline">1</td><td class="resultPrice">500</td></tr>
<tr class="resultTr2"><td class="resultBrand">Toyota</td><td class="resultDeadline"><div class="resultDeadlineBlock"><div class="info">1 - 2 дня</div><div class="warning">Заказ до 15:45</div></div></td><td class="resultPrice">250</td></tr>
<tr class="resultTr2"><td class="resultBrand">Toyota</td><td class="resultDeadline">3</td><td class="resultPrice">100</td></tr>
</table>"""
EMPTY = '<div class="fr-alert fr-alert-warning alert-noResults">No results</div>'
CAPTCHA = '<img class="captchaImg" src="data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==">'
# The live page shows the panel and the input before its script fills img.captchaImg.src.
PENDING_CAPTCHA = (
    '<div id="captcha"><img class="captchaImg" alt="">'
    '<a href="#" class="captchaReload">Обновить код</a>'
    '<input type="text" name="captcha" id="captchaSubmitInput">'
    '<input type="button" id="captchaSubmitBtn" value="Отправить"></div>'
)
RECAPTCHA = (
    '<form id="formSearchLimitCaptcha" method="POST">'
    '<iframe src="https://www.google.com/recaptcha/api2/anchor?k=test-site-key">'
    '</iframe><textarea name="g-recaptcha-response"></textarea>'
    '<input type="hidden" name="isSearchLimitCaptcha" value="1"></form>'
)
# The source serves the limit form first; Google's widget appears later.
RECAPTCHA_FORM_ONLY = (
    '<form id="formSearchLimitCaptcha" method="POST">'
    '<textarea name="g-recaptcha-response"></textarea>'
    '<input type="hidden" name="isSearchLimitCaptcha" value="1"></form>'
)
RECAPTCHA_LATE = (
    RECAPTCHA_FORM_ONLY
    + "<script>setTimeout(() => {const frame = document.createElement('iframe');"
    "frame.src = 'https://www.google.com/recaptcha/api2/anchor?k=test-site-key';"
    "document.getElementById('formSearchLimitCaptcha').prepend(frame);}, 300);</script>"
)
FAKE_KEY = "fake-provider-key" + "0" * 15


async def main():
    temporary = tempfile.TemporaryDirectory()
    scraper = StpartsScraper(
        Settings(
            _env_file=None,
            api_token="test-only-token-at-least-24-characters",
            headless=True,
            database_path=Path(temporary.name) / "jobs.sqlite3",
            diagnostics_max_samples=2,
            captcha_poll_interval_seconds=0.05,
            captcha_wait_seconds=1,
        )
    )
    browser = await scraper.get_browser()
    real_new_context = browser.new_context
    mode = "found"
    contexts = []
    resubmits = [0]  # The retry scenario rejects the first form submission.

    async def new_context(**kwargs):
        context = await real_new_context(**kwargs)
        contexts.append(context)

        async def handler(route):
            if "google.com" in route.request.url:
                await route.fulfill(status=200, body="")
                return
            html = TABLE
            status = 200
            if mode == "none":
                html = EMPTY
            elif mode == "fallback" and "pcode=" not in route.request.url:
                html = EMPTY
            elif mode in {"captcha", "captcha403", "solved403", "rejected403"}:
                status = 200 if mode == "captcha" else 403
                html = CAPTCHA
                if mode in {"solved403", "rejected403"}:
                    html += "<input name='captcha'><button id='captchaSubmitBtn'>Submit</button>"
                    html += (
                        "<script>document.getElementById('captchaSubmitBtn').onclick = () => {document.body.innerHTML = "
                        + json.dumps(TABLE)
                        + ";};</script>"
                    )
            elif mode == "pendingCaptcha":
                status = 403
                html = PENDING_CAPTCHA
            elif mode == "blocked":
                status = 200
                html = (
                    "<h1>Access Restricted</h1>"
                    "<p>Please complete the quick security check.</p>"
                )
            elif mode == "challenge":
                status = 403
                html = (
                    "<div>Мы проверяем ваш браузер</div><script>setTimeout(() => {document.body.innerHTML = "
                    + json.dumps(TABLE)
                    + ";}, 100);</script>"
                )
            elif mode in {"recaptcha403", "recaptchaRetry", "recaptchaLate"}:
                if route.request.method == "POST":
                    post_data = route.request.post_data or ""
                    assert "g-recaptcha-response=ABCD" in post_data
                    assert "isSearchLimitCaptcha=1" in post_data
                    if mode == "recaptchaRetry" and resubmits[0] == 0:
                        resubmits[0] = 1
                        status = 403
                        html = RECAPTCHA
                    else:
                        html = TABLE
                else:
                    status = 403
                    html = RECAPTCHA_LATE if mode == "recaptchaLate" else RECAPTCHA
            elif mode == "recaptchaPending":
                status = 403
                html = RECAPTCHA_FORM_ONLY
            elif mode == "reset":
                await route.abort("connectionreset")
                return
            elif mode == "forbidden":
                status = 403
                html = "<h1>Forbidden</h1>"
            await route.fulfill(
                status=status, content_type="text/html; charset=utf-8", body=html
            )

        await context.route("**/*", handler)
        return context

    browser.new_context = new_context
    try:
        for mode in [
            "found",
            "none",
            "fallback",
            "captcha",
            "captcha403",
            "challenge",
            "forbidden",
        ]:
            result = await scraper.search("Toyota", "123/45", 2)
            if mode in {"found", "fallback", "challenge"}:
                assert (result.price, result.delivery_days) == ("250.00", 1), result
            elif mode == "none":
                assert result.status == "not_found", result
            elif mode == "forbidden":
                assert result.error.code == "SOURCE_HTTP_ERROR", result
            else:
                assert result.error.code == "CAPTCHA_REQUIRED", result
            assert len(browser.contexts) == 0, "Leaked browser context"
            print(mode, "OK")
        calls = []
        hosts = []
        submissions = []
        original_client = httpx.AsyncClient

        def provider(request):
            calls.append(request.url.path)
            hosts.append(request.url.host)
            if request.content:
                submissions.append(dict(httpx.QueryParams(request.content.decode())))
            if mode == "rejected403":
                return httpx.Response(
                    200, json={"status": 0, "request": "ERROR_ZERO_BALANCE"}
                )
            return httpx.Response(
                200,
                json={
                    "status": 1,
                    "request": "123" if request.url.path == "/in.php" else "ABCD",
                },
            )

        scraper.settings.captcha_api_key = SecretStr(FAKE_KEY)
        with patch(
            "price_api.stparts.httpx.AsyncClient",
            lambda **kwargs: original_client(
                transport=httpx.MockTransport(provider), **kwargs
            ),
        ):
            for mode in ["solved403", "rejected403"]:
                result = await scraper.search("Toyota", "123/45", 2)
                if mode == "solved403":
                    assert (result.status, result.price) == ("found", "250.00"), result
                else:
                    assert result.error.code == "CAPTCHA_FAILED", result
                assert len(browser.contexts) == 0
                print(mode, "OK")
        assert calls == ["/in.php", "/res.php", "/in.php"], calls
        # The image method and the documented RuCaptcha endpoint must be used.
        assert hosts == ["rucaptcha.com"] * 3, hosts
        assert submissions[0]["method"] == "base64", submissions
        assert submissions[0]["key"] == FAKE_KEY, submissions
        folder = Path(temporary.name) / "diagnostics"
        reports = list(folder.glob("*.json"))
        assert len(reports) == 2, reports
        for report in reports:
            meta = json.loads(report.read_text())
            assert meta["http_status"] == 403
            assert report.with_suffix(".png").stat().st_size > 0
            html = report.with_suffix(".html").read_text()
            assert "<script" not in html
            assert "fake-provider-key" not in html
        assert {json.loads(f.read_text())["error"] for f in reports} == {
            "SOURCE_HTTP_ERROR",
            "CAPTCHA_FAILED",
        }
        print("Diagnostic bundles and retention OK")
        mode = "recaptcha403"
        calls.clear()
        hosts.clear()
        submissions.clear()
        with patch(
            "price_api.stparts.httpx.AsyncClient",
            lambda **kwargs: original_client(
                transport=httpx.MockTransport(provider), **kwargs
            ),
        ):
            result = await scraper.search("Toyota", "123/45", 2)
        assert (result.status, result.price) == ("found", "250.00"), result
        assert calls == ["/in.php", "/res.php"], calls
        assert hosts == ["rucaptcha.com"] * 2, hosts
        assert submissions[0]["method"] == "userrecaptcha", submissions
        assert submissions[0]["googlekey"] == "test-site-key", submissions
        assert submissions[0]["pageurl"].startswith("https://stparts.ru/"), submissions
        assert scraper.source_state is not None
        assert len(browser.contexts) == 0
        print("recaptcha403 form submission OK")
        # A token the source refuses must be replaced by a second paid attempt.
        mode = "recaptchaRetry"
        resubmits[0] = 0
        calls.clear()
        with patch(
            "price_api.stparts.httpx.AsyncClient",
            lambda **kwargs: original_client(
                transport=httpx.MockTransport(provider), **kwargs
            ),
        ):
            result = await scraper.search("Toyota", "123/45", 2)
        assert (result.status, result.price) == ("found", "250.00"), result
        assert calls == ["/in.php", "/res.php", "/in.php", "/res.php"], calls
        assert len(browser.contexts) == 0
        print("recaptcha retry after rejection OK")
        # A code image the source never loads must never reach the provider.
        mode = "pendingCaptcha"
        calls.clear()
        with patch(
            "price_api.stparts.httpx.AsyncClient",
            lambda **kwargs: original_client(
                transport=httpx.MockTransport(provider), **kwargs
            ),
        ):
            result = await scraper.search("Toyota", "123/45", 2)
        assert result.error.code == "CAPTCHA_FORMAT", result
        assert calls == [], calls
        assert len(browser.contexts) == 0
        print("pending captcha image rejected locally OK")
        # The source serves the limit form before Google renders its widget: keep waiting.
        mode = "recaptchaLate"
        calls.clear()
        hosts.clear()
        submissions.clear()
        with patch(
            "price_api.stparts.httpx.AsyncClient",
            lambda **kwargs: original_client(
                transport=httpx.MockTransport(provider), **kwargs
            ),
        ):
            result = await scraper.search("Toyota", "123/45", 2)
        assert (result.status, result.price) == ("found", "250.00"), result
        assert calls == ["/in.php", "/res.php"], calls
        assert submissions[0]["googlekey"] == "test-site-key", submissions
        assert len(browser.contexts) == 0
        print("late reCAPTCHA widget OK")
        # A limit form whose widget never arrives must not reach the provider.
        mode = "recaptchaPending"
        calls.clear()
        with patch(
            "price_api.stparts.httpx.AsyncClient",
            lambda **kwargs: original_client(
                transport=httpx.MockTransport(provider), **kwargs
            ),
        ):
            result = await scraper.search("Toyota", "123/45", 2)
        assert result.error.code == "CAPTCHA_FORMAT", result
        assert calls == [], calls
        assert len(browser.contexts) == 0
        print("pending reCAPTCHA widget rejected locally OK")
        # A dropped connection is a source error, not an unclassified crash.
        mode = "reset"
        result = await scraper.search("Toyota", "123/45", 2)
        assert result.error.code == "SOURCE_CONNECTION_ERROR", result
        assert len(browser.contexts) == 0
        print("connection reset OK")
        # A flagged client gets the source's access-restriction page, not a CAPTCHA.
        mode = "blocked"
        result = await scraper.search("Toyota", "123/45", 2)
        assert result.error.code == "SOURCE_BLOCKED", result
        assert len(browser.contexts) == 0
        print("access-restriction page OK")
    finally:
        await scraper.close()
        temporary.cleanup()
    print("Browser smoke: 16 scenarios passed; no external source requests")


asyncio.run(main())

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


async def main():
    temporary = tempfile.TemporaryDirectory()
    scraper = StpartsScraper(
        Settings(
            _env_file=None,
            api_token="test-only-token-at-least-24-characters",
            headless=True,
            database_path=Path(temporary.name) / "jobs.sqlite3",
            diagnostics_max_samples=2,
        )
    )
    browser = await scraper.get_browser()
    real_new_context = browser.new_context
    mode = "found"
    contexts = []

    async def new_context(**kwargs):
        context = await real_new_context(**kwargs)
        contexts.append(context)

        async def handler(route):
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
            elif mode == "challenge":
                status = 403
                html = (
                    "<div>Мы проверяем ваш браузер</div><script>setTimeout(() => {document.body.innerHTML = "
                    + json.dumps(TABLE)
                    + ";}, 100);</script>"
                )
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
        original_client = httpx.AsyncClient

        def provider(request):
            calls.append(request.url.path)
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

        scraper.settings.captcha_api_key = SecretStr("fake-provider-key")
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
    finally:
        await scraper.close()
        temporary.cleanup()
    print("Browser smoke: 9 scenarios passed; no external source requests")


asyncio.run(main())

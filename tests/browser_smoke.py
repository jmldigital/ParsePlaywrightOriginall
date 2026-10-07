"""Run inside the built image with Python. All source requests are intercepted."""

import asyncio
import json

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
    scraper = StpartsScraper(
        Settings(
            _env_file=None,
            api_token="test-only-token-at-least-24-characters",
            headless=True,
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
            elif mode == "captcha":
                html = CAPTCHA
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
        for mode in ["found", "none", "fallback", "captcha", "challenge", "forbidden"]:
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
    finally:
        await scraper.close()
    print("Browser smoke: 6 scenarios passed; no external source requests")


asyncio.run(main())

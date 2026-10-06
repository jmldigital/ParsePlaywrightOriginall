"""Run inside the built image with Python. All source requests are intercepted."""

import asyncio

from price_api.settings import Settings
from price_api.stparts import StpartsScraper

TABLE = """<table class="globalResult searchResultsSecondStep">
<tr class="resultTr2"><td class="resultBrand">Toyota</td><td class="resultDeadline">1</td><td class="resultPrice">500</td></tr>
<tr class="resultTr2"><td class="resultBrand">Toyota</td><td class="resultDeadline">2</td><td class="resultPrice">250</td></tr>
<tr class="resultTr2"><td class="resultBrand">Toyota</td><td class="resultDeadline">3</td><td class="resultPrice">100</td></tr>
</table>"""
EMPTY = '<div class="fr-alert fr-alert-warning alert-noResults">No results</div>'
CAPTCHA = '<img class="captchaImg" src="data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==">'


async def main():
    scraper = StpartsScraper(
        Settings(_env_file=None, api_token="test-only-token-at-least-24-characters")
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
            if mode == "none":
                html = EMPTY
            elif mode == "fallback" and "pcode=" not in route.request.url:
                html = EMPTY
            elif mode == "captcha":
                html = CAPTCHA
            await route.fulfill(status=200, content_type="text/html", body=html)

        await context.route("**/*", handler)
        return context

    browser.new_context = new_context
    try:
        for mode in ["found", "none", "fallback", "captcha"]:
            result = await scraper.search("Toyota", "123/45", 2)
            if mode in {"found", "fallback"}:
                assert (result.price, result.delivery_days) == ("250.00", 2), result
            elif mode == "none":
                assert result.status == "not_found", result
            else:
                assert result.error.code == "CAPTCHA_REQUIRED", result
            assert len(browser.contexts) == 0, "Leaked browser context"
            print(mode, "OK")
    finally:
        await scraper.close()
    print("Browser smoke: 4 scenarios passed; no external source requests")


asyncio.run(main())

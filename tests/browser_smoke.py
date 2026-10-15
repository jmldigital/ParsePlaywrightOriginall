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
# An OEM shared by several brands: the source answers the part-code search with this
# chooser. The markup repeats the live page: a row is tr.startSearching with data-link,
# the brand sits in td.caseBrand as a direct link, and analog brands are listed in a grey
# <span> inside the same cell. The description of the first row names our brand too, so
# matching a cell or a row instead of the main brand link would pick the wrong row.
SELECTION = (
    '<table><tbody><tr class="startSearching" data-link="/search/Sat/123%2F45">'
    '<td class="caseInfo"><div title="Информация о товаре скрыта"></div></td>'
    '<td class="caseBrand "><div>'
    '<a href="#" data-href="/getBrandInfo?brandName=Sat&amp;locale=ru_RU" '
    'class="brandInfoLink">Sat</a><br><span>'
    '<a href="#" data-href="/getBrandInfo?brandName=Great%20Wall&amp;locale=ru_RU" '
    'class="brandInfoLink" style="color: #999999;">Great Wall</a></span>&nbsp;'
    "</div></td>"
    '<td class="casePartCode ">123/45</td>'
    '<td class=" caseDescription">Шильдик AIRBAG Great Wall haval</td>'
    '<td class="caseUrl casePrices">'
    '<a class="startSearching" href="/search/Sat/123%2F45">Цены и аналоги</a>'
    "</td></tr>"
    '<tr class="startSearching" data-link="/search/Great%20Wall/123%2F45">'
    '<td class="caseInfo"><div title="Информация о товаре скрыта"></div></td>'
    '<td class="caseBrand "><div>'
    '<a href="#" data-href="/getBrandInfo?brandName=Great%20Wall&amp;locale=ru_RU" '
    'class="brandInfoLink">Great Wall</a><br><span>'
    '<a href="#" data-href="/getBrandInfo?brandName=HAVAL&amp;locale=ru_RU" '
    'class="brandInfoLink" style="color: #999999;">haval</a></span>&nbsp;'
    "</div></td>"
    '<td class="casePartCode ">123/45</td>'
    '<td class=" caseDescription">Шильдик AIRBAG</td>'
    '<td class="caseUrl casePrices">'
    '<a class="startSearching" href="/search/Great%20Wall/123%2F45">Цены и аналоги</a>'
    "</td></tr>"
    '<tr class="startSearching" data-link="/search/WHCQ/123%2F45">'
    '<td class="caseInfo"><div title="Информация о товаре скрыта"></div></td>'
    '<td class="caseBrand "><div>'
    '<a href="#" data-href="/getBrandInfo?brandName=WHCQ&amp;locale=ru_RU" '
    'class="brandInfoLink">WHCQ</a><br></div></td>'
    '<td class="casePartCode ">123/45</td>'
    '<td class=" caseDescription">Шильдик AIRBAG</td>'
    '<td class="caseUrl casePrices">'
    '<a class="startSearching" href="/search/WHCQ/123%2F45">Цены и аналоги</a>'
    "</td></tr></tbody></table>"
)
SELECTION_OFFERS = TABLE.replace("Toyota", "Great Wall")
SELECTION_OFFERS_URL = "https://stparts.ru/search/Great%20Wall/123%2F45"
# The limit form is served without a widget and the page then moves to the chooser: there
# is no site key to solve, so the provider must not be called at all.
RECAPTCHA_CHOOSER_PENDING = (
    RECAPTCHA_FORM_ONLY
    + "<script>setTimeout(() => {document.body.innerHTML = "
    + json.dumps(SELECTION)
    + ";}, 300);</script>"
)
# The source labels one maker with a combined name (as Hyundai-KIA), while the client
# asks for a part of that name.
COMPOUND_TABLE = TABLE.replace("Toyota", "Hyundai-KIA")


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
            source_retry_delay_seconds=0,
        )
    )
    browser = await scraper.get_browser()
    real_new_context = browser.new_context
    mode = "found"
    contexts = []
    pages = []
    resubmits = [0]  # The retry scenario rejects the first form submission.
    resets = [0]  # The reset scenario drops only the first request.

    async def new_context(**kwargs):
        context = await real_new_context(**kwargs)
        contexts.append(context)
        real_new_page = context.new_page

        async def new_page(**page_kwargs):
            page = await real_new_page(**page_kwargs)
            pages.append(page)
            return page

        context.new_page = new_page

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
            elif mode == "selection":
                # The brand path the client asked for has no offers, the OEM fallback
                # shows the chooser, and its row link leads to the real offers table.
                if "pcode=" in route.request.url:
                    html = SELECTION
                elif SELECTION_OFFERS_URL in route.request.url:
                    html = SELECTION_OFFERS
                else:
                    html = EMPTY
            elif mode == "compound":
                html = COMPOUND_TABLE
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
            elif mode == "recaptchaChooser":
                # The challenge is passed, but the source answers with the chooser of a
                # shared OEM instead of offers: that is progress, not a failure.
                if route.request.method == "POST":
                    post_data = route.request.post_data or ""
                    assert "g-recaptcha-response=ABCD" in post_data
                    html = SELECTION
                elif SELECTION_OFFERS_URL in route.request.url:
                    html = SELECTION_OFFERS
                else:
                    status = 403
                    html = RECAPTCHA
            elif mode == "recaptchaChooserPending":
                # No site key ever appears and the page settles on the chooser.
                if SELECTION_OFFERS_URL in route.request.url:
                    html = SELECTION_OFFERS
                else:
                    status = 403
                    html = RECAPTCHA_CHOOSER_PENDING
            elif mode == "recaptchaFormLost":
                status = 403
                html = RECAPTCHA
            elif mode == "reset":
                await route.abort("connectionreset")
                return
            elif mode == "resetOnce":
                if resets[0] == 0:
                    resets[0] = 1
                    await route.abort("connectionreset")
                    return
                html = TABLE
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

        async def provider(request):
            calls.append(request.url.path)
            hosts.append(request.url.host)
            if request.content:
                submissions.append(dict(httpx.QueryParams(request.content.decode())))
            if mode == "recaptchaFormLost" and request.url.path == "/res.php":
                # The source drops its form and moves on to the offers while the provider
                # is still returning the solution.
                await pages[-1].evaluate(
                    """html => {
                        document.getElementById('formSearchLimitCaptcha')?.remove();
                        document.body.insertAdjacentHTML('beforeend', html);
                    }""",
                    TABLE,
                )
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
        # A solved challenge whose page settles on the brand chooser is a success: the
        # challenge was passed and the row of our brand still has to be opened.
        mode = "recaptchaChooser"
        calls.clear()
        hosts.clear()
        submissions.clear()
        with patch(
            "price_api.stparts.httpx.AsyncClient",
            lambda **kwargs: original_client(
                transport=httpx.MockTransport(provider), **kwargs
            ),
        ):
            result = await scraper.search("Great Wall", "123/45", 2)
        assert (result.status, result.price) == ("found", "250.00"), result
        assert calls == ["/in.php", "/res.php"], calls
        assert len(browser.contexts) == 0
        print("captcha followed by the brand chooser OK")
        # The same page without a site key must not be paid for and must not be reported
        # as a captcha failure.
        mode = "recaptchaChooserPending"
        calls.clear()
        with patch(
            "price_api.stparts.httpx.AsyncClient",
            lambda **kwargs: original_client(
                transport=httpx.MockTransport(provider), **kwargs
            ),
        ):
            result = await scraper.search("Great Wall", "123/45", 2)
        assert (result.status, result.price) == ("found", "250.00"), result
        assert calls == [], calls
        assert len(browser.contexts) == 0
        print("chooser instead of a site key OK")
        # A dropped connection is a source error, not an unclassified crash.
        mode = "reset"
        result = await scraper.search("Toyota", "123/45", 2)
        assert result.error.code == "SOURCE_CONNECTION_ERROR", result
        assert len(browser.contexts) == 0
        print("connection reset OK")
        # A dropped connection is retried once before it is reported: the source throttles
        # one client and answers the same address a moment later.
        mode = "resetOnce"
        resets[0] = 0
        result = await scraper.search("Toyota", "123/45", 2)
        assert (result.status, result.price) == ("found", "250.00"), result
        assert resets[0] == 1, resets
        assert len(browser.contexts) == 0
        print("dropped connection retried OK")
        # A flagged client gets the source's access-restriction page, not a CAPTCHA.
        mode = "blocked"
        result = await scraper.search("Toyota", "123/45", 2)
        assert result.error.code == "SOURCE_BLOCKED", result
        assert len(browser.contexts) == 0
        print("access-restriction page OK")
        # An OEM shared by several brands: the chooser row of the requested brand wins.
        mode = "selection"
        result = await scraper.search("Great Wall", "123/45", 2)
        assert (result.status, result.price) == ("found", "250.00"), result
        assert len(browser.contexts) == 0
        print("brand chooser page OK")
        # The analog listed in the chooser cell is a different brand: no price for it.
        mode = "selection"
        result = await scraper.search("haval", "123/45", 2)
        assert result.status == "not_found", result
        assert len(browser.contexts) == 0
        print("analog brand is not matched OK")
        # The chooser row belongs to another brand: honest not_found, no price.
        mode = "selection"
        result = await scraper.search("Tesla", "123/45", 2)
        assert result.status == "not_found", result
        assert len(browser.contexts) == 0
        print("brand chooser without our brand OK")
        # The source's combined brand label contains our brand as a whole word.
        mode = "compound"
        result = await scraper.search("KIA", "0PN1113H51", 2)
        assert (result.status, result.price, result.delivery_days) == (
            "found",
            "250.00",
            1,
        ), result
        assert len(browser.contexts) == 0
        print("combined brand label OK")
        # A maker that shares no whole word with the label is not our brand.
        mode = "compound"
        result = await scraper.search("Toyota", "0PN1113H51", 2)
        assert result.status == "not_found", result
        assert len(browser.contexts) == 0
        print("unrelated brand against combined label OK")
        # The solution can arrive after the source dropped its form and moved on to the
        # offers: the challenge was passed, so this is not a format failure.
        mode = "recaptchaFormLost"
        calls.clear()
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
        assert submissions[0]["method"] == "userrecaptcha", submissions
        assert len(browser.contexts) == 0
        print("limit form dropped while solving OK")
    finally:
        await scraper.close()
        temporary.cleanup()
    print("Browser smoke: 25 scenarios passed; no external source requests")


asyncio.run(main())

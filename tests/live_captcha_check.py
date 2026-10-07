"""Live local check of the CAPTCHA module against stparts.ru.

Usage: python tests/live_captcha_check.py [limit] [batch-file]

It searches a few positions from a saved batch and prints the outcome of each one.
price_api.stparts logs captcha_detected / captcha_submit / captcha_solution_received /
captcha_accepted, so the log shows which challenge the source served and whether it
was accepted. The source rate-limits anonymous searches: keep the limit small, and
never treat one run as proof that the whole batch will pass.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

from price_api.settings import Settings
from price_api.stparts import StpartsScraper

DEFAULT_BATCH = Path("data/test-requests/local-captcha-250.json")


def load_parts(path, limit):
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    parts = payload["parts"] if isinstance(payload, dict) else payload
    unique = []
    seen = set()
    for part in parts:
        key = (part["brand"].casefold(), part["oem"].casefold())
        if key in seen:
            continue
        seen.add(key)
        unique.append(part)
    return unique[:limit]


async def main():
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    batch = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_BATCH
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    # httpx logs whole URLs at INFO; the provider key must never reach a log file.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    settings = Settings()
    print("provider", settings.captcha_api_url, "headless", settings.headless)
    scraper = StpartsScraper(settings)
    counts = {}
    try:
        for part in load_parts(batch, limit):
            result = await scraper.search(part["brand"], part["oem"], 2)
            detail = (
                result.error.code
                if result.error
                else f"price={result.price} days={result.delivery_days}"
            )
            print(
                f"{part['id']} {part['brand']} {part['oem']} -> {result.status} {detail}"
            )
            counts[result.status] = counts.get(result.status, 0) + 1
    finally:
        await scraper.close()
    print("summary", counts)


asyncio.run(main())

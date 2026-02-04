import requests
import json
import time

# ═══════════════════════════════════════════════════════════
# 🔧 НАСТРОЙКИ (Скопируйте свежие из DevTools)
# ═══════════════════════════════════════════════════════════

BEARER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3OTk0MDcyMzEsImtleSI6ImE3ZmU3ZGEwNmMxMjllOTY3NTgxOTdiOTNhMjZmZDhhIiwidHlwZSI6Imc5WCIsImRhdGEiOnsibG9naW4iOiJHVUVTVF8xNzY4MzAzMjMxMjg0NDUxIiwidXVpZCI6IkdkZDAxZWY1Yjk1YmZiOGRiYWM1Y2JiMjhmNDRiYmZhYSIsInV0eXBlIjoiRyIsInVmdW5jdGlvbiI6bnVsbCwiYWNsU2NoZW1lVHlwZSI6IltcImYwOGI3YzdkLTkxMGQtNDE5MC0zMWVhLWYxOGRmNGIzMTBjMlwiXSJ9fQ==.6zGZbU6lRIYtQHrEDiiXifS5meAZ+1jnQ7xZxMJfNS8="

COOKIE_STRING = "_ym_uid=1766656908325550353; _ym_d=1766656908; referrer=; _ym_isad=1; _ym_visorc=b; cf_clearance=urRPYstqN8HgZuGWs7vKtxzbjnxnGpOnmg5sMc4Xdmw-1770019445-1.2.1.1-LTUeBpxICK.RZ6ORYv9aTSV0Fi7VWZeFyzDkvuiZNNx.5td3t58pSgjQ5RnDliO29MTVyCmla3ndsq0oWC6r66vqW009fWq9peqYybTm0bV_UYGIzB7lqvs47OEZoUgWpthOdz6mRmHl4RSnbx0Sl4R6e8WGxpfW5wTn5hkdSnwpLEHec.LvB3BWmCCUPXTfDqxmn0VtTvTxvsLdNnxlmwmpOsFhNlU62.y6zAi5wRE; app_options=SlRkQ0pUSXljMlZ5ZG1WeVRHOWpZWFJwYjI1SWNtVm1KVEl5SlROQkpUSXlhSFIwY0hNbE0wRWxNa1lsTWtaaGNtMTBaV3N1Y25VbE1rWnpaV0Z5WTJnbE0wWjBaWGgwSlRORU5UZzRNekV0TUVjd01qQWxNaklsTWtNbE1qSm9iM04wSlRJeUpUTkJKVEl5YUhSMGNITWwxNTM1NDIyTTBFbE1rWWxNa1poY20xMFpXc3VjblVsTWpJbE1rTWxNakoyYTI5eVp5VXlNaVV6UVNVeU1qUXdNREFsTWpJbE1rTWxNakp6YUc5M1JYaDBjbUZOWlhOellXZGxjeVV5TWlVelFXWmhiSE5sSlRKREpUSXljMkZ3UkdsellXSnNaV1FsTWpJbE0wRm1ZV3h6WlNVM1JBJTNEJTNE1535422"

CAPTCHA_HASH = "c2e18f03dc0dc7bb8d275d2cfa48fdd1"

# ═══════════════════════════════════════════════════════════
# 🔧 API ENDPOINTS
# ═══════════════════════════════════════════════════════════

SEARCH_URL = "https://armtek.ru/rest/ru/search-microservice/v1/search"
DETAILS_URL_TEMPLATE = "https://armtek.ru/rest/ru/assortment-microservice/v1/articles/details/alias/{alias}?weightUnitType=kg&lengthUnitType=cm&country=ru"

# ═══════════════════════════════════════════════════════════
# 🧪 ТЕСТОВЫЕ АРТИКУЛЫ
# ═══════════════════════════════════════════════════════════

TEST_ARTICLES = [
    "AS320",
    "NIN81",
    "FBT002",
    "3501280BS01",
    "A212906031",
    "401001002AA",
    "8871033320",
    "J605811010BA",
    "M738A1STD",
    "S111001510DA",
    "A1N011",
    "A213600080",
    "67640-0N130-C5",
    "588310G020",
    "TEST123",
    "12345",
    "ABCDEF",
    "999999",
    "FAKE01",
    "DUMMY",
]


def parse_cookies(cookie_string):
    """Парсинг строки кук в словарь"""
    cookies = {}
    for item in cookie_string.split("; "):
        if "=" in item:
            key, val = item.split("=", 1)
            cookies[key] = val
    return cookies


def build_headers(referer="https://armtek.ru/"):
    """Построение заголовков для API"""
    return {
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json",
        "Referer": referer,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "x-app-version": "1.0.330",
        "x-auth-captcha-hash": CAPTCHA_HASH,
        "x-ca-external-system": "IM_RU",
        "x-ca-vkorg": "4000",
        "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }


def search_article(article, cookies):
    """
    Шаг 1: POST /search → получить ARTICLE_ALIAS
    """
    headers = build_headers(referer=f"https://armtek.ru/search?text={article}")

    payload = {
        "query": article,
        "queryType": 1,
        "page": 1,
        "filters": {"text": article},
        "userInfo": {"VKORG": "4000", "VSTELS_LIST": ["ME86"]},
        "ZZSIGN": "S",
    }

    try:
        response = requests.post(
            SEARCH_URL, headers=headers, cookies=cookies, json=payload, timeout=10
        )

        if response.status_code == 429:
            print(f"  ⏳ [{article}] Rate limit 429")
            return None, 429

        if response.status_code != 200:
            print(f"  ❌ [{article}] Search error: {response.status_code}")
            return None, response.status_code

        data = response.json()

        if not data.get("data"):
            print(f"  ⚠️ [{article}] Пустой ответ (data: null)")
            return None, 200

        articles_data = data["data"].get("articlesData", [])

        if not articles_data:
            print(f"  ❌ [{article}] Не найдено в базе")
            return None, 200

        alias = articles_data[0].get("ARTICLE_ALIAS")

        if not alias:
            print(f"  ⚠️ [{article}] ARTICLE_ALIAS отсутствует")
            return None, 200

        print(f"  ✅ [{article}] Найден alias: {alias[:40]}...")
        return alias, 200

    except Exception as e:
        print(f"  ❌ [{article}] Ошибка: {e}")
        return None, 0


def get_article_details(alias, article, cookies):
    """
    Шаг 2: GET /details/alias/{alias} → получить weight
    """
    url = DETAILS_URL_TEMPLATE.format(alias=alias)
    headers = build_headers(referer=f"https://armtek.ru/product/{alias}")

    try:
        response = requests.get(url, headers=headers, cookies=cookies, timeout=10)

        if response.status_code != 200:
            print(f"  ❌ [{article}] Details error: {response.status_code}")
            return None

        data = response.json()

        if not data.get("data"):
            print(f"  ⚠️ [{article}] Детали не найдены")
            return None

        weight = data["data"].get("weight")

        if weight:
            print(f"  🎯 [{article}] Вес: {weight} кг")
            return weight
        else:
            print(f"  ⚠️ [{article}] Вес не указан")
            return None

    except Exception as e:
        print(f"  ❌ [{article}] Ошибка details: {e}")
        return None


def main():
    """
    ТЕСТ: Парсинг 20 деталей с задержками
    """
    print("=" * 60)
    print("🚀 ТЕСТ ARMTEK API ПАРСЕРА (requests)")
    print("=" * 60)

    cookies = parse_cookies(COOKIE_STRING)
    print(f"\n🍪 Загружено {len(cookies)} кук")
    print(f"🔑 Bearer: {BEARER_TOKEN[:30]}...")
    print(f"🧩 Captcha: {CAPTCHA_HASH[:16]}...\n")

    results = {
        "success": 0,
        "not_found": 0,
        "rate_limit": 0,
        "errors": 0,
    }

    start_time = time.time()

    for i, article in enumerate(TEST_ARTICLES, 1):
        print(f"\n[{i}/{len(TEST_ARTICLES)}] Обработка: {article}")

        # Шаг 1: Поиск
        alias, status = search_article(article, cookies)

        if status == 429:
            results["rate_limit"] += 1
            print("  ⏸️  Пауза 3 сек (rate limit)...")
            time.sleep(3)
            continue

        if not alias:
            results["not_found"] += 1
            time.sleep(0.5)  # Короткая пауза
            continue

        # Шаг 2: Детали
        weight = get_article_details(alias, article, cookies)

        if weight:
            results["success"] += 1
        else:
            results["errors"] += 1

        # Пауза между запросами (важно!)
        time.sleep(1.0)

    elapsed = time.time() - start_time

    print("\n" + "=" * 60)
    print("📊 РЕЗУЛЬТАТЫ")
    print("=" * 60)
    print(f"✅ Успешно:      {results['success']}")
    print(f"❌ Не найдено:   {results['not_found']}")
    print(f"⏳ Rate limit:   {results['rate_limit']}")
    print(f"🔥 Ошибки:       {results['errors']}")
    print(f"⏱️  Время:        {elapsed:.1f} сек")
    print(f"⚡ Скорость:     {len(TEST_ARTICLES) / elapsed:.2f} арт/сек")
    print("=" * 60)


if __name__ == "__main__":
    main()

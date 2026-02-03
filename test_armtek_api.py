import requests
import json

# ═══════════════════════════════════════════════════════════
# 🔧 НАСТРОЙКИ (Скопируйте свежие из DevTools)
# ═══════════════════════════════════════════════════════════

BEARER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3OTk0MDcyMzEsImtleSI6ImE3ZmU3ZGEwNmMxMjllOTY3NTgxOTdiOTNhMjZmZDhhIiwidHlwZSI6Imc5WCIsImRhdGEiOnsibG9naW4iOiJHVUVTVF8xNzY4MzAzMjMxMjg0NDUxIiwidXVpZCI6IkdkZDAxZWY1Yjk1YmZiOGRiYWM1Y2JiMjhmNDRiYmZhYSIsInV0eXBlIjoiRyIsInVmdW5jdGlvbiI6bnVsbCwiYWNsU2NoZW1lVHlwZSI6IltcImYwOGI3YzdkLTkxMGQtNDE5MC0zMWVhLWYxOGRmNGIzMTBjMlwiXSJ9fQ==.6zGZbU6lRIYtQHrEDiiXifS5meAZ+1jnQ7xZxMJfNS8="

COOKIE_STRING = "_ym_uid=1766656908325550353; _ym_d=1766656908; referrer=; _ym_isad=1; _ym_visorc=b; cf_clearance=urRPYstqN8HgZuGWs7vKtxzbjnxnGpOnmg5sMc4Xdmw-1770019445-1.2.1.1-LTUeBpxICK.RZ6ORYv9aTSV0Fi7VWZeFyzDkvuiZNNx.5td3t58pSgjQ5RnDliO29MTVyCmla3ndsq0oWC6r66vqW009fWq9peqYybTm0bV_UYGIzB7lqvs47OEZoUgWpthOdz6mRmHl4RSnbx0Sl4R6e8WGxpfW5wTn5hkdSnwpLEHec.LvB3BWmCCUPXTfDqxmn0VtTvTxvsLdNnxlmwmpOsFhNlU62.y6zAi5wRE; app_options=SlRkQ0pUSXljMlZ5ZG1WeVRHOWpZWFJwYjI1SWNtVm1KVEl5SlROQkpUSXlhSFIwY0hNbE0wRWxNa1lsTWtaaGNtMTBaV3N1Y25VbE1rWnpaV0Z5WTJnbE0wWjBaWGgwSlRORU5UZzRNekV0TUVjd01qQWxNaklsTWtNbE1qSm9iM04wSlRJeUpUTkJKVEl5YUhSMGNITWwxNTM1NDIyTTBFbE1rWWxNa1poY20xMFpXc3VjblVsTWpJbE1rTWxNakoyYTI5eVp5VXlNaVV6UVNVeU1qUXdNREFsTWpJbE1rTWxNakp6YUc5M1JYaDBjbUZOWlhOellXZGxjeVV5TWlVelFXWmhiSE5sSlRKREpUSXljMkZ3UkdsellXSnNaV1FsTWpJbE0wRm1ZV3h6WlNVM1JBJTNEJTNE1535422"

CAPTCHA_HASH = "c2e18f03dc0dc7bb8d275d2cfa48fdd1"

# URL конкретной детали (где есть вес)
TARGET_URL = "https://armtek.ru/rest/ru/assortment-microservice/v1/articles/details/alias/avtozapchast-588310g020-toyota--lexus-80929294?weightUnitType=kg&lengthUnitType=cm&country=ru"

# ═══════════════════════════════════════════════════════════


def get_raw_data():
    # Парсинг куков в словарь
    cookies = {}
    for item in COOKIE_STRING.split("; "):
        if "=" in item:
            key, val = item.split("=", 1)
            cookies[key] = val

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Authorization": f"Bearer {BEARER_TOKEN}",
        "Content-Type": "application/json",
        "Referer": "https://armtek.ru/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "x-app-version": "1.0.330",
        "x-auth-captcha-hash": CAPTCHA_HASH,
        "x-ca-external-system": "IM_RU",
        "x-ca-vkorg": "4000",
        "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
    }

    print(f"🚀 Запрос к: {TARGET_URL}\n")

    try:
        response = requests.get(TARGET_URL, headers=headers, cookies=cookies)

        print(f"Статус код: {response.status_code}")

        if response.status_code == 200:
            # Выводим красивый JSON
            data = response.json()
            print("\n✅ Ответ сервера (RAW JSON):")
            print("-" * 40)
            print(json.dumps(data, indent=4, ensure_ascii=False))
            print("-" * 40)
        else:
            print(f"❌ Ошибка запроса: {response.text}")

    except Exception as e:
        print(f"❌ Ошибка выполнения: {e}")


if __name__ == "__main__":
    get_raw_data()

import asyncio
import json
from playwright.async_api import async_playwright

TEST_ARTICLES = [
    "AS320",
    "NIN81",
    "FBT002",
    "3501280BS01",
    "A212906031",
    "67640-0N130-C5",
]


async def main():
    print("🚀 ТЕСТ: Перехват JSON через браузер (Fast DOM)")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, args=["--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        # 🟢 Глобальный словарь для хранения результатов текущего поиска
        current_search = {"alias": None, "weight": None}

        # 🟢 ЕДИНЫЙ ОБРАБОТЧИК (слушает ВСЁ время)
        async def handle_response(response):
            try:
                url = response.url
                status = response.status

                if status != 200:
                    return

                # Перехват SEARCH
                if "/search-microservice/v1/search" in url:
                    # Важно: response.json() может упасть если тело пустое
                    try:
                        data = await response.json()
                        items = data.get("data", {}).get("articlesData", [])
                        if items:
                            alias = items[0].get("ARTICLE_ALIAS")
                            if alias:
                                current_search["alias"] = alias
                                print(f"  ⚡ Alias пойман: {alias[:25]}...")
                    except:
                        pass

                # Перехват DETAILS
                elif "/articles/details/alias/" in url:
                    try:
                        data = await response.json()
                        weight = data.get("data", {}).get("weight")
                        if weight:
                            current_search["weight"] = weight
                            print(f"  🎯 Вес пойман: {weight} кг")
                    except:
                        pass

            except Exception:
                pass

        # Подписываемся один раз
        page.on("response", handle_response)

        # 1. Загрузка главной
        print("🌐 Загрузка главной...")
        try:
            await page.goto(
                "https://armtek.ru", wait_until="domcontentloaded", timeout=15000
            )
            await page.wait_for_selector('img[alt="armtek logo"]', timeout=10000)
            print("✅ Главная загружена\n")
        except:
            print("⚠️ Главная не прогрузилась, но продолжаем...\n")

        results = {"success": 0, "failed": 0}

        for i, article in enumerate(TEST_ARTICLES, 1):
            print(f"[{i}/{len(TEST_ARTICLES)}] 🔍 Поиск: {article}")

            # Сбрасываем данные перед каждым поиском
            current_search["alias"] = None
            current_search["weight"] = None

            try:
                # ЭТАП 1: Поиск
                await page.goto(
                    f"https://armtek.ru/search?text={article}",
                    wait_until="domcontentloaded",
                    timeout=10000,
                )

                # Ждем alias
                for _ in range(25):
                    if current_search["alias"]:
                        break
                    await asyncio.sleep(0.2)

                if not current_search["alias"]:
                    print(f"  ❌ Alias не найден")
                    results["failed"] += 1
                    continue

                # ЭТАП 2: Детали
                # Переходим сразу, не дожидаясь лишней загрузки
                await page.goto(
                    f"https://armtek.ru/product/{current_search['alias']}",
                    wait_until="domcontentloaded",
                    timeout=10000,
                )

                # Ждем вес
                for _ in range(25):
                    if current_search["weight"]:
                        break
                    await asyncio.sleep(0.2)

                if current_search["weight"]:
                    print(f"  ✅ УСПЕХ: {article} → {current_search['weight']} кг\n")
                    results["success"] += 1
                else:
                    print(f"  ⚠️ Вес не найден\n")
                    results["failed"] += 1

            except Exception as e:
                print(f"  ❌ Ошибка навигации: {e}\n")
                results["failed"] += 1

            await asyncio.sleep(0.8)

        await browser.close()

        print("=" * 60)
        print(f"📊 ИТОГО:")
        print(f"  ✅ Успешно: {results['success']}")
        print(f"  ❌ Ошибок:  {results['failed']}")
        print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

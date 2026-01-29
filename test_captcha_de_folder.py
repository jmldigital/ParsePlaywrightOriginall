"""
Тестовый парсер для отладки HTML-страниц из папки debug/
Использует ТЕ ЖЕ функции что и основной парсер armtek.py
Запуск: python test_debug_parser.py
"""

import asyncio
import re
from pathlib import Path
from typing import Optional, Tuple
from playwright.async_api import (
    async_playwright,
    Page,
    TimeoutError as PlaywrightTimeout,
)
import logging

from config import SELECTORS

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


async def close_city_dialog(page: Page):
    """Закрывает диалог города"""
    try:
        if await page.locator("button:has-text('Верно')").is_visible(timeout=1000):
            await page.locator("button:has-text('Верно')").click()
            return
        if await page.locator("div.geo-control__click-area").is_visible(timeout=500):
            await page.locator("div.geo-control__click-area").click(force=True)
    except Exception:
        pass


async def wait_skeletons_gone(page: Page, logger, timeout: int = 20000) -> bool:
    """Глобальные скелетоны = НЕ картинки/характеристики"""
    try:
        # 🔥 1. Ждём ЛОГОТИП (страница живая!)
        logo = page.locator('img[alt="armtek logo"][src*="logo-armtek"]').first
        await logo.wait_for(state="attached", timeout=10000)
        logger.debug("✅ Логотип ARMTEK OK")

        # 🔥 2. Проверяем ПРЕЛОАДЕР (sproit-ui-loading)
        preloader = page.locator("sproit-ui-loading").first
        preloader_count = await preloader.count()

        if preloader_count > 0:
            logger.info("⏳ Ждём исчезновения прелоадера...")
            try:
                await preloader.wait_for(state="hidden", timeout=timeout)
                logger.debug("✅ Прелоадер исчез")
            except PlaywrightTimeout:
                logger.warning("⚠️ Прелоадер не исчез, продолжаем")

        # 🔥 ТОЧНЫЙ селектор ГЛОБАЛЬНЫХ!
        skeletons = page.locator(
            ".product-card__skeleton_desktop sproit-ui-skeleton, .product-card__skeleton_mobile sproit-ui-skeleton"
        )

        initial_count = await skeletons.count()
        if initial_count == 0:
            logger.debug("✅ Нет глобальных скелетонов")
            return True

        logger.info(f"⏳ Ждём {initial_count} глобальных скелетонов...")

        # Ждём ключевые (первые заголовки)
        await skeletons.filter(has_text=re.compile(r"height:\s*32px")).first.wait_for(
            state="hidden", timeout=timeout
        )

        final_count = await skeletons.count()
        success = final_count < initial_count * 0.7  # 70% исчезли = OK

        logger.debug(
            f"✅ Глобальных осталось: {final_count}/{initial_count} → {success}"
        )
        return success

    except Exception as e:
        logger.warning(f"⚠️ Skip глобальных скелетонов: {e}")
        return True  # ✅ ВАЖНО: ПРОДОЛЖАЕМ ПАРСИНГ!


async def determine_state(page: Page) -> tuple[str, str]:
    """
    Определяет состояние страницы после загрузки
    Crawlee уже сделал goto(), мы только проверяем результат

    Returns:
        tuple[str, str]: (state_name, matched_selector)
    """
    selectors = {
        "cards": SELECTORS["armtek"]["product_card-list"],
        "no_results": SELECTORS["armtek"]["no_results"],
        "captcha": SELECTORS["armtek"]["captcha"],
        "rate_limit": SELECTORS["armtek"]["rate_limit"],
        "cloudflare": SELECTORS["armtek"]["rate_limit"],
        "card_direct": SELECTORS["armtek"]["specifications"],
        "product_info": SELECTORS["armtek"]["product-card-info"],
    }

    tasks = {}

    for state_name, selector_string in selectors.items():
        # Разбиваем строку селекторов по запятой
        individual_selectors = [
            s.strip() for s in selector_string.split(",") if s.strip()
        ]

        for sel in individual_selectors:
            task = asyncio.create_task(
                page.wait_for_selector(
                    f"{sel}:has(*) >> nth=0", state="visible", timeout=20000
                )
            )
            # Сохраняем tuple (state_name, конкретный_селектор)
            tasks[task] = (state_name, sel)

    done, pending = await asyncio.wait(
        tasks.keys(), return_when=asyncio.FIRST_COMPLETED
    )

    # Отменяем остальные задачи
    for task in pending:
        task.cancel()

    try:
        first_task = list(done)[0]
        await first_task  # Проверяем на ошибки
        state_name, matched_selector = tasks[first_task]
        return state_name, matched_selector

    except PlaywrightTimeout:
        return "timeout", ""
    except Exception as e:
        return "error", ""


async def extract_weight(page: Page, html_filename: str, logger) -> Optional[str]:
    """Извлечение веса из DOM (БЕЗ скриншота)"""
    selectors = [SELECTORS["armtek"]["product-card-weight"]]

    for sel in selectors:
        try:
            elements = page.locator(sel)
            count = await elements.count()
            logger.debug(f"🔍 {sel}: {count} элементов")

            for i in range(count):
                text = await elements.nth(i).text_content()
                if text:
                    match = re.search(
                        r"(\d+(?:[.,]\d+)?)\s*(?:кг|kg)", text, re.IGNORECASE
                    )
                    if match:
                        weight = match.group(1).replace(",", ".")
                        logger.debug(f"✅ Вес найден: {weight}")
                        return weight
        except Exception as e:
            logger.debug(f"❌ Селектор {sel}: {e}")
            continue

    logger.warning(f"❌ [{html_filename}] Вес не найден")
    return None


async def parse_html_file(page: Page, html_file: Path) -> Tuple[str, Optional[str]]:
    """
    Парсинг одного HTML-файла
    Возвращает: (state, weight)
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"📄 Обработка: {html_file.name}")
    logger.info(f"{'='*60}")

    # Загружаем HTML-файл
    file_url = f"file://{html_file.absolute()}"
    await page.goto(file_url, wait_until="domcontentloaded", timeout=30000)

    await close_city_dialog(page)

    # Определяем состояние
    state, container_sel = await determine_state(page)
    logger.info(f"✅ Состояние: {state}, селектор: {container_sel}")

    # ✅ КАРТОЧКА УЖЕ ГОТОВА? → ПРОПУСКАЕМ СКЕЛЕТОНЫ!
    if state == "card_direct":
        logger.info(f"🎯 ПРЯМО НА КАРТОЧКЕ → парсим характеристики")
        weight = await extract_weight(page, html_file.name, logger)
        return state, weight

    # Обработка других состояний
    if state == "no_results":
        logger.warning(f"❌ Результаты не найдены")
        return state, None
    elif state == "captcha":
        logger.warning(f"⚠️ Обнаружена капча")
        return state, None
    elif state == "rate_limit":
        logger.warning(f"⚠️ Rate limit")
        return state, None
    elif state == "cloudflare":
        logger.warning(f"⚠️ CloudFlare")
        return state, None
    elif state in ("timeout", "error"):
        logger.error(f"❌ Ошибка определения состояния: {state}")
        return state, None

    # 🔥 Ждём исчезновения скелетонов
    await wait_skeletons_gone(page, logger, timeout=30000)

    # Проверяем наличие ссылок на карточки товаров
    if state == "cards":
        link_selectors = [
            f"{container_sel} a.title[href^='/product/']",
            f"{container_sel} a.suggestion-itemtitle-name[href^='/product/']",
            f"{container_sel} a[href^='/product/']",
        ]

        link_found = False
        for sel in link_selectors:
            count = await page.locator(sel).count()
            if count > 0:
                link_found = True
                logger.debug(f"✅ Найдено ссылок: {count} ({sel})")
                break

        if not link_found:
            logger.warning(f"⚠️ Ссылки на карточки не найдены")

    elif state == "product_info":
        link_selectors = [
            f"{container_sel} a[href^='/product/']",
            f"{container_sel} a.title[href^='/product/']",
            f"{container_sel} a.suggestion-itemtitle-name[href^='/product/']",
        ]

        link_found = False
        for sel in link_selectors:
            count = await page.locator(sel).count()
            if count > 0:
                link_found = True
                logger.debug(f"✅ Найдено ссылок: {count} ({sel})")
                break

        if not link_found:
            logger.warning(f"⚠️ Ссылки в product_info не найдены")

    # Проверяем наличие ссылки #tech-info (если это карточка)
    tech_link = page.locator('a[href="#tech-info"]').first
    tech_count = await tech_link.count()
    if tech_count > 0:
        logger.debug(f"✅ Ссылка #tech-info найдена")
        # Пытаемся извлечь вес
        weight = await extract_weight(page, html_file.name, logger)
        return state, weight
    else:
        logger.debug(f"ℹ️ Ссылка #tech-info не найдена (возможно это не карточка)")

    # Финальная попытка извлечь вес
    weight = await extract_weight(page, html_file.name, logger)
    return state, weight


async def main():
    """Главная функция для тестирования"""

    # Путь к папке с HTML-файлами
    debug_folder = Path("de")

    if not debug_folder.exists():
        logger.error(f"❌ Папка {debug_folder} не найдена!")
        return

    # Получаем все HTML-файлы
    html_files = list(debug_folder.glob("*.html"))

    if not html_files:
        logger.warning(f"⚠️ В папке {debug_folder} нет HTML-файлов!")
        return

    logger.info(f"📂 Найдено {len(html_files)} HTML-файлов\n")

    # Статистика
    stats = {"total": len(html_files), "states": {}, "weights_found": 0, "results": []}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        for html_file in sorted(html_files):
            try:
                state, weight = await parse_html_file(page, html_file)

                # Обновляем статистику
                stats["states"][state] = stats["states"].get(state, 0) + 1
                if weight:
                    stats["weights_found"] += 1

                stats["results"].append(
                    {"file": html_file.name, "state": state, "weight": weight}
                )

                # Небольшая пауза между файлами
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(
                    f"❌ Критическая ошибка при обработке {html_file.name}: {e}"
                )
                stats["results"].append(
                    {"file": html_file.name, "state": "error", "weight": None}
                )

        await context.close()
        await browser.close()

    # Итоговая статистика
    logger.info(f"\n{'='*60}")
    logger.info(f"📊 ИТОГОВАЯ СТАТИСТИКА")
    logger.info(f"{'='*60}")
    logger.info(f"Всего файлов: {stats['total']}")
    logger.info(
        f"Весов найдено: {stats['weights_found']} ({stats['weights_found']/stats['total']*100:.1f}%)"
    )
    logger.info(f"\nРаспределение по состояниям:")
    for state, count in sorted(stats["states"].items()):
        logger.info(f"  {state}: {count}")

    logger.info(f"\n{'='*60}")
    logger.info(f"📋 ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ")
    logger.info(f"{'='*60}")

    for result in stats["results"]:
        weight_str = f"{result['weight']} кг" if result["weight"] else "НЕ НАЙДЕН"
        logger.info(f"{result['file']:50} | {result['state']:15} | {weight_str}")


if __name__ == "__main__":
    asyncio.run(main())

"""
Japarts парсер - с заполнением формы поиска
"""

import re
import asyncio
from typing import Tuple, Optional
from playwright.async_api import Page
from config import SELECTORS


# async def parse_weight_japarts(
#     page: Page, part: str, logger
# ) -> Tuple[Optional[str], Optional[str]]:
#     """
#     🔥 JAPARTS: заполнение формы + парсинг веса
#     Crawlee открыл главную страницу, ЗДЕСЬ делаем поиск!
#     """
#     try:
#         # 🔥 1. ЗАПОЛНЯЕМ ФОРМУ ПОИСКА (твой старый код!)
#         search_input = page.locator(SELECTORS["japarts"]["search_input"]).first
#         await search_input.wait_for(state="visible", timeout=5000)
#         await search_input.fill(part)

#         search_button = page.locator(SELECTORS["japarts"]["search_button"]).first
#         await search_button.click()

#         # 2. Ждём результатов
#         await page.wait_for_timeout(3000)

#         # 3. Проверка: нет результатов?
#         content = await page.content()
#         if "Записей по вашему запросу не найдено" in content:
#             # logger.info(f"Jparts - ❌ Не найдено: {part}")
#             return None, None

#         # 4. Парсинг веса (твой старый код)
#         weight_loc = page.locator(SELECTORS["japarts"]["weight_row"]).first
#         weight_text = await weight_loc.text_content(timeout=5000)

#         if not weight_text or "Нет веса" in weight_text:
#             # logger.warning(f"⚠️ Вес не найден: {part}")
#             return None, None

#         # 5. Регулярки
#         p_match = re.search(r"Вес[:\s]*([\d.,]+)\s*кг", weight_text, re.IGNORECASE)
#         v_match = re.search(
#             r"объемный[:\s]*вес[:\s]*([\d.,]+)\s*кг", weight_text, re.IGNORECASE
#         )

#         physical = p_match.group(1).replace(",", ".") if p_match else None
#         volumetric = v_match.group(1).replace(",", ".") if v_match else None

#         # logger.info(f"🎯 Вес: {physical}/{volumetric} ({part})")
#         return physical, volumetric

#     except Exception as e:
#         logger.error(f" ❌ aparts error {part}: {e}")

#         # EmptyPage проверка
#         content = await page.content()
#         if len(content.strip()) < 100:
#             logger.warning(f"📭 EmptyPage: {part}")
#             return "EmptyPage", "EmptyPage"


#         return None, None
async def parse_weight_japarts(
    page: Page, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    try:
        # 1. Быстрый доступ к инпуту
        search_input = page.locator(SELECTORS["japarts"]["search_input"]).first

        # Ждем только появления инпута (макс 3 сек)
        await search_input.wait_for(state="visible", timeout=3000)
        await search_input.fill(part)

        # 2. Вместо клика жмем Enter — это быстрее и надежнее
        await search_input.press("Enter")

        # 3. УМНОЕ ОЖИДАНИЕ (вместо фиксированных 3 секунд)
        # Ждем либо строку с весом, либо сообщение, что ничего не найдено
        try:
            await asyncio.wait_for(
                page.locator(
                    f"{SELECTORS['japarts']['weight_row']}, :has-text('Записей по вашему запросу не найдено')"
                ).first.wait_for(state="attached"),
                timeout=7.0,  # Общий таймаут на поиск
            )
        except asyncio.TimeoutError:
            return None, None

        # 4. Проверка контента
        content = await page.content()
        if "Записей по вашему запросу не найдено" in content:
            return None, None

        # 5. Извлечение текста веса
        weight_loc = page.locator(SELECTORS["japarts"]["weight_row"]).first
        if await weight_loc.count() == 0:
            return None, None

        weight_text = await weight_loc.text_content()

        if not weight_text or "Нет веса" in weight_text:
            return None, None

        # 6. Регулярки (без изменений)
        p_match = re.search(r"Вес[:\s]*([\d.,]+)\s*кг", weight_text, re.IGNORECASE)
        v_match = re.search(
            r"объемный[:\s]*вес[:\s]*([\d.,]+)\s*кг", weight_text, re.IGNORECASE
        )

        physical = p_match.group(1).replace(",", ".") if p_match else None
        volumetric = v_match.group(1).replace(",", ".") if v_match else None

        return physical, volumetric

    except Exception as e:
        logger.error(f"❌ Japarts error {part}: {e}")
        return None, None

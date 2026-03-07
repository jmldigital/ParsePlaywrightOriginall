"""
Japarts парсер - с заполнением формы поиска
"""

import re
from typing import Tuple, Optional
from playwright.async_api import Page
from config import SELECTORS
import asyncio

# # Компилируем regex на уровне модуля — один раз при импорте
# _RE_PHYSICAL = re.compile(r"Вес[:\s]*([\d.,]+)\s*кг", re.IGNORECASE)
# _RE_VOLUMETRIC = re.compile(r"объемный[:\s]*вес[:\s]*([\d.,]+)\s*кг", re.IGNORECASE)
# _NOT_FOUND_TEXT = "Записей по вашему запросу не найдено"

# #  Все работает, н оиногда ерроры по first elemetn

# async def parse_weight_japarts(
#     page: Page, part: str, logger
# ) -> Tuple[Optional[str], Optional[str]]:
#     try:
#         weight_sel = SELECTORS["japarts"]["weight_row"]
#         input_sel = SELECTORS["japarts"]["search_input"]

#         search_input = page.locator(input_sel).first
#         await search_input.wait_for(state="visible", timeout=3000)

#         await search_input.fill(part)
#         await search_input.press("Enter")

#         # or_() — правильный способ ждать один из двух локаторов в Playwright
#         weight_loc = page.locator(weight_sel)
#         not_found_loc = page.locator("table[bgcolor='red'] td")

#         result_loc = weight_loc.or_(not_found_loc).first

#         try:
#             await result_loc.wait_for(state="attached", timeout=7000)
#         except Exception as e:
#             logger.debug(f"⏱️ Japarts timeout waiting for result {part}: {e}")
#             return None, None

#         first_text = await result_loc.inner_text()
#         if _NOT_FOUND_TEXT in first_text:
#             return None, None

#         weight_text = await weight_loc.first.inner_text(timeout=5000)

#         if not weight_text or "Нет веса" in weight_text:
#             return None, None

#         p_match = _RE_PHYSICAL.search(weight_text)
#         v_match = _RE_VOLUMETRIC.search(weight_text)

#         physical = p_match.group(1).replace(",", ".") if p_match else None
#         volumetric = v_match.group(1).replace(",", ".") if v_match else None

#         return physical, volumetric

#     except Exception as e:
#         logger.error(f"❌ Japarts error {part}: {e}")
#         return None, None

_RE_PHYSICAL = re.compile(r"Вес[:\s]*([\d.,]+)\s*кг", re.IGNORECASE)
_RE_VOLUMETRIC = re.compile(r"объемный[:\s]*вес[:\s]*([\d.,]+)\s*кг", re.IGNORECASE)
_NOT_FOUND_TEXT = "Записей по вашему запросу не найдено"


def _extract_weights(text: str) -> Tuple[Optional[str], Optional[str]]:
    p_match = _RE_PHYSICAL.search(text)
    v_match = _RE_VOLUMETRIC.search(text)
    physical = p_match.group(1).replace(",", ".") if p_match else None
    volumetric = v_match.group(1).replace(",", ".") if v_match else None
    return physical, volumetric


async def parse_weight_japarts(
    page: Page, part: str, logger
) -> Tuple[Optional[str], Optional[str]]:
    try:
        weight_sel = SELECTORS["japarts"]["weight_row"]
        input_sel = SELECTORS["japarts"]["search_input"]

        # Таймаут 1: ждём поле ввода
        search_input = page.locator(input_sel).first
        await search_input.wait_for(state="visible", timeout=3000)
        await search_input.fill(part)
        await search_input.press("Enter")

        # Таймаут 2: ждём появления любого результата (вес или "не найдено")
        weight_loc = page.locator(weight_sel)
        not_found_loc = page.locator("table[bgcolor='red'] td")

        try:
            await weight_loc.or_(not_found_loc).first.wait_for(
                state="attached", timeout=7000
            )
        except Exception as e:
            logger.debug(f"⏱️ Japarts timeout waiting for result {part}: {e}")
            return None, None

        # Дальше — синхронная логика, без await
        # Проверяем "не найдено" через уже загруженный DOM
        not_found_els = await not_found_loc.all()
        for el in not_found_els:
            if _NOT_FOUND_TEXT in await el.inner_text():
                return None, None

        # Собираем все строки с весом
        weight_els = await weight_loc.all()
        if not weight_els:
            return None, None

        texts = [await el.inner_text() for el in weight_els]

        # Приоритет 1: строки с обоими весами
        for text in texts:
            physical, volumetric = _extract_weights(text)
            if physical and volumetric:
                return physical, volumetric

        # Приоритет 2: строки хотя бы с одним весом
        for text in texts:
            physical, volumetric = _extract_weights(text)
            if physical or volumetric:
                return physical, volumetric

        return None, None

    except Exception as e:
        logger.error(f"❌ Japarts error {part}: {e}")
        return None, None

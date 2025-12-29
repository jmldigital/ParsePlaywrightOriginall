# scraper_armtek.py

from utils import get_site_logger
import logging
from playwright.async_api import Page, TimeoutError as PlaywrightTimeout


async def scrape_weight_armtek(
    page: Page, part: str, logger: logging.Logger
) -> tuple[str, str]:
    """ВРЕМЕННАЯ заглушка armtek.ru — возвращает тестовые веса"""
    logger.info(f"🔍 armtek.ru: поиск весов для {part}")

    # 🆕 ТЕСТОВЫЕ ДАННЫЕ (замени на реальный парсинг позже)
    import random

    # physical = f"{random.uniform(0.01, 5.0):.3f}"  # armtek_1
    # volumetric = f"{random.uniform(0.05, 10.0):.4f}"  # armtek_2

    physical = "armtek_1"
    volumetric = "armtek_2"

    logger.info(f"🧪 ЗАГЛУШКА armtek.ru: физ={physical}кг, объём={volumetric}кг")
    return physical, volumetric  # Возвращает tuple!

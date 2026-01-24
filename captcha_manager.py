# captcha_manager.py
import asyncio
from pathlib import Path
from typing import Dict
from utils import solve_captcha_universal, _save_debug_screenshot


class CaptchaManager:
    def __init__(self, max_concurrent: int = 1):
        self._semaphore = asyncio.Semaphore(
            max_concurrent
        )  # Макс. 1 капча одновременно!
        self._active_captchas: Dict[str, str] = {}  # site_key → статус

    async def solve_captcha(self, page, logger, site_key: str, selectors: dict) -> bool:
        """
        Очередь на капчу: максимум 1 решение одновременно!
        """
        async with self._semaphore:  # 🆕 БЛОКИРУЕМ ВСЕХ КРОМЕ 1‑ГО!
            logger.info(f"🔒 [{site_key}] Капча очередь: мой черёд!")

            if site_key in self._active_captchas:
                logger.info(f"⏳ [{site_key}] Уже решаем, ждём...")

            self._active_captchas[site_key] = "active"

            try:
                # ТВОЯ ФУНКЦИЯ!
                success = await solve_captcha_universal(
                    page=page,
                    logger=logger,
                    site_key=site_key,
                    selectors=selectors,
                    max_attempts=3,
                )

                self._active_captchas[site_key] = "success" if success else "failed"
                logger.info(
                    f"✅ [{site_key}] CaptchaManager: {'OK' if success else 'FAIL'}"
                )
                return success

            except Exception as e:
                logger.error(f"❌ [{site_key}] CaptchaManager error: {e}")
                self._active_captchas[site_key] = "error"
                return False
            finally:
                if site_key in self._active_captchas:
                    del self._active_captchas[site_key]


# Глобальный экземпляр
captcha_manager = CaptchaManager(max_concurrent=1)

# utils.py
import logging
import re
import time

import pandas as pd
from pathlib import Path
from datetime import datetime
import json
from config import (
    ARMTEK_P_W,
    ARMTEK_V_W,
    JPARTS_P_W,
    JPARTS_V_W,
    stparts_price,
    stparts_delivery,
    avtoformula_price,
    avtoformula_delivery,
    API_KEY_2CAPTCHA,
)
from config import input_price
import asyncio

import shutil
from typing import List

# Файлы
LOG_FILE = "logs/parser.log"
COUNTER_FILE = "logs/run_counter.json"

# Глобальный логгер (инициализируется один раз)
_logger = None


import base64

import io
import os

from twocaptcha import TwoCaptcha
from PIL import Image
from playwright.async_api import Page

API_KEY_2CAPTCHA = os.getenv("API_KEY_2CAPTCHA")  # или откуда ты его берёшь


# async def solve_captcha_universal(
#     page: Page,
#     logger,
#     site_key: str,
#     selectors: dict,
#     max_attempts: int = 3,
#     scale_factor: int = 3,
#     check_changed: bool = True,
#     wait_after_submit_ms: int = 5000,
# ) -> bool:
#     """
#     Универсальное решение капчи через 2Captcha.

#     :param page: Playwright Page
#     :param logger: логгер (logger_avto / logger_armtek / др.)
#     :param site_key: строка для логов и имён файлов ("avtoformula", "armtek", ...)
#     :param selectors: словарь с селекторами:
#         {
#             "captcha_img": "...",
#             "captcha_input": "...",
#             "submit": "..."  # CSS / XPath
#         }
#     :param max_attempts: максимум попыток распознать и отправить капчу
#     :param scale_factor: во сколько раз увеличивать картинку (1 — без увеличения)
#     :param check_changed: проверять ли, изменилась ли капча во время распознавания
#     :param wait_after_submit_ms: пауза после отправки, мс
#     """
#     solver = TwoCaptcha(API_KEY_2CAPTCHA)

#     captcha_text = None
#     img = None
#     original_img_bytes = None

#     try:
#         captcha_img = page.locator(selectors["captcha_img"])

#         # Если капчи нет — выходим
#         if not await captcha_img.is_visible():
#             logger.info(f"[{site_key}] CAPCHA finde attantions")
#             return False

#         for attempt in range(1, max_attempts + 1):
#             logger.info(
#                 f"[{site_key}] 📸 Screenshot of the captcha (attempt {attempt}/{max_attempts})"
#             )

#             # 1) Скриншот
#             original_img_bytes = await captcha_img.screenshot()
#             logger.info(
#                 f"[{site_key}] 📸 Captcha screenshot received, size: {len(original_img_bytes)} bite"
#             )

#             if not original_img_bytes or len(original_img_bytes) < 100:
#                 raise Exception("The image data is empty or too small")

#             # 2) Открываем и масштабируем
#             img = Image.open(io.BytesIO(original_img_bytes))
#             logger.info(
#                 f"[{site_key}] ✅ The image is open: {img.format} {img.size} {img.mode}"
#             )

#             if scale_factor != 1:
#                 img = img.resize(
#                     (img.width * scale_factor, img.height * scale_factor),
#                     Image.BICUBIC,
#                 )
#                 logger.info(
#                     f"[{site_key}] 🔍 The image is enlarged to: {img.size}, scale={scale_factor}"
#                 )

#             # 3) Готовим base64
#             buf = io.BytesIO()
#             img.save(buf, format="PNG")
#             captcha_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")

#             # После #3) Готовим base64, ПЕРЕД Sending a captcha
#             # После buf = io.BytesIO() + captcha_base64 = ...
#             ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#             sent_dir = f"screenshots/{site_key}/sent"
#             os.makedirs(sent_dir, exist_ok=True)
#             sent_path = f"{sent_dir}/sent_attempt{attempt}_{ts}.png"
#             img.save(sent_path)
#             logger.info(f"[{site_key}] 📤 SENT PNG: {sent_path}")

#             # 4) Отправляем в 2Captcha с retry
#             await asyncio.sleep(3)
#             logger.info(f"[{site_key}] Sending a captcha to 2Captcha")

#             # ПЕРЕД циклом for api_attempt
#             # Добавьте ВМЕСТО:
#             # try:
#             #     balance = await asyncio.to_thread(solver.balance)
#             #     logger.info(f"[{site_key}] 💰 2Captcha BALANCE: ${balance}")
#             # except:
#             #     logger.warning(f"[{site_key}] 💰 Cannot check balance")

#             captcha_text = None
#             for api_attempt in range(3):
#                 try:
#                     result = await asyncio.wait_for(
#                         asyncio.to_thread(solver.normal, captcha_base64), timeout=220.0
#                     )
#                     ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#                     response_path = f"screenshots/{site_key}/sent/response_attempt{attempt}_{api_attempt}_{ts}.json"
#                     with open(response_path, "w") as f:
#                         json.dump(result, f, indent=2)  # import json сверху!
#                     logger.info(f"[{site_key}] 📥 Response saved")
#                     logger.info(f"[{site_key}] 2Captcha RAW RESPONSE: {result}")
#                     captcha_text = result["code"]
#                     logger.info(f"[{site_key}] ✅ Capcha recognized: {captcha_text}")
#                     break
#                 except asyncio.TimeoutError:
#                     logger.error(
#                         f"[{site_key}] ⏰ 2Captcha TIMEOUT 60s (attempt {api_attempt+1})"
#                     )
#                 except Exception as e:
#                     logger.error(
#                         f"[{site_key}] ❌ 2Captcha ERROR (attempt {api_attempt+1}): {e} | {type(e)}"
#                     )

#                 # 🔥 BACKOFF ПОСЛЕ ЛЮБОЙ ОШИБКИ (один уровень indent)
#                 if api_attempt < 2:
#                     await asyncio.sleep(10 + api_attempt * 5)  # 1s, 2s, 4s
#                 else:
#                     logger.error(f"[{site_key}] ❌ 2Captcha FAILED after 3 attempts")
#                     return False

#             if not captcha_text:
#                 return False

#             # ✅ ВЕРХНИЙ РЕГИСТР ПОСЛЕ присвоения
#             captcha_text = captcha_text.upper()
#             logger.info(f"[{site_key}] ✅ Capcha in upper register: {captcha_text}")

#             # 5) (опционально) проверяем, не изменилась ли капча
#             if check_changed:
#                 current_img_bytes = await captcha_img.screenshot()
#                 if current_img_bytes != original_img_bytes:
#                     logger.warning(f"[{site_key}] ⚠️ Capcha changes, tring else")
#                     os.makedirs(f"screenshots/{site_key}/changed", exist_ok=True)
#                     ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#                     Image.open(io.BytesIO(original_img_bytes)).save(
#                         f"screenshots/{site_key}/changed/original_{ts}.png"
#                     )
#                     Image.open(io.BytesIO(current_img_bytes)).save(
#                         f"screenshots/{site_key}/changed/changed_{ts}.png"
#                     )
#                     continue  # следующая попытка

#             # 6) Вводим капчу
#             input_el = page.locator(selectors["captcha_input"])
#             await input_el.fill(captcha_text)
#             logger.info(f"[{site_key}] ✅ Capcha entered: {captcha_text}")

#             # 7) Нажимаем submit
#             submit_sel = selectors["submit"]
#             submit_button = page.locator(submit_sel)
#             await submit_button.click()
#             logger.info(f"[{site_key}] ✅ Button pressed ({submit_sel})")

#             # 8) Ждём обновления страницы
#             await page.wait_for_timeout(wait_after_submit_ms)

#             # 9) Проверяем, исчезла ли капча
#             if not await captcha_img.is_visible():
#                 logger.info(f"[{site_key}] ✅ Capcha sucsess resolved")

#                 os.makedirs(f"screenshots/{site_key}/success", exist_ok=True)
#                 ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#                 success_path = (
#                     f"screenshots/{site_key}/success/success_{captcha_text}_{ts}.png"
#                 )
#                 img.save(success_path)
#                 logger.info(f"[{site_key}] 🎉 sucsess Capcha saved: {success_path}")
#                 return True

#             # Капча не ушла — делаем лог и идём на следующую попытку
#             logger.warning(
#                 f"[{site_key}] ⚠️ The captcha is still visible after the attempt {attempt}/{max_attempts}"
#             )
#             ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#             os.makedirs(f"screenshots/{site_key}/failed", exist_ok=True)
#             await page.screenshot(
#                 path=f"screenshots/{site_key}/failed/page_failed_{captcha_text}_{ts}.png"
#             )
#             os.makedirs(f"screenshots/{site_key}/processed", exist_ok=True)
#             img.save(
#                 f"screenshots/{site_key}/processed/processed_{captcha_text}_{ts}.png"
#             )

#             await page.wait_for_timeout(2000)

#         logger.error(
#             f"[{site_key}] ❌ The maximum number of attempts has been exceeded ({max_attempts})"
#         )
#         return False

#     except Exception as e:
#         logger.error(f"[{site_key}] ❌ Captcha solution error: {e}", exc_info=True)
#         logger.error(
#             f"[{site_key}] Full solver response: {e.__dict__ if hasattr(e, '__dict__') else 'No details'}"
#         )
#         ts = datetime.now().strftime("%Y%m%d_%H%M%S")
#         label = captcha_text if captcha_text else "unknown"

#         try:
#             os.makedirs(f"screenshots/{site_key}/errors", exist_ok=True)
#             await page.screenshot(
#                 path=f"screenshots/{site_key}/errors/error_page_{label}_{ts}.png"
#             )
#         except Exception as se:
#             logger.error(f"[{site_key}] Couldn't save screenshot of the page: {se}")

#         try:
#             if img is not None:
#                 os.makedirs(f"screenshots/{site_key}/processed", exist_ok=True)
#                 img.save(
#                     f"screenshots/{site_key}/processed/error_processed_{label}_{ts}.png"
#                 )
#         except Exception as se:
#             logger.error(f"[{site_key}] Не удалось сохранить обработанную капчу: {se}")

#         return False


async def solve_captcha_universal(
    page: Page,
    logger,
    site_key: str,
    selectors: dict,
    max_attempts: int = 3,
    scale_factor: int = 3,
    check_changed: bool = True,
    wait_after_submit_ms: int = 5000,
) -> bool:
    """
    Универсальное решение капчи через 2Captcha.
    """
    solver = TwoCaptcha(API_KEY_2CAPTCHA)

    captcha_text = None
    img = None
    original_img_bytes = None

    try:
        captcha_img = page.locator(selectors["captcha_img"])

        # Если капчи нет — выходим
        if not await captcha_img.is_visible():
            logger.info(f"[{site_key}] CAPTCHA not visible")
            return False

        for attempt in range(1, max_attempts + 1):
            logger.info(
                f"[{site_key}] 📸 Screenshot of the captcha (attempt {attempt}/{max_attempts})"
            )

            # 1) Скриншот
            original_img_bytes = await captcha_img.screenshot()
            logger.info(
                f"[{site_key}] 📸 Captcha screenshot received, size: {len(original_img_bytes)} bytes"
            )

            if not original_img_bytes or len(original_img_bytes) < 100:
                raise Exception("The image data is empty or too small")

            # 2) Открываем и масштабируем
            img = Image.open(io.BytesIO(original_img_bytes))
            logger.info(
                f"[{site_key}] ✅ The image is open: {img.format} {img.size} {img.mode}"
            )

            if scale_factor != 1:
                img = img.resize(
                    (img.width * scale_factor, img.height * scale_factor),
                    Image.BICUBIC,
                )
                logger.info(
                    f"[{site_key}] 🔍 The image is enlarged to: {img.size}, scale={scale_factor}"
                )

            # 3) Готовим base64
            buf = io.BytesIO()
            img.save(buf, format="PNG")
            captcha_base64 = base64.b64encode(buf.getvalue()).decode("utf-8")

            # Сохраняем отправленную капчу
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            sent_dir = f"screenshots/{site_key}/sent"
            os.makedirs(sent_dir, exist_ok=True)
            sent_path = f"{sent_dir}/sent_attempt{attempt}_{ts}.png"
            img.save(sent_path)
            logger.info(f"[{site_key}] 📤 SENT PNG: {sent_path}")

            # 4) Отправляем в 2Captcha с retry
            await asyncio.sleep(3)
            logger.info(f"[{site_key}] Sending a captcha to 2Captcha")

            captcha_text = None

            # ✅ ИСПРАВЛЕНО: Правильная логика retry для 2Captcha
            for api_attempt in range(3):
                try:
                    logger.info(f"[{site_key}] 🔄 2Captcha attempt {api_attempt+1}/3")

                    # ✅ Увеличен таймаут до 150 секунд
                    result = await asyncio.wait_for(
                        asyncio.to_thread(solver.normal, captcha_base64),
                        timeout=90.0,  # ✅ Было 220, стало 150
                    )

                    # Сохраняем ответ
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    response_path = f"screenshots/{site_key}/sent/response_attempt{attempt}_{api_attempt}_{ts}.json"
                    with open(response_path, "w") as f:
                        json.dump(result, f, indent=2)

                    logger.info(f"[{site_key}] 📥 Response saved")
                    logger.info(f"[{site_key}] 2Captcha RAW RESPONSE: {result}")

                    captcha_text = result.get("code")
                    if captcha_text:
                        logger.info(
                            f"[{site_key}] ✅ Captcha recognized: {captcha_text}"
                        )
                        break  # ✅ Успех - выходим из цикла retry
                    else:
                        logger.warning(f"[{site_key}] ⚠️ Empty code in response")

                except asyncio.TimeoutError:
                    logger.error(
                        f"[{site_key}] ⏰ 2Captcha TIMEOUT (attempt {api_attempt+1}/3)"
                    )
                    # 🔥 ДИАГНОСТИКА
                    try:
                        # 1. Быстрая проверка баланса (1–2 сек)
                        balance = await asyncio.wait_for(
                            asyncio.to_thread(solver.balance), timeout=5.0
                        )
                        logger.warning(f"[{site_key}] 💰 Balance OK: ${balance}")
                    except:
                        logger.error(f"[{site_key}] ❌ Balance check FAILED!")

                    try:
                        # 2. Status API (0.5 сек)
                        status = await asyncio.wait_for(
                            asyncio.to_thread(solver.getbalance), timeout=3.0
                        )
                        logger.warning(f"[{site_key}] 📊 2Captcha status: {status}")
                    except:
                        logger.error(f"[{site_key}] ❌ Status check FAILED!")

                except Exception as e:
                    logger.error(
                        f"[{site_key}] ❌ 2Captcha ERROR (attempt {api_attempt+1}/3): {e}"
                    )

                # ✅ BACKOFF между попытками (но НЕ после успеха!)
                if api_attempt < 2 and not captcha_text:
                    backoff_delay = 5 + api_attempt * 10  # 10s, 20s
                    logger.info(
                        f"[{site_key}] ⏳ Waiting {backoff_delay}s before retry..."
                    )
                    await asyncio.sleep(backoff_delay)

            # ✅ После всех попыток проверяем результат
            if not captcha_text:
                logger.error(
                    f"[{site_key}] ❌ Failed to recognize captcha after 3 API attempts"
                )
                # Пробуем следующую попытку (attempt)
                if attempt < max_attempts:
                    logger.info(f"[{site_key}] 🔄 Trying with new captcha image...")
                    await asyncio.sleep(5)
                    continue
                else:
                    return False

            # ✅ ВЕРХНИЙ РЕГИСТР
            captcha_text = captcha_text.upper().strip()
            logger.info(f"[{site_key}] ✅ Captcha in upper register: '{captcha_text}'")

            # 5) (опционально) проверяем, не изменилась ли капча
            if check_changed:
                await asyncio.sleep(1)  # Даём время на обновление
                try:
                    current_img_bytes = await captcha_img.screenshot()
                    if current_img_bytes != original_img_bytes:
                        logger.warning(
                            f"[{site_key}] ⚠️ Captcha changed during recognition, retrying..."
                        )
                        os.makedirs(f"screenshots/{site_key}/changed", exist_ok=True)
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        Image.open(io.BytesIO(original_img_bytes)).save(
                            f"screenshots/{site_key}/changed/original_{ts}.png"
                        )
                        Image.open(io.BytesIO(current_img_bytes)).save(
                            f"screenshots/{site_key}/changed/changed_{ts}.png"
                        )
                        continue  # следующая попытка
                except Exception as e:
                    logger.warning(
                        f"[{site_key}] Could not check if captcha changed: {e}"
                    )

            # 6) Вводим капчу
            try:
                input_el = page.locator(selectors["captcha_input"])
                await input_el.clear()  # ✅ Очищаем перед вводом
                await input_el.fill(captcha_text)
                logger.info(f"[{site_key}] ✅ Captcha entered: '{captcha_text}'")
            except Exception as e:
                logger.error(f"[{site_key}] ❌ Failed to enter captcha: {e}")
                continue

            # 7) Нажимаем submit
            try:
                submit_sel = selectors["submit"]
                submit_button = page.locator(submit_sel)

                # ✅ Проверяем что кнопка видима
                if not await submit_button.is_visible():
                    logger.warning(
                        f"[{site_key}] ⚠️ Submit button not visible: {submit_sel}"
                    )

                await submit_button.click()
                logger.info(f"[{site_key}] ✅ Button pressed ({submit_sel})")
            except Exception as e:
                logger.error(f"[{site_key}] ❌ Failed to click submit: {e}")
                continue

            # 8) Ждём обновления страницы
            logger.info(
                f"[{site_key}] ⏳ Waiting {wait_after_submit_ms}ms after submit..."
            )
            await page.wait_for_timeout(wait_after_submit_ms)

            # 9) Проверяем, исчезла ли капча
            try:
                is_still_visible = await captcha_img.is_visible()

                if not is_still_visible:
                    logger.info(f"[{site_key}] ✅ Captcha successfully resolved!")

                    os.makedirs(f"screenshots/{site_key}/success", exist_ok=True)
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    success_path = f"screenshots/{site_key}/success/success_{captcha_text}_{ts}.png"
                    img.save(success_path)
                    logger.info(
                        f"[{site_key}] 🎉 Success captcha saved: {success_path}"
                    )
                    return True
                else:
                    logger.warning(
                        f"[{site_key}] ⚠️ Captcha still visible after attempt {attempt}/{max_attempts}"
                    )
            except Exception as e:
                logger.warning(f"[{site_key}] Could not check captcha visibility: {e}")

            # Капча не ушла — делаем лог и идём на следующую попытку
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            os.makedirs(f"screenshots/{site_key}/failed", exist_ok=True)

            try:
                await page.screenshot(
                    path=f"screenshots/{site_key}/failed/page_failed_{captcha_text}_{ts}.png"
                )
            except:
                pass

            try:
                os.makedirs(f"screenshots/{site_key}/processed", exist_ok=True)
                img.save(
                    f"screenshots/{site_key}/processed/processed_{captcha_text}_{ts}.png"
                )
            except:
                pass

            # ✅ Задержка перед следующей попыткой
            if attempt < max_attempts:
                delay = 3 + attempt * 2  # 5s, 7s, 9s
                logger.info(f"[{site_key}] ⏳ Waiting {delay}s before next attempt...")
                await page.wait_for_timeout(delay * 1000)

        logger.error(f"[{site_key}] ❌ Maximum attempts exceeded ({max_attempts})")
        return False

    except Exception as e:
        logger.error(f"[{site_key}] ❌ Captcha solution error: {e}", exc_info=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        label = captcha_text if captcha_text else "unknown"

        try:
            os.makedirs(f"screenshots/{site_key}/errors", exist_ok=True)
            await page.screenshot(
                path=f"screenshots/{site_key}/errors/error_page_{label}_{ts}.png"
            )
        except Exception as se:
            logger.error(f"[{site_key}] Couldn't save screenshot: {se}")

        try:
            if img is not None:
                os.makedirs(f"screenshots/{site_key}/processed", exist_ok=True)
                img.save(
                    f"screenshots/{site_key}/processed/error_processed_{label}_{ts}.png"
                )
        except Exception as se:
            logger.error(f"[{site_key}] Couldn't save processed captcha: {se}")

        return False


# def get_site_logger(site_name: str) -> logging.Logger:
#     """Создает отдельный логгер для конкретного сайта"""
#     log_dir = Path("logs")
#     log_dir.mkdir(exist_ok=True)

#     log_file = log_dir / f"{site_name}.log"

#     logger = logging.getLogger(site_name)
#     if logger.handlers:
#         return logger  # избегаем дублирования

#     logger.setLevel(logging.INFO)
#     fh = logging.FileHandler(log_file, encoding="utf-8", mode="w")
#     fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
#     fh.setFormatter(fmt)
#     logger.addHandler(fh)

#     return logger


def get_site_logger(site_name: str) -> logging.Logger:
    """Логгер для сайта: ФАЙЛ + КОНСОЛЬ UTF-8 (Windows/Ubuntu)"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"{site_name}.log"

    logger = logging.getLogger(site_name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    fmt_str = f"[ {site_name} ] %(asctime)s - %(levelname)s - %(message)s"

    # 1. ФАЙЛ логгер
    fh = logging.FileHandler(log_file, encoding="utf-8", mode="w")
    fh.setFormatter(logging.Formatter(fmt_str))
    logger.addHandler(fh)

    # 2. КОНСОЛЬ логгер — БЕЗОПАСНЫЙ UTF-8
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(fmt_str, datefmt="%H:%M:%S"))

    # 🔥 UTF-8 ТОЛЬКО где возможно
    try:
        if os.name == "nt":
            ch.stream = io.TextIOWrapper(ch.stream.buffer, encoding="utf-8")
        else:
            # Ubuntu — try encoding
            if hasattr(ch.stream, "encoding"):
                ch.stream.encoding = "utf-8"
    except AttributeError:
        pass  # Игнорируем ошибки кодировки

    logger.addHandler(ch)
    logger.propagate = False

    return logger


def get_run_count():
    """Возвращает номер текущего запуска (счётчик)"""
    path = Path(COUNTER_FILE)
    path.parent.mkdir(exist_ok=True)

    try:
        with open(path, "r+", encoding="utf-8") as f:
            data = json.load(f)
            count = data.get("count", 0) + 1
            f.seek(0)
            json.dump({"count": count}, f, ensure_ascii=False, indent=2)
            f.truncate()
    except (FileNotFoundError, json.JSONDecodeError):
        count = 1
        path.write_text(
            json.dumps({"count": count}, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:
        print(f"⚠️ Ошибка работы со счётчиком: {e}")
        count = 1

    return count


def setup_logger():
    """Настраивает основной логгер"""
    global _logger
    if _logger is not None:
        return _logger

    count = get_run_count()
    log_path = Path(LOG_FILE)

    if count % 10 == 1:
        if log_path.exists():
            log_path.unlink()
            print(f"parser.log очищен (запуск №{count})")

    _logger = logging.getLogger("parser")
    _logger.setLevel(logging.INFO)

    if _logger.handlers:
        _logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    fh = logging.FileHandler(log_path, encoding="utf-8", mode="w")
    fh.setFormatter(formatter)
    _logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    _logger.addHandler(ch)

    _logger.info(f"🔄 Запуск парсера №{count}")
    return _logger


def get_logger():
    """Возвращает глобальный логгер (ленивая инициализация)"""
    global _logger
    if _logger is None:
        _logger = setup_logger()
    return _logger


logger = get_logger()  # ← теперь безопасно


def parse_price(text):
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return None

    # Если это уже int или float — вернуть как float
    if isinstance(text, (int, float)):
        return float(text)

    # Дальше обычная логика для строки
    clean = re.sub(r"[^\d,.\s]", "", str(text).lower()).strip()
    clean = clean.replace("\u00a0", "").replace(" ", "")

    # Попытка парсинга с учетом десятичных знаков
    try:
        # Заменяем запятую на точку для корректного парсинга
        normalized = clean.replace(",", ".")
        return float(normalized)
    except (ValueError, AttributeError):
        return None


def clean_text(s):
    if isinstance(s, str):
        # Удаляем управляющие символы
        return re.sub(r"[\x00-\x1F]", "", s)
    return s


def preprocess_dataframe(df):
    """
    Предобработка DataFrame:
    - Конвертирует имена столбцов в строки (удаляет .0)
    - Очищает значения от пробелов
    - Преобразует столбец с ценой: если не float, приводит к float через безопасную обработку запятых
    """

    # Преобразование имён столбцов
    df.columns = df.columns.astype(str).str.replace(".0", "", regex=False).str.strip()

    # Обработка строковых столбцов — убираем лишние пробелы
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()

    # Обработка столбца с ценой (например, input_price)
    if input_price in df.columns:
        # Только если не float — иначе не трогаем
        if df[input_price].dtype != "float64":
            # Преобразуем ВСЕ значения к строкам, чтобы .str.replace не упал с ошибкой
            df[input_price] = (
                df[input_price].astype(str).str.replace(",", ".", regex=False)
            )
            # Конвертируем в float, нечисловые значения станут NaN
            df[input_price] = pd.to_numeric(df[input_price], errors="coerce")
            # Явно приводим к float dtype для совместимости
            df[input_price] = df[input_price].astype("float64")

    df = df.applymap(clean_text)

    return df


def normalize_brand(brand_str):
    if not brand_str:
        return ""
    return re.sub(r"[^a-z0-9]", "", str(brand_str).lower())


def brand_matches(search_brand, result_brand):
    if not search_brand or not result_brand:
        return False
    norm_search = normalize_brand(search_brand)
    norm_result = normalize_brand(result_brand)

    if norm_search == norm_result:
        return True
    if norm_search in norm_result:
        return True
    return False


def consolidate_weights(df):
    """
    Из 4 колонок весов → 2 финальные
    Приоритет: japarts > armtek
    """
    logger.info("🔄 Консолидация весов: 4 колонки → 2 финальные")

    # Создаём финальные колонки
    df["physical_weight"] = None
    df["volumetric_weight"] = None

    for idx, row in df.iterrows():
        # Физический вес: japarts ИЛИ armtek
        if pd.notna(row[JPARTS_P_W]):
            df.at[idx, "physical_weight"] = row[JPARTS_P_W]
        elif pd.notna(row[ARMTEK_P_W]):
            df.at[idx, "physical_weight"] = row[ARMTEK_P_W]

        # Объёмный вес: japarts ИЛИ armtek
        if pd.notna(row[JPARTS_V_W]):
            df.at[idx, "volumetric_weight"] = row[JPARTS_V_W]
        elif pd.notna(row[ARMTEK_V_W]):
            df.at[idx, "volumetric_weight"] = row[ARMTEK_V_W]

    # Удаляем промежуточные колонки
    cols_to_drop = [
        JPARTS_P_W,
        JPARTS_V_W,
        ARMTEK_P_W,
        ARMTEK_V_W,
        stparts_price,
        stparts_delivery,
        avtoformula_price,
        avtoformula_delivery,
    ]
    df.drop(columns=[col for col in cols_to_drop if col in df.columns], inplace=True)

    logger.info("✅ Консолидация весов завершена")
    return df


async def save_debug_info(
    page: Page,
    part: str,
    reason: str,
    logger: logging.Logger = None,
    site: str = "unknown",
):
    """DEBUG: скрин + HTML для armtek/japarts"""
    if logger is None:
        logger = logging.getLogger(__name__)

    os.makedirs(f"debug_{site}", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    screenshot_path = f"debug_{site}/{reason}_{part}_{timestamp}.png"
    await page.screenshot(path=screenshot_path)

    html_path = f"debug_{site}/{reason}_{part}_{timestamp}.html"
    html_content = await page.content()
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.warning(f"📸 DEBUG {reason} {site} {part}:")
    logger.warning(f"   📍 URL: {page.url}")
    logger.warning(f"   🖼️ {screenshot_path}")
    logger.warning(f"   📄 {html_path}")


def clear_debug_folders_sync(sites: List[str], logger: logging.Logger):
    """Синхронная очистка debug_* + скринов капчи перед запуском."""
    for site in sites:
        # 1️⃣ Debug папки (как было)
        debug_dir = f"debug_{site}"
        if os.path.exists(debug_dir):
            _safe_rmtree(debug_dir, logger, f"debug_{site}")

        # 2️⃣ Скрины капчи
        screenshot_base = f"screenshots/{site}"
        if os.path.exists(screenshot_base):
            captcha_folders = [
                "sent",
                "success",
                "failed",
                "changed",
                "errors",
                "processed",
            ]
            for folder in captcha_folders:
                folder_path = f"{screenshot_base}/{folder}"
                if os.path.exists(folder_path):
                    _safe_rmtree(folder_path, logger, f"{site}/{folder}")

            # ✅ Создать пустые папки заново
            os.makedirs(screenshot_base, exist_ok=True)
            for folder in captcha_folders:
                os.makedirs(f"{screenshot_base}/{folder}", exist_ok=True)
            logger.info(f"🧹 Cleared & recreated screenshots/{site}/")


def _safe_rmtree(path: str, logger, label: str, max_retries: int = 3):
    """Безопасное удаление с retry."""
    for retry in range(max_retries):
        try:
            shutil.rmtree(path, ignore_errors=True)
            logger.info(f"🧹 Cleared {label}")
            return
        except Exception as e:
            logger.warning(f"Failed to clear {label} (retry {retry+1}): {e}")
            if retry < max_retries - 1:
                time.sleep(1)
            else:
                logger.error(f"❌ Could not clear {label}")

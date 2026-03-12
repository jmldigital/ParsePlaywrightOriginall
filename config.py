# config.py
import os
from pathlib import Path
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)  # ✅ Локальный логгер
load_dotenv()


TEMP_RAW = 20
MAX_WORKERS = 1
DELAY_EXIST = False  # ✅ Задержка перед тасками


# === API и авторизация ===

AVTO_LOGIN = os.getenv("AVTO_LOGIN", "your_login_here")
AVTO_PASSWORD = os.getenv("AVTO_PASSWORD", "your_password_here")

API_KEY_2CAPTCHA = os.getenv("API_KEY_2CAPTCHA", "your_api_key_here")
PROXY_COUNTRY = "Russia"
PROXY_PROTOCOL = "http"
PROXY_CONNECTIONS = 5
PROXY_IP = "152.53.136.84"
PROXY_USERNAME = "u038f310456a605c8"
PROXY_PASSWORD = "u038f310456a605c1"

# === Telegram ===
BOT_TOKEN = os.getenv("BOT_TOKEN", "8364237483AAERd9UAqQO_EAPt62AepFSojT41v9Vmw3s")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "-4688651319"))
SEND_TO_TELEGRAM = False

# === Файлы ===
INPUT_FILE = "input/наличие.xlsx"
TEMP_FILES_DIR = "input/temp_file.xlsx"
INTERIM_FILE = "input/output_interim.xlsx"
# TEMP_FILES_DIR.mkdir(parents=True, exist_ok=True)  # Авто-создание при импорте!
COOKIE_FILE = "output/avtoformula_cookies.json"
STATE_FILE = "output/state.json"
CACHE_FILE = "output/cache.json"


# ENABLE_AVTOFORMULA = True
ENABLE_NAME_PARSING = os.getenv("ENABLE_NAME_PARSING", "False").lower() == "true"
ENABLE_WEIGHT_PARSING = os.getenv("ENABLE_WEIGHT_PARSING", "False").lower() == "true"
ENABLE_PRICE_PARSING = os.getenv("ENABLE_PRICE_PARSING", "False").lower() == "true"


# === Параметры ===
MAX_ROWS = 23000
SAVE_INTERVAL = 10
PAGE_LOAD_TIMEOUT = 60
DEFAULT_WAIT = 15
CAPTCHA_WAIT = 5
MAX_RETRIES = 3
RETRY_DELAY = 2
TASK_TIMEOUT = 180
PROXY_TIMOUT = 240

# === Колонки для поиска цен ===
stparts_price = "stparts_price"
stparts_delivery = "stparts_delivery"
avtoformula_price = "avtoformula_price"
avtoformula_delivery = "avtoformula_delivery"
adeo_price = "adeo_price"
adeo_delivery = "adeo_delivery"

corrected_price = "corrected_price"


# === Колонки для поиска весов ===
JPARTS_P_W = "japarts_physical_weight"
JPARTS_V_W = "japarts_volumetric_weight"
ARMTEK_P_W = "armtek_physical_weight"
ARMTEK_V_W = "armtek_volumetric_weight"
corrected_price = "corrected_price"


# === Названия столбцов во входном файле ===
INPUT_COL_ARTICLE = "1"  # ← или как у тебя в файле
INPUT_COL_BRAND = "3"  # ← или "Производитель", "Brand" и т.п.
input_price = "5"  # индекс колонки             # ← если нужно читать цену по имени


# === Селекторы ===
SELECTORS = {
    "stparts": {
        "captcha_img": "img.captchaImg",
        "captcha_input": "input[name='captcha']",
        "captcha_submit": "#captchaSubmitBtn",
        "results_table": "table.globalResult.searchResultsSecondStep",
        "result_row": "tr.resultTr2",
        "brand": "td.resultBrand",
        "delivery": "td.resultDeadline",
        "price": "td.resultPrice",
        # новые селекторы для названий деталей
        "case_table": "table.globalCase",
        "case_description": "td.caseDescription",
        "alt_results_table": "table.globalResult",
        "alt_result_row": "tr",
        "alt_result_description": "td.resultDescription",
    },
    "avtoformula": {
        "login_field": "userlogin",
        "password_field": "userpassword",
        "login_button": "input[type='submit'][name='login']",
        "article_field": "article",
        "search_button": 'input[name="search"][data-action="ajaxSearch"]',
        "smode_select": "smode",
        "results_table": "table.web_ar_datagrid.search_results",
        "brand_cell": "td.td_prd_info_link",
        "delivery_cell": "td.td_term",
        "price_cell": "td.td_final_price",
        # селектор имени детали
        "name_cell": "td.td_spare_info",
        # Селекторы капчи
        "captcha_img": 'img[src*="/_phplib/check/img.php"]',
        "captcha_input": "input#ban_hc_code",
        "captcha_submit": 'input[name="submit"][value="Отправить"]',  # новый селектор
    },
    "japarts": {
        "search_form": "form[name='search']",  # 🆕 КОНТЕКСТ!
        "search_input": "form[name='search'] input.search[name='original_id']",  # 🆕 ТОЧНЫЙ!
        "search_button": "form[name='search'] input.postbutton[value='Найти']",  # 🆕 ТОЧНЫЙ!
        "weight_row": "font:has-text('Вес')",
    },
    "armtek": {
        "search_input": "input[data-test-id='search-input']",
        "search_button": "div.search-input__btn button",
        # "captcha_img": "sproit-ui-modal project-ui-captcha img",  # Модалка + img
        "captcha_img": "sproit-ui-modal img[src*='blob']",
        "captcha_input": "sproit-ui-modal project-ui-captcha input.sproit-ui-input__input",  # Модалка + input
        "captcha_submit": "sproit-ui-modal project-ui-captcha sproit-ui-button[color='primary']",  # Модалка + кнопка
        # "product_card": "product-card-info",
        # "product_list": ".results-list",  # ✅ КОНТЕЙНЕР списка
        # "product_cards": ".scroll-item",
        # "weight_value": "div.product-key-values__item__values span.font__body2",
        # 🔥 Ubuntu/Windows универсальные
        "product_list": ".results-list",
        "product_cards": ".scroll-item, div[data-id]",
        "product_card": "product-card-info, [data-id]",
        # 🎯 ВЕСА — множественные приоритеты
        "weight_selectors": [
            ".product-key-values__item__right-side span.font__body2",  # Right-side → span
            ".product-key-values__item__values span.font__body2",  # Values → span
            "[class*='right-side'] span[class*='font']",  # Right-side любой
            "[class*='values'] span[class*='font']",  # Values любой
            "span.font__body2:has-text('кг')",  # Быстрый
            "span.font__body2",  # Основной
        ],
    },
    "adeo": {
        "captcha_img": "",  # reCAPTCHA v2 не имеет img
        "captcha_input": "",  # Не нужен
        "captcha_submit": ".g-recaptcha, iframe[src*='recaptcha']",
        # Для reCAPTCHA v2 используем sitekey напрямую
    },
}


BAD_DETAIL_NAMES = {
    "деталь",
    "автозапчасть",
    "запчасть",
    "part",
}  # Расширяй по необходимости


def reload_config():
    """Принудительно перечитать .env и обновить глобалки"""
    global ENABLE_NAME_PARSING, ENABLE_WEIGHT_PARSING, ENABLE_PRICE_PARSING  # ❌ Без AVTO

    load_dotenv(override=True)
    ENABLE_NAME_PARSING = os.getenv("ENABLE_NAME_PARSING", "False").lower() == "true"
    ENABLE_WEIGHT_PARSING = (
        os.getenv("ENABLE_WEIGHT_PARSING", "False").lower() == "true"
    )
    ENABLE_PRICE_PARSING = os.getenv("ENABLE_PRICE_PARSING", "False").lower() == "true"

    logger.info(
        f"🔄 Config: ИМЕНА={ENABLE_NAME_PARSING}, ВЕСА={ENABLE_WEIGHT_PARSING}, ЦЕНЫ={ENABLE_PRICE_PARSING}"
    )


def get_output_file(mode: str = None) -> str:
    """Только 3 режима парсинга"""
    if mode == "ВЕСА" or ENABLE_WEIGHT_PARSING:
        return "output/веса_деталей.xlsx"
    elif mode == "ИМЕНА" or ENABLE_NAME_PARSING:
        return "output/найденные_имена.xlsx"
    elif mode == "ЦЕНЫ" or ENABLE_PRICE_PARSING:
        return "output/цены_конкурентов.xlsx"
    else:
        raise ValueError("❌ Ни один режим не выбран!")

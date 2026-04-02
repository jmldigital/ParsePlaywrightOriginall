# config.py
import os
from pathlib import Path
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)  # ✅ Локальный логгер
load_dotenv()

# Уровень логирования (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL = "DEBUG"  # Измените на "DEBUG" чтобы видеть все логи

# LOG_LEVEL = "INFO"


ARMTEK_WORKERS = 10
JPARTS_WORKERS = 5
MAX_ROWS = 35000
BATCH_SIZE = 500

YUMBO_PRICE = "yumbo_price"
ENABLE_PRICE_PARSING = True


PROXY_COUNT = 5

ARMTEK_PROXY = False
STPARTS_PROXY = False


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
# TEMP_FILES_DIR.mkdir(parents=True, exist_ok=True)  # Авто-создание при импорте!
COOKIE_FILE = "output/avtoformula_cookies.json"
STATE_FILE = "output/state.json"
CACHE_FILE = "output/cache.json"


# ENABLE_AVTOFORMULA = True
ENABLE_NAME_PARSING = os.getenv("ENABLE_NAME_PARSING", "False").lower() == "true"
ENABLE_WEIGHT_PARSING = os.getenv("ENABLE_WEIGHT_PARSING", "False").lower() == "true"
ENABLE_PRICE_PARSING = os.getenv("ENABLE_PRICE_PARSING", "False").lower() == "true"


# === Параметры ===


PAGE_LOAD_TIMEOUT = 60
DEFAULT_WAIT = 15
CAPTCHA_WAIT = 5
MAX_RETRIES = 3
RETRY_DELAY = 2


# === Колонки для поиска цен ===
stparts_price = "stparts_price"
stparts_delivery = "stparts_delivery"
avtoformula_price = "avtoformula_price"
avtoformula_delivery = "avtoformula_delivery"
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
        "alt_result_description": "td.resultDescription",
    },
    "avtoformula": {
        "login_field": "userlogin",
        "username": "avtoportt",
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
        "captcha_img": "sproit-ui-modal img[src*='blob']",
        # "captcha": ".captcha-modal p:has-text('Введите код с картинки')",
        "captcha": ".captcha-modal p:has-text('Введите код с картинки'), p.sproit-ui-modal-header__title:has-text('Введите код с картинки'), .captcha-modal",
        "captcha_input": "sproit-ui-modal project-ui-captcha input.sproit-ui-input__input",  # Модалка + input
        "captcha_submit": "sproit-ui-modal project-ui-captcha sproit-ui-button[color='primary']",  # Модалка + кнопка
        "specifications": 'a[href="#tech-info"]',
        "rate_limit": "sproit-ui-modal p:has-text('Превышен лимит запросов')",
        "cloudflare": """
.lds-ring,
#challenge-error-text,
#challenge-success-text,
.h2.spacer-bottom:has-text('Verify'),
.core-msg:has-text('review'),
div:has-text('Just a moment'),
div:has-text('Ray ID'),
cf-turnstile-response,
[class*='cf-chl'],
body:has(.lds-ring)
""",
        "product_list": ".search-result__list a, .results-list__items, .card-view",
        "no_results": "div.not-found.ng-star-inserted div.not-found__image",
        "product_cards": ".project-ui-article-card a, .app-article-card-tile a",
        "product_card-list": ".project-ui-article-card, .app-article-card-tile, .list-view, .card-view, .results-list__items",
        "product-card-info": ".project-ui-smart-scroll, .product-card-info, [data-id], .product-card-info__wrapper, #tech-info",
        "product-card-weight": ".product-card-info div:has-text('Вес'), .product-card-info tr:has-text('Вес'), .product-params__item:has-text('Вес'), div.params-row:has-text('Вес'), li:has-text('Вес'), .product-key-values__item__values span",
        # "loading": ".lds-ring, .sproit-ui-loading:visible, .product-card__skeleton_desktop .sproit-ui-skeleton:visible, .product-card__skeleton_mobile sproit-ui-skeleton:visible, sproit-ui-loading, [sproit-ui-loading], [class*='loading'], [class*='skeleton']",
        "loading": """
sproit-ui-loading:visible,
[class*="sproit-ui-loading"]:visible,
[class*="skeleton"]:visible,
[class*="loading"]:visible,
.lds-ring:visible,
[ngcontent-server][class*="loading"]
""",
    },
}
BAD_DETAIL_NAMES = {
    "деталь",
    "Деталь",
    "автозапчасть",
    "запчасть",
    "part",
    "Detail",
    "detail",
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

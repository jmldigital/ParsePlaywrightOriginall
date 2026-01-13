# config.py
import os
from dotenv import load_dotenv
import logging

logger = logging.getLogger(__name__)  # ✅ Локальный логгер
load_dotenv()

# === API и авторизация ===
API_KEY_2CAPTCHA = os.getenv("API_KEY_2CAPTCHA", "your_api_key_here")
AVTO_LOGIN = os.getenv("AVTO_LOGIN", "your_login_here")
AVTO_PASSWORD = os.getenv("AVTO_PASSWORD", "your_password_here")

# === Telegram ===
BOT_TOKEN = os.getenv("BOT_TOKEN", "8364237483AAERd9UAqQO_EAPt62AepFSojT41v9Vmw3s")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "-4688651319"))
SEND_TO_TELEGRAM = False

# === Файлы ===
INPUT_FILE = "input/наличие.xlsx"
TEMP_FILE = "output/наличие_temp.xlsx"
COOKIE_FILE = "output/avtoformula_cookies.json"
STATE_FILE = "output/state.json"
CACHE_FILE = "output/cache.json"


# ENABLE_AVTOFORMULA = True
ENABLE_NAME_PARSING = os.getenv("ENABLE_NAME_PARSING", "False").lower() == "true"
ENABLE_WEIGHT_PARSING = os.getenv("ENABLE_WEIGHT_PARSING", "False").lower() == "true"
ENABLE_PRICE_PARSING = os.getenv("ENABLE_PRICE_PARSING", "False").lower() == "true"


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


# === Параметры ===
MAX_ROWS = 23000
SAVE_INTERVAL = 10
PAGE_LOAD_TIMEOUT = 60
DEFAULT_WAIT = 15
CAPTCHA_WAIT = 5
MAX_RETRIES = 3
RETRY_DELAY = 2
TASK_TIMEOUT = 90

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


TEMP_RAW = 20

# === Названия столбцов во входном файле ===
INPUT_COL_ARTICLE = "1"  # ← или как у тебя в файле
INPUT_COL_BRAND = "3"  # ← или "Производитель", "Brand" и т.п.
input_price = "5"  # индекс колонки             # ← если нужно читать цену по имени

MAX_WORKERS = 4


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
        # "captcha_img": "div.captcha__img-wrap img",
        # "captcha_input": "div.captcha__input-wrapper input",
        # "captcha_submit": "sproit-ui-button",
        # ✅ КАПЧА В МОДАЛКЕ - новые селекторы
        "captcha_img": "sproit-ui-modal project-ui-captcha img",  # Модалка + img
        "captcha_input": "sproit-ui-modal project-ui-captcha input.sproit-ui-input__input",  # Модалка + input
        "captcha_submit": "sproit-ui-modal project-ui-captcha sproit-ui-button[color='primary']",  # Модалка + кнопка
        "product_card": "product-card-info",
        # "product_list": "div.list-view.sit-ui-smart-scroll__items",
        "product_list": ".results-list",  # ✅ КОНТЕЙНЕР списка
        "product_cards": ".scroll-item",
        "weight_value": "div.product-key-values__item__values span.font__body2",
    },
}
BAD_DETAIL_NAMES = {
    "деталь",
    "автозапчасть",
    "запчасть",
    "part",
}  # Расширяй по необходимости

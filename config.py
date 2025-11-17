# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# === API и авторизация ===
API_KEY_2CAPTCHA = os.getenv('API_KEY_2CAPTCHA', 'your_api_key_here')
AVTO_LOGIN = os.getenv('AVTO_LOGIN', 'your_login_here')
AVTO_PASSWORD = os.getenv('AVTO_PASSWORD', 'your_password_here')

# === Telegram ===
BOT_TOKEN = os.getenv('BOT_TOKEN', '8364237483AAERd9UAqQO_EAPt62AepFSojT41v9Vmw3s')
ADMIN_CHAT_ID = int(os.getenv('ADMIN_CHAT_ID', '-4688651319'))
SEND_TO_TELEGRAM = False

# === Файлы ===
INPUT_FILE = 'input/наличие.xls'
OUTPUT_FILE = 'output/наличие_with_competitors.xlsx'
TEMP_FILE = 'output/наличие_temp.xlsx'
COOKIE_FILE = 'output/avtoformula_cookies.json'
STATE_FILE = 'output/state.json'
CACHE_FILE = 'output/cache.json'

# === Параметры ===
MAX_ROWS = 23000
SAVE_INTERVAL = 10
PAGE_LOAD_TIMEOUT = 60
DEFAULT_WAIT = 15
CAPTCHA_WAIT = 5
MAX_RETRIES = 3
RETRY_DELAY = 2

# === Колонки ===
competitor1 = 'stparts_price'
competitor1_delivery = 'stparts_delivery'
competitor2 = 'avtoformula_price'
competitor2_delivery = 'avtoformula_delivery'
corrected_price = 'corrected_price'

ENABLE_AVTOFORMULA = True
ENABLE_NAME_PARSING = os.getenv('ENABLE_NAME_PARSING', 'False').lower() == 'true'

TEMP_RAW = 100

# === Названия столбцов во входном файле ===
INPUT_COL_ARTICLE = '1'         # ← или как у тебя в файле
INPUT_COL_BRAND = '3'             # ← или "Производитель", "Brand" и т.п.
input_price = '5'  # индекс колонки             # ← если нужно читать цену по имени

MAX_WORKERS=4


# === Селекторы ===
SELECTORS = {
    'stparts': {
        'captcha_img': 'img.captchaImg',
        'captcha_input': "input[name='captcha']",
        'captcha_submit': 'captchaSubmitBtn',
        'results_table': 'table.globalResult.searchResultsSecondStep',
        'result_row': 'tr.resultTr2',
        'brand': 'td.resultBrand',
        'delivery': 'td.resultDeadline',
        'price': 'td.resultPrice',
        # новые селекторы для названий деталей
        'case_table': 'table.globalCase',
        'case_description': 'td.caseDescription',
        'alt_results_table': 'table.globalResult',
        'alt_result_row': 'tr',
        'alt_result_description': 'td.resultDescription'
    },
    'avtoformula': {
        'login_field': 'userlogin',
        'password_field': 'userpassword',
        'login_button': "input[type='submit'][name='login']",
        'article_field': 'article',
        'search_button': 'input[name="search"][data-action="ajaxSearch"]',
        'smode_select': 'smode',
        'results_table': 'table.web_ar_datagrid.search_results',
        'brand_cell': 'td.td_prd_info_link',
        'delivery_cell': 'td.td_term',
        'price_cell': 'td.td_final_price',
          # селектор имени детали
        'name_cell': 'td.td_spare_info',
            # Селекторы капчи
        'captcha_img': 'img[src*="/_phplib/check/img.php"]',
        'captcha_input': 'input#ban_hc_code',
        'captcha_submit': 'input[name="submit"][value="Отправить"]',  # новый селектор
    }
}

BAD_DETAIL_NAMES = {"деталь", "автозапчасть", "запчасть", "part"}  # Расширяй по необходимости

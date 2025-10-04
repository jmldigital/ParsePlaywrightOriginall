
# utils.py
import logging
import re
import pandas as pd
from pathlib import Path
import json 

# Файлы
LOG_FILE = 'logs/parser.log'
COUNTER_FILE = 'logs/run_counter.json'


def get_run_count():
    """Возвращает номер текущего запуска (счётчик)"""
    path = Path(COUNTER_FILE)
    path.parent.mkdir(exist_ok=True)  # Создаём папку logs, если её нет

    if path.exists():
        try:
            data = path.read_text(encoding='utf-8')
            count = json.loads(data).get("count", 0) + 1
        except Exception as e:
            print(f"⚠️ Ошибка чтения счётчика запусков: {e}")
            count = 1
    else:
        count = 1

    # Сохраняем новый счётчик
    path.write_text(
        json.dumps({"count": count}, ensure_ascii=False, indent=2),
        encoding='utf-8'
    )
    return count


def setup_logger():
    """Настраивает логгер с ротацией parser.log каждые 10 запусков"""
    import json  # ← локально, чтобы не мешать другим

    count = get_run_count()
    log_path = Path(LOG_FILE)

    # Каждые 10 запусков — пересоздаём лог-файл
    if count % 10 == 1:
        if log_path.exists():
            log_path.unlink()
            print(f"parser.log очищен (запуск №{count})")

    # Настройка логгера
    logger = logging.getLogger("parser")
    logger.setLevel(logging.INFO)

    # Очищаем старые обработчики (чтобы не было дублей)
    if logger.handlers:
        logger.handlers.clear()

    # Формат
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File Handler
    file_handler = logging.FileHandler(log_path, encoding='utf-8', mode='a')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.info(f"🔄 Запуск парсера №{count}")

    return logger


# Глобальный логгер
logger = setup_logger()


# === Остальные функции — без изменений ===
def parse_price(text):
    if not text or not isinstance(text, str):
        return None
    clean = re.sub(r"[^\d,\.\s]", "", text).strip().replace(" ", "")
    if clean.count(",") == 1 and clean.count(".") == 0:
        clean = clean.replace(",", ".")
    elif clean.count(",") > 1:
        clean = clean.replace(",", "")
    try:
        return float(clean)
    except (ValueError, AttributeError):
        logger.warning(f"Не удалось распарсить цену: {text}")
        return None


# def preprocess_dataframe(df):
#     try:
#         brand_col_idx = 2
#         if len(df.columns) > brand_col_idx:
#             df.iloc[:, brand_col_idx] = (
#                 df.iloc[:, brand_col_idx]
#                 .astype(str)
#                 .str.replace('/', '', regex=False)
#                 .str.replace('\\', '', regex=False)
#                 .str.strip()
#             )
#     except Exception as e:
#         logger.error(f"Ошибка при предобработке данных: {e}")
#     return df

def preprocess_dataframe(df):
    from config import INPUT_COL_BRAND  # ← импортируем здесь

    if INPUT_COL_BRAND in df.columns:
        df[INPUT_COL_BRAND] = (
            df[INPUT_COL_BRAND]
            .astype(str)
            .str.replace('/', '', regex=False)
            .str.replace('\\', '', regex=False)
            .str.strip()
        )
    else:
        logger.warning(f"⚠️ Столбец '{INPUT_COL_BRAND}' не найден при предобработке")
    return df


def normalize_brand(brand_str):
    if not brand_str:
        return ""
    return re.sub(r'[^a-z0-9]', '', str(brand_str).lower())

def brand_matches(search_brand, result_brand):
    if not search_brand or not result_brand:
        return False
    norm_search = normalize_brand(search_brand)
    norm_result = normalize_brand(result_brand)
    return norm_search in norm_result or norm_result in norm_search

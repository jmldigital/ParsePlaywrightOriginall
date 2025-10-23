
# utils.py
import logging
import re
import pandas as pd
from pathlib import Path
import json
from config import INPUT_COL_BRAND  # ← импорт в начале
from decimal import Decimal, InvalidOperation
from config import (
    input_price)

# Файлы
LOG_FILE = 'logs/parser.log'
COUNTER_FILE = 'logs/run_counter.json'

# Глобальный логгер (инициализируется один раз)
_logger = None


def get_site_logger(site_name: str) -> logging.Logger:
    """Создает отдельный логгер для конкретного сайта"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"{site_name}.log"

    logger = logging.getLogger(site_name)
    if logger.handlers:
        return logger  # избегаем дублирования

    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_file, encoding="utf-8", mode='w')
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger


def get_run_count():
    """Возвращает номер текущего запуска (счётчик)"""
    path = Path(COUNTER_FILE)
    path.parent.mkdir(exist_ok=True)

    try:
        with open(path, 'r+', encoding='utf-8') as f:
            data = json.load(f)
            count = data.get("count", 0) + 1
            f.seek(0)
            json.dump({"count": count}, f, ensure_ascii=False, indent=2)
            f.truncate()
    except (FileNotFoundError, json.JSONDecodeError):
        count = 1
        path.write_text(json.dumps({"count": count}, ensure_ascii=False, indent=2), encoding='utf-8')
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

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    fh = logging.FileHandler(log_path, encoding='utf-8', mode='w')
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
    clean = re.sub(r'[^\d,.\s]', '', str(text).lower()).strip()
    clean = clean.replace("\u00a0", "").replace(" ", "")
    
    # Попытка парсинга с учетом десятичных знаков
    try:
        # Заменяем запятую на точку для корректного парсинга
        normalized = clean.replace(',', '.')
        return float(normalized)
    except (ValueError, AttributeError):
        return None




def preprocess_dataframe(df):
    """
    Предобработка DataFrame:
    - Конвертирует имена столбцов в строки (удаляет .0)
    - Очищает значения от пробелов
    - Преобразует столбец с ценой: если не float, приводит к float через безопасную обработку запятых
    """

    # Преобразование имён столбцов
    df.columns = df.columns.astype(str).str.replace('.0', '', regex=False).str.strip()

    # Обработка строковых столбцов — убираем лишние пробелы
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.strip()
    
    # Обработка столбца с ценой (например, input_price)
    if input_price in df.columns:
        # Только если не float — иначе не трогаем
        if df[input_price].dtype != 'float64':
            # Преобразуем ВСЕ значения к строкам, чтобы .str.replace не упал с ошибкой
            df[input_price] = df[input_price].astype(str).str.replace(',', '.', regex=False)
            # Конвертируем в float, нечисловые значения станут NaN
            df[input_price] = pd.to_numeric(df[input_price], errors='coerce')
            # Явно приводим к float dtype для совместимости
            df[input_price] = df[input_price].astype('float64')
    
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

    if norm_search == norm_result:
        return True
    if norm_search in norm_result:
        return True
    return False
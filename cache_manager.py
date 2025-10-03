
# cache_manager.py
import json
from pathlib import Path
from config import CACHE_FILE

def load_cache():
    if Path(CACHE_FILE).exists():
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_cache(cache):
    try:
        Path(CACHE_FILE).parent.mkdir(parents=True, exist_ok=True)
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2)
    except Exception as e:
        print(f"Ошибка сохранения кэша: {e}")

def get_cache_key(brand, part):
    return f"{brand.lower().strip()}:{part.lower().strip()}"
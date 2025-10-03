
# state_manager.py
import json
from pathlib import Path
from config import STATE_FILE

def load_state():
    """Возвращает: {'last_index': -1, 'processed_count': 0}"""
    if Path(STATE_FILE).exists():
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return {
                'last_index': data.get('last_index', -1),
                'processed_count': data.get('processed_count', 0)
            }
        except Exception as e:
            print(f"Ошибка чтения state.json: {e}")
    return {'last_index': -1, 'processed_count': 0}

def save_state(last_index, processed_count):
    """Сохраняет последний обработанный индекс"""
    try:
        Path(STATE_FILE).parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump({'last_index': last_index, 'processed_count': processed_count}, f, indent=2)
    except Exception as e:
        print(f"Ошибка сохранения state.json: {e}")
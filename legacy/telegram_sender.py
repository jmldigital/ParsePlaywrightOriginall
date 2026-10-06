
# telegram_sender.py
import logging
from pathlib import Path
from telegram import Bot
import asyncio
import os

# Переменные окружения должны быть загружены в основном скрипте (bot.py или main-parser.py)
BOT_TOKEN = os.getenv('BOT_TOKEN', '8364237483:AAERd9UAqQO_EAPt62AepFSojT41v9Vmw3s')
ADMIN_CHAT_ID = int(os.getenv('ADMIN_CHAT_ID', '-4688651319'))

logger = logging.getLogger(__name__)


async def send_telegram_file(file_path: str, caption: str = None):
    """
    Отправляет файл в Telegram через Bot API.
    
    Args:
        file_path (str): Путь к файлу
        caption (str): Подпись к файлу (опционально)

    Returns:
        bool: Успешно ли отправлено
    """
    try:
        bot = Bot(token=BOT_TOKEN)
        file_size = Path(file_path).stat().st_size / 1024  # в KB

        default_caption = (
            f"✅ Обработка завершена!\n\n"
            f"📄 Файл: {Path(file_path).name}\n"
            f"📊 Размер: {file_size:.2f} KB"
        )

        with open(file_path, 'rb') as f:
            await bot.send_document(
                chat_id=ADMIN_CHAT_ID,
                document=f,
                caption=caption or default_caption,
                filename=Path(file_path).name
            )
        logger.info(f"✅ Файл {file_path} отправлен в Telegram (чат {ADMIN_CHAT_ID})")
        return True
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке файла в Telegram: {e}")
        return False


def send_result_to_telegram(file_path: str, processed_count: int = 0, total_count: int = 0):
    """
    Синхронная обёртка для отправки результата.
    
    Используется в main-parser.py
    """
    caption = (
        f"✅ Обработка завершена!\n\n"
        f"📄 Файл: {Path(file_path).name}\n"
        f"📊 Обработано: {processed_count}/{total_count} позиций\n"
        f"📦 Размер: {Path(file_path).stat().st_size / 1024:.2f} KB"
    )
    try:
        asyncio.run(send_telegram_file(file_path, caption))
    except Exception as e:
        logger.error(f"❌ Ошибка при вызове Telegram: {e}")
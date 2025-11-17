
# bot.py
import logging
import os
from pathlib import Path
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv
import subprocess
import traceback
import asyncio
from telegram import Bot
import telegram
import sys
# Загружаем переменные окружения
load_dotenv()
parse_task = None
# Импортируем функцию отправки из нашего модуля
from telegram_sender import send_telegram_file

# Конфигурация
BOT_TOKEN = os.getenv('BOT_TOKEN', '8364237483:AAERd9UAqQO_EAPt62AepFSojT41v9Vmw3s')
ADMIN_CHAT_ID = int(os.getenv('ADMIN_CHAT_ID', '-4688651319'))
INPUT_DIR = Path('input')
INPUT_FILE = INPUT_DIR / 'наличие.xls'

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Создаём папку input, если её нет
INPUT_DIR.mkdir(exist_ok=True)




# from pathlib import Path

def set_env_variable(key: str, value: str):
    """Изменяет или добавляет переменную в .env файле"""
    env_path = Path('.env')
    
    if env_path.exists():
        with open(env_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        found = False
        for i, line in enumerate(lines):
            if line.strip().startswith(f"{key}="):
                lines[i] = f"{key}={value}\n"
                found = True
                break
        
        if not found:
            lines.append(f"{key}={value}\n")
        
        with open(env_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)
    else:
        with open(env_path, 'w', encoding='utf-8') as f:
            f.write(f"{key}={value}\n")
    
    logger.info(f"✅ Переменная {key} установлена в {value}")


async def mode_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключить на режим поиска цен"""
    try:
        set_env_variable('ENABLE_NAME_PARSING', 'False')
        await update.message.reply_text(
            "✅ Режим переключён: **Поиск цен и доставки**\n"
            "Изменения вступят в силу при следующем запуске парсера.",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Ошибка при переключении режима: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def mode_name_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключить на режим поиска имён"""
    try:
        set_env_variable('ENABLE_NAME_PARSING', 'True')
        await update.message.reply_text(
            "✅ Режим переключён: **Поиск названий деталей**\n"
            "Изменения вступят в силу при следующем запуске парсера.",
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Ошибка при переключении режима: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")



async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "👋 Привет! Я бот для обработки прайс-листов автозапчастей.\n\n"
        "📋 Доступные команды:\n"
        "/start - Показать это сообщение\n"
        "/parse - Отправить файл для обработки\n"
        "/mode_price - Режим: поиск цен и доставки\n"
        "/mode_name - Режим: поиск названий деталей\n\n"
        "📎 Для загрузки файла отправьте команду /parse и прикрепите .xls/.xlsx"
    )


async def nalichie_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /parse"""
    await update.message.reply_text(
        "📎 Отправьте файл наличие.xls или наличие.xlsx\n"
        "Файл будет сохранён и использован для следующей обработки."
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик загрузки документов с повторными попытками и увеличенным таймаутом"""
    try:
        document = update.message.document
        file_name = document.file_name.lower()

        if not (file_name.endswith('.xls') or file_name.endswith('.xlsx')):
            await update.message.reply_text("❌ Ошибка: принимаются только .xls или .xlsx")
            return

        await update.message.reply_text("⏳ Загружаю файл...")

        # === Попытка получить файл с увеличенным таймаутом ===
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                # Увеличиваем таймауты: connect, read, write, pool
                file = await context.bot.get_file(
                    document.file_id,
                    read_timeout=30,
                    write_timeout=30,
                    connect_timeout=30,
                    pool_timeout=30
                )
                logger.info(f"✅ Файл получен (попытка {attempt})")
                break
            except telegram.error.TimedOut as e:
                logger.warning(f"❌ Таймаут при попытке {attempt}/{max_retries}: {e}")
                if attempt == max_retries:
                    await update.message.reply_text("❌ Не удалось загрузить файл: таймаут соединения.")
                    return
                await asyncio.sleep(3 * attempt)  # экспоненциальная задержка
        # =============================================

        target_file = INPUT_DIR / 'наличие.xls'
        await file.download_to_drive(target_file)
        logger.info(f"✅ Файл сохранён: {target_file}")

        await update.message.reply_text(
            f"✅ Файл загружен!\n"
            f"📁 Сохранён как: {target_file.name}\n"
            f"🚀 Запускаю парсер..."
        )



        # # Запуск парсера
        # result = subprocess.run(
        #     [sys.executable, 'main.py'],
        #     capture_output=True,      # ← захватывает stdout и stderr
        #     text=True,                # ← делает их строками, а не bytes
        #     encoding='utf-8',         # ← поддержка кириллицы
        
        # )

        # Запуск парсера с безопасным чтением вывода
        try:
            result = subprocess.run(
                [sys.executable, 'main.py'],
                capture_output=True,
               
                check=False   # не выбрасывать исключение при returncode != 0
            )
        except subprocess.TimeoutExpired as e:
            logger.error(f"❌ Парсер превысил время ожидания: {e}")
            await update.message.reply_text("❌ Ошибка: парсер превысил время выполнения.")
            return

        # --- Безопасное декодирование stdout ---
        try:
            stdout = result.stdout.decode('utf-8')
        except UnicodeDecodeError:
            try:
                stdout = result.stdout.decode('cp1251')  # Windows-кодировка
            except:
                stdout = result.stdout.decode('latin1', errors='replace')  # fallback

        # --- Безопасное декодирование stderr ---
        try:
            stderr = result.stderr.decode('utf-8')
        except UnicodeDecodeError:
            try:
                stderr = result.stderr.decode('cp1251')
            except:
                stderr = result.stderr.decode('latin1', errors='replace')

        # Логируем в консоль
        print("----- STDOUT парсера -----")
        print(stdout)
        print("----- STDERR парсера -----")
        print(stderr)

        logger.info(f"✅ Парсер завершился (код: {result.returncode})")



        # stdout = result.stdout.decode('cp1251', errors='replace')  # Или ваша локальная кодировка
        # stderr = result.stderr.decode('cp1251', errors='replace')

        # print(stdout)
        # print(stderr)

        if result.returncode == 0:
            logger.info("✅ Парсер завершился успешно")
            output_file = 'output/наличие_with_competitors.xlsx'
            if Path(output_file).exists():
                await send_telegram_file(
                    file_path=output_file,
                    caption="✅ Результат обработки"
                )
            else:
                await update.message.reply_text("❌ Файл результата не найден!")
        else:
            logger.error(f"❌ Ошибка парсера: {stderr}")
            error_msg = stderr[-3000:]  # последние 3000 символов stderr
            text = error_msg
            await update.message.reply_text(
                text,
                parse_mode='MarkdownV2'
            )

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при обработке файла: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    text = update.message.text.lower().strip()
    if text == "parse":
        await nalichie_command(update, context)
    else:
        await update.message.reply_text("ℹ️ Используйте /start для списка команд")


def main():
    """Запуск бота"""
    logger.info("🤖 Запуск Telegram бота...")
    application = Application.builder().token(BOT_TOKEN).build()

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("parse", nalichie_command))
    application.add_handler(CommandHandler("mode_price", mode_price_command))
    application.add_handler(CommandHandler("mode_name", mode_name_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    logger.info("✅ Бот запущен и готов к работе")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

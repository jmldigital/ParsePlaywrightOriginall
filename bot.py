# bot.py
import logging
import signal  # 🆕 для graceful stop
import os
from pathlib import Path
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
from dotenv import load_dotenv
import subprocess
import traceback
import asyncio
from telegram import Bot
import telegram
import sys

from config import INPUT_FILE

# Загружаем переменные окружения
load_dotenv()
parse_task = None
# Импортируем функцию отправки из нашего модуля
from telegram_sender import send_telegram_file
from config import get_output_file

# Конфигурация
BOT_TOKEN = os.getenv("BOT_TOKEN", "8364237483:AAERd9UAqQO_EAPt62AepFSojT41v9Vmw3s")
ADMIN_CHAT_ID = int(os.getenv("ADMIN_CHAT_ID", "-4688651319"))
INPUT_DIR = Path("input")


# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Создаём папку input, если её нет
INPUT_DIR.mkdir(exist_ok=True)


# from pathlib import Path


def set_env_variable(key: str, value: str):
    """Изменяет или добавляет переменную в .env файле"""
    env_path = Path(".env")

    if env_path.exists():
        with open(env_path, "r", encoding="utf-8") as f:
            lines = f.readlines()

        found = False
        for i, line in enumerate(lines):
            if line.strip().startswith(f"{key}="):
                lines[i] = f"{key}={value}\n"
                found = True
                break

        if not found:
            lines.append(f"{key}={value}\n")

        with open(env_path, "w", encoding="utf-8") as f:
            f.writelines(lines)
    else:
        with open(env_path, "w", encoding="utf-8") as f:
            f.write(f"{key}={value}\n")

    logger.info(f"✅ Переменная {key} установлена в {value}")


async def mode_price_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключить на режим поиска цен"""
    try:
        set_env_variable("ENABLE_NAME_PARSING", "False")
        set_env_variable("ENABLE_WEIGHT_PARSING", "False")
        set_env_variable("ENABLE_PRICE_PARSING", "True")
        await update.message.reply_text(
            "✅ Режим переключён: **Поиск цен и доставки**\n"
            "Изменения вступят в силу при следующем запуске парсера.",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Ошибка при переключении режима: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def mode_name_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключить на режим поиска имён"""
    try:
        set_env_variable("ENABLE_NAME_PARSING", "True")
        set_env_variable("ENABLE_WEIGHT_PARSING", "False")
        set_env_variable("ENABLE_PRICE_PARSING", "False")
        await update.message.reply_text(
            "✅ Режим переключён: **Поиск названий деталей**\n"
            "Изменения вступят в силу при следующем запуске парсера.",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Ошибка при переключении режима: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def mode_weight_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключить на режим поиска весов"""
    try:
        set_env_variable("ENABLE_WEIGHT_PARSING", "True")
        set_env_variable("ENABLE_NAME_PARSING", "False")  # отключаем поиск имён
        set_env_variable("ENABLE_PRICE_PARSING", "False")
        await update.message.reply_text(
            "✅ Режим переключён: **Поиск весов деталей**\n"
            "Изменения вступят в силу при следующем запуске парсера.",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.error(f"Ошибка при переключении режима: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "👋 Привет! Я бот для обработки прайс-листов автозапчастей.\n\n"
        "📋 Доступные команды:\n"
        "/mode_price - Режим: поиск цен и доставки\n"
        "/mode_name - Режим: поиск названий деталей\n\n"
        "/mode_weight - Режим: поиск весов\n\n"
        "• `/stop` — 🛑 **Остановить парсер**\n\n"
        "📎 Для загрузки файла отправьте файл .xls/.xlsx"
    )


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Остановить текущий парсер"""
    global parse_task

    if parse_task:
        logger.info("🛑 👤 ПОЛЬЗОВАТЕЛЬСКИЙ STOP — проверяем процесс...")

        if hasattr(parse_task, "poll") and parse_task.poll() is None:
            logger.info(f"🛑 ПРОЦЕСС ЖИВОЙ (PID: {parse_task.pid}) — TERMINATE!")
            parse_task.terminate()

            # Ждём завершения (5 сек)
            try:
                logger.info("⏳ Ждём завершения процесса (5 сек)...")
                parse_task.wait(timeout=5)
                logger.info("✅ ПРОЦЕСС УСПЕШНО ОСТАНОВЛЕН (terminate)")
            except subprocess.TimeoutExpired:
                logger.warning("⚠️ ПРОЦЕСС НЕ ОТВЕЧАЕТ — KILL!")
                parse_task.kill()
                parse_task.wait(timeout=2)
                logger.info("💥 ПРОЦЕСС УБИТ (kill)")
        else:
            logger.info("ℹ️ ПРОЦЕСС УЖЕ ЗАВЕРШЁН")

        parse_task = None
        await update.message.reply_text("🛑 **Парсер остановлен!** ✅")

    else:
        logger.info("ℹ️ /stop — парсер не запущен")
        await update.message.reply_text("ℹ️ **Парсер не запущен**")


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик загрузки документов с повторными попытками и увеличенным таймаутом"""
    try:
        document = update.message.document
        file_name = document.file_name.lower()

        if not (file_name.endswith(".xls") or file_name.endswith(".xlsx")):
            await update.message.reply_text(
                "❌ Ошибка: принимаются только .xls или .xlsx"
            )
            return

        await update.message.reply_text("⏳ Загружаю файл...")

        # === Попытка получить файл с увеличенным таймаутом ===
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                file = await context.bot.get_file(
                    document.file_id,
                    read_timeout=30,
                    write_timeout=30,
                    connect_timeout=30,
                    pool_timeout=30,
                )
                logger.info(f"✅ Файл получен (попытка {attempt})")
                break
            except telegram.error.TimedOut as e:
                logger.warning(f"❌ Таймаут при попытке {attempt}/{max_retries}: {e}")
                if attempt == max_retries:
                    await update.message.reply_text(
                        "❌ Не удалось загрузить файл: таймаут соединения."
                    )
                    return
                await asyncio.sleep(3 * attempt)

        target_file = INPUT_FILE
        await file.download_to_drive(target_file)
        logger.info(f"✅ Файл сохранён: {target_file}")

        await update.message.reply_text("✅ Файл загружен!\n🚀 Запускаю парсер...")

        # 🆕 Запуск парсера как отменяемую задачу
        global parse_task

        parse_task = await asyncio.to_thread(
            lambda: subprocess.Popen(
                [sys.executable, "main.py"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                # universal_newlines=True  # ❌ УДАЛЕНО!
            )
        )

        logger.info(f"🚀 ПАРСЕР ЗАПУЩЕН (PID: {parse_task.pid})")

        try:
            stdout_bytes, stderr_bytes = await asyncio.to_thread(parse_task.communicate)
            result = subprocess.CompletedProcess(
                parse_task.args, parse_task.returncode, stdout_bytes, stderr_bytes
            )

            # 🆕 ЕДИНОЕ ДЕКОДИРОВАНИЕ БАЙТОВ
            try:
                stdout = stdout_bytes.decode("utf-8")
            except UnicodeDecodeError:
                try:
                    stdout = stdout_bytes.decode("cp1251")
                except:
                    stdout = stdout_bytes.decode("latin1", errors="replace")

            try:
                stderr = stderr_bytes.decode("utf-8")
            except UnicodeDecodeError:
                try:
                    stderr = stderr_bytes.decode("cp1251")
                except:
                    stderr = stderr_bytes.decode("latin1", errors="replace")

            logger.info(f"✅ ПАРСЕР ЗАВЕРШЁН (код: {result.returncode})")

        except Exception as e:
            logger.error(f"❌ Ошибка парсера: {e}")
            await update.message.reply_text("❌ Ошибка при выполнении парсера")
            return

        # Логируем в консоль
        print("----- STDOUT парсера -----")
        print(stdout)
        print("----- STDERR парсера -----")
        print(stderr)

        logger.info(f"✅ Парсер завершился (код: {result.returncode})")

        if result.returncode == 0:
            logger.info("✅ Парсер завершился успешно")

            # 🆕 БЕРЁМ ПОСЛЕДНИЙ изменённый файл из output/
            import glob
            import time

            output_files = glob.glob("output/*.xlsx")
            if output_files:
                latest_file = max(output_files, key=os.path.getmtime)
                logger.info(f"📤 Отправляем: {latest_file}")
                await send_telegram_file(
                    file_path=latest_file, caption="✅ Результат обработки"
                )
            else:
                await update.message.reply_text("❌ Файлы результата не найдены!")

    except Exception as e:
        logger.error(f"❌ Критическая ошибка при обработке файла: {e}")
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    # text = update.message.text.lower().strip()

    await update.message.reply_text("ℹ️ Используйте /start для списка команд")


def main():
    """Запуск бота"""
    logger.info("🤖 Запуск Telegram бота...")
    application = Application.builder().token(BOT_TOKEN).build()

    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("mode_weight", mode_weight_command))
    application.add_handler(CommandHandler("mode_price", mode_price_command))
    application.add_handler(CommandHandler("mode_name", mode_name_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)
    )

    logger.info("✅ Бот запущен и готов к работе")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

# bot.py
import logging
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
import glob  # ← ДОБАВЬ ЭТУ СТРОКУ
from config import get_output_file  # Импорт функции
from pathlib import Path
from dotenv import load_dotenv
import subprocess
import signal
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

parse_task = None


# Настройка логирования
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Создаём папку input, если её нет
INPUT_DIR.mkdir(exist_ok=True)


# from pathlib import Path


def get_parser_script() -> str:
    """Выбор скрипта с ОТЛАДОЧНЫМИ логами"""
    load_dotenv(override=True)  # ← ПЕРЕЗАГРУЗКА!

    enable_price = os.getenv("ENABLE_PRICE_PARSING", "False").lower() == "true"
    enable_weight = os.getenv("ENABLE_WEIGHT_PARSING", "False").lower() == "true"

    logger.info(f"🔍 .env: PRICE={enable_price} | WEIGHT={enable_weight}")

    if enable_weight:
        logger.info("📄 ✅ ВЫБРАН: main.py (ВЕСА)")
        return "main.py"
    elif enable_price:
        logger.info("📄 ✅ ВЫБРАН: main-yambo.py (ЯМБО)")
        return "main-yambo.py"
    else:
        logger.warning("⚠️ Нет режимов — main.py по умолчанию")
        return "main.py"


async def monitor_parser(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Фоновый мониторинг парсера"""
    global parse_task

    try:
        stdout_bytes, stderr_bytes = await asyncio.to_thread(parse_task.communicate)

        # 🆕 ПОЛНОЕ декодирование
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

        logger.info(f"✅ ПАРСЕР ЗАВЕРШЁН (код: {parse_task.returncode})")

        # Отправка результата
        output_files = glob.glob("output/*.xlsx")
        if output_files:
            latest_file = max(output_files, key=os.path.getmtime)
            await send_telegram_file(
                file_path=latest_file, caption="✅ Результат обработки"
            )
        else:
            await update.message.reply_text("❌ Файлы результата не найдены!")

        # Логи в консоль
        print("----- STDOUT -----")
        print(stdout)
        print("----- STDERR -----")
        print(stderr)

    except Exception as e:
        logger.error(f"❌ Ошибка мониторинга: {e}")

    finally:

        parse_task = None


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
    """Режим Yambo (цены)"""
    try:
        set_env_variable("ENABLE_PRICE_PARSING", "True")
        set_env_variable("ENABLE_WEIGHT_PARSING", "False")

        await update.message.reply_text(
            "✅ **Режим Yambo (поиск цен)**\n"
            "📄 Запустится: `main-yambo.py`\n\n"
            "Изменения вступят в силу при следующем запуске.",
            parse_mode="Markdown",
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def mode_weight_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Режим поиска весов"""
    try:
        set_env_variable("ENABLE_WEIGHT_PARSING", "True")
        set_env_variable("ENABLE_PRICE_PARSING", "False")

        await update.message.reply_text(
            "✅ **Режим весов**\n"
            "📄 Запустится: `main.py`\n\n"
            "Изменения вступят в силу при следующем запуске.",
            parse_mode="Markdown",
        )
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {str(e)}")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    await update.message.reply_text(
        "👋 Привет! Я бот для обработки прайс-листов автозапчастей.\n\n"
        "📋 Доступные команды:\n"
        "/mode_yambo - Режим: поиска цен для yambo\n\n"
        "/mode_weight - Режим: поиск весов\n\n"
        "• `/stop` — 🛑 **Остановить парсер**\n\n"
        "📎 Для загрузки файла отправьте файл .xls/.xlsx"
    )


import os
from pathlib import Path
import platform
import signal


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global parse_task

    if not parse_task or parse_task.poll() is not None:
        await update.message.reply_text("ℹ️ **Парсер не запущен**")
        return

    logger.info(f"🛑 Graceful stop PID: {parse_task.pid}")

    # 🆕 УНИВЕРСАЛЬНЫЙ STOP
    stop_flag = Path("input/STOP.flag")  # В папке input!
    stop_flag.touch()

    # SIGTERM только для Ubuntu
    if platform.system() != "Windows":
        parse_task.send_signal(signal.SIGTERM)
        logger.info("🛑 SIGTERM sent (Linux/Mac)")

    try:

        parse_task.wait(timeout=60)  # 1 минута достаточно
        logger.info("✅ Graceful stop completed")
        await update.message.reply_text(
            "🛑 **Graceful stop завершён! Финальный файл отправлен** ✅"
        )
        # 🆕 ОТПРАВЛЯЕМ batch_finalize.xlsx
        batch_file = Path("output/batch_finalize.xlsx")

        if batch_file.exists():
            try:
                with open(batch_file, "rb") as f:
                    await context.bot.send_document(
                        chat_id=update.effective_chat.id,
                        document=f,
                        caption="✅ **Финальный файл batch_finalize.xlsx**",
                        timeout=120,  # ✅ Работает!
                    )
                logger.info("📤 batch_finalize.xlsx отправлен!")
            except Exception as e:
                logger.error(f"❌ Ошибка отправки: {e}")
                await update.message.reply_text(
                    "💾 **Файл готов**: output/batch_finalize.xlsx"
                )

        else:
            await update.message.reply_text(
                "❌ **Файл не найден**: output/batch_finalize.xlsx"
            )

    except subprocess.TimeoutExpired:
        logger.warning("⚠️ Timeout — kill")
        parse_task.kill()
        parse_task.wait(timeout=10)
        await update.message.reply_text("💥 **Fallback kill** (timeout)")

    # 🆕 Очистка
    if stop_flag.exists():
        stop_flag.unlink()

    parse_task = None


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик загрузки документов"""

    try:
        document = update.message.document
        file_name = document.file_name.lower()

        if not (file_name.endswith(".xls") or file_name.endswith(".xlsx")):
            await update.message.reply_text(
                "❌ Ошибка: принимаются только .xls или .xlsx"
            )
            return

        await update.message.reply_text("⏳ Загружаю файл...")

        # Получение файла с повторными попытками
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

        # 🔥 НОВОЕ: выбор скрипта по режиму
        parser_script = get_parser_script()
        mode_name = "Yambo (цены)" if "main-yambo.py" in parser_script else "Вес"

        global parse_task
        parse_task = await asyncio.to_thread(
            lambda: subprocess.Popen(
                [sys.executable, parser_script],  # ← ДИНАМИЧЕСКИЙ скрипт!
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        )

        logger.info(f"🚀 ПАРСЕР ЗАПУЩЕН: {parser_script} (PID: {parse_task.pid})")

        # Фоновая задача мониторинга
        asyncio.create_task(monitor_parser(update, context))

        await update.message.reply_text(
            f"✅ **{mode_name}** парсер запущен в фоне!\n"
            f"📊 PID: `{parse_task.pid}`\n"
            f"📄 Скрипт: `{parser_script}`\n"
            "🛑 `/stop` для остановки",
            parse_mode="Markdown",
        )

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
    application.add_handler(CommandHandler("mode_yambo", mode_price_command))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    application.add_handler(CommandHandler("stop", stop_command))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text)
    )

    logger.info("✅ Бот запущен и готов к работе")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()

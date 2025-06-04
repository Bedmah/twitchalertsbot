import logging
import requests
import json
import asyncio
import os
from datetime import datetime
from os.path import exists
from telegram import ReplyKeyboardMarkup, KeyboardButton, Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters, ConversationHandler
)

# === Конфигурация ===
TELEGRAM_TOKEN = 'ТВОЙ ТОКЕН ТГ'
TWITCH_CLIENT_ID = 'TWITCH API'
TWITCH_CLIENT_SECRET = 'Твой Twitch Secret Code'
TWITCH_USER_LOGIN = 'Ник Twitch'
CHECK_INTERVAL = 5  # интервал проверки стрима (сек)
SUBSCRIBERS_FILE = 'subscribers.json'
USERS_DIR = 'users'
ADMINS_FILE = 'admins.json'  # Файл с ID админов

LINKS_MESSAGE = (
    "📎 <b>Мои ресурсы</b>:\n"
    "🌐 <b>Сайт</b> — https://bedmah.ru/\n"
    "🎮 <b>Twitch</b> — https://www.twitch.tv/fakebedmah\n"
    "📢 <b>Telegram канал</b> — https://t.me/bedmah\n"
    "🎭 <b>TikTok</b> — https://www.tiktok.com/@bedmah\n"
    "💬 <b>Telegram (личный)</b> — https://t.me/fakebedmah\n"
    "📘 <b>VK</b> — https://vk.com/egrvkid\n"
    "📺 <b>YouTube</b> — https://www.youtube.com/@dontbedmah\n"
    "💻 <b>Скрипты</b> — https://tech.bedmah.ru/"
)

os.makedirs(USERS_DIR, exist_ok=True)

# Загрузка админов из файла
def load_admins():
    if exists(ADMINS_FILE):
        with open(ADMINS_FILE, 'r') as f:
            return set(json.load(f))
    return set()

admins = load_admins()

# Загрузка и сохранение подписчиков
def load_subscribers():
    if exists(SUBSCRIBERS_FILE):
        with open(SUBSCRIBERS_FILE, 'r') as f:
            return set(json.load(f))
    return set()

def save_subscribers(subs):
    with open(SUBSCRIBERS_FILE, 'w') as f:
        json.dump(list(subs), f)

subscribers = load_subscribers()

# Логирование пользователей
def format_user_info(user):
    return f"{user.full_name} | @{user.username} | ID: {user.id}"

def log_user_action(user, message):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user_log = os.path.join(USERS_DIR, f"{user.id}.txt")
    with open(user_log, "a", encoding="utf-8") as f:
        f.write(f"[{now}] {format_user_info(user)} | {message}\n")

# Клавиатура с динамическими кнопками и кнопкой "Admin" для админов
def get_keyboard(is_subscribed=False, is_admin=False):
    buttons = []
    if is_subscribed:
        buttons.append([KeyboardButton("❌ Отписаться")])
    else:
        buttons.append([KeyboardButton("✅ Подписаться")])
    buttons.append([KeyboardButton("📎 Все ссылки")])
    if is_admin:
        buttons.append([KeyboardButton("🔐 Admin")])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

# Twitch API
def get_twitch_token():
    url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': TWITCH_CLIENT_ID,
        'client_secret': TWITCH_CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    response = requests.post(url, params=params).json()
    return response['access_token']

def is_stream_live(token):
    headers = {
        'Client-ID': TWITCH_CLIENT_ID,
        'Authorization': f'Bearer {token}'
    }
    url = f'https://api.twitch.tv/helix/streams?user_login={TWITCH_USER_LOGIN}'
    response = requests.get(url, headers=headers).json()
    return len(response.get('data', [])) > 0

# Основные хендлеры

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    log_user_action(user, "🟢 /start")
    subscribed = user.id in subscribers
    is_admin = user.id in admins
    status = "✅ Вы уже подписаны." if subscribed else "❌ Вы пока не подписаны."
    await update.message.reply_text(
        f"👋 Привет, {user.first_name}!\n"
        f"Я уведомлю тебя, когда начнётся стрим на Twitch 🎥\n\n"
        f"{status}\n\n⬇ Выберите действие ниже:",
        reply_markup=get_keyboard(subscribed, is_admin)
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    log_user_action(user, "🆘 /help")
    base_help = (
        "/start — перезапуск\n"
        "/help — справка\n"
        "/status — ваш статус\n\n"
        "Или используйте кнопки ⬇"
    )
    admin_help = (
        "\n\n<b>Admin команды:</b>\n"
        "/admin_list — список подписчиков\n"
        "/admin_logs <user_id> — лог пользователя\n"
        "/admin_notify — уведомить всех\n"
    )
    msg = base_help + (admin_help if user.id in admins else "")
    await update.message.reply_html(msg)

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    log_user_action(user, "ℹ /status")
    if user.id in subscribers:
        await update.message.reply_text(f"✅ Вы подписаны.\n👥 Всего подписчиков: {len(subscribers)}")
    else:
        await update.message.reply_text("❌ Вы не подписаны. Нажмите '✅ Подписаться' ниже.")

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    text = update.message.text.strip()
    subscribed = user.id in subscribers
    is_admin = user.id in admins

    if text == "✅ Подписаться":
        if subscribed:
            await update.message.reply_text("📌 Вы уже подписаны.", reply_markup=get_keyboard(True, is_admin))
            log_user_action(user, "Повторная подписка")
        else:
            subscribers.add(user.id)
            save_subscribers(subscribers)
            await update.message.reply_text("🎉 Подписка оформлена!", reply_markup=get_keyboard(True, is_admin))
            log_user_action(user, "✅ Подписался")

    elif text == "❌ Отписаться":
        if not subscribed:
            await update.message.reply_text("❌ Вы не подписаны.", reply_markup=get_keyboard(False, is_admin))
            log_user_action(user, "Отписка без подписки")
        else:
            subscribers.discard(user.id)
            save_subscribers(subscribers)
            await update.message.reply_text("😢 Вы отписались от уведомлений.", reply_markup=get_keyboard(False, is_admin))
            log_user_action(user, "❌ Отписался")

    elif text == "📎 Все ссылки":
        await update.message.reply_html(LINKS_MESSAGE, disable_web_page_preview=True, reply_markup=get_keyboard(subscribed, is_admin))
        log_user_action(user, "📎 Запросил ссылки")

    elif text == "🔐 Admin" and is_admin:
        await show_admin_menu(update, context)
        log_user_action(user, "Открыл меню админа")

    else:
        await update.message.reply_text("⛔ Неизвестная команда. Пожалуйста, используйте кнопки ниже.", reply_markup=get_keyboard(subscribed, is_admin))
        log_user_action(user, f"⚠ Непонятный ввод: {text}")

async def show_admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = ReplyKeyboardMarkup([
        [KeyboardButton("/admin_list")],
        [KeyboardButton("/admin_logs")],
        [KeyboardButton("/admin_notify")],
        [KeyboardButton("⬅ Назад")]
    ], resize_keyboard=True)
    await update.message.reply_text("🔐 Админ меню:\n/admin_list — список подписчиков\n/admin_logs <user_id> — лог пользователя\n/admin_notify — уведомить всех\n\nНажмите команду или '⬅ Назад' чтобы выйти.", reply_markup=keyboard)

# Админские команды

async def admin_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    if user.id not in admins:
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    log_user_action(user, "Просмотр списка подписчиков")
    if not subscribers:
        await update.message.reply_text("📭 Список подписчиков пуст.")
        return
    text = f"👥 Всего подписчиков: {len(subscribers)}\n\n"
    # Сформируем список с инфой, постараемся прочитать имена из файлов логов, если есть
    for sub_id in subscribers:
        user_log = os.path.join(USERS_DIR, f"{sub_id}.txt")
        if exists(user_log):
            with open(user_log, "r", encoding="utf-8") as f:
                first_line = f.readline()
                name_info = first_line.split('|')[1].strip() if '|' in first_line else "Unknown"
        else:
            name_info = "Unknown"
        text += f"ID: {sub_id} | {name_info}\n"
    await update.message.reply_text(text)

async def admin_logs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    if user.id not in admins:
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    args = context.args
    if not args:
        await update.message.reply_text("ℹ Используйте: /admin_logs <user_id>")
        return
    target_id = args[0]
    log_user_action(user, f"Просмотр лога пользователя {target_id}")
    user_log = os.path.join(USERS_DIR, f"{target_id}.txt")
    if not exists(user_log):
        await update.message.reply_text("❌ Лог пользователя не найден.")
        return
    with open(user_log, "r", encoding="utf-8") as f:
        content = f.read()
    # Если слишком много — отправим файл, иначе текстом
    if len(content) > 4000:
        await update.message.reply_document(document=content.encode('utf-8'), filename=f"user_{target_id}_log.txt")
    else:
        await update.message.reply_text(f"<pre>{content}</pre>", parse_mode='HTML')

async def admin_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    if user.id not in admins:
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    log_user_action(user, "Запуск рассылки уведомлений (админ)")
    await update.message.reply_text("🚀 Начинаю рассылку уведомлений подписчикам...")
    await notify_all(context)

async def notify_all(context: ContextTypes.DEFAULT_TYPE):
    for user_id in subscribers:
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=f"🔴 Стрим начался!\n➡ https://www.twitch.tv/{TWITCH_USER_LOGIN}"
            )
            dummy_user = type("User", (), {"id": user_id, "full_name": "Unknown", "username": "unknown"})
            log_user_action(dummy_user, "📢 Получил уведомление о стриме")
        except Exception as e:
            dummy_user = type("User", (), {"id": user_id, "full_name": "Unknown", "username": "unknown"})
            log_user_action(dummy_user, f"❗ Ошибка при отправке уведомления: {e}")

# Мониторинг стрима
async def stream_monitor(application):
    token = get_twitch_token()
    stream_was_live = False
    while True:
        try:
            live = is_stream_live(token)
            if live and not stream_was_live:
                logging.info("Стрим запустился — уведомляем подписчиков.")
                await notify_all(application)
            stream_was_live = live
        except Exception as e:
            logging.error(f"[stream_monitor] Ошибка: {e}")
        await asyncio.sleep(CHECK_INTERVAL)

# Обработка команды назад из админ меню
async def back_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    subscribed = user.id in subscribers
    is_admin = user.id in admins
    log_user_action(user, "Вернулся в главное меню")
    await update.message.reply_text("⬇ Главное меню:", reply_markup=get_keyboard(subscribed, is_admin))

# Основной запуск
if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s'
    )
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("admin_list", admin_list))
    app.add_handler(CommandHandler("admin_logs", admin_logs))
    app.add_handler(CommandHandler("admin_notify", admin_notify))
    app.add_handler(MessageHandler(filters.Regex('^⬅ Назад$'), back_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    loop = asyncio.get_event_loop()
    loop.create_task(stream_monitor(app))
    app.run_polling()

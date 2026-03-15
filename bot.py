import asyncio
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from telegram import KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.error import NetworkError, RetryAfter, TimedOut
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from app.db import Database, prepare_databases
from app.twitch_api import TwitchClient, parse_twitch_login


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", encoding="utf-8-sig")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET", "")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "20"))
NOTIFY_COOLDOWN_MINUTES = int(os.getenv("NOTIFY_COOLDOWN_MINUTES", "30"))

FEEDBACK_LINK = os.getenv("FEEDBACK_LINK", "https://t.me/fakebedmah")
SITE_LINK = os.getenv("SITE_LINK", "https://bedmah.ru/")
DB_ROOT = BASE_DIR / os.getenv("DB_ROOT", "bd")

RECOMMENDED_STREAMERS = [
    s.strip().lower()
    for s in os.getenv("RECOMMENDED_STREAMERS", "bedmah,AlexNiceNice,Muriatin,angelnymp").split(",")
    if s.strip()
]
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "6968569406").split(",") if x.strip().isdigit()]

active_db: Database | None = None
twitch_client: TwitchClient | None = None

BACK = "⬅ Назад"
LIMIT_REACHED_TEXT = "Достнигнут технический лимит, при необходимости расширения, напиши @fakebedmah"


@dataclass
class BroadcastState:
    step: str  # target|content
    targets: set[int]


async def tg_call_with_retry(factory, *, attempts: int = 2, base_delay: float = 0.5):
    last_error = None
    for attempt in range(1, attempts + 1):
        try:
            return await factory()
        except RetryAfter as exc:
            last_error = exc
            delay = float(getattr(exc, "retry_after", 1.0)) + 0.5
            if attempt >= attempts:
                raise
            await asyncio.sleep(delay)
        except (TimedOut, NetworkError, aiohttp.ClientError) as exc:
            last_error = exc
            if attempt >= attempts:
                raise
            await asyncio.sleep(base_delay * attempt)
    if last_error:
        raise last_error


async def safe_reply_text(message, text: str, **kwargs) -> bool:
    try:
        await tg_call_with_retry(lambda: message.reply_text(text, **kwargs), attempts=2, base_delay=0.3)
        return True
    except Exception:
        logging.exception("reply_text failed")
        return False


def get_main_keyboard(is_admin: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton("🔔 Подписаться"), KeyboardButton("⭐ Рекомендации")],
        [KeyboardButton("📋 Мои подписки"), KeyboardButton("📎 Ссылки")],
        [KeyboardButton("💬 Обратная связь")],
    ]
    if is_admin:
        rows.append([KeyboardButton("🔐 Admin")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def get_admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("👥 Пользователи")],
            [KeyboardButton("⭐ Рекомендации (Admin)")],
            [KeyboardButton("⚙️ Лимиты")],
            [KeyboardButton("🛠 Контент")],
            [KeyboardButton("📂 Лог пользователя")],
            [KeyboardButton("📣 Рассылка")],
            [KeyboardButton(BACK)],
        ],
        resize_keyboard=True,
    )


def get_back_only_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BACK)],
        ],
        resize_keyboard=True,
    )


async def get_recommendations_keyboard() -> ReplyKeyboardMarkup:
    assert active_db
    buttons = []
    for login in await active_db.list_recommended():
        buttons.append([KeyboardButton(f"⭐ {login}")])
    buttons.append([KeyboardButton(BACK)])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


def get_unsubscribe_keyboard(subscriptions: list[str]) -> ReplyKeyboardMarkup:
    buttons = [[KeyboardButton(f"❌ {login}")] for login in subscriptions]
    buttons.append([KeyboardButton(BACK)])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)


def get_admin_recommendations_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("➕ Добавить в рекомендации")],
            [KeyboardButton("➖ Удалить из рекомендаций")],
            [KeyboardButton("📄 Список рекомендаций")],
            [KeyboardButton(BACK)],
        ],
        resize_keyboard=True,
    )


def get_users_admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("🚫 Ограничить доступ")],
            [KeyboardButton("✅ Разрешить доступ")],
            [KeyboardButton("🔄 Обновить пользователей")],
            [KeyboardButton(BACK)],
        ],
        resize_keyboard=True,
    )


def get_limits_admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("📌 Лимит по умолчанию")],
            [KeyboardButton("👤 Лимит пользователя")],
            [KeyboardButton("ℹ️ Показать лимиты")],
            [KeyboardButton(BACK)],
        ],
        resize_keyboard=True,
    )


def get_content_admin_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("🔗 Настроить ссылки")],
            [KeyboardButton("💬 Настроить обратную связь")],
            [KeyboardButton("👁 Показать контент")],
            [KeyboardButton(BACK)],
        ],
        resize_keyboard=True,
    )


def clear_states(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("await_subscribe_input", None)
    context.user_data.pop("await_unsubscribe_select", None)
    context.user_data.pop("await_user_log", None)
    context.user_data.pop("await_recommend_add", None)
    context.user_data.pop("await_recommend_remove", None)
    context.user_data.pop("await_access_block", None)
    context.user_data.pop("await_access_allow", None)
    context.user_data.pop("await_limit_default", None)
    context.user_data.pop("await_limit_user_target", None)
    context.user_data.pop("await_limit_user_value", None)
    context.user_data.pop("limit_target_user_id", None)
    context.user_data.pop("await_links_message", None)
    context.user_data.pop("await_feedback_message", None)
    context.user_data.pop("back_target", None)
    context.user_data.pop("broadcast", None)


def set_back_target(context: ContextTypes.DEFAULT_TYPE, target: str) -> None:
    context.user_data["back_target"] = target


def get_broadcast_state(context: ContextTypes.DEFAULT_TYPE) -> BroadcastState | None:
    raw = context.user_data.get("broadcast")
    if not raw:
        return None
    return BroadcastState(step=raw["step"], targets=set(raw["targets"]))


def set_broadcast_state(context: ContextTypes.DEFAULT_TYPE, state: BroadcastState | None) -> None:
    if not state:
        context.user_data.pop("broadcast", None)
        return
    context.user_data["broadcast"] = {
        "step": state.step,
        "targets": sorted(state.targets),
    }


async def ensure_user(update: Update) -> None:
    assert active_db
    user = update.effective_user
    if not user:
        return
    await active_db.upsert_user(user.id, user.username.lower() if user.username else None, user.full_name)


async def get_links_text() -> str:
    assert active_db
    custom = await active_db.get_setting("links_message")
    if custom:
        return custom
    lines = ["📎 <b>Все ссылки</b>", f"🌐 Сайт - {SITE_LINK}"]
    for login in await active_db.list_recommended():
        lines.append(f"🎮 {login} - https://www.twitch.tv/{login}")
    return "\n".join(lines)


async def get_feedback_text() -> str:
    assert active_db
    return await active_db.get_setting(
        "feedback_message",
        f"Если есть вопросы и предложения: {FEEDBACK_LINK}",
    )


async def send_long_text(message, text: str, reply_markup=None, html: bool = False, disable_preview: bool = False) -> None:
    chunk_size = 3500
    chunks = [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)] or [text]
    for idx, chunk in enumerate(chunks):
        kwargs = {}
        if idx == len(chunks) - 1 and reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
        if html:
            await tg_call_with_retry(
                lambda: message.reply_html(chunk, disable_web_page_preview=disable_preview, **kwargs),
                attempts=2,
                base_delay=0.3,
            )
        else:
            await tg_call_with_retry(
                lambda: message.reply_text(chunk, disable_web_page_preview=disable_preview, **kwargs),
                attempts=2,
                base_delay=0.3,
            )


async def build_broadcast_result_text(sent_ok: int, failed: list[tuple[int, str]]) -> str:
    assert active_db
    lines = [f"Готово. Успешно отправлено: {sent_ok}"]
    if not failed:
        lines.append("Ошибок доставки: 0")
        return "\n".join(lines)

    lines.append(f"Ошибок доставки: {len(failed)}")
    lines.append("Не доставлено:")
    for uid, reason in failed:
        username = await active_db.get_user_username(uid)
        uname = f" (@{username})" if username else ""
        lines.append(f"- ID {uid}{uname}: {reason}")
    return "\n".join(lines)


async def is_admin(user_id: int) -> bool:
    assert active_db
    return await active_db.is_admin(user_id)


async def refresh_users_from_telegram(app: Application) -> None:
    assert active_db
    for uid, _, _ in await active_db.all_users():
        try:
            chat = await app.bot.get_chat(uid)
            username = chat.username.lower() if getattr(chat, "username", None) else None
            full_name = " ".join(x for x in [getattr(chat, "first_name", None), getattr(chat, "last_name", None)] if x).strip()
            if not full_name:
                full_name = getattr(chat, "full_name", None)
            await active_db.upsert_user(uid, username, full_name)
        except Exception:
            # Not all users are reachable by get_chat; keep previous profile.
            continue


async def parse_targets(raw: str) -> tuple[set[int], str | None]:
    assert active_db
    text = raw.strip()
    if not text:
        return set(), "Пустой список получателей."

    if text.lower() == "all":
        users = await active_db.all_users()
        return {uid for uid, _, _ in users}, None

    result = set()
    for token in [x.strip() for x in text.split(",") if x.strip()]:
        if token.isdigit():
            result.add(int(token))
            continue

        if token.startswith("@"):
            resolved = await active_db.resolve_user(token)
            if resolved is None:
                return set(), f"Пользователь {token} не найден."
            result.add(resolved)
            continue

        return set(), f"Некорректный получатель: {token}"

    return result, None


async def send_to_targets(
    app: Application,
    targets: set[int],
    *,
    kind: str,
    text: str | None = None,
    photo_id: str | None = None,
    document_id: str | None = None,
    caption: str | None = None,
) -> tuple[int, list[tuple[int, str]]]:
    ok = 0
    failed: list[tuple[int, str]] = []
    for uid in sorted(targets):
        try:
            if kind == "text":
                await tg_call_with_retry(lambda: app.bot.send_message(uid, text or ""), attempts=2, base_delay=0.5)
            elif kind == "photo":
                await tg_call_with_retry(
                    lambda: app.bot.send_photo(uid, photo=photo_id, caption=caption),
                    attempts=2,
                    base_delay=0.5,
                )
            elif kind == "document":
                await tg_call_with_retry(
                    lambda: app.bot.send_document(uid, document=document_id, caption=caption),
                    attempts=2,
                    base_delay=0.5,
                )
            ok += 1
        except Exception as exc:
            reason = str(exc).strip() or exc.__class__.__name__
            failed.append((uid, reason))
            logging.exception("broadcast failed uid=%s", uid)
    return ok, failed


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    assert active_db
    user = update.effective_user
    message = update.message
    if not user or not message:
        return

    await ensure_user(update)
    clear_states(context)
    admin = await is_admin(user.id)
    allowed = await active_db.is_user_allowed(user.id)

    if not admin and not allowed:
        await active_db.log_action(user.id, "blocked_start")
        await message.reply_text("⛔ Доступ к боту ограничен. Обратитесь в поддержку.")
        return

    text = (
        "Бот уведомлений Twitch.\n\n"
        "- Подписка на любого стримера по логину или ссылке\n"
        "- Рекомендации для быстрой подписки\n"
        "- Уведомления о старте стрима с антидублем"
    )
    await active_db.log_action(user.id, "start")
    await message.reply_text(text, reply_markup=get_main_keyboard(admin))


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.message
    user = update.effective_user
    if not message or not user:
        return
    await ensure_user(update)
    if not await is_admin(user.id):
        allowed = await active_db.is_user_allowed(user.id)
        if not allowed:
            await active_db.log_action(user.id, "blocked_help")
            await message.reply_text("⛔ Доступ к боту ограничен.")
            return
    await active_db.log_action(user.id, "help")
    await message.reply_text(
        "/start - главное меню\n/help - помощь\n/admin - админ-панель",
        reply_markup=get_main_keyboard(await is_admin(user.id)),
    )


async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    message = update.message
    if not user or not message:
        return
    await ensure_user(update)
    if not await is_admin(user.id):
        await safe_reply_text(message, "⛔ Доступ запрещен.")
        return
    clear_states(context)
    await active_db.log_action(user.id, "admin_open")
    await safe_reply_text(message, "Админ-панель:", reply_markup=get_admin_keyboard())


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    assert active_db and twitch_client
    user = update.effective_user
    message = update.message
    if not user or not message or not message.text:
        return

    await ensure_user(update)
    text = message.text.strip()
    admin = await is_admin(user.id)
    allowed = await active_db.is_user_allowed(user.id)

    if not admin and not allowed:
        await active_db.log_action(user.id, "blocked_text", text)
        await message.reply_text("⛔ Доступ к боту ограничен.")
        return

    if text in {"/start", "/help", "/admin"}:
        return

    if text == BACK:
        back_target = context.user_data.get("back_target", "main")
        clear_states(context)
        if admin and back_target == "admin":
            await message.reply_text("Админ-панель:", reply_markup=get_admin_keyboard())
            return
        if admin and back_target == "admin_users":
            await message.reply_text("Раздел Пользователи.", reply_markup=get_users_admin_keyboard())
            return
        if admin and back_target == "admin_recommend":
            await message.reply_text("Управление рекомендациями:", reply_markup=get_admin_recommendations_keyboard())
            return
        if admin and back_target == "admin_limits":
            await message.reply_text("Управление лимитами подписок:", reply_markup=get_limits_admin_keyboard())
            return
        if admin and back_target == "admin_content":
            await message.reply_text("Управление контентом:", reply_markup=get_content_admin_keyboard())
            return
        await message.reply_text("Главное меню.", reply_markup=get_main_keyboard(admin))
        return

    if text in {"🔐 Admin", "/admin"} and admin:
        await admin_command(update, context)
        return

    # Top-level admin navigation should work even if there are pending states.
    if admin and text == "👥 Пользователи":
        clear_states(context)
        set_back_target(context, "admin")
        await refresh_users_from_telegram(context.application)
        report = await active_db.get_subscriptions_report()
        await active_db.log_action(user.id, "admin_users")
        await send_long_text(message, report, reply_markup=get_users_admin_keyboard())
        return

    if admin and text == "⭐ Рекомендации (Admin)":
        clear_states(context)
        set_back_target(context, "admin")
        await active_db.log_action(user.id, "admin_recommend_menu")
        await message.reply_text("Управление рекомендациями:", reply_markup=get_admin_recommendations_keyboard())
        return

    if admin and text == "⚙️ Лимиты":
        clear_states(context)
        set_back_target(context, "admin")
        await active_db.log_action(user.id, "admin_limits_menu")
        await message.reply_text("Управление лимитами подписок:", reply_markup=get_limits_admin_keyboard())
        return

    if admin and text == "🛠 Контент":
        clear_states(context)
        set_back_target(context, "admin")
        await active_db.log_action(user.id, "admin_content_menu")
        await message.reply_text("Управление контентом:", reply_markup=get_content_admin_keyboard())
        return

    if admin and text == "📂 Лог пользователя":
        clear_states(context)
        set_back_target(context, "admin")
        context.user_data["await_user_log"] = True
        await active_db.log_action(user.id, "admin_log_request")
        await message.reply_text("Введите ID или @username пользователя:", reply_markup=get_back_only_keyboard())
        return

    if admin and text == "📣 Рассылка":
        clear_states(context)
        set_back_target(context, "admin")
        set_broadcast_state(context, BroadcastState(step="target", targets=set()))
        await active_db.log_action(user.id, "admin_broadcast_menu")
        await message.reply_text(
            "Кому отправлять? all, ID или @username, можно список через запятую.",
            reply_markup=get_back_only_keyboard(),
        )
        return

    # Admin state machine
    state = get_broadcast_state(context)
    if admin and state:
        if state.step == "target":
            targets, err = await parse_targets(text)
            if err:
                await message.reply_text(err)
                return
            if not targets:
                await message.reply_text("Нет получателей.")
                return
            state.targets = targets
            state.step = "content"
            set_broadcast_state(context, state)
            await message.reply_text(
                "Отправьте текст, фото или файл для рассылки.",
                reply_markup=get_back_only_keyboard(),
            )
            return

        if state.step == "content":
            ok, failed = await send_to_targets(context.application, state.targets, kind="text", text=text)
            await active_db.log_action(user.id, "broadcast_text", f"ok={ok}, fail={len(failed)}")
            clear_states(context)
            await send_long_text(
                message,
                await build_broadcast_result_text(ok, failed),
                reply_markup=get_admin_keyboard(),
            )
            return

    if admin and context.user_data.get("await_user_log"):
        target = await active_db.resolve_user(text)
        context.user_data.pop("await_user_log", None)
        if target is None:
            await message.reply_text("Пользователь не найден.", reply_markup=get_admin_keyboard())
            return

        data = await active_db.get_user_log_text(target)
        if len(data) > 3800:
            await message.reply_document(document=data.encode("utf-8"), filename=f"user_{target}_log.txt")
        else:
            safe = data.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            await message.reply_text(f"<pre>{safe}</pre>", parse_mode=ParseMode.HTML)
        await active_db.log_action(user.id, "admin_user_log", str(target))
        return

    if admin and context.user_data.get("await_recommend_add"):
        login = parse_twitch_login(text)
        if not login:
            await message.reply_text("Нужен логин или ссылка Twitch.")
            return
        context.user_data.pop("await_recommend_add", None)
        await active_db.add_recommended(login)
        await active_db.log_action(user.id, "admin_recommend_add", login)
        await message.reply_text(f"✅ Добавил {login} в рекомендации.", reply_markup=get_admin_recommendations_keyboard())
        return

    if admin and context.user_data.get("await_recommend_remove"):
        login = text.replace("➖", "", 1).strip().lower()
        if not login:
            await message.reply_text("Выберите стримера для удаления.")
            return
        context.user_data.pop("await_recommend_remove", None)
        removed = await active_db.remove_recommended(login)
        await active_db.log_action(user.id, "admin_recommend_remove", login)
        msg = f"✅ Удалил {login} из рекомендаций." if removed else f"{login} не найден в рекомендациях."
        await message.reply_text(msg, reply_markup=get_admin_recommendations_keyboard())
        return

    if admin and context.user_data.get("await_access_block"):
        target = await active_db.resolve_user(text)
        context.user_data.pop("await_access_block", None)
        if target is None:
            await message.reply_text("Пользователь не найден.", reply_markup=get_users_admin_keyboard())
            return
        await active_db.set_user_access(target, False, note=f"Blocked by {user.id}")
        await active_db.log_action(user.id, "admin_block_user", str(target))
        await message.reply_text(f"🚫 Пользователь {target} ограничен.", reply_markup=get_users_admin_keyboard())
        return

    if admin and context.user_data.get("await_access_allow"):
        target = await active_db.resolve_user(text)
        context.user_data.pop("await_access_allow", None)
        if target is None:
            await message.reply_text("Пользователь не найден.", reply_markup=get_users_admin_keyboard())
            return
        await active_db.set_user_access(target, True, note=f"Allowed by {user.id}")
        await active_db.log_action(user.id, "admin_allow_user", str(target))
        await message.reply_text(f"✅ Доступ пользователю {target} разрешен.", reply_markup=get_users_admin_keyboard())
        return

    if admin and context.user_data.get("await_limit_default"):
        if not text.isdigit() or int(text) < 1:
            await message.reply_text("Введите целое число больше 0.")
            return
        value = int(text)
        context.user_data.pop("await_limit_default", None)
        await active_db.set_default_sub_limit(value)
        await active_db.log_action(user.id, "admin_set_default_limit", str(value))
        await message.reply_text(f"✅ Лимит по умолчанию обновлён: {value}", reply_markup=get_limits_admin_keyboard())
        return

    if admin and context.user_data.get("await_limit_user_target"):
        target = await active_db.resolve_user(text)
        if target is None:
            await message.reply_text("Пользователь не найден.")
            return
        context.user_data["limit_target_user_id"] = target
        context.user_data.pop("await_limit_user_target", None)
        context.user_data["await_limit_user_value"] = True
        await message.reply_text(f"Введите новый лимит для пользователя {target} (целое > 0):")
        return

    if admin and context.user_data.get("await_limit_user_value"):
        if not text.isdigit() or int(text) < 1:
            await message.reply_text("Введите целое число больше 0.")
            return
        target = context.user_data.get("limit_target_user_id")
        if not target:
            clear_states(context)
            await message.reply_text("Не удалось определить пользователя.", reply_markup=get_limits_admin_keyboard())
            return
        value = int(text)
        context.user_data.pop("await_limit_user_value", None)
        context.user_data.pop("limit_target_user_id", None)
        await active_db.set_user_sub_limit(int(target), value)
        await active_db.log_action(user.id, "admin_set_user_limit", f"{target}:{value}")
        await message.reply_text(
            f"✅ Лимит пользователя {target} обновлён: {value}",
            reply_markup=get_limits_admin_keyboard(),
        )
        return

    if admin and context.user_data.get("await_links_message"):
        context.user_data.pop("await_links_message", None)
        await active_db.set_setting("links_message", text)
        await active_db.log_action(user.id, "admin_set_links_message")
        await message.reply_text("✅ Раздел Ссылки обновлён.", reply_markup=get_content_admin_keyboard())
        return

    if admin and context.user_data.get("await_feedback_message"):
        context.user_data.pop("await_feedback_message", None)
        await active_db.set_setting("feedback_message", text)
        await active_db.log_action(user.id, "admin_set_feedback_message")
        await message.reply_text("✅ Раздел Обратная связь обновлён.", reply_markup=get_content_admin_keyboard())
        return

    if context.user_data.get("await_unsubscribe_select"):
        if text.startswith("❌ "):
            login = text[2:].strip().lower()
            changed = await active_db.unsubscribe(user.id, login)
            await active_db.log_action(user.id, "unsubscribe_button", login)
            if changed:
                await message.reply_text(f"✅ Отписка от {login} выполнена.")
            else:
                await message.reply_text(f"Вы не были подписаны на {login}.")
            subs = await active_db.list_user_subscriptions(user.id)
            if subs:
                await message.reply_text(
                    "Выберите, от кого еще отписаться:",
                    reply_markup=get_unsubscribe_keyboard(subs),
                )
            else:
                context.user_data.pop("await_unsubscribe_select", None)
                await message.reply_text("Подписок больше нет.", reply_markup=get_main_keyboard(admin))
            return
        await message.reply_text("Выберите стримера кнопкой или нажмите Назад.")
        return

    if context.user_data.get("await_subscribe_input"):
        login = parse_twitch_login(text)
        if not login:
            await message.reply_text("Неверный формат. Отправьте логин или ссылку Twitch.")
            return

        session: aiohttp.ClientSession = context.application.bot_data["http_session"]
        try:
            t_user = await twitch_client.get_user_by_login(session, login)
        except Exception:
            logging.exception("twitch user lookup failed")
            await message.reply_text("Не удалось проверить стримера в Twitch. Попробуйте позже.")
            return

        if not t_user:
            await message.reply_text("Такой стример не найден на Twitch.")
            return

        existing = await active_db.list_user_subscriptions(user.id)
        if t_user.login not in existing:
            current_count = len(existing)
            limit = await active_db.get_effective_sub_limit(user.id)
            if current_count >= limit:
                await active_db.log_action(user.id, "subscribe_limit_reached", f"{current_count}/{limit}")
                await message.reply_text(LIMIT_REACHED_TEXT, reply_markup=get_main_keyboard(admin))
                context.user_data.pop("await_subscribe_input", None)
                return

        changed = await active_db.subscribe(user.id, t_user.login)
        await active_db.log_action(user.id, "subscribe", t_user.login)
        context.user_data.pop("await_subscribe_input", None)

        if changed:
            await message.reply_text(
                f"✅ Вы подписались на {t_user.display_name} ({t_user.login})",
                reply_markup=get_main_keyboard(admin),
            )
        else:
            await message.reply_text(
                f"Вы уже подписаны на {t_user.login}",
                reply_markup=get_main_keyboard(admin),
            )
        return

    # Main menu actions
    if text == "🔔 Подписаться":
        clear_states(context)
        set_back_target(context, "main")
        context.user_data["await_subscribe_input"] = True
        await active_db.log_action(user.id, "subscribe_menu")
        await message.reply_text(
            "Отправьте логин Twitch или ссылку на канал.",
            reply_markup=get_back_only_keyboard(),
        )
        return

    if text == "⭐ Рекомендации":
        clear_states(context)
        set_back_target(context, "main")
        kb = await get_recommendations_keyboard()
        await active_db.log_action(user.id, "recommendations_open")
        await message.reply_text("Выберите стримера из рекомендаций:", reply_markup=kb)
        return

    if text.startswith("⭐ "):
        login = text[2:].strip().lower()
        if login not in await active_db.list_recommended():
            await message.reply_text("Стример не найден в рекомендациях.")
            return
        existing = await active_db.list_user_subscriptions(user.id)
        if login not in existing:
            current_count = len(existing)
            limit = await active_db.get_effective_sub_limit(user.id)
            if current_count >= limit:
                await active_db.log_action(user.id, "subscribe_limit_reached", f"{current_count}/{limit}")
                await message.reply_text(LIMIT_REACHED_TEXT, reply_markup=get_main_keyboard(admin))
                return
        changed = await active_db.subscribe(user.id, login)
        await active_db.log_action(user.id, "subscribe_recommended", login)
        msg = f"✅ Подписка на {login} оформлена." if changed else f"Вы уже подписаны на {login}."
        await message.reply_text(msg, reply_markup=get_main_keyboard(admin))
        return

    if text == "📋 Мои подписки":
        clear_states(context)
        set_back_target(context, "main")
        subs = await active_db.list_user_subscriptions(user.id)
        if not subs:
            context.user_data.pop("await_unsubscribe_select", None)
            reply = "У вас нет подписок."
            await safe_reply_text(message, reply, reply_markup=get_main_keyboard(admin))
        else:
            context.user_data["await_unsubscribe_select"] = True
            reply = "Ваши подписки. Нажмите кнопку, чтобы отписаться:"
            sent = await safe_reply_text(message, reply, reply_markup=get_unsubscribe_keyboard(subs))
            if not sent:
                # Do not leave user in unstable unsubscribe mode if menu message was not delivered.
                context.user_data.pop("await_unsubscribe_select", None)
        await active_db.log_action(user.id, "my_subscriptions", str(len(subs)))
        return

    if text == "📎 Ссылки":
        links_text = await get_links_text()
        await active_db.log_action(user.id, "links")
        await message.reply_html(links_text, disable_web_page_preview=True, reply_markup=get_main_keyboard(admin))
        return

    if text == "💬 Обратная связь":
        await active_db.log_action(user.id, "feedback")
        await message.reply_text(await get_feedback_text())
        return

    if admin and text == "🔄 Обновить пользователей":
        set_back_target(context, "admin_users")
        await refresh_users_from_telegram(context.application)
        report = await active_db.get_subscriptions_report()
        await active_db.log_action(user.id, "admin_users_refresh")
        await send_long_text(message, report, reply_markup=get_users_admin_keyboard())
        return

    if admin and text == "🚫 Ограничить доступ":
        set_back_target(context, "admin_users")
        context.user_data["await_access_block"] = True
        context.user_data.pop("await_access_allow", None)
        await message.reply_text(
            "Введите ID или @username пользователя для блокировки:",
            reply_markup=get_back_only_keyboard(),
        )
        return

    if admin and text == "✅ Разрешить доступ":
        set_back_target(context, "admin_users")
        context.user_data["await_access_allow"] = True
        context.user_data.pop("await_access_block", None)
        await message.reply_text(
            "Введите ID или @username пользователя для разблокировки:",
            reply_markup=get_back_only_keyboard(),
        )
        return

    if admin and text == "📄 Список рекомендаций":
        set_back_target(context, "admin_recommend")
        recs = await active_db.list_recommended()
        body = "\n".join(f"- {x}" for x in recs) if recs else "Список пуст."
        await message.reply_text(f"Рекомендации:\n{body}", reply_markup=get_admin_recommendations_keyboard())
        return

    if admin and text == "ℹ️ Показать лимиты":
        set_back_target(context, "admin_limits")
        default_limit = await active_db.get_default_sub_limit()
        await message.reply_text(
            f"Лимит по умолчанию: {default_limit}\n"
            "Персональные лимиты смотрите в разделе Пользователи (формат текущие/лимит).",
            reply_markup=get_limits_admin_keyboard(),
        )
        return

    if admin and text == "📌 Лимит по умолчанию":
        set_back_target(context, "admin_limits")
        context.user_data["await_limit_default"] = True
        await message.reply_text("Введите новый лимит по умолчанию (целое > 0):", reply_markup=get_back_only_keyboard())
        return

    if admin and text == "👤 Лимит пользователя":
        set_back_target(context, "admin_limits")
        context.user_data["await_limit_user_target"] = True
        await message.reply_text(
            "Введите ID или @username пользователя:",
            reply_markup=get_back_only_keyboard(),
        )
        return

    if admin and text == "🔗 Настроить ссылки":
        set_back_target(context, "admin_content")
        context.user_data["await_links_message"] = True
        await message.reply_text(
            "Отправьте новый текст для раздела Ссылки (можно многострочный).",
            reply_markup=get_back_only_keyboard(),
        )
        return

    if admin and text == "💬 Настроить обратную связь":
        set_back_target(context, "admin_content")
        context.user_data["await_feedback_message"] = True
        await message.reply_text(
            "Отправьте новый текст для раздела Обратная связь.",
            reply_markup=get_back_only_keyboard(),
        )
        return

    if admin and text == "👁 Показать контент":
        set_back_target(context, "admin_content")
        links_preview = await get_links_text()
        feedback_preview = await get_feedback_text()
        await send_long_text(
            message,
            "Текущий контент:\n\n[Ссылки]\n" + links_preview + "\n\n[Обратная связь]\n" + feedback_preview,
            reply_markup=get_content_admin_keyboard(),
            disable_preview=True,
        )
        return

    if admin and text == "➕ Добавить в рекомендации":
        set_back_target(context, "admin_recommend")
        context.user_data["await_recommend_add"] = True
        context.user_data.pop("await_recommend_remove", None)
        await message.reply_text("Отправьте логин или ссылку Twitch для добавления.", reply_markup=get_back_only_keyboard())
        return

    if admin and text == "➖ Удалить из рекомендаций":
        set_back_target(context, "admin_recommend")
        recs = await active_db.list_recommended()
        if not recs:
            await message.reply_text("Список рекомендаций пуст.", reply_markup=get_admin_recommendations_keyboard())
            return
        context.user_data["await_recommend_remove"] = True
        context.user_data.pop("await_recommend_add", None)
        buttons = [[KeyboardButton(f"➖ {x}")] for x in recs]
        buttons.append([KeyboardButton(BACK)])
        await message.reply_text(
            "Выберите стримера для удаления:",
            reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True),
        )
        return

    await active_db.log_action(user.id, "unknown", text)
    await message.reply_text("Используйте кнопки меню.", reply_markup=get_main_keyboard(admin))


async def handle_media(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    assert active_db
    user = update.effective_user
    message = update.message
    if not user or not message:
        return

    await ensure_user(update)
    if not await is_admin(user.id):
        return

    state = get_broadcast_state(context)
    if not state or state.step != "content":
        return

    if message.photo:
        photo_id = message.photo[-1].file_id
        ok, failed = await send_to_targets(
            context.application,
            state.targets,
            kind="photo",
            photo_id=photo_id,
            caption=message.caption,
        )
        await active_db.log_action(user.id, "broadcast_photo", f"ok={ok}, fail={len(failed)}")
        clear_states(context)
        await send_long_text(
            message,
            await build_broadcast_result_text(ok, failed),
            reply_markup=get_admin_keyboard(),
        )
        return

    if message.document:
        doc_id = message.document.file_id
        ok, failed = await send_to_targets(
            context.application,
            state.targets,
            kind="document",
            document_id=doc_id,
            caption=message.caption,
        )
        await active_db.log_action(user.id, "broadcast_file", f"ok={ok}, fail={len(failed)}")
        clear_states(context)
        await send_long_text(
            message,
            await build_broadcast_result_text(ok, failed),
            reply_markup=get_admin_keyboard(),
        )


async def monitor_tick(context: ContextTypes.DEFAULT_TYPE) -> None:
    assert active_db and twitch_client
    app = context.application
    session: aiohttp.ClientSession = app.bot_data["http_session"]
    cooldown = timedelta(minutes=NOTIFY_COOLDOWN_MINUTES)

    streamers = await active_db.list_subscribed_streamers()
    now_utc = datetime.now(timezone.utc)

    for login in streamers:
        try:
            stream = await twitch_client.get_stream(session, login)
            state = await active_db.get_stream_state(login)

            is_live = stream is not None
            stream_id = stream.get("id") if stream else None
            became_live = is_live and not state.is_live
            stream_changed = is_live and state.last_stream_id and stream_id and stream_id != state.last_stream_id

            last_notified = state.last_notified_at
            should_notify = became_live or bool(stream_changed)

            if should_notify:
                in_cooldown = last_notified and (now_utc - last_notified < cooldown)
                if not in_cooldown:
                    success_count = 0
                    for uid in await active_db.list_subscribers_for_streamer(login):
                        try:
                            if not await active_db.is_user_allowed(uid):
                                continue
                            await tg_call_with_retry(
                                lambda: app.bot.send_message(uid, f"🔴 {login} в эфире! https://www.twitch.tv/{login}"),
                                attempts=2,
                                base_delay=0.5,
                            )
                            await active_db.log_action(uid, "notify_stream_live", login)
                            success_count += 1
                        except Exception:
                            logging.exception("notify failed uid=%s login=%s", uid, login)
                    if success_count > 0:
                        last_notified = now_utc
                    else:
                        logging.warning(
                            "notify skipped cooldown update streamer=%s because all deliveries failed",
                            login,
                        )
                else:
                    logging.info("cooldown skip streamer=%s", login)

            await active_db.set_stream_state(
                login,
                is_live=is_live,
                last_stream_id=stream_id if is_live else None,
                last_notified_at=last_notified,
            )
        except Exception:
            logging.exception("monitor failed streamer=%s", login)


async def post_init(app: Application) -> None:
    app.bot_data["http_session"] = aiohttp.ClientSession()
    app.job_queue.run_repeating(monitor_tick, interval=CHECK_INTERVAL, first=5)


async def post_shutdown(app: Application) -> None:
    session: aiohttp.ClientSession | None = app.bot_data.get("http_session")
    if session and not session.closed:
        await session.close()


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logging.exception("Unhandled error", exc_info=context.error)
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text("Произошла ошибка. Попробуйте ещё раз.")
        except Exception:
            pass


def parse_required_env() -> None:
    missing = []
    if not TELEGRAM_TOKEN:
        missing.append("TELEGRAM_TOKEN")
    if not TWITCH_CLIENT_ID:
        missing.append("TWITCH_CLIENT_ID")
    if not TWITCH_CLIENT_SECRET:
        missing.append("TWITCH_CLIENT_SECRET")
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")


def main() -> None:
    global active_db, twitch_client

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    parse_required_env()

    twitch_client = TwitchClient(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET)

    old_db, active_db = asyncio.run(
        prepare_databases(
            base_dir=BASE_DIR,
            db_root=DB_ROOT,
            env_admin_ids=ADMIN_IDS,
            recommended_streamers=RECOMMENDED_STREAMERS,
        )
    )
    _ = old_db
    asyncio.set_event_loop(asyncio.new_event_loop())

    app = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .connect_timeout(10)
        .read_timeout(15)
        .write_timeout(15)
        .pool_timeout(10)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(MessageHandler(filters.PHOTO | filters.Document.ALL, handle_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_error_handler(on_error)

    app.run_polling()


if __name__ == "__main__":
    main()

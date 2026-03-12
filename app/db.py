import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import aiosqlite


LOG_LINE_RE = re.compile(r"^\[(?P<ts>[^\]]+)\]\s+(?P<identity>.+?)\s+-\s+(?P<action>.+)$")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_identity(first_line: str) -> tuple[str | None, str | None]:
    # Example: [2025-01-01 12:00:00] Name (@username, ID 123) - Action
    if "] " not in first_line:
        return None, None
    payload = first_line.split("] ", 1)[1]
    identity = payload.split(" -", 1)[0].strip()

    username = None
    if "(@" in identity:
        part = identity.split("(@", 1)[1]
        username = part.split(",", 1)[0].strip().lstrip("@").lower()

    return identity, username


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT,
    full_name TEXT,
    is_admin INTEGER NOT NULL DEFAULT 0,
    first_seen TEXT,
    last_seen TEXT
);

CREATE TABLE IF NOT EXISTS recommended_streamers (
    login TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    user_id INTEGER NOT NULL,
    streamer_login TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (user_id, streamer_login),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS activity_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    action TEXT NOT NULL,
    payload TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS stream_state (
    streamer_login TEXT PRIMARY KEY,
    is_live INTEGER NOT NULL DEFAULT 0,
    last_stream_id TEXT,
    last_notified_at TEXT
);

CREATE TABLE IF NOT EXISTS user_access (
    user_id INTEGER PRIMARY KEY,
    is_allowed INTEGER NOT NULL DEFAULT 1,
    note TEXT,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS user_limits (
    user_id INTEGER PRIMARY KEY,
    max_subscriptions INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_subs_streamer ON subscriptions(streamer_login);
CREATE INDEX IF NOT EXISTS idx_logs_user_created ON activity_logs(user_id, created_at DESC);
"""


@dataclass
class StreamState:
    is_live: bool
    last_stream_id: str | None
    last_notified_at: datetime | None


class Database:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    async def init(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(self.db_path) as db:
            await db.executescript(SCHEMA_SQL)
            await db.commit()

    async def upsert_user(self, user_id: int, username: str | None, full_name: str | None) -> None:
        ts = now_iso()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO users(id, username, full_name, first_seen, last_seen)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                  username=COALESCE(excluded.username, users.username),
                  full_name=COALESCE(excluded.full_name, users.full_name),
                  last_seen=excluded.last_seen
                """,
                (user_id, username, full_name, ts, ts),
            )
            await db.commit()

    async def set_admins(self, admin_ids: Iterable[int]) -> None:
        ids = sorted({int(x) for x in admin_ids})
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("UPDATE users SET is_admin=0")
            for uid in ids:
                await db.execute(
                    """
                    INSERT INTO users(id, first_seen, last_seen, is_admin)
                    VALUES (?, ?, ?, 1)
                    ON CONFLICT(id) DO UPDATE SET is_admin=1
                    """,
                    (uid, now_iso(), now_iso()),
                )
            await db.commit()

    async def is_admin(self, user_id: int) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT is_admin FROM users WHERE id = ?", (user_id,))
            row = await cur.fetchone()
            return bool(row and row[0])

    async def is_user_allowed(self, user_id: int) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT is_admin FROM users WHERE id = ?", (user_id,))
            admin_row = await cur.fetchone()
            if admin_row and admin_row[0]:
                return True

            cur = await db.execute("SELECT is_allowed FROM user_access WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
            if not row:
                return True
            return bool(row[0])

    async def set_user_access(self, user_id: int, is_allowed: bool, note: str | None = None) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO user_access(user_id, is_allowed, note, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                  is_allowed=excluded.is_allowed,
                  note=excluded.note,
                  updated_at=excluded.updated_at
                """,
                (user_id, 1 if is_allowed else 0, note, now_iso()),
            )
            await db.commit()

    async def add_recommended(self, login: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO recommended_streamers(login, created_at) VALUES (?, ?)",
                (login.lower(), now_iso()),
            )
            await db.commit()

    async def list_recommended(self) -> list[str]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                SELECT login
                FROM recommended_streamers
                ORDER BY CASE WHEN lower(login) = 'bedmah' THEN 0 ELSE 1 END, login
                """
            )
            rows = await cur.fetchall()
            return [r[0] for r in rows]

    async def recommended_count(self) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM recommended_streamers")
            row = await cur.fetchone()
            return int(row[0]) if row else 0

    async def remove_recommended(self, login: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM recommended_streamers WHERE lower(login) = lower(?)", (login,))
            changed = db.total_changes > 0
            await db.commit()
            return changed

    async def subscribe(self, user_id: int, streamer_login: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT OR IGNORE INTO subscriptions(user_id, streamer_login, created_at)
                VALUES (?, ?, ?)
                """,
                (user_id, streamer_login.lower(), now_iso()),
            )
            changed = db.total_changes > 0
            await db.commit()
            return changed

    async def count_user_subscriptions(self, user_id: int) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT COUNT(*) FROM subscriptions WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
            return int(row[0]) if row else 0

    async def unsubscribe(self, user_id: int, streamer_login: str) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "DELETE FROM subscriptions WHERE user_id = ? AND streamer_login = ?",
                (user_id, streamer_login.lower()),
            )
            changed = db.total_changes > 0
            await db.commit()
            return changed

    async def list_user_subscriptions(self, user_id: int) -> list[str]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "SELECT streamer_login FROM subscriptions WHERE user_id = ? ORDER BY streamer_login",
                (user_id,),
            )
            rows = await cur.fetchall()
            return [r[0] for r in rows]

    async def list_subscribers_for_streamer(self, streamer_login: str) -> list[int]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "SELECT user_id FROM subscriptions WHERE streamer_login = ?",
                (streamer_login.lower(),),
            )
            rows = await cur.fetchall()
            return [int(r[0]) for r in rows]

    async def list_subscribed_streamers(self) -> list[str]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT DISTINCT streamer_login FROM subscriptions ORDER BY streamer_login")
            rows = await cur.fetchall()
            return [r[0] for r in rows]

    async def all_users(self) -> list[tuple[int, str | None, str | None]]:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT id, username, full_name FROM users ORDER BY id")
            rows = await cur.fetchall()
            return [(int(r[0]), r[1], r[2]) for r in rows]

    async def get_user_username(self, user_id: int) -> str | None:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT username FROM users WHERE id = ?", (user_id,))
            row = await cur.fetchone()
            if not row:
                return None
            return row[0]

    async def get_default_sub_limit(self) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT value FROM app_settings WHERE key = 'default_sub_limit'")
            row = await cur.fetchone()
            if row and str(row[0]).isdigit():
                return int(row[0])
            await db.execute(
                "INSERT OR REPLACE INTO app_settings(key, value) VALUES('default_sub_limit', ?)",
                ("50",),
            )
            await db.commit()
            return 50

    async def get_setting(self, key: str, default: str | None = None) -> str | None:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT value FROM app_settings WHERE key = ?", (key,))
            row = await cur.fetchone()
            if row:
                return str(row[0])
            return default

    async def set_setting(self, key: str, value: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO app_settings(key, value) VALUES(?, ?)",
                (key, value),
            )
            await db.commit()

    async def set_default_sub_limit(self, value: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO app_settings(key, value) VALUES('default_sub_limit', ?)",
                (str(int(value)),),
            )
            await db.commit()

    async def set_user_sub_limit(self, user_id: int, value: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO user_limits(user_id, max_subscriptions, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                  max_subscriptions=excluded.max_subscriptions,
                  updated_at=excluded.updated_at
                """,
                (user_id, int(value), now_iso()),
            )
            await db.commit()

    async def get_user_sub_limit(self, user_id: int) -> int | None:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute("SELECT max_subscriptions FROM user_limits WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
            return int(row[0]) if row else None

    async def get_effective_sub_limit(self, user_id: int) -> int:
        user_limit = await self.get_user_sub_limit(user_id)
        if user_limit is not None:
            return user_limit
        return await self.get_default_sub_limit()

    async def resolve_user(self, query: str) -> int | None:
        q = query.strip().lstrip("@")
        async with aiosqlite.connect(self.db_path) as db:
            if q.isdigit():
                cur = await db.execute("SELECT id FROM users WHERE id = ?", (int(q),))
                row = await cur.fetchone()
                return int(row[0]) if row else None

            cur = await db.execute("SELECT id FROM users WHERE lower(username) = lower(?)", (q,))
            row = await cur.fetchone()
            return int(row[0]) if row else None

    async def log_action(self, user_id: int | None, action: str, payload: str | None = None) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT INTO activity_logs(user_id, action, payload, created_at) VALUES (?, ?, ?, ?)",
                (user_id, action, payload, now_iso()),
            )
            await db.commit()

    async def get_user_log_text(self, user_id: int, limit: int = 300) -> str:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                SELECT created_at, action, payload
                FROM activity_logs
                WHERE user_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (user_id, limit),
            )
            rows = await cur.fetchall()

        if not rows:
            return "Лог пуст."

        lines = []
        for created_at, action, payload in reversed(rows):
            suffix = f" | {payload}" if payload else ""
            lines.append(f"[{created_at}] {action}{suffix}")
        return "\n".join(lines)

    async def get_subscriptions_report(self) -> str:
        default_limit = await self.get_default_sub_limit()
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                SELECT
                  u.id,
                  u.username,
                  u.full_name,
                  COALESCE(ua.is_allowed, 1) AS is_allowed,
                  ul.max_subscriptions,
                  COUNT(s.streamer_login) AS subs_count,
                  GROUP_CONCAT(s.streamer_login, ', ')
                FROM users u
                LEFT JOIN subscriptions s ON s.user_id = u.id
                LEFT JOIN user_access ua ON ua.user_id = u.id
                LEFT JOIN user_limits ul ON ul.user_id = u.id
                GROUP BY u.id, u.username, u.full_name, ua.is_allowed, ul.max_subscriptions
                ORDER BY u.id
                """
            )
            rows = await cur.fetchall()

        if not rows:
            return "Пользователей нет."

        total = len(rows)
        blocked = sum(1 for r in rows if not r[3])
        with_subs = sum(1 for r in rows if int(r[5] or 0) > 0)
        lines = [
            "📑 Пользователи и подписки:",
            f"Всего: {total} | С подписками: {with_subs} | Ограничены: {blocked} | Лимит по умолчанию: {default_limit}",
            "",
        ]
        for uid, username, full_name, is_allowed, user_limit, subs_count, subs in rows:
            uname = f"@{username}" if username else "no_username"
            name = full_name or "Unknown"
            access = "allowed" if is_allowed else "blocked"
            effective_limit = int(user_limit) if user_limit is not None else default_limit
            lines.append(
                f"- {name} ({uname}, ID {uid}) [{access}] | "
                f"подписок: {subs_count}/{effective_limit} | {subs or 'нет подписок'}"
            )
        return "\n".join(lines)

    async def get_stream_state(self, streamer_login: str) -> StreamState:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "SELECT is_live, last_stream_id, last_notified_at FROM stream_state WHERE streamer_login = ?",
                (streamer_login.lower(),),
            )
            row = await cur.fetchone()

        if not row:
            return StreamState(is_live=False, last_stream_id=None, last_notified_at=None)

        last_notified = None
        if row[2]:
            try:
                last_notified = datetime.fromisoformat(row[2])
            except Exception:
                last_notified = None

        return StreamState(is_live=bool(row[0]), last_stream_id=row[1], last_notified_at=last_notified)

    async def set_stream_state(
        self,
        streamer_login: str,
        *,
        is_live: bool,
        last_stream_id: str | None,
        last_notified_at: datetime | None,
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO stream_state(streamer_login, is_live, last_stream_id, last_notified_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(streamer_login) DO UPDATE SET
                  is_live=excluded.is_live,
                  last_stream_id=excluded.last_stream_id,
                  last_notified_at=excluded.last_notified_at
                """,
                (
                    streamer_login.lower(),
                    1 if is_live else 0,
                    last_stream_id,
                    last_notified_at.isoformat() if last_notified_at else None,
                ),
            )
            await db.commit()


def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except Exception:
        return default


async def migrate_legacy_to_old_db(
    old_db: Database,
    *,
    legacy_admins: list[int],
    legacy_subscribers: dict[str, list[int]],
    legacy_user_dirs: list[Path],
    recommended_streamers: list[str],
) -> None:
    await old_db.init()

    for login in recommended_streamers:
        await old_db.add_recommended(login)

    # Add users from log files (including users with no subscriptions).
    seen_users: set[int] = set()
    for users_dir in legacy_user_dirs:
        if not users_dir.exists():
            continue
        for file in users_dir.glob("*.txt"):
            if not file.stem.isdigit():
                continue
            uid = int(file.stem)
            seen_users.add(uid)

            first_line = ""
            try:
                for line in file.read_text(encoding="utf-8", errors="ignore").splitlines():
                    if line.strip():
                        first_line = line
                        break
            except Exception:
                pass

            full_name, username = parse_identity(first_line) if first_line else (None, None)
            await old_db.upsert_user(uid, username, full_name)

            # Move legacy line logs to DB activity logs.
            try:
                for raw in file.read_text(encoding="utf-8", errors="ignore").splitlines():
                    m = LOG_LINE_RE.match(raw)
                    if not m:
                        continue
                    await old_db.log_action(uid, m.group("action"), None)
            except Exception:
                pass

    # Add users referenced only in subscriptions.
    for streamer, users in legacy_subscribers.items():
        for uid in users:
            uid_int = int(uid)
            if uid_int not in seen_users:
                await old_db.upsert_user(uid_int, None, None)

            await old_db.subscribe(uid_int, streamer)

    await old_db.set_admins(legacy_admins)


async def prepare_databases(
    *,
    base_dir: Path,
    db_root: Path,
    env_admin_ids: list[int],
    recommended_streamers: list[str],
) -> tuple[Database, Database]:
    old_db = Database(db_root / "oldBD" / "oldbot.db")
    active_db = Database(db_root / "BD" / "bot.db")
    
    async def db_has_users(db: Database) -> bool:
        if not db.db_path.exists():
            return False
        await db.init()
        async with aiosqlite.connect(db.db_path) as conn:
            cur = await conn.execute("SELECT COUNT(*) FROM users")
            row = await cur.fetchone()
            return bool(row and row[0] > 0)

    # oldBD migration from legacy files/json
    legacy_admins = load_json(base_dir / "data" / "admins.json", None)
    if legacy_admins is None:
        legacy_admins = load_json(base_dir.parent / "admins.json", [])

    legacy_subscribers = load_json(base_dir / "data" / "subscribers.json", None)
    if legacy_subscribers is None:
        legacy_subscribers = load_json(base_dir.parent / "subscribers.json", {})

    legacy_user_dirs = [base_dir / "data" / "users", base_dir.parent / "users"]

    if not await db_has_users(old_db):
        await migrate_legacy_to_old_db(
            old_db,
            legacy_admins=[int(x) for x in legacy_admins],
            legacy_subscribers={k: [int(u) for u in v] for k, v in legacy_subscribers.items()},
            legacy_user_dirs=legacy_user_dirs,
            recommended_streamers=recommended_streamers,
        )
    else:
        await old_db.init()

    # Clean active DB for test
    if not active_db.db_path.exists():
        await active_db.init()
        for login in recommended_streamers:
            await active_db.add_recommended(login)
    else:
        await active_db.init()
        # Preserve admin-managed recommendations across restarts.
        # Seed only if the table is empty.
        if await active_db.recommended_count() == 0:
            for login in recommended_streamers:
                await active_db.add_recommended(login)

    # Admins for active DB come from env only (test DB is clean but manageable).
    await active_db.set_admins(env_admin_ids)

    return old_db, active_db

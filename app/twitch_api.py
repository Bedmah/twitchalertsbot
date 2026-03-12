import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp


LOGIN_RE = re.compile(r"^[A-Za-z0-9_]{3,25}$")
TWITCH_LINK_RE = re.compile(r"(?:https?://)?(?:www\.)?twitch\.tv/([A-Za-z0-9_]{3,25})", re.IGNORECASE)


@dataclass
class TwitchUser:
    id: str
    login: str
    display_name: str


class TwitchClient:
    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self._token = ""
        self._expires_at = datetime.min.replace(tzinfo=timezone.utc)

    async def ensure_token(self, session: aiohttp.ClientSession) -> None:
        now = datetime.now(timezone.utc)
        if self._token and now + timedelta(seconds=60) < self._expires_at:
            return

        async with session.post(
            "https://id.twitch.tv/oauth2/token",
            params={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()

        self._token = data["access_token"]
        self._expires_at = now + timedelta(seconds=int(data.get("expires_in", 3600)))

    async def _get(self, session: aiohttp.ClientSession, url: str, params: dict[str, Any]) -> dict[str, Any]:
        await self.ensure_token(session)

        async def request() -> aiohttp.ClientResponse:
            return await session.get(
                url,
                params=params,
                headers={"Client-ID": self.client_id, "Authorization": f"Bearer {self._token}"},
                timeout=aiohttp.ClientTimeout(total=20),
            )

        resp = await request()
        if resp.status == 401:
            await resp.release()
            self._token = ""
            await self.ensure_token(session)
            resp = await request()

        async with resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_user_by_login(self, session: aiohttp.ClientSession, login: str) -> TwitchUser | None:
        payload = await self._get(session, "https://api.twitch.tv/helix/users", {"login": login.lower()})
        data = payload.get("data") or []
        if not data:
            return None
        row = data[0]
        return TwitchUser(id=row["id"], login=row["login"], display_name=row.get("display_name") or row["login"])

    async def get_stream(self, session: aiohttp.ClientSession, login: str) -> dict[str, Any] | None:
        try:
            payload = await self._get(session, "https://api.twitch.tv/helix/streams", {"user_login": login.lower()})
        except aiohttp.ClientResponseError as exc:
            if exc.status in {400, 404}:
                return None
            raise

        data = payload.get("data") or []
        return data[0] if data else None


def parse_twitch_login(raw: str) -> str | None:
    value = (raw or "").strip()
    if not value:
        return None

    match = TWITCH_LINK_RE.search(value)
    if match:
        return match.group(1).lower()

    value = value.lstrip("@")
    if LOGIN_RE.match(value):
        return value.lower()

    return None

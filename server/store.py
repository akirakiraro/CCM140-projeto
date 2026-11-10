"""Estado local do servidor. Cada container tem o proprio arquivo."""

import json
import os
import tempfile
from typing import TypedDict


class LoginRecord(TypedDict):
    user: str
    ts: int


class ChannelRecord(TypedDict):
    name: str
    creator: str
    ts: int


class PublicationRecord(TypedDict):
    user: str
    channel: str
    text: str
    ts: int
    request_ts: int


class ServerState(TypedDict):
    logins: list[LoginRecord]
    channels: list[ChannelRecord]
    publications: list[PublicationRecord]


def empty_state() -> ServerState:
    return {"logins": [], "channels": [], "publications": []}


def load_state(path: str) -> ServerState:
    if not os.path.exists(path):
        return empty_state()
    with open(path, encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict):
        raise ValueError(
            f"arquivo {path} nao e objeto; esperado logins, channels e publications"
        )
    if "logins" not in raw or "channels" not in raw:
        raise ValueError(f"arquivo {path} sem chaves logins/channels")
    # Volumes da parte 1 nao tinham esta lista. Entra vazia e passa a ser gravada.
    if "publications" not in raw:
        raw["publications"] = []
    return raw


def save_state(path: str, state: ServerState) -> None:
    folder = os.path.dirname(path) or "."
    os.makedirs(folder, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=folder, suffix=".tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False)
    os.replace(tmp, path)

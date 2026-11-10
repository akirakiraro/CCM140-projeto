"""Regras de login, canal, listagem e publicacao. Respostas levam o ts do servidor."""

import re
from typing import Any

from store import ChannelRecord, LoginRecord, ServerState

NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


def server_now(now_ms: int) -> int:
    if isinstance(now_ms, bool) or not isinstance(now_ms, int):
        raise ValueError(f"ts do servidor invalido: {now_ms!r}; esperado inteiro")
    return now_ms


def require_ts(message: dict[str, Any]) -> int:
    ts = message.get("ts")
    if isinstance(ts, bool) or not isinstance(ts, int):
        raise ValueError(f"ts invalido: {ts!r}; esperado inteiro epoch em ms")
    return ts


def require_name(value: Any, field: str) -> str:
    if not isinstance(value, str) or NAME_RE.fullmatch(value) is None:
        raise ValueError(
            f"{field} invalido: {value!r}; esperado 1-32 caracteres [A-Za-z0-9_-]"
        )
    return value


def channel_names(state: ServerState) -> list[str]:
    return [item["name"] for item in state["channels"]]


def handle_login(state: ServerState, message: dict[str, Any], now_ms: int) -> dict:
    user = require_name(message.get("user"), "user")
    require_ts(message)
    record: LoginRecord = {"user": user, "ts": now_ms}
    state["logins"].append(record)
    return {"op": "login_ok", "ts": now_ms, "user": user}


def handle_create(state: ServerState, message: dict[str, Any], now_ms: int) -> dict:
    user = require_name(message.get("user"), "user")
    channel = require_name(message.get("channel"), "channel")
    require_ts(message)
    if channel in channel_names(state):
        raise ValueError(f"canal ja existe: {channel!r}")
    record: ChannelRecord = {"name": channel, "creator": user, "ts": now_ms}
    state["channels"].append(record)
    return {"op": "create_ok", "ts": now_ms, "user": user, "channel": channel}


def handle_list(state: ServerState, message: dict[str, Any], now_ms: int) -> dict:
    require_name(message.get("user"), "user")
    require_ts(message)
    return {"op": "list_ok", "ts": now_ms, "channels": channel_names(state)}


def require_text(value: Any) -> str:
    if not isinstance(value, str) or value == "" or len(value) > 1024:
        raise ValueError(
            f"text invalido: {value!r}; esperado string de 1 a 1024 caracteres"
        )
    return value


def handle_publish(state: ServerState, message: dict[str, Any], now_ms: int) -> tuple[dict, dict]:
    user = require_name(message.get("user"), "user")
    channel = require_name(message.get("channel"), "channel")
    text = require_text(message.get("text"))
    request_ts = require_ts(message)
    if channel not in channel_names(state):
        raise ValueError(f"canal nao existe: {channel!r}")
    state["publications"].append(
        {"user": user, "channel": channel, "text": text, "ts": now_ms, "request_ts": request_ts}
    )
    reply = {"op": "publish_ok", "ts": now_ms, "user": user, "channel": channel}
    payload = {"op": "channel_message", "ts": now_ms, "user": user, "channel": channel, "text": text}
    return reply, payload


def dispatch(state: ServerState, message: Any, now_ms: int) -> tuple[dict, dict | None]:
    now_ms = server_now(now_ms)
    if not isinstance(message, dict):
        return {"op": "error", "ts": now_ms, "error": f"mensagem invalida: {message!r}"}, None
    op = message.get("op")
    try:
        return _route(state, message, now_ms, op)
    except ValueError as exc:
        return _error_reply(op, message, now_ms, str(exc)), None


def _route(state: ServerState, message: dict, now_ms: int, op: Any) -> tuple[dict, dict | None]:
    if op == "login":
        return handle_login(state, message, now_ms), None
    if op == "create_channel":
        return handle_create(state, message, now_ms), None
    if op == "list_channels":
        return handle_list(state, message, now_ms), None
    if op == "publish":
        return handle_publish(state, message, now_ms)
    return {"op": "error", "ts": now_ms, "error": f"operacao desconhecida: {op!r}"}, None


def _error_reply(op: Any, message: dict, now_ms: int, reason: str) -> dict:
    if op == "login":
        reply = {"op": "login_err", "ts": now_ms, "error": reason}
    elif op == "create_channel":
        reply = {"op": "create_err", "ts": now_ms, "error": reason}
    elif op == "publish":
        reply = {"op": "publish_err", "ts": now_ms, "error": reason}
    else:
        reply = {"op": "error", "ts": now_ms, "error": reason}
    if isinstance(message.get("user"), str):
        reply["user"] = message["user"]
    if isinstance(message.get("channel"), str):
        reply["channel"] = message["channel"]
    return reply

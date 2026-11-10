"""Copia passiva do registro. Quem atendeu o pedido empurra o dado ja gravado."""

from protocol import require_name, require_text
from store import ServerState


def as_ms(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field} invalido: {value!r}; esperado inteiro")
    return value


def store_record(state: ServerState, record: object) -> bool:
    if not isinstance(record, dict):
        raise ValueError(f"replica invalida: {record!r}; esperado mapa com kind")
    kind = record.get("kind")
    if kind == "login":
        return store_login(state, record)
    if kind == "channel":
        return store_channel(state, record)
    if kind == "publication":
        return store_publication(state, record)
    raise ValueError(f"kind invalido: {kind!r}; esperado login, channel ou publication")


def store_login(state: ServerState, record: dict) -> bool:
    user = require_name(record.get("user"), "user")
    ts = as_ms(record.get("ts"), "ts")
    if any(item["user"] == user and item["ts"] == ts for item in state["logins"]):
        return False
    state["logins"].append({"user": user, "ts": ts})
    return True


def store_channel(state: ServerState, record: dict) -> bool:
    name = require_name(record.get("name"), "channel")
    creator = require_name(record.get("creator"), "user")
    ts = as_ms(record.get("ts"), "ts")
    if any(item["name"] == name for item in state["channels"]):
        return False
    state["channels"].append({"name": name, "creator": creator, "ts": ts})
    return True


def publication_key(item: dict) -> tuple:
    return (item["user"], item["channel"], item["text"], item["ts"], item["request_ts"])


def store_publication(state: ServerState, record: dict) -> bool:
    stored = {
        "user": require_name(record.get("user"), "user"),
        "channel": require_name(record.get("channel"), "channel"),
        "text": require_text(record.get("text")),
        "ts": as_ms(record.get("ts"), "ts"),
        "request_ts": as_ms(record.get("request_ts"), "request_ts"),
    }
    if any(publication_key(item) == publication_key(stored) for item in state["publications"]):
        return False
    state["publications"].append(stored)
    return True


def pack_login(item: dict) -> dict:
    return {"kind": "login", "user": item["user"], "ts": item["ts"]}


def pack_channel(item: dict) -> dict:
    return {"kind": "channel", "name": item["name"], "creator": item["creator"], "ts": item["ts"]}


def pack_publication(item: dict) -> dict:
    return {
        "kind": "publication",
        "user": item["user"],
        "channel": item["channel"],
        "text": item["text"],
        "ts": item["ts"],
        "request_ts": item["request_ts"],
    }


def record_label(record: dict) -> str:
    kind = record.get("kind")
    if kind == "login":
        return f"login {record.get('user')}"
    if kind == "channel":
        return f"channel {record.get('name')}"
    if kind == "publication":
        return f"publication {record.get('channel')} {record.get('text')}"
    return repr(record)

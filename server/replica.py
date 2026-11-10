"""Copia passiva. Push na escrita e pull do estado na subida."""

import threading
import time
from collections.abc import Callable

from protocol import require_name, require_text
from store import ServerState, save_state

Ask = Callable[[object, dict, str, int], dict | None]
Now = Callable[[], int]


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


def publication_key(item: dict) -> tuple[str, str, str, int, int]:
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


def fresh_record(state: ServerState, reply: dict) -> dict | None:
    op = reply.get("op")
    if op == "login_ok" and state["logins"]:
        return pack_login(state["logins"][-1])
    if op == "create_ok" and state["channels"]:
        return pack_channel(state["channels"][-1])
    if op == "publish_ok" and state["publications"]:
        return pack_publication(state["publications"][-1])
    return None


def other_names(known: list[dict], own_name: str) -> list[str]:
    names: list[str] = []
    for item in known:
        name = item.get("name")
        if isinstance(name, str) and name != own_name:
            names.append(name)
    return names


def push_to_peers(
    context: object,
    ask: Ask,
    known: list[dict],
    record: dict,
    own_name: str,
    now_ms: Now,
    port: int,
    timeout_ms: int,
) -> None:
    label = record_label(record)
    for name in other_names(known, own_name):
        sent = {"op": "replicate", "ts": now_ms(), "name": own_name, "record": record}
        reply = ask(context, sent, f"tcp://{name}:{port}", timeout_ms)
        if isinstance(reply, dict) and reply.get("op") == "replicate_ok":
            continue
        print(f"replica pendente: {name} {label}", flush=True)


def accept_remote(
    state: ServerState,
    message: dict,
    lock: threading.Lock,
    path: str,
    now_ms: Now,
    own_name: str,
) -> dict:
    record = message.get("record")
    try:
        with lock:
            inserted = store_record(state, record)
            if inserted:
                save_state(path, state)
    except ValueError as exc:
        return {"op": "error", "ts": now_ms(), "error": str(exc)}
    status = "aplicada" if inserted else "ja estava"
    print(f"replica {status}: {record_label(record)}", flush=True)
    return {"op": "replicate_ok", "ts": now_ms(), "name": own_name}


def pack_state(state: ServerState) -> dict:
    return {
        "logins": [pack_login(item) for item in state["logins"]],
        "channels": [pack_channel(item) for item in state["channels"]],
        "publications": [pack_publication(item) for item in state["publications"]],
    }


def copy_state(state: ServerState) -> ServerState:
    return {
        "logins": [dict(item) for item in state["logins"]],
        "channels": [dict(item) for item in state["channels"]],
        "publications": [dict(item) for item in state["publications"]],
    }


def snapshot_lists(reply: dict) -> tuple[list, list, list]:
    found = (reply.get("logins"), reply.get("channels"), reply.get("publications"))
    if all(isinstance(item, list) for item in found):
        return found[0], found[1], found[2]
    kinds = tuple(type(item).__name__ for item in found)
    raise ValueError(
        f"snapshot invalido: tipos {kinds}; esperado tres listas logins, channels, publications"
    )


def as_dicts(records: list) -> list[dict]:
    found: list[dict] = []
    for record in records:
        if not isinstance(record, dict):
            raise ValueError(f"replica invalida: {record!r}; esperado mapa com kind")
        found.append(record)
    return found


def flat_records(reply: dict) -> list[dict]:
    logins, channels, publications = snapshot_lists(reply)
    records: list[dict] = []
    records.extend(as_dicts(logins))
    records.extend(as_dicts(channels))
    records.extend(as_dicts(publications))
    return records


def append_all(state: ServerState, reply: dict) -> int:
    added = 0
    for record in flat_records(reply):
        if store_record(state, record):
            added += 1
    return added


def merge_snapshot(state: ServerState, reply: dict) -> int:
    trial = copy_state(state)
    added = append_all(trial, reply)
    state["logins"] = trial["logins"]
    state["channels"] = trial["channels"]
    state["publications"] = trial["publications"]
    return added


def channel_label(state: ServerState) -> str:
    names = [item["name"] for item in state["channels"]]
    if not names:
        return "-"
    return ",".join(names)


def describe_pull(state: ServerState, reply: dict | None, source: str) -> tuple[str, int]:
    if not isinstance(reply, dict) or reply.get("op") != "snapshot_ok":
        return (f"replica pendente: {source} snapshot", 0)
    added = merge_snapshot(state, reply)
    line = f"replica puxada: de={source} novos={added} canais={channel_label(state)}"
    return (line, added)


def attempt_pull(
    context: object,
    ask: Ask,
    state: ServerState,
    lock: threading.Lock,
    name: str,
    own_name: str,
    now_ms: Now,
    port: int,
    timeout_ms: int,
    path: str,
) -> str:
    sent = {"op": "snapshot", "ts": now_ms(), "name": own_name}
    reply = ask(context, sent, f"tcp://{name}:{port}", timeout_ms)
    try:
        with lock:
            line, added = describe_pull(state, reply, name)
            if added:
                save_state(path, state)
    except ValueError as exc:
        return f"replica pendente: {name} snapshot {exc}"
    return line


def pull_named(
    context: object,
    ask: Ask,
    state: ServerState,
    lock: threading.Lock,
    name: str,
    own_name: str,
    now_ms: Now,
    port: int,
    timeout_ms: int,
    path: str,
) -> None:
    line = attempt_pull(
        context, ask, state, lock, name, own_name, now_ms, port, timeout_ms, path
    )
    # O outro container pode ainda nao ter aberto a 5560.
    if line.startswith("replica pendente:"):
        time.sleep(1)
        line = attempt_pull(
            context, ask, state, lock, name, own_name, now_ms, port, timeout_ms, path
        )
    print(line, flush=True)


def pull_known(
    context: object,
    ask: Ask,
    state: ServerState,
    lock: threading.Lock,
    known: list[dict],
    own_name: str,
    now_ms: Now,
    port: int,
    timeout_ms: int,
    path: str,
) -> None:
    for name in other_names(known, own_name):
        pull_named(
            context, ask, state, lock, name, own_name, now_ms, port, timeout_ms, path
        )

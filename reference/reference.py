"""Referencia dos servidores: rank, lista e heartbeat, sem devolver a hora."""

import re
import time

import msgpack
import zmq

BIND = "tcp://*:5559"
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
ALIVE_S = 30
servers: list[dict] = []
# Mesma regra do servidor: o rank tambem e mensagem, entao leva lc.
clock = 0


def now_ms() -> int:
    return int(time.time() * 1000)


def decode(raw: bytes) -> dict | None:
    try:
        message = msgpack.unpackb(raw, raw=False)
    except Exception:
        return None
    if isinstance(message, dict):
        return message
    return None


def find(name: str) -> dict | None:
    for item in servers:
        if item["name"] == name:
            return item
    return None


def mark_seen(item: dict) -> None:
    item["seen"] = time.monotonic()
    if item.get("quiet"):
        item["quiet"] = False
        print(f"servidor na lista: {item['name']}", flush=True)


def rank_of(name: str) -> int:
    item = find(name)
    if item is not None:
        mark_seen(item)
        return int(item["rank"])
    rank = len(servers) + 1
    servers.append({"name": name, "rank": rank, "seen": time.monotonic(), "quiet": False})
    return rank


def drop_silent() -> None:
    limit = time.monotonic() - ALIVE_S
    for item in servers:
        if not item["quiet"] and item["seen"] < limit:
            item["quiet"] = True
            print(f"servidor removido: {item['name']}", flush=True)


def reply_for(message: dict | None) -> dict:
    if not isinstance(message, dict):
        return {"op": "error", "ts": now_ms(), "error": f"pedido invalido: {message!r}"}
    if message.get("op") == "rank":
        return rank_reply(message)
    if message.get("op") == "list":
        return list_reply()
    if message.get("op") == "heartbeat":
        return heartbeat_reply(message)
    return {"op": "error", "ts": now_ms(), "error": f"pedido invalido: {message!r}"}


def rank_reply(message: dict) -> dict:
    name = message.get("name")
    if not isinstance(name, str) or NAME_RE.fullmatch(name) is None:
        return {"op": "error", "ts": now_ms(), "error": f"nome invalido: {name!r}"}
    return {"op": "rank_ok", "ts": now_ms(), "name": name, "rank": rank_of(name)}


def list_reply() -> dict:
    drop_silent()
    listed = [
        {"name": item["name"], "rank": item["rank"]}
        for item in servers
        if not item["quiet"]
    ]
    return {"op": "list_ok", "ts": now_ms(), "servers": listed}


def heartbeat_reply(message: dict) -> dict:
    name = message.get("name")
    if not isinstance(name, str) or NAME_RE.fullmatch(name) is None:
        return {"op": "error", "ts": now_ms(), "error": f"nome invalido: {name!r}"}
    item = find(name)
    if item is None:
        return {"op": "error", "ts": now_ms(), "error": f"servidor desconhecido: {name!r}"}
    mark_seen(item)
    return {"op": "heartbeat_ok", "name": name}


def observe(message: dict | None) -> None:
    global clock
    if not isinstance(message, dict):
        return
    seen = message.get("lc")
    if isinstance(seen, bool) or not isinstance(seen, int):
        return
    clock = max(clock, seen)


def stamp(message: dict) -> dict:
    global clock
    clock += 1
    message["lc"] = clock
    return message


def serve(socket: zmq.Socket) -> None:
    message = decode(socket.recv())
    print(f"Mensagem recebida: {message}", flush=True)
    observe(message)
    reply = stamp(reply_for(message))
    socket.send(msgpack.packb(reply, use_bin_type=True))
    print(f"Mensagem enviada: {reply}", flush=True)


def main() -> None:
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(BIND)
    print(f"referencia pronta: REP {BIND}", flush=True)
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    while True:
        ready = dict(poller.poll(1000))
        if socket in ready:
            serve(socket)
        drop_silent()


if __name__ == "__main__":
    main()

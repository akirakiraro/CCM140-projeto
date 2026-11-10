"""Referencia dos servidores: rank na entrada e lista dos nomes guardados."""

import re
import time

import msgpack
import zmq

BIND = "tcp://*:5559"
NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
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


def rank_of(name: str) -> int:
    for item in servers:
        if item["name"] == name:
            return int(item["rank"])
    rank = len(servers) + 1
    servers.append({"name": name, "rank": rank})
    return rank


def reply_for(message: dict | None) -> dict:
    if not isinstance(message, dict):
        return {"op": "error", "ts": now_ms(), "error": f"pedido invalido: {message!r}"}
    if message.get("op") == "rank":
        return rank_reply(message)
    if message.get("op") == "list":
        return list_reply()
    return {"op": "error", "ts": now_ms(), "error": f"pedido invalido: {message!r}"}


def rank_reply(message: dict) -> dict:
    name = message.get("name")
    if not isinstance(name, str) or NAME_RE.fullmatch(name) is None:
        return {"op": "error", "ts": now_ms(), "error": f"nome invalido: {name!r}"}
    return {"op": "rank_ok", "ts": now_ms(), "name": name, "rank": rank_of(name)}


def list_reply() -> dict:
    listed = [{"name": item["name"], "rank": item["rank"]} for item in servers]
    return {"op": "list_ok", "ts": now_ms(), "servers": listed}


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
    while True:
        serve(socket)


if __name__ == "__main__":
    main()

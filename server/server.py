"""Servidor REP: login, criacao e listagem de canais."""

import os
import time

import msgpack
import zmq

from protocol import dispatch
from store import load_state, save_state

STATE_PATH = os.environ.get("STATE_PATH", "/data/state.json")
BROKER_URL = os.environ.get("BROKER_URL", "tcp://broker:5556")


def decode(raw: bytes) -> dict | None:
    try:
        message = msgpack.unpackb(raw, raw=False)
    except Exception:
        return None
    if isinstance(message, dict):
        return message
    return None


def serve_one(socket: zmq.Socket, state: dict) -> None:
    raw = socket.recv()
    message = decode(raw)
    print(f"Mensagem recebida: {message}", flush=True)
    now_ms = int(time.time() * 1000)
    reply = dispatch(state, message, now_ms)
    if reply["op"] in ("login_ok", "create_ok"):
        save_state(STATE_PATH, state)
    socket.send(msgpack.packb(reply, use_bin_type=True))
    print(f"Mensagem enviada: {reply}", flush=True)


def main() -> None:
    state = load_state(STATE_PATH)
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.connect(BROKER_URL)
    while True:
        serve_one(socket, state)


if __name__ == "__main__":
    main()

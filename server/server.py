"""Servidor REP: login, canais e publicacao no proxy Pub/Sub."""

import os
import time

import msgpack
import zmq

from protocol import dispatch
from store import load_state, save_state

STATE_PATH = os.environ.get("STATE_PATH", "/data/state.json")
BROKER_URL = os.environ.get("BROKER_URL", "tcp://broker:5556")
PUB_URL = os.environ.get("PUB_URL", "tcp://proxy:5557")
REF_URL = os.environ.get("REF_URL", "tcp://referencia:5559")
SERVER_NAME = os.environ.get("SERVER_NAME", "servidor")
PERSIST_OPS = ("login_ok", "create_ok", "publish_ok")


def decode(raw: bytes) -> dict | None:
    try:
        message = msgpack.unpackb(raw, raw=False)
    except Exception:
        return None
    if isinstance(message, dict):
        return message
    return None


def open_rep(context: zmq.Context) -> zmq.Socket:
    socket = context.socket(zmq.REP)
    socket.connect(BROKER_URL)
    return socket


def open_pub(context: zmq.Context) -> zmq.Socket:
    socket = context.socket(zmq.PUB)
    socket.connect(PUB_URL)
    return socket


def receive(socket: zmq.Socket) -> dict | None:
    message = decode(socket.recv())
    print(f"Mensagem recebida: {message}", flush=True)
    return message


def send_publication(publisher: zmq.Socket, payload: dict) -> None:
    topic = str(payload["channel"]).encode()
    body = msgpack.packb(payload, use_bin_type=True)
    publisher.send_multipart([topic, body])
    print(f"Mensagem enviada: {payload}", flush=True)


def send_reply(socket: zmq.Socket, reply: dict) -> None:
    socket.send(msgpack.packb(reply, use_bin_type=True))
    print(f"Mensagem enviada: {reply}", flush=True)


def serve_one(socket: zmq.Socket, publisher: zmq.Socket, state: dict) -> None:
    message = receive(socket)
    now_ms = int(time.time() * 1000)
    reply, payload = dispatch(state, message, now_ms)
    if payload is not None:
        send_publication(publisher, payload)
    if reply["op"] in PERSIST_OPS:
        save_state(STATE_PATH, state)
    send_reply(socket, reply)


def request_rank(context: zmq.Context) -> int:
    socket = context.socket(zmq.REQ)
    socket.connect(REF_URL)
    time.sleep(1)
    message = {"op": "rank", "ts": int(time.time() * 1000), "name": SERVER_NAME}
    socket.send(msgpack.packb(message, use_bin_type=True))
    print(f"Mensagem enviada: {message}", flush=True)
    reply = decode(socket.recv())
    print(f"Mensagem recebida: {reply}", flush=True)
    socket.close()
    return rank_value(reply)


def rank_value(reply: dict | None) -> int:
    if not isinstance(reply, dict) or isinstance(reply.get("rank"), bool):
        raise RuntimeError(f"rank invalido: {reply!r}; esperado mapa com rank inteiro")
    rank = reply.get("rank")
    if not isinstance(rank, int):
        raise RuntimeError(f"rank invalido: {reply!r}; esperado mapa com rank inteiro")
    return rank


def main() -> None:
    state = load_state(STATE_PATH)
    context = zmq.Context()
    rank = request_rank(context)
    print(f"rank obtido: {rank} nome: {SERVER_NAME}", flush=True)
    serve_forever(open_rep(context), open_pub(context), state)


def serve_forever(socket: zmq.Socket, publisher: zmq.Socket, state: dict) -> None:
    while True:
        serve_one(socket, publisher, state)


if __name__ == "__main__":
    main()

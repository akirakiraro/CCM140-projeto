"""Servidor REP: login, canais, publicacao e eleicao do coordenador."""

import os
import threading
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
# 5555-5559 ja sao broker, proxy e referencia. A eleicao fala de servidor para servidor.
PEER_PORT = 5560
ELECTION_TIMEOUT_MS = 3000
PERSIST_OPS = ("login_ok", "create_ok", "publish_ok")
# Lamport: sobe 1 antes de cada envio; na chegada fica max(local, lc da mensagem).
clock = 0
clock_lock = threading.Lock()
client_messages = 0
# ts do servidor = relogio da maquina + este offset, vindo do heartbeat.
clock_offset = 0
coordinator = ""


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


def observe(message: dict | None) -> None:
    global clock
    if not isinstance(message, dict):
        return
    seen = message.get("lc")
    if isinstance(seen, bool) or not isinstance(seen, int):
        return
    with clock_lock:
        clock = max(clock, seen)


def stamp(message: dict) -> dict:
    global clock
    with clock_lock:
        clock += 1
        message["lc"] = clock
    return message


def receive(socket: zmq.Socket) -> dict | None:
    message = decode(socket.recv())
    print(f"Mensagem recebida: {message}", flush=True)
    observe(message)
    return message


def send_publication(publisher: zmq.Socket, payload: dict) -> None:
    stamp(payload)
    topic = str(payload["channel"]).encode()
    body = msgpack.packb(payload, use_bin_type=True)
    publisher.send_multipart([topic, body])
    print(f"Mensagem enviada: {payload}", flush=True)


def send_reply(socket: zmq.Socket, reply: dict) -> None:
    stamp(reply)
    socket.send(msgpack.packb(reply, use_bin_type=True))
    print(f"Mensagem enviada: {reply}", flush=True)


def server_now() -> int:
    return int(time.time() * 1000) + clock_offset


def serve_one(socket: zmq.Socket, publisher: zmq.Socket, state: dict, context: zmq.Context) -> None:
    message = receive(socket)
    reply, payload = dispatch(state, message, server_now())
    if payload is not None:
        send_publication(publisher, payload)
    if reply["op"] in PERSIST_OPS:
        save_state(STATE_PATH, state)
    send_reply(socket, reply)
    count_client(context)


def ask(
    context: zmq.Context,
    fields: dict,
    url: str = REF_URL,
    timeout_ms: int | None = None,
) -> dict | None:
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    arm_timeout(socket, timeout_ms)
    socket.connect(url)
    message = stamp(fields)
    socket.send(msgpack.packb(message, use_bin_type=True))
    print(f"Mensagem enviada: {message}", flush=True)
    reply = recv_or_none(socket)
    print(f"Mensagem recebida: {reply}", flush=True)
    observe(reply)
    socket.close()
    return reply


def arm_timeout(socket: zmq.Socket, timeout_ms: int | None) -> None:
    if timeout_ms is None:
        return
    socket.setsockopt(zmq.RCVTIMEO, timeout_ms)
    socket.setsockopt(zmq.SNDTIMEO, timeout_ms)


def recv_or_none(socket: zmq.Socket) -> dict | None:
    try:
        return decode(socket.recv())
    except zmq.Again:
        return None


def request_rank(context: zmq.Context) -> int:
    sent = {"op": "rank", "ts": server_now(), "name": SERVER_NAME}
    return rank_value(ask(context, sent))


def request_list(context: zmq.Context) -> list[dict]:
    sent = {"op": "list", "ts": server_now(), "name": SERVER_NAME}
    return server_list(ask(context, sent))


def count_client(context: zmq.Context) -> None:
    global client_messages
    client_messages += 1
    if client_messages % 10 == 0:
        sync_clock(context)


def sync_clock(context: zmq.Context) -> None:
    global clock_offset
    sent = {"op": "heartbeat", "ts": server_now(), "name": SERVER_NAME}
    remote = reply_ts(ask(context, sent))
    clock_offset = remote - int(time.time() * 1000)
    print(f"relogio ajustado: ts_referencia={remote} offset_ms={clock_offset}", flush=True)


def reply_ts(reply: dict | None) -> int:
    if not isinstance(reply, dict) or reply.get("op") != "heartbeat_ok":
        raise RuntimeError(f"heartbeat invalido: {reply!r}; esperado heartbeat_ok com ts inteiro")
    remote = reply.get("ts")
    if isinstance(remote, bool) or not isinstance(remote, int):
        raise RuntimeError(f"heartbeat invalido: {reply!r}; esperado heartbeat_ok com ts inteiro")
    return remote


def server_list(reply: dict | None) -> list[dict]:
    if not isinstance(reply, dict) or reply.get("op") != "list_ok":
        raise RuntimeError(f"lista invalida: {reply!r}; esperado list_ok com servers")
    found = reply.get("servers")
    if not isinstance(found, list):
        raise RuntimeError(f"lista invalida: {reply!r}; esperado list_ok com servers")
    return found


def rank_value(reply: dict | None) -> int:
    if not isinstance(reply, dict) or isinstance(reply.get("rank"), bool):
        raise RuntimeError(f"rank invalido: {reply!r}; esperado mapa com rank inteiro")
    rank = reply.get("rank")
    if not isinstance(rank, int):
        raise RuntimeError(f"rank invalido: {reply!r}; esperado mapa com rank inteiro")
    return rank


def is_rank(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def higher_than(known: list[dict], rank: int) -> list[dict]:
    found: list[dict] = []
    for item in known:
        name = item.get("name")
        if is_rank(item.get("rank")) and item["rank"] > rank and isinstance(name, str):
            found.append(item)
    return found


def election_names(replies: list[dict | None]) -> list[str]:
    names: list[str] = []
    for reply in replies:
        if not isinstance(reply, dict) or reply.get("op") != "election_ok":
            continue
        name = reply.get("name")
        if isinstance(name, str):
            names.append(name)
    return names


def rank_key(item: dict) -> int:
    return int(item["rank"])


def winner_name(replies: list[dict | None], known: list[dict]) -> str:
    names = set(election_names(replies))
    ranked = [item for item in known if item.get("name") in names and is_rank(item.get("rank"))]
    if not ranked:
        return SERVER_NAME
    return str(max(ranked, key=rank_key)["name"])


def ask_election(context: zmq.Context, name: str) -> dict | None:
    sent = {"op": "election", "ts": server_now(), "name": SERVER_NAME}
    url = f"tcp://{name}:{PEER_PORT}"
    return ask(context, sent, url, ELECTION_TIMEOUT_MS)


def adopt(name: str) -> None:
    global coordinator
    coordinator = name
    print(f"coordenador: {coordinator}", flush=True)


def elect(context: zmq.Context, known: list[dict], rank: int) -> None:
    ahead = higher_than(known, rank)
    if not ahead:
        adopt(SERVER_NAME)
        return
    replies = [ask_election(context, str(item["name"])) for item in ahead]
    adopt(winner_name(replies, known))


def open_peer(context: zmq.Context) -> zmq.Socket:
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{PEER_PORT}")
    return socket


def peer_reply(message: dict | None) -> dict:
    if isinstance(message, dict) and message.get("op") == "election":
        return {"op": "election_ok", "ts": server_now(), "name": SERVER_NAME}
    return {"op": "error", "ts": server_now(), "error": f"pedido invalido: {message!r}"}


def answer_peer(socket: zmq.Socket) -> None:
    send_reply(socket, peer_reply(receive(socket)))


def peer_loop(context: zmq.Context) -> None:
    socket = open_peer(context)
    while True:
        answer_peer(socket)


def main() -> None:
    state = load_state(STATE_PATH)
    context = zmq.Context()
    # O REP do broker fica bloqueado no recv. A eleicao responde neste outro fio.
    threading.Thread(target=peer_loop, args=(context,), daemon=True).start()
    time.sleep(1)
    rank = request_rank(context)
    print(f"rank obtido: {rank} nome: {SERVER_NAME}", flush=True)
    # O outro servidor pede o rank neste segundo. A lista sai com os dois nomes.
    time.sleep(1)
    known = request_list(context)
    print(f"servidores conhecidos: {known}", flush=True)
    elect(context, known, rank)
    serve_forever(open_rep(context), open_pub(context), state, context)


def serve_forever(
    socket: zmq.Socket, publisher: zmq.Socket, state: dict, context: zmq.Context
) -> None:
    while True:
        serve_one(socket, publisher, state, context)


if __name__ == "__main__":
    main()

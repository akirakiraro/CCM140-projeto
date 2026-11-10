"""Servidor REP: login, canais, publicacao, eleicao e copia do registro."""

import os
import threading
import time

import msgpack
import zmq

from protocol import dispatch
from replica import pack_channel, pack_login, pack_publication, record_label, store_record
from store import load_state, save_state

STATE_PATH = os.environ.get("STATE_PATH", "/data/state.json")
BROKER_URL = os.environ.get("BROKER_URL", "tcp://broker:5556")
PUB_URL = os.environ.get("PUB_URL", "tcp://proxy:5557")
SUB_URL = os.environ.get("SUB_URL", "tcp://proxy:5558")
SERVERS_TOPIC = b"servers"
REF_URL = os.environ.get("REF_URL", "tcp://referencia:5559")
SERVER_NAME = os.environ.get("SERVER_NAME", "servidor")
# 5555-5559 ja sao broker, proxy e referencia. A eleicao fala de servidor para servidor.
PEER_PORT = 5560
ELECTION_TIMEOUT_MS = 3000
# A referencia tambem espera 30 s. Da tempo de um ajuste de relogio entre os bots.
COORDINATOR_SILENCE_S = 30
PERSIST_OPS = ("login_ok", "create_ok", "publish_ok")
# Lamport: sobe 1 antes de cada envio; na chegada fica max(local, lc da mensagem).
clock = 0
clock_lock = threading.Lock()
client_messages = 0
# ts do servidor = relogio da maquina + este offset, pedido ao coordenador.
clock_offset = 0
coordinator = ""
known_servers: list[dict] = []
my_rank = 0
coordinator_seen = 0.0
# O fio do broker e o fio da porta 5560 gravam o mesmo arquivo.
state_lock = threading.Lock()


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


def copy_for_replica(state: dict, reply: dict) -> dict | None:
    op = reply.get("op")
    if op == "login_ok" and state["logins"]:
        return pack_login(state["logins"][-1])
    if op == "create_ok" and state["channels"]:
        return pack_channel(state["channels"][-1])
    if op == "publish_ok" and state["publications"]:
        return pack_publication(state["publications"][-1])
    return None


def serve_one(socket: zmq.Socket, publisher: zmq.Socket, state: dict, context: zmq.Context) -> None:
    message = receive(socket)
    with state_lock:
        reply, payload = dispatch(state, message, server_now())
        record = copy_for_replica(state, reply)
        if reply["op"] in PERSIST_OPS:
            save_state(STATE_PATH, state)
    if payload is not None:
        send_publication(publisher, payload)
    # O bot so segue depois da copia. O proximo pedido no outro servidor ja acha o registro.
    if record is not None:
        push_replica(context, record)
    send_reply(socket, reply)
    count_client(context, publisher)


def peer_names() -> list[str]:
    names: list[str] = []
    for item in known_servers:
        name = item.get("name")
        if isinstance(name, str) and name != SERVER_NAME:
            names.append(name)
    return names


def push_replica(context: zmq.Context, record: dict) -> None:
    label = record_label(record)
    sent = {"op": "replicate", "ts": server_now(), "name": SERVER_NAME, "record": record}
    for name in peer_names():
        reply = ask(context, sent, f"tcp://{name}:{PEER_PORT}", ELECTION_TIMEOUT_MS)
        if isinstance(reply, dict) and reply.get("op") == "replicate_ok":
            continue
        print(f"replica pendente: {name} {label}", flush=True)


def accept_replica(state: dict, message: dict) -> dict:
    record = message.get("record")
    try:
        with state_lock:
            inserted = store_record(state, record)
            if inserted:
                save_state(STATE_PATH, state)
    except ValueError as exc:
        return {"op": "error", "ts": server_now(), "error": str(exc)}
    status = "aplicada" if inserted else "ja estava"
    print(f"replica {status}: {record_label(record)}", flush=True)
    return {"op": "replicate_ok", "ts": server_now(), "name": SERVER_NAME}


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
    if not send_packet(socket, message):
        print(f"Mensagem enviada: {message}", flush=True)
        print("Mensagem recebida: None", flush=True)
        socket.close()
        return None
    print(f"Mensagem enviada: {message}", flush=True)
    reply = recv_or_none(socket)
    print(f"Mensagem recebida: {reply}", flush=True)
    observe(reply)
    socket.close()
    return reply


def send_packet(socket: zmq.Socket, message: dict) -> bool:
    try:
        socket.send(msgpack.packb(message, use_bin_type=True))
        return True
    except zmq.Again:
        return False


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


def count_client(context: zmq.Context, publisher: zmq.Socket) -> None:
    global client_messages
    client_messages += 1
    if client_messages % 10 == 0:
        send_heartbeat(context)
    if client_messages % 15 == 0:
        sync_from_coordinator(context, publisher)


def send_heartbeat(context: zmq.Context) -> None:
    sent = {"op": "heartbeat", "ts": server_now(), "name": SERVER_NAME}
    reply = ask(context, sent)
    if not isinstance(reply, dict) or reply.get("op") != "heartbeat_ok":
        raise RuntimeError(f"heartbeat invalido: {reply!r}; esperado heartbeat_ok")


def sync_from_coordinator(context: zmq.Context, publisher: zmq.Socket) -> None:
    if coordinator == "" or coordinator == SERVER_NAME:
        return
    t1 = server_now()
    sent = {"op": "clock", "ts": t1, "name": SERVER_NAME}
    url = f"tcp://{coordinator}:{PEER_PORT}"
    reply = ask(context, sent, url, ELECTION_TIMEOUT_MS)
    if not clock_ready(reply):
        take_over(context, publisher)
        return
    apply_berkeley(t1, server_now(), reply)
    note_coordinator_alive()


def clock_ready(reply: dict | None) -> bool:
    if not isinstance(reply, dict) or reply.get("op") != "clock_ok":
        return False
    return is_rank(reply.get("t2")) and is_rank(reply.get("ts"))


def note_coordinator_alive() -> None:
    global coordinator_seen
    coordinator_seen = time.monotonic()


def take_over(context: zmq.Context, publisher: zmq.Socket) -> None:
    previous = coordinator
    elect(context, known_servers, my_rank)
    if coordinator == SERVER_NAME and previous != SERVER_NAME:
        announce(publisher, coordinator)


def watch_coordinator(context: zmq.Context, publisher: zmq.Socket) -> None:
    if coordinator == "" or coordinator == SERVER_NAME:
        return
    if time.monotonic() - coordinator_seen < COORDINATOR_SILENCE_S:
        return
    take_over(context, publisher)


def clock_points(reply: dict | None) -> tuple[int, int]:
    if not isinstance(reply, dict) or reply.get("op") != "clock_ok":
        raise RuntimeError(f"relogio invalido: {reply!r}; esperado clock_ok com t2 e ts inteiros")
    t2 = reply.get("t2")
    t3 = reply.get("ts")
    if not is_rank(t2) or not is_rank(t3):
        raise RuntimeError(f"relogio invalido: {reply!r}; esperado clock_ok com t2 e ts inteiros")
    return (int(t2), int(t3))


def apply_berkeley(t1: int, t4: int, reply: dict | None) -> None:
    global clock_offset
    t2, t3 = clock_points(reply)
    # t1 e t4 ja incluem o offset anterior. Theta e so o ajuste desta volta.
    theta = ((t2 - t1) + (t3 - t4)) // 2
    clock_offset += theta
    print(
        f"relogio ajustado: coordenador={coordinator} ts={t3} offset_ms={clock_offset}",
        flush=True,
    )


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
    if coordinator == name:
        return
    coordinator = name
    print(f"coordenador: {coordinator}", flush=True)


def open_sub(context: zmq.Context) -> zmq.Socket:
    socket = context.socket(zmq.SUB)
    socket.connect(SUB_URL)
    socket.setsockopt(zmq.SUBSCRIBE, SERVERS_TOPIC)
    return socket


def split_announcement(frames: list[bytes]) -> tuple[str, dict | None]:
    if len(frames) != 2:
        return (repr(frames), None)
    return (frames[0].decode("utf-8", "replace"), decode(frames[1]))


def keep_announced(topic: str, body: dict | None) -> None:
    global coordinator
    if topic != "servers" or not isinstance(body, dict) or body.get("op") != "coordinator":
        return
    name = body.get("name")
    if isinstance(name, str):
        coordinator = name


def take_announcement(socket: zmq.Socket) -> None:
    topic, body = split_announcement(socket.recv_multipart())
    print(f"Mensagem recebida: [{topic!r}, {body}]", flush=True)
    observe(body)
    keep_announced(topic, body)


def listen_servers(context: zmq.Context) -> None:
    socket = open_sub(context)
    while True:
        take_announcement(socket)


def announce(publisher: zmq.Socket, name: str) -> None:
    payload = stamp({"op": "coordinator", "ts": server_now(), "name": name})
    body = msgpack.packb(payload, use_bin_type=True)
    publisher.send_multipart([SERVERS_TOPIC, body])
    print(f"Mensagem enviada: ['servers', {payload}]", flush=True)


def elect(context: zmq.Context, known: list[dict], rank: int) -> None:
    ahead = higher_than(known, rank)
    if not ahead:
        adopt(SERVER_NAME)
        note_coordinator_alive()
        return
    replies = [ask_election(context, str(item["name"])) for item in ahead]
    adopt(winner_name(replies, known))
    note_coordinator_alive()


def open_peer(context: zmq.Context) -> zmq.Socket:
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{PEER_PORT}")
    return socket


def peer_reply(message: dict | None, arrived: int) -> dict:
    if isinstance(message, dict) and message.get("op") == "election":
        return {"op": "election_ok", "ts": server_now(), "name": SERVER_NAME}
    if isinstance(message, dict) and message.get("op") == "clock":
        return {"op": "clock_ok", "ts": server_now(), "name": SERVER_NAME, "t2": arrived}
    return {"op": "error", "ts": server_now(), "error": f"pedido invalido: {message!r}"}


def answer_peer(socket: zmq.Socket, state: dict) -> None:
    message = receive(socket)
    if isinstance(message, dict) and message.get("op") == "replicate":
        send_reply(socket, accept_replica(state, message))
        return
    send_reply(socket, peer_reply(message, server_now()))


def peer_loop(context: zmq.Context, state: dict) -> None:
    socket = open_peer(context)
    while True:
        answer_peer(socket, state)


def main() -> None:
    global known_servers, my_rank
    state = load_state(STATE_PATH)
    context = zmq.Context()
    # O REP do broker fica bloqueado no recv. A eleicao responde neste outro fio.
    threading.Thread(target=peer_loop, args=(context, state), daemon=True).start()
    # O proxy nao guarda historico. A inscricao no topico sobe antes do anuncio.
    threading.Thread(target=listen_servers, args=(context,), daemon=True).start()
    time.sleep(1)
    rank = request_rank(context)
    print(f"rank obtido: {rank} nome: {SERVER_NAME}", flush=True)
    # O outro servidor pede o rank neste segundo. A lista sai com os dois nomes.
    time.sleep(1)
    known = request_list(context)
    print(f"servidores conhecidos: {known}", flush=True)
    known_servers = known
    my_rank = rank
    publisher = open_pub(context)
    elect(context, known, rank)
    if coordinator == SERVER_NAME:
        announce(publisher, coordinator)
    serve_forever(open_rep(context), publisher, state, context)


def serve_forever(
    socket: zmq.Socket, publisher: zmq.Socket, state: dict, context: zmq.Context
) -> None:
    # Para de esperar cliente quando o coordenador some. O broker nao entrega essa ausencia.
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    while True:
        ready = dict(poller.poll(1000))
        if socket in ready:
            serve_one(socket, publisher, state, context)
        watch_coordinator(context, publisher)


if __name__ == "__main__":
    main()

"""Quebra a foto do caderno em uma linha por registro.

A carta snapshot_ok carrega o historico inteiro. Numa linha so, o Docker
mistura a saida dos outros containers no meio do texto.
"""


def show(label: str, message: object) -> None:
    """Imprime a carta. A foto grande vira um resumo e uma linha por registro."""
    rows = record_rows(message)
    if rows is None:
        print(f"{label}: {message}", flush=True)
        return
    print(f"{label}: {headline(message)}", flush=True)
    for row in rows:
        print(f"  {row}", flush=True)


def record_rows(message: object) -> list[str] | None:
    if not isinstance(message, dict):
        return None
    found = False
    rows: list[str] = []
    for key in ("logins", "channels", "publications"):
        block = message.get(key)
        if not isinstance(block, list):
            continue
        found = True
        rows.extend(row_line(item) for item in block)
    if not found:
        return None
    return rows


def headline(message: dict) -> str:
    parts = [f"{key}={message[key]}" for key in ("op", "name", "ts", "lc") if key in message]
    for key in ("logins", "channels", "publications"):
        block = message.get(key)
        if isinstance(block, list):
            parts.append(f"{key}={len(block)}")
    return " ".join(parts)


def row_line(item: object) -> str:
    if not isinstance(item, dict):
        return str(item)
    kind = str(item.get("kind", ""))
    if kind == "login":
        return f"login {item.get('user')} ts={item.get('ts')}"
    if kind == "channel":
        return f"channel {item.get('name')} creator={item.get('creator')} ts={item.get('ts')}"
    if kind == "publication":
        return (
            f"publication {item.get('user')} {item.get('channel')} "
            f"{item.get('text')} ts={item.get('ts')}"
        )
    return str(item)

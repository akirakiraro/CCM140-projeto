# BBS simples (CCM140)

Versão reduzida de um bulletin board: bots entram no serviço, descobrem canais e criam canais novos. O tráfego passa por um broker ZeroMQ. Cada servidor guarda o próprio histórico em disco.  

- Rodrigo Akira Rosado Yatate   
- RA: 22.124.093-0

## Como executar

Na raiz do repositório `CCM140-projeto`:

```bash
docker compose up --build
```

Sobe 1 broker, 1 proxy Pub/Sub, 2 servidores e 2 clientes. A correção prevista no enunciado é `docker compose up`. Para encerrar: `docker compose down`.

O proxy é o da aula (`zmq.proxy` entre XSUB e XPUB). Publicadores vão conectar em `tcp://proxy:5557`. Inscritos vão conectar em `tcp://proxy:5558`. Ele não lê a mensagem; só encaminha.

Os volumes `servidor-1-data` e `servidor-2-data` sobrevivem a um restart. Cada um é de um servidor só: o enunciado não permite compartilhar o arquivo entre servidores.

## Escolhas do projeto

| Peça | Linguagem | Motivo |
| --- | --- | --- |
| Broker e servidores | Python | Mesmo estilo do laboratório de request-reply |
| Bots (2 containers) | Java | Segunda linguagem exigida no trabalho individual |

**Serialização:** Utilizei o MessagePack. Binário, com biblioteca em Python e Java, e citado no enunciado.

**Persistência:** JSON em `/data/state.json`, um arquivo por container. O formato em disco é livre. O que é obrigatório está lá:

- `logins`: cada login bem-sucedido, com o nome e o timestamp do servidor no momento do registro
- `channels`: nome, criador e timestamp de criação
- `publications`: cada publicação aceita, com usuário, canal, texto, `request_ts` (relógio de quem pediu) e `ts` (relógio do servidor no envio ao canal)

**Publicação:** o bot pede com `publish` (canal, texto e `ts` do pedido). O servidor só publica se o canal existe no arquivo dele. A mensagem no proxy vai em dois quadros ZeroMQ: o nome do canal e o corpo em MessagePack (`op=channel_message`, usuário, canal, texto e `ts` do servidor). A resposta ao bot é `publish_ok` ou `publish_err`.

Gravação atômica (arquivo temporário + rename) para um restart no meio da escrita não corromper o estado.

**Relógio das mensagens:** campo `ts`, inteiro, epoch em milissegundos, preenchido por quem envia.

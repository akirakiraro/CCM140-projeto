# BBS simples (CCM140)

Versão reduzida de um bulletin board: bots entram no serviço, descobrem canais e criam canais novos. O tráfego passa por um broker ZeroMQ. Cada servidor guarda o próprio histórico em disco.  

- Rodrigo Akira Rosado Yatate   
- RA: 22.124.093-0

## Como executar

Na raiz do repositório `CCM140-projeto`:

```bash
docker compose up --build
```

Sobe 1 broker, 2 servidores e 2 clientes. A correção prevista no enunciado é `docker compose up`. Para encerrar: `docker compose down`.

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

Gravação atômica (arquivo temporário + rename) para um restart no meio da escrita não corromper o estado.

**Relógio das mensagens:** campo `ts`, inteiro, epoch em milissegundos, preenchido por quem envia.

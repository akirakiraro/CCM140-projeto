# BBS simples (CCM140)

Versão reduzida de um bulletin board: bots entram no serviço, descobrem canais e criam canais novos. O tráfego passa por um broker ZeroMQ. Cada servidor guarda o próprio histórico em disco e copia esse histórico para o outro. O arquivo não é compartilhado.  

- Rodrigo Akira Rosado Yatate   
- RA: 22.124.093-0

## Como executar

Na pasta `CCM140-projeto`, a primeira vez:

```bash
docker compose up --build
```

Esse comando constrói as imagens e sobe todos os containers. As trocas de mensagem acontecem sem interação.

Nas vezes seguintes, com as imagens já construídas:

```bash
docker compose up
```

Para encerrar:

```bash
docker compose down
```

## Escolhas do projeto

| Peça | Linguagem | Motivo |
| --- | --- | --- |
| Broker, proxy, referência e servidores | Python | Mesmo estilo do laboratório de request-reply |
| Bots (2 containers) | Java | Segunda linguagem exigida no trabalho individual |

**Serialização.** MessagePack. É binário e tem biblioteca em Python e em Java. Toda mensagem leva `ts`, inteiro, epoch em milissegundos, preenchido por quem envia.

**Persistência.** Cada servidor grava o próprio `/data/state.json`. O arquivo não é compartilhado. O formato em disco é JSON:

- `logins`: nome e timestamp do servidor em cada login bem-sucedido
- `channels`: nome, criador e timestamp de criação
- `publications`: usuário, canal, texto, `request_ts` (relógio de quem pediu) e `ts` (relógio do servidor no envio ao canal)

**Troca de mensagens.** O pedido do bot vai ao broker. A publicação no canal passa por um proxy Pub/Sub separado. Publicadores conectam em `tcp://proxy:5557` (XSUB). Inscritos conectam em `tcp://proxy:5558` (XPUB). O proxy encaminha a mensagem.

Para publicar, o bot manda `publish` com o canal, o texto e o `ts` do pedido. O servidor publica se o canal existe no arquivo dele. A mensagem no proxy vai em dois quadros: o nome do canal e o corpo em MessagePack (`op=channel_message`, usuário, canal, texto e `ts` do servidor). A resposta ao bot é `publish_ok` ou `publish_err`.

A inscrição fica no bot, no socket SUB em `tcp://proxy:5558`. Enquanto tiver menos de 3 inscrições, cada volta sorteia um canal da lista em que ainda não entrou. A linha recebida mostra o canal, o texto, o `ts` de envio e o `ts` de recebimento.

No laço, se a lista tem menos de 5 canais, o bot cria `Sala-` mais o próximo número que não está na lista. Depois escolhe um canal e envia 10 textos, com 1 segundo entre cada envio.

## Replicação

O broker entrega cada pedido a um servidor diferente, em rodízio. Sem cópia, cada disco fica com uma parte dos logins, dos canais e das publicações. Se um servidor para, quem pergunta ao outro não vê essa parte.

**Método.** Réplica passiva, com push e pull. Quem gravou copia o registro. A outra réplica guarda essa cópia. Os dois continuam com o próprio `/data/state.json`. Depois da cópia, os dois têm os mesmos logins, canais e publicações.

**Implementação.** `server/replica.py`, chamado por `server/server.py`. A carta continua em MessagePack, na porta 5560, de um servidor ao outro.

No push, depois de `login_ok`, `create_ok` ou `publish_ok`, quem gravou manda `op=replicate` com o registro. O campo `kind` diz se é `login`, `channel` ou `publication`. O outro grava, se ainda não tinha, e responde `replicate_ok`. A resposta ao bot sai depois dessa resposta.

No pull, na subida, o servidor manda `op=snapshot`. A resposta `snapshot_ok` traz as três listas. Ele junta ao arquivo local o que faltava. Registro repetido não entra de novo. Login casa usuário e `ts`. Publicação casa usuário, canal, texto, `ts` e `request_ts`. Canal casa pelo nome. Cada disco mantém a ordem que já tinha e acrescenta o resto no fim.

**O que mudou em relação ao conteudo das aulas.**

- Quem aceitou o pedido empurra o registro que acabou de gravar. A outra réplica guarda a cópia e não executa o pedido outra vez.
- O push copia para quem está no ar. O pull na subida traz o que ficou de fora, inclusive quando a cópia não teve resposta em 3 segundos. O servidor responde ao bot mesmo assim.
- Se os dois gravarem o mesmo nome de canal antes da cópia chegar, permanece o registro que já está no disco. Criador e `ts` podem diferir.

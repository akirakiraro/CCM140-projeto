# BBS simples (CCM140)

Versão reduzida de um bulletin board: bots entram no serviço, descobrem canais e criam canais novos. O tráfego passa por um broker ZeroMQ. Cada servidor guarda o próprio histórico em disco e copia esse histórico para o outro. O arquivo não é compartilhado.  

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

**Inscrição:** o bot mantém um socket SUB em `tcp://proxy:5558` junto com o REQ do broker. Enquanto tiver menos de 3 inscrições, cada volta sorteia um canal da lista em que ainda não entrou. O que chega é impresso com o canal, o texto, o `ts` de envio (veio na mensagem) e o `ts` de recebimento (relógio do bot ao receber).

**Volta do bot:** se a lista tem menos de 5 canais, cria um. O nome é `Sala-` mais o próximo número que não está na lista (`Sala-1`, `Sala-2`, ...). Depois escolhe um canal dessa lista e envia 10 textos aleatórios (`mensagem`, um número e o nome do bot), com 1 segundo entre cada envio.

Gravação atômica (arquivo temporário + rename) para um restart no meio da escrita não corromper o estado.

**Relógio das mensagens:** campo `ts`, inteiro, epoch em milissegundos, preenchido por quem envia.

## Replicação

O broker entrega cada pedido a um servidor diferente, em rodízio. Sem cópia, cada disco fica com uma parte dos logins, dos canais e das publicações. Se um servidor para, essa parte deixa de existir. Uma lista ou uma publicação atendida pelo outro servidor não vê o que está só no primeiro.

**Método:** réplica passiva, com atualização por push e por pull. Na aula, réplica passiva é a cópia do dado entre as réplicas. No push, quem tem o dado novo inicia a cópia. No pull, a réplica pede o estado.

Isso resolve o problema sem juntar os volumes. Os dois continuam com o próprio `/data/state.json`. Depois da cópia, os dois têm os mesmos logins, canais e publicações. O histórico não depende do servidor que atendeu o pedido.

**Implementação:** `server/replica.py`, chamado por `server/server.py`. A carta continua em MessagePack. O caminho é a porta 5560, a mesma da eleição e do relógio, de um servidor ao outro.

No push, depois de `login_ok`, `create_ok` ou `publish_ok`, quem gravou manda `op=replicate` com o registro. O campo `kind` diz se é `login`, `channel` ou `publication`. O outro grava, se ainda não tinha, e responde `replicate_ok`. O ok ao bot sai depois dessa resposta. O pedido seguinte, mesmo caindo no outro servidor, já encontra o registro. No log isso aparece como `replica aplicada`.

No pull, na subida, depois de saber o nome do outro, o servidor manda `op=snapshot`. A resposta `snapshot_ok` traz as três listas. Ele junta ao arquivo local o que faltava. No log: `replica puxada: de=... novos=... canais=...`. Os nomes dos canais coincidem. A ordem pode diferir, porque cada disco mantém a que já tinha e acrescenta o resto no fim.

Registro repetido não entra de novo. Login casa usuário e `ts`. Publicação casa usuário, canal, texto, `ts` e `request_ts`. Canal casa pelo nome.

**O que mudou para caber no projeto:**

- Na aula, a réplica passiva tem um primário: só ele escreve e depois copia para os backups. Aqui o broker já escolhe quem atende. Eu não desviei toda escrita para o coordenador. Quem aceitou o pedido empurra o registro que acabou de gravar. A outra réplica não executa o pedido outra vez. Executar de novo seria réplica ativa, e o `ts` sairia diferente.
- O push só alcança quem está no ar naquela escrita. O pull na subida cobre o que já estava só no outro volume e o que ficou de fora quando a cópia não teve resposta em 3 segundos (`replica pendente`). A aula trata push e pull como alternativas. Aqui os dois se completam.
- Se o outro não responde, o servidor ainda responde ao bot. Esperar sem prazo travaria o serviço quando um container para. O que faltou entra no pull da próxima subida.
- Os dois podem gravar o mesmo nome de canal antes da cópia chegar. Na consistência eventual forte, a aula desempataria pelo relógio e ficaria com a atualização mais recente. Aqui, se o nome já está no disco, o registro local permanece. Os dois ficam com o canal. Criador e `ts` podem diferir nesse caso.

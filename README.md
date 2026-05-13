# Projeto ASD - Sistemas Distribuídos

Este repositório contém a implementação de um sistema distribuído baseado na arquitetura **Master-Worker** com balanceamento de carga dinâmica e protocolo **P2P (Peer-to-Peer)**.

O objetivo é coordenar tarefas entre múltiplos nós utilizando comunicação via **Sockets TCP** e mensagens padronizadas em **JSON**, garantindo autonomia e interoperabilidade entre sistemas de diferentes equipes.

---

# 📋 Pré-requisitos

- Python 3.x

### Bibliotecas nativas utilizadas
As bibliotecas abaixo já vêm incluídas no Python padrão:

- `socket`
- `json`
- `threading`
- `queue`
- `logging`

---

# 🏗️ Arquitetura Geral

## Nó Master
Responsável por:

- Gerenciar uma fila de tarefas
- Distribuir trabalho para Workers locais
- Monitorar carga do sistema
- Negociar empréstimo de Workers com Masters vizinhos quando saturado

## Nó Trabalhador (Worker)
Responsável por:

- Processar tarefas recebidas
- Simular execução
- Reportar status ao Master
- Ser redirecionado dinamicamente para outro Master

## Comunicação
- Protocolo TCP
- Mensagens em JSON
- Delimitador `\n` ao final de cada mensagem
- Protocolo consensual para negociação P2P

---

# 🛰️ Sprint 1 — Mecanismo de Heartbeat

Estabelece a infraestrutura de rede e verifica a disponibilidade entre Worker e Master.

## Detalhes Técnicos

| Item | Tecnologia |
|------|-------------|
| Protocolo | TCP |
| Mensageria | JSON |
| Delimitador | `\n` |
| Concorrência | Threads |

## Payloads Oficiais

### Requisição (Worker → Master)

```json
{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT"
}
```

### Resposta (Master → Worker)

```json
{
  "SERVER_UUID": "Master_A",
  "TASK": "HEARTBEAT",
  "RESPONSE": "ALIVE"
}
```

---

# 🔄 Sprint 2 — Comunicação de Tarefas e Apresentação de Trabalhadores

Implementa:

- Consumo de fila
- Distribuição de tarefas
- Relatórios de status

## Fluxo de Trabalho

### 1. Apresentação
Worker conecta e envia `WORKER_UUID`.

### 2. Distribuição
Master consome tarefa da fila e despacha para o Worker.

### 3. Processamento
Worker simula execução utilizando `sleep` aleatório.

### 4. Confirmação
Worker envia status da execução e Master responde com ACK.

---

## Payloads Oficiais

### Pedido de Tarefa (Worker → Master)

```json
{
  "WORKER": "ALIVE",
  "WORKER_UUID": "W-123",
  "SERVER_UUID": "Master_B"
}
```

> `SERVER_UUID` é opcional caso o Worker esteja emprestado.

---

### Envio de Tarefa (Master → Worker)

```json
{
  "TASK": "QUERY",
  "USER": "Nome_Usuario"
}
```

### Caso não existam tarefas

```json
{
  "TASK": "NO_TASK"
}
```

---

### Relatório de Status (Worker → Master)

```json
{
  "STATUS": "OK",
  "TASK": "QUERY",
  "WORKER_UUID": "W-123"
}
```

### Confirmação (Master → Worker)

```json
{
  "STATUS": "ACK",
  "WORKER_UUID": "W-123"
}
```

---

# 🚀 Sprint 3 — Negociação Master-to-Master e Redirecionamento Dinâmico

Implementa negociação consensual entre Masters para empréstimo de Workers quando um nó está saturado.

Os Workers podem ser redirecionados dinamicamente e devolvidos quando a carga se normaliza.

---

## Fluxo de Trabalho

### 1. Monitoramento
Master verifica o tamanho da fila.

### 2. Negociação
Caso esteja saturado, solicita ajuda aos Masters vizinhos.

### 3. Redirecionamento
O vizinho cede um Worker e o Master instrui o Worker a mudar.

### 4. Devolução
Quando a carga reduz, o Worker retorna ao Master original.

---

## Payloads Oficiais (P2P)

### Pedido de Ajuda (Master → Master)

```json
{
  "TASK": "REQUEST_HELP",
  "MASTER_UUID": "Master_A",
  "NEEDED_WORKERS": 1
}
```

---

### Resposta de Concessão

```json
{
  "TASK": "GRANT_HELP",
  "WORKER_UUID": "W-999"
}
```

### Ou negação

```json
{
  "TASK": "DENY_HELP"
}
```

---

### Redirecionamento de Worker

```json
{
  "TASK": "SWITCH_MASTER",
  "NEW_MASTER": "IP:porta"
}
```

---

### Devolução de Worker

```json
{
  "TASK": "RETURN_WORKER",
  "WORKER_UUID": "W-999"
}
```

---

# 🛠️ Como Executar

## Configuração

Exemplo de `config.json`:

```json
{
  "host": "localhost",
  "port": 5000,
  "master_uuid": "Master_A",
  "threshold": 3,
  "neighbors": ["localhost:5001"],
  "worker_uuid": "Worker_A1",
  "original_master": "Master_A"
}
```

---

## Iniciar o Master

```bash
python master.py
```

O Master abrirá a porta de escuta e iniciará o gerenciamento dos Workers.

---

## Iniciar o Worker

```bash
python work.py
```

O Worker conectará ao Master e iniciará o ciclo de heartbeat e processamento de tarefas.

---

## Testando P2P

Execute múltiplos Masters em portas diferentes:

- `5000`
- `5001`

Configure os vizinhos no `config.json` apontando para os outros Masters.

---

# 🧪 Testes e Cenários

## Cenários CT01-CT05

Verifique os cenários definidos nas especificações:

- Apresentação de Worker
- Distribuição de tarefas
- Relatórios de status
- Heartbeat
- Negociação P2P

---

## Interoperabilidade

Teste a comunicação com sistemas de outras equipes utilizando o protocolo JSON consensual.

---

## Logs

O projeto utiliza `logging` para depuração.

As mensagens de execução aparecem diretamente no console.

---

# 📝 Notas de Implementação

## Strict Parsing
Campos desconhecidos no JSON devem ser ignorados para permitir extensões futuras.

## Sensibilidade a maiúsculas
Valores como:

- `"ALIVE"`
- `"QUERY"`
- `"ACK"`

devem permanecer em maiúsculo.

## Timeout
O Worker aguarda até **5 segundos** por resposta antes de reconectar.

## Resiliência
Reconexão automática em caso de falhas de comunicação.

---

# 🤝 Contribuição

Sinta-se à vontade para abrir:

- Issues
- Pull Requests

Este projeto visa cumprir os objetivos **O1-O6** do plano geral de Sistemas Distribuídos.

---

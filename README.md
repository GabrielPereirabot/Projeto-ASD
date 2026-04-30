# Projeto ASD - Sistemas Distribuídos

Este repositório contém a implementação de um sistema distribuído baseado na arquitetura **Master-Worker** com balanceamento de carga e protocolo **P2P**.

O objetivo principal é coordenar tarefas entre múltiplos nós utilizando comunicação via **Sockets TCP** e mensagens padronizadas em **JSON**.

---

## 📋 Pré-requisitos

- Python 3.x
- Bibliotecas nativas:
  - `socket`
  - `json`
  - `threading`
  - `queue`

---

# 🛰️ Sprint 1: Mecanismo de Heartbeat

O objetivo desta sprint foi estabelecer a infraestrutura de rede e garantir a verificação de disponibilidade (saúde) entre o **Worker** e o **Master**.

## Detalhes Técnicos

- **Protocolo:** TCP  
- **Mensageria:** JSON com delimitador `\n` ao final de cada objeto  
- **Concorrência:** Master implementado com threads para atendimento simultâneo  

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

## 🛠️ Como Executar

### 1. Iniciar o Master

O Master atua como servidor e deve ser iniciado primeiro para abrir a porta de escuta.

```bash
python Master.py
```

### 2. Iniciar o Worker

O Worker atua como cliente e iniciará o ciclo de Heartbeat automaticamente ao conectar.

```bash
python worker.py
```

> **Nota:** Certifique-se de que o IP e a Porta definidos nos scripts sejam os mesmos para que a conexão seja estabelecida corretamente.

---

# 🔄 Sprint 2: Comunicação de Tarefas

Nesta fase, implementamos a lógica de consumo de fila e o reporte de status das tarefas processadas.

## Fluxo de Trabalho

1. **Apresentação:** O Worker se conecta e envia seu `WORKER_UUID`.
2. **Distribuição:** O Master consome uma tarefa da sua queue interna e despacha para o Worker.
3. **Processamento:** O Worker simula a execução da tarefa (`QUERY`).
4. **Confirmação:** Após o reporte de status pelo Worker, o Master responde com um `ACK` para finalizar o ciclo.

## Payloads de Tarefa

### Pedido

```json
{
  "WORKER": "ALIVE",
  "WORKER_UUID": "W-123"
}
```

### Envio

```json
{
  "TASK": "QUERY",
  "USER": "Nome_Usuario"
}
```

### Status

```json
{
  "STATUS": "OK",
  "TASK": "QUERY",
  "WORKER_UUID": "W-123"
}
```

### ACK

```json
{
  "STATUS": "ACK",
  "WORKER_UUID": "W-123"
}
```

---


